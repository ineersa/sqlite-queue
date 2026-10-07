<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\ValueObject\QueueName;

/**
 * Child-side queue runtime over one Amp channel.
 *
 * Receives one init request, constructs Queue over PDO storage, then serves one
 * operation at a time until close or a fatal failure.
 */
final class SqliteWorkerRuntime
{
    private const int DIAGNOSTIC_LIMIT_BYTES = 1_024;

    /**
     * @param Channel<mixed, mixed> $channel
     * @param \Closure(): int       $clock   wall-clock milliseconds sampled inside Queue after transaction acquisition
     */
    public static function run(Channel $channel, \Closure $clock): void
    {
        $storage = null;
        $request = null;
        try {
            $init = $channel->receive();
            self::assertArray($init, 'Initialization request must be an array.');
            self::assertOperation($init, SqliteWorkerOperationEnum::Init);
            if (1 !== ($init['id'] ?? null)) {
                throw new \InvalidArgumentException('Initialization request id must be 1.');
            }
            $data = self::arrayField($init, 'data');
            $database = self::stringField($data, 'database');
            $visibility = self::positiveIntField($data, 'visibility_timeout');
            $synchronous = SqliteSynchronousMode::tryFrom(self::stringField($data, 'synchronous'));
            if (null === $synchronous) {
                throw new \InvalidArgumentException('Initialization synchronous mode must be normal or full.');
            }
            $storage = SqliteQueueStorage::open($database, $synchronous);
            $queue = new Queue($storage, $visibility, $clock);
            $channel->send([
                'id' => 1,
                'op' => SqliteWorkerOperationEnum::Init->value,
                'status' => SqliteWorkerStatusEnum::Ok->value,
                'result' => $storage->configuration(),
            ]);
            self::serve($channel, $queue, $storage, $request);
        } catch (\Throwable $error) {
            try {
                $channel->send([
                    'id' => \is_array($request) && isset($request['id']) && \is_int($request['id']) ? $request['id'] : 0,
                    'op' => \is_array($request) && isset($request['op']) && \is_string($request['op']) ? $request['op'] : SqliteWorkerOperationEnum::Init->value,
                    'status' => SqliteWorkerStatusEnum::Failure->value,
                    'error' => [
                        'class' => $error::class,
                        'message' => self::diagnostic($error->getMessage()),
                    ],
                ]);
            } catch (\Throwable) {
            }
            throw $error;
        } finally {
            $storage?->close();
        }
    }

    /**
     * @param Channel<mixed, mixed>        $channel
     * @param array<array-key, mixed>|null $request
     */
    private static function serve(Channel $channel, Queue $queue, SqliteQueueStorage $storage, ?array &$request): void
    {
        while (true) {
            $request = $channel->receive();
            self::assertArray($request, 'Worker request must be an array.');
            $id = self::positiveIntField($request, 'id');
            $operation = SqliteWorkerOperationEnum::tryFrom(self::stringField($request, 'op'));
            if (null === $operation || SqliteWorkerOperationEnum::Init === $operation) {
                throw new \InvalidArgumentException('Worker request operation is unsupported.');
            }
            $data = self::arrayField($request, 'data');
            try {
                $result = self::dispatch($queue, $storage, $operation, $data);
                $channel->send([
                    'id' => $id,
                    'op' => $operation->value,
                    'status' => SqliteWorkerStatusEnum::Ok->value,
                    'result' => $result,
                ]);
                if (SqliteWorkerOperationEnum::Close === $operation) {
                    return;
                }
            } catch (InvalidReceiptException|\InvalidArgumentException $error) {
                $channel->send([
                    'id' => $id,
                    'op' => $operation->value,
                    'status' => SqliteWorkerStatusEnum::Domain->value,
                    'error' => [
                        'class' => $error::class,
                        'message' => self::diagnostic($error->getMessage()),
                    ],
                ]);
            }
        }
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function dispatch(Queue $queue, SqliteQueueStorage $storage, SqliteWorkerOperationEnum $operation, array $data): mixed
    {
        return match ($operation) {
            SqliteWorkerOperationEnum::Send => self::send($queue, $data),
            SqliteWorkerOperationEnum::Claim => self::claim($queue, $data),
            SqliteWorkerOperationEnum::Settle => self::settle($queue, $data),
            SqliteWorkerOperationEnum::EarliestEligibility => self::earliestEligibility($queue, $data),
            SqliteWorkerOperationEnum::Close => self::close($storage, $data),
            SqliteWorkerOperationEnum::Init => throw new \LogicException('Init is handled during startup.'),
        };
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function send(Queue $queue, array $data): int
    {
        $queueName = new QueueName(self::stringField($data, 'queue'));
        $body = self::stringField($data, 'body');
        $headers = self::stringField($data, 'headers');
        $delay = self::nonNegativeIntField($data, 'delay');
        self::assertPayloadBounds($body, $headers);

        return $queue->send($queueName, $body, $headers, $delay);
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @return ?array{id: int, queue: string, body: string, headers: string, receipt: string, available_at: int, reserved_until: int}
     */
    private static function claim(Queue $queue, array $data): ?array
    {
        $queueName = new QueueName(self::stringField($data, 'queue'));
        $ownerId = self::stringField($data, 'owner_id');
        if ('' === $ownerId) {
            throw new \InvalidArgumentException('Owner identity must be a non-empty string.');
        }
        $delivery = $queue->receive($queueName, $ownerId);
        if (null === $delivery) {
            return null;
        }

        return self::deliveryResult($delivery);
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function settle(Queue $queue, array $data): true
    {
        $receipt = self::stringField($data, 'receipt');
        $ownerId = self::stringField($data, 'owner_id');
        if ('' === $ownerId) {
            throw new \InvalidArgumentException('Owner identity must be a non-empty string.');
        }
        $acknowledge = self::boolField($data, 'acknowledge');
        if ($acknowledge) {
            $queue->acknowledge($receipt, $ownerId);
        } else {
            $queue->reject($receipt, $ownerId);
        }

        return true;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function earliestEligibility(Queue $queue, array $data): ?int
    {
        return $queue->earliestEligibility(new QueueName(self::stringField($data, 'queue')));
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function close(SqliteQueueStorage $storage, array $data): true
    {
        if ([] !== $data) {
            throw new \InvalidArgumentException('Close request must not carry queue data.');
        }
        $storage->close();

        return true;
    }

    /**
     * @return array{id: int, queue: string, body: string, headers: string, receipt: string, available_at: int, reserved_until: int}
     */
    private static function deliveryResult(DeliveryDTO $delivery): array
    {
        self::assertPayloadBounds($delivery->body, $delivery->headers);
        if ($delivery->id <= 0) {
            throw new \RuntimeException('Delivery identity must be positive.');
        }
        if ($delivery->availableAt < 0 || $delivery->reservedUntil < 0) {
            throw new \RuntimeException('Delivery deadlines must be nonnegative.');
        }

        return [
            'id' => $delivery->id,
            'queue' => $delivery->queue,
            'body' => $delivery->body,
            'headers' => $delivery->headers,
            'receipt' => $delivery->receipt,
            'available_at' => $delivery->availableAt,
            'reserved_until' => $delivery->reservedUntil,
        ];
    }

    private static function assertPayloadBounds(string $body, string $headers): void
    {
        $bodyLength = \strlen($body);
        $headersLength = \strlen($headers);
        if ($bodyLength > Limits::MAX_PAYLOAD || $headersLength > Limits::MAX_PAYLOAD || $bodyLength + $headersLength > Limits::MAX_PAYLOAD) {
            throw new \InvalidArgumentException('Payload exceeds the supported frame budget.');
        }
    }

    /**
     * @param array<array-key, mixed> $request
     */
    private static function assertOperation(array $request, SqliteWorkerOperationEnum $expected): void
    {
        $operation = SqliteWorkerOperationEnum::tryFrom(self::stringField($request, 'op'));
        if ($expected !== $operation) {
            throw new \InvalidArgumentException(\sprintf('Expected %s operation.', $expected->value));
        }
    }

    private static function assertArray(mixed $value, string $message): void
    {
        if (!\is_array($value)) {
            throw new \InvalidArgumentException($message);
        }
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @return array<array-key, mixed>
     */
    private static function arrayField(array $data, string $field): array
    {
        $value = $data[$field] ?? null;
        if (!\is_array($value)) {
            throw new \InvalidArgumentException(\sprintf('Field %s must be an array.', $field));
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function stringField(array $data, string $field): string
    {
        $value = $data[$field] ?? null;
        if (!\is_string($value)) {
            throw new \InvalidArgumentException(\sprintf('Field %s must be a string.', $field));
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function positiveIntField(array $data, string $field): int
    {
        $value = $data[$field] ?? null;
        if (!\is_int($value) || $value <= 0) {
            throw new \InvalidArgumentException(\sprintf('Field %s must be a positive integer.', $field));
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function nonNegativeIntField(array $data, string $field): int
    {
        $value = $data[$field] ?? null;
        if (!\is_int($value) || $value < 0) {
            throw new \InvalidArgumentException(\sprintf('Field %s must be a nonnegative integer.', $field));
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $data
     */
    private static function boolField(array $data, string $field): bool
    {
        $value = $data[$field] ?? null;
        if (!\is_bool($value)) {
            throw new \InvalidArgumentException(\sprintf('Field %s must be a boolean.', $field));
        }

        return $value;
    }

    private static function diagnostic(string $message): string
    {
        if (!mb_check_encoding($message, 'UTF-8')) {
            return 'non-utf8 diagnostic';
        }
        if (\strlen($message) <= self::DIAGNOSTIC_LIMIT_BYTES) {
            return $message;
        }

        return substr($message, 0, self::DIAGNOSTIC_LIMIT_BYTES);
    }
}
