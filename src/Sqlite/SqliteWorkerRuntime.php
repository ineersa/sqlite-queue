<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\ValueObject\QueueName;

/**
 * Child-side queue runtime over one Amp channel.
 *
 * Bootstrap acquires PDO storage and Queue, then serves one operation at a time.
 * Malformed internal protocol fails closed. Genuine queue argument and receipt
 * errors remain non-fatal domain replies.
 */
final class SqliteWorkerRuntime
{
    /**
     * @param Channel<mixed, mixed> $channel
     * @param \Closure(): int       $clock   wall-clock milliseconds sampled inside Queue after transaction acquisition
     */
    public static function run(Channel $channel, \Closure $clock): void
    {
        try {
            [$queue, $storage] = self::bootstrap($channel, $clock);
        } catch (\Throwable $error) {
            self::sendFailure($channel, 1, SqliteWorkerOperationEnum::Init, 'bootstrap', $error);
            throw $error;
        }

        try {
            self::serve($channel, $queue, $storage);
        } finally {
            $storage->close();
        }
    }

    /**
     * @param Channel<mixed, mixed> $channel
     * @param \Closure(): int       $clock
     *
     * @return array{Queue, SqliteQueueStorage}
     */
    private static function bootstrap(Channel $channel, \Closure $clock): array
    {
        $init = $channel->receive();
        self::assertArray($init, 'Initialization request must be an array.');
        self::assertOperation($init, SqliteWorkerOperationEnum::Init);
        if (1 !== ($init['id'] ?? null)) {
            throw new \RuntimeException('Initialization request id must be 1.');
        }
        $data = self::arrayField($init, 'data');
        $database = self::stringField($data, 'database');
        $visibility = self::positiveIntField($data, 'visibility_timeout');
        $synchronous = SqliteSynchronousMode::tryFrom(self::stringField($data, 'synchronous'));
        if (null === $synchronous) {
            throw new \RuntimeException('Initialization synchronous mode must be normal or full.');
        }
        $storage = SqliteQueueStorage::open($database, $synchronous);
        try {
            $queue = new Queue($storage, $visibility, $clock);
            $channel->send([
                'id' => 1,
                'op' => SqliteWorkerOperationEnum::Init->value,
                'status' => SqliteWorkerStatusEnum::Ok->value,
                'result' => $storage->configuration(),
            ]);
        } catch (\Throwable $error) {
            $storage->close();
            throw $error;
        }

        return [$queue, $storage];
    }

    /**
     * @param Channel<mixed, mixed> $channel
     */
    private static function serve(Channel $channel, Queue $queue, SqliteQueueStorage $storage): void
    {
        $expectedId = 2;
        while (true) {
            $request = $channel->receive();
            try {
                self::assertArray($request, 'Worker request must be an array.');
                $id = self::positiveIntField($request, 'id');
                if ($id !== $expectedId) {
                    throw new \RuntimeException('Worker request id must increase monotonically.');
                }
                $operation = SqliteWorkerOperationEnum::tryFrom(self::stringField($request, 'op'));
                if (null === $operation || SqliteWorkerOperationEnum::Init === $operation) {
                    throw new \RuntimeException('Worker request operation is unsupported.');
                }
                $data = self::arrayField($request, 'data');
            } catch (\Throwable $error) {
                $id = \is_array($request) && isset($request['id']) && \is_int($request['id']) ? $request['id'] : 0;
                $op = \is_array($request) && isset($request['op']) && \is_string($request['op'])
                    ? SqliteWorkerOperationEnum::tryFrom($request['op'])
                    : null;
                self::sendFailure($channel, $id, $op ?? SqliteWorkerOperationEnum::Send, 'decode', $error);
                throw $error;
            }

            try {
                $result = self::dispatch($queue, $storage, $operation, $data);
                $channel->send([
                    'id' => $id,
                    'op' => $operation->value,
                    'status' => SqliteWorkerStatusEnum::Ok->value,
                    'result' => $result,
                ]);
                ++$expectedId;
                if (SqliteWorkerOperationEnum::Close === $operation) {
                    return;
                }
            } catch (InvalidReceiptException|\InvalidArgumentException $error) {
                $channel->send([
                    'id' => $id,
                    'op' => $operation->value,
                    'status' => SqliteWorkerStatusEnum::Domain->value,
                    'error' => self::domainError($error),
                ]);
                ++$expectedId;
            } catch (\Throwable $error) {
                self::sendFailure($channel, $id, $operation, 'execute', $error);
                throw $error;
            } finally {
                unset($request, $data, $result, $error);
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
        if (self::boolField($data, 'acknowledge')) {
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
            throw new \RuntimeException('Close request must not carry queue data.');
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

    /**
     * @return array{code: string, message: string}
     */
    private static function domainError(\Throwable $error): array
    {
        if ($error instanceof InvalidReceiptException) {
            $code = ErrorCode::fromReceiptException($error)->value;
        } else {
            $code = ErrorCode::InvalidRequest->value;
        }

        return [
            'code' => $code,
            'message' => SqliteWorkerDiagnostic::text($error->getMessage()),
        ];
    }

    /**
     * @param Channel<mixed, mixed> $channel
     */
    private static function sendFailure(
        Channel $channel,
        int $id,
        SqliteWorkerOperationEnum $operation,
        string $phase,
        \Throwable $error,
    ): void {
        try {
            $payload = [
                'phase' => $phase,
                'message' => SqliteWorkerDiagnostic::text($error->getMessage()),
            ];
            if ($error instanceof \PDOException && \is_string($error->errorInfo[0] ?? null)) {
                $payload['sqlstate'] = SqliteWorkerDiagnostic::text($error->errorInfo[0]);
            }
            $channel->send([
                'id' => $id,
                'op' => $operation->value,
                'status' => SqliteWorkerStatusEnum::Failure->value,
                'error' => $payload,
            ]);
        } catch (\Throwable) {
        }
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
            throw new \RuntimeException(\sprintf('Expected %s operation.', $expected->value));
        }
    }

    private static function assertArray(mixed $value, string $message): void
    {
        if (!\is_array($value)) {
            throw new \RuntimeException($message);
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
            throw new \RuntimeException(\sprintf('Field %s must be an array.', $field));
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
            throw new \RuntimeException(\sprintf('Field %s must be a string.', $field));
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
            throw new \RuntimeException(\sprintf('Field %s must be a positive integer.', $field));
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
            throw new \RuntimeException(\sprintf('Field %s must be a nonnegative integer.', $field));
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
            throw new \RuntimeException(\sprintf('Field %s must be a boolean.', $field));
        }

        return $value;
    }
}
