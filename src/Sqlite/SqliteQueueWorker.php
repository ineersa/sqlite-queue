<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\NullCancellation;
use Amp\Sync\LocalMutex;
use Amp\TimeoutCancellation;
use Amp\TimeoutException;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageCapacityException;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageFailureException;
use Ineersa\SqliteQueue\ValueObject\QueueName;

use function Amp\async;

/**
 * Parent proxy for one persistent PDO worker.
 *
 * Caller cancellation can prevent dispatch. After channel send begins, the terminal
 * reply is always consumed or the lane fails closed.
 */
final class SqliteQueueWorker
{
    private const int EXCHANGE_TIMEOUT_SECONDS = 10;
    private const int MAX_PUBLIC_CONNECTIONS = 64;
    private const int MAX_ADMITTED_OPERATIONS = 128;
    private const int MAX_ADMITTED_PAYLOAD_BYTES = self::MAX_PUBLIC_CONNECTIONS * Limits::MAX_PAYLOAD;
    private const int DIAGNOSTIC_LIMIT_BYTES = 1_024;

    private readonly LocalMutex $exchange;
    private int $nextRequestId = 2;
    private int $admittedOperations = 0;
    private int $admittedPayloadBytes = 0;
    private bool $closing = false;
    private bool $failed = false;
    private bool $closed = false;
    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $capacityAvailable = null;

    /**
     * @param array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string} $configuration
     */
    public function __construct(
        private readonly SqliteWorkerHandle $handle,
        private readonly SqliteSynchronousMode $synchronous,
        private readonly array $configuration,
    ) {
        $this->exchange = new LocalMutex();
    }

    public function handle(): SqliteWorkerHandle
    {
        return $this->handle;
    }

    public function synchronousMode(): SqliteSynchronousMode
    {
        return $this->synchronous;
    }

    /**
     * @return array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string}
     */
    public function configuration(): array
    {
        return $this->configuration;
    }

    public function send(QueueName $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int
    {
        if ($delay < 0) {
            throw new \InvalidArgumentException('Delay must be nonnegative milliseconds.');
        }
        $this->assertRequestPayloadBounds($body, $headers);
        $result = $this->exchange(
            SqliteWorkerOperationEnum::Send,
            [
                'queue' => $queue->value,
                'body' => $body,
                'headers' => $headers,
                'delay' => $delay,
            ],
            \strlen($body) + \strlen($headers),
            $cancellation ?? new NullCancellation(),
        );
        \assert(\is_int($result) && $result > 0);

        return $result;
    }

    public function receive(QueueName $queue, string $ownerId, ?Cancellation $cancellation = null): ?DeliveryDTO
    {
        if ('' === $ownerId) {
            throw new \InvalidArgumentException('Owner identity must be a non-empty string.');
        }
        $delivery = $this->exchange(
            SqliteWorkerOperationEnum::Claim,
            [
                'queue' => $queue->value,
                'owner_id' => $ownerId,
            ],
            0,
            $cancellation ?? new NullCancellation(),
            expectedQueue: $queue->value,
        );
        \assert(null === $delivery || $delivery instanceof DeliveryDTO);

        return $delivery;
    }

    public function acknowledge(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, true, $cancellation);
    }

    public function reject(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, false, $cancellation);
    }

    public function earliestEligibility(QueueName $queue, ?Cancellation $cancellation = null): ?int
    {
        $result = $this->exchange(
            SqliteWorkerOperationEnum::EarliestEligibility,
            ['queue' => $queue->value],
            0,
            $cancellation ?? new NullCancellation(),
        );
        \assert(null === $result || (\is_int($result) && $result >= 0));

        return $result;
    }

    /** Wait until admission capacity is available or the token cancels. */
    public function awaitCapacity(Cancellation $cancellation): void
    {
        if ($this->closed || $this->failed) {
            throw new StorageFailureException('Queue storage is closed.');
        }
        if ($this->hasCapacity()) {
            return;
        }
        $this->capacityAvailable ??= new DeferredFuture();
        $this->capacityAvailable->getFuture()->await($cancellation);
    }

    /** Mark the proxy closing so queued work cannot dispatch later. */
    public function beginClose(): void
    {
        $this->closing = true;
    }

    public function close(Cancellation $budget): void
    {
        if ($this->closed) {
            return;
        }
        $this->beginClose();
        try {
            if (!$this->failed) {
                try {
                    $this->exchange(SqliteWorkerOperationEnum::Close, [], 0, $budget, allowWhileClosing: true);
                } catch (\Throwable) {
                    $this->failed = true;
                }
            }
            if ($this->failed) {
                $this->handle->close($budget);
            } else {
                try {
                    $this->handle->finish($budget);
                } catch (\Throwable) {
                    $this->handle->close($budget);
                }
            }
        } finally {
            $this->closed = true;
            $this->failed = true;
            $this->signalCapacity(new StorageFailureException('Queue storage is closed.'));
        }
    }

    private function settle(string $receipt, string $ownerId, bool $acknowledge, ?Cancellation $cancellation): void
    {
        if ('' === $ownerId) {
            throw new \InvalidArgumentException('Owner identity must be a non-empty string.');
        }
        $result = $this->exchange(
            SqliteWorkerOperationEnum::Settle,
            [
                'receipt' => $receipt,
                'owner_id' => $ownerId,
                'acknowledge' => $acknowledge,
            ],
            0,
            $cancellation ?? new NullCancellation(),
        );
        \assert(true === $result);
    }

    /**
     * @param array<string, mixed> $data
     */
    private function exchange(
        SqliteWorkerOperationEnum $operation,
        array $data,
        int $payloadBytes,
        Cancellation $cancellation,
        bool $allowWhileClosing = false,
        ?string $expectedQueue = null,
    ): mixed {
        $this->assertOpen($allowWhileClosing);
        $this->assertActive($cancellation);
        $this->reserveAdmission($payloadBytes);
        $lock = null;
        $dispatched = false;
        try {
            $acquire = async($this->exchange->acquire(...));
            try {
                $lock = $acquire->await($cancellation);
            } catch (CancelledException $error) {
                // A cancelled waiter must still release any lock acquired after abandonment.
                $acquire->map(static fn ($lock) => $lock->release())->ignore();
                if ($allowWhileClosing) {
                    throw $this->failLane('Shutdown budget expired while waiting for the storage lane.', $error);
                }
                throw new ClientContextClosedException('The client connection lifetime was cancelled.', previous: $error);
            }
            $this->assertOpen($allowWhileClosing);
            $this->assertActive($cancellation);
            if ($this->failed) {
                throw new StorageFailureException('Queue storage lane has failed.');
            }
            $id = $this->nextRequestId++;
            $context = $this->handle->context();
            $deadline = $allowWhileClosing ? $cancellation : new TimeoutCancellation(self::EXCHANGE_TIMEOUT_SECONDS);
            // No yield between the final admission check and marking dispatch attempted.
            $dispatched = true;
            $exchange = async(static function () use ($context, $id, $operation, $data): mixed {
                $context->send([
                    'id' => $id,
                    'op' => $operation->value,
                    'data' => $data,
                ]);

                return $context->receive();
            });
            try {
                $response = $exchange->await($deadline);
            } catch (CancelledException $error) {
                try {
                    $context->close();
                } catch (\Throwable) {
                }
                $exchange->ignore();
                $reason = $error->getPrevious() instanceof TimeoutException
                    ? 'Worker exchange deadline expired.'
                    : 'Worker exchange cancelled during shutdown.';
                throw $this->failLane($reason, $error);
            } catch (\Throwable $error) {
                throw $this->failLane('Worker exchange failed.', $error);
            }

            return $this->validatedResult($operation, $id, $response, $expectedQueue);
        } catch (ClientContextClosedException|StorageCapacityException|\InvalidArgumentException|InvalidReceiptException|StorageFailureException $error) {
            throw $error;
        } catch (\Throwable $error) {
            if ($dispatched) {
                throw $this->failLane('Worker exchange failed.', $error);
            }
            throw $error;
        } finally {
            $lock?->release();
            $this->releaseAdmission($payloadBytes);
        }
    }

    private function validatedResult(
        SqliteWorkerOperationEnum $operation,
        int $id,
        mixed $response,
        ?string $expectedQueue,
    ): mixed {
        if (!\is_array($response)) {
            throw $this->failLane('Worker response must be an array.');
        }
        if (($response['id'] ?? null) !== $id) {
            throw $this->failLane('Worker response id does not match the request.');
        }
        if (($response['op'] ?? null) !== $operation->value) {
            throw $this->failLane('Worker response operation does not match the request.');
        }
        $statusValue = $response['status'] ?? null;
        if (!\is_string($statusValue)) {
            throw $this->failLane('Worker response status must be a string.');
        }
        $status = SqliteWorkerStatusEnum::tryFrom($statusValue);
        if (null === $status) {
            throw $this->failLane('Worker response status is invalid.');
        }
        if (SqliteWorkerStatusEnum::Domain === $status) {
            throw $this->domainException($response['error'] ?? null);
        }
        if (SqliteWorkerStatusEnum::Failure === $status) {
            throw $this->failLane($this->failureMessage($response['error'] ?? null));
        }
        if (!\array_key_exists('result', $response)) {
            throw $this->failLane('Worker success response is missing its result.');
        }
        $result = $response['result'];

        return match ($operation) {
            SqliteWorkerOperationEnum::Send => $this->requirePositiveInt($result, 'Send'),
            SqliteWorkerOperationEnum::Claim => null === $result
                ? null
                : $this->deliveryFromResult($this->requireClaimArray($result), $expectedQueue ?? throw $this->failLane('Claim validation requires the requested queue.')),
            SqliteWorkerOperationEnum::Settle, SqliteWorkerOperationEnum::Close => true === $result
                ? true
                : throw $this->failLane($operation->value.' response must be explicit success.'),
            SqliteWorkerOperationEnum::EarliestEligibility => $this->requireEligibility($result),
            SqliteWorkerOperationEnum::Init => throw $this->failLane('Init is not a steady-state exchange.'),
        };
    }

    private function requirePositiveInt(mixed $result, string $label): int
    {
        if (!\is_int($result) || $result <= 0) {
            throw $this->failLane($label.' response did not return a positive message identity.');
        }

        return $result;
    }

    /**
     * @return array<array-key, mixed>
     */
    private function requireClaimArray(mixed $result): array
    {
        if (!\is_array($result)) {
            throw $this->failLane('Claim response must be an array or null.');
        }

        return $result;
    }

    private function requireEligibility(mixed $result): mixed
    {
        if (null === $result) {
            return null;
        }
        if (!\is_int($result) || $result < 0) {
            throw $this->failLane('Earliest eligibility response must be a nonnegative integer or null.');
        }

        return $result;
    }

    private function reserveAdmission(int $payloadBytes): void
    {
        if ($payloadBytes < 0) {
            throw new \InvalidArgumentException('Payload byte count must be nonnegative.');
        }
        if ($this->admittedOperations >= self::MAX_ADMITTED_OPERATIONS
            || $this->admittedPayloadBytes + $payloadBytes > self::MAX_ADMITTED_PAYLOAD_BYTES
        ) {
            throw new StorageCapacityException('Queue storage admission capacity is exhausted.');
        }
        ++$this->admittedOperations;
        $this->admittedPayloadBytes += $payloadBytes;
    }

    private function releaseAdmission(int $payloadBytes): void
    {
        if ($this->admittedOperations < 1 || $this->admittedPayloadBytes < $payloadBytes) {
            throw $this->failLane('Admission counters are inconsistent.');
        }
        --$this->admittedOperations;
        $this->admittedPayloadBytes -= $payloadBytes;
        if ($this->hasCapacity()) {
            $this->signalCapacity(null);
        }
    }

    private function hasCapacity(): bool
    {
        return $this->admittedOperations < self::MAX_ADMITTED_OPERATIONS
            && $this->admittedPayloadBytes < self::MAX_ADMITTED_PAYLOAD_BYTES;
    }

    private function signalCapacity(?\Throwable $error): void
    {
        $waiter = $this->capacityAvailable;
        $this->capacityAvailable = null;
        if (null === $waiter || $waiter->isComplete()) {
            return;
        }
        if (null === $error) {
            $waiter->complete(null);
        } else {
            $waiter->error($error);
        }
    }

    private function assertOpen(bool $allowWhileClosing): void
    {
        if ($this->closed || $this->failed) {
            throw new StorageFailureException('Queue storage is closed.');
        }
        if ($this->closing && !$allowWhileClosing) {
            throw new StorageFailureException('Queue storage is closing.');
        }
    }

    private function assertActive(Cancellation $cancellation): void
    {
        try {
            $cancellation->throwIfRequested();
        } catch (CancelledException $error) {
            throw new ClientContextClosedException('The client connection lifetime was cancelled.', previous: $error);
        }
    }

    private function assertRequestPayloadBounds(string $body, string $headers): void
    {
        if (!$this->payloadWithinBounds($body, $headers)) {
            throw new \InvalidArgumentException('Payload exceeds the supported frame budget.');
        }
    }

    private function payloadWithinBounds(string $body, string $headers): bool
    {
        $bodyLength = \strlen($body);
        $headersLength = \strlen($headers);

        return $bodyLength <= Limits::MAX_PAYLOAD
            && $headersLength <= Limits::MAX_PAYLOAD
            && $bodyLength + $headersLength <= Limits::MAX_PAYLOAD;
    }

    /**
     * @param array<array-key, mixed> $result
     */
    private function deliveryFromResult(array $result, string $expectedQueue): DeliveryDTO
    {
        foreach (['id', 'queue', 'body', 'headers', 'receipt', 'available_at', 'reserved_until'] as $field) {
            if (!\array_key_exists($field, $result)) {
                throw $this->failLane('Claim response is missing required delivery fields.');
            }
        }
        if (!\is_int($result['id']) || $result['id'] <= 0) {
            throw $this->failLane('Claim response identity must be a positive integer.');
        }
        if (!\is_string($result['queue']) || $result['queue'] !== $expectedQueue) {
            throw $this->failLane('Claim response queue does not match the request.');
        }
        if (!\is_string($result['body']) || !\is_string($result['headers']) || !\is_string($result['receipt'])) {
            throw $this->failLane('Claim response payloads and receipt must be strings.');
        }
        if (!$this->payloadWithinBounds($result['body'], $result['headers'])) {
            throw $this->failLane('Claim response payload exceeds the supported frame budget.');
        }
        if (!\is_int($result['available_at']) || $result['available_at'] < 0
            || !\is_int($result['reserved_until']) || $result['reserved_until'] < 0
        ) {
            throw $this->failLane('Claim response deadlines must be nonnegative integers.');
        }
        if (1 !== preg_match('/\A([1-9][0-9]*):([a-f0-9]{64})\z/D', $result['receipt'], $parts)) {
            throw $this->failLane('Claim response receipt is malformed.');
        }
        $receiptId = filter_var($parts[1], \FILTER_VALIDATE_INT);
        if (false === $receiptId || $receiptId !== $result['id']) {
            throw $this->failLane('Claim response receipt identity does not match the delivery identity.');
        }

        return new DeliveryDTO(
            $result['id'],
            $result['queue'],
            $result['body'],
            $result['headers'],
            $result['receipt'],
            $result['available_at'],
            $result['reserved_until'],
        );
    }

    private function domainException(mixed $error): \Throwable
    {
        if (!\is_array($error) || !\is_string($error['code'] ?? null)) {
            throw $this->failLane('Worker domain failure is malformed.');
        }
        $code = ErrorCode::tryFrom($error['code']);
        $exception = $code?->receiptException();
        if (null !== $exception) {
            return $exception;
        }
        if (ErrorCode::InvalidQueueName === $code) {
            return new \InvalidArgumentException('Broker rejected the queue name.');
        }
        if ('invalid_argument' === $error['code']) {
            $message = \is_string($error['message'] ?? null) ? $this->diagnostic($error['message']) : '';

            return new \InvalidArgumentException('' !== $message ? $message : 'Invalid queue argument.');
        }

        throw $this->failLane('Worker reported an unsupported domain failure.');
    }

    private function failureMessage(mixed $error): string
    {
        if (!\is_array($error)) {
            return 'Worker reported a storage failure.';
        }
        $parts = [];
        if (\is_string($error['phase'] ?? null) && '' !== $error['phase']) {
            $parts[] = 'phase='.$this->diagnostic($error['phase']);
        }
        if (\is_string($error['sqlstate'] ?? null) && '' !== $error['sqlstate']) {
            $parts[] = 'sqlstate='.$this->diagnostic($error['sqlstate']);
        }
        if (\is_string($error['message'] ?? null) && '' !== $error['message']) {
            $parts[] = $this->diagnostic($error['message']);
        }

        return [] === $parts ? 'Worker reported a storage failure.' : implode('; ', $parts);
    }

    private function failLane(string $message, ?\Throwable $previous = null): StorageFailureException
    {
        $this->failed = true;
        $this->closing = true;
        try {
            $this->handle->forceStop();
        } catch (\Throwable) {
        }
        $this->signalCapacity(new StorageFailureException('Queue storage lane has failed.'));

        return new StorageFailureException($this->diagnostic($message), previous: $previous);
    }

    private function diagnostic(string $message): string
    {
        if (!mb_check_encoding($message, 'UTF-8')) {
            return 'non-utf8 diagnostic';
        }
        if (\strlen($message) <= self::DIAGNOSTIC_LIMIT_BYTES) {
            return $message;
        }

        return mb_strcut($message, 0, self::DIAGNOSTIC_LIMIT_BYTES, 'UTF-8');
    }
}
