<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageCapacityException;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageFailureException;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Revolt\EventLoop;

use function Amp\async;

/**
 * Parent proxy for one persistent PDO worker.
 *
 * Admits requests into a bounded FIFO. One writer sends without waiting for replies.
 * One reader validates ordered replies. Caller cancellation can remove queued work;
 * after send begins, the terminal reply is consumed or the lane fails closed.
 */
final class SqliteQueueWorker
{
    private const int EXCHANGE_TIMEOUT_SECONDS = 10;
    private const int MAX_ELIGIBILITY_PROBES = Limits::MAX_CONNECTIONS;
    private const int MAX_ADMITTED_OPERATIONS = Limits::MAX_CONNECTIONS + self::MAX_ELIGIBILITY_PROBES;
    private const int MAX_ADMITTED_PAYLOAD_BYTES = Limits::MAX_CONNECTIONS * Limits::MAX_PAYLOAD;
    private const int MAX_IN_FLIGHT_OPERATIONS = 4;
    private const int MAX_IN_FLIGHT_PAYLOAD_BYTES = 2 * Limits::MAX_PAYLOAD;

    private SqliteWorkerLifecycleEnum $lifecycle = SqliteWorkerLifecycleEnum::Open;
    private int $nextRequestId = 2;
    private int $nextExpectedResponseId = 2;
    private int $admittedOperations = 0;
    private int $admittedPayloadBytes = 0;
    private int $inFlightOperations = 0;
    private int $inFlightPayloadBytes = 0;
    private bool $writerRunning = false;
    private bool $closeRequested = false;
    private bool $closeSent = false;
    private bool $readerStopped = false;
    /** @var list<SqliteWorkerRequestEntry> */
    private array $queue = [];
    /** @var array<int, SqliteWorkerRequestEntry> */
    private array $dispatched = [];
    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $capacityAvailable = null;
    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $writerSignal = null;
    /** @var Future<void>|null */
    private ?Future $reader = null;
    /** @var Future<void>|null */
    private ?Future $shutdown = null;
    private ?StorageFailureException $failure = null;
    private ?SqliteWorkerRequestEntry $pendingClose = null;

    /**
     * @param array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string} $configuration
     */
    public function __construct(
        private readonly SqliteWorkerHandle $handle,
        private readonly SqliteSynchronousMode $synchronous,
        private readonly array $configuration,
    ) {
        $this->reader = async($this->readLoop(...));
        $this->reader->ignore();
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
            Limits::MAX_PAYLOAD,
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

    /** Wait until a zero-payload eligibility probe can be admitted or the token cancels. */
    public function awaitCapacity(Cancellation $cancellation): void
    {
        if (SqliteWorkerLifecycleEnum::Open !== $this->lifecycle) {
            throw new StorageFailureException('Queue storage is closed.');
        }
        if ($this->hasProbeCapacity()) {
            return;
        }
        $this->capacityAvailable ??= new DeferredFuture();
        $this->capacityAvailable->getFuture()->await($cancellation);
    }

    /** Mark the proxy closing so queued work cannot dispatch later. */
    public function beginClose(): void
    {
        if (SqliteWorkerLifecycleEnum::Open !== $this->lifecycle) {
            return;
        }
        $this->lifecycle = SqliteWorkerLifecycleEnum::Closing;
        $this->closeRequested = true;
        $this->failQueued(new StorageFailureException('Queue storage is closing.'));
        $this->wakeWriter();
    }

    public function close(Cancellation $budget): void
    {
        $this->beginClose();
        $this->shutdown ??= async(function () use ($budget): void {
            $this->shutdownWorker($budget);
        });
        $this->shutdown->ignore();
        $this->shutdown->await($budget);
    }

    private function shutdownWorker(Cancellation $budget): void
    {
        try {
            if (null === $this->failure) {
                try {
                    $this->exchange(
                        SqliteWorkerOperationEnum::Close,
                        [],
                        0,
                        $budget,
                        lifecycle: true,
                    );
                } catch (\Throwable) {
                    $this->markFailed(new StorageFailureException('Worker close exchange failed.'));
                }
            }
            if (null !== $this->failure) {
                $this->handle->close($budget);
            } else {
                try {
                    $this->handle->finish($budget);
                } catch (\Throwable) {
                    $this->handle->close($budget);
                }
            }
        } finally {
            $this->lifecycle = SqliteWorkerLifecycleEnum::Closed;
            $this->markFailed($this->failure ?? new StorageFailureException('Queue storage is closed.'));
            $this->signalCapacity(new StorageFailureException('Queue storage is closed.'));
            $this->wakeWriter();
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
        bool $lifecycle = false,
        ?string $expectedQueue = null,
    ): mixed {
        $this->assertOpen($lifecycle);
        $this->assertActive($cancellation);
        if (!$lifecycle) {
            $this->reserveAdmission($payloadBytes);
        }
        $entry = new SqliteWorkerRequestEntry($operation, $data, $payloadBytes, $lifecycle, $expectedQueue);
        $admitted = !$lifecycle;
        try {
            if ($lifecycle) {
                if (null !== $this->pendingClose) {
                    return $this->pendingClose->completion->getFuture()->await();
                }
                $this->pendingClose = $entry;
                $this->closeRequested = true;
            } else {
                $this->queue[] = $entry;
            }
            $entry->cancellation = $cancellation;
            $entry->cancellationId = $cancellation->subscribe(function () use ($entry): void {
                $this->cancelQueued($entry);
            });
            $this->wakeWriter();

            return $entry->completion->getFuture()->await();
        } catch (ClientContextClosedException|StorageCapacityException|\InvalidArgumentException|InvalidReceiptException|StorageFailureException $error) {
            throw $error;
        } catch (\Throwable $error) {
            if ($entry->dispatched) {
                throw $this->failLane('Worker exchange failed.', $error);
            }
            throw $error;
        } finally {
            $this->unsubscribeCancellation($entry);
            if ($admitted && !$entry->removed) {
                $this->releaseAdmission($payloadBytes);
            }
            $entry->data = null;
        }
    }

    private function writeLoop(): void
    {
        if ($this->writerRunning) {
            return;
        }
        $this->writerRunning = true;
        try {
            while (true) {
                if (SqliteWorkerLifecycleEnum::Failed === $this->lifecycle || SqliteWorkerLifecycleEnum::Closed === $this->lifecycle) {
                    return;
                }
                $entry = $this->nextWritableEntry();
                if (null === $entry) {
                    if ($this->shouldSendClose()) {
                        $entry = $this->pendingClose;
                        if (null === $entry || $entry->dispatched || $entry->removed) {
                            return;
                        }
                        $this->closeSent = true;
                        $this->dispatchEntry($entry);
                        continue;
                    }
                    $this->writerSignal ??= new DeferredFuture();
                    $signal = $this->writerSignal->getFuture();
                    $this->writerRunning = false;
                    try {
                        $signal->await();
                    } finally {
                        $this->writerRunning = true;
                    }
                    continue;
                }
                $this->dispatchEntry($entry);
            }
        } catch (\Throwable $error) {
            $this->failLane('Worker writer failed.', $error);
        } finally {
            $this->writerRunning = false;
        }
    }

    private function shouldSendClose(): bool
    {
        return $this->closeRequested
            && !$this->closeSent
            && null !== $this->pendingClose
            && !$this->pendingClose->dispatched
            && !$this->pendingClose->removed
            && [] === $this->dispatched
            && !$this->hasQueuedNormalWork()
            && null === $this->failure;
    }

    private function nextWritableEntry(): ?SqliteWorkerRequestEntry
    {
        while ([] !== $this->queue) {
            $entry = $this->queue[0];
            if ($entry->removed) {
                array_shift($this->queue);
                continue;
            }
            if (!$this->hasInFlightCapacity($entry->payloadBytes)) {
                return null;
            }
            array_shift($this->queue);

            return $entry;
        }

        return null;
    }

    private function dispatchEntry(SqliteWorkerRequestEntry $entry): void
    {
        if ($entry->removed || $entry->dispatched) {
            return;
        }
        if (null !== $this->failure) {
            $this->completeEntry($entry, $this->failure);

            return;
        }
        if (!$entry->lifecycle && SqliteWorkerLifecycleEnum::Open !== $this->lifecycle) {
            $this->completeEntry($entry, new StorageFailureException('Queue storage is closing.'));

            return;
        }
        if (null !== $entry->cancellation) {
            try {
                $entry->cancellation->throwIfRequested();
            } catch (CancelledException $error) {
                $this->completeEntry(
                    $entry,
                    new ClientContextClosedException('The client connection lifetime was cancelled.', previous: $error),
                );
                if (!$entry->lifecycle && !$entry->removed) {
                    $this->releaseAdmission($entry->payloadBytes);
                    $entry->removed = true;
                }

                return;
            }
        }

        $id = $this->nextRequestId++;
        $entry->id = $id;
        $entry->dispatched = true;
        $this->unsubscribeCancellation($entry);
        if (!$entry->lifecycle) {
            $this->reserveInFlight($entry->payloadBytes);
            $entry->inFlightCharged = true;
            $entry->timeoutId = EventLoop::delay(self::EXCHANGE_TIMEOUT_SECONDS, function () use ($entry): void {
                if (!$entry->responseComplete || !$entry->writeComplete) {
                    $this->failLane('Worker exchange deadline expired.');
                }
            });
        }
        $this->dispatched[$id] = $entry;
        $payload = [
            'id' => $id,
            'op' => $entry->operation->value,
            'data' => $entry->data ?? [],
        ];
        $entry->data = null;
        try {
            $this->handle->context()->send($payload);
        } catch (\Throwable $error) {
            $entry->writeComplete = true;
            $this->releaseInFlightFor($entry);
            $this->clearTimeout($entry);
            unset($this->dispatched[$id]);
            throw $this->failLane('Worker exchange failed.', $error);
        }
        $entry->writeComplete = true;
        if ($entry->responseComplete) {
            $this->releaseInFlightFor($entry);
            $this->clearTimeout($entry);
            unset($this->dispatched[$id]);
        }
        $this->wakeWriter();
    }

    private function readLoop(): void
    {
        try {
            while (!$this->readerStopped) {
                try {
                    $response = $this->handle->context()->receive();
                } catch (\Throwable $error) {
                    if (null !== $this->failure || SqliteWorkerLifecycleEnum::Closed === $this->lifecycle) {
                        return;
                    }
                    throw $this->failLane('Worker exchange failed.', $error);
                }
                $this->handleResponse($response);
            }
        } catch (StorageFailureException) {
            // Lane already failed closed.
        } catch (\Throwable $error) {
            $this->failLane('Worker reader failed.', $error);
        }
    }

    private function handleResponse(mixed $response): void
    {
        if (!\is_array($response)) {
            throw $this->failLane('Worker response must be an array.');
        }
        $id = $response['id'] ?? null;
        if ($id !== $this->nextExpectedResponseId) {
            throw $this->failLane('Worker response id does not match the next expected response.');
        }
        $entry = $this->dispatched[$id] ?? null;
        if (null === $entry) {
            throw $this->failLane('Worker response does not match a dispatched request.');
        }
        if (($response['op'] ?? null) !== $entry->operation->value) {
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
        ++$this->nextExpectedResponseId;
        if (SqliteWorkerStatusEnum::Domain === $status) {
            $error = $this->domainException($response['error'] ?? null);
            $entry->responseComplete = true;
            $this->completeEntry($entry, $error);
            $this->finishDispatched($entry);

            return;
        }
        if (SqliteWorkerStatusEnum::Failure === $status) {
            throw $this->failLane(SqliteWorkerDiagnostic::failure($response['error'] ?? null));
        }
        if (!\array_key_exists('result', $response)) {
            throw $this->failLane('Worker success response is missing its result.');
        }
        $result = $this->validatedResult($entry->operation, $response['result'], $entry->expectedQueue);
        $entry->responseComplete = true;
        $this->completeEntry($entry, $result);
        $this->finishDispatched($entry);
        if (SqliteWorkerOperationEnum::Close === $entry->operation) {
            $this->readerStopped = true;

            return;
        }
        $this->wakeWriter();
    }

    private function finishDispatched(SqliteWorkerRequestEntry $entry): void
    {
        $id = $entry->id;
        if (null === $id) {
            return;
        }
        if ($entry->writeComplete) {
            $this->releaseInFlightFor($entry);
            $this->clearTimeout($entry);
            unset($this->dispatched[$id]);
        }
    }

    private function releaseInFlightFor(SqliteWorkerRequestEntry $entry): void
    {
        if (!$entry->inFlightCharged) {
            return;
        }
        $entry->inFlightCharged = false;
        $this->releaseInFlight($entry->payloadBytes);
    }

    private function clearTimeout(SqliteWorkerRequestEntry $entry): void
    {
        if (null === $entry->timeoutId) {
            return;
        }
        EventLoop::cancel($entry->timeoutId);
        $entry->timeoutId = null;
    }

    private function cancelQueued(SqliteWorkerRequestEntry $entry): void
    {
        if ($entry->dispatched || $entry->removed || $entry->completion->isComplete()) {
            return;
        }
        foreach ($this->queue as $index => $candidate) {
            if ($candidate === $entry) {
                array_splice($this->queue, $index, 1);
                break;
            }
        }
        $entry->removed = true;
        if (!$entry->lifecycle) {
            $this->releaseAdmission($entry->payloadBytes);
        }
        $this->completeEntry($entry, new ClientContextClosedException('The client connection lifetime was cancelled.'));
        $this->wakeWriter();
    }

    private function failQueued(\Throwable $error): void
    {
        $queued = $this->queue;
        $this->queue = [];
        foreach ($queued as $entry) {
            if ($entry->removed || $entry->completion->isComplete()) {
                continue;
            }
            $entry->removed = true;
            $this->unsubscribeCancellation($entry);
            if (!$entry->lifecycle) {
                $this->releaseAdmission($entry->payloadBytes);
            }
            $this->completeEntry($entry, $error);
        }
    }

    private function completeEntry(SqliteWorkerRequestEntry $entry, mixed $result): void
    {
        if ($entry->completion->isComplete()) {
            return;
        }
        if ($result instanceof \Throwable) {
            $entry->completion->error($result);
        } else {
            $entry->completion->complete($result);
        }
    }

    private function unsubscribeCancellation(SqliteWorkerRequestEntry $entry): void
    {
        if (null === $entry->cancellationId || null === $entry->cancellation) {
            $entry->cancellationId = null;
            $entry->cancellation = null;

            return;
        }
        $entry->cancellation->unsubscribe($entry->cancellationId);
        $entry->cancellationId = null;
        $entry->cancellation = null;
    }

    private function wakeWriter(): void
    {
        $signal = $this->writerSignal;
        $this->writerSignal = null;
        if (null !== $signal && !$signal->isComplete()) {
            $signal->complete(null);
        }
        if (!$this->writerRunning
            && SqliteWorkerLifecycleEnum::Closed !== $this->lifecycle
            && SqliteWorkerLifecycleEnum::Failed !== $this->lifecycle
        ) {
            async($this->writeLoop(...))->ignore();
        }
    }

    private function hasQueuedNormalWork(): bool
    {
        foreach ($this->queue as $entry) {
            if (!$entry->removed) {
                return true;
            }
        }

        return false;
    }

    private function hasInFlightCapacity(int $payloadBytes): bool
    {
        return $this->inFlightOperations < self::MAX_IN_FLIGHT_OPERATIONS
            && $this->inFlightPayloadBytes + $payloadBytes <= self::MAX_IN_FLIGHT_PAYLOAD_BYTES;
    }

    private function reserveInFlight(int $payloadBytes): void
    {
        ++$this->inFlightOperations;
        $this->inFlightPayloadBytes += $payloadBytes;
    }

    private function releaseInFlight(int $payloadBytes): void
    {
        if ($this->inFlightOperations < 1 || $this->inFlightPayloadBytes < $payloadBytes) {
            throw $this->failLane('In-flight counters are inconsistent.');
        }
        --$this->inFlightOperations;
        $this->inFlightPayloadBytes -= $payloadBytes;
        $this->wakeWriter();
    }

    private function validatedResult(SqliteWorkerOperationEnum $operation, mixed $result, ?string $expectedQueue): mixed
    {
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
        if ($this->hasProbeCapacity()) {
            $this->signalCapacity(null);
        }
    }

    private function hasProbeCapacity(): bool
    {
        return $this->admittedOperations < self::MAX_ADMITTED_OPERATIONS;
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

    private function assertOpen(bool $lifecycle): void
    {
        if (SqliteWorkerLifecycleEnum::Closed === $this->lifecycle || SqliteWorkerLifecycleEnum::Failed === $this->lifecycle) {
            throw new StorageFailureException('Queue storage is closed.');
        }
        if (SqliteWorkerLifecycleEnum::Closing === $this->lifecycle && !$lifecycle) {
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
        if (ErrorCode::InvalidRequest === $code) {
            $message = \is_string($error['message'] ?? null) ? SqliteWorkerDiagnostic::text($error['message']) : '';

            return new \InvalidArgumentException('' !== $message ? $message : 'Invalid queue argument.');
        }

        throw $this->failLane('Worker reported an unsupported domain failure.');
    }

    private function markFailed(StorageFailureException $error): void
    {
        $this->failure ??= $error;
        if (SqliteWorkerLifecycleEnum::Closed !== $this->lifecycle) {
            $this->lifecycle = SqliteWorkerLifecycleEnum::Failed;
        }
        $this->closeRequested = true;
        $this->failQueued($this->failure);
        if (null !== $this->pendingClose && !$this->pendingClose->completion->isComplete()) {
            $this->completeEntry($this->pendingClose, $this->failure);
        }
        foreach ($this->dispatched as $entry) {
            if (!$entry->responseComplete) {
                $entry->responseComplete = true;
                $this->completeEntry($entry, $this->failure);
            }
            if ($entry->writeComplete) {
                $this->releaseInFlightFor($entry);
                $this->clearTimeout($entry);
            }
        }
        $this->dispatched = [];
        $this->signalCapacity(new StorageFailureException('Queue storage lane has failed.'));
        $this->wakeWriter();
    }

    private function failLane(string $message, ?\Throwable $previous = null): StorageFailureException
    {
        $error = new StorageFailureException(SqliteWorkerDiagnostic::text($message), previous: $previous);
        if (null === $this->failure) {
            $this->markFailed($error);
            try {
                $this->handle->forceStop();
            } catch (\Throwable) {
            }
            $this->readerStopped = true;
        }

        return $this->failure ?? $error;
    }
}
