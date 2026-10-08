<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\NullCancellation;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\ExpiredReceiptException;
use Ineersa\SqliteQueue\Exception\MalformedReceiptException;
use Ineersa\SqliteQueue\Exception\NoActiveReservationException;
use Ineersa\SqliteQueue\Exception\ReceiptEpochMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptOwnerMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptTokenMismatchException;
use Ineersa\SqliteQueue\Sqlite\SettlementResultEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\ValueObject\QueueName;

/**
 * Queue message policy over SQLite persistence.
 *
 * Deadlines are Unix wall-clock milliseconds. Storage owns the connection and SQL.
 * Callers own client lifetime and may cancel before a storage operation starts.
 * Once storage begins, the operation finishes normally. This class does not open,
 * configure, or close storage.
 */
final class Queue
{
    public const int DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS = 60_000;

    private const int BROKER_EPOCH_BYTES = 32;
    private const int RESERVATION_TOKEN_BYTES = 32;

    private readonly string $epoch;
    /** @var \Closure(): int */
    private readonly \Closure $clock;

    /**
     * @param \Closure(): int|null $clock wall-clock milliseconds, sampled after transaction acquisition
     */
    public function __construct(
        private readonly SqliteQueueStorage $storage,
        private readonly int $visibilityTimeout = self::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS,
        ?\Closure $clock = null,
    ) {
        if ($visibilityTimeout <= 0) {
            throw new \InvalidArgumentException('Visibility timeout must be positive milliseconds.');
        }
        $this->clock = $clock ?? static fn (): int => (int) floor(microtime(true) * 1000);
        $this->epoch = bin2hex(random_bytes(self::BROKER_EPOCH_BYTES));
    }

    private function __clone(): void
    {
    }

    public function send(QueueName $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int
    {
        if ($delay < 0) {
            throw new \InvalidArgumentException('Delay must be nonnegative milliseconds.');
        }
        $this->assertActive($cancellation ?? new NullCancellation());

        return $this->storage->insert(
            $queue->value,
            $body,
            $headers,
            fn (): int => $this->deadline($delay),
        );
    }

    /** Immediately claim one eligible message, or return null. Never waits for future work. */
    public function receive(QueueName $queue, string $ownerId, ?Cancellation $cancellation = null): ?DeliveryDTO
    {
        $this->assertActive($cancellation ?? new NullCancellation());
        $token = bin2hex(random_bytes(self::RESERVATION_TOKEN_BYTES));
        $claimed = $this->storage->claim(
            $queue->value,
            $ownerId,
            $this->epoch,
            $token,
            $this->now(...),
            fn (int $now): int => $this->deadline($this->visibilityTimeout, $now),
        );
        if (null === $claimed) {
            return null;
        }

        return new DeliveryDTO(
            $claimed['id'],
            $queue->value,
            $claimed['body'],
            $claimed['headers'],
            $this->receipt($claimed['id'], $token),
            $claimed['available_at'],
            $claimed['reserved_until'],
        );
    }

    public function acknowledge(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, $cancellation);
    }

    public function reject(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, $cancellation);
    }

    /**
     * Earliest effective eligibility deadline for one queue, or null when the queue has no rows.
     *
     * Effective eligibility accounts for both availability and visibility expiry. This is a
     * scheduling hint for wait coordination, not a claim.
     */
    public function earliestEligibility(QueueName $queue, ?Cancellation $cancellation = null): ?int
    {
        $this->assertActive($cancellation ?? new NullCancellation());

        return $this->storage->earliestEligibility($queue->value);
    }

    private function settle(string $receipt, string $ownerId, ?Cancellation $cancellation): void
    {
        if (1 !== preg_match('/\A([1-9][0-9]*):([a-f0-9]{64})\z/D', $receipt, $parts)) {
            throw new MalformedReceiptException();
        }
        $id = filter_var($parts[1], \FILTER_VALIDATE_INT);
        if (false === $id) {
            throw new MalformedReceiptException();
        }
        $this->assertActive($cancellation ?? new NullCancellation());
        $settled = $this->storage->settle(
            $id,
            $parts[2],
            $ownerId,
            $this->epoch,
            $this->now(...),
        );
        match ($settled) {
            SettlementResultEnum::Settled => null,
            SettlementResultEnum::NoActiveReservation => throw new NoActiveReservationException(),
            SettlementResultEnum::OwnerMismatch => throw new ReceiptOwnerMismatchException(),
            SettlementResultEnum::EpochMismatch => throw new ReceiptEpochMismatchException(),
            SettlementResultEnum::TokenMismatch => throw new ReceiptTokenMismatchException(),
            SettlementResultEnum::Expired => throw new ExpiredReceiptException(),
        };
    }

    private function receipt(int $id, string $token): string
    {
        return $id.':'.$token;
    }

    private function assertActive(Cancellation $cancellation): void
    {
        try {
            $cancellation->throwIfRequested();
        } catch (CancelledException $error) {
            throw new ClientContextClosedException('The client connection lifetime was cancelled.', previous: $error);
        }
    }

    private function now(): int
    {
        $now = ($this->clock)();
        if ($now < 0) {
            throw new \InvalidArgumentException('Wall-clock milliseconds must be nonnegative.');
        }

        return $now;
    }

    private function deadline(int $milliseconds, ?int $now = null): int
    {
        $now ??= $this->now();
        if ($milliseconds > \PHP_INT_MAX - $now) {
            throw new \InvalidArgumentException('Deadline exceeds the supported timestamp range.');
        }

        return $now + $milliseconds;
    }
}
