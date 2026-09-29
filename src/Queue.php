<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\NullCancellation;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\ValueObject\QueueName;

/**
 * Queue message policy over SQLite persistence.
 *
 * Deadlines are Unix wall-clock milliseconds. Storage owns the connection and SQL.
 * Callers own client lifetime and pass an Amp cancellation for in-flight disconnects.
 * This class does not open, configure, or close storage.
 */
final class Queue
{
    private const int BROKER_EPOCH_BYTES = 32;
    private const int RESERVATION_TOKEN_BYTES = 32;

    private readonly string $epoch;
    /** @var \Closure(): int */
    private readonly \Closure $clock;

    /**
     * @param \Closure(): int|null $clock wall-clock milliseconds, sampled inside storage ownership
     */
    public function __construct(
        private readonly SqliteQueueStorage $storage,
        private readonly int $visibilityTimeout = 5000,
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
        $cancellation ??= new NullCancellation();

        return $this->storage->exclusive(function () use ($queue, $body, $headers, $delay, $cancellation): int {
            $this->assertActive($cancellation);
            $availableAt = $this->deadline($delay);

            return $this->storage->insert($queue->value, $body, $headers, $availableAt);
        });
    }

    /** Immediately claim one eligible message, or return null. Never waits for future work. */
    public function receive(QueueName $queue, string $ownerId, ?Cancellation $cancellation = null): ?DeliveryDTO
    {
        $cancellation ??= new NullCancellation();

        return $this->storage->exclusive(function () use ($queue, $ownerId, $cancellation): ?DeliveryDTO {
            $this->assertActive($cancellation);
            $now = $this->now();
            $expires = $this->deadline($this->visibilityTimeout, $now);
            $token = bin2hex(random_bytes(self::RESERVATION_TOKEN_BYTES));
            $claimed = $this->storage->claim($queue->value, $ownerId, $this->epoch, $token, $now, $expires);
            // A disconnect may have occurred while waiting for persistence.
            $this->assertActive($cancellation);
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
        });
    }

    public function acknowledge(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, $cancellation);
    }

    public function reject(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void
    {
        $this->settle($receipt, $ownerId, $cancellation);
    }

    private function settle(string $receipt, string $ownerId, ?Cancellation $cancellation): void
    {
        if (1 !== preg_match('/\A([1-9][0-9]*):([a-f0-9]{64})\z/D', $receipt, $parts)) {
            throw new InvalidReceiptException('Invalid delivery receipt.');
        }
        $cancellation ??= new NullCancellation();
        $this->storage->exclusive(function () use ($parts, $ownerId, $cancellation): void {
            $this->assertActive($cancellation);
            $settled = $this->storage->settle(
                (int) $parts[1],
                $parts[2],
                $ownerId,
                $this->epoch,
                $this->now(),
                function () use ($cancellation): void {
                    $this->assertActive($cancellation);
                },
            );
            if (!$settled) {
                throw new InvalidReceiptException('Expired, unknown, or foreign delivery receipt.');
            }
        });
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
            throw new InvalidReceiptException('Unknown or disconnected client context.', previous: $error);
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
