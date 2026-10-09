<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Revolt\EventLoop;

/**
 * Coordinates bounded WAIT hints from persisted readiness deadlines.
 *
 * A true result is only a hint to receive, never a claim. Waiters are registered before the
 * readiness recheck so a committed send between empty receive and WAIT cannot be lost. Derived
 * timer state exists only for watched queues and is rebuilt lazily from storage.
 *
 * WAIT_ANY reuses the same per-queue watches and timers. One waiter occupies every selected queue
 * until settlement, then detaches from all of them. Claims and selection priority stay outside this
 * coordinator.
 *
 * Positive wait bounds use an event-loop delay only. The controlled wall clock schedules delayed
 * readiness, but it must not expire a positive WAIT early when it jumps forward, and a backward
 * jump must not leave a zero-duration probe hanging. Zero duration is an explicit readiness probe:
 * register, recheck, settle without a timeout timer.
 *
 * When the last waiter for a queue detaches, the deadline timer is cancelled immediately even if a
 * readiness query is still in flight. Watch identity discards that stale result so it cannot
 * re-arm timers for an idle queue.
 *
 * Internal waiter and watch maps use a `queue:` prefix so numeric queue names stay strings under
 * PHP array-key coercion. Storage queries always use the original {@see QueueName}.
 */
final class QueueNotifier
{
    private const string WATCH_KEY_PREFIX = 'queue:';

    /** @var array<string, list<QueueNotifierWaiter>> */
    private array $waiters = [];

    /** @var array<string, QueueNotifierWatch> */
    private array $watches = [];

    private bool $closed = false;

    /**
     * @param \Closure(QueueName, ?Cancellation): (?int) $earliestEligibility persisted readiness lookup
     * @param \Closure(): int                            $clock               Unix wall-clock milliseconds
     * @param \Closure(\Throwable): void                 $onFailure           fail-closed callback for async query or timer faults
     */
    public function __construct(
        private readonly \Closure $earliestEligibility,
        private readonly \Closure $clock,
        private readonly \Closure $onFailure,
    ) {
    }

    /**
     * Wait until the named queue may have eligible work, or until the bound expires.
     *
     * Zero duration performs a nonblocking readiness probe after waiter registration and the
     * initial recheck. Cancellation fails the wait with {@see CancelledException}.
     */
    public function wait(QueueName $queue, int $timeoutMilliseconds, Cancellation $cancellation): bool
    {
        if ($timeoutMilliseconds < 0) {
            throw new \InvalidArgumentException('Wait timeout must be nonnegative milliseconds.');
        }
        if ($this->closed) {
            throw new ClientContextClosedException('Broker wait service is closed.');
        }
        $cancellation->throwIfRequested();

        $key = self::watchKey($queue);
        $deferred = new DeferredFuture();
        $waiter = new QueueNotifierWaiter($deferred, $timeoutMilliseconds);
        $waiter->keys = [$key];
        if (0 === $timeoutMilliseconds) {
            $waiter->pendingProbeKeys = [$key];
        }
        $this->waiters[$key][] = $waiter;

        return $this->awaitWaiter($waiter, [$queue], $cancellation);
    }

    /**
     * Wait until any selected queue may have eligible work, or until the shared bound expires.
     *
     * Every selected queue is registered before any readiness recheck can suspend. The boolean is
     * only a hint to receive on the caller's chosen queues. Cancellation fails the wait with
     * {@see CancelledException}.
     *
     * @param list<QueueName> $queues
     */
    public function waitAny(array $queues, int $timeoutMilliseconds, Cancellation $cancellation): bool
    {
        if ($timeoutMilliseconds < 0) {
            throw new \InvalidArgumentException('Wait timeout must be nonnegative milliseconds.');
        }
        $selected = $this->validatedQueues($queues);
        if ($this->closed) {
            throw new ClientContextClosedException('Broker wait service is closed.');
        }
        $cancellation->throwIfRequested();

        $deferred = new DeferredFuture();
        $waiter = new QueueNotifierWaiter($deferred, $timeoutMilliseconds);
        $keys = [];
        foreach ($selected as $queue) {
            $keys[] = self::watchKey($queue);
        }
        $waiter->keys = $keys;
        if (0 === $timeoutMilliseconds) {
            $waiter->pendingProbeKeys = $keys;
        }
        // Occupy every selected queue before any refresh callback can suspend.
        foreach ($keys as $key) {
            $this->waiters[$key][] = $waiter;
        }

        return $this->awaitWaiter($waiter, $selected, $cancellation);
    }

    /** Called only after a committed mutation that may change readiness for the named queue. */
    public function notify(QueueName $queue): void
    {
        if ($this->closed) {
            return;
        }
        $key = self::watchKey($queue);
        if (!isset($this->waiters[$key]) && !isset($this->watches[$key])) {
            return;
        }
        $this->watches[$key] ??= new QueueNotifierWatch($queue);
        $this->scheduleRefresh($key);
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        foreach (array_keys($this->watches) as $key) {
            $this->clearDeadline($key);
        }
        $outstanding = [];
        foreach ($this->waiters as $waiters) {
            foreach ($waiters as $waiter) {
                $outstanding[spl_object_id($waiter)] = $waiter;
            }
        }
        foreach ($outstanding as $waiter) {
            $this->failWaiter($waiter, new CancelledException());
        }
        $this->waiters = [];
        $this->watches = [];
    }

    /**
     * @param list<QueueName> $queues
     */
    private function awaitWaiter(QueueNotifierWaiter $waiter, array $queues, Cancellation $cancellation): bool
    {
        $future = $waiter->deferred->getFuture();
        try {
            $waiter->cancellationId = $cancellation->subscribe(
                function (CancelledException $error) use ($waiter): void {
                    $this->failWaiter($waiter, $error);
                },
            );
            if ($waiter->settled) {
                return $future->await();
            }
            if ($waiter->durationMilliseconds > 0) {
                // Bound the wait by event-loop duration, not by comparing the controlled wall clock.
                // A forward clock jump must not expire a still-running wait early.
                $waiter->timeoutId = EventLoop::delay($waiter->durationMilliseconds / 1000, function () use ($waiter): void {
                    try {
                        if ($waiter->settled || $this->closed) {
                            return;
                        }
                        $waiter->timeoutId = null;
                        $this->completeWaiter($waiter, false);
                    } catch (\Throwable $error) {
                        foreach ($waiter->keys as $key) {
                            $this->failQueue($key, $error);

                            return;
                        }
                    }
                });
            }

            foreach ($queues as $queue) {
                $key = self::watchKey($queue);
                $this->watches[$key] ??= new QueueNotifierWatch($queue);
                // Register first, then recheck on a queued callback. notify() may dirty the watch
                // before that callback runs, forcing another pass.
                $this->scheduleRefresh($key);
            }

            return $future->await();
        } finally {
            if (null !== $waiter->cancellationId) {
                $cancellation->unsubscribe($waiter->cancellationId);
                $waiter->cancellationId = null;
            }
            $this->detachWaiter($waiter);
        }
    }

    /**
     * @param list<QueueName> $queues
     *
     * @return list<QueueName>
     */
    private function validatedQueues(array $queues): array
    {
        if ([] === $queues) {
            throw new \InvalidArgumentException('WAIT_ANY requires at least one queue.');
        }
        if (\count($queues) > Limits::MAX_WAIT_QUEUES) {
            throw new \InvalidArgumentException(\sprintf('WAIT_ANY accepts at most %d queues.', Limits::MAX_WAIT_QUEUES));
        }
        $selected = [];
        $seen = [];
        foreach ($queues as $queue) {
            if (!$queue instanceof QueueName) {
                throw new \InvalidArgumentException('WAIT_ANY queues must be QueueName instances.');
            }
            if (isset($seen[$queue->value])) {
                throw new \InvalidArgumentException('WAIT_ANY queue names must be unique.');
            }
            $seen[$queue->value] = true;
            $selected[] = $queue;
        }

        return $selected;
    }

    /** Stable string map key that keeps numeric queue names from becoming integer array keys. */
    private static function watchKey(QueueName $queue): string
    {
        return self::WATCH_KEY_PREFIX.$queue->value;
    }

    private function scheduleRefresh(string $key): void
    {
        $watch = $this->watches[$key] ?? null;
        if (null === $watch || $this->closed) {
            return;
        }
        $watch->dirty = true;
        foreach ($this->waiters[$key] ?? [] as $waiter) {
            if (0 !== $waiter->durationMilliseconds || $waiter->settled) {
                continue;
            }
            if (!\in_array($key, $waiter->pendingProbeKeys, true)) {
                // A later notify must keep a zero-duration WAIT_ANY open until every selected
                // queue has a fresh not-ready sample for this generation.
                $waiter->pendingProbeKeys[] = $key;
            }
        }
        if ($watch->querying) {
            return;
        }
        $watch->querying = true;
        EventLoop::queue(function () use ($key, $watch): void {
            try {
                $this->runRefresh($key, $watch);
            } catch (\Throwable $error) {
                $this->failQueue($key, $error);
            }
        });
    }

    private function runRefresh(string $key, QueueNotifierWatch $watch): void
    {
        if ($this->closed) {
            return;
        }
        if (($this->watches[$key] ?? null) !== $watch) {
            return;
        }
        $watch->dirty = false;
        try {
            $readyAt = $this->queryEligibility($watch);
        } catch (\Throwable $error) {
            $this->failQueue($key, $error);

            return;
        }
        $this->applyReadiness($key, $watch, $readyAt);
    }

    private function queryEligibility(QueueNotifierWatch $watch): ?int
    {
        if ($this->closed || ($this->watches[self::watchKey($watch->queue)] ?? null) !== $watch) {
            return null;
        }

        return ($this->earliestEligibility)($watch->queue, null);
    }

    /** A null deadline means the query found no messages in this queue. */
    private function applyReadiness(string $key, QueueNotifierWatch $watch, ?int $readyAt): void
    {
        if (($this->watches[$key] ?? null) !== $watch) {
            // The watch may have changed in another fiber while the query was suspended.
            return;
        }
        $watch->querying = false;
        if ($watch->dirty) {
            // Mutation arrived while the query was suspended; discard and re-query so a stale
            // later deadline cannot overwrite earlier readiness.
            $this->scheduleRefresh($key);

            return;
        }
        $now = $this->now();
        if (null !== $readyAt && $readyAt <= $now) {
            $this->clearDeadline($key);
            foreach ($this->waiters[$key] ?? [] as $waiter) {
                $this->completeWaiter($waiter, true);
            }
            $this->forgetIdle($key);

            return;
        }
        // Positive waits expire only through their event-loop timers. Zero-duration probes settle
        // false only after every selected queue has reported not-ready for this generation.
        foreach ($this->waiters[$key] ?? [] as $waiter) {
            if (0 === $waiter->durationMilliseconds) {
                $waiter->pendingProbeKeys = array_values(array_filter(
                    $waiter->pendingProbeKeys,
                    static fn (string $candidate): bool => $candidate !== $key,
                ));
                if ([] === $waiter->pendingProbeKeys) {
                    $this->completeWaiter($waiter, false);
                }
            }
        }
        if ([] === ($this->waiters[$key] ?? [])) {
            $this->clearDeadline($key);
            $this->forgetIdle($key);

            return;
        }
        if (null === $readyAt) {
            $this->clearDeadline($key);

            return;
        }
        $this->armDeadline($key, $readyAt, $now);
    }

    private function armDeadline(string $key, int $readyAt, int $now): void
    {
        $watch = $this->watches[$key];
        if ($watch->timerReadyAt === $readyAt && null !== $watch->timerId) {
            return;
        }
        $this->clearDeadline($key);
        $delayMs = max(0, $readyAt - $now);
        $watch->timerReadyAt = $readyAt;
        $watch->timerId = EventLoop::delay($delayMs / 1000, function (string $timerId) use ($key, $watch): void {
            try {
                if (($this->watches[$key] ?? null) !== $watch || $watch->timerId !== $timerId) {
                    return;
                }
                $watch->timerId = null;
                $watch->timerReadyAt = null;
                // Due reserved rows must re-query; do not wake forever from stale availability.
                $this->scheduleRefresh($key);
            } catch (\Throwable $error) {
                $this->failQueue($key, $error);
            }
        });
    }

    private function completeWaiter(QueueNotifierWaiter $waiter, bool $ready): void
    {
        if ($waiter->settled) {
            return;
        }
        $waiter->settled = true;
        $this->cancelTimeout($waiter);
        $waiter->deferred->complete($ready);
        $this->detachWaiter($waiter);
    }

    private function failWaiter(QueueNotifierWaiter $waiter, \Throwable $error): void
    {
        if ($waiter->settled) {
            return;
        }
        $waiter->settled = true;
        $this->cancelTimeout($waiter);
        $future = $waiter->deferred->getFuture();
        $future->ignore();
        $waiter->deferred->error($error);
        $this->detachWaiter($waiter);
    }

    private function detachWaiter(QueueNotifierWaiter $waiter): void
    {
        $this->cancelTimeout($waiter);
        foreach ($waiter->keys as $key) {
            if (!isset($this->waiters[$key])) {
                continue;
            }
            $this->waiters[$key] = array_values(array_filter(
                $this->waiters[$key],
                static fn (QueueNotifierWaiter $candidate): bool => $candidate !== $waiter,
            ));
            if ([] === $this->waiters[$key]) {
                unset($this->waiters[$key]);
                if (!$this->closed) {
                    // Cancel the deadline immediately, even while a readiness query is still running.
                    // Dropping this identity prevents stale results from mutating a replacement watch.
                    $this->clearDeadline($key);
                    unset($this->watches[$key]);
                }
            }
        }
    }

    private function cancelTimeout(QueueNotifierWaiter $waiter): void
    {
        if (null !== $waiter->timeoutId) {
            EventLoop::cancel($waiter->timeoutId);
            $waiter->timeoutId = null;
        }
    }

    private function clearDeadline(string $key): void
    {
        $watch = $this->watches[$key] ?? null;
        if (null === $watch) {
            return;
        }
        if (null !== $watch->timerId) {
            EventLoop::cancel($watch->timerId);
            $watch->timerId = null;
        }
        $watch->timerReadyAt = null;
    }

    private function forgetIdle(string $key): void
    {
        if ([] !== ($this->waiters[$key] ?? [])) {
            return;
        }
        $watch = $this->watches[$key] ?? null;
        if (null === $watch || $watch->querying || $watch->dirty) {
            return;
        }
        $this->clearDeadline($key);
        unset($this->watches[$key]);
    }

    private function failQueue(string $key, \Throwable $error): void
    {
        if ($this->closed) {
            return;
        }
        $watch = $this->watches[$key] ?? null;
        if (null !== $watch) {
            $watch->querying = false;
            $watch->dirty = false;
            $this->clearDeadline($key);
            unset($this->watches[$key]);
        }
        foreach ($this->waiters[$key] ?? [] as $waiter) {
            $this->failWaiter($waiter, $error);
        }
        ($this->onFailure)($error);
    }

    private function now(): int
    {
        $now = ($this->clock)();
        if ($now < 0) {
            throw new \InvalidArgumentException('Wall-clock milliseconds must be nonnegative.');
        }

        return $now;
    }
}
