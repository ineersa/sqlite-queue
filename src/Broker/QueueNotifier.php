<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Revolt\EventLoop;

/**
 * Coordinates bounded WAIT hints from persisted readiness deadlines.
 *
 * A true result is only a hint to receive, never a claim. Waiters are registered before the
 * readiness recheck so a committed send between empty receive and WAIT cannot be lost. Derived
 * timer state exists only for watched queues and is rebuilt lazily from storage.
 *
 * Positive wait bounds use an event-loop delay only. The controlled wall clock schedules delayed
 * readiness, but it must not expire a positive WAIT early when it jumps forward, and a backward
 * jump must not leave a zero-duration probe hanging. Zero duration is an explicit readiness probe:
 * register, recheck, settle without a timeout timer.
 *
 * When the last waiter for a queue detaches, the deadline timer is cancelled immediately even if a
 * readiness query is still in flight. Watch identity discards that stale result so it cannot
 * re-arm timers for an idle queue.
 */
final class QueueNotifier
{
    /** @var array<string, list<QueueNotifierWaiter>> */
    private array $waiters = [];

    /** @var array<string, QueueNotifierWatch> */
    private array $watches = [];

    private bool $closed = false;

    /**
     * @param \Closure(): int            $clock     Unix wall-clock milliseconds
     * @param \Closure(\Throwable): void $onFailure fail-closed callback for async query or timer faults
     */
    public function __construct(
        private readonly Queue $queue,
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

        $name = $queue->value;
        $deferred = new DeferredFuture();
        $future = $deferred->getFuture();
        $waiter = new QueueNotifierWaiter($deferred, $timeoutMilliseconds);
        $this->waiters[$name][] = $waiter;
        try {
            $waiter->cancellationId = $cancellation->subscribe(
                function (CancelledException $error) use ($name, $waiter): void {
                    $this->failWaiter($name, $waiter, $error);
                },
            );
            if ($waiter->settled) {
                return $future->await();
            }
            if ($timeoutMilliseconds > 0) {
                // Bound the wait by event-loop duration, not by comparing the controlled wall clock.
                // A forward clock jump must not expire a still-running wait early.
                $waiter->timeoutId = EventLoop::delay($timeoutMilliseconds / 1000, function () use ($name, $waiter): void {
                    try {
                        if ($waiter->settled || $this->closed) {
                            return;
                        }
                        $waiter->timeoutId = null;
                        $this->completeWaiter($name, $waiter, false);
                    } catch (\Throwable $error) {
                        $this->failQueue($name, $error);
                    }
                });
            }

            $this->watches[$name] ??= new QueueNotifierWatch();
            // Register first, then recheck on a queued callback. notify() may dirty the watch
            // before that callback runs, forcing another pass.
            $this->scheduleRefresh($name);

            return $future->await();
        } finally {
            if (null !== $waiter->cancellationId) {
                $cancellation->unsubscribe($waiter->cancellationId);
                $waiter->cancellationId = null;
            }
            $this->detachWaiter($name, $waiter);
        }
    }

    /** Called only after a committed mutation that may change readiness for the named queue. */
    public function notify(QueueName $queue): void
    {
        if ($this->closed) {
            return;
        }
        $name = $queue->value;
        if (!isset($this->waiters[$name]) && !isset($this->watches[$name])) {
            return;
        }
        $this->watches[$name] ??= new QueueNotifierWatch();
        $this->scheduleRefresh($name);
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        foreach (array_keys($this->watches) as $name) {
            $this->clearDeadline($name);
        }
        foreach ($this->waiters as $name => $waiters) {
            foreach ($waiters as $waiter) {
                $this->failWaiter($name, $waiter, new CancelledException());
            }
        }
        $this->waiters = [];
        $this->watches = [];
    }

    private function scheduleRefresh(string $name): void
    {
        $watch = $this->watches[$name] ?? null;
        if (null === $watch || $this->closed) {
            return;
        }
        $watch->dirty = true;
        if ($watch->querying) {
            return;
        }
        $watch->querying = true;
        EventLoop::queue(function () use ($name, $watch): void {
            try {
                $this->runRefresh($name, $watch);
            } catch (\Throwable $error) {
                $this->failQueue($name, $error);
            }
        });
    }

    private function runRefresh(string $name, QueueNotifierWatch $watch): void
    {
        if ($this->closed) {
            return;
        }
        if (($this->watches[$name] ?? null) !== $watch) {
            return;
        }
        $watch->dirty = false;
        try {
            $readyAt = $this->queue->earliestEligibility(new QueueName($name));
        } catch (\Throwable $error) {
            $this->failQueue($name, $error);

            return;
        }
        $this->applyReadiness($name, $watch, $readyAt);
    }

    /** A null deadline means the query found no messages in this queue. */
    private function applyReadiness(string $name, QueueNotifierWatch $watch, ?int $readyAt): void
    {
        if (($this->watches[$name] ?? null) !== $watch) {
            // The watch may have changed in another fiber while the query was suspended.
            return;
        }
        $watch->querying = false;
        if ($watch->dirty) {
            // Mutation arrived while the query was suspended; discard and re-query so a stale
            // later deadline cannot overwrite earlier readiness.
            $this->scheduleRefresh($name);

            return;
        }
        $now = $this->now();
        if (null !== $readyAt && $readyAt <= $now) {
            $this->clearDeadline($name);
            foreach ($this->waiters[$name] ?? [] as $waiter) {
                $this->completeWaiter($name, $waiter, true);
            }
            $this->forgetIdle($name);

            return;
        }
        // Positive waits expire only through their event-loop timers. Zero-duration probes settle
        // here once the first successful readiness sample for this generation is known.
        foreach ($this->waiters[$name] ?? [] as $waiter) {
            if (0 === $waiter->durationMilliseconds) {
                $this->completeWaiter($name, $waiter, false);
            }
        }
        if ([] === ($this->waiters[$name] ?? [])) {
            $this->clearDeadline($name);
            $this->forgetIdle($name);

            return;
        }
        if (null === $readyAt) {
            $this->clearDeadline($name);

            return;
        }
        $this->armDeadline($name, $readyAt, $now);
    }

    private function armDeadline(string $name, int $readyAt, int $now): void
    {
        $watch = $this->watches[$name];
        if ($watch->timerReadyAt === $readyAt && null !== $watch->timerId) {
            return;
        }
        $this->clearDeadline($name);
        $delayMs = max(0, $readyAt - $now);
        $watch->timerReadyAt = $readyAt;
        $watch->timerId = EventLoop::delay($delayMs / 1000, function (string $timerId) use ($name, $watch): void {
            try {
                if (($this->watches[$name] ?? null) !== $watch || $watch->timerId !== $timerId) {
                    return;
                }
                $watch->timerId = null;
                $watch->timerReadyAt = null;
                // Due reserved rows must re-query; do not wake forever from stale availability.
                $this->scheduleRefresh($name);
            } catch (\Throwable $error) {
                $this->failQueue($name, $error);
            }
        });
    }

    private function completeWaiter(string $name, QueueNotifierWaiter $waiter, bool $ready): void
    {
        if ($waiter->settled) {
            return;
        }
        $waiter->settled = true;
        $this->cancelTimeout($waiter);
        $waiter->deferred->complete($ready);
        $this->detachWaiter($name, $waiter);
    }

    private function failWaiter(string $name, QueueNotifierWaiter $waiter, \Throwable $error): void
    {
        if ($waiter->settled) {
            return;
        }
        $waiter->settled = true;
        $this->cancelTimeout($waiter);
        $future = $waiter->deferred->getFuture();
        $future->ignore();
        $waiter->deferred->error($error);
        $this->detachWaiter($name, $waiter);
    }

    private function detachWaiter(string $name, QueueNotifierWaiter $waiter): void
    {
        if (!isset($this->waiters[$name])) {
            return;
        }
        $this->cancelTimeout($waiter);
        $this->waiters[$name] = array_values(array_filter(
            $this->waiters[$name],
            static fn (QueueNotifierWaiter $candidate): bool => $candidate !== $waiter,
        ));
        if ([] === $this->waiters[$name]) {
            unset($this->waiters[$name]);
            if (!$this->closed) {
                // Cancel the deadline immediately, even while a readiness query is still running.
                // Dropping this identity prevents stale results from mutating a replacement watch.
                $watch = $this->watches[$name] ?? null;
                if (null !== $watch) {
                    $this->clearDeadline($name);
                    unset($this->watches[$name]);
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

    private function clearDeadline(string $name): void
    {
        $watch = $this->watches[$name] ?? null;
        if (null === $watch) {
            return;
        }
        if (null !== $watch->timerId) {
            EventLoop::cancel($watch->timerId);
            $watch->timerId = null;
        }
        $watch->timerReadyAt = null;
    }

    private function forgetIdle(string $name): void
    {
        if ([] !== ($this->waiters[$name] ?? [])) {
            return;
        }
        $watch = $this->watches[$name] ?? null;
        if (null === $watch || $watch->querying || $watch->dirty) {
            return;
        }
        $this->clearDeadline($name);
        unset($this->watches[$name]);
    }

    private function failQueue(string $name, \Throwable $error): void
    {
        if ($this->closed) {
            return;
        }
        $watch = $this->watches[$name] ?? null;
        if (null !== $watch) {
            $watch->querying = false;
            $watch->dirty = false;
            $this->clearDeadline($name);
            unset($this->watches[$name]);
        }
        foreach ($this->waiters[$name] ?? [] as $waiter) {
            $this->failWaiter($name, $waiter, $error);
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
