<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Parallel\Context\ContextException;
use Amp\Parallel\Context\ProcessContext;

use function Amp\async;

/**
 * Lifecycle owner for one package PDO worker process.
 *
 * Owns pipe drains and joins the process context at most once. Forced termination uses
 * ProcessContext::close() and tolerates the expected missing-result failure. AMPHP owns
 * underlying process-exit tracking.
 */
final class SqliteWorkerHandle
{
    /** @var Future<mixed>|null */
    private ?Future $joined = null;

    /**
     * @param ProcessContext<mixed, mixed, mixed> $context
     * @param list<Future<void>>                  $drains
     */
    public function __construct(
        private readonly ProcessContext $context,
        private array $drains,
        private readonly DeferredCancellation $drainCancellation,
    ) {
    }

    public function pid(): int
    {
        return $this->context->getPid();
    }

    /** Observe pipe EOF. This is not proof that the process has been reaped. */
    public function awaitExit(): void
    {
        foreach ($this->drains as $drain) {
            $drain->await();
        }
    }

    /**
     * Join the process context once and share that outcome.
     *
     * After SIGKILL the join may throw before the underlying process wait because the
     * worker cannot send its return-value message. Callers must tolerate that path and
     * must never retry this method.
     */
    public function join(?Cancellation $cancellation = null): mixed
    {
        $this->joined ??= async(function (): mixed {
            try {
                return $this->context->join();
            } catch (ContextException $error) {
                // Forced termination commonly loses the exit-result message. AMPHP still
                // tracks process exit; treat the missing result as an expected terminal state.
                return $error;
            }
        });

        return $this->joined->await($cancellation ?? new NullCancellation());
    }

    /** Kill the owned child without joining it. */
    public function forceStop(): void
    {
        $this->context->close();
    }

    /**
     * Force-stop the child, then observe pipes until EOF or the shared budget expires.
     *
     * Cancellation is required: this runs inside a shutdown budget. A timeout created here
     * would extend that budget behind the caller's back.
     */
    public function close(Cancellation $budget): void
    {
        $this->forceStop();
        try {
            // Absorb the expected missing-result exception once without retrying join().
            $this->join($budget);
        } catch (\Throwable) {
        }
        try {
            foreach ($this->drains as $drain) {
                $drain->await($budget);
            }
        } finally {
            $this->drainCancellation->cancel();
            foreach ($this->drains as $drain) {
                $drain->ignore();
            }
            $this->drains = [];
        }
    }

    /**
     * Join a gracefully exited child once, then release owned pipe readers.
     *
     * Use this after a successful close exchange. Forced termination still uses close().
     */
    public function finish(Cancellation $budget): void
    {
        try {
            $this->join($budget);
        } finally {
            try {
                foreach ($this->drains as $drain) {
                    $drain->await($budget);
                }
            } finally {
                $this->drainCancellation->cancel();
                foreach ($this->drains as $drain) {
                    $drain->ignore();
                }
                $this->drains = [];
            }
        }
    }

    /** Expose the live process channel for the parent proxy. */
    /**
     * @return ProcessContext<mixed, mixed, mixed>
     */
    public function context(): ProcessContext
    {
        return $this->context;
    }
}
