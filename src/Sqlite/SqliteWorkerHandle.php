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
 * Joins the process context at most once. Forced termination uses ProcessContext::close()
 * and tolerates a missing exit result. AMPHP owns process-exit tracking.
 */
final class SqliteWorkerHandle
{
    /**
     * Absent until join is requested.
     *
     * @var Future<null>|null
     */
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
     * Join once and share that outcome.
     *
     * Repeated calls never retry ProcessContext::join(). Null means no caller deadline.
     * Forced termination may throw ContextException because there is no exit result.
     */
    public function join(?Cancellation $cancellation = null): void
    {
        $this->joined ??= async(function (): void {
            $this->context->join();
        });
        $this->joined->ignore();
        $this->joined->await($cancellation ?? new NullCancellation());
    }

    /** Kill the owned child without joining it. */
    public function forceStop(): void
    {
        $this->context->close();
    }

    /**
     * Force-stop, tolerate a missing exit result, then release pipe readers.
     *
     * Cancellation is required: this runs inside a shared shutdown budget.
     */
    public function close(Cancellation $budget): void
    {
        $this->forceStop();
        try {
            $this->join($budget);
        } catch (ContextException) {
        } finally {
            $this->releaseDrains($budget);
        }
    }

    /**
     * Join a gracefully exited child once, then release pipe readers.
     *
     * Unexpected join failures propagate. Forced termination still uses close().
     */
    public function finish(Cancellation $budget): void
    {
        try {
            $this->join($budget);
        } finally {
            $this->releaseDrains($budget);
        }
    }

    /**
     * @return ProcessContext<mixed, mixed, mixed>
     */
    public function context(): ProcessContext
    {
        return $this->context;
    }

    private function releaseDrains(Cancellation $budget): void
    {
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
