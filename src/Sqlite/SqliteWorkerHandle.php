<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Parallel\Context\ProcessContext;

/**
 * Observable handle for the same SQLite worker the vendor connector starts.
 *
 * Vendor SqliteConnector calls SqliteWorkerContextFactory, which delegates to Amp and retains
 * this handle to that same worker. The driver alone owns joining the process context. This
 * handle supplies independent observation and termination because the locked driver's idle
 * graceful close has no timeout, marks the connection closed before awaiting, and cannot be
 * interrupted by repeating close(). Temporary integration workaround until the driver offers
 * a bounded close/abort API; not a replacement SQL driver.
 *
 * Always constructed with a live process: creation belongs to SqliteWorkerContextFactory.
 *
 * The handle owns the token that stops its pipe reads. Cancelling those reads releases
 * the readability watchers they hold, which is what lets the broker process exit.
 */
final class SqliteWorkerHandle
{
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

    /** The driver owns join(); observing pipe EOF avoids joining its context twice. */
    public function awaitExit(): void
    {
        foreach ($this->drains as $drain) {
            $drain->await();
        }
    }

    /**
     * Kill the owned child without joining it.
     *
     * A repeated connection close() cannot stop a worker that stopped answering: the driver
     * marks its connection closed before the graceful close returns, and a later close()
     * returns immediately. ProcessContext::close() kills the child unconditionally, so it
     * works even though the channel flag can read closed while the worker still runs.
     */
    public function forceStop(): void
    {
        $this->context->close();
    }

    /**
     * Kill the owned child, then observe its pipes until EOF or the shared budget expires.
     *
     * Cancellation is required: this runs inside a shutdown budget, and a timeout created here
     * would extend that budget behind the caller's back. The pipes can outlive the child, so the
     * observation must be bounded — a shell launcher that never exits keeps their write ends
     * open, and EOF never arrives.
     */
    public function close(Cancellation $budget): void
    {
        // The child must be gone even when the graceful close or a drain gives up.
        $this->forceStop();
        try {
            foreach ($this->drains as $drain) {
                $drain->await($budget);
            }
        } finally {
            // Cancel the pipe reads before dropping their futures. A read that stays pending holds
            // an enabled, referenced readability watcher, and the event loop cannot exit while such
            // a watcher remains, so releasing the pipes is what lets the broker process stop.
            $this->drainCancellation->cancel();
            // A pipe still open past the budget is abandoned: nothing can act on its result
            // after shutdown, so its error must not reach the event loop handler.
            foreach ($this->drains as $drain) {
                $drain->ignore();
            }
            $this->drains = [];
        }
    }
}
