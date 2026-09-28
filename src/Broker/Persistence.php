<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Future;
use Amp\Parallel\Context\ProcessContext;
use Amp\TimeoutCancellation;

/**
 * Observable handle for the driver's persistence child.
 *
 * Always constructed with a live process: creation belongs to PersistenceFactory,
 * which the SQLite connector drives through the ContextFactory contract.
 */
final class Persistence
{
    /**
     * @param ProcessContext<mixed, mixed, mixed> $context
     * @param list<Future<void>>                  $drains
     */
    public function __construct(private readonly ProcessContext $context, private array $drains)
    {
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

    public function close(): void
    {
        try {
            if (!$this->context->isClosed()) {
                $this->context->close();
            }
            foreach ($this->drains as $drain) {
                $drain->await(new TimeoutCancellation(5));
            }
        } finally {
            // The child must be gone even when the graceful close or a drain gave up.
            $this->forceStop();
            $this->drains = [];
        }
    }
}
