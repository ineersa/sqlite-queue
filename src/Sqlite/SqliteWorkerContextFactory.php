<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;

use function Amp\async;

/**
 * Starts the vendor SQLite worker and retains a handle to that same process.
 *
 * Vendor SqliteConnector calls this ContextFactory. The factory delegates to Amp's
 * ProcessContextFactory and keeps SqliteWorkerHandle for the identical worker the driver
 * starts. The driver alone joins that context; the handle supplies independent observation
 * and termination because the locked driver's idle graceful close has no timeout, marks the
 * connection closed before awaiting, and cannot be interrupted by repeating close().
 * Temporary integration workaround until the driver offers a bounded close/abort API; not a
 * replacement SQL driver.
 *
 * The connector owns when start() runs; worker() exposes the single handle it created.
 */
final class SqliteWorkerContextFactory implements ContextFactory
{
    /** @var list<SqliteWorkerHandle> */
    private array $created = [];

    /**
     * The signature is fixed by {@see ContextFactory}: the connector passes either a plain script
     * path or a non-empty `[path, ...arguments]` list, and a cancellation is optional in the
     * interface contract. The union and the nullable parameter are API requirements, not local
     * choices, so neither may be narrowed here.
     *
     * @param string|non-empty-list<string> $script
     *
     * @return ProcessContext<mixed, mixed, mixed>
     */
    #[\Override]
    public function start(string|array $script, ?Cancellation $cancellation = null): ProcessContext
    {
        // The child inherits the broker's environment. Amp replaces the entire environment when
        // given a non-empty array, which would drop what a PHP child needs to start correctly:
        // TMPDIR, and the configuration paths PHPRC and PHP_INI_SCAN_DIR that load shared
        // extensions such as sqlite3.
        $context = (new ProcessContextFactory())->start($script, $cancellation);
        // Both pipe reads share one token, so the handle can stop them as a unit. Cancelling a read
        // releases the readability watcher it holds, and a pending read would otherwise keep the
        // event loop referenced forever when the pipes never reach EOF.
        $drainCancellation = new DeferredCancellation();
        $drains = [];
        // Drain without logging: worker diagnostics may contain SQL or data.
        foreach ([$context->getStdout(), $context->getStderr()] as $stream) {
            $drains[] = async(static function () use ($stream, $drainCancellation): void {
                while (null !== $stream->read($drainCancellation->getCancellation())) {
                }
            });
        }
        $this->created[] = new SqliteWorkerHandle($context, $drains, $drainCancellation);

        return $context;
    }

    public function worker(): SqliteWorkerHandle
    {
        // The connector starts exactly one SQLite worker per connection.
        if (1 !== \count($this->created)) {
            throw new \LogicException('The connector must start exactly one SQLite worker per connection.');
        }

        return $this->created[0];
    }

    /**
     * Every handle the connector has started through this factory, oldest first.
     *
     * @return list<SqliteWorkerHandle>
     */
    public function created(): array
    {
        return $this->created;
    }

    /**
     * Kill every spawned child without joining any of them.
     *
     * The driver alone joins its contexts; this only guarantees no child survives a failed
     * startup, even when connect() fails after spawning the worker but before the caller
     * could observe the handle.
     */
    public function forceStopAll(): void
    {
        $failure = null;
        foreach ($this->created as $worker) {
            try {
                $worker->forceStop();
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
        }
        if (null !== $failure) {
            throw $failure;
        }
    }
}
