<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;

use function Amp\async;

/**
 * Starts the driver's persistence child and keeps its observable handle.
 *
 * The SQLite connector owns when start() runs; persistence() exposes the single
 * handle it created. Observe the existing driver's process, never replace its
 * worker implementation.
 */
final class PersistenceFactory implements ContextFactory
{
    /** @var list<Persistence> */
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
        $drains = [];
        // Drain without logging: persistence diagnostics may contain SQL or data.
        foreach ([$context->getStdout(), $context->getStderr()] as $stream) {
            $drains[] = async(static function () use ($stream): void {
                while (null !== $stream->read()) {
                }
            });
        }
        $this->created[] = new Persistence($context, $drains);

        return $context;
    }

    public function persistence(): Persistence
    {
        // The connector starts exactly one persistence child per connection.
        if (1 !== \count($this->created)) {
            throw new \LogicException('The connector must start exactly one persistence child per connection.');
        }

        return $this->created[0];
    }

    /**
     * Every handle the connector has started through this factory, oldest first.
     *
     * @return list<Persistence>
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
        foreach ($this->created as $persistence) {
            try {
                $persistence->forceStop();
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
        }
        if (null !== $failure) {
            throw $failure;
        }
    }
}
