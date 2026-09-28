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
    /** @var array<string, string> */
    private const array ISOLATED_ENVIRONMENT = ['PATH' => '/usr/bin:/bin', 'LANG' => 'C', 'TZ' => 'UTC'];

    /** @var list<Persistence> */
    private array $created = [];

    /**
     * @param string|non-empty-list<string> $script
     *
     * @return ProcessContext<mixed, mixed, mixed>
     */
    public function start(string|array $script, ?Cancellation $cancellation = null): ProcessContext
    {
        $context = (new ProcessContextFactory(environment: self::ISOLATED_ENVIRONMENT))->start($script, $cancellation);
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
