<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Amp\Cancellation;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextFactory;

final class QueueFaultContextFactory implements ContextFactory
{
    public int $starts = 0;

    public function __construct(private readonly ContextFactory $factory, private readonly string $directory, private readonly string $uri)
    {
    }

    /** Optional cancellation is the ContextFactory startup contract. */
    public function start(string|array $script, ?Cancellation $cancellation = null): Context
    {
        ++$this->starts;

        return $this->factory->start([
            __DIR__.'/../Sqlite/Fixtures/operation-fault-worker.php',
            $this->directory.'/WorkerProcess.php',
            $this->directory.'/worker.php',
            $this->uri,
        ], $cancellation);
    }
}
