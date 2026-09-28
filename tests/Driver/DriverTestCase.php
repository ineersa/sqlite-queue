<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Driver;

use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\TestCase;
use function Amp\async;

/**
 * Base class for driver checks.
 *
 * Each test owns an isolated database directory, runs its async work on a fresh event
 * loop, and proves on teardown that no persistence worker process is left behind.
 *
 * The process proof reads /proc, so it exists only on Linux. On a platform without a
 * readable /proc the driver suite skips explicitly instead of reporting a pass that
 * never observed a process.
 */
abstract class DriverTestCase extends TestCase
{
    protected ?IsolatedDatabase $database = null;

    protected function setUp(): void
    {
        parent::setUp();

        if (!ProcessTree::available()) {
            self::markTestSkipped(
                'The driver suite proves process cleanup by reading /proc, which is unavailable here.',
            );
        }

        $this->database = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        try {
            if (ProcessTree::available()) {
                $survivors = $this->waitForOwnedProcesses([], 5.0);
                self::assertSame([], $survivors, 'A persistence worker process survived the test.');
            }
        } finally {
            $this->database?->remove();
        }

        parent::tearDown();
    }

    final protected function runAsync(callable $operation): mixed
    {
        return async($operation)->await();
    }

    /**
     * Waits until the processes owned by this test match $expected, then returns what is left.
     *
     * @param list<int> $expected PIDs that may stay alive, if any.
     * @return list<int>
     */
    final protected function waitForOwnedProcesses(array $expected, float $timeout): array
    {
        $deadline = \microtime(true) + $timeout;
        $owned = $this->ownedProcessIds();

        while (\microtime(true) < $deadline) {
            $owned = $this->ownedProcessIds();
            if ($owned === $expected) {
                return $owned;
            }
            \usleep(20_000);
        }

        return $owned;
    }

    /**
     * @return list<int>
     */
    final protected function ownedProcessIds(): array
    {
        if (!ProcessTree::available()) {
            return [];
        }

        $owned = ProcessTree::ownedBy(\getmypid());

        return \array_values(\array_unique([...$owned['launchers'], ...$owned['workers']]));
    }
}
