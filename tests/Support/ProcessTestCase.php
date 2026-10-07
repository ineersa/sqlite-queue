<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Isolates database files and verifies owned worker cleanup on Linux.
 * Tests requiring this process proof skip when /proc is unavailable.
 */
abstract class ProcessTestCase extends TestCase
{
    protected ?IsolatedDatabase $database = null;

    protected function setUp(): void
    {
        parent::setUp();

        if (!ProcessTree::available()) {
            $this->markTestSkipped(
                'Process cleanup checks require readable /proc.',
            );
        }

        $this->database = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        try {
            if (ProcessTree::available()) {
                $survivors = $this->waitForOwnedProcesses([], 5.0);
                $this->assertSame([], $survivors, 'A persistence worker process survived the test.');
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
     * @param list<int> $expected PIDs that may stay alive, if any
     *
     * @return list<int>
     */
    final protected function waitForOwnedProcesses(array $expected, float $timeout): array
    {
        $deadline = microtime(true) + $timeout;
        $owned = $this->ownedProcessIds();

        while (microtime(true) < $deadline) {
            $owned = $this->ownedProcessIds();
            if ($owned === $expected) {
                return $owned;
            }
            usleep(20_000);
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

        $owned = ProcessTree::ownedBy(getmypid());

        return array_values(array_unique([...$owned['launchers'], ...$owned['workers']]));
    }
}
