<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Driver;

use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;

use function Amp\delay;

/**
 * Process lifecycle evidence. Every test here reads /proc, so the base class skips the whole
 * driver suite when /proc is unreadable instead of reporting a pass that observed nothing.
 *
 * Connections close in a finally block. close() is idempotent and force-closes an active
 * transaction, so a failed assertion cannot strand a persistence process.
 */
final class BootstrapProcessTest extends DriverTestCase
{
    public function testConnectionRunsInItsOwnProcessAndClosingReapsIt(): void
    {
        $before = $this->ownedProcessIds();

        $this->runAsync(function () use ($before): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));

            try {
                $connection->execute('CREATE TABLE probe (id INTEGER PRIMARY KEY)');

                $spawned = $this->waitForSpawnedProcesses($before, 5.0);
                self::assertNotSame([], $spawned, 'Connecting did not start a persistence process.');

                foreach ($spawned as $pid) {
                    self::assertTrue(
                        ProcessTree::isDescendantOf($pid, getmypid(), ProcessTree::snapshot()),
                        \sprintf('Process %d is not a child of the test process.', $pid),
                    );
                    self::assertNotSame(getmypid(), $pid);
                }

                $workers = ProcessTree::ownedBy(getmypid())['workers'];
                self::assertNotSame([], $workers, 'No process titled "amp-process" was found.');

                $connection->close();

                self::assertSame(
                    [],
                    $this->waitForOwnedProcesses([], 5.0),
                    'Closing the connection left a persistence process behind.',
                );
            } finally {
                $connection->close();
            }
        });
    }

    public function testClosingAConnectionWithAnOpenTransactionReapsItsProcess(): void
    {
        $this->runAsync(function (): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));

            try {
                $connection->execute('CREATE TABLE probe (id INTEGER PRIMARY KEY)');

                $transaction = $connection->beginTransaction();
                $transaction->execute('INSERT INTO probe (id) VALUES (1)');

                $connection->close();

                self::assertSame([], $this->waitForOwnedProcesses([], 5.0));
            } finally {
                $connection->close();
            }
        });
    }

    public function testWorkerDeathSurfacesAsAConnectionException(): void
    {
        if (!\function_exists('posix_kill')) {
            $this->markTestSkipped('ext-posix is required to kill the persistence worker.');
        }

        $this->runAsync(function (): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));

            try {
                $connection->execute('CREATE TABLE probe (id INTEGER PRIMARY KEY)');

                $workers = $this->waitForWorkers(5.0);
                self::assertNotSame([], $workers, 'No persistence worker was found to kill.');

                posix_kill($workers[0], \SIGKILL);
                delay(0.2);

                try {
                    $connection->query('SELECT count(*) c FROM probe')->fetchRow();
                    self::fail('A query on a dead persistence worker did not fail.');
                } catch (SqliteConnectionException $exception) {
                    self::assertStringContainsString('stopped unexpectedly', $exception->getMessage());
                }

                $connection->close();

                self::assertSame([], $this->waitForOwnedProcesses([], 5.0));
            } finally {
                $connection->close();
            }
        });
    }

    /**
     * @param list<int> $before
     *
     * @return list<int>
     */
    private function waitForSpawnedProcesses(array $before, float $timeout): array
    {
        $deadline = microtime(true) + $timeout;

        do {
            $spawned = array_values(array_diff($this->ownedProcessIds(), $before));
            if ([] !== $spawned) {
                return $spawned;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        return [];
    }

    /**
     * @return list<int>
     */
    private function waitForWorkers(float $timeout): array
    {
        $deadline = microtime(true) + $timeout;

        do {
            $workers = ProcessTree::ownedBy(getmypid())['workers'];
            if ([] !== $workers) {
                return $workers;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        return [];
    }
}
