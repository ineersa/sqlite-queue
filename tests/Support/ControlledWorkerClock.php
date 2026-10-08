<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Ineersa\SqliteQueue\Sqlite\SqliteWorkerContextFactory;
use Symfony\Component\Filesystem\Filesystem;

/** Test bootstrap clocks never travel through the production worker protocol. */
trait ControlledWorkerClock
{
    /** A null clock selects the normal wall-clock worker. */
    private function workerFactory(string $databasePath, ?\Closure $clock = null): SqliteWorkerContextFactory
    {
        if (null === $clock) {
            return new SqliteWorkerContextFactory();
        }
        $now = $clock();
        $clockFile = $databasePath.'.clock';
        (new Filesystem())->dumpFile($clockFile, (string) $now);

        return new SqliteWorkerContextFactory([
            \dirname(__DIR__).'/Sqlite/Fixtures/worker-controlled-clock.php',
            (string) $now,
            $clockFile,
        ]);
    }

    /** Null leaves the broker's production notifier clock unchanged. */
    private function syncedClock(?\Closure $clock, string $databasePath): ?\Closure
    {
        if (null === $clock) {
            return null;
        }
        $clockFile = $databasePath.'.clock';

        return static function () use ($clock, $clockFile): int {
            $value = $clock();
            (new Filesystem())->dumpFile($clockFile, (string) $value);

            return $value;
        };
    }
}
