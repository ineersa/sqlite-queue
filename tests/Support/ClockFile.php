<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

final class ClockFile
{
    public const string ENVIRONMENT_VARIABLE = 'SQLITE_QUEUE_TEST_CLOCK';

    public static function configure(string $database, int $now): void
    {
        self::set($database, $now);
        putenv(self::ENVIRONMENT_VARIABLE.'='.$database.'.clock');
    }

    public static function set(string $database, int $now): void
    {
        if (false === file_put_contents($database.'.clock', (string) $now)) {
            throw new \RuntimeException('Could not update the controlled worker clock.');
        }
    }
}
