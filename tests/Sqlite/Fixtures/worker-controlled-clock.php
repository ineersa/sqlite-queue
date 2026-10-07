<?php

declare(strict_types=1);

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerRuntime;

/*
 * Test-only worker bootstrap.
 *
 * Amp passes argv after the script path. The first argument is the initial wall-clock
 * millisecond value used by Queue after transaction acquisition.
 */
return static function (Channel $channel) use ($argv): void {
    $now = isset($argv[1]) && is_numeric($argv[1]) ? (int) $argv[1] : 1_700_000_000_000;
    SqliteWorkerRuntime::run(
        $channel,
        static function () use (&$now): int {
            return $now;
        },
    );
};
