<?php

declare(strict_types=1);

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerRuntime;

/*
 * Test-only worker bootstrap.
 *
 * Amp passes argv after the script path:
 *   argv[1] = initial wall-clock milliseconds
 *   argv[2] = optional absolute path to a clock file
 *
 * When a clock file is provided, each sample reads that file so the parent can
 * advance child storage time without production IPC clock commands.
 */
return static function (Channel $channel) use ($argv): void {
    $now = isset($argv[1]) && is_numeric($argv[1]) ? (int) $argv[1] : 1_700_000_000_000;
    $clockFile = isset($argv[2]) && is_string($argv[2]) && '' !== $argv[2] ? $argv[2] : null;
    SqliteWorkerRuntime::run(
        $channel,
        static function () use (&$now, $clockFile): int {
            if (null !== $clockFile && is_file($clockFile)) {
                $raw = file_get_contents($clockFile);
                if (false !== $raw && is_numeric(trim($raw))) {
                    $now = (int) trim($raw);
                }
            }

            return $now;
        },
    );
};
