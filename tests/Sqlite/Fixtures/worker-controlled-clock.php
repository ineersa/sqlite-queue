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
    $options = ['options' => ['min_range' => 0]];
    $now = filter_var($argv[1] ?? null, \FILTER_VALIDATE_INT, $options);
    if (false === $now) {
        throw new InvalidArgumentException('Test worker requires initial wall-clock milliseconds.');
    }
    $clock = static fn (): int => $now;
    if (isset($argv[2])) {
        $clockFile = $argv[2];
        $clock = static function () use ($clockFile, $options): int {
            $raw = file_get_contents($clockFile);
            if (false === $raw) {
                throw new RuntimeException('Cannot read controlled worker clock.');
            }
            $now = filter_var(trim($raw), \FILTER_VALIDATE_INT, $options);
            if (false === $now) {
                throw new InvalidArgumentException('Controlled worker clock must contain nonnegative milliseconds.');
            }

            return $now;
        };
    }
    SqliteWorkerRuntime::run($channel, $clock);
};
