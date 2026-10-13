<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

final class DeadlinePolicy
{
    public static function now(QueueClock $clock): int
    {
        $now = $clock->now();
        if ($now < 0) {
            throw new \InvalidArgumentException('Wall-clock milliseconds must be nonnegative.');
        }

        return $now;
    }

    public static function deadline(int $now, int $milliseconds): int
    {
        if ($milliseconds < 0) {
            throw new \InvalidArgumentException('Duration must be nonnegative milliseconds.');
        }
        if ($milliseconds > \PHP_INT_MAX - $now) {
            throw new \InvalidArgumentException('Deadline exceeds the supported timestamp range.');
        }

        return $now + $milliseconds;
    }
}
