<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Monotonic and wall clock helpers.
 *
 * Durations always come from `hrtime(true)`, which is `CLOCK_MONOTONIC` on Linux and is
 * shared by every process on the host. `uptime()` reads the same clock base from /proc and
 * exists so a child can report that its monotonic clock agrees with the host's, which is
 * what makes cross-process subtraction defensible on a single host.
 */
final class Clock
{
    public static function monotonicNs(): int
    {
        return \hrtime(true);
    }

    public static function wall(): float
    {
        return \microtime(true);
    }

    /**
     * Seconds since boot, from /proc/uptime. Zero when /proc/uptime is unavailable.
     */
    public static function uptime(): float
    {
        $contents = @\file_get_contents('/proc/uptime');
        if (!\is_string($contents)) {
            return 0.0;
        }

        $parts = \preg_split('/\s+/', \trim($contents));

        return isset($parts[0]) ? (float) $parts[0] : 0.0;
    }

    /**
     * A single observation of all three clocks.
     *
     * @return array{monotonic_ns: int, wall: float, uptime: float}
     */
    public static function anchor(): array
    {
        return [
            'monotonic_ns' => self::monotonicNs(),
            'wall' => self::wall(),
            'uptime' => self::uptime(),
        ];
    }

    public static function nsToMs(int $nanoseconds): float
    {
        return $nanoseconds / 1_000_000;
    }

    /**
     * Difference between two monotonic readings, in milliseconds.
     */
    public static function elapsedMs(int $startNs, int $endNs): float
    {
        return self::nsToMs($endNs - $startNs);
    }
}
