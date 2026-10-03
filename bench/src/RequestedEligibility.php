<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Requested delay measured at the public send boundary, not the persisted deadline. */
final class RequestedEligibility
{
    /** @return array{anchor_monotonic_ns: int, anchor_wall_ms: int, delay_ms: int, requested_monotonic_ns: int, requested_wall_ms: int} */
    public static function at(int $monotonicNanoseconds, int $wallMilliseconds, int $delayMilliseconds): array
    {
        if ($delayMilliseconds < 0) {
            throw new \InvalidArgumentException('Requested delay must not be negative.');
        }

        return ['anchor_monotonic_ns' => $monotonicNanoseconds, 'anchor_wall_ms' => $wallMilliseconds, 'delay_ms' => $delayMilliseconds, 'requested_monotonic_ns' => $monotonicNanoseconds + $delayMilliseconds * 1000000, 'requested_wall_ms' => $wallMilliseconds + $delayMilliseconds];
    }
}
