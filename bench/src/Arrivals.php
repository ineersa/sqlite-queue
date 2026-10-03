<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Planned arrivals never shift when service or the generator falls behind. */
final readonly class Arrivals
{
    public function __construct(private int $start, private int $end, private float $rate)
    {
        if ($start < 0) {
            throw new \InvalidArgumentException('Arrival start must be a nonnegative monotonic timestamp.');
        }
        if ($end <= $start) {
            throw new \InvalidArgumentException('Arrival window must be positive.');
        }
        if (!is_finite($rate) || $rate <= 0) {
            throw new \InvalidArgumentException('Arrival rate must be positive and finite.');
        }
    }

    public function at(int $index): int
    {
        if ($index < 0) {
            throw new \InvalidArgumentException('Arrival index must not be negative.');
        }

        $offset = floor($index * 1e9 / $this->rate);
        if (!is_finite($offset) || $offset >= (float) \PHP_INT_MAX) {
            throw new \InvalidArgumentException('Arrival offset exceeds the native integer range.');
        }
        $nanoseconds = (int) $offset;
        if ($nanoseconds > \PHP_INT_MAX - $this->start) {
            throw new \InvalidArgumentException('Arrival timestamp exceeds the native integer range.');
        }

        return $this->start + $nanoseconds;
    }

    public function count(): int
    {
        return $this->countBefore($this->end);
    }

    /** @return array{indices: list<int>, next: int, overflow: int} */
    public function admit(int $now, int $next, int $slots): array
    {
        if ($slots < 0 || $next < 0) {
            throw new \InvalidArgumentException('Arrival cursor and slots must be nonnegative.');
        }
        $due = $now < $this->start ? 0 : $this->countBefore($now >= $this->end - 1 ? $this->end : $now + 1);
        $available = max(0, $due - $next);
        $accepted = min($slots, $available);
        $indices = [];
        for ($index = $next; $index < $next + $accepted; ++$index) {
            $indices[] = $index;
        }

        return ['indices' => $indices, 'next' => max($next, $due), 'overflow' => $available - $accepted];
    }

    private function countBefore(int $end): int
    {
        $window = $end - $this->start;
        $estimate = ceil($window / 1e9 * $this->rate);
        if (!is_finite($estimate) || $estimate >= (float) \PHP_INT_MAX) {
            throw new \InvalidArgumentException('Arrival count exceeds the native integer range.');
        }
        $count = (int) $estimate;
        // Correct floating ceil against the actual floored schedule, not another rounded product.
        while ($count > 0 && !$this->offsetBefore($count - 1, $window)) {
            --$count;
        }
        while ($this->offsetBefore($count, $window)) {
            if (\PHP_INT_MAX === $count) {
                throw new \InvalidArgumentException('Arrival count exceeds the native integer range.');
            }
            ++$count;
        }

        return $count;
    }

    private function offsetBefore(int $index, int $window): bool
    {
        $offset = floor($index * 1e9 / $this->rate);
        // Outside representable offsets cannot lie inside a native-integer monotonic window.
        if (!is_finite($offset) || $offset >= (float) \PHP_INT_MAX) {
            return false;
        }

        return (int) $offset < $window;
    }
}
