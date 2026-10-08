<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Fixed-size observed-memory accumulator. Null endpoints mean no available metric sample. */
final class MemoryTrend
{
    private int $count = 0;
    private ?int $first = null;
    private ?int $last = null;
    private ?int $minimum = null;
    private ?int $maximum = null;
    private ?int $firstHistorical = null;
    private ?int $lastHistorical = null;

    public function observe(int $bytes, int $historical): void
    {
        if (0 === $this->count) {
            $this->first = $bytes;
            $this->firstHistorical = $historical;
        }
        ++$this->count;
        $this->last = $bytes;
        $this->lastHistorical = $historical;
        $this->minimum = null === $this->minimum ? $bytes : min($this->minimum, $bytes);
        $this->maximum = null === $this->maximum ? $bytes : max($this->maximum, $bytes);
    }

    /** @return array<string, mixed> */
    public function summary(): array
    {
        $delta = null === $this->first || null === $this->last ? null : $this->last - $this->first;
        $work = null === $this->firstHistorical || null === $this->lastHistorical ? 0 : $this->lastHistorical - $this->firstHistorical;

        return ['count' => $this->count, 'first' => $this->first, 'last' => $this->last, 'minimum' => $this->minimum, 'maximum' => $this->maximum, 'first_historical' => $this->firstHistorical, 'last_historical' => $this->lastHistorical, 'delta_bytes' => $delta, 'endpoint_bytes_per_historical_completion' => $work > 0 && null !== $delta ? $delta / $work : null, 'coverage' => 0 === $this->count ? 'unavailable' : 'matched-state observed endpoints and range, not regression or leak verdict'];
    }
}
