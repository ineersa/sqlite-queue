<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Psr\Clock\ClockInterface;

final readonly class NativeQueueClock implements QueueClock
{
    public function __construct(private ClockInterface $clock)
    {
    }

    public function now(): int
    {
        return (int) $this->clock->now()->format('Uv');
    }
}
