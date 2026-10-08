<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

abstract readonly class BenchmarkMessage
{
    public function __construct(public string $id, public Phase $phase, public string $payload, public bool $followUp, public int $workMilliseconds = 0)
    {
        if ($workMilliseconds < 0) {
            throw new \InvalidArgumentException('Handler work must not be negative.');
        }
    }
}
