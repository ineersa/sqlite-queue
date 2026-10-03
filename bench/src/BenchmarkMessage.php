<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

abstract readonly class BenchmarkMessage
{
    // Closed-loop arrivals have no independent planned time. Zero work/delay is the verification-only profile.
    public function __construct(public string $id, public Phase $phase, public string $payload, public bool $followUp, public ?int $scheduledNs = null, public int $workMilliseconds = 0, public int $delayMilliseconds = 0)
    {
        if ($workMilliseconds < 0) {
            throw new \InvalidArgumentException('Handler work must not be negative.');
        }
        if ($delayMilliseconds < 0) {
            throw new \InvalidArgumentException('Requested delay must not be negative.');
        }
    }
}
