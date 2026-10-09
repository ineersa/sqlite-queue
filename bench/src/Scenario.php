<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Scenario: string
{
    case Roundtrip = 'roundtrip';
    case Idle = 'idle';
    case MultiQueue = 'multi-queue';
    case Application = 'application';
    case Retention = 'retention';
    case Concurrent = 'concurrent';

    /** @return list<self> */
    public static function core(): array
    {
        return [self::Roundtrip, self::Idle, self::Application, self::Retention, self::Concurrent];
    }

    /** Single-queue and multi-queue pickup both separate an empty interval from isolated arrivals. */
    public function isPickup(): bool
    {
        return self::Idle === $this || self::MultiQueue === $this;
    }
}
