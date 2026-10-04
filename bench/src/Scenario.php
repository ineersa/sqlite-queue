<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Scenario: string
{
    case Roundtrip = 'roundtrip';
    case Idle = 'idle';
    case FixedRate = 'fixed-rate';
    case Application = 'application';
    case Delayed = 'delayed';
    case Retention = 'retention';

    /** @return list<self> */
    public static function core(): array
    {
        return [self::Roundtrip, self::Idle, self::FixedRate, self::Application, self::Delayed, self::Retention];
    }
}
