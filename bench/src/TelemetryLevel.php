<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum TelemetryLevel: string
{
    case Detailed = 'detailed';
    case Essential = 'essential';
}
