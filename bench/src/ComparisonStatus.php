<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum ComparisonStatus: string
{
    case Inconclusive = 'inconclusive';
    case Win = 'win';
    case Neutral = 'neutral';
    case Regression = 'regression';
}
