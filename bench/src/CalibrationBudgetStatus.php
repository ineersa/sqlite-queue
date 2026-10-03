<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum CalibrationBudgetStatus: string
{
    case PartialPairs = 'partial-pairs';
    case InsufficientEvidence = 'insufficient-duration-or-repetitions';
    case Exceeded = 'exceeds-proposed-budget';
    case Within = 'within-proposed-budget';
}
