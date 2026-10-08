<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum AccountingStatus: string
{
    case Complete = 'complete';
    case Partial = 'partial';
    case Unavailable = 'unavailable';
}
