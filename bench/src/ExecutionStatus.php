<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum ExecutionStatus: string
{
    case Scheduled = 'scheduled';
    case Complete = 'complete';
    case Failed = 'failed';
}
