<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum IntegrityStatus: string
{
    case Pass = 'pass';
    case Fail = 'fail';
    case Unknown = 'unknown';
}
