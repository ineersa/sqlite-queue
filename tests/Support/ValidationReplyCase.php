<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

enum ValidationReplyCase: string
{
    case Commit = 'commit-validation';
    case UnknownCode = 'unknown-code';
    case NonStringCode = 'non-string-code';
    case SuccessData = 'success-data';
    case NonInsert = 'non-insert';
    case NegativeDelay = 'negative-delay';
    case DeadlineOverflow = 'deadline-overflow';
}
