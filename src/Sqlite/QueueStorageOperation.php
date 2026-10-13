<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

enum QueueStorageOperation: string
{
    case Insert = 'insert';
    case Claim = 'claim';
    case Settle = 'settle';
    case Eligibility = 'eligibility';
}
