<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Finite diagnostic categories for internal worker replies. */
enum SqliteWorkerErrorCategoryEnum: string
{
    case Domain = 'domain';
    case Failure = 'failure';
}
