<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Terminal status for one worker exchange. */
enum SqliteWorkerStatusEnum: string
{
    case Ok = 'ok';
    case Domain = 'domain';
    case Failure = 'failure';
}
