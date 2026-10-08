<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Parent proxy lifecycle for one PDO worker channel. */
enum SqliteWorkerLifecycleEnum
{
    case Open;
    case Closing;
    case Failed;
    case Closed;
}
