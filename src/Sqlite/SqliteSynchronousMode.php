<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Supported WAL durability policies for queue storage. */
enum SqliteSynchronousMode: string
{
    case Normal = 'normal';
    case Full = 'full';
}
