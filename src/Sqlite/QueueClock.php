<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

interface QueueClock
{
    /** Unix wall-clock milliseconds, sampled inside the acquired transaction. */
    public function now(): int;
}
