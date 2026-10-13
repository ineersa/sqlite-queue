<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Expected send validation failures, returned only after the driver rolls back. */
enum QueueValidationError: string
{
    case NegativeDelay = 'negative_delay';
    case DeadlineOverflow = 'deadline_overflow';

    public function message(): string
    {
        return match ($this) {
            self::NegativeDelay => 'Duration must be nonnegative milliseconds.',
            self::DeadlineOverflow => 'Deadline exceeds the supported timestamp range.',
        };
    }
}
