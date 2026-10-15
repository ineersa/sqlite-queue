<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Expected duration validation failures, returned only after the driver rolls back. */
enum QueueValidationError: string
{
    case NegativeDelay = 'negative_delay';
    case NegativeKeepalive = 'negative_keepalive';
    case DeadlineOverflow = 'deadline_overflow';

    public function message(): string
    {
        return match ($this) {
            self::NegativeDelay, self::NegativeKeepalive => 'Duration must be nonnegative milliseconds.',
            self::DeadlineOverflow => 'Deadline exceeds the supported timestamp range.',
        };
    }
}
