<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Ineersa\SqliteQueue\ValueObject\QueueName;

/**
 * Derived readiness state for one watched queue.
 *
 * A watch exists only while the queue is observed. `querying` and `dirty` coordinate
 * in-flight refreshes; object identity protects replacement watches from stale results.
 * Deadline timer fields are nullable because a watch may have no armed timer.
 *
 * @internal
 */
final class QueueNotifierWatch
{
    public bool $querying = false;
    public bool $dirty = false;
    public ?string $timerId = null;
    public ?int $timerReadyAt = null;

    public function __construct(public readonly QueueName $queue)
    {
    }
}
