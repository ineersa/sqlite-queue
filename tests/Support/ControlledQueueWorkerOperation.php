<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerOperation;

final class ControlledQueueWorkerOperation extends QueueWorkerOperation
{
    public static function create(SqlExecutor $sql): self
    {
        $path = getenv(ClockFile::ENVIRONMENT_VARIABLE);
        if (false === $path || '' === $path) {
            throw new \RuntimeException('Controlled worker clock path is required.');
        }

        return new self($sql, new FileQueueClock($path));
    }
}
