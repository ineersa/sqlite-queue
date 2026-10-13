<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Sqlite\NativeQueueClock;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerOperation;
use Symfony\Component\Clock\NativeClock;

/** The driver commits a valid mutation before returning deliberately invalid application data. */
final class MalformedQueueWorkerOperation extends QueueWorkerOperation
{
    public static function create(SqlExecutor $sql): self
    {
        return new self($sql, new NativeQueueClock(new NativeClock()));
    }

    public function run(array $parameters): OperationResultDTO
    {
        $result = parent::run($parameters);

        return new OperationResultDTO($result->outcome, ['id' => 'invalid identity']);
    }
}
