<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Sqlite\NativeQueueClock;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerOperation;
use Symfony\Component\Clock\MockClock;

/** Execute real queue SQL, then give valid success data the wrong transaction outcome. */
final class OutcomeMismatchQueueWorkerOperation extends QueueWorkerOperation
{
    public static function create(SqlExecutor $sql): self
    {
        return new self($sql, new NativeQueueClock(new MockClock('@2')));
    }

    public function run(array $parameters): OperationResultDTO
    {
        $result = parent::run($parameters);
        $outcome = Outcome::Rollback === $result->outcome ? Outcome::Commit : Outcome::Rollback;

        return new OperationResultDTO($outcome, $result->data);
    }
}
