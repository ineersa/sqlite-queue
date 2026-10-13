<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Sqlite\NativeQueueClock;
use Ineersa\SqliteQueue\Sqlite\QueueValidationError;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerOperation;
use Symfony\Component\Clock\MockClock;

final class ValidationReplyQueueWorkerOperation extends QueueWorkerOperation
{
    public static function create(SqlExecutor $sql): self
    {
        return new self($sql, new NativeQueueClock(new MockClock('@2')));
    }

    public function run(array $parameters): OperationResultDTO
    {
        if ('jobs' === $parameters['queue']) {
            return parent::run($parameters);
        }
        $case = ValidationReplyCase::from($parameters['queue']);
        $data = ['validation_error' => match ($case) {
            ValidationReplyCase::UnknownCode => 'unsupported-validation-code',
            ValidationReplyCase::NonStringCode => 1,
            ValidationReplyCase::DeadlineOverflow => QueueValidationError::DeadlineOverflow->value,
            default => QueueValidationError::NegativeDelay->value,
        }];
        if (ValidationReplyCase::SuccessData === $case) {
            $data['id'] = 1;
        }

        return new OperationResultDTO(ValidationReplyCase::Commit === $case ? Outcome::Commit : Outcome::Rollback, $data);
    }
}
