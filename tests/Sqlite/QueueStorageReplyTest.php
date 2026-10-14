<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Amp\Future;
use Amp\Parallel\Context\Internal\AbstractContext;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\Process\Process;
use Fabpot\Amp\Sqlite\Internal\WorkerChannel;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode as DriverSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteWorkerOperationConnection;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Ineersa\SqliteQueue\Sqlite\QueueStorageOperation;
use Ineersa\SqliteQueue\Sqlite\QueueValidationError;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerProtocol;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\ClockFile;
use Ineersa\SqliteQueue\Tests\Support\ControlledQueueWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\OutcomeMismatchQueueWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\ProcessTestCase;
use Ineersa\SqliteQueue\Tests\Support\RecordingContextFactory;
use Ineersa\SqliteQueue\Tests\Support\ValidationReplyCase;
use Ineersa\SqliteQueue\Tests\Support\ValidationReplyQueueWorkerOperation;
use PHPUnit\Framework\Attributes\DataProvider;

final class QueueStorageReplyTest extends ProcessTestCase
{
    private const int NOW_MILLISECONDS = 2_000;
    private const int VISIBILITY_MILLISECONDS = 5_000;
    private const string OWNER = 'reply-owner';
    private const string EPOCH = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    private const string TOKEN = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';

    public static function successfulMutations(): iterable
    {
        yield 'insert' => [QueueStorageOperation::Insert, 'Worker insert reply must have commit outcome.'];
        yield 'populated claim' => [QueueStorageOperation::Claim, 'Worker populated claim reply must have commit outcome.'];
        yield 'settled' => [QueueStorageOperation::Settle, 'Worker settlement reply must have commit outcome.'];
        yield 'renewed' => [QueueStorageOperation::Keepalive, 'Worker keepalive reply must have commit outcome.'];
    }

    #[DataProvider('successfulMutations')]
    public function testRollbackCannotConfirmSuccessfulMutation(QueueStorageOperation $operation, string $diagnostic): void
    {
        $seed = $this->seed();
        try {
            $id = QueueStorageOperation::Insert === $operation ? 0 : $seed->insert('jobs', 'payload', '', 0);
            if (QueueStorageOperation::Settle === $operation || QueueStorageOperation::Keepalive === $operation) {
                $this->assertNotNull($seed->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS));
            }
        } finally {
            $seed->close();
        }
        $before = $this->rows();
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(OutcomeMismatchQueueWorkerOperation::class);
        try {
            try {
                match ($operation) {
                    QueueStorageOperation::Insert => $storage->insert('jobs', 'must roll back', '', 0),
                    QueueStorageOperation::Claim => $storage->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS),
                    QueueStorageOperation::Settle => $storage->settle($id, self::TOKEN, self::OWNER, self::EPOCH),
                    QueueStorageOperation::Keepalive => $storage->keepalive($id, self::TOKEN, self::OWNER, self::EPOCH, self::VISIBILITY_MILLISECONDS, 10_000),
                    default => throw new \LogicException('Expected a mutation.'),
                };
                $this->fail('Rollback success data must not confirm a mutation.');
            } catch (\RuntimeException $error) {
                $this->assertSame($diagnostic.' Outcome may be unknown; storage is closed.', $error->getMessage());
            }
            $this->assertFailedWithoutReplay($storage, $connection, $requestBaseline);
            $this->assertSame($before, $this->rows(), 'Real INSERT, reservation UPDATE or DELETE must have rolled back.');
        } finally {
            $storage->close();
        }
    }

    public function testCommitCannotConfirmRejectedKeepalive(): void
    {
        $seed = $this->seed();
        $seed->close();
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(OutcomeMismatchQueueWorkerOperation::class);
        try {
            try {
                $storage->keepalive(1, self::TOKEN, self::OWNER, self::EPOCH, self::VISIBILITY_MILLISECONDS, 0);
                $this->fail('Rejected renewal must have rollback outcome.');
            } catch (\RuntimeException $error) {
                $this->assertSame('Worker rejected keepalive reply must have rollback outcome. Outcome may be unknown; storage is closed.', $error->getMessage());
            }
            $this->assertFailedWithoutReplay($storage, $connection, $requestBaseline);
            $this->assertSame([], $this->rows());
        } finally {
            $storage->close();
        }
    }

    public function testNegativeKeepaliveAtStorageBoundaryRollsBackWithoutClosing(): void
    {
        $storage = $this->seed();
        try {
            $id = $storage->insert('jobs', 'payload', '', 0);
            $storage->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS);
            $before = $this->rows();
            try {
                $storage->keepalive($id, self::TOKEN, self::OWNER, self::EPOCH, self::VISIBILITY_MILLISECONDS, -1);
                $this->fail('Negative duration must be an application validation failure.');
            } catch (\InvalidArgumentException $error) {
                $this->assertSame(QueueValidationError::NegativeKeepalive->message(), $error->getMessage());
            }
            $this->assertSame($before, $this->rows());
            $this->assertSame(self::NOW_MILLISECONDS + self::VISIBILITY_MILLISECONDS, $storage->keepalive($id, self::TOKEN, self::OWNER, self::EPOCH, self::VISIBILITY_MILLISECONDS, 0));
        } finally {
            $storage->close();
        }
    }

    public function testCommitCannotConfirmEmptyClaim(): void
    {
        $seed = $this->seed();
        $seed->close();
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(OutcomeMismatchQueueWorkerOperation::class);
        try {
            try {
                $storage->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS);
                $this->fail('An empty claim must have rollback outcome.');
            } catch (\RuntimeException $error) {
                $this->assertSame('Worker empty claim reply must have rollback outcome. Outcome may be unknown; storage is closed.', $error->getMessage());
            }
            $this->assertFailedWithoutReplay($storage, $connection, $requestBaseline);
            $this->assertSame([], $this->rows());
        } finally {
            $storage->close();
        }
    }

    public static function malformedValidationReplies(): iterable
    {
        yield 'commit with validation' => [ValidationReplyCase::Commit, 'Worker validation reply must have rollback outcome.'];
        yield 'unknown code' => [ValidationReplyCase::UnknownCode, 'Worker validation error code is unknown.'];
        yield 'non-string code' => [ValidationReplyCase::NonStringCode, 'Worker validation error code must be a string.'];
        yield 'validation plus success data' => [ValidationReplyCase::SuccessData, 'Worker validation reply must not contain success data.'];
        yield 'validation on claim' => [ValidationReplyCase::NonInsert, 'Worker validation reply is only supported for insert or keepalive.'];
    }

    #[DataProvider('malformedValidationReplies')]
    public function testMalformedValidationReplyFailsClosed(ValidationReplyCase $case, string $diagnostic): void
    {
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(ValidationReplyQueueWorkerOperation::class);
        try {
            try {
                if (ValidationReplyCase::NonInsert === $case) {
                    $storage->claim($case->value, self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS);
                } else {
                    $storage->insert($case->value, 'payload', '', 0);
                }
                $this->fail('Malformed validation data must not become an expected argument error.');
            } catch (\RuntimeException $error) {
                $this->assertNotInstanceOf(\InvalidArgumentException::class, $error);
                $this->assertSame($diagnostic.' Outcome may be unknown; storage is closed.', $error->getMessage());
            }
            $this->assertFailedWithoutReplay($storage, $connection, $requestBaseline);
            $this->assertSame([], $this->rows());
        } finally {
            $storage->close();
        }
    }

    public static function supportedValidationReplies(): iterable
    {
        yield 'negative delay' => [ValidationReplyCase::NegativeDelay, QueueValidationError::NegativeDelay];
        yield 'deadline overflow' => [ValidationReplyCase::DeadlineOverflow, QueueValidationError::DeadlineOverflow];
    }

    #[DataProvider('supportedValidationReplies')]
    public function testSupportedValidationReplyKeepsStorageHealthy(ValidationReplyCase $case, QueueValidationError $expected): void
    {
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(ValidationReplyQueueWorkerOperation::class);
        try {
            try {
                $storage->insert($case->value, 'invalid request', '', 0);
                $this->fail('Expected an explicit argument validation error.');
            } catch (\InvalidArgumentException $error) {
                $this->assertSame($expected->message(), $error->getMessage());
            }
            $this->assertFalse($connection->isClosed());
            $this->assertSame([], $this->rows());
            $this->assertGreaterThan(0, $storage->insert('jobs', 'healthy', '', 0));
            $this->assertSame($requestBaseline + 2, $this->nextRequestId($connection));
            $this->assertCount(1, $this->rows());
        } finally {
            $storage->close();
        }
    }

    public function testNegativeDelayAtStorageBoundaryRollsBackWithoutClosing(): void
    {
        ClockFile::configure($this->database->path(), self::NOW_MILLISECONDS);
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(ControlledQueueWorkerOperation::class);
        try {
            try {
                $storage->insert('jobs', 'negative delay', '', -1);
                $this->fail('Storage must validate delay independently of Queue.');
            } catch (\InvalidArgumentException $error) {
                $this->assertSame(QueueValidationError::NegativeDelay->message(), $error->getMessage());
            }
            $this->assertSame([], $this->rows());
            $this->assertFalse($connection->isClosed());
            $this->assertGreaterThan(0, $storage->insert('jobs', 'healthy', '', 0));
            $this->assertSame($requestBaseline + 2, $this->nextRequestId($connection));
        } finally {
            $storage->close();
        }
    }

    public function testCorruptStoredDataWithNullNativeCodesIsNotArgumentValidation(): void
    {
        $storage = $this->seed();
        try {
            $storage->insert('jobs', 'payload', '', 0);
            $observer = new \SQLite3($this->database->path());
            $observer->enableExceptions(true);
            try {
                $observer->exec('PRAGMA ignore_check_constraints = ON');
                $observer->exec('UPDATE queue_messages SET body = 42');
            } finally {
                $observer->close();
            }
            try {
                $storage->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS);
                $this->fail('Corrupt stored data must retain its SQL error type.');
            } catch (SqliteQueryError $error) {
                $this->assertSame('Stored message is missing or has invalid payload types.', $error->getMessage());
                $this->assertNull($error->getResultCode());
                $this->assertNull($error->getExtendedResultCode());
            }
            $this->assertNull($this->rows()[0]['reserved_until']);
            $this->assertGreaterThan(0, $storage->insert('jobs', 'still healthy', '', 0));
        } finally {
            $storage->close();
        }
    }

    public function testIdleCloseSendsCloseAndCompletesContextJoinExactlyOnce(): void
    {
        ClockFile::configure($this->database->path(), self::NOW_MILLISECONDS);
        [$storage, $context, $connection, $requestBaseline] = $this->openWithSpy(ControlledQueueWorkerOperation::class);
        try {
            $this->assertGreaterThan(0, $storage->insert('jobs', 'committed', '', 0));
            $beforeClose = $this->nextRequestId($connection);
            $resultProperty = new \ReflectionProperty(AbstractContext::class, 'result');
            $this->assertNull($resultProperty->getValue($context), 'Join must not have run during initialization or insert.');
            $process = (new \ReflectionProperty(ProcessContext::class, 'process'))->getValue($context);
            $this->assertInstanceOf(Process::class, $process);
            $this->assertTrue($process->isRunning());
            $storage->close();
            $this->assertTrue($connection->isClosed());
            $this->assertSame($beforeClose + 1, $this->nextRequestId($connection), 'Idle close must send exactly one close RPC.');
            $result = $resultProperty->getValue($context);
            $this->assertInstanceOf(Future::class, $result, 'The real AbstractContext join must initialize its cached result.');
            $this->assertTrue($result->isComplete(), 'Storage close must wait for the cached exit result.');
            $this->assertFalse($process->isRunning(), 'Storage close must reap the underlying process before returning.');
            $storage->close();
            $connection->close();
            $this->assertSame($beforeClose + 1, $this->nextRequestId($connection));
            $this->assertSame($result, $resultProperty->getValue($context));
            $this->assertFalse($process->isRunning());
            $this->assertCount(1, $this->rows());
        } finally {
            $storage->close();
        }
    }

    private function seed(): SqliteQueueStorage
    {
        ClockFile::configure($this->database->path(), self::NOW_MILLISECONDS);

        return SqliteQueueStorage::open($this->database->path(), workerOperation: ControlledQueueWorkerOperation::class);
    }

    /** @param class-string<Operation> $operation
     * @return array{SqliteQueueStorage, ProcessContext, SqliteWorkerOperationConnection, int}
     */
    private function openWithSpy(string $operation): array
    {
        $factory = new RecordingContextFactory(new ProcessContextFactory());
        $config = (new SqliteConfig($this->database->path()))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withSynchronousMode(DriverSynchronousMode::Normal)
            ->withWorkerOperations([QueueWorkerProtocol::OPERATION_NAME => $operation]);
        $connection = (new SqliteConnector($factory))->connect($config);
        $storage = SqliteQueueStorage::fromConnection($connection, SqliteSynchronousMode::Normal);
        $this->assertCount(1, $factory->contexts);

        return [$storage, $factory->contexts[0], $connection, $this->nextRequestId($connection)];
    }

    private function nextRequestId(SqliteWorkerOperationConnection $connection): int
    {
        $channel = (new \ReflectionProperty($connection, 'channel'))->getValue($connection);
        $this->assertInstanceOf(WorkerChannel::class, $channel);

        return (new \ReflectionProperty(WorkerChannel::class, 'nextRequestId'))->getValue($channel);
    }

    private function assertFailedWithoutReplay(SqliteQueueStorage $storage, SqliteWorkerOperationConnection $connection, int $requestBaseline): void
    {
        $this->assertTrue($connection->isClosed());
        $executor = (new \ReflectionProperty($storage, 'executor'))->getValue($storage);
        $this->assertTrue($executor->isClosed());
        // One application request followed by invalidReply's graceful close request.
        $this->assertSame($requestBaseline + 2, $this->nextRequestId($connection));
        try {
            $storage->insert('jobs', 'must not replay', '', 0);
            $this->fail('Failed storage must reject further work.');
        } catch (\RuntimeException $error) {
            $this->assertSame('Queue storage is closed.', $error->getMessage());
        }
        $this->assertSame($requestBaseline + 2, $this->nextRequestId($connection));
    }

    /** @return list<array<string, mixed>> */
    private function rows(): array
    {
        $observer = new \SQLite3($this->database->path());
        $observer->enableExceptions(true);
        try {
            $result = $observer->query('SELECT * FROM queue_messages ORDER BY id');
            $rows = [];
            while (false !== ($row = $result->fetchArray(\SQLITE3_ASSOC))) {
                $rows[] = $row;
            }
            $result->finalize();

            return $rows;
        } finally {
            $observer->close();
        }
    }
}
