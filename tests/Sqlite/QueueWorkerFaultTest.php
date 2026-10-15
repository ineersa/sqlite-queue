<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Amp\ByteStream\StreamChannel;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\Internal\ConnectionLeases;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Ineersa\SqliteQueue\Sqlite\QueueStorageOperation;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerProtocol;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\ClockFile;
use Ineersa\SqliteQueue\Tests\Support\ControlledQueueWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\ProcessTestCase;
use Ineersa\SqliteQueue\Tests\Support\QueueFaultContextFactory;
use Ineersa\SqliteQueue\Tests\Support\WorkerFaultAction;
use PHPUnit\Framework\Attributes\DataProvider;

use function Amp\async;
use function Amp\Socket\listen;

final class QueueWorkerFaultTest extends ProcessTestCase
{
    private const int SAFETY_SECONDS = 5;
    private const int NOW_MILLISECONDS = 2_000;
    private const int VISIBILITY_MILLISECONDS = 5_000;
    private const string OWNER = 'fault-owner';
    private const string EPOCH = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    private const string TOKEN = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';

    public static function faults(): iterable
    {
        foreach ([QueueStorageOperation::Claim, QueueStorageOperation::Settle, QueueStorageOperation::Keepalive] as $operation) {
            foreach ([WorkerFaultAction::ExitBeforeCommit, WorkerFaultAction::LostReply, WorkerFaultAction::MalformedReply] as $action) {
                yield $operation->value.' '.$action->value => [$operation, $action];
            }
        }
    }

    #[DataProvider('faults')]
    public function testTransactionFaultsPreserveTheExpectedQueueState(QueueStorageOperation $operation, WorkerFaultAction $action): void
    {
        $path = $this->database->path();
        ClockFile::configure($path, self::NOW_MILLISECONDS);
        $seed = SqliteQueueStorage::open($path, workerOperation: ControlledQueueWorkerOperation::class);
        try {
            $id = $seed->insert('jobs', 'payload', '', 0);
            if (QueueStorageOperation::Settle === $operation || QueueStorageOperation::Keepalive === $operation) {
                $this->assertNotNull($seed->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS));
            }
        } finally {
            $seed->close();
        }
        $this->instrumentWorker($this->database->directory());
        $server = listen('127.0.0.1:0');
        $factory = new QueueFaultContextFactory(new ProcessContextFactory(), $this->database->directory(), 'tcp://'.$server->getAddress()->toString());
        $connection = (new SqliteConnector($factory))->connect((new SqliteConfig($path))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withWorkerOperations([QueueWorkerProtocol::OPERATION_NAME => ControlledQueueWorkerOperation::class]));
        $storage = SqliteQueueStorage::fromConnection($connection, SqliteSynchronousMode::Normal);
        try {
            $socket = $server->accept(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertNotNull($socket);
            $control = new StreamChannel($socket, $socket);
            try {
                $confirmations = 0;
                $invoke = static fn () => match ($operation) {
                    QueueStorageOperation::Claim => $storage->claim('jobs', self::OWNER, self::EPOCH, self::TOKEN, self::VISIBILITY_MILLISECONDS),
                    QueueStorageOperation::Settle => $storage->settle($id, self::TOKEN, self::OWNER, self::EPOCH),
                    QueueStorageOperation::Keepalive => $storage->keepalive($id, self::TOKEN, self::OWNER, self::EPOCH, self::VISIBILITY_MILLISECONDS, self::VISIBILITY_MILLISECONDS * 2),
                    default => throw new \LogicException('Fault fixture requires claim or settlement.'),
                };
                $future = async(static function () use ($invoke, &$confirmations): void {
                    $invoke();
                    ++$confirmations;
                });
                $future->ignore();
                $this->assertSame('mutated', $control->receive(new TimeoutCancellation(self::SAFETY_SECONDS)));
                $this->assertFalse($future->isComplete());
                $this->assertDatabaseState($operation, false);
                if (WorkerFaultAction::ExitBeforeCommit !== $action) {
                    $control->send(WorkerFaultAction::Continue->value);
                    $this->assertSame('committed', $control->receive(new TimeoutCancellation(self::SAFETY_SECONDS)));
                    $this->assertFalse($future->isComplete());
                    $this->assertDatabaseState($operation, true);
                }
                $control->send($action->value);
                try {
                    $future->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                    $this->fail('A fault must not produce successful confirmation.');
                } catch (SqliteConnectionException) {
                    $this->assertTrue($connection->isClosed());
                }
                $this->assertSame(0, $confirmations);
                $executor = (new \ReflectionProperty($storage, 'executor'))->getValue($storage);
                $this->assertTrue($executor->isClosed());
                $leases = (new \ReflectionProperty($connection, 'leases'))->getValue($connection);
                $this->assertInstanceOf(ConnectionLeases::class, $leases);
                $this->assertFalse($leases->isBusy());
                try {
                    $invoke();
                    $this->fail('Failed storage must not replay or dispatch another operation.');
                } catch (SqliteConnectionException) {
                    $this->addToAssertionCount(1);
                }
                $this->assertSame(1, $factory->starts);
                $this->assertDatabaseState($operation, WorkerFaultAction::ExitBeforeCommit !== $action, acquireWrite: true);
            } finally {
                $control->close();
            }
        } finally {
            $storage->close();
            $server->close();
        }
    }

    private function assertDatabaseState(QueueStorageOperation $operation, bool $committed, bool $acquireWrite = false): void
    {
        $observer = new \SQLite3($this->database->path());
        $observer->enableExceptions(true);
        // A successful writer proves lock release. Busy timeout only aborts leaked ownership.
        $observer->busyTimeout(self::SAFETY_SECONDS * 1000);
        try {
            if ($acquireWrite) {
                $observer->exec('BEGIN IMMEDIATE');
            }
            $rows = $observer->querySingle('SELECT count(*) FROM queue_messages');
            if (QueueStorageOperation::Settle === $operation && $committed) {
                $this->assertSame(0, $rows);
            } else {
                $this->assertSame(1, $rows);
                $row = $observer->querySingle('SELECT body, owner_id, broker_epoch, reservation_token, reserved_until FROM queue_messages', true);
                $this->assertSame('payload', $row['body']);
                $reserved = QueueStorageOperation::Settle === $operation || QueueStorageOperation::Keepalive === $operation || $committed;
                $this->assertSame($reserved ? self::OWNER : null, $row['owner_id']);
                $this->assertSame($reserved ? self::EPOCH : null, $row['broker_epoch']);
                $this->assertSame($reserved ? self::TOKEN : null, $row['reservation_token']);
                $duration = QueueStorageOperation::Keepalive === $operation && $committed ? self::VISIBILITY_MILLISECONDS * 2 : self::VISIBILITY_MILLISECONDS;
                $this->assertSame($reserved ? self::NOW_MILLISECONDS + $duration : null, $row['reserved_until']);
            }
            if ($acquireWrite) {
                $observer->exec('ROLLBACK');
            }
        } finally {
            $observer->close();
        }
    }

    private function instrumentWorker(string $directory): void
    {
        $connector = (new \ReflectionClass(SqliteConnector::class))->getFileName();
        $this->assertIsString($connector);
        $sourceDirectory = \dirname($connector).'/Internal';
        $runtime = file_get_contents($sourceDirectory.'/WorkerProcess.php');
        $worker = file_get_contents($sourceDirectory.'/worker.php');
        $this->assertIsString($runtime);
        $this->assertIsString($worker);
        $mutation = "            \$result = \$operation->run(\$parameters);\n            if ((\$mode ===";
        $runtime = $this->hook($runtime, $mutation, "            \$result = \$operation->run(\$parameters);\n            \$GLOBALS['queueFaultBarrier']->at('mutated');\n            if ((\$mode ===");
        $commit = "                \$this->workerControl(\$result->outcome === \\Fabpot\\Amp\\Sqlite\\Worker\\Outcome::Commit ? 'COMMIT' : 'ROLLBACK');";
        $runtime = $this->hook($runtime, $commit, $commit."\n                if (\$result->outcome === \\Fabpot\\Amp\\Sqlite\\Worker\\Outcome::Commit) {\n                    \$GLOBALS['queueFaultBarrier']->at('committed');\n                }");
        $reply = "            \$channel->send(['id' => \$id, 'value' => \$worker->handle(\$request)]);";
        $worker = $this->hook($worker, $reply, "            \$value = \$worker->handle(\$request);\n            if (\$request['operation'] === 'executeWorkerOperation') {\n                \$value = \$GLOBALS['queueFaultBarrier']->reply(\$value);\n            }\n            \$channel->send(['id' => \$id, 'value' => \$value]);");
        $this->assertNotFalse(file_put_contents($directory.'/WorkerProcess.php', $runtime));
        $this->assertNotFalse(file_put_contents($directory.'/worker.php', $worker));
    }

    private function hook(string $source, string $anchor, string $replacement): string
    {
        $this->assertSame(1, substr_count($source, $anchor), 'Driver source hook must match exactly once.');

        return str_replace($anchor, $replacement, $source);
    }
}
