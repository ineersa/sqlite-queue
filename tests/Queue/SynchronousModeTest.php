<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteTransactionError;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\OperationExecutor;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerProtocol;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\ConfigurationWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\ProcessTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class SynchronousModeTest extends ProcessTestCase
{
    public static function allowedModes(): iterable
    {
        yield 'normal' => [SqliteSynchronousMode::Normal, 1];
        yield 'full' => [SqliteSynchronousMode::Full, 2];
    }

    public function testOpenDefaultsToNormal(): void
    {
        $storage = SqliteQueueStorage::open($this->database->path(), workerOperation: ConfigurationWorkerOperation::class);
        try {
            $this->assertSame(SqliteSynchronousMode::Normal, $storage->synchronousMode());
            $this->assertSame(1, $this->synchronousPragma($storage));
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testOpenSelectsEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $storage = SqliteQueueStorage::open($this->database->path(), $mode, workerOperation: ConfigurationWorkerOperation::class);
        try {
            $this->assertSame($mode, $storage->synchronousMode());
            $this->assertSame($expected, $this->synchronousPragma($storage));
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testExecutorRejectsChangingEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $storage = SqliteQueueStorage::open($this->database->path(), $mode, workerOperation: ConfigurationWorkerOperation::class);
        try {
            $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
            $this->assertInstanceOf(SqliteConnection::class, $connection);
            try {
                $connection->query('PRAGMA synchronous = '.(SqliteSynchronousMode::Normal === $mode ? 'FULL' : 'NORMAL'))->close();
                $this->fail('A legacy query must not bypass executor ownership.');
            } catch (SqliteTransactionError $error) {
                $this->assertSame('A worker operation executor owns the connection', $error->getMessage());
            }
            $this->assertSame($mode, $storage->synchronousMode());
            $this->assertSame($expected, $this->synchronousPragma($storage));
        } finally {
            $storage->close();
        }
    }

    public function testOpenRejectsMemoryPathBeforeAcquisition(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        SqliteQueueStorage::open(':memory:');
    }

    private function synchronousPragma(SqliteQueueStorage $storage): int
    {
        $executor = (new \ReflectionProperty($storage, 'executor'))->getValue($storage);
        $this->assertInstanceOf(OperationExecutor::class, $executor);

        return $executor->run(QueueWorkerProtocol::OPERATION_NAME, [], Mode::ReadOnly)->data['value'];
    }
}
