<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Driver\DriverTestCase;
use Pdo\Sqlite;
use PHPUnit\Framework\Attributes\DataProvider;

final class SynchronousModeTest extends DriverTestCase
{
    public static function allowedModes(): iterable
    {
        yield 'normal' => [SqliteSynchronousMode::Normal, 1];
        yield 'full' => [SqliteSynchronousMode::Full, 2];
    }

    public function testOpenDefaultsToNormal(): void
    {
        $storage = SqliteQueueStorage::open($this->database->path());
        try {
            $this->assertSame(SqliteSynchronousMode::Normal, $storage->synchronousMode());
            $this->assertSame(1, $this->pragma($storage, 'synchronous'));
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testOpenSelectsEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $storage = SqliteQueueStorage::open($this->database->path(), $mode);
        try {
            $this->assertSame($mode, $storage->synchronousMode());
            $this->assertSame($expected, $this->pragma($storage, 'synchronous'));
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testReadbackRejectsChangedEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $this->assertSame(SqliteSynchronousMode::Normal === $mode ? 1 : 2, $expected);
        $storage = SqliteQueueStorage::open($this->database->path(), $mode);
        try {
            $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
            $this->assertInstanceOf(Sqlite::class, $connection);
            $connection->exec('PRAGMA synchronous = '.(SqliteSynchronousMode::Normal === $mode ? 'FULL' : 'NORMAL'));
            try {
                $storage->synchronousMode();
                $this->fail('Changed effective mode must be rejected.');
            } catch (\RuntimeException $error) {
                $this->assertSame('Effective synchronous mode does not match the configured mode.', $error->getMessage());
            }
        } finally {
            $storage->close();
        }
    }

    public function testOpenRejectsMemoryPathBeforeAcquisition(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        SqliteQueueStorage::open(':memory:');
    }

    private function pragma(SqliteQueueStorage $storage, string $name): int
    {
        $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
        $this->assertInstanceOf(Sqlite::class, $connection);

        return (int) $connection->query('PRAGMA '.$name)->fetchColumn();
    }
}
