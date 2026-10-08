<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

final class SqliteQueueStorageTest extends TestCase
{
    public function testOpenReportsConfiguredSynchronousModeAndClosesCleanly(): void
    {
        $database = new IsolatedDatabase();
        try {
            $storage = SqliteQueueStorage::open($database->path(), SqliteSynchronousMode::Normal);
            try {
                $this->assertSame(SqliteSynchronousMode::Normal, $storage->synchronousMode());
                $configuration = $storage->configuration();
                $this->assertSame('wal', strtolower((string) $configuration['journal_mode']));
                $this->assertSame(SqliteSynchronousMode::Normal->value, $configuration['synchronous']);
            } finally {
                $storage->close();
            }
        } finally {
            $database->remove();
        }
    }
}
