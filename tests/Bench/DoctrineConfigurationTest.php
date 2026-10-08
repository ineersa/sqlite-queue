<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

final class DoctrineConfigurationTest extends TestCase
{
    private const SQLITE_BUSY = 5;

    public function testDoctrineNativeImmediateModeReservesWriterAtBeginAndRollbackReleasesIt(): void
    {
        $database = new IsolatedDatabase();
        $first = Baseline::connect($database->path());
        $second = Baseline::connect($database->path());
        try {
            $first->executeStatement('CREATE TABLE writer_probe (id INTEGER PRIMARY KEY)');
            $native = $second->getNativeConnection();
            $this->assertInstanceOf(\Pdo\Sqlite::class, $native);
            $this->assertSame(\Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE, $native->getAttribute(\Pdo\Sqlite::ATTR_TRANSACTION_MODE));
            $settings = Baseline::durability($first);
            $this->assertSame('immediate', $settings['transaction_mode']);
            $this->assertSame(\Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE, $settings['native_transaction_mode']);
            // No timing inference: a held writer plus timeout zero makes BEGIN decisive.
            $second->executeStatement('PRAGMA busy_timeout=0');
            $first->beginTransaction();
            try {
                $native->beginTransaction();
                $this->fail('The second writer must fail at BEGIN, before any read/write upgrade.');
            } catch (\PDOException $error) {
                $this->assertSame(self::SQLITE_BUSY, $error->errorInfo[1]);
            }
            $this->assertFalse($native->inTransaction());
            $first->executeStatement('INSERT INTO writer_probe VALUES (1)');
            $this->assertSame(0, (int) $second->fetchOne('SELECT COUNT(*) FROM writer_probe'));
            $first->rollBack();
            $second->beginTransaction();
            $second->executeStatement('INSERT INTO writer_probe VALUES (2)');
            $second->rollBack();
            $this->assertSame(0, (int) $first->fetchOne('SELECT COUNT(*) FROM writer_probe'));
            $first->beginTransaction();
            $first->executeStatement('INSERT INTO writer_probe VALUES (3)');
            $first->commit();
            $this->assertSame(3, (int) $second->fetchOne('SELECT id FROM writer_probe'));
            $this->assertFalse($first->isTransactionActive());
            $this->assertFalse($second->isTransactionActive());
        } finally {
            $first->close();
            $second->close();
            $database->remove();
        }
    }
}
