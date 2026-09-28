<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Driver;

use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;

use function Amp\async;
use function Amp\delay;

/**
 * The queue engine must serialize complete storage operations. These checks show that the
 * driver's per-connection lock already covers the case, and that a second connection on a
 * WAL database never observes uncommitted rows.
 *
 * Connections close in a finally block. A pending future is ignored in the same block, so a
 * failed assertion cannot leave an unconsumed future or an open transaction behind.
 */
final class OperationSerializationTest extends DriverTestCase
{
    public function testAnotherOperationCannotEnterAnOpenTransaction(): void
    {
        $this->runAsync(function (): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));
            $concurrent = null;

            try {
                $connection->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');
                $connection->execute('INSERT INTO item (label) VALUES (?)', ['committed']);

                $transaction = $connection->beginTransaction();
                $transaction->execute('INSERT INTO item (label) VALUES (?)', ['pending']);

                $startedAt = microtime(true);
                $concurrent = async(static function () use ($connection, $startedAt): array {
                    $result = $connection->query('SELECT count(*) c FROM item');
                    $row = $result->fetchRow();
                    $result->close();

                    return [microtime(true) - $startedAt, $row['c']];
                });

                delay(0.1);

                self::assertFalse(
                    $concurrent->isComplete(),
                    'A second operation completed while a transaction was open on the same connection.',
                );

                $transaction->rollback();

                [$elapsed, $count] = $concurrent->await();
                $concurrent = null;

                self::assertGreaterThanOrEqual(0.1, $elapsed);
                self::assertSame(1, $count, 'The concurrent operation observed an uncommitted row.');
            } finally {
                if (null !== $concurrent) {
                    $concurrent->ignore();
                }

                $connection->close();
            }
        });
    }

    public function testSecondConnectionOnWalSeesOnlyCommittedRows(): void
    {
        $this->runAsync(function (): void {
            $writer = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));
            $reader = null;

            try {
                $writer->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');
                $writer->execute('INSERT INTO item (label) VALUES (?)', ['committed']);

                $reader = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));

                $transaction = $writer->beginTransaction();
                $transaction->execute('INSERT INTO item (label) VALUES (?)', ['pending']);

                $result = $reader->query('SELECT count(*) c FROM item');
                $row = $result->fetchRow();
                $result->close();
                self::assertSame(1, $row['c'], 'A WAL reader observed an uncommitted row.');

                $transaction->commit();

                $result = $reader->query('SELECT count(*) c FROM item');
                $row = $result->fetchRow();
                $result->close();
                self::assertSame(2, $row['c']);
            } finally {
                $reader?->close();
                $writer->close();
            }
        });
    }
}
