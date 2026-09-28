<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Driver;

use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteTransaction;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;

/**
 * Real storage evidence for the commit-before-confirmation and rollback contracts.
 *
 * Every connection closes in a finally block. close() is idempotent and force-closes an
 * active transaction, which is the cleanup path the rest of the driver suite relies on.
 */
final class TransactionOutcomeTest extends DriverTestCase
{
    public function testCommitPersistsAndRollbackDiscards(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();

            try {
                $connection->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');

                $transaction = $connection->beginTransaction();
                $transaction->execute('INSERT INTO item (label) VALUES (?)', ['committed']);
                $transaction->commit();
                self::assertSame(1, $this->countRows($connection));

                $transaction = $connection->beginTransaction();
                $transaction->execute('INSERT INTO item (label) VALUES (?)', ['discarded']);
                self::assertSame(2, $this->countRowsIn($transaction), 'The open transaction must see its own write.');
                $transaction->rollback();
                self::assertSame(1, $this->countRows($connection), 'Rollback must remove the uncommitted write.');
            } finally {
                $connection->close();
            }
        });
    }

    public function testNestedSavepointRollbackKeepsTheOuterWrite(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();

            try {
                $connection->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');

                $outer = $connection->beginTransaction();
                $outer->execute('INSERT INTO item (label) VALUES (?)', ['outer']);

                $inner = $outer->beginTransaction();
                $inner->execute('INSERT INTO item (label) VALUES (?)', ['inner']);
                $inner->rollback();

                $outer->commit();

                self::assertSame(['outer'], $this->labels($connection));
            } finally {
                $connection->close();
            }
        });
    }

    public function testDataChangingStatementWithReturningIsRejected(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();

            try {
                $connection->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');

                try {
                    $connection->execute('INSERT INTO item (label) VALUES (?) RETURNING id', ['row']);
                    self::fail('A data-changing statement with RETURNING was accepted.');
                } catch (SqliteQueryError $exception) {
                    self::assertStringContainsString(
                        'Row-producing DML statements are not supported',
                        $exception->getMessage(),
                    );
                }

                try {
                    $connection->execute('UPDATE item SET label = ? WHERE id = 1 RETURNING id', ['changed']);
                    self::fail('A data-changing UPDATE with RETURNING was accepted.');
                } catch (SqliteQueryError $exception) {
                    self::assertStringContainsString(
                        'Row-producing DML statements are not supported',
                        $exception->getMessage(),
                    );
                }

                self::assertSame(0, $this->countRows($connection));
            } finally {
                $connection->close();
            }
        });
    }

    /**
     * close() is idempotent and force-closes an active transaction. The open write is
     * discarded, so a reopened connection never sees a half-finished delivery.
     */
    public function testClosingWithAnOpenTransactionIsIdempotentAndDiscardsTheOpenWrite(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();

            $connection->execute('CREATE TABLE item (id INTEGER PRIMARY KEY, label TEXT NOT NULL)');
            $connection->execute('INSERT INTO item (label) VALUES (?)', ['committed']);

            $transaction = $connection->beginTransaction();
            $transaction->execute('INSERT INTO item (label) VALUES (?)', ['uncommitted']);

            $connection->close();
            $connection->close();

            self::assertTrue($connection->isClosed());

            $reopened = $this->connection();

            try {
                self::assertSame(['committed'], $this->labels($reopened), 'Closing must discard the open write.');
            } finally {
                $reopened->close();
            }
        });
    }

    private function connection(): SqliteConnection
    {
        return (new SqliteConnector())->connect(
            (new SqliteConfig($this->database->path()))->withTransactionMode(SqliteTransactionMode::Immediate),
        );
    }

    private function countRows(SqliteConnection $connection): int
    {
        $result = $connection->query('SELECT count(*) c FROM item');
        $row = $result->fetchRow();
        $result->close();

        return $row['c'];
    }

    /**
     * Reads through the open transaction. A connection-level read would wait for the
     * transaction to finish and deadlock the calling fiber.
     */
    private function countRowsIn(SqliteTransaction $transaction): int
    {
        $result = $transaction->query('SELECT count(*) c FROM item');
        $row = $result->fetchRow();
        $result->close();

        return $row['c'];
    }

    /**
     * @return list<string>
     */
    private function labels(SqliteConnection $connection): array
    {
        $result = $connection->query('SELECT label FROM item ORDER BY id');
        $labels = [];

        while (($row = $result->fetchRow()) !== null) {
            $labels[] = $row['label'];
        }

        $result->close();

        return $labels;
    }
}
