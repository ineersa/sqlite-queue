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
 * The driver rejects data-changing statements with RETURNING, so the queue engine must
 * claim a message with a transactionally protected select and a conditional update.
 * These tests exercise that outline against real file storage.
 *
 * Each connection closes in a finally block. close() is idempotent and force-closes an
 * active transaction, so cleanup holds even when an assertion fails mid-transaction.
 */
final class ClaimTransactionTest extends DriverTestCase
{
    public function testParameterizedReadsRequireExecute(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();
            $this->createMessageTable($connection);

            try {
                $connection->query('SELECT id FROM message WHERE queue = ?', ['default']);
                self::fail('query() accepted bound parameters.');
            } catch (SqliteQueryError $exception) {
                self::assertStringContainsString('Parameters are not allowed in direct queries', $exception->getMessage());
            } finally {
                $connection->close();
            }
        });
    }

    public function testTransactionalSelectAndConditionalClaimIsReadableInsideTheTransaction(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();
            $this->createMessageTable($connection);

            try {
                $connection->execute('INSERT INTO message (queue, body, available_at) VALUES (?, ?, ?)', ['default', 'one', 0]);
                $connection->execute('INSERT INTO message (queue, body, available_at) VALUES (?, ?, ?)', ['default', 'two', 0]);

                $claim = $this->claim($connection, 'default', 0);

                self::assertSame(1, $claim['id']);
                self::assertSame('one', $claim['body']);
                self::assertSame(1, $claim['changed']);

                $result = $connection->execute(
                    'SELECT reservation_token FROM message WHERE id = ?',
                    [$claim['id']],
                );
                $row = $result->fetchRow();
                $result->close();

                self::assertSame($claim['token'], $row['reservation_token']);
            } finally {
                $connection->close();
            }
        });
    }

    public function testConditionalClaimOnAReservedMessageChangesNoRows(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();
            $this->createMessageTable($connection);

            try {
                $connection->execute('INSERT INTO message (queue, body, available_at) VALUES (?, ?, ?)', ['default', 'one', 0]);

                $first = $this->claim($connection, 'default', 0);
                self::assertSame(1, $first['changed']);

                $second = $this->claim($connection, 'default', 0);
                self::assertNull($second);
            } finally {
                $connection->close();
            }
        });
    }

    /**
     * The zero-row-count result is the lost-claim branch: another consumer reserved the row
     * between the select and the conditional update, so the update matches nothing and the
     * transaction rolls back. The seam injects that competing write after the select, inside
     * the same transaction, so the branch runs deterministically instead of depending on
     * timing. The seam is a test-only hook; production code passes no callback.
     */
    public function testConditionalClaimThatChangesNoRowsRollsBackAndLeavesTheFenceUntouched(): void
    {
        $this->runAsync(function (): void {
            $connection = $this->connection();
            $this->createMessageTable($connection);

            try {
                $connection->execute('INSERT INTO message (queue, body, available_at) VALUES (?, ?, ?)', ['default', 'one', 0]);

                $claim = $this->claim(
                    $connection,
                    'default',
                    0,
                    static function (SqliteTransaction $transaction, int $id): void {
                        $transaction->execute(
                            'UPDATE message SET reservation_token = ? WHERE id = ?',
                            ['competitor', $id],
                        );
                    },
                );

                self::assertNull($claim, 'A claim whose conditional update changed no rows must fail.');

                $result = $connection->execute('SELECT reservation_token, reserved_at FROM message WHERE id = ?', [1]);
                $row = $result->fetchRow();
                $result->close();

                self::assertNull($row['reservation_token'], 'The rollback must discard the losing reservation.');
                self::assertNull($row['reserved_at']);
            } finally {
                $connection->close();
            }
        });
    }

    /**
     * @param null|callable(SqliteTransaction, int): void $afterSelect Test-only seam that runs
     *        between the select and the conditional update, inside the same transaction.
     *
     * @return array{id: int, body: string, token: string, changed: int}|null
     */
    private function claim(
        SqliteConnection $connection,
        string $queue,
        int $now,
        ?callable $afterSelect = null,
    ): ?array {
        $transaction = $connection->beginTransaction();

        try {
            $result = $transaction->execute(
                'SELECT id, body FROM message
                 WHERE queue = ? AND available_at <= ? AND reservation_token IS NULL
                 ORDER BY id LIMIT 1',
                [$queue, $now],
            );
            $row = $result->fetchRow();
            $result->close();

            if ($row === null) {
                $transaction->rollback();

                return null;
            }

            if ($afterSelect !== null) {
                $afterSelect($transaction, (int) $row['id']);
            }

            $token = \bin2hex(\random_bytes(8));
            $update = $transaction->execute(
                'UPDATE message SET reservation_token = ?, reserved_at = ?
                 WHERE id = ? AND reservation_token IS NULL',
                [$token, $now, $row['id']],
            );
            $changed = $update->getRowCount();

            if ($changed !== 1) {
                $transaction->rollback();

                return null;
            }

            $transaction->commit();
        } catch (\Throwable $exception) {
            if ($transaction->isActive()) {
                $transaction->rollback();
            }

            throw $exception;
        }

        return ['id' => $row['id'], 'body' => $row['body'], 'token' => $token, 'changed' => $changed];
    }

    private function connection(): SqliteConnection
    {
        return (new SqliteConnector())->connect(
            (new SqliteConfig($this->database->path()))->withTransactionMode(SqliteTransactionMode::Immediate),
        );
    }

    private function createMessageTable(SqliteConnection $connection): void
    {
        $connection->executeScript(<<<'SQL'
            CREATE TABLE message (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                body BLOB NOT NULL,
                available_at INTEGER NOT NULL,
                reserved_at INTEGER NULL,
                reservation_token TEXT NULL
            );
            CREATE INDEX message_ready ON message (queue, available_at, id);
            SQL);
    }
}
