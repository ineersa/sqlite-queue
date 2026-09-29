<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\Sync\LocalMutex;
use Fabpot\Amp\Sqlite\SqliteBlob;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransaction;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Revolt\EventLoop\FiberLocal;

/**
 * Owns one async SQLite connection and the queue-specific persistence operations.
 *
 * Call {@see exclusive()} for every mutation. {@see insert()}, {@see claim()}, and {@see settle()}
 * require that ownership and throw if invoked without it. Storage never tracks live clients and
 * never chooses receipt tokens or epochs.
 */
final class SqliteQueueStorage
{
    private readonly LocalMutex $mutex;
    private bool $closed = false;
    /** @var FiberLocal<bool> Ownership belongs to the calling fiber, not all concurrent callers. */
    private readonly FiberLocal $owned;

    /**
     * Transfers exclusive ownership of the connection, including on initialization failure.
     */
    public function __construct(private readonly SqliteConnection $connection)
    {
        $this->mutex = new LocalMutex();
        $this->owned = new FiberLocal(static fn (): bool => false);
        try {
            $config = $connection->getConfig();
            if (SqliteJournalMode::Wal !== $config->getJournalMode() || SqliteSynchronousMode::Full !== $config->getSynchronousMode()) {
                throw new \InvalidArgumentException('Queue storage requires an explicitly configured WAL/FULL connection.');
            }
            $connection->setTransactionIsolation(SqliteTransactionMode::Immediate);
            $mode = $connection->query('PRAGMA journal_mode');
            $journal = $mode->fetchRow();
            $mode->close();
            $sync = $connection->query('PRAGMA synchronous');
            $synchronous = $sync->fetchRow();
            $sync->close();
            if ('wal' !== ($journal['journal_mode'] ?? null) || 2 !== ($synchronous['synchronous'] ?? null)) {
                throw new \RuntimeException('Queue storage requires file-backed WAL/FULL durability.');
            }
            $connection->executeScript(<<<'SQL'
                CREATE TABLE IF NOT EXISTS queue_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    body BLOB NOT NULL CHECK (typeof(body) = 'blob'),
                    headers BLOB NOT NULL CHECK (typeof(headers) = 'blob'),
                    available_at INTEGER NOT NULL CHECK (available_at >= 0),
                    reserved_until INTEGER,
                    reservation_token TEXT,
                    owner_id TEXT,
                    broker_epoch TEXT,
                    CHECK (
                        (reserved_until IS NULL AND reservation_token IS NULL AND owner_id IS NULL AND broker_epoch IS NULL)
                        OR (reserved_until IS NOT NULL AND reservation_token IS NOT NULL AND owner_id IS NOT NULL AND broker_epoch IS NOT NULL)
                    )
                );
                CREATE INDEX IF NOT EXISTS queue_messages_order ON queue_messages (queue, id);
                SQL);
        } catch (\Throwable $error) {
            $connection->close();
            throw $error;
        }
    }

    private function __clone(): void
    {
    }

    /** Open durable queue storage for a database file. */
    public static function open(string $path, ?Cancellation $cancellation = null): self
    {
        if ('' === $path || ':memory:' === $path) {
            throw new \InvalidArgumentException('Queue storage requires a database file path.');
        }
        $config = (new SqliteConfig($path))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withSynchronousMode(SqliteSynchronousMode::Full)
            ->withTransactionMode(SqliteTransactionMode::Immediate);

        return self::fromConnection((new SqliteConnector())->connect($config, $cancellation));
    }

    /** Transfer an existing WAL/FULL connection into storage ownership. */
    public static function fromConnection(SqliteConnection $connection): self
    {
        return new self($connection);
    }

    /**
     * Serialize one persistence-owned operation. Policy clocks must be sampled inside this closure.
     *
     * @template T
     *
     * @param \Closure(): T $operation
     *
     * @return T
     */
    public function exclusive(\Closure $operation): mixed
    {
        if (true === $this->owned->get()) {
            throw new \LogicException('Queue storage ownership cannot be acquired recursively.');
        }
        $lock = $this->mutex->acquire();
        $this->owned->set(true);
        try {
            $this->assertOpen();

            return $operation();
        } catch (SqliteConnectionException $error) {
            $this->failClosed();
            throw $error;
        } finally {
            $this->owned->unset();
            $lock->release();
        }
    }

    /** @throws \LogicException when called outside {@see exclusive()} */
    public function insert(string $queue, string $body, string $headers, int $availableAt): int
    {
        $this->assertOwned();

        return $this->transaction(static function (SqliteTransaction $transaction) use ($queue, $body, $headers, $availableAt): int {
            $result = $transaction->execute(
                'INSERT INTO queue_messages (queue, body, headers, available_at) VALUES (?, ?, ?, ?)',
                [$queue, new SqliteBlob($body), new SqliteBlob($headers), $availableAt],
            );
            $id = $result->getLastInsertId();
            $result->close();
            if (null === $id) {
                throw new \RuntimeException('Insert did not return a message identity.');
            }

            return $id;
        });
    }

    /**
     * Atomically select, reserve, and read one eligible message. Empty and zero-row claims roll back.
     *
     * @return ?array{id: int, body: string, headers: string, available_at: int, reserved_until: int}
     *
     * @throws \LogicException when called outside {@see exclusive()}
     */
    public function claim(string $queue, string $ownerId, string $epoch, string $token, int $now, int $expires): ?array
    {
        $this->assertOwned();
        /** @var array{id: int, body: string, headers: string, available_at: int, reserved_until: int}|null $claimed */
        $claimed = $this->transaction(static function (SqliteTransaction $transaction) use ($queue, $ownerId, $epoch, $token, $now, $expires) {
            $select = $transaction->execute(
                'SELECT id FROM queue_messages WHERE queue = ? AND available_at <= ?
                 AND (reserved_until IS NULL OR reserved_until <= ?) ORDER BY id LIMIT 1',
                [$queue, $now, $now],
            );
            $row = $select->fetchRow();
            $select->close();
            if (null === $row) {
                return null;
            }
            $update = $transaction->execute(
                'UPDATE queue_messages SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
                 WHERE id = ? AND queue = ? AND available_at <= ? AND (reserved_until IS NULL OR reserved_until <= ?)',
                [$expires, $token, $ownerId, $epoch, $row['id'], $queue, $now, $now],
            );
            $changed = $update->getRowCount();
            $update->close();
            if (1 !== $changed) {
                return null;
            }
            $data = $transaction->execute('SELECT body, headers, available_at FROM queue_messages WHERE id = ?', [$row['id']]);
            $payload = $data->fetchRow();
            $data->close();
            if (null === $payload || !$payload['body'] instanceof SqliteBlob || !$payload['headers'] instanceof SqliteBlob) {
                throw new \RuntimeException('Stored message is missing or has invalid payload types.');
            }

            return [
                'id' => (int) $row['id'],
                'body' => $payload['body']->getBytes(),
                'headers' => $payload['headers']->getBytes(),
                'available_at' => (int) $payload['available_at'],
                'reserved_until' => $expires,
            ];
        }, rollbackEmpty: true);

        return $claimed;
    }

    /**
     * Conditionally delete one fenced delivery.
     *
     * `$beforeCommit` runs after a successful DELETE and before commit so a disconnect can roll
     * the settlement back. It is required on every call.
     *
     * @param \Closure(): void $beforeCommit
     *
     * @return bool true when exactly one fenced row was deleted
     *
     * @throws \LogicException when called outside {@see exclusive()}
     */
    public function settle(int $id, string $token, string $ownerId, string $epoch, int $now, \Closure $beforeCommit): bool
    {
        $this->assertOwned();
        /** @var bool $settled */
        $settled = $this->transaction(static function (SqliteTransaction $transaction) use ($id, $token, $ownerId, $epoch, $now, $beforeCommit): bool {
            $result = $transaction->execute(
                'DELETE FROM queue_messages WHERE id = ? AND reservation_token = ? AND owner_id = ? AND broker_epoch = ? AND reserved_until > ?',
                [$id, $token, $ownerId, $epoch, $now],
            );
            $changed = $result->getRowCount();
            $result->close();
            if (1 !== $changed) {
                return false;
            }
            $beforeCommit();

            return true;
        });

        return $settled;
    }

    /** Wait for current operation ownership, then release the persistence worker. */
    public function close(): void
    {
        $lock = $this->mutex->acquire();
        try {
            $this->failClosed();
        } finally {
            $lock->release();
        }
    }

    /**
     * @param \Closure(SqliteTransaction): mixed $operation
     */
    private function transaction(\Closure $operation, bool $rollbackEmpty = false): mixed
    {
        $transaction = $this->connection->beginTransaction();
        try {
            $value = $operation($transaction);
            if ($rollbackEmpty && null === $value) {
                $transaction->rollback();
            } else {
                $transaction->commit();
            }

            return $value;
        } catch (\Throwable $error) {
            if ($transaction->isActive()) {
                try {
                    $transaction->rollback();
                } catch (\Throwable $rollbackError) {
                    $this->failClosed();
                    throw new \RuntimeException('Rollback failed; queue storage is closed. '.$rollbackError->getMessage(), previous: $error);
                }
            }
            throw $error;
        }
    }

    private function assertOwned(): void
    {
        if (true !== $this->owned->get()) {
            throw new \LogicException('Queue storage mutations require exclusive() ownership.');
        }
    }

    private function assertOpen(): void
    {
        if ($this->closed || $this->connection->isClosed()) {
            throw new \RuntimeException('Queue storage is closed.');
        }
    }

    private function failClosed(): void
    {
        $this->closed = true;
        if (!$this->connection->isClosed()) {
            $this->connection->close();
        }
    }
}
