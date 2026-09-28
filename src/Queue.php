<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

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

/** Owns one async connection. Deadlines are Unix wall-clock milliseconds. */
final class Queue
{
    private readonly LocalMutex $mutex;
    private readonly string $epoch;
    /** @var \Closure(): int */
    private readonly \Closure $clock;
    /** @var array<string, true> Active client contexts, not a message cache. */
    private array $sessions = [];
    private bool $closed = false;

    /**
     * Transfers exclusive ownership of the connection, including on initialization failure.
     *
     * @param \Closure(): int|null $clock wall-clock milliseconds, sampled inside operation ownership
     */
    public function __construct(
        private readonly SqliteConnection $connection,
        private readonly int $visibilityTimeout = 5000,
        ?\Closure $clock = null,
    ) {
        $this->mutex = new LocalMutex();
        $this->clock = $clock ?? static fn (): int => (int) floor(microtime(true) * 1000);
        $this->epoch = bin2hex(random_bytes(32));
        try {
            if ($visibilityTimeout <= 0) {
                throw new \InvalidArgumentException('Visibility timeout must be positive milliseconds.');
            }
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

    /** @param \Closure(): int|null $clock */
    public static function open(string $path, int $visibilityTimeout = 5000, ?\Closure $clock = null): self
    {
        if ('' === $path || ':memory:' === $path) {
            throw new \InvalidArgumentException('Queue storage requires a database file path.');
        }
        $config = (new SqliteConfig($path))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withSynchronousMode(SqliteSynchronousMode::Full)
            ->withTransactionMode(SqliteTransactionMode::Immediate);

        return new self((new SqliteConnector())->connect($config), $visibilityTimeout, $clock);
    }

    /** Create a new context per client connection. Never reuse a disconnected context. */
    public function openSession(): string
    {
        $this->assertOpen();
        $session = bin2hex(random_bytes(32));
        $this->sessions[$session] = true;

        return $session;
    }

    /** Invalidate receipts immediately without changing persisted reservation deadlines. */
    public function closeSession(string $session): void
    {
        unset($this->sessions[$session]);
    }

    public function send(string $queue, string $body, string $headers = '', int $delay = 0): int
    {
        self::validateQueue($queue);
        if ($delay < 0) {
            throw new \InvalidArgumentException('Delay must be nonnegative milliseconds.');
        }

        return $this->operation(function () use ($queue, $body, $headers, $delay): int {
            $availableAt = $this->deadline($delay);

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
        });
    }

    /** Immediately claim one eligible message, or return null. Never waits for future work. */
    public function receive(string $queue, string $session): ?Delivery
    {
        self::validateQueue($queue);

        return $this->operation(function () use ($queue, $session): ?Delivery {
            $this->assertSession($session);
            $delivery = $this->transaction(function (SqliteTransaction $transaction) use ($queue, $session): ?Delivery {
                $now = $this->now();
                $expires = $this->deadline($this->visibilityTimeout, $now);
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
                $token = bin2hex(random_bytes(32));
                $update = $transaction->execute(
                    'UPDATE queue_messages SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
                     WHERE id = ? AND queue = ? AND available_at <= ? AND (reserved_until IS NULL OR reserved_until <= ?)',
                    [$expires, $token, $session, $this->epoch, $row['id'], $queue, $now, $now],
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

                return new Delivery((int) $row['id'], $queue, $payload['body']->getBytes(), $payload['headers']->getBytes(), $row['id'].':'.$token, (int) $payload['available_at'], $expires);
            }, rollbackEmpty: true);
            // A disconnect may have occurred while waiting for persistence.
            $this->assertSession($session);

            return $delivery;
        });
    }

    public function acknowledge(string $receipt, string $session): void
    {
        $this->settle($receipt, $session);
    }

    public function reject(string $receipt, string $session): void
    {
        $this->settle($receipt, $session);
    }

    /** Wait for current operation ownership, then release the persistence worker. */
    public function close(): void
    {
        $lock = $this->mutex->acquire();
        try {
            $this->closed = true;
            $this->sessions = [];
            $this->connection->close();
        } finally {
            $lock->release();
        }
    }

    private function settle(string $receipt, string $session): void
    {
        if (1 !== preg_match('/\A([1-9][0-9]*):([a-f0-9]{64})\z/D', $receipt, $parts)) {
            throw new InvalidReceipt('Invalid delivery receipt.');
        }
        $this->operation(function () use ($parts, $session): void {
            $this->assertSession($session);
            $this->transaction(function (SqliteTransaction $transaction) use ($parts, $session): void {
                $result = $transaction->execute(
                    'DELETE FROM queue_messages WHERE id = ? AND reservation_token = ? AND owner_id = ? AND broker_epoch = ? AND reserved_until > ?',
                    [$parts[1], $parts[2], $session, $this->epoch, $this->now()],
                );
                $changed = $result->getRowCount();
                $result->close();
                if (1 !== $changed) {
                    throw new InvalidReceipt('Expired, unknown, or foreign delivery receipt.');
                }
                $this->assertSession($session);
            });
        });
    }

    /**
     * @template T
     *
     * @param \Closure(): T $operation
     *
     * @return T
     */
    private function operation(\Closure $operation): mixed
    {
        $lock = $this->mutex->acquire();
        try {
            $this->assertOpen();

            return $operation();
        } catch (SqliteConnectionException $error) {
            $this->closed = true;
            $this->sessions = [];
            $this->connection->close();
            throw $error;
        } finally {
            $lock->release();
        }
    }

    /**
     * @template T
     *
     * @param \Closure(SqliteTransaction): T $operation
     *
     * @return T
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
                    $this->closed = true;
                    $this->sessions = [];
                    $this->connection->close();
                    throw new \RuntimeException('Rollback failed; queue storage is closed. '.$rollbackError->getMessage(), previous: $error);
                }
            }
            throw $error;
        }
    }

    private function assertOpen(): void
    {
        if ($this->closed || $this->connection->isClosed()) {
            throw new \RuntimeException('Queue storage is closed.');
        }
    }

    private function assertSession(string $session): void
    {
        if (!isset($this->sessions[$session])) {
            throw new InvalidReceipt('Unknown or disconnected client context.');
        }
    }

    private static function validateQueue(string $queue): void
    {
        if (1 !== preg_match('/\A[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}\z/D', $queue)) {
            throw new \InvalidArgumentException('Queue names must contain 1 to 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit.');
        }
    }

    private function now(): int
    {
        $now = ($this->clock)();
        if ($now < 0) {
            throw new \InvalidArgumentException('Wall-clock milliseconds must be nonnegative.');
        }

        return $now;
    }

    private function deadline(int $milliseconds, ?int $now = null): int
    {
        $now ??= $this->now();
        if ($milliseconds > \PHP_INT_MAX - $now) {
            throw new \InvalidArgumentException('Deadline exceeds the supported timestamp range.');
        }

        return $now + $milliseconds;
    }
}
