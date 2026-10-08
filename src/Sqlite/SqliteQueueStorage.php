<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\Sync\LocalMutex;
use Fabpot\Amp\Sqlite\SqliteBlob;
use Fabpot\Amp\Sqlite\SqliteCancellableConnection;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode as DriverSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransaction;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Ineersa\SqliteQueue\Protocol\Limits;

/** Queue SQL over fabpot's connection API; no package-owned worker or IPC. */
final class SqliteQueueStorage
{
    private const int BUSY_TIMEOUT_MILLISECONDS = 5_000;
    private const int WAL_AUTOCHECKPOINT_PAGES = 1_000;
    private const string SQLITE_MINIMUM_VERSION = '3.31.0';
    private const string INSERT_SQL = 'INSERT INTO queue_messages (queue, body, headers, available_at) VALUES (?, ?, ?, ?)';
    private const string ELIGIBLE_SQL = 'SELECT id, body, headers, available_at FROM queue_messages WHERE queue = ? AND available_at <= ?
                     AND (reserved_until IS NULL OR reserved_until <= ?) ORDER BY id LIMIT 1';
    private const string RESERVE_SQL = 'UPDATE queue_messages SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
                     WHERE id = ? AND queue = ? AND available_at <= ? AND (reserved_until IS NULL OR reserved_until <= ?)';
    private const string DELETE_SQL = 'DELETE FROM queue_messages WHERE id = ? AND reservation_token = ? AND owner_id = ? AND broker_epoch = ? AND reserved_until > ?';
    private const string DIAGNOSE_SQL = 'SELECT owner_id, typeof(owner_id) AS owner_id_type,
                            broker_epoch, typeof(broker_epoch) AS broker_epoch_type,
                            reservation_token, typeof(reservation_token) AS reservation_token_type,
                            reserved_until, typeof(reserved_until) AS reserved_until_type
                     FROM queue_messages WHERE id = ?';
    private const string EARLIEST_SQL = 'SELECT max(available_at, coalesce(reserved_until, available_at)) AS ready_at
                     FROM queue_messages WHERE queue = ?
                     ORDER BY ready_at LIMIT 1';
    private const string SCHEMA_SQL = <<<'SQL'
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
        CREATE INDEX IF NOT EXISTS queue_messages_ready ON queue_messages (
            queue,
            max(available_at, coalesce(reserved_until, available_at))
        );
        SQL;

    /** Null only after close releases ownership of the driver's connection. */
    private ?SqliteCancellableConnection $connection;

    /**
     * Transfers ownership, including cleanup on initialization failure.
     * The Amp mutex serializes full queue operations, including clock sampling and cleanup.
     * Waiting suspends a fiber, not the event loop.
     */
    public function __construct(SqliteCancellableConnection $connection, private readonly SqliteSynchronousMode $synchronous, private readonly LocalMutex $mutex)
    {
        $this->connection = $connection;
        try {
            $connection->setTransactionIsolation(SqliteTransactionMode::Immediate);
            $configuration = $this->configuration();
            if (version_compare($configuration['sqlite_version'], self::SQLITE_MINIMUM_VERSION, '<')) {
                throw new \RuntimeException('SQLite '.self::SQLITE_MINIMUM_VERSION.' or newer is required.');
            }
            if ('wal' !== $configuration['journal_mode']) {
                throw new \RuntimeException('Queue storage requires effective file-backed WAL mode.');
            }
            $connection->executeScript(self::SCHEMA_SQL);
        } catch (\Throwable $error) {
            try {
                $this->close();
            } catch (\Throwable) {
            }
            throw $error;
        }
    }

    private function __clone(): void
    {
    }

    /** Optional cancellation applies to the driver's connection handshake, not subsequent schema queries. */
    public static function open(string $path, SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal, ?Cancellation $cancellation = null): self
    {
        if ('' === $path || ':memory:' === $path) {
            throw new \InvalidArgumentException('Queue storage requires a database file path.');
        }
        $config = (new SqliteConfig($path))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withSynchronousMode(match ($synchronous) {
                SqliteSynchronousMode::Normal => DriverSynchronousMode::Normal,
                SqliteSynchronousMode::Full => DriverSynchronousMode::Full,
            })
            ->withTransactionMode(SqliteTransactionMode::Immediate)
            ->withBusyTimeout(self::BUSY_TIMEOUT_MILLISECONDS)
            ->withPragma('wal_autocheckpoint', self::WAL_AUTOCHECKPOINT_PAGES);

        return new self((new SqliteConnector())->connect($config, $cancellation), $synchronous, new LocalMutex());
    }

    /** Startup/readiness readback only; callers must not run it concurrently with queue operations. */
    public function synchronousMode(): SqliteSynchronousMode
    {
        $effective = match ($this->scalar('PRAGMA synchronous')) {
            1 => SqliteSynchronousMode::Normal,
            2 => SqliteSynchronousMode::Full,
            default => throw new \RuntimeException('Effective synchronous mode must be normal or full.'),
        };
        if ($effective !== $this->synchronous) {
            throw new \RuntimeException('Effective synchronous mode does not match the configured mode.');
        }

        return $effective;
    }

    /** Startup readback only; do not call concurrently with queue operations.
     * @return array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string}
     */
    public function configuration(): array
    {
        return [
            'journal_mode' => (string) $this->scalar('PRAGMA journal_mode'),
            'synchronous' => $this->synchronousMode()->value,
            'busy_timeout' => (int) $this->scalar('PRAGMA busy_timeout'),
            'wal_autocheckpoint' => (int) $this->scalar('PRAGMA wal_autocheckpoint'),
            'sqlite_version' => (string) $this->scalar('SELECT sqlite_version()'),
        ];
    }

    /** @param \Closure(): int $availableAt */
    public function insert(string $queue, string $body, string $headers, \Closure $availableAt): int
    {
        return $this->transaction(static function (SqliteTransaction $transaction) use ($queue, $body, $headers, $availableAt): int {
            $deadline = $availableAt();
            if ($deadline < 0) {
                throw new \InvalidArgumentException('Availability deadline must be nonnegative.');
            }
            $result = $transaction->execute(self::INSERT_SQL, [$queue, new SqliteBlob($body), new SqliteBlob($headers), $deadline]);
            try {
                $id = $result->getLastInsertId();
            } finally {
                $result->close();
            }
            if (null === $id) {
                throw new \RuntimeException('Insert did not return a message identity.');
            }
            if ($id <= 0) {
                throw new \RuntimeException('Insert did not return a positive message identity.');
            }

            return $id;
        });
    }

    /**
     * @param \Closure(): int    $clock
     * @param \Closure(int): int $reservationDeadline
     *
     * @return ?array{id: int, body: string, headers: string, available_at: int, reserved_until: int}
     */
    public function claim(string $queue, string $ownerId, string $epoch, string $token, \Closure $clock, \Closure $reservationDeadline): ?array
    {
        return $this->transaction(static function (SqliteTransaction $transaction) use ($queue, $ownerId, $epoch, $token, $clock, $reservationDeadline): ?array {
            $now = $clock();
            $expires = $reservationDeadline($now);
            $result = $transaction->execute(self::ELIGIBLE_SQL, [$queue, $now, $now]);
            try {
                $row = $result->fetchRow();
            } finally {
                $result->close();
            }
            if (null === $row) {
                return null;
            }
            if (!is_numeric($row['id'] ?? null)) {
                throw new \RuntimeException('Stored message identity must be an integer.');
            }
            $id = (int) $row['id'];
            $result = $transaction->execute(self::RESERVE_SQL, [$expires, $token, $ownerId, $epoch, $id, $queue, $now, $now]);
            try {
                $changed = $result->getRowCount();
            } finally {
                $result->close();
            }
            if (0 === $changed) {
                return null;
            }
            if (1 !== $changed) {
                throw new \RuntimeException('Reservation UPDATE did not affect exactly one row.');
            }
            $body = self::payloadBytes($row['body'] ?? null);
            $headers = self::payloadBytes($row['headers'] ?? null);
            if (\strlen($body) + \strlen($headers) > Limits::MAX_PAYLOAD) {
                throw new \RuntimeException('Stored message payload exceeds the supported size.');
            }
            if (!is_numeric($row['available_at'] ?? null)) {
                throw new \RuntimeException('Stored availability deadline must be an integer.');
            }
            $availableAt = (int) $row['available_at'];
            if ($availableAt < 0) {
                throw new \RuntimeException('Stored availability deadline must be nonnegative.');
            }

            return ['id' => $id, 'body' => $body, 'headers' => $headers, 'available_at' => $availableAt, 'reserved_until' => $expires];
        }, rollbackEmpty: true);
    }

    /** @param \Closure(): int $clock */
    public function settle(int $id, string $token, string $ownerId, string $epoch, \Closure $clock): SettlementResultEnum
    {
        return $this->transaction(function (SqliteTransaction $transaction) use ($id, $token, $ownerId, $epoch, $clock): SettlementResultEnum {
            $now = $clock();
            $result = $transaction->execute(self::DELETE_SQL, [$id, $token, $ownerId, $epoch, $now]);
            try {
                $changed = $result->getRowCount();
            } finally {
                $result->close();
            }
            if (0 === $changed) {
                return $this->settlementFailure($transaction, $id, $token, $ownerId, $epoch, $now);
            }
            if (1 !== $changed) {
                throw new \RuntimeException('Settlement DELETE did not affect exactly one reservation.');
            }

            return SettlementResultEnum::Settled;
        });
    }

    public function earliestEligibility(string $queue): ?int
    {
        $lock = $this->mutex->acquire();
        try {
            $result = $this->requireConnection()->execute(self::EARLIEST_SQL, [$queue]);
            try {
                $row = $result->fetchRow();
            } finally {
                $result->close();
            }
            if (null === $row) {
                return null;
            }
            if (!is_numeric($row['ready_at'] ?? null)) {
                throw new \RuntimeException('Stored eligibility deadline must be an integer.');
            }

            return (int) $row['ready_at'];
        } finally {
            $lock->release();
        }
    }

    /** Null uses the driver's default close budget; brokers supply their existing shutdown budget. */
    public function close(?Cancellation $cancellation = null): void
    {
        // Do not wait for an operation holding our mutex. The driver interrupts active work
        // and owns child termination; queued callers must see closed storage when they resume.
        $connection = $this->connection;
        $this->connection = null;
        $connection?->close($cancellation);
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
        $lock = $this->mutex->acquire();
        try {
            $transaction = $this->requireConnection()->beginTransaction();
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
                        $connection = $this->connection;
                        $this->connection = null;
                        try {
                            $connection?->close();
                        } catch (\Throwable) {
                        }
                        throw new \RuntimeException('Rollback failed; queue storage is closed. '.$rollbackError->getMessage(), previous: $error);
                    }
                }
                throw $error;
            }
        } finally {
            $lock->release();
        }
    }

    private function settlementFailure(SqliteTransaction $transaction, int $id, string $token, string $ownerId, string $epoch, int $now): SettlementResultEnum
    {
        $result = $transaction->execute(self::DIAGNOSE_SQL, [$id]);
        try {
            $row = $result->fetchRow();
        } finally {
            $result->close();
        }
        if (null === $row || null === $row['reserved_until']) {
            return SettlementResultEnum::NoActiveReservation;
        }
        if ('text' !== ($row['owner_id_type'] ?? null) || !\is_string($row['owner_id'])) {
            throw new \RuntimeException('Stored reservation owner must be a string.');
        }
        if ('text' !== ($row['broker_epoch_type'] ?? null) || !\is_string($row['broker_epoch'])) {
            throw new \RuntimeException('Stored reservation epoch must be a string.');
        }
        if ('text' !== ($row['reservation_token_type'] ?? null) || !\is_string($row['reservation_token'])) {
            throw new \RuntimeException('Stored reservation token must be a string.');
        }
        if ('integer' !== ($row['reserved_until_type'] ?? null) || !is_numeric($row['reserved_until'])) {
            throw new \RuntimeException('Stored reservation expiry must be an integer.');
        }
        if ($ownerId !== $row['owner_id']) {
            return SettlementResultEnum::OwnerMismatch;
        }
        if ($epoch !== $row['broker_epoch']) {
            return SettlementResultEnum::EpochMismatch;
        }
        if ($token !== $row['reservation_token']) {
            return SettlementResultEnum::TokenMismatch;
        }
        if ((int) $row['reserved_until'] <= $now) {
            return SettlementResultEnum::Expired;
        }

        throw new \RuntimeException('Settlement DELETE failed despite a matching active reservation.');
    }

    private function scalar(string $sql): mixed
    {
        $result = $this->requireConnection()->query($sql);
        try {
            $row = $result->fetchRow();
            if (null === $row) {
                throw new \RuntimeException('SQLite configuration query returned no row.');
            }

            return reset($row);
        } finally {
            $result->close();
        }
    }

    private static function payloadBytes(mixed $value): string
    {
        if ($value instanceof SqliteBlob) {
            return $value->getBytes();
        }
        if (\is_string($value)) {
            return $value;
        }

        throw new \RuntimeException('Stored message is missing or has invalid payload types.');
    }

    private function requireConnection(): SqliteCancellableConnection
    {
        if (null === $this->connection) {
            throw new \RuntimeException('Queue storage is closed.');
        }

        return $this->connection;
    }
}
