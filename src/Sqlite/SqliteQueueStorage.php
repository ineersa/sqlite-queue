<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Pdo\Sqlite;

/**
 * Owns one native PDO SQLite connection and the queue-specific persistence operations.
 *
 * One operation runs at a time in the worker process. Callers supply policy clocks that are
 * sampled after transaction acquisition. Storage never tracks live clients and never chooses
 * receipt tokens or epochs.
 */
final class SqliteQueueStorage
{
    private const int BUSY_TIMEOUT_MILLISECONDS = 5_000;
    private const int WAL_AUTOCHECKPOINT_PAGES = 1_000;
    private const string SQLITE_MINIMUM_VERSION = '3.31.0';
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

    private bool $closed = false;
    /**
     * Null after close or failed startup so PDO and statement resources are released.
     * Closed storage must not keep a usable connection handle.
     */
    private ?Sqlite $connection;
    private readonly \PDOStatement $insertStatement;
    private readonly \PDOStatement $eligibleIdStatement;
    private readonly \PDOStatement $reserveStatement;
    private readonly \PDOStatement $payloadStatement;
    private readonly \PDOStatement $deleteStatement;
    private readonly \PDOStatement $diagnoseStatement;
    private readonly \PDOStatement $earliestEligibilityStatement;

    /**
     * Takes exclusive ownership of the connection, including on initialization failure.
     */
    public function __construct(
        Sqlite $connection,
        private readonly SqliteSynchronousMode $synchronous,
    ) {
        $this->connection = $connection;
        try {
            $this->configure($connection, $synchronous);
            $connection->exec(self::SCHEMA_SQL);
            $this->insertStatement = $connection->prepare(
                'INSERT INTO queue_messages (queue, body, headers, available_at) VALUES (?, ?, ?, ?)',
            );
            $this->eligibleIdStatement = $connection->prepare(
                'SELECT id FROM queue_messages WHERE queue = ? AND available_at <= ?
                 AND (reserved_until IS NULL OR reserved_until <= ?) ORDER BY id LIMIT 1',
            );
            $this->reserveStatement = $connection->prepare(
                'UPDATE queue_messages SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
                 WHERE id = ? AND queue = ? AND available_at <= ? AND (reserved_until IS NULL OR reserved_until <= ?)',
            );
            $this->payloadStatement = $connection->prepare(
                'SELECT body, headers, available_at FROM queue_messages WHERE id = ?',
            );
            $this->deleteStatement = $connection->prepare(
                'DELETE FROM queue_messages WHERE id = ? AND reservation_token = ? AND owner_id = ? AND broker_epoch = ? AND reserved_until > ?',
            );
            $this->diagnoseStatement = $connection->prepare(
                'SELECT owner_id, typeof(owner_id) AS owner_id_type,
                        broker_epoch, typeof(broker_epoch) AS broker_epoch_type,
                        reservation_token, typeof(reservation_token) AS reservation_token_type,
                        reserved_until, typeof(reserved_until) AS reserved_until_type
                 FROM queue_messages WHERE id = ?',
            );
            $this->earliestEligibilityStatement = $connection->prepare(
                'SELECT max(available_at, coalesce(reserved_until, available_at)) AS ready_at
                 FROM queue_messages WHERE queue = ?
                 ORDER BY ready_at LIMIT 1',
            );
        } catch (\Throwable $error) {
            $this->releaseConnection();
            throw $error;
        }
    }

    private function __clone(): void
    {
    }

    /** Open queue storage. NORMAL is the product default. */
    public static function open(string $path, SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal): self
    {
        if ('' === $path || ':memory:' === $path) {
            throw new \InvalidArgumentException('Queue storage requires a database file path.');
        }
        try {
            $connection = new Sqlite('sqlite:'.$path, options: [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                \PDO::ATTR_STRINGIFY_FETCHES => false,
            ]);
        } catch (\PDOException $error) {
            throw new \RuntimeException('Failed to open queue database.', previous: $error);
        }

        return new self($connection, $synchronous);
    }

    /** Read the owning connection before serving, never an observer connection. */
    public function synchronousMode(): SqliteSynchronousMode
    {
        $connection = $this->requireConnection();
        $effective = $this->readSynchronousMode($connection);
        if ($effective !== $this->synchronous) {
            throw new \RuntimeException('Effective synchronous mode does not match the configured mode.');
        }

        return $effective;
    }

    /**
     * Insert one message after sampling availability inside the write transaction.
     *
     * @param \Closure(): int $availableAt
     */
    public function insert(string $queue, string $body, string $headers, \Closure $availableAt): int
    {
        $this->requireConnection();

        return $this->transaction(function () use ($queue, $body, $headers, $availableAt): int {
            $deadline = $availableAt();
            if ($deadline < 0) {
                throw new \InvalidArgumentException('Availability deadline must be nonnegative.');
            }
            $this->insertStatement->bindValue(1, $queue, \PDO::PARAM_STR);
            $this->insertStatement->bindValue(2, $body, \PDO::PARAM_LOB);
            $this->insertStatement->bindValue(3, $headers, \PDO::PARAM_LOB);
            $this->insertStatement->bindValue(4, $deadline, \PDO::PARAM_INT);
            $this->insertStatement->execute();
            $this->insertStatement->closeCursor();
            $rawId = $this->requireConnection()->lastInsertId();
            if (!is_numeric($rawId)) {
                throw new \RuntimeException('Insert did not return a message identity.');
            }
            $id = (int) $rawId;
            if ($id <= 0) {
                throw new \RuntimeException('Insert did not return a positive message identity.');
            }

            return $id;
        });
    }

    /**
     * Atomically select, reserve, and read one eligible message. Empty and zero-row claims roll back.
     *
     * @param \Closure(): int    $clock
     * @param \Closure(int): int $reservationDeadline receives the sampled transaction-time clock
     *
     * @return ?array{id: int, body: string, headers: string, available_at: int, reserved_until: int}
     */
    public function claim(string $queue, string $ownerId, string $epoch, string $token, \Closure $clock, \Closure $reservationDeadline): ?array
    {
        $this->requireConnection();

        return $this->transaction(function () use ($queue, $ownerId, $epoch, $token, $clock, $reservationDeadline): ?array {
            $now = $clock();
            $expires = $reservationDeadline($now);
            $this->eligibleIdStatement->bindValue(1, $queue, \PDO::PARAM_STR);
            $this->eligibleIdStatement->bindValue(2, $now, \PDO::PARAM_INT);
            $this->eligibleIdStatement->bindValue(3, $now, \PDO::PARAM_INT);
            $this->eligibleIdStatement->execute();
            $row = $this->eligibleIdStatement->fetch(\PDO::FETCH_ASSOC);
            $this->eligibleIdStatement->closeCursor();
            if (false === $row) {
                return null;
            }
            if (!is_numeric($row['id'] ?? null)) {
                throw new \RuntimeException('Stored message identity must be an integer.');
            }
            $id = (int) $row['id'];
            $this->reserveStatement->bindValue(1, $expires, \PDO::PARAM_INT);
            $this->reserveStatement->bindValue(2, $token, \PDO::PARAM_STR);
            $this->reserveStatement->bindValue(3, $ownerId, \PDO::PARAM_STR);
            $this->reserveStatement->bindValue(4, $epoch, \PDO::PARAM_STR);
            $this->reserveStatement->bindValue(5, $id, \PDO::PARAM_INT);
            $this->reserveStatement->bindValue(6, $queue, \PDO::PARAM_STR);
            $this->reserveStatement->bindValue(7, $now, \PDO::PARAM_INT);
            $this->reserveStatement->bindValue(8, $now, \PDO::PARAM_INT);
            $this->reserveStatement->execute();
            $changed = $this->reserveStatement->rowCount();
            $this->reserveStatement->closeCursor();
            if (0 === $changed) {
                return null;
            }
            if (1 !== $changed) {
                throw new \RuntimeException('Reservation UPDATE did not affect exactly one row.');
            }
            $this->payloadStatement->bindValue(1, $id, \PDO::PARAM_INT);
            $this->payloadStatement->execute();
            $payload = $this->payloadStatement->fetch(\PDO::FETCH_ASSOC);
            $this->payloadStatement->closeCursor();
            if (false === $payload) {
                throw new \RuntimeException('Stored message is missing or has invalid payload types.');
            }
            $body = $this->payloadBytes($payload['body'] ?? null, 'body');
            $headers = $this->payloadBytes($payload['headers'] ?? null, 'headers');
            if (!is_numeric($payload['available_at'] ?? null)) {
                throw new \RuntimeException('Stored availability deadline must be an integer.');
            }

            return [
                'id' => $id,
                'body' => $body,
                'headers' => $headers,
                'available_at' => (int) $payload['available_at'],
                'reserved_until' => $expires,
            ];
        }, rollbackEmpty: true);
    }

    /**
     * Conditionally delete one fenced delivery.
     *
     * Failure precedence is no active reservation, owner, epoch, token, then expiry.
     * Diagnosis uses the same write transaction and sampled clock as DELETE.
     *
     * @param \Closure(): int $clock sampled after transaction acquisition
     */
    public function settle(int $id, string $token, string $ownerId, string $epoch, \Closure $clock): SettlementResultEnum
    {
        $this->requireConnection();

        return $this->transaction(function () use ($id, $token, $ownerId, $epoch, $clock): SettlementResultEnum {
            $now = $clock();
            $this->deleteStatement->bindValue(1, $id, \PDO::PARAM_INT);
            $this->deleteStatement->bindValue(2, $token, \PDO::PARAM_STR);
            $this->deleteStatement->bindValue(3, $ownerId, \PDO::PARAM_STR);
            $this->deleteStatement->bindValue(4, $epoch, \PDO::PARAM_STR);
            $this->deleteStatement->bindValue(5, $now, \PDO::PARAM_INT);
            $this->deleteStatement->execute();
            $changed = $this->deleteStatement->rowCount();
            $this->deleteStatement->closeCursor();
            if (0 === $changed) {
                return $this->settlementFailure($id, $token, $ownerId, $epoch, $now);
            }
            if (1 !== $changed) {
                throw new \RuntimeException('Settlement DELETE did not affect exactly one reservation.');
            }

            return SettlementResultEnum::Settled;
        });
    }

    /**
     * Earliest effective eligibility deadline for one queue, or null when the queue has no rows.
     *
     * Effective eligibility is max(available_at, coalesce(reserved_until, available_at)).
     */
    public function earliestEligibility(string $queue): ?int
    {
        $this->requireConnection();
        $this->earliestEligibilityStatement->bindValue(1, $queue, \PDO::PARAM_STR);
        $this->earliestEligibilityStatement->execute();
        $row = $this->earliestEligibilityStatement->fetch(\PDO::FETCH_ASSOC);
        $this->earliestEligibilityStatement->closeCursor();
        if (false === $row) {
            return null;
        }
        if (!is_numeric($row['ready_at'] ?? null)) {
            throw new \RuntimeException('Stored eligibility deadline must be an integer.');
        }

        return (int) $row['ready_at'];
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->failClosed();
    }

    /**
     * @template T
     *
     * @param \Closure(): T $operation
     *
     * @return T
     */
    private function transaction(\Closure $operation, bool $rollbackEmpty = false): mixed
    {
        $connection = $this->requireConnection();
        $began = false;
        try {
            $connection->beginTransaction();
            $began = true;
            $value = $operation();
            if ($rollbackEmpty && null === $value) {
                $connection->rollBack();
                $began = false;

                return $value;
            }
            $connection->commit();

            return $value;
        } catch (\Throwable $error) {
            if ($began && $connection->inTransaction()) {
                try {
                    $connection->rollBack();
                } catch (\Throwable $rollbackError) {
                    $this->failClosed();
                    throw new \RuntimeException('Rollback failed; queue storage is closed. '.$rollbackError->getMessage(), previous: $error);
                }
            }
            throw $error;
        }
    }

    private function settlementFailure(int $id, string $token, string $ownerId, string $epoch, int $now): SettlementResultEnum
    {
        $this->diagnoseStatement->bindValue(1, $id, \PDO::PARAM_INT);
        $this->diagnoseStatement->execute();
        $row = $this->diagnoseStatement->fetch(\PDO::FETCH_ASSOC);
        $this->diagnoseStatement->closeCursor();
        if (false === $row || null === $row['reserved_until']) {
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

    private function configure(Sqlite $connection, SqliteSynchronousMode $synchronous): void
    {
        $connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $connection->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
        $connection->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, false);
        $connection->setAttribute(Sqlite::ATTR_TRANSACTION_MODE, Sqlite::TRANSACTION_MODE_IMMEDIATE);
        $connection->setAttribute(Sqlite::ATTR_EXTENDED_RESULT_CODES, true);

        $version = (string) $connection->query('SELECT sqlite_version()')->fetchColumn();
        if (version_compare($version, self::SQLITE_MINIMUM_VERSION, '<')) {
            throw new \RuntimeException(\sprintf('SQLite %s or newer is required, %s is installed.', self::SQLITE_MINIMUM_VERSION, $version));
        }

        $connection->exec('PRAGMA foreign_keys = ON');
        $connection->exec('PRAGMA trusted_schema = OFF');
        $connection->exec('PRAGMA busy_timeout = '.self::BUSY_TIMEOUT_MILLISECONDS);
        $connection->exec('PRAGMA journal_mode = WAL');
        $connection->exec('PRAGMA wal_autocheckpoint = '.self::WAL_AUTOCHECKPOINT_PAGES);
        $connection->exec(match ($synchronous) {
            SqliteSynchronousMode::Normal => 'PRAGMA synchronous = NORMAL',
            SqliteSynchronousMode::Full => 'PRAGMA synchronous = FULL',
        });

        $journal = $connection->query('PRAGMA journal_mode')->fetchColumn();
        if ('wal' !== $journal) {
            throw new \RuntimeException('Queue storage requires effective file-backed WAL mode.');
        }
        if ($this->readSynchronousMode($connection) !== $synchronous) {
            throw new \RuntimeException('Effective synchronous mode does not match the configured mode.');
        }
        $foreignKeys = $connection->query('PRAGMA foreign_keys')->fetchColumn();
        if (1 !== (int) $foreignKeys) {
            throw new \RuntimeException('Queue storage requires foreign keys enabled.');
        }
    }

    private function readSynchronousMode(Sqlite $connection): SqliteSynchronousMode
    {
        $value = $connection->query('PRAGMA synchronous')->fetchColumn();

        return match ((int) $value) {
            1 => SqliteSynchronousMode::Normal,
            2 => SqliteSynchronousMode::Full,
            default => throw new \RuntimeException('Effective synchronous mode must be normal or full.'),
        };
    }

    private function payloadBytes(mixed $value, string $field): string
    {
        if (\is_string($value)) {
            return $value;
        }
        if (\is_resource($value)) {
            $bytes = stream_get_contents($value);
            if (false === $bytes) {
                throw new \RuntimeException(\sprintf('Stored message %s could not be read.', $field));
            }

            return $bytes;
        }

        throw new \RuntimeException('Stored message is missing or has invalid payload types.');
    }

    private function requireConnection(): Sqlite
    {
        if ($this->closed || null === $this->connection) {
            throw new \RuntimeException('Queue storage is closed.');
        }

        return $this->connection;
    }

    private function failClosed(): void
    {
        $this->closed = true;
        $this->releaseConnection();
    }

    private function releaseConnection(): void
    {
        foreach ([
            'insertStatement',
            'eligibleIdStatement',
            'reserveStatement',
            'payloadStatement',
            'deleteStatement',
            'diagnoseStatement',
            'earliestEligibilityStatement',
        ] as $property) {
            if (isset($this->{$property})) {
                $this->{$property}->closeCursor();
            }
        }
        $this->connection = null;
    }
}
