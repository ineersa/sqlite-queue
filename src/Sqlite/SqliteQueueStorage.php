<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode as DriverSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Fabpot\Amp\Sqlite\SqliteWorkerOperationConnection;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Fabpot\Amp\Sqlite\Worker\OperationExecutor;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Ineersa\SqliteQueue\Protocol\ProtocolLimits;

use function Amp\async;

/** Complete queue operations over the driver's exclusively owned serial executor. */
final class SqliteQueueStorage
{
    private const int BUSY_TIMEOUT_MILLISECONDS = 5_000;
    private const int WAL_AUTOCHECKPOINT_PAGES = 1_000;
    private const string SQLITE_MINIMUM_VERSION = '3.31.0';
    private bool $closeStarted = false;
    /** @var DeferredFuture<void> Completion of the first close, including driver cleanup. */
    private readonly DeferredFuture $closeCompletion;
    /** Every close caller may tighten the one driver close's budget. */
    private readonly DeferredCancellation $closeCancellation;

    /** @param array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string} $configuration */
    private function __construct(
        private readonly SqliteWorkerOperationConnection $connection,
        private readonly OperationExecutor $executor,
        private readonly array $configuration,
        private readonly ProtocolLimits $limits,
    ) {
        $this->closeCompletion = new DeferredFuture();
        $this->closeCompletion->getFuture()->ignore();
        $this->closeCancellation = new DeferredCancellation();
    }

    /**
     * Cancellation covers acquisition, configuration readback and schema setup.
     * The operation class injects its worker clock.
     *
     * @param class-string<Operation> $workerOperation
     */
    public static function open(
        string $path,
        SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal,
        ?Cancellation $cancellation = null,
        string $workerOperation = QueueWorkerOperation::class,
        ?ProtocolLimits $limits = null,
        // Ordinary callers keep the driver's default ProcessContextFactory isolation without constructing the connector.
        SqliteConnector $connector = new SqliteConnector(),
    ): self {
        if ('' === $path || ':memory:' === $path) {
            throw new \InvalidArgumentException('Queue storage requires a database file path.');
        }
        $limits ??= ProtocolLimits::defaults();
        $config = (new SqliteConfig($path))
            ->withJournalMode(SqliteJournalMode::Wal)
            ->withSynchronousMode(match ($synchronous) {
                SqliteSynchronousMode::Normal => DriverSynchronousMode::Normal,
                SqliteSynchronousMode::Full => DriverSynchronousMode::Full,
            })
            ->withTransactionMode(SqliteTransactionMode::Immediate)
            ->withBusyTimeout(self::BUSY_TIMEOUT_MILLISECONDS)
            ->withPragma('wal_autocheckpoint', self::WAL_AUTOCHECKPOINT_PAGES)
            ->withWorkerOperations([QueueWorkerProtocol::OPERATION_NAME => $workerOperation]);

        return self::fromConnection($connector->connect($config, $cancellation), $synchronous, $cancellation, $limits);
    }

    /** Transfers an acquired connection. Optional cancellation also bounds readback/schema initialization. */
    public static function fromConnection(SqliteWorkerOperationConnection $connection, SqliteSynchronousMode $synchronous, ?Cancellation $cancellation = null, ?ProtocolLimits $limits = null): self
    {
        $limits ??= ProtocolLimits::defaults();
        $initialization = async(static function () use ($connection, $synchronous, $limits): self {
            $configuration = [
                'journal_mode' => (string) self::scalar($connection, 'PRAGMA journal_mode'),
                'synchronous' => match (self::scalar($connection, 'PRAGMA synchronous')) {
                    1 => SqliteSynchronousMode::Normal->value,
                    2 => SqliteSynchronousMode::Full->value,
                    default => throw new \RuntimeException('Effective synchronous mode must be normal or full.'),
                },
                'busy_timeout' => (int) self::scalar($connection, 'PRAGMA busy_timeout'),
                'wal_autocheckpoint' => (int) self::scalar($connection, 'PRAGMA wal_autocheckpoint'),
                'sqlite_version' => (string) self::scalar($connection, 'SELECT sqlite_version()'),
            ];
            if ($configuration['synchronous'] !== $synchronous->value) {
                throw new \RuntimeException('Effective synchronous mode does not match the configured mode.');
            }
            if (version_compare($configuration['sqlite_version'], self::SQLITE_MINIMUM_VERSION, '<')) {
                throw new \RuntimeException('SQLite '.self::SQLITE_MINIMUM_VERSION.' or newer is required.');
            }
            if ('wal' !== $configuration['journal_mode']) {
                throw new \RuntimeException('Queue storage requires effective file-backed WAL mode.');
            }
            $connection->executeScript(QueueSql::SCHEMA_SQL);
            $executor = $connection->createWorkerOperationExecutor($limits->maximumResultBytes(), QueueWorkerProtocol::MAXIMUM_RESULT_ROWS);

            return new self($connection, $executor, $configuration, $limits);
        });
        $initialization->ignore();
        try {
            return $initialization->await($cancellation);
        } catch (\Throwable $error) {
            try {
                $connection->close($cancellation);
            } finally {
                // Cleanup must not replace the original initialization failure.
                throw $error;
            }
        }
    }

    public function protocolLimits(): ProtocolLimits
    {
        return $this->limits;
    }

    public function synchronousMode(): SqliteSynchronousMode
    {
        return SqliteSynchronousMode::from($this->configuration['synchronous']);
    }

    /** Startup snapshot, acquired before exclusive executor ownership.
     * @return array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string}
     */
    public function configuration(): array
    {
        return $this->configuration;
    }

    public function insert(string $queue, string $body, string $headers, int $delay): int
    {
        $result = $this->run(QueueStorageOperation::Insert, ['queue' => $queue, 'body' => $body, 'headers' => $headers, 'delay' => $delay]);
        $this->requireOutcome($result, Outcome::Commit, 'Worker insert reply');
        $data = $result->data;
        if (!\is_int($data['id'] ?? null)) {
            $this->invalidReply('Worker insert reply must contain an integer identity.');
        }

        return $data['id'];
    }

    /** @return ?array{id: int, body: string, headers: string, available_at: int, reserved_until: int} */
    public function claim(string $queue, string $ownerId, string $epoch, string $token, int $visibility): ?array
    {
        $result = $this->run(QueueStorageOperation::Claim, ['queue' => $queue, 'owner' => $ownerId, 'epoch' => $epoch, 'token' => $token, 'visibility' => $visibility]);
        $data = $result->data;
        if (!\array_key_exists('message', $data)) {
            $this->invalidReply('Worker claim reply is missing its message field.');
        }
        $message = $data['message'];
        if (null === $message) {
            $this->requireOutcome($result, Outcome::Rollback, 'Worker empty claim reply');

            return null;
        }
        $this->requireOutcome($result, Outcome::Commit, 'Worker populated claim reply');
        if (!\is_array($message)) {
            $this->invalidReply('Worker claim reply must contain a message or null.');
        }
        foreach (['id', 'available_at', 'reserved_until'] as $field) {
            if (!\is_int($message[$field] ?? null)) {
                $this->invalidReply('Worker claim '.$field.' must be an integer.');
            }
        }
        foreach (['body', 'headers'] as $field) {
            if (!\is_string($message[$field] ?? null)) {
                $this->invalidReply('Worker claim '.$field.' must contain bytes.');
            }
        }

        return ['id' => $message['id'], 'body' => $message['body'], 'headers' => $message['headers'], 'available_at' => $message['available_at'], 'reserved_until' => $message['reserved_until']];
    }

    public function settle(int $id, string $token, string $ownerId, string $epoch): SettlementResultEnum
    {
        $result = $this->run(QueueStorageOperation::Settle, ['id' => $id, 'token' => $token, 'owner' => $ownerId, 'epoch' => $epoch]);
        $this->requireOutcome($result, Outcome::Commit, 'Worker settlement reply');
        $data = $result->data;
        if (!\is_string($data['settlement'] ?? null)) {
            $this->invalidReply('Worker settlement reply must contain a status.');
        }
        $settlement = SettlementResultEnum::tryFrom($data['settlement']);
        if (null === $settlement) {
            $this->invalidReply('Worker settlement status is unknown.');
        }

        return $settlement;
    }

    public function keepalive(int $id, string $token, string $ownerId, string $epoch, int $visibility, int $minimum): int|SettlementResultEnum
    {
        $result = $this->run(QueueStorageOperation::Keepalive, ['id' => $id, 'token' => $token, 'owner' => $ownerId, 'epoch' => $epoch, 'visibility' => $visibility, 'minimum' => $minimum]);
        if (\array_key_exists('reserved_until', $result->data)) {
            $this->requireOutcome($result, Outcome::Commit, 'Worker keepalive reply');
            $deadline = $result->data['reserved_until'];
            if (['reserved_until'] !== array_keys($result->data) || !\is_int($deadline) || $deadline < 0) {
                $this->invalidReply('Worker keepalive reply must contain only a nonnegative integer expiry.');
            }

            return $deadline;
        }
        $this->requireOutcome($result, Outcome::Rollback, 'Worker rejected keepalive reply');
        if (['settlement'] !== array_keys($result->data) || !\is_string($result->data['settlement'])) {
            $this->invalidReply('Worker rejected keepalive reply must contain only a receipt failure.');
        }
        $failure = SettlementResultEnum::tryFrom($result->data['settlement']);
        if (null === $failure || SettlementResultEnum::Settled === $failure) {
            $this->invalidReply('Worker rejected keepalive receipt status is invalid.');
        }

        return $failure;
    }

    public function earliestEligibility(string $queue): ?int
    {
        $result = $this->run(QueueStorageOperation::Eligibility, ['queue' => $queue]);
        $this->requireOutcome($result, Outcome::ReadOnly, 'Worker eligibility reply');
        $data = $result->data;
        if (!\array_key_exists('ready_at', $data)) {
            $this->invalidReply('Worker eligibility reply is missing its ready_at field.');
        }
        $readyAt = $data['ready_at'];
        if (null !== $readyAt && !\is_int($readyAt)) {
            $this->invalidReply('Worker eligibility reply must contain an integer or null.');
        }

        return $readyAt;
    }

    /** Null adds no caller budget. Later cancellation tightens the existing close; all callers await cleanup. */
    public function close(?Cancellation $cancellation = null): void
    {
        $subscription = $cancellation?->subscribe($this->closeCancellation->cancel(...));
        try {
            if ($this->closeStarted) {
                // Cancellation reaches the driver, never abandons this completion await.
                $this->closeCompletion->getFuture()->await();

                return;
            }
            $this->closeStarted = true;
            try {
                $this->executor->close();
                $this->connection->close($this->closeCancellation->getCancellation());
                $this->closeCompletion->complete();
            } catch (\Throwable $error) {
                $this->closeCompletion->error($error);
                throw $error;
            }
        } finally {
            if (null !== $subscription) {
                $cancellation?->unsubscribe($subscription);
            }
        }
    }

    private function invalidReply(string $message): never
    {
        try {
            $this->close();
        } finally {
            // Cleanup must not replace the application-reply diagnostic.
            throw new \RuntimeException($message.' Outcome may be unknown; storage is closed.');
        }
    }

    private function requireOutcome(OperationResultDTO $result, Outcome $expected, string $description): void
    {
        if ($result->outcome !== $expected) {
            $this->invalidReply($description.' must have '.$expected->value.' outcome.');
        }
    }

    /** @param array<string, mixed> $parameters */
    private function run(QueueStorageOperation $operation, array $parameters): OperationResultDTO
    {
        if ($this->closeStarted) {
            throw new \RuntimeException('Queue storage is closed.');
        }

        if (QueueStorageOperation::Insert === $operation || QueueStorageOperation::Claim === $operation) {
            $parameters[QueueWorkerProtocol::MAX_PAYLOAD_PARAMETER] = $this->limits->maxPayload;
        }
        $result = $this->executor->run(QueueWorkerProtocol::OPERATION_NAME, ['operation' => $operation->value, ...$parameters], QueueStorageOperation::Eligibility === $operation ? Mode::ReadOnly : Mode::Transaction);
        if (\array_key_exists('validation_error', $result->data)) {
            $this->requireOutcome($result, Outcome::Rollback, 'Worker validation reply');
            if (QueueStorageOperation::Insert !== $operation && QueueStorageOperation::Keepalive !== $operation) {
                $this->invalidReply('Worker validation reply is only supported for insert or keepalive.');
            }
            if (['validation_error'] !== array_keys($result->data)) {
                $this->invalidReply('Worker validation reply must not contain success data.');
            }
            $code = $result->data['validation_error'];
            if (!\is_string($code)) {
                $this->invalidReply('Worker validation error code must be a string.');
            }
            $error = QueueValidationError::tryFrom($code);
            if (null === $error) {
                $this->invalidReply('Worker validation error code is unknown.');
            }
            if (QueueValidationError::NegativeKeepalive === $error && QueueStorageOperation::Keepalive !== $operation) {
                $this->invalidReply('Worker keepalive validation category requires a keepalive operation.');
            }
            if (QueueValidationError::NegativeDelay === $error && QueueStorageOperation::Insert !== $operation) {
                $this->invalidReply('Worker delay validation category requires an insert operation.');
            }

            throw new \InvalidArgumentException($error->message());
        }

        return $result;
    }

    private static function scalar(SqliteWorkerOperationConnection $connection, string $sql): mixed
    {
        $result = $connection->query($sql);
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
}
