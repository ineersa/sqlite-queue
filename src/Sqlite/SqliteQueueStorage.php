<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\Sync\LocalSemaphore;
use Amp\Sync\Semaphore;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode as DriverSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Fabpot\Amp\Sqlite\SqliteWorkerOperationConnection;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Fabpot\Amp\Sqlite\Worker\OperationExecutor;

/** Complete queue operations over the driver's bounded, exclusively owned pipeline. */
final class SqliteQueueStorage
{
    private const int BUSY_TIMEOUT_MILLISECONDS = 5_000;
    private const int WAL_AUTOCHECKPOINT_PAGES = 1_000;
    private const string SQLITE_MINIMUM_VERSION = '3.31.0';
    private bool $closed = false;

    /** @param array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string} $configuration */
    private function __construct(
        private readonly SqliteWorkerOperationConnection $connection,
        private readonly OperationExecutor $executor,
        private readonly array $configuration,
        private readonly int $pipelineDepth,
        private readonly Semaphore $eligibilitySlots,
    ) {
    }

    /**
     * Cancellation applies to acquisition. The operation class injects its worker clock.
     *
     * @param class-string<Operation> $workerOperation
     */
    public static function open(string $path, SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal, ?Cancellation $cancellation = null, string $workerOperation = QueueWorkerOperation::class, int $pipelineDepth = QueuePipeline::DEFAULT_DEPTH): self
    {
        QueuePipeline::validateDepth($pipelineDepth);
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
            ->withPragma('wal_autocheckpoint', self::WAL_AUTOCHECKPOINT_PAGES)
            ->withWorkerOperations([QueueWorkerProtocol::OPERATION_NAME => $workerOperation]);

        return self::fromConnection((new SqliteConnector())->connect($config, $cancellation), $synchronous, $pipelineDepth);
    }

    /** Transfers an acquired connection, including cleanup on startup failure. */
    public static function fromConnection(SqliteWorkerOperationConnection $connection, SqliteSynchronousMode $synchronous, int $pipelineDepth = QueuePipeline::DEFAULT_DEPTH): self
    {
        try {
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
            $executor = $connection->createWorkerOperationExecutor(QueueWorkerProtocol::MAXIMUM_RESULT_BYTES, QueueWorkerProtocol::MAXIMUM_RESULT_ROWS, QueuePipeline::limits($pipelineDepth));

            return new self($connection, $executor, $configuration, $pipelineDepth, new LocalSemaphore(QueuePipeline::ELIGIBILITY_SLOTS));
        } catch (\Throwable $error) {
            $connection->close();
            throw $error;
        }
    }

    public function synchronousMode(): SqliteSynchronousMode
    {
        return SqliteSynchronousMode::from($this->configuration['synchronous']);
    }

    public function pipelineDepth(): int
    {
        return $this->pipelineDepth;
    }

    /** Startup snapshot, acquired before exclusive executor ownership.
     * @return array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string}
     */
    public function configuration(): array
    {
        return $this->configuration;
    }

    /** Null cancellation means the public caller supplied no cancellation. */
    public function insert(string $queue, string $body, string $headers, int $delay, ?Cancellation $cancellation = null): int
    {
        $data = $this->run(QueueStorageOperation::Insert, ['queue' => $queue, 'body' => $body, 'headers' => $headers, 'delay' => $delay], $cancellation);
        if (!\is_int($data['id'] ?? null)) {
            $this->invalidReply('Worker insert reply must contain an integer identity.');
        }

        return $data['id'];
    }

    /** @return ?array{id: int, body: string, headers: string, available_at: int, reserved_until: int} */
    public function claim(string $queue, string $ownerId, string $epoch, string $token, int $visibility, ?Cancellation $cancellation = null): ?array
    {
        $data = $this->run(QueueStorageOperation::Claim, ['queue' => $queue, 'owner' => $ownerId, 'epoch' => $epoch, 'token' => $token, 'visibility' => $visibility], $cancellation);
        if (!\array_key_exists('message', $data)) {
            $this->invalidReply('Worker claim reply is missing its message field.');
        }
        $message = $data['message'];
        if (null === $message) {
            return null;
        }
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

    public function settle(int $id, string $token, string $ownerId, string $epoch, ?Cancellation $cancellation = null): SettlementResultEnum
    {
        $data = $this->run(QueueStorageOperation::Settle, ['id' => $id, 'token' => $token, 'owner' => $ownerId, 'epoch' => $epoch], $cancellation);
        if (!\is_string($data['settlement'] ?? null)) {
            $this->invalidReply('Worker settlement reply must contain a status.');
        }
        $settlement = SettlementResultEnum::tryFrom($data['settlement']);
        if (null === $settlement) {
            $this->invalidReply('Worker settlement status is unknown.');
        }

        return $settlement;
    }

    public function earliestEligibility(string $queue, ?Cancellation $cancellation = null): ?int
    {
        // Selected queues are bounded by broker connections and WAIT_ANY membership.
        // Limit active probes, not mutations, so a large legal watch set cannot fill the lane.
        $slot = $this->eligibilitySlots->acquire();
        try {
            $cancellation?->throwIfRequested();
            $data = $this->run(QueueStorageOperation::Eligibility, ['queue' => $queue], $cancellation);
        } finally {
            $slot->release();
        }
        if (!\array_key_exists('ready_at', $data)) {
            $this->invalidReply('Worker eligibility reply is missing its ready_at field.');
        }
        $readyAt = $data['ready_at'];
        if (null !== $readyAt && !\is_int($readyAt)) {
            $this->invalidReply('Worker eligibility reply must contain an integer or null.');
        }

        return $readyAt;
    }

    /** Null selects the driver's default budget; brokers supply their shutdown cancellation. */
    public function close(?Cancellation $cancellation = null): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->connection->close($cancellation);
    }

    private function invalidReply(string $message): never
    {
        $this->close();
        throw new \RuntimeException($message.' Outcome may be unknown; storage is closed.');
    }

    /** @param array<string, mixed> $parameters
     * @return array<array-key, mixed>
     */
    private function run(QueueStorageOperation $operation, array $parameters, ?Cancellation $cancellation): array
    {
        if ($this->closed) {
            throw new \RuntimeException('Queue storage is closed.');
        }

        $replyBytes = QueueStorageOperation::Claim === $operation ? QueueWorkerProtocol::MAXIMUM_RESULT_BYTES : QueuePipeline::SCALAR_RESULT_BYTES;
        $pending = $this->executor->submit(QueueWorkerProtocol::OPERATION_NAME, ['operation' => $operation->value, ...$parameters], QueueStorageOperation::Eligibility === $operation ? Mode::ReadOnly : Mode::Transaction, $replyBytes, $cancellation);
        try {
            // Cancellation removes queued work, but cannot revoke a dispatched transaction.
            // Observe its validated completion so the broker can notify a known commit.
            return $pending->getFuture()->await()->data;
        } finally {
            $pending->discard();
        }
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
