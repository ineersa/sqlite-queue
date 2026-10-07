<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageFailureException;

use function Amp\async;

/**
 * Starts exactly one package-owned PDO worker and returns its parent proxy.
 *
 * Owns failed-startup cleanup. The caller supplies one monotonic startup budget that
 * already covers process spawn, initialization and readiness.
 */
final class SqliteWorkerContextFactory
{
    private const int FAILED_STARTUP_RELEASE_SECONDS = 5;

    /** @var list<SqliteWorkerHandle> */
    private array $created = [];

    /**
     * @param string|non-empty-list<string> $script production worker.php, or a test bootstrap with Amp arguments
     */
    public function __construct(
        private readonly string|array $script = __DIR__.'/worker.php',
    ) {
    }

    public function create(
        string $database,
        int $visibilityTimeout,
        SqliteSynchronousMode $synchronous,
        Cancellation $budget,
    ): SqliteQueueWorker {
        if ($visibilityTimeout <= 0) {
            throw new \InvalidArgumentException('Visibility timeout must be positive milliseconds.');
        }
        if ('' === $database) {
            throw new \InvalidArgumentException('Worker database path must be non-empty.');
        }

        // Inherit the broker environment. A non-empty Amp environment array replaces it.
        $context = (new ProcessContextFactory())->start($this->script, $budget);
        $drainCancellation = new DeferredCancellation();
        $drains = [];
        foreach ([$context->getStdout(), $context->getStderr()] as $stream) {
            $drains[] = async(static function () use ($stream, $drainCancellation): void {
                while (null !== $stream->read($drainCancellation->getCancellation())) {
                }
            });
        }
        $handle = new SqliteWorkerHandle($context, $drains, $drainCancellation);
        $this->created[] = $handle;
        $exchange = async(static function () use ($context, $database, $visibilityTimeout, $synchronous): mixed {
            $context->send([
                'id' => 1,
                'op' => SqliteWorkerOperationEnum::Init->value,
                'data' => [
                    'database' => $database,
                    'visibility_timeout' => $visibilityTimeout,
                    'synchronous' => $synchronous->value,
                ],
            ]);

            return $context->receive();
        });
        try {
            $response = $exchange->await($budget);
            if (!\is_array($response)) {
                throw new StorageFailureException('Worker initialization response must be an array.');
            }
            if (1 !== ($response['id'] ?? null)) {
                throw new StorageFailureException('Worker initialization response id must be 1.');
            }
            if (SqliteWorkerOperationEnum::Init->value !== ($response['op'] ?? null)) {
                throw new StorageFailureException('Worker initialization response operation must be init.');
            }
            if (SqliteWorkerStatusEnum::Failure->value === ($response['status'] ?? null)) {
                throw new StorageFailureException('Worker initialization failed: '.SqliteWorkerDiagnostic::failure($response['error'] ?? null));
            }
            if (SqliteWorkerStatusEnum::Ok->value !== ($response['status'] ?? null)) {
                throw new StorageFailureException('Worker initialization response status must be ok or failure.');
            }
            if (!\is_array($response['result'] ?? null)) {
                throw new StorageFailureException('Worker initialization configuration must be an array.');
            }
            /** @var array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string} $configuration */
            $configuration = $this->validatedConfiguration($response['result'], $synchronous);

            return new SqliteQueueWorker($handle, $synchronous, $configuration);
        } catch (\Throwable $error) {
            $exchange->ignore();
            try {
                $handle->close(new TimeoutCancellation(self::FAILED_STARTUP_RELEASE_SECONDS));
            } catch (\Throwable) {
            }
            throw $error;
        }
    }

    /**
     * @return list<SqliteWorkerHandle>
     */
    public function created(): array
    {
        return $this->created;
    }

    public function forceStopAll(): void
    {
        $failure = null;
        foreach ($this->created as $worker) {
            try {
                $worker->forceStop();
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
        }
        if (null !== $failure) {
            throw $failure;
        }
    }

    /**
     * @param array<array-key, mixed> $result
     *
     * @return array{journal_mode: string, synchronous: string, busy_timeout: int, wal_autocheckpoint: int, sqlite_version: string}
     */
    private function validatedConfiguration(array $result, SqliteSynchronousMode $synchronous): array
    {
        foreach (['journal_mode', 'synchronous', 'busy_timeout', 'wal_autocheckpoint', 'sqlite_version'] as $field) {
            if (!\array_key_exists($field, $result)) {
                throw new StorageFailureException('Worker configuration readback is incomplete.');
            }
        }
        if (!\is_string($result['journal_mode']) || 'wal' !== strtolower($result['journal_mode'])) {
            throw new StorageFailureException('Worker configuration must report WAL journal mode.');
        }
        if (!\is_string($result['synchronous']) || $result['synchronous'] !== $synchronous->value) {
            throw new StorageFailureException('Worker configuration synchronous mode mismatch.');
        }
        if (!\is_int($result['busy_timeout']) || $result['busy_timeout'] < 0) {
            throw new StorageFailureException('Worker configuration busy_timeout is invalid.');
        }
        if (!\is_int($result['wal_autocheckpoint']) || $result['wal_autocheckpoint'] < 0) {
            throw new StorageFailureException('Worker configuration wal_autocheckpoint is invalid.');
        }
        if (!\is_string($result['sqlite_version']) || '' === $result['sqlite_version']) {
            throw new StorageFailureException('Worker configuration sqlite_version is invalid.');
        }

        return [
            'journal_mode' => $result['journal_mode'],
            'synchronous' => $result['synchronous'],
            'busy_timeout' => $result['busy_timeout'],
            'wal_autocheckpoint' => $result['wal_autocheckpoint'],
            'sqlite_version' => $result['sqlite_version'],
        ];
    }
}
