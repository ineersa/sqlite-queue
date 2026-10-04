<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerContextFactory;
use Symfony\Component\Filesystem\Exception\IOException;
use Symfony\Component\Filesystem\Filesystem;

use function Amp\Socket\listen;

/**
 * Acquires every resource a Broker needs, then hands over a fully initialized service.
 *
 * Steps release in reverse order when a later step fails, so a partial startup never
 * keeps lifetime locks, a socket, or a SQLite worker. Failed steps are best-effort:
 * the original failure is what the caller receives.
 */
final class BrokerFactory
{
    private const int BUSY_TIMEOUT_MS = 5000;
    /** Budget for observing the SQLite worker pipes while a failed startup releases resources, in seconds. */
    private const int RELEASE_BUDGET_SECONDS = 5;

    /**
     * @param SqliteSynchronousMode  $synchronous       NORMAL is the product default; FULL enables stronger commit durability
     * @param int                    $visibilityTimeout redelivery delay in milliseconds; defaults to Queue::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS
     * @param (\Closure(): int)|null $clock             deterministic millisecond clock for tests; the wall clock otherwise
     * @param ?Cancellation          $cancellation      cooperative cancellation for the blocking startup only; serving cancellation stays on Broker::run()
     */
    public function __construct(
        private readonly string $database,
        private readonly string $endpoint,
        private readonly int $visibilityTimeout = Queue::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS,
        private readonly ?\Closure $clock = null,
        private readonly ?Cancellation $cancellation = null,
        private readonly SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal,
    ) {
        SqliteQueueStorage::validateSynchronousMode($synchronous);
    }

    public function create(): Broker
    {
        // Capability check before acquisition: private path validation calls posix_geteuid(),
        // and a disabled extension would otherwise surface as an undefined-function Error
        // halfway through startup.
        if (!\function_exists('posix_geteuid')) {
            throw new \RuntimeException('The broker requires the posix extension to validate file ownership.');
        }
        /** @var list<callable(): void> */
        $release = [];
        $filesystem = new Filesystem();
        $workerFactory = new SqliteWorkerContextFactory();
        try {
            $locks = new BrokerLifetimeLocks($this->database, $this->endpoint);
            $release[] = $locks->close(...);
            $this->prepareDatabaseFile($locks->database, $filesystem);
            clearstatcache(true, $locks->endpoint);
            if ($filesystem->exists($locks->endpoint) || is_link($locks->endpoint)) {
                throw new \RuntimeException('Endpoint already exists; it will not be removed without verified ownership.');
            }
            $config = (new SqliteConfig($locks->database))
                ->withJournalMode(SqliteJournalMode::Wal)
                ->withSynchronousMode($this->synchronous)
                ->withTransactionMode(SqliteTransactionMode::Immediate)
                ->withBusyTimeout(self::BUSY_TIMEOUT_MS);
            $connection = (new SqliteConnector($workerFactory))->connect($config, $this->cancellation);
            $worker = $workerFactory->worker();
            // Pushed before the connection so cleanup still closes the connection first.
            $release[] = static function () use ($worker): void {
                $worker->close(new TimeoutCancellation(self::RELEASE_BUDGET_SECONDS));
            };
            $release[] = $connection->close(...);
            // Storage becomes the sole connection owner after construction succeeds.
            $storage = new SqliteQueueStorage($connection);
            array_pop($release);
            $release[] = $storage->close(...);
            $clock = $this->clock ?? static fn (): int => (int) floor(microtime(true) * 1000);
            $queue = new Queue($storage, $this->visibilityTimeout, $clock);
            $mask = umask(0077);
            try {
                $server = listen('unix://'.$locks->endpoint);
            } finally {
                umask($mask);
            }
            $release[] = $server->close(...);
            $socketIdentity = SocketIdentity::fromEndpoint($locks->endpoint);
            $release[] = static function () use ($socketIdentity, $locks, $filesystem): void {
                if ($socketIdentity->matches($locks->endpoint)) {
                    $filesystem->remove($locks->endpoint);
                }
            };
            try {
                $filesystem->chmod($locks->endpoint, 0600);
            } catch (IOException $error) {
                throw new \RuntimeException('Cannot make socket private.', 0, $error);
            }

            return new Broker($server, $queue, $storage, $worker, $locks, $socketIdentity, $clock);
        } catch (\Throwable $error) {
            // Kill any spawned child before graceful releases: connect() may fail after the
            // connector starts the worker but before the handle is assigned, and a stuck
            // worker would block the connection close below.
            try {
                $workerFactory->forceStopAll();
            } catch (\Throwable) {
                // Best-effort: the original failure is what the caller receives.
            }
            foreach (array_reverse($release) as $step) {
                try {
                    $step();
                } catch (\Throwable) {
                }
            }
            throw $error;
        }
    }

    private function prepareDatabaseFile(string $path, Filesystem $filesystem): void
    {
        clearstatcache(true, $path);
        if (is_link($path)) {
            throw new \RuntimeException('Database path must be a regular file, not a symlink.');
        }
        if (!$filesystem->exists($path)) {
            $mask = umask(0077);
            try {
                $filesystem->touch($path);
            } catch (IOException $error) {
                throw new \RuntimeException('Cannot open database file.', 0, $error);
            } finally {
                umask($mask);
            }
            try {
                $filesystem->chmod($path, 0600);
            } catch (IOException $error) {
                throw new \RuntimeException('Cannot open database file.', 0, $error);
            }
            clearstatcache(true, $path);
        }
        // Native stat has no Symfony equivalent; checking here keeps the hardlink, owner,
        // and mode defenses on the database file that this broker will actually use.
        $stat = stat($path);
        if (false === $stat || !is_file($path) || $stat['uid'] !== posix_geteuid() || 0 !== ($stat['mode'] & 0077) || 1 !== $stat['nlink']) {
            throw new \RuntimeException('Database unavailable or file is not private and singly linked.');
        }
    }
}
