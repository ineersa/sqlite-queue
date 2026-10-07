<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
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
    /** Total budget for worker spawn, initialization, and readiness, in seconds. */
    private const float STARTUP_BUDGET_SECONDS = 15.0;
    /** Budget for observing the SQLite worker while a failed startup releases resources, in seconds. */
    private const float RELEASE_BUDGET_SECONDS = 5.0;

    /**
     * @param SqliteSynchronousMode       $synchronous       NORMAL is the product default; FULL enables stronger commit durability
     * @param int                         $visibilityTimeout redelivery delay in milliseconds; defaults to Queue::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS
     * @param (\Closure(): int)|null      $clock             parent wall-clock milliseconds for notifier timers; never a worker clock
     * @param ?Cancellation               $cancellation      cooperative cancellation for the blocking startup only; serving cancellation stays on Broker::run()
     * @param ?SqliteWorkerContextFactory $workers           optional factory for tests; production constructs the package default
     */
    public function __construct(
        private readonly string $database,
        private readonly string $endpoint,
        private readonly int $visibilityTimeout = Queue::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS,
        private readonly ?\Closure $clock = null,
        private readonly ?Cancellation $cancellation = null,
        private readonly SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal,
        private readonly ?SqliteWorkerContextFactory $workers = null,
    ) {
        if ($visibilityTimeout <= 0) {
            throw new \InvalidArgumentException('Visibility timeout must be positive milliseconds.');
        }
    }

    public function create(): Broker
    {
        // Capability check before acquisition: private path validation calls posix_geteuid(),
        // and a disabled extension would otherwise surface as an undefined-function Error
        // halfway through startup.
        if (!\function_exists('posix_geteuid')) {
            throw new \RuntimeException('The broker requires the posix extension to validate file ownership.');
        }
        if (!\extension_loaded('pdo_sqlite')) {
            throw new \RuntimeException('The broker requires the pdo_sqlite extension.');
        }
        /** @var list<callable(): void> */
        $release = [];
        $filesystem = new Filesystem();
        $workers = $this->workers ?? new SqliteWorkerContextFactory();
        try {
            $locks = new BrokerLifetimeLocks($this->database, $this->endpoint);
            $release[] = $locks->close(...);
            $this->prepareDatabaseFile($locks->database, $filesystem);
            clearstatcache(true, $locks->endpoint);
            if ($filesystem->exists($locks->endpoint) || is_link($locks->endpoint)) {
                throw new \RuntimeException('Endpoint already exists; it will not be removed without verified ownership.');
            }

            $startup = new TimeoutCancellation(self::STARTUP_BUDGET_SECONDS);
            $budget = null === $this->cancellation
                ? $startup
                : new CompositeCancellation($this->cancellation, $startup);
            $worker = $workers->create(
                $locks->database,
                $this->visibilityTimeout,
                $this->synchronous,
                $budget,
            );
            $release[] = static function () use ($worker): void {
                $worker->close(new TimeoutCancellation(self::RELEASE_BUDGET_SECONDS));
            };

            $clock = $this->clock ?? static fn (): int => (int) floor(microtime(true) * 1000);
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

            return new Broker($server, $worker, $locks, $socketIdentity, $clock);
        } catch (\Throwable $error) {
            try {
                $workers->forceStopAll();
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
