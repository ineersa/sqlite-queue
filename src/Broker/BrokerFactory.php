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

use function Amp\Socket\listen;

/**
 * Acquires every resource a Broker needs, then hands over a fully initialized service.
 *
 * Steps release in reverse order when a later step fails, so a partial startup never
 * keeps ownership, a socket, or a persistence child. Failed steps are best-effort:
 * the original failure is what the caller receives.
 */
final class BrokerFactory
{
    private const int BUSY_TIMEOUT_MS = 5000;
    /** Budget for observing the persistence pipes while a failed startup releases resources, in seconds. */
    private const int RELEASE_BUDGET_SECONDS = 5;

    /**
     * @param int                    $visibilityTimeout redelivery delay in milliseconds; 5000 is the approved product default
     * @param (\Closure(): int)|null $clock             deterministic millisecond clock for tests; the wall clock otherwise
     * @param ?Cancellation          $cancellation      cooperative cancellation for the blocking startup only; serving cancellation stays on Broker::run()
     */
    public function __construct(
        private readonly string $database,
        private readonly string $endpoint,
        private readonly int $visibilityTimeout = 5000,
        private readonly ?\Closure $clock = null,
        private readonly ?Cancellation $cancellation = null,
    ) {
    }

    public function listen(): Broker
    {
        // Capability check before acquisition: private path validation calls posix_geteuid(),
        // and a disabled extension would otherwise surface as an undefined-function Error
        // halfway through startup.
        if (!\function_exists('posix_geteuid')) {
            throw new \RuntimeException('The broker requires the posix extension to validate file ownership.');
        }
        /** @var list<callable(): void> */
        $release = [];
        $persistenceFactory = new PersistenceFactory();
        try {
            $ownership = new Ownership($this->database, $this->endpoint);
            $release[] = $ownership->close(...);
            $config = (new SqliteConfig($ownership->database))
                ->withJournalMode(SqliteJournalMode::Wal)
                ->withSynchronousMode(SqliteSynchronousMode::Full)
                ->withTransactionMode(SqliteTransactionMode::Immediate)
                ->withBusyTimeout(self::BUSY_TIMEOUT_MS);
            $connection = (new SqliteConnector($persistenceFactory))->connect($config, $this->cancellation);
            $persistence = $persistenceFactory->persistence();
            // Pushed before the connection so cleanup still closes the connection first.
            $release[] = static function () use ($persistence): void {
                $persistence->close(new TimeoutCancellation(self::RELEASE_BUDGET_SECONDS));
            };
            $release[] = $connection->close(...);
            // Queue closes the transferred connection when its own initialization fails.
            $queue = new Queue($connection, $this->visibilityTimeout, $this->clock);
            $release[] = $queue->close(...);
            $mask = umask(0077);
            try {
                $server = listen('unix://'.$ownership->endpoint);
            } finally {
                umask($mask);
            }
            $release[] = $server->close(...);
            $ownership->recordSocket();

            return new Broker($server, $queue, $connection, $persistence, $ownership);
        } catch (\Throwable $error) {
            // Kill any spawned child before graceful releases: connect() may fail after the
            // connector starts the worker but before the handle is assigned, and a wedged
            // worker would block the connection close below.
            try {
                $persistenceFactory->forceStopAll();
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
}
