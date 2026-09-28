<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Driver;

use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;

/**
 * The async client applies its own journal and synchronous defaults. These tests record
 * what the driver sets so the queue engine can ask for durability explicitly instead of
 * inheriting whatever the constructor picks.
 *
 * Connections close in a finally block. close() is idempotent, so cleanup holds even when
 * an assertion fails.
 */
final class DurabilitySettingsTest extends DriverTestCase
{
    private const SYNCHRONOUS_NORMAL = 1;
    private const SYNCHRONOUS_FULL = 2;

    public function testExplicitWalAndFullAreAppliedToTheDatabase(): void
    {
        $this->runAsync(function (): void {
            $config = (new SqliteConfig($this->database->path()))
                ->withJournalMode(SqliteJournalMode::Wal)
                ->withSynchronousMode(SqliteSynchronousMode::Full);

            $connection = (new SqliteConnector())->connect($config);

            try {
                self::assertSame('wal', $connection->query('PRAGMA journal_mode')->fetchRow()['journal_mode']);
                self::assertSame(self::SYNCHRONOUS_FULL, $connection->query('PRAGMA synchronous')->fetchRow()['synchronous']);
            } finally {
                $connection->close();
            }
        });
    }

    public function testFileDatabaseDefaultsToWalAndNormalSynchronous(): void
    {
        $this->runAsync(function (): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));

            try {
                self::assertSame('wal', $connection->query('PRAGMA journal_mode')->fetchRow()['journal_mode']);
                self::assertSame(self::SYNCHRONOUS_NORMAL, $connection->query('PRAGMA synchronous')->fetchRow()['synchronous']);
            } finally {
                $connection->close();
            }
        });
    }

    /**
     * An in-memory database cannot use the WAL journal, and its default synchronous mode is
     * already FULL. The documented durability table must match this measurement.
     */
    public function testMemoryDatabaseUsesMemoryJournalAndFullSynchronous(): void
    {
        $this->runAsync(static function (): void {
            $connection = (new SqliteConnector())->connect(new SqliteConfig(':memory:'));

            try {
                self::assertSame('memory', $connection->query('PRAGMA journal_mode')->fetchRow()['journal_mode']);
                self::assertSame(self::SYNCHRONOUS_FULL, $connection->query('PRAGMA synchronous')->fetchRow()['synchronous']);
            } finally {
                $connection->close();
            }
        });
    }

    public function testDurabilityOptionsCannotBeOverriddenThroughRawPragmas(): void
    {
        $config = new SqliteConfig($this->database->path());

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('dedicated configuration option');

        $config->withPragma('journal_mode', 'delete');
    }

    /**
     * This proves that a committed write survives close and reopen with explicit WAL and
     * FULL. It does not simulate power loss, so it is a commit-persistence check, not a
     * durability guarantee under a crash.
     */
    public function testCommittedWriteSurvivesCloseAndReopenWithExplicitWalAndFull(): void
    {
        $databasePath = $this->database->path();

        $this->runAsync(static function () use ($databasePath): void {
            $config = static fn (): SqliteConfig => (new SqliteConfig($databasePath))
                ->withJournalMode(SqliteJournalMode::Wal)
                ->withSynchronousMode(SqliteSynchronousMode::Full);

            $connection = (new SqliteConnector())->connect($config());

            try {
                $connection->execute('CREATE TABLE durable (id INTEGER PRIMARY KEY, body TEXT NOT NULL)');
                $connection->execute('INSERT INTO durable (body) VALUES (?)', ['kept']);

                // A truncating checkpoint folds the write-ahead log into the main file.
                $checkpoint = $connection->query('PRAGMA wal_checkpoint(TRUNCATE)')->fetchRow();
                self::assertSame(0, $checkpoint['busy']);
            } finally {
                $connection->close();
            }

            $reopened = (new SqliteConnector())->connect($config());

            try {
                self::assertSame(['body' => 'kept'], $reopened->query('SELECT body FROM durable')->fetchRow());
            } finally {
                $reopened->close();
            }
        });
    }
}
