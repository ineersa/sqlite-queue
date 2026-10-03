<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Symfony\Component\Messenger\Bridge\Doctrine\Transport\Connection as DoctrineTransportConnection;
use Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransport;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * The standard Symfony Messenger Doctrine SQLite transport, configured for this benchmark.
 *
 * This is the baseline under measurement. It is the upstream bridge, used through its public
 * API, with an explicitly configured connection: the benchmark never relies on constructor
 * defaults for durability.
 */
final class Baseline
{
    public static function connect(string $databasePath): DbalConnection
    {
        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'path' => $databasePath,
            'driverOptions' => [\Pdo\Sqlite::ATTR_TRANSACTION_MODE => \Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE],
        ]);

        // WAL is persistent per database file; synchronous and busy_timeout are per connection
        // and are applied on every connection this benchmark opens.
        $connection->executeStatement('PRAGMA journal_mode='.Config::EXPECTED_JOURNAL_MODE);
        $connection->executeStatement('PRAGMA synchronous=FULL');
        $connection->executeStatement('PRAGMA busy_timeout='.Config::BUSY_TIMEOUT_MS);
        $connection->executeStatement('PRAGMA wal_autocheckpoint=1000');

        return $connection;
    }

    /**
     * @return array{journal_mode: string, synchronous: int, busy_timeout: int, wal_autocheckpoint: int, database: string, file_backed: bool, transaction_mode: string, native_transaction_mode: int}
     */
    public static function durability(DbalConnection $connection): array
    {
        $parameters = $connection->getParams();
        $database = (string) ($parameters['path'] ?? ':memory:');
        $native = $connection->getNativeConnection();
        if (!$native instanceof \Pdo\Sqlite) {
            throw new \RuntimeException('Benchmark baseline requires native PDO SQLite.');
        }
        $mode = $native->getAttribute(\Pdo\Sqlite::ATTR_TRANSACTION_MODE);
        if (\Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE !== $mode) {
            throw new \RuntimeException('Benchmark baseline requires immediate transactions.');
        }

        return [
            'transaction_mode' => 'immediate',
            'native_transaction_mode' => $mode,
            'journal_mode' => strtolower((string) $connection->executeQuery('PRAGMA journal_mode')->fetchOne()),
            'synchronous' => (int) $connection->executeQuery('PRAGMA synchronous')->fetchOne(),
            'busy_timeout' => (int) $connection->executeQuery('PRAGMA busy_timeout')->fetchOne(),
            'wal_autocheckpoint' => (int) $connection->executeQuery('PRAGMA wal_autocheckpoint')->fetchOne(),
            'database' => $database,
            'file_backed' => '' !== $database && ':memory:' !== $database,
        ];
    }

    /**
     * @param array{journal_mode: string, synchronous: int, file_backed: bool} $durability
     */
    public static function isDurabilityEquivalent(array $durability): bool
    {
        return Config::EXPECTED_JOURNAL_MODE === $durability['journal_mode']
            && Config::EXPECTED_SYNCHRONOUS === $durability['synchronous']
            && true === $durability['file_backed'];
    }

    public static function transport(
        DbalConnection $connection,
        string $queue,
        SerializerInterface $serializer,
    ): DoctrineTransport {
        $transportConnection = new DoctrineTransportConnection([
            'table_name' => Config::MESSENGER_TABLE,
            'queue_name' => $queue,
            'redeliver_timeout' => Config::REDELIVER_TIMEOUT_S,
            'auto_setup' => true,
        ], $connection);

        return new DoctrineTransport($transportConnection, $serializer);
    }
}
