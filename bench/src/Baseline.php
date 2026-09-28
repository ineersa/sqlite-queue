<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Symfony\Component\Messenger\Bridge\Doctrine\Transport\Connection as DoctrineTransportConnection;
use Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransport;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
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
     * @return array{journal_mode: string, synchronous: int, busy_timeout: int, wal_autocheckpoint: int, database: string, file_backed: bool}
     */
    public static function durability(DbalConnection $connection): array
    {
        $parameters = $connection->getParams();
        $database = (string) ($parameters['path'] ?? ':memory:');

        return [
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
        ?SerializerInterface $serializer = null,
    ): DoctrineTransport {
        $transportConnection = new DoctrineTransportConnection([
            'table_name' => Config::MESSENGER_TABLE,
            'queue_name' => $queue,
            'redeliver_timeout' => Config::REDELIVER_TIMEOUT_S,
            'auto_setup' => true,
        ], $connection);

        return new DoctrineTransport($transportConnection, $serializer ?? new PhpSerializer());
    }

    /**
     * @return array<string, int> pending rows per queue
     */
    public static function inventory(DbalConnection $connection): array
    {
        if (!self::tableExists($connection)) {
            return [];
        }

        $inventory = [];
        /** @var array<string, mixed> $row */
        foreach ($connection->executeQuery(\sprintf(
            'SELECT queue_name, COUNT(*) AS pending FROM %s GROUP BY queue_name ORDER BY queue_name',
            Config::MESSENGER_TABLE,
        ))->fetchAllAssociative() as $row) {
            $inventory[(string) $row['queue_name']] = (int) $row['pending'];
        }

        return $inventory;
    }

    public static function tableExists(DbalConnection $connection): bool
    {
        $count = $connection->executeQuery(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            [Config::MESSENGER_TABLE],
        )->fetchOne();

        return (int) $count > 0;
    }

    /**
     * Records the checkpoint result, which also bounds the size of the kept artifacts.
     *
     * @return array{busy: int, log: int, checkpointed: int}
     */
    public static function checkpoint(DbalConnection $connection): array
    {
        /** @var array<string, mixed>|false $row */
        $row = $connection->executeQuery('PRAGMA wal_checkpoint(TRUNCATE)')->fetchAssociative();

        if (!\is_array($row)) {
            return ['busy' => -1, 'log' => -1, 'checkpointed' => -1];
        }

        return [
            'busy' => (int) $row['busy'],
            'log' => (int) $row['log'],
            'checkpointed' => (int) $row['checkpointed'],
        ];
    }

    /**
     * Row count of a message id, read from a second connection.
     *
     * The publisher calls this after a send returned to prove the row is already durable on
     * another connection, which is what commit-before-confirmation means in practice.
     */
    public static function rowExists(DbalConnection $connection, string $messageId): bool
    {
        if (!self::tableExists($connection)) {
            return false;
        }

        $count = $connection->executeQuery(
            \sprintf('SELECT COUNT(*) FROM %s WHERE id = ?', Config::MESSENGER_TABLE),
            [$messageId],
        )->fetchOne();

        return (int) $count > 0;
    }
}
