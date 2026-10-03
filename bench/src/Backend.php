<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Doctrine\DBAL\Connection;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\TransportInterface;

enum Backend: string
{
    case Doctrine = 'doctrine';
    case Broker = 'broker';

    public function transport(Connection $connection, string $directory, string $queue): TransportInterface
    {
        return match ($this) {
            self::Doctrine => Baseline::transport($connection, $queue),
            self::Broker => (new TransportFactory())->createTransport('sqlite-queue://'.$queue, ['endpoint' => self::endpoint($directory)], new PhpSerializer()),
        };
    }

    public function table(): string
    {
        return self::Doctrine === $this ? Config::MESSENGER_TABLE : 'queue_messages';
    }

    /** A short private path avoids Unix socket path limits in deeply nested checkouts. */
    public static function endpoint(string $directory): string
    {
        return '/tmp/sqbench-'.substr(hash('sha256', $directory), 0, 16).'/queue.sock';
    }

    /** @return array<string, int> */
    public function inventory(Connection $connection): array
    {
        $column = self::Doctrine === $this ? 'queue_name' : 'queue';
        $inventory = [];
        foreach ($connection->fetchAllAssociative('SELECT '.$column.' AS queue, COUNT(*) AS pending FROM '.$this->table().' GROUP BY '.$column) as $row) {
            $inventory[(string) $row['queue']] = (int) $row['pending'];
        }

        return $inventory;
    }

    /** Missing row means no deadline observation, not a synthetic timestamp. */
    public function deadline(Connection $connection, int|string $id): ?float
    {
        $stored = $connection->fetchOne('SELECT available_at FROM '.$this->table().' WHERE id = ?', [$id]);
        if (false === $stored) {
            return null;
        }

        return self::Broker === $this ? (float) $stored / 1000 : (float) (new \DateTimeImmutable((string) $stored, new \DateTimeZone('UTC')))->format('U.u');
    }
}
