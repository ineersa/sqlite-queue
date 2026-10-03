<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;

/** Read-only startup instrumentation; no production diagnostic API or serving-time SQL hook. */
final class BrokerReadback
{
    /** @return array<string, int|string|bool> */
    public static function durability(Broker $broker, string $database): array
    {
        // The initialized factory result is not serving yet. Inspect the actual worker-owned
        // connection, not a second connection whose synchronous setting proves nothing about it.
        $storage = (new \ReflectionProperty(Broker::class, 'storage'))->getValue($broker);
        if (!$storage instanceof SqliteQueueStorage) {
            throw new \RuntimeException('Cannot observe broker storage.');
        }
        $connection = (new \ReflectionProperty(SqliteQueueStorage::class, 'connection'))->getValue($storage);
        if (!$connection instanceof SqliteConnection) {
            throw new \RuntimeException('Cannot observe broker connection.');
        }
        $mode = $connection->getTransactionIsolation();
        if (SqliteTransactionMode::Immediate !== $mode) {
            throw new \RuntimeException('Broker requires immediate transactions.');
        }
        $settings = ['database' => $database, 'file_backed' => true, 'transaction_mode' => $mode->value];
        foreach (['journal_mode', 'synchronous', 'busy_timeout', 'wal_autocheckpoint'] as $pragma) {
            $result = $connection->query('PRAGMA '.$pragma);
            try {
                $row = $result->fetchRow();
                $field = 'busy_timeout' === $pragma ? 'timeout' : $pragma;
                $value = $row[$field] ?? null;
                if (!\is_int($value) && !\is_string($value)) {
                    throw new \RuntimeException('Missing broker PRAGMA '.$pragma);
                }
                $settings[$pragma] = $value;
            } finally {
                $result->close();
            }
        }

        return $settings;
    }
}
