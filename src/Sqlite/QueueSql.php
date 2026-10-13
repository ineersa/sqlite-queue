<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

final class QueueSql
{
    public const string INSERT_SQL = 'INSERT INTO queue_messages (queue, body, headers, available_at) VALUES (?, ?, ?, ?)';
    public const string ELIGIBLE_SQL = 'SELECT id, body, headers, available_at FROM queue_messages WHERE queue = ? AND available_at <= ?
                     AND (reserved_until IS NULL OR reserved_until <= ?) ORDER BY id LIMIT 1';
    public const string RESERVE_SQL = 'UPDATE queue_messages SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
                     WHERE id = ? AND queue = ? AND available_at <= ? AND (reserved_until IS NULL OR reserved_until <= ?)';
    public const string DELETE_SQL = 'DELETE FROM queue_messages WHERE id = ? AND reservation_token = ? AND owner_id = ? AND broker_epoch = ? AND reserved_until > ?';
    public const string DIAGNOSE_SQL = 'SELECT owner_id, typeof(owner_id) AS owner_id_type,
                            broker_epoch, typeof(broker_epoch) AS broker_epoch_type,
                            reservation_token, typeof(reservation_token) AS reservation_token_type,
                            reserved_until, typeof(reserved_until) AS reserved_until_type
                     FROM queue_messages WHERE id = ?';
    public const string EARLIEST_SQL = 'SELECT max(available_at, coalesce(reserved_until, available_at)) AS ready_at
                     FROM queue_messages WHERE queue = ?
                     ORDER BY ready_at LIMIT 1';
    public const string SCHEMA_SQL = <<<'SQL'
        CREATE TABLE IF NOT EXISTS queue_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            body BLOB NOT NULL CHECK (typeof(body) = 'blob'),
            headers BLOB NOT NULL CHECK (typeof(headers) = 'blob'),
            available_at INTEGER NOT NULL CHECK (available_at >= 0),
            reserved_until INTEGER,
            reservation_token TEXT,
            owner_id TEXT,
            broker_epoch TEXT,
            CHECK (
                (reserved_until IS NULL AND reservation_token IS NULL AND owner_id IS NULL AND broker_epoch IS NULL)
                OR (reserved_until IS NOT NULL AND reservation_token IS NOT NULL AND owner_id IS NOT NULL AND broker_epoch IS NOT NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS queue_messages_order ON queue_messages (queue, id);
        CREATE INDEX IF NOT EXISTS queue_messages_ready ON queue_messages (
            queue,
            max(available_at, coalesce(reserved_until, available_at))
        );
        SQL;
}
