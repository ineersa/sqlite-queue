<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Bounded internal diagnostics. Never include operation parameters. */
final class SqliteWorkerDiagnostic
{
    private const int MAX_BYTES = 1_024;

    private function __construct()
    {
    }

    public static function text(string $message): string
    {
        if (!mb_check_encoding($message, 'UTF-8')) {
            return 'Non-UTF-8 diagnostic.';
        }

        return mb_strcut($message, 0, self::MAX_BYTES, 'UTF-8');
    }

    public static function failure(mixed $error): string
    {
        if (!\is_array($error)) {
            return 'Worker reported a storage failure.';
        }
        $parts = [];
        foreach (['phase', 'sqlstate', 'message'] as $field) {
            if (\is_string($error[$field] ?? null) && '' !== $error[$field]) {
                $parts[] = $field.'='.self::text($error[$field]);
            }
        }

        return [] === $parts ? 'Worker reported a storage failure.' : self::text(implode('; ', $parts));
    }
}
