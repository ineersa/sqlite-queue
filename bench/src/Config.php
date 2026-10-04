<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Shared queue and actor settings for the native comparisons. */
final class Config
{
    public const METHOD_REVISION = 'native-messenger-diagnostic';
    public const SMALL_PAYLOAD_BYTES = 256;
    public const LARGE_PAYLOAD_BYTES = 16384;
    public const BUSY_TIMEOUT_MS = 5000;
    public const REDELIVER_TIMEOUT_S = 3600;
    public const STARTUP_TIMEOUT_S = 20;
    public const KILL_GRACE_S = 5;
    public const EXPECTED_JOURNAL_MODE = 'wal';
    public const MESSENGER_TABLE = 'messenger_messages';
}
