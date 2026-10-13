<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Ineersa\SqliteQueue\Protocol\Limits;

final class QueueWorkerProtocol
{
    public const string OPERATION_NAME = 'queue';
    public const int MAXIMUM_RESULT_ROWS = 1;
    /** Covers serialized scalar fields, column names, BLOB wrappers and operation outcome. */
    public const int RESULT_METADATA_BYTES = 8_192;
    public const int MAXIMUM_RESULT_BYTES = Limits::MAX_PAYLOAD + self::RESULT_METADATA_BYTES;
}
