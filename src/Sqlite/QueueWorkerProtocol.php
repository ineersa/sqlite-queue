<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\ProtocolLimits;

final class QueueWorkerProtocol
{
    public const string OPERATION_NAME = 'queue';
    public const int MAXIMUM_RESULT_ROWS = 1;
    /** Covers serialized scalar fields, column names, BLOB wrappers and operation outcome. */
    public const int RESULT_METADATA_BYTES = ProtocolLimits::RESULT_METADATA_BYTES;
    /** Default worker result-byte budget for the published payload limit. */
    public const int MAXIMUM_RESULT_BYTES = Limits::MAX_PAYLOAD + self::RESULT_METADATA_BYTES;
    /** Per-call parameter that carries the broker's configured payload budget into the worker. */
    public const string MAX_PAYLOAD_PARAMETER = 'max_payload';
}
