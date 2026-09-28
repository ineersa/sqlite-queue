<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

enum ErrorCode: string
{
    case FrameTooLarge = 'frame_too_large';
    case InvalidRequest = 'invalid_request';
    case UnsupportedProtocolVersion = 'unsupported_protocol_version';
    case StaleReceipt = 'stale_receipt';
    case InvalidQueueName = 'invalid_queue_name';
    case BrokerShuttingDown = 'broker_shutting_down';
    case InternalStorageFailure = 'internal_storage_failure';
}
