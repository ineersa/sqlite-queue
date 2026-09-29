<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

/** Wire control-object field names for protocol v1. */
enum ControlField: string
{
    case Version = 'v';
    case Id = 'id';
    case Operation = 'op';
    case Ok = 'ok';
    case Result = 'result';
    case Error = 'error';
    case Code = 'code';
    case BodyLength = 'body_length';
    case HeadersLength = 'headers_length';
    case Queue = 'queue';
    case Delay = 'delay';
    case Receipt = 'receipt';
    case MaxPayload = 'max_payload';
    case AvailableAt = 'available_at';
    case ReservedUntil = 'reserved_until';
}
