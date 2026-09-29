<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Command;

/** Finite CLI lifecycle event names emitted as JSON lines. */
enum BrokerEvent: string
{
    case Ready = 'ready';
    case Stopped = 'stopped';
    case Failed = 'failed';
}
