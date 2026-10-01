<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

/** Broker lifecycle event names shared with the foreground command. */
enum BrokerEventEnum: string
{
    case Ready = 'ready';
    case Stopped = 'stopped';
    case Failed = 'failed';
}
