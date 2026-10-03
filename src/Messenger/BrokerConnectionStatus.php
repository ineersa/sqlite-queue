<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

enum BrokerConnectionStatus: string
{
    case Unconnected = 'unconnected';
    case Connecting = 'connecting';
    case Closed = 'closed';
}
