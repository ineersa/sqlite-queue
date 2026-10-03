<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Operation: string
{
    case Send = 'send';
    case Receive = 'receive';
    case Delivery = 'delivery';
    case Handler = 'handler';
    case Ack = 'ack';
    case Reject = 'reject';
    case Control = 'control';
}
