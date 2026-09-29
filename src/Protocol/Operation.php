<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

enum Operation: string
{
    case Hello = 'hello';
    case Send = 'send';
    case Receive = 'receive';
    case Acknowledge = 'acknowledge';
    case Reject = 'reject';
}
