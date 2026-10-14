<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures;

enum KeepalivePeerReply: string
{
    case Success = 'success';
    case Expired = 'expired';
    case Malformed = 'malformed';
    case Disconnect = 'disconnect';
}
