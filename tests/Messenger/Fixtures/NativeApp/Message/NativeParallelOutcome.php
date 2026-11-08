<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message;

enum NativeParallelOutcome: string
{
    case Success = 'success';
    case Failure = 'failure';
}
