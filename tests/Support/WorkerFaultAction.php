<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

enum WorkerFaultAction: string
{
    case Continue = 'continue';
    case ExitBeforeCommit = 'exit_before_commit';
    case LostReply = 'lose_reply';
    case MalformedReply = 'malformed_reply';
}
