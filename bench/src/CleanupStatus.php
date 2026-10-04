<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum CleanupStatus: string
{
    case Exited = 'exited';
    case PidReused = 'pid-reused-original-exited';
    case Zombie = 'exited-zombie';
    case IdentityUnavailable = 'identity-unavailable-no-signal';
    case KillSent = 'kill-sent-survivor';
    case KillFailed = 'kill-failed-survivor';
}
