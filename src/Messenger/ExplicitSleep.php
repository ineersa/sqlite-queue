<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

/** Classification of an explicit messenger:consume --sleep value for WAIT activation. */
enum ExplicitSleep
{
    case Omitted;
    case Zero;
    case Positive;
    case Invalid;
}
