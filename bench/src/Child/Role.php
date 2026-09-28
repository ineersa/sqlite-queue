<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

enum Role: string
{
    case Consumer = 'consumer';
    case Publisher = 'publisher';
    case Prefill = 'prefill';
}
