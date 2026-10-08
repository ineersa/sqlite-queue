<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Outcome: string
{
    case Enter = 'enter';
    case Success = 'success';
    case Empty = 'empty';
    case Error = 'error';
}
