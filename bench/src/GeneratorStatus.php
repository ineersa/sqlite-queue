<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Offered-load completion is independent of transport and integrity status. */
enum GeneratorStatus: string
{
    case Insufficient = 'generator-insufficient';
    case Unknown = 'unknown-after-failure';
    case Met = 'offered-load-met';
}
