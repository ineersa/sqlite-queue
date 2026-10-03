<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Phase: string
{
    case Boot = 'boot';
    case Connected = 'connected';
    case Warmup = 'warmup';
    case Reset = 'reset';
    case Measure = 'measure';
    case Idle = 'idle';
    case IdleEnd = 'idle-end';
    case Pickup = 'pickup';
    case Drain = 'drain';
    case Settle = 'settle';
    case Audit = 'audit';
    case Shutdown = 'shutdown';
}
