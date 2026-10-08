<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Role: string
{
    case ObserverPublisher = 'observer-publisher-shared';
    case Consumer = 'consumer';
    case Consumer0 = 'consumer-0';
    case Consumer1 = 'consumer-1';
    case Publisher0 = 'publisher-0';
    case Publisher1 = 'publisher-1';
    case Publisher2 = 'publisher-2';
    case ResultsConsumer = 'consumer-results';
    case Broker = 'broker';
    case Persistence = 'persistence';
}
