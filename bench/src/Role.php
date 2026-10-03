<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Role: string
{
    case ObserverPublisher = 'observer-publisher-shared';
    case Consumer = 'consumer';
    case ResultsConsumer = 'consumer-results';
    case Broker = 'broker';
    case Persistence = 'persistence';
}
