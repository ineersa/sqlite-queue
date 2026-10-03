<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\DTO;

use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Messenger\Transport;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Messenger\Worker;

/**
 * Initialized wait state for one native messenger:consume invocation.
 *
 * Constructed only after WorkerStartedEvent confirms the selected receiver and idleTimeout 0.
 * deadline null means the consume run has no --time-limit; WAIT then uses only the wait budget.
 */
final class ConsumeWaitSessionDTO
{
    public function __construct(
        public readonly Command $command,
        public readonly Transport $transport,
        public readonly string $receiverName,
        public readonly int|string|float|bool|null $originalSleepDefault,
        public readonly bool $sleepDefaultMutated,
        public readonly DeferredCancellation $stop,
        public readonly int $waitBudgetMilliseconds,
        public readonly ?float $deadline,
        public readonly Worker $worker,
    ) {
    }
}
