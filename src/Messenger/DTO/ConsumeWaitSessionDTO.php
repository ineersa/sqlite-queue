<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\DTO;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Messenger\Worker;

/**
 * Initialized idle wait for one native messenger:consume invocation.
 *
 * Constructed after WorkerStartedEvent confirms the selection activated a wait and idleTimeout is
 * 0. wait is a fully initialized Closure for either shared broker WAIT or the bounded Clock
 * fallback used when our transports mix with foreign receivers. deadline null means the consume
 * run has no --time-limit; WAIT then uses only the wait budget.
 */
final class ConsumeWaitSessionDTO
{
    /**
     * @param \Closure(int, Cancellation): bool $wait
     */
    public function __construct(
        public readonly Command $command,
        public readonly \Closure $wait,
        public readonly DeferredCancellation $stop,
        public readonly int $waitBudgetMilliseconds,
        public readonly ?float $deadline,
        public readonly Worker $worker,
    ) {
    }
}
