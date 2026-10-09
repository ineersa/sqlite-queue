<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\DTO;

use Symfony\Component\Console\Command\Command;

/**
 * Sleep-default mutation captured on ConsoleEvents::COMMAND before WorkerStartedEvent.
 *
 * Captures only sleep/default and time-limit configuration. Receiver selection and WAIT
 * versus bounded fallback are decided later from WorkerStartedEvent metadata.
 */
final class ConsumeWaitPendingDTO
{
    public function __construct(
        public readonly Command $command,
        public readonly int|string|float|bool|null $originalSleepDefault,
        public readonly bool $sleepDefaultMutated,
        public readonly int $waitBudgetMilliseconds,
        public readonly ?int $timeLimitSeconds,
    ) {
    }
}
