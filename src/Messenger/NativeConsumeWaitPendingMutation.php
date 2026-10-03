<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Symfony\Component\Console\Command\Command;

/**
 * Sleep-default mutation captured on ConsoleEvents::COMMAND before WorkerStartedEvent.
 *
 * Constructed only after the exact sqlite-queue receiver activation checks succeed and
 * before any InputOption mutation. originalSleepDefault keeps the option's native type.
 */
final class NativeConsumeWaitPendingMutation
{
    public function __construct(
        public readonly Command $command,
        public readonly Transport $transport,
        public readonly string $receiverName,
        public readonly int|string|float|bool|null $originalSleepDefault,
        public readonly bool $sleepDefaultMutated,
        public readonly int $waitBudgetMilliseconds,
        public readonly ?int $timeLimitSeconds,
    ) {
    }
}
