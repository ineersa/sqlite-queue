<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\DTO;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;

/**
 * Captured messenger:consume invocation waiting for stock receiver selection.
 *
 * ConsoleEvents::COMMAND stores the live input and sleep mode without changing the command's
 * sleep default. Activation happens when ConsumeReceiverLocator reports the first selected
 * sqlite-queue Transport, before Worker options read --sleep.
 *
 * originalSleepOption is mixed because InputInterface::getOption() returns mixed for the native
 * sleep option; it is captured at COMMAND after the input is bound.
 */
final class ConsumeWaitPendingDTO
{
    public bool $activated = false;

    public bool $sleepOptionMutated = false;

    public function __construct(
        public readonly Command $command,
        public readonly InputInterface $input,
        public readonly mixed $originalSleepOption,
        public readonly bool $sleepOmitted,
        public readonly int $waitBudgetMilliseconds,
        public readonly ?int $timeLimitSeconds,
    ) {
    }
}
