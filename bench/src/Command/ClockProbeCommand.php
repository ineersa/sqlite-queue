<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\StreamableInputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'clock-probe', description: 'Internal cross-process monotonic clock responder.', hidden: true)]
final class ClockProbeCommand extends Command
{
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stream = $input instanceof StreamableInputInterface ? $input->getStream() : null;
        $stream ??= STDIN;

        for ($round = 0; $round < 20 && fgets($stream) !== false; ++$round) {
            $output->writeln((string) hrtime(true));
        }

        return Command::SUCCESS;
    }
}
