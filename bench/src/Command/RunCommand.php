<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Runner;
use Ineersa\SqliteQueue\Bench\Scenario;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class RunCommand extends Command
{
    public function __construct()
    {
        parent::__construct('run');
    }

    public static function options(InputInterface $input): RunOptionsDTO
    {
        $workload = $input->getOption('workload');
        $scenario = \is_string($workload) ? Scenario::tryFrom($workload) : null;
        if (null === $scenario) {
            throw new \InvalidArgumentException('Use roundtrip, concurrent, application, idle or retention.');
        }
        $duration = $input->getOption('duration');
        if (!\is_string($duration) || !is_numeric($duration)) {
            throw new \InvalidArgumentException('Duration must be numeric seconds.');
        }
        $mode = $input->getOption('synchronous');
        $synchronous = \is_string($mode) ? SqliteSynchronousMode::tryFrom($mode) : null;
        if (null === $synchronous) {
            throw new \InvalidArgumentException('Synchronous mode must be normal or full.');
        }
        $polling = filter_var($input->getOption('polling-ms'), \FILTER_VALIDATE_INT);
        if (false === $polling) {
            throw new \InvalidArgumentException('Doctrine polling interval must be integer milliseconds.');
        }

        return new RunOptionsDTO($scenario, true === $input->getOption('smoke'), (float) $duration, $synchronous, $polling);
    }

    protected function configure(): void
    {
        $this->setDescription('Compare native Messenger workloads, one run per backend.')
            ->addOption('workload', null, InputOption::VALUE_REQUIRED, 'roundtrip, concurrent, application, idle, multi-queue or retention.', 'roundtrip')
            ->addOption('smoke', null, InputOption::VALUE_NONE, 'Tiny control-path checks, not performance evidence.')
            ->addOption('duration', null, InputOption::VALUE_REQUIRED, 'Measured window in seconds.', (string) RunOptionsDTO::DEFAULT_DURATION_SECONDS)
            ->addOption('polling-ms', null, InputOption::VALUE_REQUIRED, 'Doctrine idle poll interval, 1..1000 milliseconds. Broker uses WAIT.', (string) RunOptionsDTO::DEFAULT_DOCTRINE_POLLING_MILLISECONDS)
            ->addOption('synchronous', null, InputOption::VALUE_REQUIRED, 'WAL synchronous mode for both backends: normal or full.', 'normal');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        return (new Runner(static fn (string $line) => $output->writeln($line), self::options($input)))->run();
    }
}
