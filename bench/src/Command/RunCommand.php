<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Runner;
use Ineersa\SqliteQueue\Bench\Scenario;
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
            throw new \InvalidArgumentException('Unsupported workload. Use roundtrip, idle, delayed, fixed-rate, application or retention.');
        }
        $duration = $input->getOption('duration');
        if (!\is_string($duration) || !is_numeric($duration)) {
            throw new \InvalidArgumentException('Duration must be numeric seconds.');
        }
        $repetitions = filter_var($input->getOption('repetitions'), \FILTER_VALIDATE_INT);
        if (false === $repetitions) {
            throw new \InvalidArgumentException('Repetitions must be an integer.');
        }
        $delay = filter_var($input->getOption('delay'), \FILTER_VALIDATE_INT);
        if (false === $delay) {
            throw new \InvalidArgumentException('Delay must be an integer number of milliseconds.');
        }

        $rate = $input->getOption('rate');
        if (!\is_string($rate) || !is_numeric($rate)) {
            throw new \InvalidArgumentException('Rate must be numeric arrivals per second.');
        }
        $capacity = filter_var($input->getOption('capacity'), \FILTER_VALIDATE_INT);
        if (false === $capacity) {
            throw new \InvalidArgumentException('Capacity must be an integer.');
        }

        $handler = filter_var($input->getOption('handler-ms'), \FILTER_VALIDATE_INT);
        if (false === $handler) {
            throw new \InvalidArgumentException('Handler work must be integer milliseconds.');
        }

        $cycles = filter_var($input->getOption('cycles'), \FILTER_VALIDATE_INT);
        if (false === $cycles) {
            throw new \InvalidArgumentException('Cycles must be an integer.');
        }
        $messages = filter_var($input->getOption('cycle-messages'), \FILTER_VALIDATE_INT);
        if (false === $messages) {
            throw new \InvalidArgumentException('Cycle messages must be an integer.');
        }
        $settling = $input->getOption('settling');
        if (!\is_string($settling) || !is_numeric($settling)) {
            throw new \InvalidArgumentException('Settling must be numeric seconds.');
        }

        return new RunOptionsDTO($scenario, true === $input->getOption('smoke'), true === $input->getOption('pilot'), (float) $duration, $repetitions, $delay, (float) $rate, $capacity, $handler, $cycles, $messages, (float) $settling);
    }

    protected function configure(): void
    {
        $this->setDescription('Run native Messenger workload measurements.')
            ->addOption('smoke', null, InputOption::VALUE_NONE, 'Tiny fixed cohorts, not performance evidence.')
            ->addOption('pilot', null, InputOption::VALUE_NONE, 'Allow dirty sources and label the capture as a pilot.')
            ->addOption('duration', null, InputOption::VALUE_REQUIRED, 'Fixed measured window in seconds.', (string) RunOptionsDTO::DEFAULT_DURATION_SECONDS)
            ->addOption('repetitions', null, InputOption::VALUE_REQUIRED, 'Predeclared paired repetitions.', (string) RunOptionsDTO::DEFAULT_REPETITIONS)
            ->addOption('delay', null, InputOption::VALUE_REQUIRED, 'Delayed scenario DelayStamp in milliseconds, 1..30000.', (string) RunOptionsDTO::DEFAULT_DELAY_MILLISECONDS)
            ->addOption('rate', null, InputOption::VALUE_REQUIRED, 'Fixed-rate arrivals per second, (0, 100000].', '20')
            ->addOption('capacity', null, InputOption::VALUE_REQUIRED, 'Maximum outstanding messages, 1..10000.', '64')
            ->addOption('handler-ms', null, InputOption::VALUE_REQUIRED, 'Application synchronous external-wait work, 0..10000 milliseconds.', (string) RunOptionsDTO::DEFAULT_HANDLER_MILLISECONDS)
            ->addOption('cycles', null, InputOption::VALUE_REQUIRED, 'Retention cycles, 20..1000.', (string) RunOptionsDTO::DEFAULT_CYCLES)
            ->addOption('cycle-messages', null, InputOption::VALUE_REQUIRED, 'Messages per retention cycle, 1..10000.', (string) RunOptionsDTO::DEFAULT_CYCLE_MESSAGES)
            ->addOption('settling', null, InputOption::VALUE_REQUIRED, 'Retention settling seconds, positive and at most 60.', (string) RunOptionsDTO::DEFAULT_SETTLING_SECONDS)
            ->addOption('workload', null, InputOption::VALUE_REQUIRED, 'roundtrip, idle, delayed, fixed-rate, application or retention.', 'roundtrip');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        return (new Runner($output->writeln(...), self::options($input)))->run();
    }
}
