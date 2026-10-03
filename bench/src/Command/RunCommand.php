<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\Benchmark;
use Ineersa\SqliteQueue\Bench\Cancellation;
use Ineersa\SqliteQueue\Bench\Config;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Command\SignalableCommandInterface;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'run', description: 'Capture paired Doctrine SQLite and broker Messenger runs.')]
final class RunCommand extends Command implements SignalableCommandInterface
{
    private readonly Cancellation $cancellation;

    public function __construct()
    {
        $this->cancellation = new Cancellation();
        parent::__construct();
    }

    /**
     * @return list<int>
     */
    public function getSubscribedSignals(): array
    {
        return [\SIGINT, \SIGTERM];
    }

    public function handleSignal(int $signal, int|false $previousExitCode = 0): int|false
    {
        $this->cancellation->request();

        return false;
    }

    protected function configure(): void
    {
        $this
            ->addOption('smoke', null, InputOption::VALUE_NONE, 'Run a small execution check, not a performance measurement.')
            ->addOption('workload', null, InputOption::VALUE_REQUIRED, 'Workload name, or all.', 'all')
            ->setHelp('Available workloads: '.implode(', ', array_keys(Config::workloads())));
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $benchmark = new Benchmark($this->cancellation);
        $report = $benchmark->run(
            (string) $input->getOption('workload'),
            (bool) $input->getOption('smoke'),
            $io->text(...),
        );

        $io->note('Paired comparison: '.$report['comparison']['status'].'.');
        if ('complete' !== $report['baseline_status']) {
            $io->warning('The capture contains failed or unexecuted repetitions. Their evidence has been retained.');

            return Command::FAILURE;
        }

        $io->success('Every scheduled backend repetition completed.');

        return Command::SUCCESS;
    }
}
