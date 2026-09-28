<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\Child\Assignment;
use Ineersa\SqliteQueue\Bench\Child\Consumer;
use Ineersa\SqliteQueue\Bench\Child\Publisher;
use Ineersa\SqliteQueue\Bench\Child\Role;
use Ineersa\SqliteQueue\Bench\Child\Session;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'worker', description: 'Internal publisher or Messenger consumer for one repetition.', hidden: true)]
final class WorkerCommand extends Command
{
    protected function configure(): void
    {
        $this
            ->addArgument('config', InputArgument::REQUIRED, 'Coordinator-generated assignment JSON.')
            ->addArgument('role', InputArgument::REQUIRED, 'consumer, publisher, or prefill.')
            ->addArgument('index', InputArgument::REQUIRED, 'Worker index within this role.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $role = Role::tryFrom((string) $input->getArgument('role'));
        $index = filter_var($input->getArgument('index'), \FILTER_VALIDATE_INT, ['options' => ['min_range' => 0]]);
        if (null === $role || false === $index) {
            throw new \InvalidArgumentException('Expected a known worker role and a nonnegative integer index.');
        }

        date_default_timezone_set('UTC');
        $assignment = Assignment::fromFile((string) $input->getArgument('config'), $role, $index);
        $session = new Session($assignment);
        $worker = Role::Consumer === $role ? new Consumer($session) : new Publisher($session);
        $session->execute($worker->run(...));

        return Command::SUCCESS;
    }
}
