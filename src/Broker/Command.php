<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\DeferredCancellation;
use Revolt\EventLoop;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'broker', description: 'Run the foreground SQLite queue broker on a private local Unix socket.')]
final class Command extends BaseCommand
{
    protected function configure(): void
    {
        $this
            ->addOption('database', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private queue database file.')
            ->addOption('endpoint', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private Unix socket file.')
            ->setHelp('Readiness, framing bounds, ownership, failure handling, and client recovery are documented in docs/broker-protocol.md.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $errors = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;
        $watchers = [];
        try {
            $database = $input->getOption('database');
            $endpoint = $input->getOption('endpoint');
            if (!\is_string($database) || '' === $database || !\is_string($endpoint) || '' === $endpoint) {
                throw new \InvalidArgumentException('Both --database and --endpoint are required.');
            }
            if (!\function_exists('pcntl_signal') || !\function_exists('posix_geteuid')) {
                throw new \RuntimeException('The broker requires the pcntl and posix extensions.');
            }
            // The token covers startup too: a signal during listen() cancels the blocking
            // connect, and run() stops the serving loop on the same token. The closure takes
            // no arguments because signal watchers are invoked with (id, signal), which must
            // not bind to DeferredCancellation::cancel(?Throwable).
            $shutdown = new DeferredCancellation();
            foreach ([\SIGINT, \SIGTERM] as $signal) {
                $watchers[] = EventLoop::onSignal($signal, static function () use ($shutdown): void {
                    $shutdown->cancel();
                });
            }
            $broker = (new BrokerFactory($database, $endpoint, cancellation: $shutdown->getCancellation()))->listen();
            $code = $broker->run(function (array $event) use ($output): void {
                $this->write($event, $output);
            }, $shutdown->getCancellation());
            $this->write(['event' => 'stopped', 'exit_code' => $code], $output);

            return $code;
        } catch (\Throwable $error) {
            $this->write(['event' => 'failed', 'error_type' => $error::class], $errors);

            return self::FAILURE;
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
        }
    }

    /** @param array<string, int|string> $event */
    private function write(array $event, OutputInterface $output): void
    {
        $output->writeln(json_encode($event, \JSON_THROW_ON_ERROR), OutputInterface::OUTPUT_RAW);
    }
}
