<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Command;

use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Broker\BrokerEventEnum;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Revolt\EventLoop;
use Symfony\Component\Clock\Clock;
use Symfony\Component\Clock\ClockInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'broker', description: 'Run the foreground SQLite queue broker on a private local Unix socket.')]
final class BrokerCommand extends BaseCommand
{
    public const int DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS = 5000;
    private const int MILLISECONDS_PER_SECOND = 1000;

    /**
     * Upper bound, in seconds, on how long the serving loop may wait for events without waking.
     *
     * Revolt's stream-select driver drains its queued signals, computes the select timeout, and
     * then blocks in select(). A shutdown signal delivered in the gap between that drain and that
     * select is queued, but nothing can wake a select that has no deadline: the signal waits until
     * some descriptor becomes readable. This repeating no-op timer keeps a deadline pending, so
     * the driver recomputes its timeout and dispatches the queued signal within one interval. It
     * wakes the loop once per second; it never touches storage.
     */
    private const float SIGNAL_DISPATCH_INTERVAL_SECONDS = 1.0;

    /** Standalone defaults match queue policy and use the wall clock; injection supports controlled runtime clocks. */
    public function __construct(
        private readonly int $visibilityTimeoutMilliseconds = self::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS,
        private readonly ClockInterface $clock = new Clock(),
    ) {
        if ($visibilityTimeoutMilliseconds <= 0) {
            throw new \InvalidArgumentException('Visibility timeout must be positive milliseconds.');
        }
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('database', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private queue database file.')
            ->addOption('endpoint', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private Unix socket file.')
            ->addOption('visibility-timeout', null, InputOption::VALUE_REQUIRED, 'Reservation visibility timeout in milliseconds.', (string) $this->visibilityTimeoutMilliseconds)
            ->setHelp('Readiness, framing bounds, lifetime locks, failure handling, and client recovery are documented in docs/broker-protocol.md.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $errors = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;
        $watchers = [];
        try {
            $database = $input->getOption('database');
            $endpoint = $input->getOption('endpoint');
            $visibilityTimeout = filter_var($input->getOption('visibility-timeout'), \FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
            if (false === $visibilityTimeout) {
                throw new \InvalidArgumentException('--visibility-timeout must be positive integer milliseconds.');
            }
            if (!\is_string($database) || '' === $database || !\is_string($endpoint) || '' === $endpoint) {
                throw new \InvalidArgumentException('Both --database and --endpoint are required.');
            }
            if (!\function_exists('pcntl_signal') || !\function_exists('posix_geteuid')) {
                throw new \RuntimeException('The broker requires the pcntl and posix extensions.');
            }
            // The token covers startup too: a signal during create() cancels the blocking
            // connect, and run() stops the serving loop on the same token. The closure takes
            // no arguments because signal watchers are invoked with (id, signal), which must
            // not bind to DeferredCancellation::cancel(?Throwable).
            $shutdown = new DeferredCancellation();
            foreach ([\SIGINT, \SIGTERM] as $signal) {
                $watchers[] = EventLoop::onSignal($signal, static function () use ($shutdown): void {
                    $shutdown->cancel();
                });
            }
            $watchers[] = EventLoop::repeat(self::SIGNAL_DISPATCH_INTERVAL_SECONDS, static function (): void {
            });

            $broker = (new BrokerFactory(
                $database,
                $endpoint,
                visibilityTimeout: $visibilityTimeout,
                clock: fn (): int => $this->nowMilliseconds(),
                cancellation: $shutdown->getCancellation(),
            ))->create();
            $code = $broker->run(function (array $event) use ($output): void {
                $this->write($event, $output);
            }, $shutdown->getCancellation());
            $this->write(['event' => BrokerEventEnum::Stopped->value, 'exit_code' => $code], $output);

            return $code;
        } catch (\Throwable $error) {
            $this->write(['event' => BrokerEventEnum::Failed->value, 'error_type' => $error::class], $errors);

            return self::FAILURE;
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
        }
    }

    private function nowMilliseconds(): int
    {
        $now = $this->clock->now();

        return $now->getTimestamp() * self::MILLISECONDS_PER_SECOND + (int) $now->format('v');
    }

    /** @param array<string, int|string> $event */
    private function write(array $event, OutputInterface $output): void
    {
        $output->writeln(json_encode($event, \JSON_THROW_ON_ERROR), OutputInterface::OUTPUT_RAW);
    }
}
