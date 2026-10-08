<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Command;

use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Broker\BrokerEventEnum;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
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
    public const int DEFAULT_REDELIVER_TIMEOUT_SECONDS = Queue::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS / 1000;

    /**
     * Upper bound, in seconds, on how long the serving loop may wait for events without waking.
     *
     * Revolt's stream-select driver drains its queued signals, computes the select timeout, and
     * then blocks in select(). A shutdown signal delivered in the gap between that drain and that
     * select is queued, but nothing can wake a select that has no deadline: the signal waits until
     * some descriptor becomes readable. This repeating no-op timer keeps a deadline pending, so
     * the driver recomputes its timeout and dispatches the queued signal within one interval when
     * the loop is able to run. It does not preempt a blocked local PDO call.
     */
    private const float SIGNAL_DISPATCH_INTERVAL_SECONDS = 1.0;

    /** Standalone defaults match queue policy and use the wall clock; injection supports controlled runtime clocks. */
    public function __construct(
        private readonly int $redeliverTimeoutSeconds = self::DEFAULT_REDELIVER_TIMEOUT_SECONDS,
        private readonly ClockInterface $clock = new Clock(),
        private readonly SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal,
    ) {
        if ($redeliverTimeoutSeconds <= 0) {
            throw new \InvalidArgumentException('Redelivery timeout must be positive seconds.');
        }
        if ($redeliverTimeoutSeconds > intdiv(\PHP_INT_MAX, 1000)) {
            throw new \InvalidArgumentException('Redelivery timeout exceeds the supported milliseconds range.');
        }
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('database', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private queue database file.')
            ->addOption('endpoint', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private Unix socket file.')
            ->addOption('redeliver-timeout', null, InputOption::VALUE_REQUIRED, 'Reservation redelivery timeout in seconds.', (string) $this->redeliverTimeoutSeconds)
            ->addOption('synchronous', null, InputOption::VALUE_REQUIRED, 'SQLite WAL synchronous mode: normal or full.', $this->synchronous->value)
            ->setHelp('Readiness, framing bounds, lifetime locks, failure handling, and client recovery are documented in docs/broker-protocol.md.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $errors = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;
        $watchers = [];
        try {
            $synchronousValue = $input->getOption('synchronous');
            if (!\is_string($synchronousValue)) {
                throw new \InvalidArgumentException('--synchronous must be normal or full.');
            }
            $synchronous = SqliteSynchronousMode::tryFrom($synchronousValue);
            if (null === $synchronous) {
                throw new \InvalidArgumentException('--synchronous must be normal or full.');
            }
            $database = $input->getOption('database');
            $endpoint = $input->getOption('endpoint');
            $redeliverTimeoutSeconds = filter_var($input->getOption('redeliver-timeout'), \FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
            if (false === $redeliverTimeoutSeconds) {
                throw new \InvalidArgumentException('--redeliver-timeout must be positive integer seconds.');
            }
            if ($redeliverTimeoutSeconds > intdiv(\PHP_INT_MAX, 1000)) {
                throw new \InvalidArgumentException('--redeliver-timeout exceeds the supported milliseconds range.');
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
                visibilityTimeout: $redeliverTimeoutSeconds * 1000,
                clock: fn (): int => $this->nowMilliseconds(),
                cancellation: $shutdown->getCancellation(),
                synchronous: $synchronous,
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

        return $now->getTimestamp() * 1000 + (int) $now->format('v');
    }

    /** @param array<string, int|string> $event */
    private function write(array $event, OutputInterface $output): void
    {
        $output->writeln(json_encode($event, \JSON_THROW_ON_ERROR), OutputInterface::OUTPUT_RAW);
    }
}
