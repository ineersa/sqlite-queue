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

    protected function configure(): void
    {
        $this
            ->addOption('database', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private queue database file.')
            ->addOption('endpoint', null, InputOption::VALUE_REQUIRED, 'Absolute path to the private Unix socket file.')
            ->addOption('trace-file', null, InputOption::VALUE_REQUIRED, 'Optional new regular file that receives non-payload shutdown lifecycle lines after cleanup; exclusive create only.')
            ->setHelp('Readiness, framing bounds, ownership, failure handling, and client recovery are documented in docs/broker-protocol.md.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $errors = $output instanceof ConsoleOutputInterface ? $output->getErrorOutput() : $output;
        $watchers = [];
        $trace = null;
        /** @var list<array{event: string, pid: int, monotonic_ns: int}> */
        $signalMilestones = [];
        /** @var list<array{event: string, pid: int, monotonic_ns: int}> */
        $brokerMilestones = [];
        try {
            $database = $input->getOption('database');
            $endpoint = $input->getOption('endpoint');
            if (!\is_string($database) || '' === $database || !\is_string($endpoint) || '' === $endpoint) {
                throw new \InvalidArgumentException('Both --database and --endpoint are required.');
            }
            $traceFile = $input->getOption('trace-file');
            // The destination is validated and created before any broker resource is acquired, so an
            // unusable one fails the command clearly instead of silently dropping shutdown evidence.
            $trace = \is_string($traceFile) && '' !== $traceFile ? self::openTraceFile($traceFile) : null;
            if (!\function_exists('pcntl_signal') || !\function_exists('posix_geteuid')) {
                throw new \RuntimeException('The broker requires the pcntl and posix extensions.');
            }
            // The token covers startup too: a signal during listen() cancels the blocking
            // connect, and run() stops the serving loop on the same token. The closure takes
            // no arguments because signal watchers are invoked with (id, signal), which must
            // not bind to DeferredCancellation::cancel(?Throwable).
            $shutdown = new DeferredCancellation();
            foreach ([\SIGINT, \SIGTERM] as $signal) {
                $watchers[] = EventLoop::onSignal($signal, static function () use ($shutdown, &$signalMilestones): void {
                    // Bound the trace to the first signal transition. Later signals keep cancel()
                    // idempotent without appending unbounded duplicate milestones during cleanup.
                    if ($shutdown->getCancellation()->isRequested()) {
                        return;
                    }
                    // Memory only during the critical path. File I/O happens after broker cleanup.
                    $signalMilestones[] = ['event' => 'signal-dispatched', 'pid' => (int) getmypid(), 'monotonic_ns' => (int) hrtime(true)];
                    $shutdown->cancel();
                    $signalMilestones[] = ['event' => 'cancellation-requested', 'pid' => (int) getmypid(), 'monotonic_ns' => (int) hrtime(true)];
                });
            }
            $watchers[] = EventLoop::repeat(self::SIGNAL_DISPATCH_INTERVAL_SECONDS, static function (): void {
            });

            $broker = (new BrokerFactory($database, $endpoint, cancellation: $shutdown->getCancellation()))->listen();
            $code = $broker->run(function (array $event) use ($output): void {
                $this->write($event, $output);
            }, $shutdown->getCancellation(), static function (array $event) use (&$brokerMilestones): void {
                $brokerMilestones[] = $event;
            });
            $this->write(['event' => 'stopped', 'exit_code' => $code], $output);

            return $code;
        } catch (\Throwable $error) {
            $this->write(['event' => 'failed', 'error_type' => $error::class], $errors);

            return self::FAILURE;
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
            // Flush after watcher cleanup even when run() throws the shared-budget cancellation.
            // An empty file during a hang still no longer identifies the missing stage. Sorting by
            // captured timestamps preserves chronology when storage failure starts shutdown before
            // a later signal. Trace I/O can still delay process exit on a slow filesystem.
            if (\is_resource($trace)) {
                self::writeTrace($trace, self::orderedTraceEvents($signalMilestones, $brokerMilestones));
            }
            if (\is_resource($trace)) {
                @fclose($trace);
            }
        }
    }

    /** @param array<string, int|string> $event */
    private function write(array $event, OutputInterface $output): void
    {
        $output->writeln(json_encode($event, \JSON_THROW_ON_ERROR), OutputInterface::OUTPUT_RAW);
    }

    /**
     * Writes buffered shutdown milestones after broker cleanup. Failures are swallowed because the
     * trace is an observation and must never undo completed shutdown work.
     *
     * @param resource                                                $trace
     * @param list<array{event: string, pid: int, monotonic_ns: int}> $events
     */
    private static function writeTrace($trace, array $events): void
    {
        foreach ($events as $event) {
            try {
                $written = @fwrite($trace, json_encode($event, \JSON_THROW_ON_ERROR)."\n");
                if (false !== $written) {
                    @fflush($trace);
                }
            } catch (\Throwable) {
            }
        }
    }

    /**
     * Merges signal and broker milestones by the timestamps captured when each event occurred.
     *
     * @param list<array{event: string, pid: int, monotonic_ns: int}> $signalEvents
     * @param list<array{event: string, pid: int, monotonic_ns: int}> $brokerEvents
     *
     * @return list<array{event: string, pid: int, monotonic_ns: int}>
     */
    private static function orderedTraceEvents(array $signalEvents, array $brokerEvents): array
    {
        $events = [...$signalEvents, ...$brokerEvents];
        usort($events, static fn (array $left, array $right): int => $left['monotonic_ns'] <=> $right['monotonic_ns']);

        return $events;
    }

    /**
     * Validates and opens the optional trace destination as a new exclusive regular file.
     *
     * Only a new regular file is accepted. An existing path is refused without modifying it.
     * Opening a FIFO would block until a reader appears, and a device would consume the evidence
     * instead of recording it. The file is created before the broker acquires anything, so its
     * existence proves the destination was usable.
     *
     * @return resource
     */
    private static function openTraceFile(string $path)
    {
        if (file_exists($path)) {
            throw new \InvalidArgumentException('The trace file must not already exist: '.$path);
        }
        $trace = @fopen($path, 'xb');
        if (false === $trace) {
            throw new \RuntimeException('The trace file could not be created exclusively: '.$path);
        }

        return $trace;
    }
}
