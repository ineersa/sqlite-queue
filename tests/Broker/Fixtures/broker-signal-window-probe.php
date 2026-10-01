<?php

declare(strict_types=1);

/*
 * Child probe for BrokerProcessTest::testBrokerStopsWhenTheSignalArrivesInsideTheSelectWindow.
 *
 * It runs the real broker command and delivers SIGTERM inside the one window where the
 * event loop can lose the signal. Revolt's stream-select driver drains its queued signals,
 * computes the select timeout, and then blocks in select(). A signal that arrives after the
 * drain and before that select blocks is queued, but a select with no deadline cannot be woken
 * by it: the signal stays queued until some descriptor becomes readable. The wrapper stream
 * below is read by every select, so its cast is the only probe code that runs inside that
 * window. It reads the enclosing select deadline from the call stack and injects the signal
 * only while that select would wait without a deadline, which is exactly the window the
 * broker's periodic wakeup removes.
 *
 * Arguments: <log> <database> <endpoint>
 */

use Ineersa\SqliteQueue\Command\BrokerCommand;
use Revolt\EventLoop;
use Revolt\EventLoop\Driver\StreamSelectDriver;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\Output;

require dirname(__DIR__, 3).'/vendor/autoload.php';

// Exercise this driver's race even when another event-loop extension is installed.
EventLoop::setDriver(new StreamSelectDriver());

/**
 * Wrapped socket that reports and injects inside the select window.
 */
final class SignalWindowStream
{
    /** Select deadline at or above this value proves the loop waits with a deadline, in seconds. */
    private const float DEADLINE_MARK_SECONDS = 0.5;

    /** Driver frame that computes the timeout passed to the kernel select. */
    private const string DRIVER_CLASS = StreamSelectDriver::class;
    private const string SELECT_METHOD = 'selectStreams';

    private static string $log = '';
    private static bool $armed = false;
    private static bool $injected = false;
    private static bool $reportedDeadline = false;
    private static bool $reportedMissingDeadline = false;
    /** @var resource|null */
    private static $target;
    /** @var resource|null Peer end, held open so the watched descriptor stays silent. */
    private static $peer;
    /** @var resource|null Watched stream; its presence in the read set is what runs the probe. */
    private static $watched;

    /**
     * Registers the wrapper and joins its watcher to the loop.
     *
     * The loop reads the wrapped descriptor in every select, so this stream is what lets the
     * probe see the deadline of the select that is about to run.
     */
    public static function install(string $log): void
    {
        self::$log = $log;
        @unlink($log);
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        if (false === $pair) {
            throw new RuntimeException('The probe needs a socket pair.');
        }
        // The probe end stays silent: a readable stream would spin the loop instead of letting
        // it settle into the wait this test is about.
        stream_set_blocking($pair[0], false);
        self::$target = $pair[0];
        // The peer must stay open: a collected peer closes the pair, and the resulting EOF
        // would make the wrapped descriptor permanently readable and spin the loop.
        self::$peer = $pair[1];
        stream_wrapper_register('signalwindow', self::class);
        $watched = fopen('signalwindow://probe', 'r+b');
        if (false === $watched) {
            throw new RuntimeException('The probe could not open its wrapper stream.');
        }
        // Never readable: the watcher exists so the loop selects on the wrapped descriptor.
        EventLoop::onReadable($watched, static function (): void {
        });
        self::$watched = $watched;
    }

    public static function arm(): void
    {
        self::$armed = true;
        self::record(['probe' => 'armed']);
    }

    /** @param array<string, int|float|string> $entry */
    public static function record(array $entry): void
    {
        file_put_contents(self::$log, json_encode($entry, \JSON_THROW_ON_ERROR)."\n", \FILE_APPEND);
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool
    {
        return true;
    }

    /**
     * @return resource|null
     */
    public function stream_cast(int $castAs): mixed
    {
        if (self::$armed) {
            $deadline = self::enclosingSelectDeadline();
            if (null === $deadline) {
                self::reportMissingDeadline();
            } elseif ($deadline < 0.0) {
                self::inject();
            } elseif ($deadline >= self::DEADLINE_MARK_SECONDS) {
                self::reportDeadline($deadline);
            }
        }

        return self::$target;
    }

    /** The timeout the enclosing select passes to the kernel; negative means no deadline. */
    private static function enclosingSelectDeadline(): ?float
    {
        foreach (debug_backtrace(\DEBUG_BACKTRACE_PROVIDE_OBJECT) as $frame) {
            if (self::SELECT_METHOD !== ($frame['function'] ?? null) || self::DRIVER_CLASS !== ($frame['class'] ?? null)) {
                continue;
            }
            $deadline = $frame['args'][2] ?? null;

            return is_int($deadline) || is_float($deadline) ? (float) $deadline : null;
        }

        return null;
    }

    /**
     * Delivers SIGTERM while the enclosing select is about to wait without a deadline.
     *
     * Asynchronous signal dispatch is enabled by the console application, so the handler queues
     * the signal here, before the kernel blocks, and nothing interrupts that block.
     */
    private static function inject(): void
    {
        if (self::$injected) {
            return;
        }
        self::$injected = true;
        self::record(['probe' => 'lost-window']);
        posix_kill(posix_getpid(), \SIGTERM);
    }

    private static function reportDeadline(float $deadline): void
    {
        if (self::$reportedDeadline) {
            return;
        }
        self::$reportedDeadline = true;
        self::record(['probe' => 'deadline', 'seconds' => round($deadline, 3)]);
    }

    private static function reportMissingDeadline(): void
    {
        if (self::$reportedMissingDeadline) {
            return;
        }
        self::$reportedMissingDeadline = true;
        self::record(['probe' => 'deadline-unreadable']);
    }
}

/** Forwards broker events to the test and arms the probe once the broker reports readiness. */
final class SignalWindowOutput extends Output
{
    protected function doWrite(string $message, bool $newline): void
    {
        if (str_contains($message, '"event":"ready"')) {
            SignalWindowStream::arm();
        }

        fwrite(\STDOUT, $message.($newline ? "\n" : ''));
    }
}

$log = (string) ($argv[1] ?? '');
$database = (string) ($argv[2] ?? '');
$endpoint = (string) ($argv[3] ?? '');
SignalWindowStream::install($log);

$application = new Application('SQLite queue broker');
$application->setCatchExceptions(false);
$application->setAutoExit(false);
$application->addCommand(new BrokerCommand());
$input = new ArrayInput(['command' => 'broker', '--database' => $database, '--endpoint' => $endpoint]);

try {
    $exit = $application->run($input, new SignalWindowOutput());
} catch (Throwable $error) {
    SignalWindowStream::record(['probe' => 'threw', 'class' => $error::class]);

    exit(1);
}

SignalWindowStream::record(['probe' => 'exit', 'code' => $exit]);

exit($exit);
