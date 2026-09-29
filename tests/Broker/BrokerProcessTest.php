<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\ByteStream\BufferedReader;
use Amp\CancelledException;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;

#[RequiresOperatingSystem('Linux')]
final class BrokerProcessTest extends TestCase
{
    /** Bounded wait for a kernel-reported process state; stops and signal delivery are not ordered. */
    private const int STATE_POLL_TIMEOUT_SECONDS = 5;
    /** Bounded wait for the signal-window probe to observe how the broker waits for events. */
    private const int PROBE_MARKER_TIMEOUT_SECONDS = 5;
    /** Bound on a broker shutdown; the production budget is five seconds. */
    private const int SIGNAL_BOUND_SECONDS = 10;
    private ?IsolatedDatabase $fixture = null;
    private string $socket = '';
    /** @var list<Process> */
    private array $processes = [];
    /** @var list<Client> */
    private array $clients = [];
    /** @var list<int> */
    private array $tracked = [];

    protected function setUp(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        $this->fixture = new IsolatedDatabase();
        $this->socket = $this->fixture->path('queue.sock');
    }

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            $client->close();
        }
        foreach (array_reverse($this->tracked) as $pid) {
            if (isset(ProcessTree::snapshot()[$pid]) && \function_exists('posix_kill')) {
                @posix_kill($pid, \SIGKILL);
            }
        }
        foreach ($this->processes as $process) {
            if ($process->isRunning()) {
                $process->kill();
            }
        }
        if (is_file($this->socket) || is_link($this->socket)) {
            @unlink($this->socket);
        }
        $this->fixture?->remove();
    }

    public function testGracefulStopReleasesOwnedTreeAndDatabaseSurvives(): void
    {
        $database = $this->fixture->path();
        $first = $this->startBroker($database, $this->socket);
        $pid = $first->getPid();
        $owned = $this->trackOwned($pid);
        $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');

        $client = $this->client();
        $body = random_bytes(64);
        $headers = random_bytes(8);
        $sent = $client->send('jobs', $body, $headers);
        $this->assertGreaterThan(0, $sent);
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame($sent, $delivery->id);
        $this->assertSame($body, $delivery->body);
        $this->assertSame($headers, $delivery->headers);
        $client->acknowledge($delivery->receipt);

        $rejected = $client->send('jobs', 'discard me');
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame($rejected, $delivery->id);
        $client->reject($delivery->receipt);
        $this->assertNull($client->receive('jobs'));

        $persisted = $client->send('jobs', 'survives restart');
        $client->close();
        array_pop($this->clients);

        $this->stopBroker($first);
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket, 'Completed shutdown must release the endpoint.');
        $this->assertFileExists($database, 'Shutdown must preserve the database.');

        $second = $this->startBroker($database, $this->socket);
        $this->trackOwned($second->getPid());
        $client = $this->client();
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery, 'A restarted broker must serve persisted messages.');
        $this->assertSame($persisted, $delivery->id);
        $this->assertSame('survives restart', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);
        $this->stopBroker($second);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    public function testExclusiveOwnershipRejectsConflictingBrokers(): void
    {
        $database = $this->fixture->path();
        $broker = $this->startBroker($database, $this->socket);
        $this->trackOwned($broker->getPid());
        $client = $this->client();
        $this->assertGreaterThan(0, $client->send('jobs', 'before conflict'));

        $sameDatabase = $this->conflictingBroker($database, $this->fixture->path('other.sock'));
        $this->assertNotSame(0, $sameDatabase->join(new TimeoutCancellation(10)), 'A second broker must not write the same database.');
        $otherDatabase = $this->conflictingBroker($this->fixture->path('other.sqlite'), $this->socket);
        $this->assertNotSame(0, $otherDatabase->join(new TimeoutCancellation(10)), 'A second broker must not take the same endpoint.');

        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame('before conflict', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $this->assertGreaterThan(0, $client->send('jobs', 'after conflict'));
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame('after conflict', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);

        $this->stopBroker($broker);
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket);
    }

    public function testIdlePersistenceDeathFailsBrokerAndPreservesConfirmedData(): void
    {
        $database = $this->fixture->path();
        $broker = $this->startBroker($database, $this->socket);
        $owned = $this->trackOwned($broker->getPid());
        $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');
        $worker = $owned['workers'][0];

        $client = $this->client();
        $confirmed = $client->send('jobs', 'confirmed message');
        $this->assertGreaterThan(0, $confirmed);
        $client->close();
        array_pop($this->clients);

        $this->assertNotSame(0, posix_geteuid());
        $this->assertSame(posix_geteuid(), fileowner('/proc/'.$worker), 'The killed process must be this user\'s persistence worker.');
        $this->assertTrue(posix_kill($worker, \SIGKILL), 'The test must kill only the persistence worker.');

        $this->assertNotSame(0, $broker->join(new TimeoutCancellation(10)), 'An idle persistence death must fail the broker without another client request.');
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket, 'A failed broker must release the endpoint.');
        $this->assertFileExists($database, 'A failed broker must preserve confirmed data.');

        $restarted = $this->startBroker($database, $this->socket);
        $this->trackOwned($restarted->getPid());
        $client = $this->client();
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery, 'A confirmation must survive a persistence failure.');
        $this->assertSame($confirmed, $delivery->id);
        $this->assertSame('confirmed message', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);
        $this->stopBroker($restarted);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    public function testInvalidStorageFailsStartupAndReleasesOwnership(): void
    {
        $database = $this->fixture->path();
        $invalid = str_repeat('not a sqlite database', 8);
        file_put_contents($database, $invalid);
        chmod($database, 0o600);

        $before = ProcessTree::ownedBy(getmypid());
        $failed = $this->conflictingBroker($database, $this->socket);
        $this->tracked[] = $failed->getPid();

        $stdout = '';
        while (null !== ($chunk = $failed->getStdout()->read(new TimeoutCancellation(10)))) {
            $stdout .= $chunk;
        }
        $this->assertNotSame(0, $failed->join(new TimeoutCancellation(10)), 'Unreadable storage must fail startup.');
        $this->assertStringNotContainsString('ready', $stdout, 'Readiness must not precede storage initialization.');
        $this->assertSame($invalid, file_get_contents($database), 'Failed startup must not modify the rejected database file.');
        $after = ProcessTree::ownedBy(getmypid());
        $this->assertSame([], array_values(array_diff($after['workers'], $before['workers'])), 'Failed startup left an owned persistence worker.');
        $this->assertSame([], array_values(array_diff($after['launchers'], $before['launchers'])), 'Failed startup left an owned worker launcher.');
        $this->assertTrackedGone();

        unlink($database);
        file_put_contents($database, '');
        chmod($database, 0o600);
        $restarted = $this->startBroker($database, $this->socket);
        $this->trackOwned($restarted->getPid());
        $this->assertFileExists($this->socket);
        $this->stopBroker($restarted);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    public function testMissingPosixExtensionFailsBeforeAcquiringResources(): void
    {
        $database = $this->fixture->path();
        $before = glob($this->fixture->directory().'/*');
        $this->assertSame([], false === $before ? [] : $before, 'The fixture directory must start empty.');
        $owned = ProcessTree::ownedBy(getmypid());

        $process = Process::start([
            \PHP_BINARY,
            '-d',
            'disable_functions=posix_geteuid',
            __DIR__.'/Fixtures/broker-posix-probe.php',
            $database,
            $this->socket,
        ], null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
        $this->processes[] = $process;

        $stdout = '';
        while (null !== ($chunk = $process->getStdout()->read(new TimeoutCancellation(10)))) {
            $stdout .= $chunk;
        }
        $report = json_decode($stdout, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($report);
        $this->assertSame('threw', $report['outcome'] ?? null, 'A broker without ext-posix must refuse to start.');
        $this->assertSame(\RuntimeException::class, $report['class'] ?? null, 'A missing capability must raise a catchable exception, not an undefined-function Error.');
        $this->assertSame(0, $process->join(new TimeoutCancellation(10)));

        $after = glob($this->fixture->directory().'/*');
        $this->assertSame([], false === $after ? [] : $after, 'A refused startup must not create a database, endpoint, or lock file.');
        $started = ProcessTree::ownedBy(getmypid());
        $this->assertSame([], array_values(array_diff($started['workers'], $owned['workers'])), 'A refused startup left a persistence worker.');
        $this->assertSame([], array_values(array_diff($started['launchers'], $owned['launchers'])), 'A refused startup left a worker launcher.');
    }

    /**
     * The optional trace destination is validated before the broker acquires anything, so an
     * unusable one fails clearly instead of blocking (a FIFO) or discarding evidence (a device).
     */
    public function testInvalidTraceDestinationFailsBeforeTheBrokerAcquiresAnything(): void
    {
        if (!\function_exists('posix_mkfifo')) {
            $this->markTestSkipped('The posix extension is unavailable.');
        }
        $fifo = $this->evidenceDirectory().'/refused-trace.fifo';
        @unlink($fifo);
        $this->assertTrue(posix_mkfifo($fifo, 0o600), 'The test needs a FIFO to prove it is refused rather than opened.');
        $existing = $this->evidenceDirectory().'/existing-trace.log';
        @unlink($existing);
        $this->assertNotFalse(@file_put_contents($existing, "keep-me\n"), 'The test needs an existing regular file to prove exclusive create refuses it.');
        $before = ProcessTree::ownedBy(getmypid());
        try {
            $destinations = [
                'device' => '/dev/null',
                'fifo' => $fifo,
                'directory' => $this->fixture->directory(),
                'existing-file' => $existing,
            ];
            foreach ($destinations as $kind => $destination) {
                $database = $this->fixture->path();
                $process = $this->spawn($database, $this->socket, $destination);
                $this->processes[] = $process;
                try {
                    $exit = $process->join(new TimeoutCancellation(10));
                } catch (CancelledException) {
                    $process->kill();
                    $this->fail('A rejected trace destination ('.$kind.') must fail instead of blocking: '.$destination);
                }
                $this->assertNotSame(0, $exit, 'A rejected trace destination ('.$kind.') must fail the command.');

                $stderr = '';
                while (null !== ($chunk = $process->getStderr()->read(new TimeoutCancellation(10)))) {
                    $stderr .= $chunk;
                }
                $report = json_decode($stderr, true, 16, \JSON_THROW_ON_ERROR);
                $this->assertIsArray($report);
                $this->assertSame('failed', $report['event'] ?? null, 'A rejected trace destination ('.$kind.') must report a failure.');
                $this->assertSame(\InvalidArgumentException::class, $report['error_type'] ?? null, 'A rejected trace destination ('.$kind.') must fail as invalid input.');
                $this->assertFileDoesNotExist($database, 'A rejected trace destination ('.$kind.') must fail before the database exists.');
                $this->assertFileDoesNotExist($this->socket, 'A rejected trace destination ('.$kind.') must fail before the endpoint binds.');
                $this->assertSame([], glob($this->fixture->directory().'/*') ?: [], 'A rejected trace destination ('.$kind.') must not create broker files.');
            }
            $this->assertSame("keep-me\n", (string) file_get_contents($existing), 'An existing regular file must be refused without modification.');
            $after = ProcessTree::ownedBy(getmypid());
            $this->assertSame([], array_values(array_diff($after['workers'], $before['workers'])), 'A rejected trace destination must not start a persistence worker.');
            $this->assertSame([], array_values(array_diff($after['launchers'], $before['launchers'])), 'A rejected trace destination must not start a worker launcher.');
        } finally {
            @unlink($fifo);
            @unlink($existing);
        }
    }

    public function testBrokerStopsWhenTheSignalArrivesInsideTheSelectWindow(): void
    {
        $database = $this->fixture->path();
        $log = $this->fixture->path('signal-window.log');
        $probe = $this->startSignalWindowProbe($database, $log);
        $pid = $probe->getPid();
        $owned = $this->trackOwned($pid);
        $this->assertCount(1, $owned['workers'], 'The probe broker must own exactly one persistence worker.');
        $worker = $owned['workers'][0];

        $marker = $this->awaitProbeMarker($log, ['lost-window', 'deadline']);
        if ('lost-window' === $marker) {
            // The probe delivered SIGTERM while the driver had already drained its signal queue
            // and was about to block without a deadline, which is the window where a signal
            // cannot wake the loop. The broker must still stop on its own.
            $this->assertBrokerStopsOnItsOwn($probe, $pid, $worker);
        } else {
            // The loop never waited without a deadline, so the probe found no window to target.
            // The broker must still stop when the signal arrives from outside.
            $deadline = $this->probeDeadlineSeconds($log);
            $this->assertIsFloat($deadline, 'The probe must report the deadline it observed.');
            $this->assertLessThanOrEqual(2.0, $deadline, 'The loop must keep waking near its one-second interval.');
            $this->assertTrue(posix_kill($pid, \SIGTERM), 'The test must signal the broker the way a shell kill does.');
            $this->assertBrokerStops($probe, $pid, $worker, 'A signalled broker must stop.');
        }

        $this->assertNotContains('lost-window', $this->probeMarkers($log), 'The broker must never wait for events without a deadline.');
        $this->assertFileDoesNotExist($this->socket, 'A stopped broker must release the endpoint.');
        $this->assertFileExists($database, 'Shutdown must preserve the database.');
        $this->assertTrackedGone();
    }

    public function testStoppedPersistenceWorkerCannotWedgeShutdown(): void
    {
        $database = $this->fixture->path();
        $trace = $this->tracePath();
        $broker = $this->startBroker($database, $this->socket, $trace);
        // Positive barrier for the trace wiring: the destination is created before the broker serves,
        // so a wedged run leaves a readable destination. An empty on-disk file no longer identifies
        // the missing stage, because flushing is deferred until after cleanup.
        $this->assertFileExists($trace, 'The trace destination must exist before the broker serves.');
        $this->assertSame('', (string) file_get_contents($trace), 'A serving broker must record no shutdown milestone.');
        $owned = $this->trackOwned($broker->getPid());
        $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');
        $worker = $owned['workers'][0];

        $client = $this->client();
        $confirmed = $client->send('jobs', 'confirmed before stop');
        $this->assertGreaterThan(0, $confirmed);
        $client->close();
        array_pop($this->clients);

        $this->assertNotSame(0, posix_geteuid());
        $this->assertSame(posix_geteuid(), fileowner('/proc/'.$worker), 'The stopped process must be this user\'s persistence worker.');
        $this->assertTrue(posix_kill($worker, \SIGSTOP), 'The test must stop only the persistence worker.');
        $this->assertSame('T', $this->waitForState($worker, 'T'), 'A stopped worker must be observable before the broker is signalled.');

        $this->assertTrue(posix_kill($broker->getPid(), \SIGTERM), 'The test must signal the broker the same way a shell kill does.');
        try {
            $exit = $broker->join(new TimeoutCancellation(15));
        } catch (CancelledException $error) {
            // A join timeout still fails the test, but the broker and worker state are retained.
            // An empty on-disk trace no longer identifies the missing stage during a hang.
            $report = $this->captureWedgeEvidence($broker->getPid(), $worker, $trace);
            $this->fail('Shutdown wedged past its 15s budget; process evidence at '.$report.' and trace at '.$trace.': '.$error->getMessage());
        }
        $this->assertNotSame(0, $exit, 'A stopped persistence worker must not stop the broker from exiting.');
        $this->assertSame(
            [
                'signal-dispatched',
                'cancellation-requested',
                'cancellation-delivered',
                'shutdown-requested',
                'deadline-armed',
                'deadline-fired',
                'persistence-force-stop',
            ],
            $this->traceMilestones($trace),
            'The broker must record every shutdown edge it reached.',
        );
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket, 'Shutdown must release the endpoint without the worker responding.');
        $this->assertFileExists($database, 'Shutdown must preserve confirmed data.');
        // The trace is support evidence for a wedge: a clean run keeps nothing.
        @unlink($trace);

        $restarted = $this->startBroker($database, $this->socket);
        $this->trackOwned($restarted->getPid());
        $client = $this->client();
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery, 'A confirmation must survive a force-stopped shutdown.');
        $this->assertSame($confirmed, $delivery->id);
        $this->assertSame('confirmed before stop', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);
        $this->stopBroker($restarted);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    /**
     * Runs the real Broker Command through Symfony Console and records the SIGTERM handler at
     * readiness. The fixture does not duplicate signal registration; production code never
     * inspects handlers.
     */
    public function testShutdownTraceFixtureRecordsTheSigtermHandlerAtReadiness(): void
    {
        $database = $this->fixture->path();
        $trace = $this->tracePath();
        $handler = $this->evidenceDirectory().'/handler-'.(int) getmypid().'-'.time().'.json';
        $broker = Process::start([
            \PHP_BINARY,
            __DIR__.'/Fixtures/broker-shutdown-trace-probe.php',
            $database,
            $this->socket,
            $trace,
            $handler,
        ], null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
        $this->processes[] = $broker;
        $line = (new BufferedReader($broker->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null);
        $this->assertSame($broker->getPid(), $ready['pid'] ?? null);
        $this->trackOwned($broker->getPid());

        $this->assertFileExists($handler, 'The fixture must record the SIGTERM handler at readiness.');
        $probe = json_decode((string) file_get_contents($handler), true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($probe);
        $this->assertSame('sigterm-handler', $probe['event'] ?? null);
        $this->assertSame('Closure', $probe['type'] ?? null);
        $this->assertSame('Revolt\\EventLoop\\Driver\\StreamSelectDriver', $probe['scope'] ?? null);
        $this->assertSame('handleSignal', $probe['name'] ?? null);

        $this->assertTrue(posix_kill($broker->getPid(), \SIGTERM));
        $this->assertSame(0, $broker->join(new TimeoutCancellation(10)));
        $this->assertSame(
            [
                'signal-dispatched',
                'cancellation-requested',
                'cancellation-delivered',
                'shutdown-requested',
                'deadline-armed',
            ],
            $this->traceMilestones($trace),
            'A clean stop must record the pre-stop edges and arm the deadline without firing it.',
        );
        @unlink($trace);
        @unlink($handler);
        $this->assertTrackedGone();
    }

    private function startBroker(string $database, string $socket, ?string $traceFile = null): Process
    {
        $process = $this->spawn($database, $socket, $traceFile);
        $line = (new BufferedReader($process->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null, 'Readiness must be reported as a positive event.');
        $this->assertSame($process->getPid(), $ready['pid'] ?? null);
        $this->assertSame($socket, $ready['endpoint'] ?? null);
        $this->processes[] = $process;

        return $process;
    }

    private function conflictingBroker(string $database, string $socket): Process
    {
        $process = $this->spawn($database, $socket);
        $this->processes[] = $process;

        return $process;
    }

    /**
     * Starts the signal-window probe, which runs the real broker command and reports how the
     * waiting loop looks from inside the driver's select call.
     */
    private function startSignalWindowProbe(string $database, string $log): Process
    {
        $process = Process::start([
            \PHP_BINARY,
            __DIR__.'/Fixtures/broker-signal-window-probe.php',
            $log,
            $database,
            $this->socket,
        ], null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
        $line = (new BufferedReader($process->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null, 'The probe broker must report readiness.');
        $this->processes[] = $process;

        return $process;
    }

    /** Waits for one of the probe's markers, so a stale probe fails instead of passing silently. */
    private function awaitProbeMarker(string $log, array $markers): string
    {
        $deadline = microtime(true) + self::PROBE_MARKER_TIMEOUT_SECONDS;
        do {
            $seen = $this->probeMarkers($log);
            foreach ($markers as $marker) {
                if (\in_array($marker, $seen, true)) {
                    return $marker;
                }
            }
            usleep(5000);
        } while (microtime(true) < $deadline);

        $this->fail('The probe recorded no select-window marker within '.self::PROBE_MARKER_TIMEOUT_SECONDS.'s: '.implode(', ', $seen));
    }

    /** @return list<string> */
    private function probeMarkers(string $log): array
    {
        $markers = [];
        foreach ($this->probeEntries($log) as $entry) {
            if (\is_string($entry['probe'] ?? null)) {
                $markers[] = $entry['probe'];
            }
        }

        return $markers;
    }

    /** The select deadline the probe observed while the broker waits for events, in seconds. */
    private function probeDeadlineSeconds(string $log): ?float
    {
        foreach ($this->probeEntries($log) as $entry) {
            if ('deadline' === ($entry['probe'] ?? null)) {
                $seconds = $entry['seconds'] ?? null;

                return \is_int($seconds) || \is_float($seconds) ? (float) $seconds : null;
            }
        }

        return null;
    }

    /**
     * The probe's JSON records, oldest first.
     *
     * @return list<array<string, mixed>>
     */
    private function probeEntries(string $log): array
    {
        $lines = is_file($log) ? file($log, \FILE_IGNORE_NEW_LINES | \FILE_SKIP_EMPTY_LINES) : [];
        $entries = [];
        foreach (false === $lines ? [] : $lines as $line) {
            $entry = json_decode($line, true, 8);
            if (\is_array($entry)) {
                $entries[] = $entry;
            }
        }

        return $entries;
    }

    private function assertBrokerStopsOnItsOwn(Process $probe, int $pid, int $worker): void
    {
        $this->assertBrokerStops($probe, $pid, $worker, 'A signal delivered inside the select window must still stop the broker.');
    }

    private function assertBrokerStops(Process $probe, int $pid, int $worker, string $message): void
    {
        try {
            $exit = $probe->join(new TimeoutCancellation(self::SIGNAL_BOUND_SECONDS));
        } catch (CancelledException $error) {
            $report = $this->captureWedgeEvidence($pid, $worker);
            $this->fail($message.' Evidence at '.$report.': '.$error->getMessage());
        }
        $this->assertSame(0, $exit, $message);
    }

    private function spawn(string $database, string $socket, ?string $traceFile = null): Process
    {
        $command = [
            \PHP_BINARY,
            \dirname(__DIR__, 2).'/bin/sqlite-queue',
            'broker',
            '--database='.$database,
            '--endpoint='.$socket,
        ];
        if (null !== $traceFile) {
            $command[] = '--trace-file='.$traceFile;
        }

        return Process::start($command, null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
    }

    private function client(): Client
    {
        $client = Client::connect($this->socket, 10);
        $this->clients[] = $client;

        return $client;
    }

    /** @return array{launchers: list<int>, workers: list<int>} */
    private function trackOwned(int $pid): array
    {
        $owned = ProcessTree::ownedBy($pid);
        $this->tracked = [...$this->tracked, ...$owned['launchers'], ...$owned['workers'], $pid];

        return $owned;
    }

    private function stopBroker(Process $broker): void
    {
        $broker->signal(\SIGTERM);
        $this->assertSame(0, $broker->join(new TimeoutCancellation(10)), 'A signal must stop the broker cleanly.');
    }

    private function assertTrackedGone(): void
    {
        $snapshot = ProcessTree::snapshot();
        foreach ($this->tracked as $pid) {
            $this->assertArrayNotHasKey($pid, $snapshot, 'A broker-owned process survived shutdown.');
        }
    }

    private function waitForState(int $pid, string $expected): string
    {
        $deadline = microtime(true) + self::STATE_POLL_TIMEOUT_SECONDS;
        do {
            $state = $this->processState($pid);
            if ($expected === $state) {
                return $state;
            }
            usleep(1000);
        } while (microtime(true) < $deadline);

        return $state;
    }

    private function processState(int $pid): string
    {
        $stat = @file_get_contents('/proc/'.$pid.'/stat');
        if (false === $stat) {
            return '';
        }
        $end = strrpos($stat, ')');

        return false === $end ? '' : substr($stat, $end + 2, 1);
    }

    /**
     * Snapshot the wedged tree to a report file before fallback cleanup destroys it.
     *
     * States and signal masks only; no payloads, no broker debug output.
     */
    private function captureWedgeEvidence(int $brokerPid, int $worker, ?string $trace = null): string
    {
        $directory = \dirname(__DIR__, 2).'/var/qa/p1-wedge';
        if (!is_dir($directory) && !mkdir($directory, 0o700, true) && !is_dir($directory)) {
            return $directory.' (unwritable)';
        }
        $path = $directory.'/wedge-'.getmypid().'-'.time().'.log';
        $lines = [];
        $snapshot = ProcessTree::snapshot();
        $owned = ProcessTree::ownedBy($brokerPid);
        foreach (array_unique([...$owned['launchers'], ...$owned['workers'], $brokerPid, $worker]) as $pid) {
            $stat = @file_get_contents('/proc/'.$pid.'/stat');
            $state = '?';
            if (\is_string($stat) && 1 === preg_match('/\)\s+(\S)/', $stat, $match)) {
                $state = $match[1];
            }
            $masks = [];
            $status = @file_get_contents('/proc/'.$pid.'/status');
            if (\is_string($status)) {
                foreach (['SigBlk', 'SigIgn', 'SigCgt', 'SigPnd', 'ShdPnd'] as $name) {
                    if (1 === preg_match('/^'.$name.':\s+([0-9a-f]+)/m', $status, $match)) {
                        $masks[] = $name.'='.$match[1];
                    }
                }
            }
            $inTree = isset($snapshot[$pid]) ? substr($snapshot[$pid]['cmd'], -60) : 'absent-from-snapshot';
            $syscall = trim((string) @file_get_contents('/proc/'.$pid.'/syscall'));
            $lines[] = "pid={$pid} state={$state} wchan=".trim((string) @file_get_contents('/proc/'.$pid.'/wchan')).' '.implode(' ', $masks).' syscall='.$syscall.' cmd='.$inTree;
        }
        $lines[] = 'socket-exists='.var_export(file_exists($this->socket), true);
        if (null !== $trace && is_file($trace)) {
            // Milestones only: the trace carries a pid and a monotonic timestamp per shutdown edge.
            // The byte count makes an empty trace explicit: the destination was usable, yet no
            // milestone was recorded, which a failed write would also produce.
            $lines[] = 'trace='.$trace.' bytes='.(string) @filesize($trace);
            foreach (explode("\n", trim((string) @file_get_contents($trace))) as $line) {
                if ('' !== $line) {
                    $lines[] = 'trace:'.$line;
                }
            }
        }
        @file_put_contents($path, implode("\n", $lines)."\n");

        return $path;
    }

    /** Trace path for one P1 run; the file is retained only when the shutdown wedges. */
    private function tracePath(): string
    {
        static $sequence = 0;

        return $this->evidenceDirectory().'/trace-'.(int) getmypid().'-'.hrtime(true).'-'.(++$sequence).'.log';
    }

    /** Private scratch directory for support evidence; created on demand. */
    private function evidenceDirectory(): string
    {
        $directory = \dirname(__DIR__, 2).'/var/qa/p1-wedge';
        if (!is_dir($directory) && !mkdir($directory, 0o700, true) && !is_dir($directory)) {
            throw new \RuntimeException('Cannot create the evidence directory '.$directory.'.');
        }

        return $directory;
    }

    /**
     * Shutdown milestones in the order the broker recorded them.
     *
     * @return list<string>
     */
    private function traceMilestones(string $trace): array
    {
        $contents = @file_get_contents($trace);
        if (!\is_string($contents)) {
            return [];
        }
        $milestones = [];
        foreach (explode("\n", trim($contents)) as $line) {
            $entry = '' === $line ? null : json_decode($line, true, 8);
            if (\is_array($entry) && \is_string($entry['event'] ?? null)) {
                $milestones[] = $entry['event'];
            }
        }

        return $milestones;
    }
}
