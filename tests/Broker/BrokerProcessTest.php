<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\ByteStream\BufferedReader;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Process\Process;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\connect;

#[RequiresOperatingSystem('Linux')]
final class BrokerProcessTest extends TestCase
{
    /** Bounded wait for a kernel-reported process state; stops and signal delivery are not ordered. */
    /** Bounded wait for the signal-window probe to observe how the broker waits for events. */
    private const int PROBE_MARKER_TIMEOUT_SECONDS = 5;
    /** Bound on a broker shutdown; the production budget is five seconds. */
    private const int SIGNAL_BOUND_SECONDS = 10;
    /** Bound for positive notifier registration or deadline observation. */
    private const int CONTROL_OBSERVE_SECONDS = 5;
    /** Controlled clock origin for delayed WAIT process evidence, in Unix milliseconds. */
    private const int CONTROLLED_NOW_MS = 1_700_000_000_000;
    /** Positive subsecond delay used by real-process delayed WAIT evidence. */
    private const int SUBSECOND_DELAY_MS = 250;
    /** Real due-timer smoke delay; short enough for CI, long enough to schedule. */
    private const int REAL_TIMER_DELAY_MS = 200;
    /** Safety bound around a real due-timer WAIT smoke, in seconds. */
    private const float REAL_TIMER_SAFETY_SECONDS = 5.0;
    private ?IsolatedDatabase $fixture = null;
    private string $socket = '';
    /** @var list<Process> */
    private array $processes = [];
    /** @var list<Client> */
    private array $clients = [];
    /** @var list<int> */
    private array $tracked = [];
    /** @var list<Socket> */
    private array $controls = [];

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
        foreach ($this->controls as $control) {
            $control->close();
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
            $this->assertTrue(posix_kill($pid, \SIGTERM), 'The test must signal the broker the way a shell kill does.');
            $this->assertBrokerStops($probe, $pid, $worker, 'A signalled broker must stop.');
        }

        $this->assertNotContains('lost-window', $this->probeMarkers($log), 'The broker must never wait for events without a deadline.');
        $this->assertFileDoesNotExist($this->socket, 'A stopped broker must release the endpoint.');
        $this->assertFileExists($database, 'Shutdown must preserve the database.');
        $this->assertTrackedGone();
    }

    public function testIdleProtocolWaitWakesOnCommittedImmediatePublication(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $controlPath = $this->fixture->path('control.sock');
            [$broker, $control] = $this->startControlledClockBroker($database, $this->socket, $controlPath, self::CONTROLLED_NOW_MS);
            $owned = $this->trackOwned($broker->getPid());
            $this->assertCount(1, $owned['workers']);

            $waiter = $this->client();
            $publisher = $this->client();
            $this->assertNull($waiter->receive('jobs'));
            $waiting = async(static fn (): bool => $waiter->wait('jobs', 5_000));
            $this->awaitControlWaiters($control, 1);
            $publisher->send('jobs', 'wake');
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $delivery = $waiter->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame('wake', $delivery->body);
            $waiter->acknowledge($delivery->receipt);

            $waiter->close();
            array_pop($this->clients);
            $publisher->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($broker, $control);
            $this->assertTrackedGone();
            $this->assertFileDoesNotExist($this->socket);
        })->await();
    }

    public function testControlledClockDelayedWaitWakesWithoutAnotherPublication(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $controlPath = $this->fixture->path('control.sock');
            [$broker, $control] = $this->startControlledClockBroker($database, $this->socket, $controlPath, self::CONTROLLED_NOW_MS);
            $owned = $this->trackOwned($broker->getPid());
            $this->assertCount(1, $owned['workers']);

            $client = $this->client();
            $readyAt = self::CONTROLLED_NOW_MS + self::SUBSECOND_DELAY_MS;
            $id = $client->send('jobs', 'later', delay: self::SUBSECOND_DELAY_MS);
            $this->assertNull($client->receive('jobs'), 'A delayed message must not be claimable before its deadline.');
            $waiting = async(static fn (): bool => $client->wait('jobs', 5_000));
            $timerId = $this->awaitControlDeadline($control, $readyAt);
            $this->controlCommand($control, ['op' => 'set_now', 'now' => $readyAt]);
            $this->assertTrue($this->controlCommand($control, ['op' => 'fire', 'timer_id' => $timerId])['ok']);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('later', $delivery->body);
            $this->assertSame($readyAt, $delivery->availableAt);
            $client->acknowledge($delivery->receipt);

            $client->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($broker, $control);
            $this->assertTrackedGone();
            $this->assertFileDoesNotExist($this->socket);
        })->await();
    }

    public function testCliDelayedWaitWakesOnRealDueTimerWithoutAnotherPublication(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $broker = $this->startBroker($database, $this->socket);
            $owned = $this->trackOwned($broker->getPid());
            $this->assertCount(1, $owned['workers']);

            $client = $this->client();
            $id = $client->send('jobs', 'due', delay: self::REAL_TIMER_DELAY_MS);
            $this->assertNull($client->receive('jobs'));
            $waiting = async(static fn (): bool => $client->wait('jobs', 5_000));
            $this->assertTrue($waiting->await(new TimeoutCancellation(self::REAL_TIMER_SAFETY_SECONDS)));
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('due', $delivery->body);
            $client->acknowledge($delivery->receipt);

            $client->close();
            array_pop($this->clients);
            $this->stopBroker($broker);
            $this->assertTrackedGone();
        })->await();
    }

    public function testWaitCancellationAndDisconnectInvalidateOnlyThatClient(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $controlPath = $this->fixture->path('control.sock');
            [$broker, $control] = $this->startControlledClockBroker($database, $this->socket, $controlPath, self::CONTROLLED_NOW_MS);
            $this->trackOwned($broker->getPid());

            $waitingClient = $this->client();
            $session = new DeferredCancellation();
            $waiting = async(static fn (): bool => $waitingClient->wait('jobs', 5_000, $session->getCancellation()));
            $this->awaitControlWaiters($control, 1);
            $healthy = $this->client();
            $session->cancel();
            try {
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Cancellation during WAIT must invalidate the client.');
            } catch (TransportException) {
                $this->addToAssertionCount(1);
            }
            try {
                $waitingClient->send('jobs', 'must not reuse cancelled client');
                $this->fail('A cancelled WAIT client must stay closed.');
            } catch (TransportException) {
                $this->addToAssertionCount(1);
            }

            $peer = $this->client();
            $peerWaiting = async(static fn (): bool => $peer->wait('jobs', 5_000));
            $this->awaitControlWaiters($control, 1);
            $peer->close();
            array_pop($this->clients);
            try {
                $peerWaiting->await(new TimeoutCancellation(5));
                $this->fail('Disconnect during WAIT must invalidate that exchange.');
            } catch (TransportException) {
                $this->addToAssertionCount(1);
            }
            $this->awaitControlWaiters($control, 0);

            $healthy->send('jobs', 'still usable');
            $delivery = $healthy->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame('still usable', $delivery->body);
            $healthy->acknowledge($delivery->receipt);

            $healthy->close();
            array_pop($this->clients);
            $waitingClient->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($broker, $control);
            $this->assertTrackedGone();
        })->await();
    }

    public function testGracefulShutdownWithPendingWaitPreservesFutureMessage(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $controlPath = $this->fixture->path('control.sock');
            [$broker, $control] = $this->startControlledClockBroker($database, $this->socket, $controlPath, self::CONTROLLED_NOW_MS);
            $this->trackOwned($broker->getPid());

            $publisher = $this->client();
            $readyAt = self::CONTROLLED_NOW_MS + self::SUBSECOND_DELAY_MS;
            $id = $publisher->send('jobs', 'future', delay: self::SUBSECOND_DELAY_MS);
            $this->assertSame($readyAt, $this->availableAt($database, $id));
            $publisher->close();
            array_pop($this->clients);

            $waiter = $this->client();
            $waiting = async(static fn (): bool => $waiter->wait('jobs', 5_000));
            $this->awaitControlWaiters($control, 1);
            $this->stopControlledClockBroker($broker, $control);
            try {
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Shutdown must invalidate an outstanding WAIT.');
            } catch (TransportException) {
                $this->addToAssertionCount(1);
            }
            $this->assertTrackedGone();
            $this->assertFileDoesNotExist($this->socket);
            $this->assertSame($readyAt, $this->availableAt($database, $id), 'Shutdown must preserve the original availability deadline.');

            [$restarted, $restartControl] = $this->startControlledClockBroker(
                $database,
                $this->socket,
                $this->fixture->path('control-restart.sock'),
                self::CONTROLLED_NOW_MS,
            );
            $this->trackOwned($restarted->getPid());
            $client = $this->client();
            $this->assertNull($client->receive('jobs'), 'A future deadline must remain unavailable after restart.');
            $client->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($restarted, $restartControl);
            $this->assertTrackedGone();
            $this->assertSame($readyAt, $this->availableAt($database, $id));
        })->await();
    }

    public function testControlledClockRestartPreservesFutureAndOverdueDeadlines(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $controlPath = $this->fixture->path('control.sock');
            $start = self::CONTROLLED_NOW_MS;
            [$first, $control] = $this->startControlledClockBroker($database, $this->socket, $controlPath, $start);
            $this->trackOwned($first->getPid());

            $client = $this->client();
            $readyAt = $start + self::SUBSECOND_DELAY_MS;
            $id = $client->send('jobs', 'persisted', delay: self::SUBSECOND_DELAY_MS);
            $this->assertNull($client->receive('jobs'));
            $this->assertSame($readyAt, $this->availableAt($database, $id));
            $client->close();
            array_pop($this->clients);

            $this->controlCommand($control, ['op' => 'set_now', 'now' => $start + 25]);
            $this->stopControlledClockBroker($first, $control);
            $this->assertTrackedGone();
            $this->assertSame($readyAt, $this->availableAt($database, $id), 'Restart before the deadline must leave the original timestamp intact.');

            [$second, $beforeControl] = $this->startControlledClockBroker($database, $this->socket, $this->fixture->path('control-before.sock'), $start + 25);
            $this->trackOwned($second->getPid());
            $client = $this->client();
            $this->assertNull($client->receive('jobs'), 'A restarted broker must keep a future deadline unavailable.');
            $waiting = async(static fn (): bool => $client->wait('jobs', 5_000));
            $timerId = $this->awaitControlDeadline($beforeControl, $readyAt);
            $this->controlCommand($beforeControl, ['op' => 'set_now', 'now' => $readyAt]);
            $this->assertTrue($this->controlCommand($beforeControl, ['op' => 'fire', 'timer_id' => $timerId])['ok']);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame($readyAt, $delivery->availableAt);
            $client->acknowledge($delivery->receipt);
            $client->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($second, $beforeControl);
            $this->assertTrackedGone();

            [$seed, $seedControl] = $this->startControlledClockBroker($database, $this->socket, $this->fixture->path('control-seed.sock'), $readyAt);
            $this->trackOwned($seed->getPid());
            $seedClient = $this->client();
            $overdueId = $seedClient->send('jobs', 'overdue-seed', delay: self::SUBSECOND_DELAY_MS);
            $overdueAt = $readyAt + self::SUBSECOND_DELAY_MS;
            $this->assertSame($overdueAt, $this->availableAt($database, $overdueId));
            $seedClient->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($seed, $seedControl);
            $this->assertTrackedGone();
            $this->assertSame($overdueAt, $this->availableAt($database, $overdueId), 'Restart after the deadline must keep the original availability timestamp.');

            [$third, $overdueControl] = $this->startControlledClockBroker($database, $this->socket, $this->fixture->path('control-late.sock'), $overdueAt);
            $this->trackOwned($third->getPid());
            $client = $this->client();
            $waiting = async(static fn (): bool => $client->wait('jobs', 5_000));
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)), 'An overdue message must wake WAIT after restart without another publication.');
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($overdueId, $delivery->id);
            $this->assertSame($overdueAt, $delivery->availableAt);
            $client->acknowledge($delivery->receipt);
            $client->close();
            array_pop($this->clients);
            $this->stopControlledClockBroker($third, $overdueControl);
            $this->assertTrackedGone();
        })->await();
    }

    public function testCliIdleWaitWakesOnCommittedImmediatePublication(): void
    {
        async(function (): void {
            $database = $this->fixture->path();
            $broker = $this->startBroker($database, $this->socket);
            $owned = $this->trackOwned($broker->getPid());
            $this->assertCount(1, $owned['workers']);

            $waiter = $this->client();
            $publisher = $this->client();
            $this->assertNull($waiter->receive('jobs'));
            $waiting = async(static fn (): bool => $waiter->wait('jobs', 5_000));
            // A second empty receive proves the broker is still serving while WAIT is outstanding.
            // Exact waiter registration is covered by the controlled-clock process evidence.
            $this->assertNull($publisher->receive('jobs'));
            $publisher->send('jobs', 'cli-wake');
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $delivery = $waiter->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame('cli-wake', $delivery->body);
            $waiter->acknowledge($delivery->receipt);

            $waiter->close();
            array_pop($this->clients);
            $publisher->close();
            array_pop($this->clients);
            $this->stopBroker($broker);
            $this->assertTrackedGone();
            $this->assertFileDoesNotExist($this->socket);
        })->await();
    }

    private function startBroker(string $database, string $socket): Process
    {
        $process = $this->spawn($database, $socket);
        $line = (new BufferedReader($process->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null, 'Readiness must be reported as a positive event.');
        $this->assertSame($process->getPid(), $ready['pid'] ?? null);
        $this->assertSame($socket, $ready['endpoint'] ?? null);
        $this->processes[] = $process;

        return $process;
    }

    /**
     * Starts the controlled-clock probe and returns the process plus its control socket.
     *
     * @return array{0: Process, 1: Socket}
     */
    private function startControlledClockBroker(string $database, string $socket, string $controlPath, int $now): array
    {
        $process = Process::start([
            \PHP_BINARY,
            __DIR__.'/Fixtures/broker-controlled-clock-probe.php',
            $database,
            $socket,
            $controlPath,
            (string) $now,
        ], null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
        $line = (new BufferedReader($process->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null, 'The controlled-clock probe must report readiness.');
        $this->assertSame($process->getPid(), $ready['pid'] ?? null);
        $this->assertSame($socket, $ready['endpoint'] ?? null);
        $this->processes[] = $process;
        $control = connect('unix://'.$controlPath, cancellation: new TimeoutCancellation(5));
        $this->controls[] = $control;

        return [$process, $control];
    }

    private function stopControlledClockBroker(Process $broker, Socket $control): void
    {
        $this->controlCommand($control, ['op' => 'stop']);
        $this->assertSame(0, $broker->join(new TimeoutCancellation(10)), 'The controlled-clock probe must stop cleanly.');
        $control->close();
        $index = array_search($control, $this->controls, true);
        if (false !== $index) {
            unset($this->controls[$index]);
            $this->controls = array_values($this->controls);
        }
    }

    /**
     * @param array<string, mixed> $command
     *
     * @return array<string, mixed>
     */
    private function controlCommand(Socket $control, array $command): array
    {
        $control->write(json_encode($command, \JSON_THROW_ON_ERROR)."\n");
        $line = (new BufferedReader($control))->readUntil("\n", new TimeoutCancellation(5), 4096);
        $reply = json_decode((string) $line, true, 8, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($reply);

        return $reply;
    }

    private function awaitControlWaiters(Socket $control, int $count): void
    {
        $deadline = microtime(true) + self::CONTROL_OBSERVE_SECONDS;
        do {
            $reply = $this->controlCommand($control, ['op' => 'waiter_count']);
            if ($count === ($reply['count'] ?? null)) {
                return;
            }
            usleep(5_000);
        } while (microtime(true) < $deadline);

        $this->fail('Expected '.$count.' process notifier waiters.');
    }

    private function awaitControlDeadline(Socket $control, int $readyAt): string
    {
        $deadline = microtime(true) + self::CONTROL_OBSERVE_SECONDS;
        do {
            $reply = $this->controlCommand($control, ['op' => 'deadline', 'ready_at' => $readyAt]);
            if (\is_string($reply['timer_id'] ?? null)) {
                $this->assertFalse($reply['enabled'], 'The fixture must freeze the deadline before replying across IPC.');

                return $reply['timer_id'];
            }
            usleep(5_000);
        } while (microtime(true) < $deadline);

        $this->fail('Deadline timer was not scheduled at '.$readyAt.'.');
    }

    private function availableAt(string $database, int $id): int
    {
        $sqlite = new \SQLite3($database);
        try {
            $value = $sqlite->querySingle('SELECT available_at FROM queue_messages WHERE id = '.$id);
            $this->assertIsInt($value);

            return $value;
        } finally {
            $sqlite->close();
        }
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
            $this->fail($message.' Safety timeout: '.$error->getMessage());
        }
        $this->assertSame(0, $exit, $message);
    }

    private function spawn(string $database, string $socket): Process
    {
        $command = [
            \PHP_BINARY,
            \dirname(__DIR__, 2).'/bin/sqlite-queue',
            'broker',
            '--database='.$database,
            '--endpoint='.$socket,
        ];

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
}
