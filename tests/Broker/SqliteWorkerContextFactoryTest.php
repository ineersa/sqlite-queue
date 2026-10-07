<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\ByteStream\PendingReadError;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerContextFactory;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Revolt\EventLoop\CallbackType;

use function Amp\async;

final class SqliteWorkerContextFactoryTest extends TestCase
{
    /** Path fragment that identifies this test's probe child in the process tree. */
    private const PROBE_SCRIPT = 'persistence-probe.php';

    private ?SqliteWorkerContextFactory $factory = null;
    private ?int $probePid = null;
    private ?int $launcherPid = null;

    protected function tearDown(): void
    {
        try {
            // A stopped launcher must never outlive the test that stopped it.
            if (null !== $this->launcherPid && isset(ProcessTree::snapshot()[$this->launcherPid]) && \function_exists('posix_kill')) {
                @posix_kill($this->launcherPid, \SIGCONT);
                @posix_kill($this->launcherPid, \SIGKILL);
            }
            // Never leave the probe child behind when an assertion fails mid-test.
            $this->factory?->forceStopAll();
            // Belt and suspenders: a failed force-stop must still not leak the probe.
            if (null !== $this->probePid && isset(ProcessTree::snapshot()[$this->probePid]) && \function_exists('posix_kill')) {
                @posix_kill($this->probePid, \SIGKILL);
            }
        } catch (\Throwable) {
        } finally {
            $this->factory = null;
            $this->probePid = null;
            $this->launcherPid = null;
            parent::tearDown();
        }
    }

    public function testForceStopAllKillsSpawnedChildrenWithoutJoining(): void
    {
        if (!method_exists(SqliteWorkerContextFactory::class, 'start')) {
            $this->markTestSkipped('IPC factory create() replaces ContextFactory::start(); rewrite this probe after worker merge.');
        }
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        async(function (): void {
            $factory = new SqliteWorkerContextFactory();
            $this->factory = $factory;
            $context = $factory->start([__DIR__.'/Fixtures/persistence-probe.php'], new TimeoutCancellation(10));
            // Readiness comes from the child itself, not from a sleep: a child that dies
            // naturally can never send this.
            $this->assertSame('ready', $context->receive(new TimeoutCancellation(10)));
            $this->assertCount(1, $factory->created());
            $pid = $factory->worker()->pid();
            $this->probePid = $pid;
            $this->assertArrayHasKey($pid, ProcessTree::snapshot(), 'The spawned child must be observable before the kill.');

            // The driver alone joins its contexts; the factory only guarantees no survivor.
            $factory->forceStopAll();
            $deadline = microtime(true) + 5;
            do {
                if (!isset(ProcessTree::snapshot()[$pid])) {
                    break;
                }
                usleep(50_000);
            } while (microtime(true) < $deadline);
            $this->assertArrayNotHasKey($pid, ProcessTree::snapshot(), 'forceStopAll must leave no surviving child.');
        })->await(new TimeoutCancellation(20));
    }

    public function testShutdownBudgetCancelsPipeReadsAndReleasesTheirWatchers(): void
    {
        if (!method_exists(SqliteWorkerContextFactory::class, 'start')) {
            $this->markTestSkipped('IPC factory create() replaces ContextFactory::start(); rewrite this probe after worker merge.');
        }
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        async(function (): void {
            $factory = new SqliteWorkerContextFactory();
            $this->factory = $factory;
            $context = $factory->start([__DIR__.'/Fixtures/persistence-probe.php'], new TimeoutCancellation(10));
            $this->assertSame('ready', $context->receive(new TimeoutCancellation(10)));
            $worker = $factory->worker();
            $this->probePid = $worker->pid();

            // The drain reads are queued tasks, so run the loop once to put both pipes under a
            // watcher that keeps the loop alive.
            $this->turnEventLoop();
            $watched = $this->enabledReadableWatchers();
            $this->assertCount(2, $watched, 'Both pipe reads must hold an enabled, referenced watcher before shutdown.');

            $launcher = $this->launcherFor($worker->pid());
            if (null === $launcher) {
                $this->fail('The probe child must run behind the shell launcher Amp creates for it.');
            }
            $this->launcherPid = $launcher;
            $this->assertNotSame(0, posix_geteuid(), 'Stopping a foreign process would require root privileges.');
            $this->assertSame(posix_geteuid(), fileowner('/proc/'.$launcher), 'The launcher must belong to this user before it is stopped.');
            $this->assertTrue(posix_kill($launcher, \SIGSTOP), 'The test must stop only the launcher.');

            try {
                $this->waitForState($launcher, 'T');

                // A stopped launcher holds the pipe write ends, so the pipes never reach EOF and
                // only the shared budget can end the wait.
                $budget = new DeferredCancellation();
                $closing = async(static fn () => $worker->close($budget->getCancellation()));
                $this->turnEventLoop();
                $this->assertFalse($closing->isComplete(), 'Open pipe writers must keep close pending.');
                $budget->cancel();
                $expired = false;
                try {
                    $closing->await(new TimeoutCancellation(10));
                } catch (CancelledException) {
                    $expired = true;
                }
                $this->assertTrue($expired, 'The shared budget must end a wait on pipes that stay open.');

                // The cancellation callbacks run on the loop, so give them their turn before
                // inspecting the watchers that the abandoned reads were holding.
                $this->turnEventLoop();
                $driver = EventLoop::getDriver();
                foreach ($watched as $id) {
                    $this->assertContains($id, $driver->getIdentifiers(), 'A cancelled pipe read must not be rebuilt.');
                    $this->assertFalse($driver->isEnabled($id), 'A cancelled pipe read must release its readability watcher, or the loop cannot exit.');
                }

                // A settled read releases the stream's read slot; a pending one rejects a second read.
                $readCancellation = new DeferredCancellation();
                $read = async(static fn () => $context->getStdout()->read($readCancellation->getCancellation()));
                $this->turnEventLoop();
                $readCancellation->cancel();
                try {
                    $read->await(new TimeoutCancellation(10));
                    $this->fail('The probe read must end through its own cancellation, not with data or EOF.');
                } catch (PendingReadError) {
                    $this->fail('The cancelled pipe read must not stay pending after shutdown.');
                } catch (CancelledException) {
                    // The stream accepted a new read, so the abandoned drain read had settled.
                }
            } finally {
                $this->resumeLauncher();
            }
        })->await(new TimeoutCancellation(20));
    }

    public function testChildProcessRunsWithTheSameInterpreterAndTheBrokerEnvironment(): void
    {
        $marker = 'SQLITE_QUEUE_PARENT_MARKER';
        $markerValue = 'inherited-'.bin2hex(random_bytes(4));
        $directive = 'default_mimetype';
        $configValue = 'application/x-sqlite-queue-child';
        $temporary = \dirname(__DIR__, 2).'/var';
        $scan = new IsolatedDatabase();
        file_put_contents($scan->directory().'/99-sqlite-queue-probe.ini', $directive.'='.$configValue."\n");

        $restore = [];
        foreach ([$marker, 'TMPDIR', 'PHP_INI_SCAN_DIR'] as $name) {
            $restore[$name] = getenv($name);
        }

        try {
            // A child that started with a replaced environment would see none of these values.
            $existingScan = $restore['PHP_INI_SCAN_DIR'];
            putenv($marker.'='.$markerValue);
            putenv('TMPDIR='.$temporary);
            // A leading colon keeps the compiled-in scan directory, which is where the shared
            // sqlite3 extension is loaded from.
            putenv('PHP_INI_SCAN_DIR='.(\is_string($existingScan) && '' !== $existingScan ? $existingScan.':' : ':').$scan->directory());

            async(function () use ($marker, $directive, $markerValue, $configValue, $temporary): void {
                $factory = new SqliteWorkerContextFactory();
                $this->factory = $factory;
                $context = $factory->start(
                    [__DIR__.'/Fixtures/persistence-env-probe.php', $marker, $directive],
                    new TimeoutCancellation(10),
                );
                $report = $context->receive(new TimeoutCancellation(10));
                $this->assertIsArray($report);
                $this->assertSame(\PHP_BINARY, $report['php_binary'] ?? null, 'The child must run the broker interpreter.');
                $this->assertSame(\PHP_SAPI, $report['sapi'] ?? null);
                $this->assertSame($markerValue, $report['marker'] ?? null, 'The child must inherit the broker environment.');
                $this->assertSame($temporary, $report['tmpdir'] ?? null, 'The child must inherit TMPDIR.');
                $this->assertSame($configValue, $report['config'] ?? null, 'The child must read the broker PHP configuration.');
                $this->assertTrue($report['sqlite3'] ?? null, 'The child must keep loading sqlite3 from the scan directory.');

                // The driver joins the contexts of its own connections. This probe has no other
                // join owner, so joining here proves a clean exit instead of a kill.
                $this->assertNull($context->join(new TimeoutCancellation(10)));
            })->await(new TimeoutCancellation(20));
        } finally {
            foreach ($restore as $name => $value) {
                if (\is_string($value)) {
                    putenv($name.'='.$value);
                } else {
                    putenv($name);
                }
            }
            $scan->remove();
        }
    }

    /** Runs one turn of the event loop so queued tasks and stream watchers reach a steady state. */
    private function turnEventLoop(): void
    {
        async(static function (): void {
        })->await(new TimeoutCancellation(2));
    }

    /** @return list<string> Readable watchers that currently keep the loop alive. */
    private function enabledReadableWatchers(): array
    {
        $driver = EventLoop::getDriver();
        $identifiers = [];
        foreach ($driver->getIdentifiers() as $id) {
            if (CallbackType::Readable !== $driver->getType($id) || !$driver->isEnabled($id) || !$driver->isReferenced($id)) {
                continue;
            }
            $identifiers[] = $id;
        }

        return $identifiers;
    }

    /** The shell Amp starts around the probe child, which holds the pipes this test keeps open. */
    private function launcherFor(int $worker): ?int
    {
        foreach (ProcessTree::snapshot() as $pid => $process) {
            if ($pid === $worker || getmypid() !== $process['ppid']) {
                continue;
            }
            if (str_contains($process['cmd'], self::PROBE_SCRIPT)) {
                return $pid;
            }
        }

        return null;
    }

    private function waitForState(int $pid, string $state): void
    {
        $deadline = microtime(true) + 2;
        do {
            if ($state === $this->processState($pid)) {
                return;
            }
            usleep(10_000);
        } while (microtime(true) < $deadline);

        $this->assertSame($state, $this->processState($pid), 'A stopped launcher must be observable before the pipes are observed.');
    }

    /** One-letter process state from /proc, or an empty string when the process is gone. */
    private function processState(int $pid): string
    {
        $stat = @file_get_contents('/proc/'.$pid.'/stat');
        if (!\is_string($stat)) {
            return '';
        }
        $end = strrpos($stat, ')');
        if (false === $end) {
            return '';
        }

        return $stat[$end + 2] ?? '';
    }

    /** Resumes and kills the stopped launcher, which lives outside the factory's ownership. */
    private function resumeLauncher(): void
    {
        if (null === $this->launcherPid) {
            return;
        }
        if (\function_exists('posix_kill') && isset(ProcessTree::snapshot()[$this->launcherPid])) {
            @posix_kill($this->launcherPid, \SIGCONT);
            @posix_kill($this->launcherPid, \SIGKILL);
        }
        $this->launcherPid = null;
    }
}
