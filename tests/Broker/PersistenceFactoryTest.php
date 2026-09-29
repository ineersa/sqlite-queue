<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\PersistenceFactory;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\TestCase;

use function Amp\async;

final class PersistenceFactoryTest extends TestCase
{
    private ?PersistenceFactory $factory = null;
    private ?int $probePid = null;

    protected function tearDown(): void
    {
        try {
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
            parent::tearDown();
        }
    }

    public function testForceStopAllKillsSpawnedChildrenWithoutJoining(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        async(function (): void {
            $factory = new PersistenceFactory();
            $this->factory = $factory;
            $context = $factory->start([__DIR__.'/Fixtures/persistence-probe.php'], new TimeoutCancellation(10));
            // Readiness comes from the child itself, not from a sleep: a child that dies
            // naturally can never send this.
            $this->assertSame('ready', $context->receive(new TimeoutCancellation(10)));
            $this->assertCount(1, $factory->created());
            $pid = $factory->persistence()->pid();
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
                $factory = new PersistenceFactory();
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
}
