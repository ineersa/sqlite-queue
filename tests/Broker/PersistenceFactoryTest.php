<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\PersistenceFactory;
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
}
