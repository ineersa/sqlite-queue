<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\MalformedReceiptException;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageCapacityException;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageFailureException;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueWorker;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerContextFactory;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

final class SqliteQueueWorkerTest extends TestCase
{
    private const int CLOCK_MILLISECONDS = 1_700_000_000_000;

    private ?SqliteQueueWorker $worker = null;
    private ?IsolatedDatabase $database = null;

    protected function tearDown(): void
    {
        try {
            $this->worker?->close(new TimeoutCancellation(5));
        } catch (\Throwable) {
            try {
                $this->worker?->handle()->forceStop();
            } catch (\Throwable) {
            }
        } finally {
            $this->worker = null;
            $this->database?->remove();
            $this->database = null;
            parent::tearDown();
        }
    }

    public function testSendReceiveAcknowledgeRoundTripPreservesBinaryPayloads(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $queue = new QueueName('jobs');
            $body = "bin\0ary";
            $headers = "h\xFFaders";
            $id = $worker->send($queue, $body, $headers);
            $this->assertSame(1, $id);
            $this->assertSame('wal', strtolower($worker->configuration()['journal_mode']));
            $this->assertSame(SqliteSynchronousMode::Normal, $worker->synchronousMode());

            $delivery = $worker->receive($queue, 'owner-1');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame($body, $delivery->body);
            $this->assertSame($headers, $delivery->headers);
            $worker->acknowledge($delivery->receipt, 'owner-1');
            $this->assertNull($worker->receive($queue, 'owner-2'));
        })->await(new TimeoutCancellation(20));
    }

    public function testCancellationBeforeDispatchPreventsSend(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $lifetime = new DeferredCancellation();
            $lifetime->cancel();
            try {
                $worker->send(new QueueName('jobs'), 'payload', cancellation: $lifetime->getCancellation());
                $this->fail('Cancelled admission must throw.');
            } catch (ClientContextClosedException) {
            }
            $this->assertNull($worker->receive(new QueueName('jobs'), 'owner-1'));
        })->await(new TimeoutCancellation(20));
    }

    public function testDomainReceiptFailureDoesNotFailLane(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            try {
                $worker->acknowledge('1:'.str_repeat('a', 64), 'owner-1');
                $this->fail('Unknown receipt must raise a domain failure.');
            } catch (MalformedReceiptException|\Ineersa\SqliteQueue\Exception\NoActiveReservationException) {
            }
            $id = $worker->send(new QueueName('jobs'), 'still-open');
            $this->assertSame(1, $id);
        })->await(new TimeoutCancellation(20));
    }

    public function testQueuedCancellationRemovesWorkWithoutConsumingWireIds(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $queue = new QueueName('jobs');
            $count = new \ReflectionProperty($worker, 'admittedOperations');
            $dispatched = new \ReflectionProperty($worker, 'dispatched');
            $nextId = new \ReflectionProperty($worker, 'nextRequestId');
            $lifetime = new DeferredCancellation();
            $blocked = [];
            for ($i = 0; $i < 2; ++$i) {
                $blocked[] = async(static fn (): ?\Ineersa\SqliteQueue\DTO\DeliveryDTO => $worker->receive($queue, 'blocker-'.$i));
            }
            while (\count($dispatched->getValue($worker)) < 2) {
                $this->yieldOnce();
            }
            $wireBefore = $nextId->getValue($worker);
            $this->assertSame(4, $wireBefore);
            $queued = async(static fn (): int => $worker->send($queue, 'queued-cancel', cancellation: $lifetime->getCancellation()));
            while ($count->getValue($worker) < 3) {
                $this->yieldOnce();
            }
            $this->assertSame($wireBefore, $nextId->getValue($worker));
            $lifetime->cancel();
            try {
                $queued->await(new TimeoutCancellation(5));
                $this->fail('Queued cancellation must return without dispatch.');
            } catch (ClientContextClosedException) {
            }
            $this->assertSame(2, $count->getValue($worker));
            $this->assertSame($wireBefore, $nextId->getValue($worker));
            foreach ($blocked as $future) {
                $this->assertNull($future->await(new TimeoutCancellation(5)));
            }
            $id = $worker->send($queue, 'after-cancel');
            $this->assertSame(1, $id);
        })->await(new TimeoutCancellation(20));
    }

    public function testScriptedPeerReceivesSecondRequestBeforeFirstReply(): void
    {
        async(function (): void {
            $worker = $this->startScriptedWorker();
            $queue = new QueueName('jobs');
            $first = async(static fn (): int => $worker->send($queue, 'A'));
            $second = async(static fn (): int => $worker->send($queue, 'B'));
            $this->assertSame(1, $first->await(new TimeoutCancellation(5)));
            $this->assertSame(2, $second->await(new TimeoutCancellation(5)));
        })->await(new TimeoutCancellation(20));
    }

    public function testInFlightByteCreditBlocksThirdMaxPayloadReservation(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $queue = new QueueName('jobs');
            $inFlightBytes = new \ReflectionProperty($worker, 'inFlightPayloadBytes');
            $dispatched = new \ReflectionProperty($worker, 'dispatched');
            $queuedDepth = new \ReflectionProperty($worker, 'queue');
            $first = async(static fn (): ?\Ineersa\SqliteQueue\DTO\DeliveryDTO => $worker->receive($queue, 'owner-a'));
            $second = async(static fn (): ?\Ineersa\SqliteQueue\DTO\DeliveryDTO => $worker->receive($queue, 'owner-b'));
            while (\count($dispatched->getValue($worker)) < 2) {
                $this->yieldOnce();
            }
            $this->assertSame(2 * Limits::MAX_PAYLOAD, $inFlightBytes->getValue($worker));
            $third = async(static fn (): ?\Ineersa\SqliteQueue\DTO\DeliveryDTO => $worker->receive($queue, 'owner-c'));
            while (0 === \count($queuedDepth->getValue($worker))) {
                $this->yieldOnce();
            }
            $this->assertCount(2, $dispatched->getValue($worker));
            $this->assertNull($first->await(new TimeoutCancellation(5)));
            $this->assertNull($second->await(new TimeoutCancellation(5)));
            $this->assertNull($third->await(new TimeoutCancellation(5)));
        })->await(new TimeoutCancellation(20));
    }

    public function testMalformedWorkerReplyFailsLaneWithoutReplay(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $pid = $worker->handle()->pid();
            $context = $worker->handle()->context();
            $context->send([
                'id' => 99,
                'op' => 'send',
                'data' => [
                    'queue' => 'jobs',
                    'body' => 'x',
                    'headers' => '',
                    'delay' => 0,
                ],
            ]);
            try {
                $worker->send(new QueueName('jobs'), 'after-desync');
                $this->fail('Desynchronized lane must fail closed.');
            } catch (StorageFailureException) {
            }
            if (ProcessTree::available()) {
                $deadline = microtime(true) + 5;
                do {
                    if (!isset(ProcessTree::snapshot()[$pid])) {
                        break;
                    }
                    usleep(20_000);
                } while (microtime(true) < $deadline);
                $this->assertArrayNotHasKey($pid, ProcessTree::snapshot());
            }
            try {
                $worker->send(new QueueName('jobs'), 'replay');
                $this->fail('Failed lane must reject later operations.');
            } catch (StorageFailureException) {
            }
        })->await(new TimeoutCancellation(20));
    }

    public function testAdmissionCapacityIsLocalAndRecoverable(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $payload = str_repeat('p', Limits::MAX_PAYLOAD);
            $reserved = [];
            for ($i = 0; $i < Limits::MAX_CONNECTIONS; ++$i) {
                $reserved[] = $this->forceReserve($worker, \strlen($payload));
            }
            try {
                $worker->send(new QueueName('jobs'), $payload);
                $this->fail('Exhausted payload budget must refuse locally.');
            } catch (StorageCapacityException) {
            }
            foreach ($reserved as $release) {
                $release();
            }
            $id = $worker->send(new QueueName('jobs'), 'after-capacity');
            $this->assertSame(1, $id);
        })->await(new TimeoutCancellation(20));
    }

    public function testGracefulCloseJoinsOnceAndReleasesPipes(): void
    {
        async(function (): void {
            $worker = $this->startWorker();
            $pid = $worker->handle()->pid();
            $worker->close(new TimeoutCancellation(5));
            $this->worker = null;
            if (ProcessTree::available()) {
                $this->assertArrayNotHasKey($pid, ProcessTree::snapshot());
            }
        })->await(new TimeoutCancellation(20));
    }

    private function startWorker(): SqliteQueueWorker
    {
        $this->database = new IsolatedDatabase();
        touch($this->database->path());
        $factory = new SqliteWorkerContextFactory([
            __DIR__.'/Fixtures/worker-controlled-clock.php',
            (string) self::CLOCK_MILLISECONDS,
        ]);
        $worker = $factory->create(
            $this->database->path(),
            60_000,
            SqliteSynchronousMode::Normal,
            new TimeoutCancellation(15),
        );
        $this->worker = $worker;

        return $worker;
    }

    private function startScriptedWorker(): SqliteQueueWorker
    {
        $this->database = new IsolatedDatabase();
        touch($this->database->path());
        $factory = new SqliteWorkerContextFactory([
            __DIR__.'/Fixtures/worker-scripted-pipeline.php',
        ]);
        $worker = $factory->create(
            $this->database->path(),
            60_000,
            SqliteSynchronousMode::Normal,
            new TimeoutCancellation(15),
        );
        $this->worker = $worker;

        return $worker;
    }

    private function yieldOnce(): void
    {
        $barrier = new DeferredFuture();
        EventLoop::queue(static fn () => $barrier->complete(null));
        $barrier->getFuture()->await(new TimeoutCancellation(5));
    }

    /**
     * @return callable(): void
     */
    private function forceReserve(SqliteQueueWorker $worker, int $payloadBytes): callable
    {
        $method = new \ReflectionMethod($worker, 'reserveAdmission');
        $method->invoke($worker, $payloadBytes);

        return static function () use ($worker, $payloadBytes): void {
            $release = new \ReflectionMethod($worker, 'releaseAdmission');
            $release->invoke($worker, $payloadBytes);
        };
    }
}
