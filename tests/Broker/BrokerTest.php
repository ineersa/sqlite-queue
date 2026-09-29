<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\InvalidReceipt;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use Ineersa\SqliteQueue\TransportException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\connect;

final class BrokerTest extends TestCase
{
    /** The broker's shared shutdown budget is five seconds; the floor proves a step actually blocked. */
    private const float SHUTDOWN_FLOOR_SECONDS = 4.0;
    /** Upper bound for a shutdown that must not wait on a fresh per-step clock. */
    private const int SHUTDOWN_BOUND_SECONDS = 10;
    private ?IsolatedDatabase $database = null;
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerFuture = null;
    private string $endpoint = '';
    /** @var list<Client> */
    private array $clients = [];
    /** @var list<Socket> */
    private array $peers = [];
    private int $now = 1_700_000_000_000;

    protected function setUp(): void
    {
        parent::setUp();
        $this->database = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        try {
            foreach ($this->clients as $client) {
                $client->close();
            }
            foreach ($this->peers as $peer) {
                $peer->close();
            }
            $this->clients = [];
            $this->peers = [];
            $this->broker?->stop();
            if (null !== $this->brokerFuture) {
                $this->assertSame(0, $this->brokerFuture->await(new TimeoutCancellation(10)));
            }
        } finally {
            $this->broker = null;
            $this->brokerFuture = null;
            $this->database?->remove();
        }

        parent::tearDown();
    }

    public function testConnectionLimitRejectsExtraClientAndKeepsAdmittedClientsWorking(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $admitted = [];
            for ($i = 0; $i < Broker::MAX_CONNECTIONS; ++$i) {
                $admitted[] = $this->connectClient();
            }
            $this->assertCount(Broker::MAX_CONNECTIONS, $admitted);

            try {
                Client::connect($this->endpoint, 5);
                $this->fail('A broker beyond its connection bound must refuse the handshake.');
            } catch (TransportException) {
            }

            $first = $this->clients[0];
            $id = $first->send('jobs', 'still accepted');
            $delivery = $first->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('still accepted', $delivery->body);
            $first->acknowledge($delivery->receipt);
            $this->assertNull($first->receive('jobs'));
        });
    }

    public function testSlowRawPeerDoesNotBlockStorageOrOtherClients(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $publisher = $this->connectClient();
            $publisher->send('jobs', str_repeat('p', Frame::MAX_PAYLOAD));

            $peer = $this->rawPeer();
            Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'hello']))->encode(), new TimeoutCancellation(5));
            $this->assertNotNull(Frame::read($peer, new TimeoutCancellation(5)));
            Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs']))->encode(), new TimeoutCancellation(5));

            $prefix = '';
            $deadline = new TimeoutCancellation(2);
            while (\strlen($prefix) < 4) {
                $chunk = $peer->read($deadline, 4 - \strlen($prefix));
                $this->assertNotNull($chunk, 'The broker must start writing the large reply.');
                $prefix .= $chunk;
            }
            $this->assertGreaterThan(Frame::MAX_PAYLOAD, (int) unpack('Nlength', $prefix)['length']);

            // Finish before the slow peer's five-second write deadline can free storage.
            $progress = new TimeoutCancellation(2);
            $other = $this->connectClient(2);
            $id = $other->send('jobs', 'unrelated', cancellation: $progress);
            $delivery = $other->receive('jobs', $progress);
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('unrelated', $delivery->body);
            $other->acknowledge($delivery->receipt, $progress);

            $peer->close();
        });
    }

    public function testVisibilityExpiryAndReceiptFencingWithControlledClock(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker(50, fn (): int => $this->now);
            $owner = $this->connectClient();
            $start = $this->now;
            $id = $owner->send('jobs', 'payload', delay: 100);
            $this->assertNull($owner->receive('jobs'), 'A delayed message must not be claimable before its deadline.');

            $this->now = $start + 25;
            $this->assertNull($owner->receive('jobs'), 'A restarted broker must still observe the persisted deadline.');
            $this->restartBroker(50, fn (): int => $this->now);
            $owner = $this->connectClient();

            $this->now = $start + 100;
            $delivery = $owner->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('payload', $delivery->body);
            $this->assertSame($start + 100, $delivery->availableAt);

            $other = $this->connectClient();
            try {
                $other->acknowledge($delivery->receipt);
                $this->fail('A receipt must not settle through a foreign client session.');
            } catch (InvalidReceipt) {
            }

            $owner->close();
            $this->now = $delivery->reservedUntil;
            $redelivered = $other->receive('jobs');
            $this->assertNotNull($redelivered);
            $this->assertSame('payload', $redelivered->body);
            $this->assertNotSame($delivery->receipt, $redelivered->receipt);

            try {
                $other->acknowledge($delivery->receipt);
                $this->fail('A superseded receipt must not settle the new reservation.');
            } catch (InvalidReceipt) {
            }
            $other->acknowledge($redelivered->receipt);
            $this->assertNull($other->receive('jobs'));
        });
    }

    public static function malformedTraffic(): iterable
    {
        yield 'version 2 request' => ['version 2 request', 'unsupported_protocol_version'];
        yield 'missing version' => ['missing version', 'unsupported_protocol_version'];
        yield 'out of sequence request id' => ['out of sequence request id', 'invalid_request'];
        yield 'non-integer request id' => ['non-integer request id', 'invalid_request'];
        yield 'oversized length prefix' => ['oversized length prefix', 'frame_too_large'];
        yield 'unknown operation' => ['unknown operation', 'invalid_request'];
        yield 'missing operation' => ['missing operation', 'invalid_request'];
        yield 'non-string operation' => ['non-string operation', 'invalid_request'];
        yield 'unsupported control field' => ['unsupported control field', 'invalid_request'];
        yield 'extra field on send' => ['extra field on send', 'invalid_request'];
        yield 'receive with body' => ['receive with body', 'invalid_request'];
        yield 'acknowledge with headers' => ['acknowledge with headers', 'invalid_request'];
        yield 'missing delay' => ['missing delay', 'invalid_request'];
        yield 'negative delay' => ['negative delay', 'invalid_request'];
        yield 'string delay' => ['string delay', 'invalid_request'];
        yield 'missing receipt' => ['missing receipt', 'invalid_request'];
        yield 'non-string receipt' => ['non-string receipt', 'invalid_request'];
    }

    #[DataProvider('malformedTraffic')]
    public function testMalformedTrafficFailsExplicitlyAndEndsOnlyThatSession(string $traffic, string $expectedCode): void
    {
        $this->runAsync(function () use ($traffic, $expectedCode): void {
            $this->startBroker();
            $healthy = $this->connectClient();
            $id = $healthy->send('jobs', 'before malformed peer');

            $peer = $this->rawPeer();
            Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'hello']))->encode(), new TimeoutCancellation(5));
            $this->assertNotNull(Frame::read($peer, new TimeoutCancellation(5)), 'The raw peer must complete its handshake.');

            $bytes = match ($traffic) {
                'version 2 request' => (new Frame(['v' => 2, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'missing version' => (new Frame(['id' => 1, 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'out of sequence request id' => (new Frame(['v' => Frame::VERSION, 'id' => 2, 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'non-integer request id' => (new Frame(['v' => Frame::VERSION, 'id' => '1', 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'oversized length prefix' => pack('N', Frame::MAX_FRAME + 1),
                'unknown operation' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'frobnicate']))->encode(),
                'missing operation' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'queue' => 'jobs']))->encode(),
                'non-string operation' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 5, 'queue' => 'jobs']))->encode(),
                'unsupported control field' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs', 'wait_ms' => 10]))->encode(),
                'extra field on send' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0, 'receipt' => 'x']))->encode(),
                'receive with body' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs'], 'body'))->encode(),
                'acknowledge with headers' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'acknowledge', 'receipt' => 'r'], '', 'h'))->encode(),
                'missing delay' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'send', 'queue' => 'jobs']))->encode(),
                'negative delay' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => -1]))->encode(),
                'string delay' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => '5']))->encode(),
                'missing receipt' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'acknowledge']))->encode(),
                'non-string receipt' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'reject', 'receipt' => 7]))->encode(),
            };
            Frame::write($peer, $bytes, new TimeoutCancellation(5));

            $reply = Frame::read($peer, new TimeoutCancellation(5));
            $this->assertNotNull($reply, 'The broker must report a bounded protocol error.');
            $this->assertSame(1, $reply->control['id'] ?? null);
            $this->assertFalse($reply->control['ok'] ?? true);
            $this->assertSame($expectedCode, $reply->control['error']['code'] ?? null);
            $this->assertNull(Frame::read($peer, new TimeoutCancellation(5)), 'The broker must end the malformed session.');

            $delivery = $healthy->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('before malformed peer', $delivery->body);
            $healthy->acknowledge($delivery->receipt);
        });
    }

    public function testInvalidQueueNameKeepsSessionAndAdvancesSequence(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $client = $this->connectClient();
            try {
                $client->send('', 'bad queue');
                $this->fail('An invalid queue name must be rejected.');
            } catch (\InvalidArgumentException) {
            }
            try {
                $client->receive('also bad queue!');
                $this->fail('An invalid queue name must be rejected.');
            } catch (\InvalidArgumentException) {
            }
            try {
                $client->receive('-jobs');
                $this->fail('An invalid queue name must be rejected.');
            } catch (\InvalidArgumentException) {
            }
            // All rejections advanced the sequence: the session still works.
            $id = $client->send('jobs', 'good queue');
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('good queue', $delivery->body);
            $client->acknowledge($delivery->receipt);
            $this->assertNull($client->receive('jobs'));
        });
    }

    /** @return iterable<string, array{string, int}> */
    public static function handshakeTraffic(): iterable
    {
        yield 'send before handshake' => ['send before handshake', 0];
        yield 'receive before handshake' => ['receive before handshake', 0];
        yield 'hello with body' => ['hello with body', 0];
        yield 'hello after handshake' => ['hello after handshake', 1];
    }

    #[DataProvider('handshakeTraffic')]
    public function testHandshakeOrderingFailsExplicitlyAndEndsOnlyThatSession(string $traffic, int $expectedId): void
    {
        $this->runAsync(function () use ($traffic, $expectedId): void {
            $this->startBroker();
            $healthy = $this->connectClient();
            $id = $healthy->send('jobs', 'before malformed peer');

            $peer = $this->rawPeer();
            if ('hello after handshake' === $traffic) {
                Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'hello']))->encode(), new TimeoutCancellation(5));
                $this->assertNotNull(Frame::read($peer, new TimeoutCancellation(5)), 'The raw peer must complete its handshake.');
            }
            $bytes = match ($traffic) {
                'send before handshake' => (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0]))->encode(),
                'receive before handshake' => (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'hello with body' => (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'hello'], 'body'))->encode(),
                'hello after handshake' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'hello']))->encode(),
            };
            Frame::write($peer, $bytes, new TimeoutCancellation(5));

            $reply = Frame::read($peer, new TimeoutCancellation(5));
            $this->assertNotNull($reply, 'The broker must report a bounded protocol error.');
            $this->assertSame($expectedId, $reply->control['id'] ?? null);
            $this->assertFalse($reply->control['ok'] ?? true);
            $this->assertSame('invalid_request', $reply->control['error']['code'] ?? null);
            $this->assertNull(Frame::read($peer, new TimeoutCancellation(5)), 'The broker must end the malformed session.');

            $delivery = $healthy->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('before malformed peer', $delivery->body);
            $healthy->acknowledge($delivery->receipt);
        });
    }

    public function testReadinessCallbackFailureReleasesOwnedTreeAndPreservesDatabase(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        $this->runAsync(function (): void {
            $database = $this->database ?? throw new \LogicException('Missing test database.');
            $this->endpoint = $database->path('queue.sock');
            $during = null;
            $failure = new \RuntimeException('Readiness callback failure sentinel.');
            $broker = (new BrokerFactory($database->path(), $this->endpoint, 5000, fn (): int => $this->now))->listen();
            $future = async(static function () use ($broker, &$during, $failure): int {
                return $broker->run(static function (array $event) use (&$during, $failure): void {
                    $during = ProcessTree::ownedBy((int) getmypid());
                    throw $failure;
                });
            });
            try {
                $future->await(new TimeoutCancellation(15));
                $this->fail('A failing readiness callback must fail broker startup.');
            } catch (\RuntimeException $error) {
                $this->assertSame($failure, $error);
            }
            $this->assertNotNull($during);
            $this->assertNotSame([], $during['workers'], 'The failing broker must have owned its persistence worker.');
            $this->assertNotSame([], $during['launchers'], 'The failing broker must have owned its worker launcher.');
            $after = array_keys(ProcessTree::snapshot());
            $this->assertSame([], array_values(array_intersect($during['workers'], $after)), 'Failed readiness must not leave a persistence worker.');
            $this->assertSame([], array_values(array_intersect($during['launchers'], $after)), 'Failed readiness must not leave a worker launcher.');
            $this->assertFalse(file_exists($this->endpoint), 'Failed readiness must release the socket path.');
            $this->assertFileExists($database->path());

            $this->startBroker(5000, fn (): int => $this->now);
            $client = $this->connectClient();
            $id = $client->send('jobs', 'preserved after failed readiness');
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('preserved after failed readiness', $delivery->body);
            $client->acknowledge($delivery->receipt);
        });
    }

    /**
     * A stopped launcher keeps the persistence pipes and the exit-code pipe open, so the driver
     * close blocks in its join and force-stop cannot release it. Only a shared shutdown budget
     * can end that shutdown.
     */
    public function testShutdownBudgetEndsACloseBlockedByAStoppedLauncher(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        $this->runAsync(function (): void {
            $database = $this->database ?? throw new \LogicException('Missing test database.');
            $this->endpoint = $database->path('queue.sock');
            $broker = (new BrokerFactory($database->path(), $this->endpoint, 5000, fn (): int => $this->now))->listen();
            $ready = new DeferredFuture();
            $brokerFuture = async(static function () use ($broker, $ready): int {
                return $broker->run(static function (array $event) use ($ready): void {
                    $ready->complete($event);
                });
            });
            $ready->getFuture()->await(new TimeoutCancellation(15));

            $client = $this->connectClient();
            $confirmed = $client->send('jobs', 'confirmed before the stopped launcher');
            $client->close();

            $owned = ProcessTree::ownedBy((int) getmypid());
            $this->assertCount(1, $owned['launchers'], 'The broker must own exactly one worker launcher.');
            $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');
            $launcher = $owned['launchers'][0];
            $worker = $owned['workers'][0];
            $this->assertNotSame(0, posix_geteuid());
            $this->assertSame(posix_geteuid(), fileowner('/proc/'.$launcher), 'The stopped process must be this user\'s worker launcher.');

            try {
                $this->assertTrue(posix_kill($launcher, \SIGSTOP), 'The test must stop only the worker launcher.');
                $this->assertSame('T', $this->waitForState($launcher, 'T'), 'A stopped launcher must be observable before the broker stops.');

                // Isolate the shutdown budget from signal delivery.
                $started = microtime(true);
                $broker->stop();
                $outcome = $brokerFuture->catch(static fn (\Throwable $error): string => $error::class);
                try {
                    $settled = $outcome->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS));
                } catch (CancelledException) {
                    $settled = null;
                }
                $elapsed = microtime(true) - $started;

                $this->assertNotNull($settled, 'Shutdown must settle within the shared budget while the launcher holds the pipes open.');
                $this->assertGreaterThan(self::SHUTDOWN_FLOOR_SECONDS, $elapsed, 'The driver close must have blocked until the shared budget expired.');
                $this->assertLessThan(self::SHUTDOWN_BOUND_SECONDS, $elapsed, 'Shutdown must not wait on a fresh per-step clock.');
                $this->assertSame(CancelledException::class, $settled, 'The shared budget must cancel the blocked shutdown.');
                $this->assertFileDoesNotExist($this->endpoint, 'Shutdown must release the endpoint while the launcher is stopped.');
                $this->assertFileExists($database->path(), 'Shutdown must preserve confirmed data.');
                $this->assertSame([], ProcessTree::ownedBy((int) getmypid())['workers'], 'The force-stopped worker must not survive the shutdown.');
                $this->assertSame('T', $this->processState($launcher), 'The broker does not own the launcher, so this test must reap it.');
            } finally {
                // A stopped launcher is outside the broker's ownership and would otherwise
                // survive the test run.
                @posix_kill($launcher, \SIGKILL);
                @posix_kill($worker, \SIGKILL);
                $broker->stop();
                $brokerFuture->ignore();
            }

            $this->startBroker(5000, fn (): int => $this->now);
            $client = $this->connectClient();
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery, 'A confirmation must survive a budgeted shutdown.');
            $this->assertSame($confirmed, $delivery->id);
            $this->assertSame('confirmed before the stopped launcher', $delivery->body);
            $client->acknowledge($delivery->receipt);
            $client->close();
        });
    }

    /**
     * The deadline must exist as soon as shutdown is requested, not once cleanup happens to reach
     * its finally: a serving loop that never resumes would otherwise have no armed deadline at all.
     */
    public function testFirstStopRequestArmsTheShutdownDeadlineBeforeCleanup(): void
    {
        $this->runAsync(function (): void {
            $events = [];
            $this->startBroker(5000, null, static function (array $event) use (&$events): void {
                $events[] = $event;
            });
            $this->assertSame([], $events, 'A serving broker must not hold a shutdown deadline.');

            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');
            $broker->stop();

            // stop() is synchronous and the serving loop is suspended in accept(), so both events
            // can only come from stop() itself: the deadline is armed before any cleanup step runs.
            $this->assertSame(['shutdown-requested', 'deadline-armed'], array_column($events, 'event'));
            $this->assertFalse($future->isComplete(), 'The serving loop must still be suspended when the deadline is armed.');
            $this->assertSame((int) getmypid(), $events[0]['pid']);
            $this->assertLessThanOrEqual($events[1]['monotonic_ns'], $events[0]['monotonic_ns']);

            $this->assertSame(0, $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS)));
            $this->assertSame(['shutdown-requested', 'deadline-armed'], array_column($events, 'event'), 'A shutdown inside the budget must disarm the deadline before it fires.');
            $this->assertFileDoesNotExist($this->endpoint);
        });
    }

    /** A repeated stop request, including the serving loop's own, shares the one deadline. */
    public function testRepeatedStopRequestsDoNotRearmTheShutdownDeadline(): void
    {
        $this->runAsync(function (): void {
            $events = [];
            $this->startBroker(5000, null, static function (array $event) use (&$events): void {
                $events[] = $event;
            });
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');

            $broker->stop();
            $broker->stop();
            $broker->stop();

            $this->assertSame(['shutdown-requested', 'deadline-armed'], array_column($events, 'event'));
            $this->assertSame(0, $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS)));
            // The finally calls stop() again: still one request and one arm.
            $this->assertSame(['shutdown-requested', 'deadline-armed'], array_column($events, 'event'));
        });
    }

    /**
     * A force-stop failure during escalation must not escape into the event loop, and it must not
     * leave the budget unreleased either: it becomes the cancellation cause. Persistence is final,
     * so the escalation helper is driven directly with a throwing step.
     */
    public function testDeadlineEscalationReleasesTheBudgetWhenTheForceStopFails(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $deadline = new DeferredCancellation();
            $failure = new \RuntimeException('Force-stop sentinel.');

            (new \ReflectionMethod(Broker::class, 'releaseBudgetAfter'))->invoke($broker, $deadline, static function () use ($failure): void {
                throw $failure;
            });

            $cancelled = null;
            try {
                $deadline->getCancellation()->throwIfRequested();
            } catch (CancelledException $error) {
                $cancelled = $error;
            }
            $this->assertInstanceOf(CancelledException::class, $cancelled, 'A failed force-stop must still release the budget.');
            $this->assertSame($failure, $cancelled->getPrevious(), 'The escalation failure must become the cancellation cause.');
        });
    }

    /** The trace is an observation: a failing observer must not arm, release, or skip cleanup. */
    public function testFailingDiagnosticObserverDoesNotHoldUpShutdown(): void
    {
        $this->runAsync(function (): void {
            $calls = 0;
            $this->startBroker(5000, null, static function (array $event) use (&$calls): void {
                ++$calls;
                throw new \RuntimeException('Observer sentinel: '.$event['event']);
            });
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');
            $broker->stop();

            $this->assertSame(2, $calls, 'A failing observer must still be notified for both shutdown milestones.');
            $this->assertSame(0, $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS)));
            $this->assertFileDoesNotExist($this->endpoint);
        });
    }

    private function connectClient(float $timeout = 10): Client
    {
        $client = Client::connect($this->endpoint, $timeout);
        $this->clients[] = $client;

        return $client;
    }

    private function restartBroker(int $visibilityTimeout = 5000, ?\Closure $clock = null): void
    {
        $this->broker?->stop();
        if (null !== $this->brokerFuture) {
            $this->assertSame(0, $this->brokerFuture->await(new TimeoutCancellation(10)));
        }
        $this->broker = null;
        $this->brokerFuture = null;
        $this->startBroker($visibilityTimeout, $clock);
    }

    private function rawPeer(): Socket
    {
        $peer = connect('unix://'.$this->endpoint, cancellation: new TimeoutCancellation(5));
        $this->peers[] = $peer;

        return $peer;
    }

    /** Bounded wait for a kernel-reported process state; stops and signal delivery are not ordered. */
    private function waitForState(int $pid, string $expected): string
    {
        $deadline = microtime(true) + 5;
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

    private function runAsync(\Closure $operation): void
    {
        async($operation)->await();
    }

    private function startBroker(int $visibilityTimeout = 5000, ?\Closure $clock = null, ?\Closure $diagnostic = null): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $this->broker = (new BrokerFactory($database->path(), $this->endpoint, $visibilityTimeout, $clock))->listen();
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }, null, $diagnostic));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }
}
