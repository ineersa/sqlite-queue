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
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Tests\Broker\Fixtures\GatingServerSocket;
use Ineersa\SqliteQueue\Tests\Broker\Fixtures\WriteGate;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\Socket\connect;

final class BrokerTest extends TestCase
{
    /** Harness safety timeout, not a correctness threshold. */
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
            $gate = new WriteGate();
            // Gate every accepted socket. Only oversized response frames enter the barrier;
            // unrelated clients keep using small confirmations that stay ungated.
            $this->startBrokerWithGatedServer($gate, Frame::MAX_PAYLOAD);
            $publisher = $this->connectClient();
            $publisher->send('jobs', str_repeat('p', Frame::MAX_PAYLOAD));

            $peer = $this->rawPeer();
            try {
                Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 0, 'op' => 'hello']))->encode(), new TimeoutCancellation(5));
                $this->assertNotNull(Frame::read($peer, new TimeoutCancellation(5)));
                Frame::write($peer, (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs']))->encode(), new TimeoutCancellation(5));

                $bytes = $gate->entered()->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS));
                $this->assertGreaterThan(Frame::MAX_PAYLOAD, $bytes, 'The gated write must be the oversized receive reply.');
                $this->assertFalse($gate->isReleased(), 'Production write timeout must not release the test gate.');

                $other = $this->connectClient();
                $id = $other->send('jobs', 'unrelated');
                $delivery = $other->receive('jobs');
                $this->assertNotNull($delivery);
                $this->assertSame($id, $delivery->id);
                $this->assertSame('unrelated', $delivery->body);
                $other->acknowledge($delivery->receipt);
                $this->assertFalse($gate->isReleased(), 'Unrelated clients must finish while the gated reply is still pending.');
            } finally {
                $gate->release();
                $peer->close();
            }
        });
    }

    public function testGatedWriterNeedsExplicitReleaseAfterSocketClose(): void
    {
        $this->runAsync(function (): void {
            [$inner, $peer] = \Amp\Socket\createSocketPair();
            $gate = new WriteGate();
            $socket = new Fixtures\GatingSocket($inner, $gate, 0);
            $writing = async(static fn () => $socket->write('blocked'));
            try {
                $this->assertSame(7, $gate->entered()->await(new TimeoutCancellation(10)));
                $socket->close(); // The same action taken by production write cancellation.
                $this->assertFalse($gate->isReleased());
                $this->assertFalse($writing->isComplete());
                $gate->release();
                try {
                    $writing->await(new TimeoutCancellation(10));
                    $this->fail('A released write must observe the closed socket.');
                } catch (\Amp\ByteStream\ClosedException $error) {
                    $this->assertSame('Gated write released after the socket closed.', $error->getMessage());
                }
            } finally {
                $gate->release();
                $writing->ignore();
                $socket->close();
                $peer->close();
            }
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
            } catch (InvalidReceiptException) {
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
            } catch (InvalidReceiptException) {
            }
            $other->acknowledge($redelivered->receipt);
            $this->assertNull($other->receive('jobs'));
        });
    }

    #[DataProviderExternal(ClientTest::class, 'receiptRejections')]
    public function testSpecificReceiptRejectionsPreserveTheSession(ErrorCode $code, InvalidReceiptException $expected): void
    {
        $this->runAsync(function () use ($code, $expected): void {
            $this->startBroker(50, fn (): int => $this->now);
            $owner = $this->connectClient();
            $owner->send('jobs', 'reserved');
            $delivery = $owner->receive('jobs');
            $this->assertNotNull($delivery);
            $actor = $owner;
            $receipt = $delivery->receipt;
            switch ($code) {
                case ErrorCode::MalformedReceipt:
                    $receipt = 'malformed';
                    break;
                case ErrorCode::NoActiveReservation:
                    $owner->acknowledge($receipt);
                    break;
                case ErrorCode::ReceiptOwnerMismatch:
                    $actor = $this->connectClient();
                    break;
                case ErrorCode::ReceiptEpochMismatch:
                    // Change only the persisted epoch so the current connection still owns the row.
                    $database = new \SQLite3($this->database->path());
                    try {
                        $this->assertTrue($database->exec("UPDATE queue_messages SET broker_epoch = 'previous-epoch'"));
                    } finally {
                        $database->close();
                    }
                    break;
                case ErrorCode::ReceiptTokenMismatch:
                    [$id, $token] = explode(':', $receipt);
                    $receipt = $id.':'.('0' === $token[0] ? '1' : '0').substr($token, 1);
                    break;
                case ErrorCode::ExpiredReceipt:
                    $this->now = $delivery->reservedUntil;
                    break;
                default:
                    $this->fail('Expected a receipt error code.');
            }
            foreach ([$actor->acknowledge(...), $actor->reject(...)] as $settle) {
                try {
                    $settle($receipt);
                    $this->fail('The broker accepted an invalid receipt.');
                } catch (InvalidReceiptException $error) {
                    $this->assertSame($expected::class, $error::class);
                    $this->assertSame($expected->getMessage(), $error->getMessage());
                }
            }
            $id = $actor->send('other', 'still usable');
            $next = $actor->receive('other');
            $this->assertNotNull($next);
            $this->assertSame($id, $next->id);
            $actor->acknowledge($next->receipt);
        });
    }

    public function testShutdownDuringClaimDoesNotTreatContextCancellationAsStorageFailure(): void
    {
        $this->runAsync(function (): void {
            $entered = new DeferredFuture();
            $release = new DeferredFuture();
            $armed = false;
            $this->startBroker(50, function () use ($entered, $release, &$armed): int {
                if ($armed) {
                    $armed = false;
                    $entered->complete();
                    $release->getFuture()->await(new TimeoutCancellation(10));
                }

                return $this->now;
            });
            $client = $this->connectClient();
            $client->send('jobs', 'reserved');
            $armed = true;
            $receiving = async(static fn () => $client->receive('jobs'));
            try {
                $entered->getFuture()->await(new TimeoutCancellation(10));
                $this->broker->stop();
            } finally {
                $release->complete();
            }
            try {
                $receiving->await(new TimeoutCancellation(10));
                $this->fail('Shutdown must invalidate the in-flight client exchange.');
            } catch (TransportException) {
                $this->assertSame(0, $this->brokerFuture->await(new TimeoutCancellation(10)));
            }
            $database = new \SQLite3($this->database->path());
            try {
                $this->assertSame($this->now + 50, $database->querySingle('SELECT reserved_until FROM queue_messages'));
            } finally {
                $database->close();
            }
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
            $broker = (new BrokerFactory($database->path(), $this->endpoint, 5000, fn (): int => $this->now))->create();
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
            $broker = (new BrokerFactory($database->path(), $this->endpoint, 5000, fn (): int => $this->now))->create();
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

                // Control expiry explicitly. Wall-clock elapsed time is not the proof.
                $broker->stop();
                $timer = $this->shutdownTimerId($broker);
                $this->assertNotNull($timer);
                EventLoop::cancel($timer);
                $storage = (new \ReflectionProperty($broker, 'storage'))->getValue($broker);
                $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
                $safety = new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS);
                while (!$connection->isClosed()) {
                    \Amp\delay(0, cancellation: $safety);
                }
                $this->assertFalse($brokerFuture->isComplete(), 'Driver close must still await the stopped launcher.');
                $deadline = (new \ReflectionProperty($broker, 'deadline'))->getValue($broker);
                $handle = (new \ReflectionProperty($broker, 'worker'))->getValue($broker);
                (new \ReflectionMethod($broker, 'releaseBudgetAfter'))->invoke($broker, $deadline, $handle->forceStop(...));
                $outcome = $brokerFuture->catch(static fn (\Throwable $error): string => $error::class);
                $settled = $outcome->await($safety);

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
            $this->startBroker();
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');
            $this->assertNull($this->shutdownTimerId($broker));
            $broker->stop();

            $this->assertNotNull($this->shutdownTimerId($broker), 'The first stop request must arm the deadline before cleanup.');
            $this->assertFalse($future->isComplete(), 'The serving loop must still be suspended when the deadline is armed.');

            $this->assertSame(0, $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS)));
            $this->assertNull($this->shutdownTimerId($broker), 'Completed cleanup must disarm the deadline.');
            $this->assertFileDoesNotExist($this->endpoint);
        });
    }

    /** A repeated stop request, including the serving loop's own, shares the one deadline. */
    public function testRepeatedStopRequestsDoNotRearmTheShutdownDeadline(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');

            $broker->stop();
            $timer = $this->shutdownTimerId($broker);
            $this->assertNotNull($timer);
            $broker->stop();
            $broker->stop();

            $this->assertSame($timer, $this->shutdownTimerId($broker));
            $this->assertSame(0, $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS)));
            $this->assertNull($this->shutdownTimerId($broker));
        });
    }

    /**
     * A force-stop failure during escalation must not escape into the event loop, and it must not
     * leave the budget unreleased either: it becomes the cancellation cause. SqliteWorkerHandle is final,
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

    /**
     * Storage failure must stop the service under the shared budget instead of spending a separate
     * five-second write attempting to deliver a storage-failure error reply.
     */
    public function testStorageFailureStopsWithoutAPreBudgetErrorWrite(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $broker = $this->broker ?? throw new \LogicException('Missing test broker.');
            $future = $this->brokerFuture ?? throw new \LogicException('Missing broker future.');
            $peer = $this->rawPeer();
            $peer->write((new Frame(['v' => 1, 'id' => 0, 'op' => 'hello']))->encode());
            $this->assertNotNull(Frame::read($peer, new TimeoutCancellation(10)));
            $storage = (new \ReflectionProperty(Broker::class, 'storage'))->getValue($broker);
            $this->assertInstanceOf(\Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage::class, $storage);
            // Fail the next storage operation without racing the independent worker-death monitor.
            (new \ReflectionProperty($storage, 'closed'))->setValue($storage, true);

            $peer->write((new Frame(['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0], 'must fail'))->encode());
            $this->assertNull(Frame::read($peer, new TimeoutCancellation(10)), 'Fatal storage failure must close without writing an error frame.');
            $exit = $future->await(new TimeoutCancellation(self::SHUTDOWN_BOUND_SECONDS));

            $this->assertSame(1, $exit, 'Storage failure must fail the broker process.');
            $this->assertFileDoesNotExist($this->endpoint, 'Storage failure must release the endpoint.');
            // tearDown asserts a clean exit; this case already consumed the failed future.
            $this->broker = null;
            $this->brokerFuture = null;
        });
    }

    private function shutdownTimerId(Broker $broker): ?string
    {
        $timer = (new \ReflectionProperty(Broker::class, 'shutdownTimer'))->getValue($broker);
        $this->assertTrue(null === $timer || \is_string($timer));

        return $timer;
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

    private function startBroker(int $visibilityTimeout = 5000, ?\Closure $clock = null): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $this->broker = (new BrokerFactory($database->path(), $this->endpoint, $visibilityTimeout, $clock))->create();
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }

    /**
     * Rebuilds a factory-created broker with a gating server so one oversized response write
     * stays pending until WriteGate::release(). No production hooks.
     */
    private function startBrokerWithGatedServer(WriteGate $gate, int $thresholdBytes): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $original = (new BrokerFactory($database->path(), $this->endpoint, clock: fn (): int => $this->now))->create();
        $constructor = (new \ReflectionClass(Broker::class))->getConstructor() ?? throw new \LogicException('Missing Broker constructor.');
        $arguments = [];
        foreach ($constructor->getParameters() as $parameter) {
            $arguments[] = (new \ReflectionProperty(Broker::class, $parameter->getName()))->getValue($original);
        }
        $arguments[0] = new GatingServerSocket($arguments[0], $gate, $thresholdBytes);
        $this->broker = (new \ReflectionClass(Broker::class))->newInstanceArgs($arguments);
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }
}
