<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

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
        yield 'out of sequence request id' => ['out of sequence request id', 'invalid_request'];
        yield 'oversized length prefix' => ['oversized length prefix', 'frame_too_large'];
        yield 'unknown operation' => ['unknown operation', 'invalid_request'];
        yield 'unsupported control field' => ['unsupported control field', 'invalid_request'];
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
                'out of sequence request id' => (new Frame(['v' => Frame::VERSION, 'id' => 2, 'op' => 'receive', 'queue' => 'jobs']))->encode(),
                'oversized length prefix' => pack('N', Frame::MAX_FRAME + 1),
                'unknown operation' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'frobnicate']))->encode(),
                'unsupported control field' => (new Frame(['v' => Frame::VERSION, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs', 'wait_ms' => 10]))->encode(),
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
            // Both rejections advanced the sequence: the session still works.
            $id = $client->send('jobs', 'good queue');
            $delivery = $client->receive('jobs');
            $this->assertNotNull($delivery);
            $this->assertSame($id, $delivery->id);
            $this->assertSame('good queue', $delivery->body);
            $client->acknowledge($delivery->receipt);
            $this->assertNull($client->receive('jobs'));
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

    private function runAsync(\Closure $operation): void
    {
        async($operation)->await();
    }

    private function startBroker(int $visibilityTimeout = 5000, ?\Closure $clock = null): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $this->broker = (new BrokerFactory($database->path(), $this->endpoint, $visibilityTimeout, $clock))->listen();
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }
}
