<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException as ClientTransportException;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Messenger\Stamp\DeliveryReceiptStamp;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Tests\Support\ControlledWorkerClock;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

use function Amp\async;
use function Amp\Socket\listen;

final class TransportTest extends TestCase
{
    use ControlledWorkerClock;

    private const int PROBE_SAFETY_SECONDS = 5;

    private ?IsolatedDatabase $database = null;
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerFuture = null;
    private string $endpoint = '';
    /** @var list<Client> */
    private array $clients = [];
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
            $this->clients = [];
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

    public function testBodyUtf8HeadersDelayAndIdReplacementRoundTrip(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $serializer = new class implements SerializerInterface {
                public function decode(array $encodedEnvelope): Envelope
                {
                    $body = $encodedEnvelope['body'] ?? null;
                    $headers = $encodedEnvelope['headers'] ?? null;
                    if (!\is_string($body)) {
                        throw new MessageDecodingFailedException('missing body');
                    }
                    if (!\is_array($headers)) {
                        throw new MessageDecodingFailedException('missing headers');
                    }
                    foreach ($headers as $name => $value) {
                        if (!\is_string($name) || !\is_string($value)) {
                            throw new MessageDecodingFailedException('non-string header');
                        }
                    }

                    return new Envelope(new TransportProbeMessage($body), [
                        new TransportHeaderStamp($headers),
                    ]);
                }

                public function encode(Envelope $envelope): array
                {
                    $message = $envelope->getMessage();
                    if (!$message instanceof TransportProbeMessage) {
                        throw new \InvalidArgumentException('Unexpected message type.');
                    }

                    return [
                        'body' => $message->body,
                        'headers' => [
                            'type' => TransportProbeMessage::class,
                            // JSON preserves U+0000 inside a UTF-8 string value.
                            'meta' => "bin\0data",
                        ],
                    ];
                }
            };
            $transport = $this->transportWithSerializer($serializer);
            $message = new TransportProbeMessage("payload\0binary");
            $envelope = new Envelope($message, [
                new DelayStamp(250),
                new TransportMessageIdStamp('stale-id'),
            ]);

            $sent = $transport->send($envelope);
            $idStamp = $sent->last(TransportMessageIdStamp::class);
            $this->assertInstanceOf(TransportMessageIdStamp::class, $idStamp);
            $this->assertNotSame('stale-id', $idStamp->getId());
            $this->assertIsInt($idStamp->getId());
            $this->assertNull($sent->last(DeliveryReceiptStamp::class));
            $this->assertSame([], iterator_to_array($transport->get()));

            $this->setNow($this->now + 249);
            $this->assertSame([], iterator_to_array($transport->get()), 'Subsecond delay must not truncate to an earlier second.');
            $this->setNow($this->now + 1);
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $delivery = $received[0];
            $this->assertInstanceOf(TransportProbeMessage::class, $delivery->getMessage());
            $this->assertSame("payload\0binary", $delivery->getMessage()->body);
            $headerStamp = $delivery->last(TransportHeaderStamp::class);
            $this->assertInstanceOf(TransportHeaderStamp::class, $headerStamp);
            $this->assertSame("bin\0data", $headerStamp->headers['meta']);
            $this->assertSame($idStamp->getId(), $delivery->last(TransportMessageIdStamp::class)?->getId());
            $receivedStamp = $delivery->last(DeliveryReceiptStamp::class);
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $receivedStamp);
            $this->assertSame($transport, $receivedStamp->transport);

            $transport->ack($delivery);
            $this->assertSame([], iterator_to_array($transport->get()));
        });
    }

    public function testSendStripsDeliveryStampsBeforeSerializerEncode(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $seenReceived = false;
            $seenOldId = false;
            $serializer = new class($seenReceived, $seenOldId) implements SerializerInterface {
                public function __construct(
                    private bool &$seenReceived,
                    private bool &$seenOldId,
                ) {
                }

                public function decode(array $encodedEnvelope): Envelope
                {
                    return new Envelope(new TransportProbeMessage($encodedEnvelope['body']));
                }

                public function encode(Envelope $envelope): array
                {
                    $this->seenReceived = null !== $envelope->last(DeliveryReceiptStamp::class);
                    $id = $envelope->last(TransportMessageIdStamp::class);
                    $this->seenOldId = $id instanceof TransportMessageIdStamp && 'old' === $id->getId();

                    return ['body' => 'x', 'headers' => ['type' => 'probe']];
                }
            };
            $transport = $this->transportWithSerializer($serializer);
            $transport->send(new Envelope(new TransportProbeMessage('seed')));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $withStamps = $received[0]->with(new TransportMessageIdStamp('old'));
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $withStamps->last(DeliveryReceiptStamp::class));

            $transport->send($withStamps);
            $this->assertFalse($seenReceived);
            $this->assertFalse($seenOldId);
            $transport->ack($received[0]);
            $queued = iterator_to_array($transport->get());
            $this->assertCount(1, $queued);
            $transport->ack($queued[0]);
        });
    }

    public function testExplicitNullHeadersAreRejected(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $serializer = new class implements SerializerInterface {
                public function decode(array $encodedEnvelope): Envelope
                {
                    throw new \LogicException('decode is unused.');
                }

                public function encode(Envelope $envelope): array
                {
                    return ['body' => 'x', 'headers' => null];
                }
            };
            $transport = $this->transportWithSerializer($serializer);
            try {
                $transport->send(new Envelope(new TransportProbeMessage('x')));
                $this->fail('Explicit null headers must be rejected.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('must not be null', $error->getMessage());
            }
        });
    }

    public function testAbsentHeadersDefaultToEmptyObject(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $serializer = new class implements SerializerInterface {
                public function decode(array $encodedEnvelope): Envelope
                {
                    $headers = $encodedEnvelope['headers'] ?? null;
                    if (!\is_array($headers) || [] !== $headers) {
                        throw new MessageDecodingFailedException('expected empty headers object');
                    }

                    return new Envelope(new TransportProbeMessage($encodedEnvelope['body']));
                }

                public function encode(Envelope $envelope): array
                {
                    return ['body' => 'no-headers'];
                }
            };
            $transport = $this->transportWithSerializer($serializer);
            $transport->send(new Envelope(new TransportProbeMessage('ignored')));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $transport->ack($received[0]);
        });
    }

    public function testInvalidUtf8HeadersAreRejectedBeforeSend(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $serializer = new class implements SerializerInterface {
                public function decode(array $encodedEnvelope): Envelope
                {
                    throw new \LogicException('decode is unused.');
                }

                public function encode(Envelope $envelope): array
                {
                    return ['body' => 'x', 'headers' => ['bad' => "\xFF"]];
                }
            };
            $transport = $this->transportWithSerializer($serializer);
            try {
                $transport->send(new Envelope(new TransportProbeMessage('x')));
                $this->fail('Invalid UTF-8 headers must be rejected.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('UTF-8', $error->getMessage());
            }
        });
    }

    public function testRejectSettlesTheCommittedReceipt(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $transport = $this->transport();
            $transport->send(new Envelope(new TransportProbeMessage('reject-me')));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $transport->reject($received[0]);
            $this->assertSame([], iterator_to_array($transport->get()));
        });
    }

    public function testForeignTransportReceiptIsRejectedLocally(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $first = $this->transport();
            $second = $this->transport();
            $first->send(new Envelope(new TransportProbeMessage('shared-client')));
            $received = iterator_to_array($first->get());
            $this->assertCount(1, $received);

            try {
                $second->ack($received[0]);
                $this->fail('A foreign transport instance must not settle another claim.');
            } catch (LogicException $error) {
                $this->assertStringContainsString('not received by this transport instance', $error->getMessage());
            }

            $first->ack($received[0]);
        });
    }

    public function testStaleReceiptFailsAsTransportException(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker(visibilityTimeout: 50);
            $transport = $this->transport();
            $transport->send(new Envelope(new TransportProbeMessage('expires')));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);

            $this->setNow($this->now + 50);
            $other = $this->transport();
            $redelivery = iterator_to_array($other->get());
            $this->assertCount(1, $redelivery);
            $other->ack($redelivery[0]);

            try {
                $transport->ack($received[0]);
                $this->fail('A superseded receipt must not acknowledge.');
            } catch (TransportException $error) {
                $this->assertInstanceOf(\Ineersa\SqliteQueue\Exception\InvalidReceiptException::class, $error->getPrevious());
            }
        });
    }

    public function testDecodeFailureUsesSupportedCleanupPathWithoutCallingWrap(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            // Throw instead of MessageDecodingFailedException::wrap() so this stays runnable on 8.0.
            $broken = new class implements SerializerInterface {
                public function decode(array $encodedEnvelope): Envelope
                {
                    throw new MessageDecodingFailedException('probe decode failure');
                }

                public function encode(Envelope $envelope): array
                {
                    return ['body' => 'x', 'headers' => ['type' => 'probe']];
                }
            };
            $transport = $this->transportWithSerializer($broken);
            $transport->send(new Envelope(new TransportProbeMessage('ignored')));

            try {
                $received = iterator_to_array($transport->get());
            } catch (MessageDecodingFailedException $error) {
                $this->assertStringContainsString('probe decode failure', $error->getMessage());
                $this->assertSame([], iterator_to_array($this->transport()->get()), '8.0 cleanup must reject the claim.');

                return;
            }

            $this->assertCount(1, $received);
            $envelope = $received[0];
            $this->assertInstanceOf(MessageDecodingFailedException::class, $envelope->getMessage());
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $envelope->last(DeliveryReceiptStamp::class));
            $this->assertInstanceOf(TransportMessageIdStamp::class, $envelope->last(TransportMessageIdStamp::class));
            $transport->reject($envelope);
            $this->assertSame([], iterator_to_array($this->transport()->get()));
        });
    }

    public function testMalformedStoredHeadersPreserveOpaqueBytes(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $client = $this->connectClient();
            $client->send('jobs', 'raw-body', 'not-json');
            $transport = $this->transport();

            try {
                $received = iterator_to_array($transport->get());
            } catch (MessageDecodingFailedException $error) {
                $this->assertStringContainsString('not valid JSON', $error->getMessage());
                $this->assertSame([], iterator_to_array($this->transport()->get()));

                return;
            }

            $this->assertCount(1, $received);
            $failure = $received[0]->getMessage();
            $this->assertInstanceOf(MessageDecodingFailedException::class, $failure);
            if (property_exists($failure, 'encodedEnvelope')) {
                $this->assertSame('not-json', $failure->encodedEnvelope['headers'][Transport::RAW_HEADERS_HEADER] ?? null);
            }
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $received[0]->last(DeliveryReceiptStamp::class));
            $transport->reject($received[0]);
        });
    }

    public function testPeerFailureDoesNotReplaySend(): void
    {
        $this->runAsync(function (): void {
            $directory = sys_get_temp_dir().'/sq-messenger-'.bin2hex(random_bytes(6));
            $this->assertTrue(mkdir($directory, 0700));
            $endpoint = $directory.'/broker.sock';
            $received = 0;
            $server = listen('unix://'.$endpoint);
            $peer = async(static function () use ($server, &$received): void {
                $socket = $server->accept();
                if (null === $socket) {
                    return;
                }
                Frame::read($socket, new TimeoutCancellation(3));
                ++$received;
                $socket->write((new Frame([
                    'v' => Frame::VERSION,
                    'id' => 0,
                    'ok' => true,
                    'result' => ['max_payload' => Frame::MAX_PAYLOAD],
                ]))->encode());
                Frame::read($socket, new TimeoutCancellation(3));
                ++$received;
            });

            try {
                $transport = new Transport($this->connection($endpoint), new QueueName('jobs'), new PhpSerializer(), $this->connection($endpoint));
                try {
                    $transport->send(new Envelope(new TransportProbeMessage('lost')));
                    $this->fail('A peer close without confirmation must fail the send.');
                } catch (TransportException $error) {
                    $this->assertInstanceOf(ClientTransportException::class, $error->getPrevious());
                }
                $peer->await(new TimeoutCancellation(3));
                $this->assertSame(2, $received);
                try {
                    $transport->send(new Envelope(new TransportProbeMessage('again')));
                    $this->fail('A failed transport must not reconnect and replay.');
                } catch (TransportException) {
                    $this->addToAssertionCount(1);
                }
                $this->assertSame(2, $received);
            } finally {
                $server->close();
                @unlink($endpoint);
                @rmdir($directory);
            }
        });
    }

    public function testWaitDelegatesToTheConfiguredQueue(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $transport = $this->transport();
            $waiting = async(static fn (): bool => $transport->wait(5_000, new TimeoutCancellation(self::PROBE_SAFETY_SECONDS)));
            $this->awaitNotifierWaiters(1);
            $publisher = $this->transport();
            $publisher->send(new Envelope(new TransportProbeMessage('wake')));
            $this->assertTrue($waiting->await(new TimeoutCancellation(self::PROBE_SAFETY_SECONDS)));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $transport->ack($received[0]);
        });
    }

    public function testSameConnectionOwnerIsRejectedBeforeAcquisition(): void
    {
        $attempts = 0;
        $owner = new BrokerConnection(static function (Cancellation $cancellation) use (&$attempts): Client {
            ++$attempts;
            throw new \LogicException('Constructor validation must not acquire a client.');
        });
        try {
            new Transport($owner, new QueueName('jobs'), new PhpSerializer(), $owner);
            $this->fail('Reservation and notification owners must be distinct.');
        } catch (\InvalidArgumentException $error) {
            $this->assertStringContainsString('must be distinct', $error->getMessage());
        }
        $this->assertSame(0, $attempts);
    }

    public function testCancellationDuringNotificationHandshakePreservesReservationOwner(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $endpoint = $this->database->path('notification.sock');
            $server = listen('unix://'.$endpoint);
            $entered = new DeferredFuture();
            $peer = async(static function () use ($server, $entered): bool {
                $socket = $server->accept();
                if (null === $socket) {
                    throw new \LogicException('Missing notification probe peer.');
                }
                try {
                    Frame::read($socket, new TimeoutCancellation(self::PROBE_SAFETY_SECONDS));
                    // Hold HELLO confirmation until the caller cancels acquisition.
                    $entered->complete();

                    return null === Frame::read($socket, new TimeoutCancellation(self::PROBE_SAFETY_SECONDS));
                } finally {
                    $socket->close();
                }
            });
            $transport = new Transport($this->connection($this->endpoint), new QueueName('jobs'), new PhpSerializer(), $this->connection($endpoint));
            $stop = new DeferredCancellation();
            try {
                $transport->send(new Envelope(new TransportProbeMessage('reserved-during-handshake')));
                $envelopes = iterator_to_array($transport->get());
                $this->assertCount(1, $envelopes);
                $wait = async(static fn (): bool => $transport->wait(Limits::MAX_WAIT_MILLISECONDS, $stop->getCancellation()));
                $entered->getFuture()->await(new TimeoutCancellation(self::PROBE_SAFETY_SECONDS));
                $stop->cancel();
                try {
                    $wait->await(new TimeoutCancellation(self::PROBE_SAFETY_SECONDS));
                    $this->fail('Notification handshake must honor WAIT cancellation.');
                } catch (TransportException $error) {
                    $cause = $error;
                    while (null !== $cause->getPrevious()) {
                        $cause = $cause->getPrevious();
                    }
                    $this->assertInstanceOf(\Amp\CancelledException::class, $cause);
                }
                $this->assertTrue($peer->await(new TimeoutCancellation(self::PROBE_SAFETY_SECONDS)), 'Cancelling acquisition must close its socket.');
                $transport->ack($envelopes[0]);
                $this->assertSame([], iterator_to_array($transport->get()));
            } finally {
                $stop->cancel();
                $transport->close();
                $server->close();
                @unlink($endpoint);
            }
        });
    }

    public function testFetchSizeDefaultStillReturnsAtMostOne(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $transport = $this->transport();
            $transport->send(new Envelope(new TransportProbeMessage('one')));
            $transport->send(new Envelope(new TransportProbeMessage('two')));
            $received = iterator_to_array($transport->get(10));
            $this->assertCount(1, $received);
            $transport->ack($received[0]);
            $remaining = iterator_to_array($transport->get());
            $this->assertCount(1, $remaining);
            $transport->ack($remaining[0]);
        });
    }

    public function testDeliveryReceiptStampDoesNotLeakOntoAResend(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $transport = $this->transport();
            $transport->send(new Envelope(new TransportProbeMessage('first')));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $withReceipt = $received[0];
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $withReceipt->last(DeliveryReceiptStamp::class));

            $resent = $transport->send($withReceipt);
            $this->assertNull($resent->last(DeliveryReceiptStamp::class));
            $this->assertInstanceOf(TransportMessageIdStamp::class, $resent->last(TransportMessageIdStamp::class));
            $transport->ack($withReceipt);

            $queued = iterator_to_array($transport->get());
            $this->assertCount(1, $queued);
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $queued[0]->last(DeliveryReceiptStamp::class));
            $this->assertNotSame(
                $withReceipt->last(DeliveryReceiptStamp::class)?->receipt,
                $queued[0]->last(DeliveryReceiptStamp::class)?->receipt,
            );
            $transport->ack($queued[0]);
        });
    }

    public function testMissingDeliveryReceiptStampIsALocalLogicError(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $transport = $this->transport();
            try {
                $transport->ack(new Envelope(new TransportProbeMessage('no-stamp')));
                $this->fail('ACK without a DeliveryReceiptStamp must fail locally.');
            } catch (LogicException $error) {
                $this->assertStringContainsString(DeliveryReceiptStamp::class, $error->getMessage());
            }
        });
    }

    private function transport(): Transport
    {
        return $this->transportWithSerializer(new PhpSerializer());
    }

    private function transportWithSerializer(SerializerInterface $serializer): Transport
    {
        return new Transport($this->connection($this->endpoint), new QueueName('jobs'), $serializer, $this->connection($this->endpoint));
    }

    private function connection(string $endpoint): BrokerConnection
    {
        return new BrokerConnection(function (Cancellation $cancellation) use ($endpoint): Client {
            $client = Client::connect($endpoint, cancellation: $cancellation);
            $this->clients[] = $client;

            return $client;
        });
    }

    private function connectClient(): Client
    {
        $client = Client::connect($this->endpoint);
        $this->clients[] = $client;

        return $client;
    }

    private function setNow(int $value): void
    {
        $this->now = $value;
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        (new \Symfony\Component\Filesystem\Filesystem())->dumpFile($database->path().'.clock', (string) $value);
    }

    private function startBroker(int $visibilityTimeout = 5_000): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $this->broker = (new BrokerFactory(
            $database->path(),
            $this->endpoint,
            $visibilityTimeout,
            clock: $this->syncedClock(fn (): int => $this->now, $database->path()),
            workers: $this->workerFactory($database->path(), fn (): int => $this->now),
        ))->create();
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }

    private function awaitNotifierWaiters(int $count): void
    {
        $deadline = microtime(true) + 5;
        do {
            $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
            $waiters = (new \ReflectionProperty($notifier, 'waiters'))->getValue($notifier);
            $total = 0;
            foreach ($waiters as $list) {
                $total += \count($list);
            }
            if ($count === $total) {
                return;
            }
            $this->turn();
        } while (microtime(true) < $deadline);
        $this->fail('Expected '.$count.' notifier waiters.');
    }

    private function runAsync(\Closure $operation): void
    {
        async($operation)->await();
    }

    private function turn(): void
    {
        $suspension = EventLoop::getSuspension();
        $id = EventLoop::delay(0, static function () use ($suspension): void {
            $suspension->resume();
        });
        try {
            $suspension->suspend();
        } finally {
            EventLoop::cancel($id);
        }
    }
}

final class TransportProbeMessage
{
    public function __construct(public readonly string $body)
    {
    }
}

final class TransportHeaderStamp implements StampInterface
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(public readonly array $headers)
    {
    }
}
