<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\ByteStream\BufferedReader;
use Amp\ByteStream\ResourceStream;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\ExpiredReceiptException;
use Ineersa\SqliteQueue\Exception\TransportException as ClientTransportException;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Messenger\Stamp\DeliveryReceiptStamp;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Protocol\KeepaliveStream;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ProtocolException;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\KeepalivePeerReply;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function Amp\async;
use function Amp\ByteStream\buffer;
use function Amp\Future\awaitFirst;

final class KeepaliveTest extends TestCase
{
    private const int SAFETY_SECONDS = 5;
    private const string RECEIPT = '1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    /** @var list<array{Client, Process, IsolatedDatabase, Future<string>}> */
    private array $resources = [];

    protected function tearDown(): void
    {
        foreach ($this->resources as [$client, $peer, $directory, $errors]) {
            $client->close();
            try {
                $this->assertSame(0, $peer->join(new TimeoutCancellation(self::SAFETY_SECONDS)), $errors->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            } finally {
                if ($peer->isRunning()) {
                    $peer->kill();
                }
                $directory->remove();
            }
        }
    }

    /** @return iterable<string, array{float}> */
    public static function invalidTimeouts(): iterable
    {
        yield 'zero' => [0.0];
        yield 'negative' => [-1.0];
        yield 'infinite' => [\INF];
        yield 'not a number' => [\NAN];
    }

    #[DataProvider('invalidTimeouts')]
    public function testNativeBridgeRequiresABoundedTimeout(float $seconds): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Keepalive timeout must be positive finite seconds.');
        new KeepaliveStream($this->createStub(ResourceStream::class), $seconds);
    }

    public function testRealSignalUsesExistingOperationsSocketAndSharesRequestIds(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, Operation::Wait);
        $delivery = $this->delivery($transport);
        foreach ([null, 0, 30, 90_000] as $seconds) {
            $this->alarm(static fn () => $transport->keepalive($delivery, $seconds));
            $packet = $this->packet($reader);
            $this->assertSame(Operation::Keepalive->value, $packet['op']);
            $this->assertSame(($seconds ?? 0) * 1000, $packet['keepalive_ms']);
            $this->assertSame(self::RECEIPT, $packet['receipt']);
        }
        $sent = $transport->send(new Envelope(new \stdClass()));
        $this->assertSame(42, $sent->last(TransportMessageIdStamp::class)->getId());
        $this->assertSame(5, $this->packet($reader)['id']);
        $transport->close();
    }

    /** @return iterable<string, array{KeepalivePeerReply}> */
    public static function replies(): iterable
    {
        foreach (KeepalivePeerReply::cases() as $reply) {
            yield $reply->value => [$reply];
        }
    }

    #[DataProvider('replies')]
    public function testInterruptedSendCoalescesAndPreservesConfirmedSuccess(KeepalivePeerReply $reply): void
    {
        [$transport, $client, $peer, $reader] = $this->peer($reply, Operation::Send);
        $delivery = $this->delivery($transport);
        $send = async(static fn (): Envelope => $transport->send(new Envelope(new \stdClass())));
        $this->assertSame(Operation::Send->value, $this->packet($reader)['op']);
        $this->alarm(static fn () => $transport->keepalive($delivery, 5));
        $this->alarm(static fn () => $transport->keepalive($delivery, 30));
        $pending = (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client);
        $this->assertCount(1, $pending);
        $peer->getStdin()->write("release\n");
        $sent = $send->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame(42, $sent->last(TransportMessageIdStamp::class)->getId());
        $packet = $this->packet($reader);
        $this->assertSame(Operation::Keepalive->value, $packet['op']);
        $this->assertSame(30_000, $packet['keepalive_ms']);
        $this->assertSame([], (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        if (KeepalivePeerReply::Success !== $reply) {
            try {
                $transport->get();
                $this->fail('Deferred keepalive failure was lost.');
            } catch (TransportException $error) {
                $this->assertStringContainsString('Deferred queue keepalive failed:', $error->getMessage());
                $this->assertInstanceOf(match ($reply) {
                    KeepalivePeerReply::Expired => ExpiredReceiptException::class,
                    KeepalivePeerReply::Malformed => ProtocolException::class,
                    default => ClientTransportException::class,
                }, $error->getPrevious());
            }
        }
        $transport->close();
    }

    public function testPendingRenewalsAreCoalescedSeparatelyForEachActiveReceipt(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, Operation::Send);
        $first = $this->delivery($transport);
        // Client receipt strings remain opaque, including numeric strings issued by this peer.
        $secondReceipt = '2';
        $second = new Envelope(new \stdClass(), [new DeliveryReceiptStamp($secondReceipt, $transport)]);
        $send = async(static fn (): Envelope => $transport->send(new Envelope(new \stdClass())));
        $this->packet($reader);
        $this->alarm(static fn () => $transport->keepalive($first, 5));
        $this->alarm(static fn () => $transport->keepalive($second, 15));
        $this->alarm(static fn () => $transport->keepalive($first, 30));
        $this->assertCount(2, (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        $peer->getStdin()->write("release\n");
        $send->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame(self::RECEIPT, $this->packet($reader)['receipt']);
        $this->assertSame($secondReceipt, $this->packet($reader)['receipt']);
        $this->assertSame([], (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        $transport->close();
    }

    public function testDeferredFailureIsObservableOnCloseAfterConfirmedSend(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Expired, Operation::Send);
        $delivery = $this->delivery($transport);
        $send = async(static fn (): Envelope => $transport->send(new Envelope(new \stdClass())));
        $this->packet($reader);
        $this->alarm(static fn () => $transport->keepalive($delivery));
        $peer->getStdin()->write("release\n");
        $this->assertSame(42, $send->await(new TimeoutCancellation(self::SAFETY_SECONDS))->last(TransportMessageIdStamp::class)->getId());
        $this->packet($reader);
        unset($delivery);
        gc_collect_cycles();
        try {
            $transport->close();
            $this->fail('Closing must expose a deferred keepalive failure.');
        } catch (TransportException $error) {
            $this->assertInstanceOf(ExpiredReceiptException::class, $error->getPrevious());
        }
        $this->assertTrue((new \ReflectionProperty(Client::class, 'closed'))->getValue($client));
    }

    /** @return iterable<string, array{Operation}> */
    public static function settlements(): iterable
    {
        yield 'ack' => [Operation::Acknowledge];
        yield 'reject' => [Operation::Reject];
    }

    public function testSiblingRenewalFailureCannotDiscardConfirmedAcknowledgement(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Expired, Operation::Acknowledge);
        $delivery = $this->delivery($transport);
        $sibling = new Envelope(new \stdClass(), [new DeliveryReceiptStamp('2:'.str_repeat('b', 64), $transport)]);
        $ack = async(static fn () => $transport->ack($delivery));
        $this->packet($reader);
        $this->alarm(static fn () => $transport->keepalive($sibling));
        $peer->getStdin()->write("release\n");
        $this->assertNull($ack->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
        $this->assertSame(Operation::Keepalive->value, $this->packet($reader)['op']);
        try {
            $transport->get();
            $this->fail('Sibling renewal failure must remain observable.');
        } catch (TransportException $error) {
            $this->assertInstanceOf(ExpiredReceiptException::class, $error->getPrevious());
        }
        $transport->close();
    }

    #[DataProvider('settlements')]
    public function testSettlementCancelsRenewalQueuedDuringItsReply(Operation $operation): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, $operation);
        $delivery = $this->delivery($transport);
        $settle = async(static function () use ($transport, $delivery, $operation): void {
            if (Operation::Acknowledge === $operation) {
                $transport->ack($delivery);
            } else {
                $transport->reject($delivery);
            }
        });
        $this->assertSame($operation->value, $this->packet($reader)['op']);
        $this->alarm(static fn () => $transport->keepalive($delivery));
        $errors = [];
        $this->alarm(static fn () => $client->keepaliveSignalSafe(self::RECEIPT, 0, static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        }));
        $this->assertCount(1, (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        $peer->getStdin()->write("release\n");
        $settle->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame([], $errors);
        $this->assertSame([], (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        $this->alarm(static fn () => $transport->keepalive($delivery));
        foreach ([-1, intdiv(\PHP_INT_MAX, 1000) + 1] as $seconds) {
            try {
                $transport->keepalive($delivery, $seconds);
                $this->fail('Settled receipts must not bypass duration validation.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('Keepalive seconds', $error->getMessage());
            }
        }
        $transport->send(new Envelope(new \stdClass()));
        $packet = $this->packet($reader);
        $this->assertSame(Operation::Send->value, $packet['op']);
        $this->assertSame(2, $packet['id'], 'The obsolete heartbeat must consume no request ID.');
        $transport->close();
    }

    public function testCloseCancelsPendingRenewalAndReportsInterruptedSend(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, Operation::Send);
        $delivery = $this->delivery($transport);
        $send = async(static fn (): Envelope => $transport->send(new Envelope(new \stdClass())));
        $this->packet($reader);
        $this->alarm(static fn () => $transport->keepalive($delivery));
        $transport->close();
        $peer->getStdin()->write("release\n");
        try {
            $send->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->fail('An interrupted send cannot report confirmation.');
        } catch (TransportException $error) {
            $this->assertInstanceOf(ClientTransportException::class, $error->getPrevious());
        }
        $this->assertSame([], (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
    }

    public function testInvalidMinimumAndMissingOrForeignReceiptDoNotDispatch(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, Operation::Wait);
        foreach ([-1, intdiv(\PHP_INT_MAX, 1000) + 1] as $seconds) {
            try {
                $transport->keepalive($this->delivery($transport), $seconds);
                $this->fail('Invalid minimum was accepted.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('Keepalive seconds', $error->getMessage());
            }
        }
        try {
            $transport->keepalive(new Envelope(new \stdClass()));
            $this->fail('Missing receipt stamp was accepted.');
        } catch (LogicException $error) {
            $this->assertStringContainsString('No ', $error->getMessage());
        }
        $foreign = $this->unconnectedTransport();
        try {
            $transport->keepalive($this->delivery($foreign));
            $this->fail('Foreign receipt stamp was accepted.');
        } catch (LogicException $error) {
            $this->assertStringContainsString('not received by this transport', $error->getMessage());
        }
        $transport->send(new Envelope(new \stdClass()));
        $this->assertSame(1, $this->packet($reader)['id']);
        $transport->close();
    }

    #[DataProvider('dequeueOperations')]
    public function testSignalAtAtomicDequeueCannotLoseAConfirmedResult(Operation $operation): void
    {
        $process = Process::start([\PHP_BINARY, __DIR__.'/Fixtures/keepalive-dequeue-boundary.php', $operation->value]);
        $stdout = async(static fn (): string => buffer($process->getStdout()));
        $stderr = async(static fn (): string => buffer($process->getStderr()));
        try {
            $this->assertSame(0, $process->join(new TimeoutCancellation(self::SAFETY_SECONDS)), $stderr->await());
            $result = json_decode($stdout->await(), true, 16, \JSON_THROW_ON_ERROR);
            $this->assertTrue($result['original_confirmed'], json_encode($result));
            $this->assertSame(Operation::Send === $operation ? 42 : null, $result['original_result']);
            $this->assertSame('array_splice', $result['boundary']);
            $this->assertFalse($result['busy_at_boundary']);
            $this->assertTrue($result['flushing_at_boundary']);
            $this->assertNull($result['signal_result'], 'A signal during drain must defer without entering native I/O.');
            $this->assertSame([], $result['signal_errors']);
            $this->assertSame([], $result['warnings']);
            $this->assertCount(1, $result['errors']);
            $this->assertSame(ProtocolException::class, $result['errors'][0]['class']);
            $this->assertStringContainsString('KEEPALIVE result', $result['errors'][0]['message']);
            $this->assertSame(0, $result['peer_exit']);
            $this->assertSame('', $result['peer_stderr']);
        } finally {
            if ($process->isRunning()) {
                $process->kill();
            }
        }
    }

    /** @return iterable<string, array{Operation}> */
    public static function dequeueOperations(): iterable
    {
        yield 'confirmed send' => [Operation::Send];
        yield 'confirmed ACK' => [Operation::Acknowledge];
    }

    public function testRepeatedTicksCannotExtendTheOriginalAmpDrain(): void
    {
        [$transport, $client, $peer, $reader] = $this->peer(KeepalivePeerReply::Success, [Operation::Send, Operation::Keepalive]);
        $delivery = $this->delivery($transport);
        $sibling = new Envelope(new \stdClass(), [new DeliveryReceiptStamp('sibling', $transport)]);
        $send = async(static fn (): Envelope => $transport->send(new Envelope(new \stdClass())));
        $this->assertSame(Operation::Send->value, $this->packet($reader)['op']);
        $this->alarm(static fn () => $transport->keepalive($delivery, 5));
        $peer->getStdin()->write("release\n");
        $firstRenewal = $this->packet($reader);
        $this->assertSame(Operation::Keepalive->value, $firstRenewal['op']);
        $this->assertSame(5000, $firstRenewal['keepalive_ms']);
        $this->alarm(static fn () => $transport->keepalive($delivery, 30));
        $this->alarm(static fn () => $transport->keepalive($sibling, 15));
        $this->alarm(static fn () => $transport->keepalive($delivery, 10));
        $cancellation = new DeferredCancellation();
        $nextPacket = async(static fn (): ?string => $reader->readUntil("\n", $cancellation->getCancellation()));
        $nextPacket->ignore();
        $competingPacket = $nextPacket->map(static fn (): string => 'another renewal dispatched');
        $competingPacket->ignore();
        $confirmed = $send->map(static fn (): string => 'confirmed');
        $confirmed->ignore();
        try {
            $peer->getStdin()->write("release\n");
            $winner = awaitFirst([
                $confirmed,
                $competingPacket,
            ], new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertSame('confirmed', $winner, 'A second gated renewal must not hold the original send hostage.');
            $this->assertSame(42, $send->await()->last(TransportMessageIdStamp::class)->getId());
            $pending = array_values((new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
            $this->assertSame([self::RECEIPT, 'sibling'], array_column($pending, 'receipt'));
            $this->assertSame([30_000, 15_000], array_column($pending, 'minimum'));
        } finally {
            $cancellation->cancel();
            try {
                $nextPacket->await();
            } catch (CancelledException) {
                // Release BufferedReader's pending read before the next control assertion.
            }
            if (!$send->isComplete()) {
                $send->ignore();
                $client->close();
                $peer->getStdin()->write("release\n");
            }
        }
        // Settlement cancels its own queued tick, while a sibling remains real active work.
        $ack = async(static fn () => $transport->ack($delivery));
        $this->assertSame(Operation::Acknowledge->value, $this->packet($reader)['op']);
        $siblingRenewal = $this->packet($reader);
        $this->assertSame('sibling', $siblingRenewal['receipt']);
        $this->assertSame(15_000, $siblingRenewal['keepalive_ms']);
        $peer->getStdin()->write("release\n");
        $ack->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame([], (new \ReflectionProperty(Client::class, 'pendingKeepalives'))->getValue($client));
        $transport->close();
    }

    /**
     * @param Operation|list<Operation> $gate
     *
     * @return array{Transport, Client, Process, BufferedReader}
     */
    private function peer(KeepalivePeerReply $reply, Operation|array $gate): array
    {
        $directory = new IsolatedDatabase();
        $endpoint = $directory->path('heartbeat.sock');
        $gates = $gate instanceof Operation ? [$gate] : $gate;
        $peer = Process::start([\PHP_BINARY, __DIR__.'/Fixtures/keepalive-peer.php', $endpoint, implode(',', array_map(static fn (Operation $operation): string => $operation->value, $gates)), $reply->value]);
        $errors = async(static fn (): string => buffer($peer->getStderr()));
        $errors->ignore();
        $reader = new BufferedReader($peer->getStdout());
        $this->assertSame('ready', $reader->readUntil("\n", new TimeoutCancellation(self::SAFETY_SECONDS)));
        $client = Client::connect($endpoint);
        $this->resources[] = [$client, $peer, $directory, $errors];
        $this->assertSame(Operation::Hello->value, $this->packet($reader)['op']);
        $transport = new Transport(
            new BrokerConnection(static fn (Cancellation $cancellation): Client => $client),
            new QueueName('jobs'),
            new PhpSerializer(),
            new BrokerConnection(static fn (Cancellation $cancellation): Client => throw new \LogicException('Keepalive must not acquire the notification socket.')),
            $endpoint,
        );

        return [$transport, $client, $peer, $reader];
    }

    private function unconnectedTransport(): Transport
    {
        $connector = static fn (Cancellation $cancellation): Client => throw new \LogicException('Unexpected connection acquisition.');

        return new Transport(new BrokerConnection($connector), new QueueName('jobs'), new PhpSerializer(), new BrokerConnection($connector), '/unused.sock');
    }

    private function delivery(Transport $transport): Envelope
    {
        return new Envelope(new \stdClass(), [new DeliveryReceiptStamp(self::RECEIPT, $transport)]);
    }

    /** @return array<string, mixed> */
    private function packet(BufferedReader $reader): array
    {
        return json_decode((string) $reader->readUntil("\n", new TimeoutCancellation(self::SAFETY_SECONDS)), true, 16, \JSON_THROW_ON_ERROR);
    }

    private function alarm(\Closure $callback): void
    {
        $previous = pcntl_signal_get_handler(\SIGALRM);
        $asynchronous = pcntl_async_signals(true);
        pcntl_signal(\SIGALRM, $callback);
        try {
            $this->assertTrue(posix_kill(getmypid(), \SIGALRM));
        } finally {
            pcntl_signal(\SIGALRM, $previous);
            pcntl_async_signals($asynchronous);
        }
    }
}
