<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\ProtocolException;
use Ineersa\SqliteQueue\TransportException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\listen;

final class ClientTest extends TestCase
{
    private string $directory = '';
    /** @var list<ServerSocket> */
    private array $servers = [];
    /** @var list<Socket> */
    private array $sockets = [];
    /** @var list<Client> */
    private array $clients = [];
    /** @var list<Future<void>> */
    private array $background = [];

    protected function setUp(): void
    {
        $this->directory = sys_get_temp_dir().'/sq-client-'.bin2hex(random_bytes(6));
        if (!mkdir($this->directory, 0700, true)) {
            $this->fail('Cannot create the temporary socket directory.');
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            $client->close();
        }
        $this->clients = [];
        foreach ($this->background as $future) {
            try {
                $future->await(new TimeoutCancellation(3));
            } catch (\Throwable) {
                // A failed or hung fake peer is reported by the test body, not by teardown.
            }
        }
        $this->background = [];
        foreach ($this->sockets as $socket) {
            $socket->close();
        }
        $this->sockets = [];
        foreach ($this->servers as $server) {
            $server->close();
        }
        $this->servers = [];
        @unlink($this->directory.'/broker.sock');
        @rmdir($this->directory);
    }

    public function testServerCloseWithoutConfirmationFailsTheCallAndNeverReplays(): void
    {
        $received = 0;
        $endpoint = $this->serve(function (Socket $socket) use (&$received): void {
            Frame::read($socket, new TimeoutCancellation(3));
            ++$received;
            $socket->write(self::helloReply());
            $request = Frame::read($socket, new TimeoutCancellation(3));
            $this->assertInstanceOf(Frame::class, $request);
            $this->assertSame('send', $request->control['op']);
            $this->assertSame('payload', $request->body);
            ++$received;
            // Close the connection without confirming the send.
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send('jobs', 'payload');
            $this->fail('A closed connection must not confirm the send.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
        }
        $this->awaitBackground();
        $this->assertSame(2, $received, 'The broker received exactly the handshake and the send.');
        try {
            $client->send('jobs', 'payload');
            $this->fail('A closed client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
        $this->awaitBackground();
        $this->assertSame(2, $received, 'The failed call must not be replayed on the wire.');
    }

    public function testConcurrentCallIsRejectedAndCancellationEndsPendingCallAtPeerEof(): void
    {
        $requestSeen = new DeferredFuture();
        $peerEof = new DeferredFuture();
        $endpoint = $this->serve(static function (Socket $socket) use ($requestSeen, $peerEof): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $requestSeen->complete();
            }
            try {
                while (null !== $socket->read(new TimeoutCancellation(3), 1024)) {
                }
            } catch (\Throwable) {
                // A reset peer still ends the connection.
            }
            $peerEof->complete();
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        $session = new DeferredCancellation();
        $pending = async(static fn () => $client->receive('jobs', $session->getCancellation()));
        try {
            $requestSeen->getFuture()->await(new TimeoutCancellation(3));
            try {
                $client->send('jobs', 'concurrent');
                $this->fail('A second concurrent call must be rejected.');
            } catch (\LogicException $error) {
                $this->assertStringContainsString('one outstanding request', $error->getMessage());
            }
            $this->assertFalse($pending->isComplete());
            $session->cancel();
            try {
                $pending->await(new TimeoutCancellation(3));
                $this->fail('A cancelled call must not report success.');
            } catch (TransportException $error) {
                $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
            }
            $peerEof->getFuture()->await(new TimeoutCancellation(3));
        } finally {
            $session->cancel();
            try {
                $pending->await(new TimeoutCancellation(3));
            } catch (\Throwable) {
                // The pending call is expected to fail; this only releases it.
            }
        }
    }

    /** @return iterable<string, array{string}> */
    public static function malformedReplies(): iterable
    {
        yield 'uncorrelated id' => [(new Frame(['v' => 1, 'id' => 7, 'ok' => true, 'result' => 1]))->encode()];
        yield 'missing confirmation flag' => [(new Frame(['v' => 1, 'id' => 1, 'result' => 1]))->encode()];
        yield 'missing result' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true]))->encode()];
        yield 'unsupported version' => [(new Frame(['v' => 2, 'id' => 1, 'ok' => true, 'result' => 1]))->encode()];
        yield 'hand crafted control' => [pack('NN', 6, 2).'{}'];
        yield 'unknown error code' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'nope']]))->encode()];
        yield 'missing error' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false]))->encode()];
        yield 'failure with body' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'stale_receipt']], 'b'))->encode()];
        yield 'failure with result' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'result' => null, 'error' => ['code' => 'stale_receipt']]))->encode()];
        yield 'sent id zero' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => 0]))->encode()];
        yield 'sent id string' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => '7']))->encode()];
        yield 'sent with body' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => 7], 'b'))->encode()];
        yield 'delivery wrong queue' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'other', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]]))->encode()];
        yield 'delivery id zero' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 0, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]]))->encode()];
        yield 'delivery integer receipt' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 5, 'available_at' => 1, 'reserved_until' => 2]]))->encode()];
        yield 'delivery string available_at' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => '1', 'reserved_until' => 2]]))->encode()];
    }

    public function testOversizePayloadFailsLocallyWithoutConsumingTheSequence(): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control['id'];
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 41]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send('jobs', str_repeat('x', Frame::MAX_PAYLOAD + 1));
            $this->fail('An oversize payload must fail before it is sent.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::FrameTooLarge, $error->errorCode);
        }
        $this->assertSame(41, $client->send('jobs', 'fits'));
        $this->assertSame([1], $seen, 'The failed send must not consume a sequence id.');
    }

    public function testUnencodableControlClosesClientWithoutSending(): void
    {
        $seen = 0;
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                ++$seen;
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send("bad\xffqueue", 'payload');
            $this->fail('Unencodable control must fail before it is sent.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
        }
        $this->awaitBackground();
        $this->assertSame(0, $seen, 'No operation bytes may reach the broker.');
        try {
            $client->send('jobs', 'payload');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    #[DataProvider('malformedReplies')]
    public function testMalformedOrUncorrelatedReplyInvalidatesClient(string $reply): void
    {
        $endpoint = $this->serve(static function (Socket $socket) use ($reply): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $socket->write($reply);
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send('jobs', 'payload');
            $this->fail('A malformed reply must not be accepted as confirmation.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
        }
        try {
            $client->send('jobs', 'payload');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    /** @return iterable<string, array{array<string, mixed>|int|null, string, string}> */
    public static function malformedDeliveries(): iterable
    {
        $good = ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2];
        yield 'missing id' => [[...$good, 'id' => null], '', ''];
        yield 'string id' => [[...$good, 'id' => '7'], '', ''];
        yield 'zero id' => [[...$good, 'id' => 0], '', ''];
        yield 'negative id' => [[...$good, 'id' => -3], '', ''];
        yield 'other queue' => [[...$good, 'queue' => 'other'], '', ''];
        yield 'missing queue' => [[...$good, 'queue' => null], '', ''];
        yield 'integer receipt' => [[...$good, 'receipt' => 5], '', ''];
        yield 'missing receipt' => [[...$good, 'receipt' => null], '', ''];
        yield 'string available_at' => [[...$good, 'available_at' => '1'], '', ''];
        yield 'missing available_at' => [[...$good, 'available_at' => null], '', ''];
        yield 'string reserved_until' => [[...$good, 'reserved_until' => '2'], '', ''];
        yield 'missing reserved_until' => [[...$good, 'reserved_until' => null], '', ''];
        yield 'integer result' => [7, '', ''];
        yield 'empty receive with body' => [null, 'b', ''];
        yield 'empty receive with headers' => [null, '', 'h'];
    }

    /**
     * @param array<string, mixed>|int|null $result
     */
    #[DataProvider('malformedDeliveries')]
    public function testMalformedDeliveryInvalidatesClient(array|int|null $result, string $body, string $headers): void
    {
        $endpoint = $this->serve(static function (Socket $socket) use ($result, $body, $headers): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => true, 'result' => $result], $body, $headers))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->receive('jobs');
            $this->fail('A malformed delivery must not be accepted.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
        }
        try {
            $client->receive('jobs');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    /** @return iterable<string, array{int|null, string, string}> */
    public static function malformedSettlements(): iterable
    {
        yield 'with result' => [5, '', ''];
        yield 'with body' => [null, 'b', ''];
        yield 'with headers' => [null, '', 'h'];
    }

    #[DataProvider('malformedSettlements')]
    public function testMalformedSettlementInvalidatesClient(?int $result, string $body, string $headers): void
    {
        $endpoint = $this->serve(static function (Socket $socket) use ($result, $body, $headers): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => true, 'result' => $result], $body, $headers))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->acknowledge('receipt');
            $this->fail('A malformed settlement must not be accepted.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('outcome may be unknown', $error->getMessage());
        }
        try {
            $client->acknowledge('receipt');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    public function testConnectRejectsWrongMaxPayload(): void
    {
        $this->assertConnectFails(['max_payload' => 1], '', '', 'A broker advertising a different payload bound must not be trusted.');
    }

    /** @return iterable<string, array{array<string, mixed>|string, string, string}> */
    public static function malformedHellos(): iterable
    {
        $good = ['max_payload' => Frame::MAX_PAYLOAD];
        yield 'string result' => ['x', '', ''];
        yield 'missing max payload' => [[], '', ''];
        yield 'with body' => [$good, 'b', ''];
        yield 'with headers' => [$good, '', 'h'];
    }

    /**
     * @param array<string, mixed>|string $result
     */
    #[DataProvider('malformedHellos')]
    public function testMalformedHelloRejectsConnection(array|string $result, string $body, string $headers): void
    {
        $this->assertConnectFails($result, $body, $headers, 'A malformed hello must not be trusted.');
    }

    /**
     * @param array<string, mixed>|string $result
     */
    private function assertConnectFails(array|string $result, string $body, string $headers, string $message): void
    {
        $endpoint = $this->serve(static function (Socket $socket) use ($result, $body, $headers): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => $result], $body, $headers))->encode());
        });
        try {
            $client = Client::connect($endpoint);
            $this->clients[] = $client;
            $this->fail($message);
        } catch (TransportException $error) {
            $this->assertStringContainsString('Invalid broker response', $error->getMessage());
        }
    }

    /** @param \Closure(Socket): void $handler */
    private function serve(\Closure $handler): string
    {
        $path = $this->directory.'/broker.sock';
        $server = listen('unix://'.$path);
        $this->servers[] = $server;
        $this->background[] = async(function () use ($server, $handler): void {
            $socket = $server->accept();
            if (null === $socket) {
                return;
            }
            $this->sockets[] = $socket;
            try {
                $handler($socket);
            } finally {
                $socket->close();
            }
        });

        return $path;
    }

    private static function helloReply(): string
    {
        return (new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => Frame::MAX_PAYLOAD]]))->encode();
    }

    private function awaitBackground(): void
    {
        foreach ($this->background as $index => $future) {
            $future->await(new TimeoutCancellation(3));
            unset($this->background[$index]);
        }
    }
}
