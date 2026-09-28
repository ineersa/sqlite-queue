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
use Ineersa\SqliteQueue\Protocol\Frame;
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
