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
use Ineersa\SqliteQueue\Exception\ExpiredReceiptException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\MalformedReceiptException;
use Ineersa\SqliteQueue\Exception\NoActiveReservationException;
use Ineersa\SqliteQueue\Exception\ReceiptEpochMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptOwnerMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptTokenMismatchException;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ProtocolException;
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

    /** @return iterable<string, array{string, string}> */
    public static function malformedReplies(): iterable
    {
        yield 'uncorrelated id' => [(new Frame(['v' => 1, 'id' => 7, 'ok' => true, 'result' => 1]))->encode(), 'Response id must equal the outstanding request id.'];
        yield 'missing confirmation flag' => [(new Frame(['v' => 1, 'id' => 1, 'result' => 1]))->encode(), 'Response ok must be a boolean.'];
        yield 'missing result' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true]))->encode(), 'Successful response must include result.'];
        yield 'unsupported version' => [(new Frame(['v' => 2, 'id' => 1, 'ok' => true, 'result' => 1]))->encode(), 'Response v must equal protocol version 1.'];
        yield 'hand crafted control' => [pack('NN', 6, 2).'{}', 'Missing payload lengths.'];
        yield 'unknown error code' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'nope']]))->encode(), 'Failure response error.code is not a known protocol error.'];
        yield 'removed internal storage failure' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'internal_storage_failure']]))->encode(), 'Failure response error.code is not a known protocol error.'];
        yield 'missing error' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false]))->encode(), 'Failure response must include an error object.'];
        yield 'failure with body' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => ErrorCode::ExpiredReceipt->value]], 'b'))->encode(), 'Failure response must not include a payload.'];
        yield 'failure with result' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => false, 'result' => null, 'error' => ['code' => ErrorCode::ExpiredReceipt->value]]))->encode(), 'Failure response must not include result.'];
        yield 'sent id zero' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => 0]))->encode(), 'SEND result must be a positive integer.'];
        yield 'sent id string' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => '7']))->encode(), 'SEND result must be an integer.'];
        yield 'sent with body' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => 7], 'b'))->encode(), 'SEND must not include a payload.'];
        yield 'delivery wrong queue' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'other', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]]))->encode(), 'RECEIVE result.queue must equal the requested queue.'];
        yield 'delivery id zero' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 0, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]]))->encode(), 'RECEIVE result.id must be a positive integer.'];
        yield 'delivery integer receipt' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 5, 'available_at' => 1, 'reserved_until' => 2]]))->encode(), 'RECEIVE result.receipt must be a string.'];
        yield 'delivery string available_at' => [(new Frame(['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => '1', 'reserved_until' => 2]]))->encode(), 'RECEIVE result.available_at must be an integer.'];
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

    public function testUnencodableControlFailsLocallyWithoutSendingOrClosing(): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control['id'] ?? null;
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 17]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send("bad\xffqueue", 'payload');
            $this->fail('Unencodable control must fail before it is sent.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
        }
        $this->assertSame(17, $client->send('jobs', 'payload'));
        $this->assertSame([1], $seen, 'The rejected request must not be sent or consume a sequence id.');
    }

    #[DataProvider('malformedReplies')]
    public function testMalformedOrUncorrelatedReplyInvalidatesClient(string $reply, string $reason): void
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
        $operation = str_starts_with($reason, 'RECEIVE') ? 'receive' : 'send';
        try {
            if ('receive' === $operation) {
                $client->receive('jobs');
            } else {
                $client->send('jobs', 'payload');
            }
            $this->fail('A malformed reply must not be accepted as confirmation.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
            $this->assertSame($reason, $error->getMessage());
        }
        try {
            $client->send('jobs', 'payload');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    /** @return iterable<string, array{array<string, mixed>|int|null, string, string, string}> */
    public static function malformedDeliveries(): iterable
    {
        $good = ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2];
        yield 'missing id' => [[...$good, 'id' => null], '', '', 'RECEIVE result.id must be an integer.'];
        yield 'string id' => [[...$good, 'id' => '7'], '', '', 'RECEIVE result.id must be an integer.'];
        yield 'zero id' => [[...$good, 'id' => 0], '', '', 'RECEIVE result.id must be a positive integer.'];
        yield 'negative id' => [[...$good, 'id' => -3], '', '', 'RECEIVE result.id must be a positive integer.'];
        yield 'other queue' => [[...$good, 'queue' => 'other'], '', '', 'RECEIVE result.queue must equal the requested queue.'];
        yield 'missing queue' => [[...$good, 'queue' => null], '', '', 'RECEIVE result.queue must equal the requested queue.'];
        yield 'integer receipt' => [[...$good, 'receipt' => 5], '', '', 'RECEIVE result.receipt must be a string.'];
        yield 'missing receipt' => [[...$good, 'receipt' => null], '', '', 'RECEIVE result.receipt must be a string.'];
        yield 'string available_at' => [[...$good, 'available_at' => '1'], '', '', 'RECEIVE result.available_at must be an integer.'];
        yield 'missing available_at' => [[...$good, 'available_at' => null], '', '', 'RECEIVE result.available_at must be an integer.'];
        yield 'string reserved_until' => [[...$good, 'reserved_until' => '2'], '', '', 'RECEIVE result.reserved_until must be an integer.'];
        yield 'missing reserved_until' => [[...$good, 'reserved_until' => null], '', '', 'RECEIVE result.reserved_until must be an integer.'];
        yield 'integer result' => [7, '', '', 'RECEIVE result must be an object or null.'];
        yield 'empty receive with body' => [null, 'b', '', 'empty RECEIVE must not include a payload.'];
        yield 'empty receive with headers' => [null, '', 'h', 'empty RECEIVE must not include a payload.'];
    }

    /**
     * @param array<string, mixed>|int|null $result
     */
    #[DataProvider('malformedDeliveries')]
    public function testMalformedDeliveryInvalidatesClient(array|int|null $result, string $body, string $headers, string $reason): void
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
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
            $this->assertSame($reason, $error->getMessage());
        }
        try {
            $client->receive('jobs');
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    /** @return iterable<string, array{int|null, string, string, string}> */
    public static function malformedSettlements(): iterable
    {
        yield 'with result' => [5, '', '', 'ACKNOWLEDGE result must be null.'];
        yield 'with body' => [null, 'b', '', 'ACKNOWLEDGE must not include a payload.'];
        yield 'with headers' => [null, '', 'h', 'ACKNOWLEDGE must not include a payload.'];
    }

    #[DataProvider('malformedSettlements')]
    public function testMalformedSettlementInvalidatesClient(?int $result, string $body, string $headers, string $reason): void
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
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
            $this->assertSame($reason, $error->getMessage());
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
        $this->assertConnectFails(['max_payload' => 1], '', '', 'HELLO result.max_payload must equal the client payload limit.');
    }

    /** @return iterable<string, array{array<string, mixed>|string, string, string, string}> */
    public static function malformedHellos(): iterable
    {
        $good = ['max_payload' => Frame::MAX_PAYLOAD];
        yield 'string result' => ['x', '', '', 'HELLO result must be an object.'];
        yield 'missing max payload' => [[], '', '', 'HELLO result.max_payload must equal the client payload limit.'];
        yield 'with body' => [$good, 'b', '', 'HELLO must not include a payload.'];
        yield 'with headers' => [$good, '', 'h', 'HELLO must not include a payload.'];
    }

    /**
     * @param array<string, mixed>|string $result
     */
    #[DataProvider('malformedHellos')]
    public function testMalformedHelloRejectsConnection(array|string $result, string $body, string $headers, string $reason): void
    {
        $this->assertConnectFails($result, $body, $headers, $reason);
    }

    public static function receiptRejections(): iterable
    {
        yield 'malformed' => [ErrorCode::MalformedReceipt, new MalformedReceiptException()];
        yield 'no reservation' => [ErrorCode::NoActiveReservation, new NoActiveReservationException()];
        yield 'owner mismatch' => [ErrorCode::ReceiptOwnerMismatch, new ReceiptOwnerMismatchException()];
        yield 'epoch mismatch' => [ErrorCode::ReceiptEpochMismatch, new ReceiptEpochMismatchException()];
        yield 'token mismatch' => [ErrorCode::ReceiptTokenMismatch, new ReceiptTokenMismatchException()];
        yield 'expired' => [ErrorCode::ExpiredReceipt, new ExpiredReceiptException()];
    }

    #[DataProvider('receiptRejections')]
    public function testReceiptRejectionKeepsTheClientUsable(ErrorCode $code, InvalidReceiptException $expected): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen, $code): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control['id'];
                if (1 === $request->control['id']) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => false, 'error' => ['code' => $code->value]]))->encode());
                    continue;
                }
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 9]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->acknowledge('receipt');
            $this->fail('The receipt must be rejected.');
        } catch (InvalidReceiptException $error) {
            $this->assertSame($expected::class, $error::class);
            $this->assertSame($expected->getMessage(), $error->getMessage());
            $this->assertSame($code, ErrorCode::fromReceiptException($error));
        }
        $this->assertSame(9, $client->send('jobs', 'next'));
        $this->assertSame([1, 2], $seen);
    }

    public function testInvalidQueueNameKeepsTheClientUsable(): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control['id'];
                if (1 === $request->control['id']) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => false, 'error' => ['code' => ErrorCode::InvalidQueueName->value]]))->encode());
                    continue;
                }
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 11]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send('-jobs', 'payload');
            $this->fail('An invalid queue name must be rejected.');
        } catch (\InvalidArgumentException) {
        }
        $this->assertSame(11, $client->send('jobs', 'next'));
        $this->assertSame([1, 2], $seen);
    }

    public function testBrokerShuttingDownInvalidatesTheClient(): void
    {
        $endpoint = $this->serve(static function (Socket $socket): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => false, 'error' => ['code' => ErrorCode::BrokerShuttingDown->value]]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->send('jobs', 'payload');
            $this->fail('A shutting-down broker must invalidate the client.');
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

    public function testAllowedFieldsMatchTheBrokerRequestContract(): void
    {
        $this->assertSame([], Operation::Hello->allowedFields());
        $this->assertSame(['queue', 'delay'], Operation::Send->allowedFields());
        $this->assertSame(['queue'], Operation::Receive->allowedFields());
        $this->assertSame(['receipt'], Operation::Acknowledge->allowedFields());
        $this->assertSame(['receipt'], Operation::Reject->allowedFields());
        $this->assertSame(['queue', 'wait_ms'], Operation::Wait->allowedFields());
        $this->assertSame(['queues', 'wait_ms'], Operation::WaitAny->allowedFields());
        $this->assertSame(16, Limits::MAX_WAIT_QUEUES);
    }

    public function testWaitBooleanReplyAndLocalBoundsPreserveSequence(): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control['id'];
                $op = $request->control['op'] ?? null;
                if ('wait' === $op) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => true]))->encode());
                    continue;
                }
                if ('send' === $op) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 7]))->encode());
                }
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        $this->assertTrue($client->wait('jobs', 0));
        try {
            $client->wait('jobs', -1);
            $this->fail('Negative wait must fail locally.');
        } catch (\InvalidArgumentException) {
        }
        try {
            $client->wait('jobs', Limits::MAX_WAIT_MILLISECONDS + 1);
            $this->fail('Oversized wait must fail locally.');
        } catch (\InvalidArgumentException) {
        }
        $this->assertSame(7, $client->send('jobs', 'after local wait validation'));
        $this->assertSame([1, 2], $seen);
    }

    public function testWaitAnyBooleanReplyAndLocalBoundsPreserveSequence(): void
    {
        $seen = [];
        $endpoint = $this->serve(static function (Socket $socket) use (&$seen): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            while (null !== ($request = Frame::read($socket, new TimeoutCancellation(3)))) {
                $seen[] = $request->control;
                $op = $request->control['op'] ?? null;
                if ('wait_any' === $op) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => true]))->encode());
                    continue;
                }
                if ('send' === $op) {
                    $socket->write((new Frame(['v' => Frame::VERSION, 'id' => $request->control['id'], 'ok' => true, 'result' => 9]))->encode());
                }
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        $this->assertTrue($client->waitAny(['jobs', 'other'], 0));
        $this->assertSame(['jobs', 'other'], $seen[0]['queues'] ?? null);
        foreach ([
            [[], 'WAIT_ANY requires at least one queue.'],
            [['jobs', 'jobs'], 'WAIT_ANY queue names must be unique.'],
            [array_fill(0, Limits::MAX_WAIT_QUEUES + 1, 'jobs'), \sprintf('WAIT_ANY accepts at most %d queues.', Limits::MAX_WAIT_QUEUES)],
            [['bad name'], 'Queue names must contain 1 to 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit.'],
            [[1], 'WAIT_ANY queue names must be strings.'],
        ] as [$queues, $message]) {
            try {
                $client->waitAny($queues, 0);
                $this->fail('Invalid WAIT_ANY input must fail locally.');
            } catch (\InvalidArgumentException $error) {
                $this->assertSame($message, $error->getMessage());
            }
        }
        try {
            $client->waitAny(['jobs'], -1);
            $this->fail('Negative WAIT_ANY timeout must fail locally.');
        } catch (\InvalidArgumentException) {
        }
        $this->assertSame(9, $client->send('jobs', 'after local wait_any validation'));
        $this->assertCount(2, $seen);
        $this->assertSame(1, $seen[0]['id'] ?? null);
        $this->assertSame(2, $seen[1]['id'] ?? null);
    }

    public function testWaitNonBooleanResultInvalidatesTheClient(): void
    {
        $endpoint = $this->serve(static function (Socket $socket): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write(self::helloReply());
            if (null !== Frame::read($socket, new TimeoutCancellation(3))) {
                $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 1, 'ok' => true, 'result' => 1]))->encode());
            }
        });
        $client = Client::connect($endpoint);
        $this->clients[] = $client;
        try {
            $client->wait('jobs', 0);
            $this->fail('A non-boolean WAIT result must invalidate the client.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
            $this->assertSame('WAIT result must be a boolean.', $error->getMessage());
        }
        try {
            $client->wait('jobs', 0);
            $this->fail('An invalidated client must reject further calls.');
        } catch (TransportException $error) {
            $this->assertStringContainsString('Client is closed', $error->getMessage());
        }
    }

    /**
     * @param array<string, mixed>|string $result
     */
    private function assertConnectFails(array|string $result, string $body, string $headers, string $reason): void
    {
        $endpoint = $this->serve(static function (Socket $socket) use ($result, $body, $headers): void {
            Frame::read($socket, new TimeoutCancellation(3));
            $socket->write((new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => $result], $body, $headers))->encode());
        });
        try {
            $client = Client::connect($endpoint);
            $this->clients[] = $client;
            $this->fail('A malformed hello must not be trusted.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
            $this->assertSame($reason, $error->getMessage());
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
