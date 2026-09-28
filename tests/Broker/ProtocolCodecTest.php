<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Ineersa\SqliteQueue\Delivery;
use Ineersa\SqliteQueue\Protocol\AcknowledgeRequest;
use Ineersa\SqliteQueue\Protocol\EmptyReceiveResponse;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\FailedResponse;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\HelloRequest;
use Ineersa\SqliteQueue\Protocol\HelloResponse;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\ReceivedResponse;
use Ineersa\SqliteQueue\Protocol\ReceiveRequest;
use Ineersa\SqliteQueue\Protocol\RejectRequest;
use Ineersa\SqliteQueue\Protocol\Request;
use Ineersa\SqliteQueue\Protocol\RequestCodec;
use Ineersa\SqliteQueue\Protocol\Response;
use Ineersa\SqliteQueue\Protocol\ResponseCodec;
use Ineersa\SqliteQueue\Protocol\SendRequest;
use Ineersa\SqliteQueue\Protocol\SentResponse;
use Ineersa\SqliteQueue\Protocol\SettledResponse;
use Ineersa\SqliteQueue\ProtocolException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ProtocolCodecTest extends TestCase
{
    /** @return iterable<string, array{Request, int}> */
    public static function requests(): iterable
    {
        yield 'hello' => [new HelloRequest(0), 0];
        yield 'send' => [new SendRequest(1, 'jobs', 1500, 'body', 'headers'), 1];
        yield 'send binary empty delay' => [new SendRequest(2, 'q', 0, "\x00\xff\x01", "\x80\x7f"), 2];
        yield 'receive' => [new ReceiveRequest(3, 'jobs'), 3];
        yield 'acknowledge' => [new AcknowledgeRequest(4, '7:token'), 4];
        yield 'reject' => [new RejectRequest(5, '9:token'), 5];
    }

    #[DataProvider('requests')]
    public function testRequestRoundTrip(Request $request, int $expected): void
    {
        $this->assertEquals($request, RequestCodec::decode(RequestCodec::encode($request), $expected));
    }

    public function testRequestDecodeAcceptsFramingLengths(): void
    {
        $frame = new Frame(
            ['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0, 'body_length' => 4, 'headers_length' => 0],
            'body',
        );

        $this->assertEquals(new SendRequest(1, 'jobs', 0, 'body', ''), RequestCodec::decode($frame, 1));
    }

    public function testRequestEncodeUsesExactV1WireKeys(): void
    {
        $frame = RequestCodec::encode(new SendRequest(4, 'jobs', 1500, 'b', 'h'));

        $this->assertSame(['v' => 1, 'id' => 4, 'op' => 'send', 'queue' => 'jobs', 'delay' => 1500], $frame->control);
        $this->assertSame('b', $frame->body);
        $this->assertSame('h', $frame->headers);
        $this->assertSame(['v' => 1, 'id' => 0, 'op' => 'hello'], RequestCodec::encode(new HelloRequest(0))->control);
    }

    /** @return iterable<string, array{array<string, mixed>, string, string, int, string}> */
    public static function malformedRequests(): iterable
    {
        yield 'wrong version' => [['v' => 2, 'id' => 0, 'op' => 'hello'], '', '', 0, 'unsupported_protocol_version'];
        yield 'missing version' => [['id' => 0, 'op' => 'hello'], '', '', 0, 'unsupported_protocol_version'];
        yield 'id mismatch' => [['v' => 1, 'id' => 1, 'op' => 'hello'], '', '', 0, 'invalid_request'];
        yield 'non-integer id' => [['v' => 1, 'id' => '0', 'op' => 'hello'], '', '', 0, 'invalid_request'];
        yield 'missing op' => [['v' => 1, 'id' => 0], '', '', 0, 'invalid_request'];
        yield 'non-string op' => [['v' => 1, 'id' => 0, 'op' => 5], '', '', 0, 'invalid_request'];
        yield 'unknown op' => [['v' => 1, 'id' => 1, 'op' => 'wait'], '', '', 1, 'invalid_request'];
        yield 'extra field on hello' => [['v' => 1, 'id' => 0, 'op' => 'hello', 'queue' => 'jobs'], '', '', 0, 'invalid_request'];
        yield 'extra field on send' => [['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0, 'receipt' => 'x'], '', '', 1, 'invalid_request'];
        yield 'send before handshake' => [['v' => 1, 'id' => 0, 'op' => 'send', 'queue' => 'jobs', 'delay' => 0], '', '', 0, 'invalid_request'];
        yield 'receive before handshake' => [['v' => 1, 'id' => 0, 'op' => 'receive', 'queue' => 'jobs'], '', '', 0, 'invalid_request'];
        yield 'hello after handshake' => [['v' => 1, 'id' => 1, 'op' => 'hello'], '', '', 1, 'invalid_request'];
        yield 'hello with body' => [['v' => 1, 'id' => 0, 'op' => 'hello'], 'body', '', 0, 'invalid_request'];
        yield 'receive with body' => [['v' => 1, 'id' => 1, 'op' => 'receive', 'queue' => 'jobs'], 'body', '', 1, 'invalid_request'];
        yield 'acknowledge with headers' => [['v' => 1, 'id' => 1, 'op' => 'acknowledge', 'receipt' => 'r'], '', 'h', 1, 'invalid_request'];
        yield 'missing queue' => [['v' => 1, 'id' => 1, 'op' => 'send', 'delay' => 0], '', '', 1, 'invalid_queue_name'];
        yield 'non-string queue' => [['v' => 1, 'id' => 1, 'op' => 'receive', 'queue' => 5], '', '', 1, 'invalid_queue_name'];
        yield 'empty queue' => [['v' => 1, 'id' => 1, 'op' => 'receive', 'queue' => ''], '', '', 1, 'invalid_queue_name'];
        yield 'queue leading dash' => [['v' => 1, 'id' => 1, 'op' => 'receive', 'queue' => '-jobs'], '', '', 1, 'invalid_queue_name'];
        yield 'queue too long' => [['v' => 1, 'id' => 1, 'op' => 'receive', 'queue' => str_repeat('a', 256)], '', '', 1, 'invalid_queue_name'];
        yield 'missing delay' => [['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs'], '', '', 1, 'invalid_request'];
        yield 'negative delay' => [['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => -1], '', '', 1, 'invalid_request'];
        yield 'string delay' => [['v' => 1, 'id' => 1, 'op' => 'send', 'queue' => 'jobs', 'delay' => '5'], '', '', 1, 'invalid_request'];
        yield 'missing receipt' => [['v' => 1, 'id' => 1, 'op' => 'acknowledge'], '', '', 1, 'invalid_request'];
        yield 'non-string receipt' => [['v' => 1, 'id' => 1, 'op' => 'reject', 'receipt' => 7], '', '', 1, 'invalid_request'];
    }

    /**
     * @param array<string, mixed> $control
     */
    #[DataProvider('malformedRequests')]
    public function testMalformedRequestFailsWithExpectedCode(array $control, string $body, string $headers, int $expected, string $code): void
    {
        try {
            RequestCodec::decode(new Frame($control, $body, $headers), $expected);
            $this->fail('Malformed request must raise ProtocolException.');
        } catch (ProtocolException $error) {
            $this->assertSame($code, $error->errorCode->value);
        }
    }

    /** @return iterable<string, array{Response, Request}> */
    public static function responses(): iterable
    {
        yield 'hello' => [new HelloResponse(0, Limits::MAX_PAYLOAD), new HelloRequest(0)];
        yield 'sent' => [new SentResponse(1, 7), new SendRequest(1, 'jobs', 0, '', '')];
        yield 'received binary' => [
            new ReceivedResponse(2, new Delivery(7, 'jobs', "\x00\xff", "\x80", 'r', 10, 20)),
            new ReceiveRequest(2, 'jobs'),
        ];
        yield 'empty receive' => [new EmptyReceiveResponse(3), new ReceiveRequest(3, 'jobs')];
        yield 'settled acknowledge' => [new SettledResponse(4), new AcknowledgeRequest(4, 'r')];
        yield 'settled reject' => [new SettledResponse(5), new RejectRequest(5, 'r')];
    }

    #[DataProvider('responses')]
    public function testResponseRoundTrip(Response $response, Request $request): void
    {
        $this->assertEquals($response, ResponseCodec::decode(ResponseCodec::encode($response), $request));
    }

    /** @return iterable<string, array{ErrorCode}> */
    public static function errorCodes(): iterable
    {
        foreach (ErrorCode::cases() as $code) {
            yield $code->value => [$code];
        }
    }

    #[DataProvider('errorCodes')]
    public function testFailureRoundTripsEveryKnownCode(ErrorCode $code): void
    {
        $response = new FailedResponse(6, $code);

        $this->assertEquals($response, ResponseCodec::decode(ResponseCodec::encode($response), new SendRequest(6, 'jobs', 0, '', '')));
    }

    public function testResponseEncodeUsesExactV1WireKeys(): void
    {
        $failed = ResponseCodec::encode(new FailedResponse(3, ErrorCode::StaleReceipt));

        $this->assertSame(['v' => 1, 'id' => 3, 'ok' => false, 'error' => ['code' => 'stale_receipt']], $failed->control);
        $this->assertSame('', $failed->body);
        $this->assertSame('', $failed->headers);

        $received = ResponseCodec::encode(new ReceivedResponse(2, new Delivery(7, 'jobs', 'b', 'h', 'r', 10, 20)));

        $this->assertSame(
            ['v' => 1, 'id' => 2, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 10, 'reserved_until' => 20]],
            $received->control,
        );
        $this->assertSame('b', $received->body);
        $this->assertSame('h', $received->headers);
    }

    /** @return iterable<string, array{array<string, mixed>, string, string, Request}> */
    public static function malformedResponses(): iterable
    {
        $send = new SendRequest(1, 'jobs', 0, '', '');
        $receive = new ReceiveRequest(1, 'jobs');
        $hello = new HelloRequest(0);
        $acknowledge = new AcknowledgeRequest(1, 'r');

        yield 'wrong version' => [['v' => 2, 'id' => 1, 'ok' => true, 'result' => 7], '', '', $send];
        yield 'id mismatch' => [['v' => 1, 'id' => 2, 'ok' => true, 'result' => 7], '', '', $send];
        yield 'non-integer id' => [['v' => 1, 'id' => '1', 'ok' => true, 'result' => 7], '', '', $send];
        yield 'missing ok' => [['v' => 1, 'id' => 1, 'result' => 7], '', '', $send];
        yield 'non-boolean ok' => [['v' => 1, 'id' => 1, 'ok' => 1, 'result' => 7], '', '', $send];
        yield 'success without result' => [['v' => 1, 'id' => 1, 'ok' => true], '', '', $send];
        yield 'unknown error code' => [['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'nope']], '', '', $send];
        yield 'missing error' => [['v' => 1, 'id' => 1, 'ok' => false], '', '', $send];
        yield 'non-array error' => [['v' => 1, 'id' => 1, 'ok' => false, 'error' => 'x'], '', '', $send];
        yield 'non-string error code' => [['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 5]], '', '', $send];
        yield 'failure with body' => [['v' => 1, 'id' => 1, 'ok' => false, 'error' => ['code' => 'stale_receipt']], 'b', '', $send];
        yield 'failure with result' => [['v' => 1, 'id' => 1, 'ok' => false, 'result' => null, 'error' => ['code' => 'stale_receipt']], '', '', $send];
        yield 'sent id zero' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => 0], '', '', $send];
        yield 'sent id string' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => '7'], '', '', $send];
        yield 'sent null result' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => null], '', '', $send];
        yield 'sent with body' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => 7], 'b', '', $send];
        yield 'array result for send' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7]], '', '', $send];
        yield 'hello wrong max payload' => [['v' => 1, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => 1]], '', '', $hello];
        yield 'hello string result' => [['v' => 1, 'id' => 0, 'ok' => true, 'result' => 'x'], '', '', $hello];
        yield 'hello with headers' => [['v' => 1, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => Limits::MAX_PAYLOAD]], '', 'h', $hello];
        yield 'delivery wrong queue' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'other', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]], '', '', $receive];
        yield 'delivery id zero' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 0, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1, 'reserved_until' => 2]], '', '', $receive];
        yield 'delivery integer receipt' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 5, 'available_at' => 1, 'reserved_until' => 2]], '', '', $receive];
        yield 'delivery string available_at' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => '1', 'reserved_until' => 2]], '', '', $receive];
        yield 'delivery missing reserved_until' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => ['id' => 7, 'queue' => 'jobs', 'receipt' => 'r', 'available_at' => 1]], '', '', $receive];
        yield 'integer result for receive' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => 7], '', '', $receive];
        yield 'empty receive with body' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => null], 'b', '', $receive];
        yield 'settle with result' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => 5], '', '', $acknowledge];
        yield 'settle with body' => [['v' => 1, 'id' => 1, 'ok' => true, 'result' => null], 'b', '', $acknowledge];
    }

    /**
     * @param array<string, mixed> $control
     */
    #[DataProvider('malformedResponses')]
    public function testMalformedResponseFailsAsInvalidRequest(array $control, string $body, string $headers, Request $request): void
    {
        try {
            ResponseCodec::decode(new Frame($control, $body, $headers), $request);
            $this->fail('Malformed response must raise ProtocolException.');
        } catch (ProtocolException $error) {
            $this->assertSame(ErrorCode::InvalidRequest, $error->errorCode);
        }
    }
}
