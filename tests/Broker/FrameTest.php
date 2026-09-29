<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\ProtocolException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\createSocketPair;

final class FrameTest extends TestCase
{
    /** @return iterable<string, array{string, string}> */
    public static function payloads(): iterable
    {
        yield 'empty' => ['', ''];
        yield 'body only' => ['body', ''];
        yield 'headers only' => ['', 'headers'];
        yield 'both' => ['body', 'headers'];
        yield 'binary' => ["\x00\xff\x01body\x00", "\x80\x7f\x00headers\xff"];
        yield 'large' => [str_repeat("\x00\xff", 4096), str_repeat('h', 4096)];
    }

    #[DataProvider('payloads')]
    public function testRoundTripsBinaryAndEmptyPayloadsAcrossPartialIo(string $body, string $headers): void
    {
        $frame = new Frame(['v' => 1, 'id' => 7, 'ok' => true, 'result' => null], $body, $headers);
        $decoded = $this->exchange($frame->encode(), 4);
        $this->assertInstanceOf(Frame::class, $decoded);
        $this->assertSame(7, $decoded->control['id']);
        $this->assertSame(\strlen($body), $decoded->control['body_length']);
        $this->assertSame(\strlen($headers), $decoded->control['headers_length']);
        $this->assertSame(\strlen($body) + \strlen($headers), \strlen($decoded->body) + \strlen($decoded->headers));
        $this->assertTrue($body === $decoded->body, 'Body must survive framing byte for byte.');
        $this->assertTrue($headers === $decoded->headers, 'Headers must survive framing byte for byte.');
    }

    public function testReadReturnsNullOnCleanEof(): void
    {
        $this->assertNull($this->exchange(''));
        $this->assertNull($this->exchange('', 4));
    }

    /** @return iterable<string, array{string}> */
    public static function truncatedPrefixes(): iterable
    {
        yield 'one byte' => ["\x00"];
        yield 'two bytes' => ["\x00\x00"];
        yield 'three bytes' => ["\x00\x00\x10"];
    }

    #[DataProvider('truncatedPrefixes')]
    public function testTruncatedPrefixFails(string $bytes): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Truncated frame.');
        $this->exchange($bytes);
    }

    public function testTruncatedControlFails(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Truncated frame.');
        $this->exchange($this->handCrafted(20, 8).'{"v');
    }

    public function testTruncatedBodyFails(): void
    {
        $json = '{"body_length":5,"headers_length":0}';
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Truncated frame.');
        $this->exchange($this->handCrafted(4 + \strlen($json) + 5, \strlen($json), $json).'ab');
    }

    public function testTruncatedHeadersFails(): void
    {
        $json = '{"body_length":2,"headers_length":4}';
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Truncated frame.');
        $this->exchange($this->handCrafted(4 + \strlen($json) + 6, \strlen($json), $json).'abx');
    }

    /** @return iterable<string, array{int}> */
    public static function oversizedPrefixes(): iterable
    {
        yield 'zero' => [0];
        yield 'below minimum' => [3];
        yield 'above maximum' => [Frame::MAX_FRAME + 1];
    }

    #[DataProvider('oversizedPrefixes')]
    public function testOversizedPrefixIsRejectedWithoutReadingBody(int $length): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid frame length.');
        $this->exchange(pack('N', $length));
    }

    public function testOversizedPrefixReportsFrameTooLarge(): void
    {
        try {
            $this->exchange(pack('N', Frame::MAX_FRAME + 1).pack('N', 8));
            $this->fail('Oversized frame accepted.');
        } catch (ProtocolException $error) {
            $this->assertSame('frame_too_large', $error->errorCode->value);
        }
    }

    /** @return iterable<string, array{int, int}> */
    public static function invalidControlLengths(): iterable
    {
        yield 'zero' => [0, 100];
        yield 'one' => [1, 100];
        yield 'above maximum' => [Frame::MAX_CONTROL + 1, 9000];
        yield 'longer than frame' => [100, 20];
    }

    #[DataProvider('invalidControlLengths')]
    public function testInvalidControlLengthFails(int $controlLength, int $frameLength): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid control length.');
        $this->exchange($this->handCrafted($frameLength, $controlLength));
    }

    public function testInvalidControlJsonFails(): void
    {
        $json = '{"v';
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid control JSON.');
        $this->exchange($this->handCrafted(4 + \strlen($json), \strlen($json), $json));
    }

    /** @return iterable<string, array{string}> */
    public static function invalidPayloadLengths(): iterable
    {
        yield 'missing body length' => ['{"headers_length":0}'];
        yield 'missing header length' => ['{"body_length":0}'];
        yield 'non-integer' => ['{"body_length":"0","headers_length":0}'];
        yield 'negative' => ['{"body_length":-1,"headers_length":0}'];
        yield 'mismatched sum' => ['{"body_length":1,"headers_length":1}'];
        yield 'beyond payload limit' => ['{"body_length":'.(Frame::MAX_PAYLOAD + 1).',"headers_length":0}'];
    }

    #[DataProvider('invalidPayloadLengths')]
    public function testInvalidPayloadLengthsFailWithoutReadingDeclaredBody(string $json): void
    {
        $this->expectException(ProtocolException::class);
        $this->exchange($this->handCrafted(4 + \strlen($json), \strlen($json), $json));
    }

    public function testEncodeRejectsPayloadAndControlBeyondLimits(): void
    {
        try {
            (new Frame([], str_repeat('x', Frame::MAX_PAYLOAD + 1)))->encode();
            $this->fail('Oversized payload accepted.');
        } catch (ProtocolException $error) {
            $this->assertSame('frame_too_large', $error->errorCode->value);
        }
        try {
            (new Frame(['padding' => str_repeat('y', Frame::MAX_CONTROL)]))->encode();
            $this->fail('Oversized control accepted.');
        } catch (ProtocolException $error) {
            $this->assertSame('frame_too_large', $error->errorCode->value);
        }
    }

    public function testEncodeAcceptsBoundaryPayload(): void
    {
        $body = str_repeat("\x00\xf0", intdiv(Frame::MAX_PAYLOAD, 2));
        $encoded = (new Frame([], $body))->encode();
        $this->assertLessThanOrEqual(Frame::MAX_FRAME, \strlen($encoded));
        $this->assertGreaterThan(Frame::MAX_PAYLOAD, \strlen($encoded));
        $decoded = $this->exchange($encoded);
        $this->assertInstanceOf(Frame::class, $decoded);
        $this->assertSame(\strlen($body), \strlen($decoded->body));
        $this->assertTrue($body === $decoded->body);
    }

    public function testWriteDoesNotTouchSocketWhenCancellationAlreadyRequested(): void
    {
        [$writer, $reader] = createSocketPair();
        $cancellation = new DeferredCancellation();
        $cancellation->cancel();
        try {
            Frame::write($writer, 'payload', $cancellation->getCancellation());
            $this->fail('Cancelled write proceeded.');
        } catch (\Amp\CancelledException) {
            $this->assertFalse($writer->isClosed());
        } finally {
            $writer->close();
            $reader->close();
        }
    }

    private function handCrafted(int $frameLength, int $controlLength, string $control = ''): string
    {
        return pack('NN', $frameLength, $controlLength).$control;
    }

    private function exchange(string $bytes, int $chunkSize = 8192, bool $eof = true): ?Frame
    {
        [$writer, $reader] = createSocketPair($chunkSize);
        $writing = async(static function () use ($writer, $bytes, $eof): void {
            Frame::write($writer, $bytes, new TimeoutCancellation(2));
            if ($eof) {
                $writer->end();
            }
        });
        try {
            return Frame::read($reader, new TimeoutCancellation(2));
        } finally {
            try {
                $writing->await(new TimeoutCancellation(2));
            } catch (\Throwable) {
                // A pending write is irrelevant to the read assertion and is released by closing below.
            }
            $writer->close();
            $reader->close();
        }
    }
}
