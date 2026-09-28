<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Amp\Cancellation;
use Amp\Socket\Socket;
use Ineersa\SqliteQueue\ProtocolException;

final readonly class Frame
{
    public const int VERSION = 1;
    public const int MAX_FRAME = 1_048_576;
    public const int MAX_CONTROL = 8192;
    public const int MAX_PAYLOAD = 1_040_000;

    /** @param array<string, mixed> $control */
    public function __construct(public array $control, public string $body = '', public string $headers = '')
    {
    }

    public function encode(): string
    {
        if (\strlen($this->body) + \strlen($this->headers) > self::MAX_PAYLOAD) {
            throw new ProtocolException('frame_too_large', 'Message exceeds the payload limit.');
        }
        $control = json_encode(array_replace($this->control, ['body_length' => \strlen($this->body), 'headers_length' => \strlen($this->headers)]), \JSON_THROW_ON_ERROR);
        $size = \strlen($control);
        $length = 4 + $size + \strlen($this->body) + \strlen($this->headers);
        if ($size > self::MAX_CONTROL || $length > self::MAX_FRAME) {
            throw new ProtocolException('frame_too_large', 'Frame exceeds the protocol limit.');
        }

        return pack('NN', $length, $size).$control.$this->body.$this->headers;
    }

    public static function read(Socket $socket, Cancellation $cancellation): ?self
    {
        $prefix = $socket->read($cancellation, 4);
        if (null === $prefix) {
            return null;
        }
        $prefix .= self::exact($socket, 4 - \strlen($prefix), $cancellation);
        $length = (int) unpack('Nlength', $prefix)['length'];
        if ($length < 4 || $length > self::MAX_FRAME) {
            throw new ProtocolException('frame_too_large', 'Invalid frame length.');
        }
        $controlLength = (int) unpack('Nlength', self::exact($socket, 4, $cancellation))['length'];
        if ($controlLength < 2 || $controlLength > self::MAX_CONTROL || $controlLength > $length - 4) {
            throw new ProtocolException('invalid_request', 'Invalid control length.');
        }
        try {
            $control = json_decode(self::exact($socket, $controlLength, $cancellation), true, 32, \JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            throw new ProtocolException('invalid_request', 'Invalid control JSON.');
        }
        if (!\is_array($control) || !\is_int($control['body_length'] ?? null) || !\is_int($control['headers_length'] ?? null)) {
            throw new ProtocolException('invalid_request', 'Missing payload lengths.');
        }
        $bodyLength = $control['body_length'];
        $headersLength = $control['headers_length'];
        if ($bodyLength < 0 || $headersLength < 0 || $bodyLength > self::MAX_PAYLOAD || $headersLength > self::MAX_PAYLOAD
            || $bodyLength + $headersLength > self::MAX_PAYLOAD || $bodyLength + $headersLength !== $length - 4 - $controlLength) {
            throw new ProtocolException('invalid_request', 'Payload lengths do not match frame.');
        }

        return new self($control, self::exact($socket, $bodyLength, $cancellation), self::exact($socket, $headersLength, $cancellation));
    }

    /** Socket writes have no cancellation argument; closing interrupts Amp's pending write. */
    public static function write(Socket $socket, string $bytes, Cancellation $cancellation): void
    {
        $cancellation->throwIfRequested();
        $subscription = $cancellation->subscribe(static fn () => $socket->close());
        try {
            $socket->write($bytes);
            $cancellation->throwIfRequested();
        } finally {
            $cancellation->unsubscribe($subscription);
        }
    }

    private static function exact(Socket $socket, int $length, Cancellation $cancellation): string
    {
        $bytes = '';
        while (\strlen($bytes) < $length) {
            $chunk = $socket->read($cancellation, $length - \strlen($bytes));
            if (null === $chunk) {
                throw new ProtocolException('invalid_request', 'Truncated frame.');
            }
            $bytes .= $chunk;
        }

        return $bytes;
    }
}
