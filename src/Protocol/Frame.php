<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Amp\Cancellation;
use Amp\Socket\Socket;
use Ineersa\SqliteQueue\ProtocolException;

final readonly class Frame
{
    public const int VERSION = 1;
    public const int MAX_FRAME = Limits::MAX_FRAME;
    public const int MAX_CONTROL = Limits::MAX_CONTROL;
    public const int MAX_PAYLOAD = Limits::MAX_PAYLOAD;

    /** @param array<string, mixed> $control */
    public function __construct(public array $control, public string $body = '', public string $headers = '')
    {
    }

    public function encode(): string
    {
        $bodyLength = \strlen($this->body);
        $headersLength = \strlen($this->headers);
        if ($bodyLength + $headersLength > Limits::MAX_PAYLOAD) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Message exceeds the payload limit.');
        }
        $control = json_encode(array_replace($this->control, ['body_length' => $bodyLength, 'headers_length' => $headersLength]), \JSON_THROW_ON_ERROR);
        $controlLength = \strlen($control);
        $length = Limits::CONTROL_LENGTH_BYTES + $controlLength + $bodyLength + $headersLength;
        if ($controlLength > Limits::MAX_CONTROL || $length > Limits::MAX_FRAME) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Frame exceeds the protocol limit.');
        }

        return pack('NN', $length, $controlLength).$control.$this->body.$this->headers;
    }

    public static function read(Socket $socket, Cancellation $cancellation): ?self
    {
        $prefix = $socket->read($cancellation, Limits::LENGTH_PREFIX_BYTES);
        if (null === $prefix) {
            return null;
        }
        $prefix .= self::exact($socket, Limits::LENGTH_PREFIX_BYTES - \strlen($prefix), $cancellation);
        $length = (int) unpack('Nlength', $prefix)['length'];
        if (!self::isFrameLength($length)) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Invalid frame length.');
        }
        $controlLength = (int) unpack('Nlength', self::exact($socket, Limits::CONTROL_LENGTH_BYTES, $cancellation))['length'];
        if (!self::isControlLength($controlLength, $length)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid control length.');
        }
        try {
            $control = json_decode(self::exact($socket, $controlLength, $cancellation), true, Limits::JSON_DEPTH, \JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid control JSON.');
        }
        if (!\is_array($control) || !\is_int($control['body_length'] ?? null) || !\is_int($control['headers_length'] ?? null)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Missing payload lengths.');
        }
        $bodyLength = $control['body_length'];
        $headersLength = $control['headers_length'];
        if (!self::isPayloadLengths($bodyLength, $headersLength, $length, $controlLength)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Payload lengths do not match frame.');
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

    private static function isFrameLength(int $length): bool
    {
        return $length >= Limits::CONTROL_LENGTH_BYTES && $length <= Limits::MAX_FRAME;
    }

    private static function isControlLength(int $controlLength, int $length): bool
    {
        return $controlLength >= Limits::MIN_CONTROL_BYTES
            && $controlLength <= Limits::MAX_CONTROL
            && $controlLength <= $length - Limits::CONTROL_LENGTH_BYTES;
    }

    private static function isPayloadLengths(int $bodyLength, int $headersLength, int $length, int $controlLength): bool
    {
        if ($bodyLength < 0 || $headersLength < 0) {
            return false;
        }

        if ($bodyLength > Limits::MAX_PAYLOAD || $headersLength > Limits::MAX_PAYLOAD) {
            return false;
        }

        if ($bodyLength + $headersLength > Limits::MAX_PAYLOAD) {
            return false;
        }

        return $bodyLength + $headersLength === $length - Limits::CONTROL_LENGTH_BYTES - $controlLength;
    }

    private static function exact(Socket $socket, int $length, Cancellation $cancellation): string
    {
        $bytes = '';
        $remaining = $length;
        while ($remaining > 0) {
            $chunk = $socket->read($cancellation, $remaining);
            if (null === $chunk) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Truncated frame.');
            }
            $bytes .= $chunk;
            $remaining -= \strlen($chunk);
        }

        return $bytes;
    }
}
