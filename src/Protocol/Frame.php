<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Amp\Cancellation;
use Amp\Socket\Socket;

final readonly class Frame
{
    public const int VERSION = 1;
    /** Default frame budget for the published 1 MiB configuration. */
    public const int MAX_FRAME = Limits::MAX_FRAME;
    public const int MAX_CONTROL = Limits::MAX_CONTROL;
    /** Default combined body and headers budget. */
    public const int MAX_PAYLOAD = Limits::MAX_PAYLOAD;

    /** @param array<string, mixed> $control */
    public function __construct(public array $control, public string $body = '', public string $headers = '')
    {
    }

    public function encode(?ProtocolLimits $limits = null): string
    {
        $limits ??= ProtocolLimits::defaults();
        $bodyLength = \strlen($this->body);
        $headersLength = \strlen($this->headers);
        if ($bodyLength + $headersLength > $limits->maxPayload) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Message exceeds the payload limit.');
        }
        $control = json_encode(array_replace($this->control, [
            ControlField::BodyLength->value => $bodyLength,
            ControlField::HeadersLength->value => $headersLength,
        ]), \JSON_THROW_ON_ERROR);
        $controlLength = \strlen($control);
        $length = Limits::CONTROL_LENGTH_BYTES + $controlLength + $bodyLength + $headersLength;
        if ($controlLength > Limits::MAX_CONTROL || $length > $limits->maxFrame) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Frame exceeds the protocol limit.');
        }

        return pack('NN', $length, $controlLength).$control.$this->body.$this->headers;
    }

    public static function read(Socket $socket, Cancellation $cancellation, ?ProtocolLimits $limits = null): ?self
    {
        return self::readUsing(static fn (int $length): ?string => $socket->read($cancellation, $length), $limits);
    }

    /** @param \Closure(int): ?string $read reads at most the requested bytes, or null at EOF */
    public static function readUsing(\Closure $read, ?ProtocolLimits $limits = null): ?self
    {
        $prefix = $read(Limits::LENGTH_PREFIX_BYTES);
        if (null === $prefix) {
            return null;
        }

        return self::complete($prefix, $read, $limits);
    }

    /** Completes a frame after the first one to four length-prefix bytes have arrived. */
    public static function readAfterPrefix(Socket $socket, string $prefix, Cancellation $cancellation, ?ProtocolLimits $limits = null): self
    {
        return self::complete($prefix, static fn (int $length): ?string => $socket->read($cancellation, $length), $limits);
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

    /** @param \Closure(int): ?string $read */
    private static function complete(string $prefix, \Closure $read, ?ProtocolLimits $limits): self
    {
        $limits ??= ProtocolLimits::defaults();
        $prefixLength = \strlen($prefix);
        if (0 === $prefixLength) {
            throw new \InvalidArgumentException('Frame prefix must not be empty.');
        }
        if ($prefixLength > Limits::LENGTH_PREFIX_BYTES) {
            throw new \InvalidArgumentException('Frame prefix exceeds the length-prefix size.');
        }
        $prefix .= self::exact($read, Limits::LENGTH_PREFIX_BYTES - $prefixLength);
        $length = (int) unpack('Nlength', $prefix)['length'];
        if (!self::isFrameLength($length, $limits)) {
            throw new ProtocolException(ErrorCode::FrameTooLarge, 'Invalid frame length.');
        }
        $controlLength = (int) unpack('Nlength', self::exact($read, Limits::CONTROL_LENGTH_BYTES))['length'];
        if (!self::isControlLength($controlLength, $length)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid control length.');
        }
        try {
            $control = json_decode(self::exact($read, $controlLength), true, Limits::JSON_DEPTH, \JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid control JSON.');
        }
        if (
            !\is_array($control)
            || !\is_int($control[ControlField::BodyLength->value] ?? null)
            || !\is_int($control[ControlField::HeadersLength->value] ?? null)
        ) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Missing payload lengths.');
        }
        $bodyLength = $control[ControlField::BodyLength->value];
        $headersLength = $control[ControlField::HeadersLength->value];
        if (!self::isPayloadLengths($bodyLength, $headersLength, $length, $controlLength, $limits)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Payload lengths do not match frame.');
        }

        return new self($control, self::exact($read, $bodyLength), self::exact($read, $headersLength));
    }

    private static function isFrameLength(int $length, ProtocolLimits $limits): bool
    {
        return $length >= Limits::CONTROL_LENGTH_BYTES && $length <= $limits->maxFrame;
    }

    private static function isControlLength(int $controlLength, int $length): bool
    {
        return $controlLength >= Limits::MIN_CONTROL_BYTES
            && $controlLength <= Limits::MAX_CONTROL
            && $controlLength <= $length - Limits::CONTROL_LENGTH_BYTES;
    }

    private static function isPayloadLengths(int $bodyLength, int $headersLength, int $length, int $controlLength, ProtocolLimits $limits): bool
    {
        if ($bodyLength < 0 || $headersLength < 0) {
            return false;
        }

        if ($bodyLength > $limits->maxPayload || $headersLength > $limits->maxPayload) {
            return false;
        }

        if ($bodyLength + $headersLength > $limits->maxPayload) {
            return false;
        }

        return $bodyLength + $headersLength === $length - Limits::CONTROL_LENGTH_BYTES - $controlLength;
    }

    /** @param \Closure(int): ?string $read */
    private static function exact(\Closure $read, int $length): string
    {
        $bytes = '';
        $remaining = $length;
        while ($remaining > 0) {
            $chunk = $read($remaining);
            if (null === $chunk) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Truncated frame.');
            }
            $bytes .= $chunk;
            $remaining -= \strlen($chunk);
        }

        return $bytes;
    }
}
