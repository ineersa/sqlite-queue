<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

/**
 * Configurable broker resource bounds for protocol v1.
 *
 * Wire framing still uses the fixed length-prefix, control-length, control-byte,
 * wait, and wait-queue constants on Limits. maxPayload and maxConnections are the
 * only operator-tunable values; maxFrame is derived so a worst-case control block
 * still fits beside the configured payload.
 */
final readonly class ProtocolLimits
{
    /** Unsigned 32-bit maximum for the outer length prefix. */
    public const int MAX_UINT32 = 4_294_967_295;

    /** Covers serialized scalar fields, column names, BLOB wrappers, and operation outcome. */
    public const int RESULT_METADATA_BYTES = 8_192;

    /**
     * Extra frame bytes beyond CONTROL_LENGTH + MAX_CONTROL + payload.
     *
     * Preserves the published default headroom (MAX_FRAME - MAX_PAYLOAD - 4 - 8192 = 380)
     * for length fields that framing adds to the control object.
     */
    public const int FRAME_HEADROOM = Limits::MAX_FRAME - Limits::MAX_PAYLOAD - Limits::CONTROL_LENGTH_BYTES - Limits::MAX_CONTROL;

    public int $maxFrame;

    /**
     * @param int $maxPayload     Maximum combined body plus headers bytes
     * @param int $maxConnections Maximum simultaneous broker client sessions, including notification sockets
     */
    public function __construct(
        public int $maxPayload = Limits::MAX_PAYLOAD,
        public int $maxConnections = Limits::MAX_CONNECTIONS,
    ) {
        if ($maxPayload < 1) {
            throw new \InvalidArgumentException('Maximum payload must be a positive byte count.');
        }
        if ($maxConnections < 1) {
            throw new \InvalidArgumentException('Maximum connections must be a positive integer.');
        }

        // CONTROL_LENGTH_BYTES + MAX_CONTROL always accompany a payload. Reject
        // configurations whose framed length cannot fit in the uint32 prefix.
        $overhead = Limits::CONTROL_LENGTH_BYTES + Limits::MAX_CONTROL + self::FRAME_HEADROOM;
        if ($maxPayload > self::MAX_UINT32 - $overhead) {
            throw new \InvalidArgumentException('Maximum payload exceeds the framed uint32 length budget.');
        }
        $this->maxFrame = $maxPayload + $overhead;
    }

    public static function defaults(): self
    {
        return new self();
    }

    public function maximumResultBytes(): int
    {
        if ($this->maxPayload > \PHP_INT_MAX - self::RESULT_METADATA_BYTES) {
            throw new \InvalidArgumentException('Maximum payload exceeds the worker result-byte budget.');
        }

        return $this->maxPayload + self::RESULT_METADATA_BYTES;
    }

    public function assertPayloadBounds(string $body, string $headers): void
    {
        $bodyLength = \strlen($body);
        $headersLength = \strlen($headers);
        if ($bodyLength > $this->maxPayload
            || $headersLength > $this->maxPayload
            || $bodyLength + $headersLength > $this->maxPayload
        ) {
            throw new \InvalidArgumentException('Payload exceeds the supported frame budget.');
        }
    }
}
