<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

/** Shared resource bounds for protocol v1. */
final class Limits
{
    /** Maximum simultaneous broker client sessions, including notification sockets. */
    public const int MAX_CONNECTIONS = 64;

    /** Bytes of the outer big-endian length prefix. */
    public const int LENGTH_PREFIX_BYTES = 4;

    /** Bytes of the big-endian control-length field inside the frame. */
    public const int CONTROL_LENGTH_BYTES = 4;

    /** Maximum buffered frame body: 1 MiB (1024 * 1024 bytes), excluding the length prefix. */
    public const int MAX_FRAME = 1_048_576;

    /** Maximum UTF-8 JSON control block: 8 KiB (8 * 1024 bytes). */
    public const int MAX_CONTROL = 8192;

    /**
     * Maximum combined body plus headers payload.
     *
     * A worst-case frame holds the 4-byte control-length field plus an 8 KiB control block, so
     * 1_040_000 bytes of payload always fit inside the 1 MiB frame budget with a few hundred
     * bytes of headroom for the length fields framing adds to the control object.
     */
    public const int MAX_PAYLOAD = 1_040_000;

    /** Smallest possible control block: an empty JSON object (2 bytes, "{}"). */
    public const int MIN_CONTROL_BYTES = 2;

    /** Maximum nesting accepted when decoding JSON control. */
    public const int JSON_DEPTH = 32;

    /** Maximum WAIT bound in milliseconds. Zero is an immediate readiness probe. */
    public const int MAX_WAIT_MILLISECONDS = 30_000;

    private function __construct()
    {
    }
}
