<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * The benchmark message.
 *
 * Bytes are generated deterministically from the correlation id, so any process can
 * regenerate and verify them. The content is not valid UTF-8 on purpose: the round trip
 * must survive arbitrary bytes, not readable text.
 */
final class BenchMessage
{
    public function __construct(
        public readonly string $corrId,
        public readonly string $queue,
        public readonly string $digest,
        public readonly string $bytes,
        public readonly int $size,
    ) {
    }

    public static function generate(string $corrId, string $queue, int $size): self
    {
        $bytes = self::payload($corrId, $size);

        return new self($corrId, $queue, hash('sha256', $bytes), $bytes, $size);
    }

    public static function payload(string $corrId, int $size): string
    {
        if ($size <= 0) {
            return '';
        }

        $payload = '';
        $chunk = 0;

        while (\strlen($payload) < $size) {
            $payload .= hash('sha256', $corrId.':'.$chunk, true);
            ++$chunk;
        }

        return substr($payload, 0, $size);
    }

    public function verify(): bool
    {
        return $this->size === \strlen($this->bytes) && hash('sha256', $this->bytes) === $this->digest;
    }

    /**
     * Rebuilds the payload from the correlation id and compares it with the received bytes.
     *
     * This is stronger than verifying the received digest against the received bytes: it
     * proves the payload that arrived is the payload the publisher generated.
     */
    public function matchesRegeneratedPayload(): bool
    {
        $expected = self::payload($this->corrId, $this->size);

        return $this->size === \strlen($this->bytes)
            && hash_equals(hash('sha256', $expected), $this->digest)
            && hash_equals($expected, $this->bytes);
    }
}
