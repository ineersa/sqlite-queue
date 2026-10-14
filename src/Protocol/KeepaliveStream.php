<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Amp\ByteStream\ResourceStream;
use Ineersa\SqliteQueue\Exception\TransportException;

/** Borrows the idle operations socket without switching fibers inside a native signal callback. */
final readonly class KeepaliveStream
{
    private float $deadline;

    public function __construct(private ResourceStream $stream, float $timeoutSeconds)
    {
        if ($timeoutSeconds <= 0 || !is_finite($timeoutSeconds)) {
            throw new \InvalidArgumentException('Keepalive timeout must be positive finite seconds.');
        }
        $this->deadline = hrtime(true) / 1_000_000_000 + $timeoutSeconds;
    }

    public function read(int $maximumBytes): ?string
    {
        while (true) {
            $this->wait(reading: true);
            $resource = $this->resource();
            $bytes = @fread($resource, $maximumBytes);
            if (false === $bytes) {
                throw new TransportException('Could not read the keepalive confirmation.');
            }
            if ('' !== $bytes) {
                return $bytes;
            }
            if (feof($resource)) {
                return null;
            }
        }
    }

    public function write(string $bytes): void
    {
        $offset = 0;
        $length = \strlen($bytes);
        while ($offset < $length) {
            $this->wait(reading: false);
            $written = @fwrite($this->resource(), substr($bytes, $offset));
            if (false === $written || 0 === $written) {
                throw new TransportException('Could not write the keepalive request.');
            }
            $offset += $written;
        }
    }

    private function wait(bool $reading): void
    {
        while (true) {
            $remaining = $this->deadline - hrtime(true) / 1_000_000_000;
            if ($remaining <= 0) {
                throw new TransportException('The keepalive exchange deadline expired.');
            }
            $resource = $this->resource();
            $read = $reading ? [$resource] : [];
            $write = $reading ? [] : [$resource];
            $except = [];
            $seconds = (int) $remaining;
            $microseconds = (int) (($remaining - $seconds) * 1_000_000);
            // Other signals may interrupt select. Retry against the same monotonic deadline.
            $ready = @stream_select($read, $write, $except, $seconds, $microseconds);
            if (false !== $ready && $ready > 0) {
                return;
            }
        }
    }

    /** @return resource PHP has no native stream-resource type. */
    private function resource(): mixed
    {
        $resource = $this->stream->getResource();
        if (!\is_resource($resource)) {
            throw new TransportException('The keepalive operations socket is closed.');
        }

        return $resource;
    }
}
