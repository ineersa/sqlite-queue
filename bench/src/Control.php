<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** One line per one-in-flight completion. Socket timeouts abort hangs, not prove performance. */
final class Control
{
    public const MAX_PACKET_BYTES = 1024;

    private bool $closed = false;

    /** @param resource $stream */
    public function __construct(private readonly mixed $stream)
    {
        if (!\is_resource($stream)) {
            throw new \InvalidArgumentException('Control requires an acquired stream.');
        }
        stream_set_timeout($stream, Config::STARTUP_TIMEOUT_S);
    }

    public function send(string $id): void
    {
        $this->sendPacket(['id' => $id]);
    }

    /** @param array<string, mixed> $data */
    public function sendPacket(array $data): void
    {
        $packet = json_encode($data, \JSON_THROW_ON_ERROR)."\n";
        if (\strlen($packet) > self::MAX_PACKET_BYTES) {
            throw new \InvalidArgumentException('Control packet exceeds bound.');
        }
        if (fwrite($this->stream, $packet) !== \strlen($packet)) {
            throw new \RuntimeException('Control write failed.');
        }
    }

    public function receive(): string
    {
        return $this->receivePacket()['id'];
    }

    public function setTimeoutSeconds(float $seconds): void
    {
        if (!is_finite($seconds) || $seconds <= 0) {
            throw new \InvalidArgumentException('Control timeout must be finite and positive.');
        }
        $whole = (int) $seconds;
        stream_set_timeout($this->stream, $whole, (int) (($seconds - $whole) * 1000000));
    }

    public function setDeadline(int $deadlineNanoseconds): void
    {
        $remaining = ($deadlineNanoseconds - hrtime(true)) / 1e9;
        if ($remaining <= 0) {
            throw new \RuntimeException('Control phase deadline expired.');
        }
        $this->setTimeoutSeconds($remaining);
    }

    /** Null means the fixed window ended before a completion became readable. */
    public function receiveBefore(int $deadlineNanoseconds): ?string
    {
        $remaining = $deadlineNanoseconds - hrtime(true);
        if ($remaining <= 0) {
            return null;
        }
        $read = [$this->stream];
        $write = [];
        $except = [];
        $seconds = intdiv($remaining, 1000000000);
        $microseconds = intdiv($remaining % 1000000000, 1000);
        $ready = stream_select($read, $write, $except, $seconds, $microseconds);
        if (false === $ready) {
            throw new \RuntimeException('Control readiness wait failed.');
        }
        if (0 === $ready || hrtime(true) >= $deadlineNanoseconds) {
            return null;
        }
        $this->setDeadline($deadlineNanoseconds);

        return $this->receive();
    }

    /** @return array<string, mixed>&array{id: string} */
    public function receivePacket(): array
    {
        $packet = fgets($this->stream, self::MAX_PACKET_BYTES + 1);
        if (false === $packet || !str_ends_with($packet, "\n")) {
            throw new \RuntimeException('Control completion missing or oversized.');
        }
        $data = json_decode($packet, true, flags: \JSON_THROW_ON_ERROR);
        if (!\is_array($data) || !\is_string($data['id'] ?? null)) {
            throw new \RuntimeException('Invalid control completion.');
        }

        return $data;
    }

    public function hasPacket(): bool
    {
        $read = [$this->stream];
        $write = [];
        $except = [];
        $ready = stream_select($read, $write, $except, 0);
        if (false === $ready) {
            throw new \RuntimeException('Control readiness check failed.');
        }

        return $ready > 0;
    }

    public function close(): void
    {
        if (!$this->closed) {
            $this->closed = true;
            fclose($this->stream);
        }
    }

    public function wait(float $seconds): bool
    {
        return self::waitAny([$this], $seconds);
    }

    /** @param non-empty-list<self> $controls */
    public static function waitAny(array $controls, float $seconds): bool
    {
        if (!is_finite($seconds) || $seconds < 0) {
            throw new \InvalidArgumentException('Control wait must be finite and nonnegative.');
        }
        $read = array_map(static fn (self $control): mixed => $control->stream, $controls);
        $write = [];
        $except = [];
        $whole = (int) floor($seconds);
        $ready = stream_select($read, $write, $except, $whole, (int) (($seconds - $whole) * 1000000));
        if (false === $ready) {
            throw new \RuntimeException('Control wait failed.');
        }

        return $ready > 0;
    }
}
