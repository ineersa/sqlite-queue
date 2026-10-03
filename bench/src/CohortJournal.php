<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Bounded expected-ID ledger independent of optional operation telemetry. */
final class CohortJournal
{
    private const CAPACITY_BYTES = 65536;
    private const MAX_ID_BYTES = 128;
    private string $buffer = '';

    /** @param resource $stream */
    private function __construct(private readonly mixed $stream, private readonly string $path)
    {
    }

    public static function create(string $path): self
    {
        $stream = fopen($path, 'xb');
        if (false === $stream) {
            throw new \RuntimeException('Cannot acquire expected-ID journal.');
        }

        return new self($stream, $path);
    }

    public function append(string $id): void
    {
        if ('' === $id || \strlen($id) > self::MAX_ID_BYTES || str_contains($id, "\n")) {
            throw new \InvalidArgumentException('Invalid expected correlation ID.');
        }
        $line = $id."\n";
        if (\strlen($this->buffer) + \strlen($line) > self::CAPACITY_BYTES) {
            $this->flush();
        }
        $this->buffer .= $line;
    }

    public function flush(): void
    {
        if ('' !== $this->buffer && fwrite($this->stream, $this->buffer) !== \strlen($this->buffer)) {
            throw new \RuntimeException('Expected-ID journal write failed.');
        }
        $this->buffer = '';
        if (!fflush($this->stream)) {
            throw new \RuntimeException('Expected-ID journal flush failed.');
        }
    }

    /** @return \Generator<int, string> */
    public function ids(): \Generator
    {
        $this->flush();
        $file = new \SplFileObject($this->path, 'rb');
        while (!$file->eof()) {
            $line = $file->fgets();
            if ('' !== $line) {
                yield rtrim($line, "\n");
            }
        }
    }

    public function close(): void
    {
        try {
            $this->flush();
        } finally {
            fclose($this->stream);
        }
    }
}
