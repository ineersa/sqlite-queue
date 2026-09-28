<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Append-only JSON Lines storage for raw samples.
 *
 * Every child process owns one file, so no two writers touch the same file and a crashed
 * child cannot corrupt another child's samples. A malformed line is counted, not dropped
 * silently.
 */
final class SampleStore
{
    /** @var resource|null */
    private $handle = null;

    public function __construct(private string $path)
    {
        $directory = \dirname($path);
        if (!\is_dir($directory) && !\mkdir($directory, 0o700, true) && !\is_dir($directory)) {
            throw new \RuntimeException(\sprintf('Cannot create the sample directory "%s".', $directory));
        }
    }

    public function path(): string
    {
        return $this->path;
    }

    /**
     * @param array<string, mixed> $record
     */
    public function append(array $record): void
    {
        $handle = $this->handle();

        $line = \json_encode($record, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_PRESERVE_ZERO_FRACTION);
        \fwrite($handle, $line . "\n");
        \fflush($handle);
    }

    public function close(): void
    {
        if (null !== $this->handle) {
            \fclose($this->handle);
            $this->handle = null;
        }
    }

    /**
     * @return array{records: list<array<string, mixed>>, corrupt: int, missing: bool}
     */
    public static function read(string $path): array
    {
        if (!\is_file($path)) {
            return ['records' => [], 'corrupt' => 0, 'missing' => true];
        }

        $contents = \file_get_contents($path);
        if (false === $contents) {
            return ['records' => [], 'corrupt' => 0, 'missing' => true];
        }

        $records = [];
        $corrupt = 0;

        foreach (\explode("\n", $contents) as $line) {
            $line = \trim($line);
            if ('' === $line) {
                continue;
            }

            try {
                /** @var mixed $decoded */
                $decoded = \json_decode($line, true, 512, \JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                ++$corrupt;

                continue;
            }

            if (!\is_array($decoded)) {
                ++$corrupt;

                continue;
            }

            $records[] = $decoded;
        }

        return ['records' => $records, 'corrupt' => $corrupt, 'missing' => false];
    }

    /**
     * @return resource
     */
    private function handle()
    {
        if (null === $this->handle) {
            $handle = \fopen($this->path, 'ab');
            if (false === $handle) {
                throw new \RuntimeException(\sprintf('Cannot open the sample file "%s".', $this->path));
            }

            $this->handle = $handle;
        }

        return $this->handle;
    }
}
