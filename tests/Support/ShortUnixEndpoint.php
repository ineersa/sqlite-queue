<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

/** Disposable Unix socket path under /tmp that stays within the 100-byte broker bound. */
final class ShortUnixEndpoint
{
    private string $directory;
    private string $endpoint;

    public function __construct(string $basename = 'q.sock')
    {
        $this->directory = '/tmp/sq-'.bin2hex(random_bytes(4));
        if (!mkdir($this->directory, 0700) && !is_dir($this->directory)) {
            throw new \RuntimeException('Cannot create short Unix endpoint directory.');
        }
        $this->endpoint = $this->directory.'/'.$basename;
    }

    public function path(): string
    {
        return $this->endpoint;
    }

    public function remove(): void
    {
        @unlink($this->endpoint);
        if (is_dir($this->directory)) {
            @rmdir($this->directory);
        }
    }
}
