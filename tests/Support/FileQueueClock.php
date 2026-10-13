<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Ineersa\SqliteQueue\Sqlite\QueueClock;

/** Worker-only fixture; an optional one-shot socket barrier exposes the acquired transaction. */
final readonly class FileQueueClock implements QueueClock
{
    public function __construct(private string $path)
    {
    }

    public function now(): int
    {
        if (is_file($this->path.'.gate')) {
            $endpoint = file_get_contents($this->path.'.gate');
            unlink($this->path.'.gate');
            $socket = stream_socket_client('unix://'.$endpoint);
            if (false === $socket) {
                throw new \RuntimeException('Could not connect the worker clock barrier.');
            }
            try {
                fwrite($socket, "acquired\n");
                if ("release\n" !== fgets($socket)) {
                    throw new \RuntimeException('Worker clock barrier was not released.');
                }
            } finally {
                fclose($socket);
            }
        }
        $contents = file_get_contents($this->path);
        $now = filter_var($contents, \FILTER_VALIDATE_INT);
        if (false === $now) {
            throw new \RuntimeException('Controlled worker clock must contain integer milliseconds.');
        }

        return $now;
    }
}
