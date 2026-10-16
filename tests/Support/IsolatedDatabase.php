<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

/**
 * A disposable directory for one test.
 *
 * Every test opens its own file database under var/tests, so no test reads or writes
 * a live application database.
 */
final class IsolatedDatabase
{
    private string $directory;

    public function __construct()
    {
        // Keep fixtures under /tmp so Unix socket endpoints stay within the 100-byte broker bound
        // even when the checkout path itself is long (for example nested worktrees).
        $base = '/tmp/sqlite-queue-tests';

        if (!is_dir($base) && !mkdir($base, 0o700, true) && !is_dir($base)) {
            throw new \RuntimeException(\sprintf('Cannot create the test directory "%s".', $base));
        }

        $this->directory = $base.'/'.bin2hex(random_bytes(8));

        if (!mkdir($this->directory, 0o700)) {
            throw new \RuntimeException(\sprintf('Cannot create the test directory "%s".', $this->directory));
        }
    }

    public function path(string $name = 'queue.sqlite'): string
    {
        return $this->directory.'/'.$name;
    }

    public function directory(): string
    {
        return $this->directory;
    }

    public function remove(): void
    {
        $files = glob($this->directory.'/*');
        foreach (false === $files ? [] : $files as $file) {
            if (is_file($file)) {
                @unlink($file);
            }
        }

        if (is_dir($this->directory)) {
            @rmdir($this->directory);
        }
    }
}
