<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

/** Advisory locks remain open until the persistence process has stopped. */
final class Ownership
{
    public readonly string $database;
    public readonly string $endpoint;
    /** @var list<resource> */
    private array $locks = [];
    /** @var array{dev: int, ino: int}|null */
    private ?array $socketIdentity = null;

    public function __construct(string $database, string $endpoint)
    {
        $this->database = self::path($database);
        $this->endpoint = self::path($endpoint);
        if (\strlen($this->endpoint) > 100 || $this->endpoint === $this->database || $this->endpoint.'.lock' === $this->database) {
            throw new \InvalidArgumentException('Invalid or conflicting Unix socket path.');
        }
        try {
            $this->lock($this->database);
            $this->lock($this->endpoint.'.lock');
            clearstatcache(true, $this->endpoint);
            if (file_exists($this->endpoint) || is_link($this->endpoint)) {
                throw new \RuntimeException('Endpoint already exists; it will not be removed without verified ownership.');
            }
        } catch (\Throwable $error) {
            $this->close();
            throw $error;
        }
    }

    public function recordSocket(): void
    {
        clearstatcache(true, $this->endpoint);
        $stat = @lstat($this->endpoint);
        if (false === $stat || 'socket' !== filetype($this->endpoint)) {
            throw new \RuntimeException('Cannot verify bound socket ownership.');
        }
        $this->socketIdentity = ['dev' => $stat['dev'], 'ino' => $stat['ino']];
        if (!@chmod($this->endpoint, 0600)) {
            throw new \RuntimeException('Cannot make socket private.');
        }
    }

    public function close(): void
    {
        if (null !== $this->socketIdentity) {
            clearstatcache(true, $this->endpoint);
            $stat = @lstat($this->endpoint);
            if (false !== $stat && $stat['dev'] === $this->socketIdentity['dev'] && $stat['ino'] === $this->socketIdentity['ino']) {
                unlink($this->endpoint);
            }
            $this->socketIdentity = null;
        }
        foreach (array_reverse($this->locks) as $lock) {
            flock($lock, \LOCK_UN);
            fclose($lock);
        }
        $this->locks = [];
    }

    private function lock(string $path): void
    {
        if (is_link($path) || (file_exists($path) && !is_file($path))) {
            throw new \RuntimeException('Ownership path must be a regular file, not a symlink.');
        }
        $mask = umask(0077);
        try {
            $handle = @fopen($path, 'c+b');
        } finally {
            umask($mask);
        }
        if (false === $handle) {
            throw new \RuntimeException('Cannot open ownership file.');
        }
        $stat = fstat($handle);
        if (false === $stat || $stat['uid'] !== posix_geteuid() || 0 !== ($stat['mode'] & 0077) || 1 !== $stat['nlink'] || !flock($handle, \LOCK_EX | \LOCK_NB)) {
            fclose($handle);
            throw new \RuntimeException('Ownership unavailable or file is not private and singly linked.');
        }
        $this->locks[] = $handle;
    }

    private static function path(string $path): string
    {
        if (str_contains($path, "\0") || !str_starts_with($path, '/') || \in_array(basename($path), ['', '.', '..'], true)) {
            throw new \InvalidArgumentException('Broker paths must be absolute file paths.');
        }
        $directory = realpath(\dirname($path));
        $stat = false === $directory ? false : stat($directory);
        if (false === $directory || false === $stat || !is_dir($directory) || $stat['uid'] !== posix_geteuid() || 0 !== ($stat['mode'] & 0077)) {
            throw new \InvalidArgumentException('Broker directories must exist, belong to this user, and be private.');
        }

        return $directory.'/'.basename($path);
    }
}
