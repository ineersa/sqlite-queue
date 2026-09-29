<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Symfony\Component\Filesystem\Path;

/**
 * Canonical broker file locations.
 *
 * Parsing only checks path preconditions: absolute locations under directories that exist,
 * belong to the effective user, and are private. Lock acquisition stays in Ownership so a
 * second broker racing the same paths still conflicts on the lock instead of the check.
 */
final readonly class BrokerPaths
{
    /**
     * Unix socket paths must stay short for portability; the broker measures the full
     * canonical endpoint path against this bound.
     */
    private const MAX_ENDPOINT_PATH_BYTES = 100;

    public function __construct(
        public string $database,
        public string $endpoint,
    ) {
    }

    public static function parse(string $database, string $endpoint): self
    {
        $database = self::canonicalFile($database);
        $endpoint = self::canonicalFile($endpoint);
        if (\strlen($endpoint) > self::MAX_ENDPOINT_PATH_BYTES || $endpoint === $database) {
            throw new \InvalidArgumentException('Invalid or conflicting Unix socket path.');
        }

        return new self($database, $endpoint);
    }

    private static function canonicalFile(string $path): string
    {
        // Path::isAbsolute also accepts stream wrappers and URLs, which are never valid
        // broker locations, so absolute here means a leading slash on this platform.
        if (str_contains($path, "\0") || !str_starts_with($path, '/') || \in_array(basename($path), ['', '.', '..'], true)) {
            throw new \InvalidArgumentException('Broker paths must be absolute file paths.');
        }
        // realpath resolves parent-directory aliases so different spellings of the same
        // directory map onto one canonical path and therefore one lock.
        $resolved = realpath(Path::getDirectory($path));
        self::privateDirectory($resolved);

        return Path::join($resolved, basename($path));
    }

    /**
     * Native metadata has no Symfony equivalent, so it stays isolated here.
     */
    private static function privateDirectory(string|false $resolved): void
    {
        $stat = false === $resolved ? false : stat($resolved);
        if (false === $resolved || false === $stat || !is_dir($resolved) || $stat['uid'] !== posix_geteuid() || 0 !== ($stat['mode'] & 0077)) {
            throw new \InvalidArgumentException('Broker directories must exist, belong to this user, and be private.');
        }
    }
}
