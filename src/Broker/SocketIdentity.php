<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

/**
 * Identity of the socket inode created by this broker.
 *
 * An inode number is only unique within its filesystem, so the device id is part of the
 * identity. The broker removes the endpoint on shutdown only while both still match what
 * recordSocket() observed, which keeps a restart or another process from losing its path.
 */
final readonly class SocketIdentity
{
    /** File-type mask selecting the stat mode bits that encode the entry kind. */
    private const int TYPE_MASK = 0o170000;
    /** File-type value identifying a Unix socket within the masked mode bits. */
    private const int SOCKET_TYPE = 0o140000;

    public function __construct(
        public int $filesystemDeviceId,
        public int $inode,
    ) {
    }

    public static function fromEndpoint(string $endpoint): self
    {
        clearstatcache(true, $endpoint);
        $stat = @lstat($endpoint);
        // One lstat decides everything: a second filetype() call could warn, race the path,
        // and follow a switched symlink instead of reporting the bound entry.
        if (false === $stat || ($stat['mode'] & self::TYPE_MASK) !== self::SOCKET_TYPE) {
            throw new \RuntimeException('Cannot verify bound socket ownership.');
        }

        return new self($stat['dev'], $stat['ino']);
    }

    public function matches(string $endpoint): bool
    {
        clearstatcache(true, $endpoint);
        $stat = @lstat($endpoint);
        if (false === $stat || ($stat['mode'] & self::TYPE_MASK) !== self::SOCKET_TYPE) {
            return false;
        }

        return $stat['dev'] === $this->filesystemDeviceId && $stat['ino'] === $this->inode;
    }
}
