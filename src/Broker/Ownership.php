<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Symfony\Component\Filesystem\Exception\IOException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Lock\Exception\LockAcquiringException;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\LockInterface;
use Symfony\Component\Lock\Store\FlockStore;

/**
 * Exclusive database and endpoint ownership for one broker lifetime.
 *
 * Locks are non-blocking Symfony FlockStore locks held until close(). The permission
 * boundary is the resource parent directory: BrokerPaths requires it to exist, belong
 * to the effective user, and deny group and other access, so the sidecar files the
 * store manages beneath it inherit that protection. Symfony owns their names, modes,
 * and lifetime; this class only owns the lock keys, which derive from canonical paths
 * so a second broker using a different endpoint for the same database still conflicts.
 */
final class Ownership
{
    public readonly string $database;
    public readonly string $endpoint;
    /** @var list<LockInterface> */
    private array $locks = [];
    // Null is the genuine "no socket bound yet" state before recordSocket() and after
    // close(): the broker is fully usable without it, and it only gates endpoint removal.
    private ?SocketIdentity $socketIdentity = null;
    private Filesystem $filesystem;

    public function __construct(string $database, string $endpoint)
    {
        $paths = BrokerPaths::parse($database, $endpoint);
        $this->database = $paths->database;
        $this->endpoint = $paths->endpoint;
        $this->filesystem = new Filesystem();
        try {
            $this->ensurePrivateFile($this->database);
            $this->acquire($this->databaseKey(), \dirname($this->database));
            $this->acquire($this->endpointKey(), \dirname($this->endpoint));
            clearstatcache(true, $this->endpoint);
            if ($this->filesystem->exists($this->endpoint) || is_link($this->endpoint)) {
                throw new \RuntimeException('Endpoint already exists; it will not be removed without verified ownership.');
            }
        } catch (\Throwable $error) {
            $this->close();
            throw $error;
        }
    }

    public function recordSocket(): void
    {
        $this->socketIdentity = SocketIdentity::fromEndpoint($this->endpoint);
        try {
            $this->filesystem->chmod($this->endpoint, 0600);
        } catch (IOException $error) {
            throw new \RuntimeException('Cannot make socket private.', 0, $error);
        }
    }

    public function close(): void
    {
        // A failed socket removal must not skip lock release, so every step records its
        // failure and this method releases everything before reporting the first one.
        $failure = $this->removeOwnedSocket();
        $this->socketIdentity = null;
        foreach (array_reverse($this->locks) as $lock) {
            try {
                $lock->release();
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
        }
        $this->locks = [];
        if (null !== $failure) {
            throw $failure;
        }
    }

    private function acquire(string $key, string $lockDirectory): void
    {
        $lock = (new LockFactory(new FlockStore($lockDirectory)))->createLock($key, null, false);
        try {
            $acquired = $lock->acquire(false);
        } catch (LockAcquiringException $error) {
            throw new \RuntimeException('Ownership unavailable for this database or endpoint.', 0, $error);
        }
        if (!$acquired) {
            throw new \RuntimeException('Ownership unavailable for this database or endpoint.');
        }
        $this->locks[] = $lock;
    }

    private function ensurePrivateFile(string $path): void
    {
        clearstatcache(true, $path);
        if (is_link($path)) {
            throw new \RuntimeException('Ownership path must be a regular file, not a symlink.');
        }
        if (!$this->filesystem->exists($path)) {
            $mask = umask(0077);
            try {
                $this->filesystem->touch($path);
            } catch (IOException $error) {
                throw new \RuntimeException('Cannot open ownership file.', 0, $error);
            } finally {
                umask($mask);
            }
            try {
                $this->filesystem->chmod($path, 0600);
            } catch (IOException $error) {
                throw new \RuntimeException('Cannot open ownership file.', 0, $error);
            }
            clearstatcache(true, $path);
        }
        // Native stat has no Symfony equivalent; checking here keeps the hardlink, owner,
        // and mode defenses on the database file that this broker will actually use.
        $stat = stat($path);
        if (false === $stat || !is_file($path) || $stat['uid'] !== posix_geteuid() || 0 !== ($stat['mode'] & 0077) || 1 !== $stat['nlink']) {
            throw new \RuntimeException('Ownership unavailable or file is not private and singly linked.');
        }
    }

    private function removeOwnedSocket(): ?\Throwable
    {
        if (null === $this->socketIdentity || !$this->socketIdentity->matches($this->endpoint)) {
            return null;
        }
        try {
            $this->filesystem->remove($this->endpoint);
        } catch (\Throwable $error) {
            return $error;
        }

        return null;
    }

    private function databaseKey(): string
    {
        return 'sqlite-queue-db:'.$this->database;
    }

    private function endpointKey(): string
    {
        return 'sqlite-queue-endpoint:'.$this->endpoint;
    }
}
