<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Symfony\Component\Lock\Exception\LockAcquiringException;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\LockInterface;
use Symfony\Component\Lock\Store\FlockStore;

/**
 * Exclusive database and endpoint locks for one broker lifetime.
 *
 * Acquires, retains, and releases non-blocking Symfony FlockStore locks keyed by the
 * canonical paths. File preparation, socket binding, and identity-checked socket removal
 * belong to BrokerFactory and Broker.
 */
final class BrokerLifetimeLocks
{
    public readonly string $database;
    public readonly string $endpoint;
    /** @var list<LockInterface> */
    private array $locks = [];

    public function __construct(string $database, string $endpoint)
    {
        $paths = BrokerPaths::parse($database, $endpoint);
        $this->database = $paths->database;
        $this->endpoint = $paths->endpoint;
        try {
            $this->acquire($this->databaseKey(), \dirname($this->database));
            $this->acquire($this->endpointKey(), \dirname($this->endpoint));
        } catch (\Throwable $error) {
            try {
                $this->close();
            } catch (\Throwable) {
                // Preserve the acquisition failure after attempting every release.
            }
            throw $error;
        }
    }

    public function close(): void
    {
        $failure = null;
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
            throw new \RuntimeException('Broker lifetime locks unavailable for this database or endpoint.', 0, $error);
        }
        if (!$acquired) {
            throw new \RuntimeException('Broker lifetime locks unavailable for this database or endpoint.');
        }
        $this->locks[] = $lock;
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
