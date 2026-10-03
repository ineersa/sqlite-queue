<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException;

/** Acquires one client, with cancellation owned across acquisition and its full lifetime. */
final class BrokerConnection
{
    private Client|BrokerConnectionStatus $connection = BrokerConnectionStatus::Unconnected;
    private readonly DeferredCancellation $lifetime;

    /** @param \Closure(Cancellation): Client $connect */
    public function __construct(private readonly \Closure $connect)
    {
        $this->lifetime = new DeferredCancellation();
    }

    /** Operations can omit caller cancellation; close() always cancels pending acquisition. */
    public function client(Cancellation $cancellation = new NullCancellation()): Client
    {
        if ($this->connection instanceof Client) {
            return $this->connection;
        }
        if (BrokerConnectionStatus::Closed === $this->connection) {
            throw new TransportException('The broker connection owner is closed.');
        }
        if (BrokerConnectionStatus::Connecting === $this->connection) {
            throw new \LogicException('The broker connection is already being acquired.');
        }

        $this->connection = BrokerConnectionStatus::Connecting;
        $acquisition = new CompositeCancellation($cancellation, $this->lifetime->getCancellation());
        try {
            $acquisition->throwIfRequested();
            $client = ($this->connect)($acquisition);
        } catch (\Throwable $error) {
            $this->connection = BrokerConnectionStatus::Closed;
            $this->lifetime->cancel();

            throw $error;
        }

        try {
            // A connector can ignore cancellation and return after close(). Do not resurrect it.
            $acquisition->throwIfRequested();
        } catch (\Throwable $error) {
            $this->connection = BrokerConnectionStatus::Closed;
            $this->lifetime->cancel();
            $client->close();

            throw $error;
        }
        $this->connection = $client;

        return $client;
    }

    public function close(): void
    {
        $connection = $this->connection;
        $this->connection = BrokerConnectionStatus::Closed;
        $this->lifetime->cancel();
        if ($connection instanceof Client) {
            $connection->close();
        }
    }
}
