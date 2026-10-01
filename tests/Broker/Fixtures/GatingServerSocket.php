<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Cancellation;
use Amp\Socket\BindContext;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;

/**
 * Accepts from an inner server and wraps each client in a GatingSocket.
 *
 * Every accepted socket shares one WriteGate. Only oversized response writes enter
 * the gate, so an unrelated client's small confirmations stay ungated while the
 * blocked large reply remains pending.
 */
final class GatingServerSocket implements ServerSocket
{
    public function __construct(
        private readonly ServerSocket $inner,
        private readonly WriteGate $gate,
        private readonly int $thresholdBytes,
    ) {
    }

    public function accept(?Cancellation $cancellation = null): ?Socket
    {
        $socket = $this->inner->accept($cancellation);
        if (null === $socket) {
            return null;
        }

        return new GatingSocket($socket, $this->gate, $this->thresholdBytes);
    }

    public function getAddress(): SocketAddress
    {
        return $this->inner->getAddress();
    }

    public function getBindContext(): BindContext
    {
        return $this->inner->getBindContext();
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function isClosed(): bool
    {
        return $this->inner->isClosed();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->inner->onClose($onClose);
    }
}
