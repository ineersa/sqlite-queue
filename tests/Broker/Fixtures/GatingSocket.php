<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\ReadableStreamIteratorAggregate;
use Amp\Cancellation;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\TlsInfo;
use Amp\Socket\TlsState;

/**
 * Socket double that suspends large writes until WriteGate::release().
 *
 * Small control frames pass through. Oversized response frames enter the gate and stay
 * pending until the test releases them, even if production closes the socket first.
 *
 * @implements \IteratorAggregate<int, string>
 */
final class GatingSocket implements Socket, \IteratorAggregate
{
    use ReadableStreamIteratorAggregate;

    private bool $closed = false;

    public function __construct(
        private readonly Socket $inner,
        private readonly WriteGate $gate,
        private readonly int $thresholdBytes,
        // Small mutation replies can be gated without blocking initial client acquisition.
        private readonly bool $skipHello = false,
    ) {
    }

    public function read(?Cancellation $cancellation = null, ?int $limit = null): ?string
    {
        return $this->inner->read($cancellation, $limit);
    }

    public function write(string $bytes): void
    {
        if (\strlen($bytes) > $this->thresholdBytes && (!$this->skipHello || !str_contains($bytes, '"max_payload"'))) {
            $this->gate->markEntered(\strlen($bytes));
            $this->gate->released()->await();
            if ($this->closed || $this->inner->isClosed()) {
                throw new ClosedException('Gated write released after the socket closed.');
            }
        }
        $this->inner->write($bytes);
    }

    public function end(): void
    {
        $this->inner->end();
    }

    public function close(): void
    {
        $this->closed = true;
        $this->inner->close();
    }

    public function isClosed(): bool
    {
        return $this->closed || $this->inner->isClosed();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->inner->onClose($onClose);
    }

    public function isReadable(): bool
    {
        return $this->inner->isReadable();
    }

    public function isWritable(): bool
    {
        return !$this->closed && $this->inner->isWritable();
    }

    public function reference(): void
    {
        $this->inner->reference();
    }

    public function unreference(): void
    {
        $this->inner->unreference();
    }

    public function getLocalAddress(): SocketAddress
    {
        return $this->inner->getLocalAddress();
    }

    public function getRemoteAddress(): SocketAddress
    {
        return $this->inner->getRemoteAddress();
    }

    public function setupTls(?Cancellation $cancellation = null): void
    {
        $this->inner->setupTls($cancellation);
    }

    public function shutdownTls(?Cancellation $cancellation = null): void
    {
        $this->inner->shutdownTls($cancellation);
    }

    public function isTlsConfigurationAvailable(): bool
    {
        return $this->inner->isTlsConfigurationAvailable();
    }

    public function getTlsState(): TlsState
    {
        return $this->inner->getTlsState();
    }

    public function getTlsInfo(): ?TlsInfo
    {
        return $this->inner->getTlsInfo();
    }
}
