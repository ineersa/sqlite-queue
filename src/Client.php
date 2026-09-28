<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Protocol\Frame;

use function Amp\Socket\connect;

/** One persistent connection, one outstanding call, and no automatic retries. */
final class Client
{
    private int $nextId = 0;
    private bool $busy = false;
    private bool $closed = false;

    private function __construct(private readonly Socket $socket, private readonly float $timeout)
    {
    }

    private function __clone(): void
    {
    }

    public static function connect(string $endpoint, float $timeout = 10, ?Cancellation $cancellation = null): self
    {
        if ($timeout <= 0 || !is_finite($timeout)) {
            throw new \InvalidArgumentException('Timeout must be positive and finite.');
        }
        try {
            $socket = connect('unix://'.$endpoint, (new ConnectContext())->withConnectTimeout($timeout), $cancellation);
        } catch (\Throwable $error) {
            throw new TransportException('Could not connect to queue broker.', previous: $error);
        }
        $client = new self($socket, $timeout);
        try {
            $reply = $client->exchange(['op' => 'hello'], cancellation: $cancellation);
            if (($reply->control['result']['max_payload'] ?? null) !== Frame::MAX_PAYLOAD) {
                $client->invalidReply();
            }
        } catch (\Throwable $error) {
            $client->close();
            throw $error;
        }

        return $client;
    }

    public function send(string $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int
    {
        $reply = $this->exchange(['op' => 'send', 'queue' => $queue, 'delay' => $delay], $body, $headers, $cancellation);
        $id = $reply->control['result'];
        if (!\is_int($id) || $id < 1 || '' !== $reply->body || '' !== $reply->headers) {
            $this->invalidReply();
        }

        return $id;
    }

    public function receive(string $queue, ?Cancellation $cancellation = null): ?Delivery
    {
        $reply = $this->exchange(['op' => 'receive', 'queue' => $queue], cancellation: $cancellation);
        $result = $reply->control['result'];
        if (null === $result && '' === $reply->body && '' === $reply->headers) {
            return null;
        }
        if (!\is_array($result) || !\is_int($result['id'] ?? null) || $result['id'] < 1
            || ($result['queue'] ?? null) !== $queue || !\is_string($result['receipt'] ?? null)
            || !\is_int($result['available_at'] ?? null) || !\is_int($result['reserved_until'] ?? null)) {
            $this->invalidReply();
        }

        return new Delivery($result['id'], $queue, $reply->body, $reply->headers, $result['receipt'], $result['available_at'], $result['reserved_until']);
    }

    public function acknowledge(string $receipt, ?Cancellation $cancellation = null): void
    {
        $this->settle('acknowledge', $receipt, $cancellation);
    }

    public function reject(string $receipt, ?Cancellation $cancellation = null): void
    {
        $this->settle('reject', $receipt, $cancellation);
    }

    public function close(): void
    {
        $this->closed = true;
        $this->socket->close();
    }

    private function settle(string $operation, string $receipt, ?Cancellation $cancellation): void
    {
        $reply = $this->exchange(['op' => $operation, 'receipt' => $receipt], cancellation: $cancellation);
        if (null !== $reply->control['result'] || '' !== $reply->body || '' !== $reply->headers) {
            $this->invalidReply();
        }
    }

    /** @param array<string, mixed> $control */
    private function exchange(array $control, string $body = '', string $headers = '', ?Cancellation $cancellation = null): Frame
    {
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy) {
            throw new \LogicException('Only one outstanding request is permitted per client.');
        }
        $id = $this->nextId;
        $bytes = (new Frame(['v' => Frame::VERSION, 'id' => $id] + $control, $body, $headers))->encode();
        ++$this->nextId;
        $deadline = new TimeoutCancellation($this->timeout);
        $cancellation = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
        $this->busy = true;
        try {
            Frame::write($this->socket, $bytes, $cancellation);
            $reply = Frame::read($this->socket, $cancellation);
            if (null === $reply || ($reply->control['v'] ?? null) !== Frame::VERSION || ($reply->control['id'] ?? null) !== $id
                || !\is_bool($reply->control['ok'] ?? null)
                || (true === $reply->control['ok'] && !\array_key_exists('result', $reply->control))) {
                throw new ProtocolException('invalid_request', 'Invalid or uncorrelated broker response.');
            }
        } catch (\Throwable $error) {
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $error);
        } finally {
            $this->busy = false;
        }
        if (true === $reply->control['ok']) {
            return $reply;
        }
        $code = $reply->control['error']['code'] ?? null;
        if ('stale_receipt' === $code) {
            throw new InvalidReceipt('Broker rejected a stale or foreign receipt.');
        }
        if ('invalid_queue_name' === $code) {
            throw new \InvalidArgumentException('Broker rejected the queue name.');
        }
        $this->close();
        if (\in_array($code, ['invalid_request', 'unsupported_protocol_version', 'frame_too_large'], true)) {
            throw new ProtocolException($code, 'Broker rejected the request.');
        }

        throw new TransportException('Broker failed the request; outcome may be unknown. Client is closed.');
    }

    private function invalidReply(): never
    {
        $this->close();
        throw new TransportException('Invalid broker response; outcome may be unknown. Client is closed.');
    }
}
