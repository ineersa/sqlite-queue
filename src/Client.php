<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Operation;

use function Amp\Socket\connect;

/**
 * One persistent connection, one outstanding call, and no automatic retries.
 *
 * Cancellation policy, stated once for every ?Cancellation below: cancelling a call only
 * aborts the local wait. The broker may already have applied the operation, so a cancelled
 * call closes the client with an unknown outcome and is never retried; inspect the queue
 * with a new connection created explicitly.
 */
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

    /**
     * @param float $timeout per-exchange bound in seconds; 10 is the documented protocol default
     */
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
            $reply = $client->exchange(Operation::Hello, cancellation: $cancellation);
            $result = $reply->control['result'];
            if (!\is_array($result) || ($result['max_payload'] ?? null) !== Frame::MAX_PAYLOAD) {
                $client->invalidReply();
            }
            $client->assertNoPayload($reply);
        } catch (\Throwable $error) {
            $client->close();
            throw $error;
        }

        return $client;
    }

    public function send(string $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int
    {
        $reply = $this->exchange(Operation::Send, ['queue' => $queue, 'delay' => $delay], $body, $headers, $cancellation);
        $id = $this->positiveId($reply->control['result']);
        $this->assertNoPayload($reply);

        return $id;
    }

    public function receive(string $queue, ?Cancellation $cancellation = null): ?Delivery
    {
        $reply = $this->exchange(Operation::Receive, ['queue' => $queue], cancellation: $cancellation);
        $result = $reply->control['result'];
        if (null === $result) {
            $this->assertNoPayload($reply);

            return null;
        }
        if (!\is_array($result)) {
            $this->invalidReply();
        }
        $id = $this->positiveId($result['id'] ?? null);
        if (($result['queue'] ?? null) !== $queue) {
            $this->invalidReply();
        }
        $receipt = $this->readString($result['receipt'] ?? null);
        $availableAt = $this->readInteger($result['available_at'] ?? null);
        $reservedUntil = $this->readInteger($result['reserved_until'] ?? null);

        return new Delivery($id, $queue, $reply->body, $reply->headers, $receipt, $availableAt, $reservedUntil);
    }

    public function acknowledge(string $receipt, ?Cancellation $cancellation = null): void
    {
        $this->settle(Operation::Acknowledge, $receipt, $cancellation);
    }

    public function reject(string $receipt, ?Cancellation $cancellation = null): void
    {
        $this->settle(Operation::Reject, $receipt, $cancellation);
    }

    public function close(): void
    {
        $this->closed = true;
        $this->socket->close();
    }

    private function settle(Operation $operation, string $receipt, ?Cancellation $cancellation): void
    {
        $reply = $this->exchange($operation, ['receipt' => $receipt], cancellation: $cancellation);
        if (null !== $reply->control['result']) {
            $this->invalidReply();
        }
        $this->assertNoPayload($reply);
    }

    /**
     * The operation stays typed until serialization; only Frame builds the wire array.
     *
     * @param array<string, mixed> $params
     */
    private function exchange(Operation $operation, array $params = [], string $body = '', string $headers = '', ?Cancellation $cancellation = null): Frame
    {
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy) {
            throw new \LogicException('Only one outstanding request is permitted per client.');
        }
        $id = $this->nextId;
        // Local encoding never consumes the sequence. An oversize payload throws ProtocolException
        // with the connection usable; unencodable control throws JsonException, which closes the
        // client because the request can never be framed.
        try {
            $bytes = (new Frame(['v' => Frame::VERSION, 'id' => $id, 'op' => $operation->value] + $params, $body, $headers))->encode();
        } catch (\JsonException $error) {
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $error);
        }
        ++$this->nextId;
        $deadline = new TimeoutCancellation($this->timeout);
        $cancellation = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
        $this->busy = true;
        try {
            Frame::write($this->socket, $bytes, $cancellation);
            $reply = $this->correlatedReply(Frame::read($this->socket, $cancellation), $id);
        } catch (\Throwable $error) {
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $error);
        } finally {
            $this->busy = false;
        }
        if (true === $reply->control['ok']) {
            return $reply;
        }
        if ('' !== $reply->body || '' !== $reply->headers || \array_key_exists('result', $reply->control)) {
            $this->invalidReply();
        }

        throw $this->failure($reply->control['error']['code'] ?? null);
    }

    /** Each guard below reports the same uncorrelated reply; the call site stays readable. */
    private function correlatedReply(?Frame $reply, int $id): Frame
    {
        if (null === $reply) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
        if (($reply->control['v'] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
        if (($reply->control['id'] ?? null) !== $id) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
        if (!\is_bool($reply->control['ok'] ?? null)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
        if (true === $reply->control['ok'] && !\array_key_exists('result', $reply->control)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }

        return $reply;
    }

    private function assertNoPayload(Frame $reply): void
    {
        if ('' !== $reply->body || '' !== $reply->headers) {
            $this->invalidReply();
        }
    }

    private function readInteger(mixed $value): int
    {
        if (!\is_int($value)) {
            $this->invalidReply();
        }

        return $value;
    }

    private function readString(mixed $value): string
    {
        if (!\is_string($value)) {
            $this->invalidReply();
        }

        return $value;
    }

    private function positiveId(mixed $value): int
    {
        $id = $this->readInteger($value);
        if ($id < 1) {
            $this->invalidReply();
        }

        return $id;
    }

    private function failure(mixed $code): \Throwable
    {
        $known = \is_string($code) ? ErrorCode::tryFrom($code) : null;
        // Recoverable replies keep the connection usable; anything else closes it.
        if (ErrorCode::StaleReceipt === $known) {
            return new InvalidReceipt('Broker rejected a stale or foreign receipt.');
        }
        if (ErrorCode::InvalidQueueName === $known) {
            return new \InvalidArgumentException('Broker rejected the queue name.');
        }
        $this->close();
        if (ErrorCode::BrokerShuttingDown === $known || ErrorCode::InternalStorageFailure === $known || null === $known) {
            return new TransportException('Broker failed the request; outcome may be unknown. Client is closed.');
        }

        return new ProtocolException($known, 'Broker rejected the request.');
    }

    private function invalidReply(): never
    {
        $this->close();
        throw new TransportException('Invalid broker response; outcome may be unknown. Client is closed.');
    }
}
