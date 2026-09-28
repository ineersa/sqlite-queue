<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Protocol\AcknowledgeRequest;
use Ineersa\SqliteQueue\Protocol\EmptyReceiveResponse;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\FailedResponse;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\HelloRequest;
use Ineersa\SqliteQueue\Protocol\HelloResponse;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ReceivedResponse;
use Ineersa\SqliteQueue\Protocol\ReceiveRequest;
use Ineersa\SqliteQueue\Protocol\RejectRequest;
use Ineersa\SqliteQueue\Protocol\Request;
use Ineersa\SqliteQueue\Protocol\RequestCodec;
use Ineersa\SqliteQueue\Protocol\Response;
use Ineersa\SqliteQueue\Protocol\ResponseCodec;
use Ineersa\SqliteQueue\Protocol\SendRequest;
use Ineersa\SqliteQueue\Protocol\SentResponse;
use Ineersa\SqliteQueue\Protocol\SettledResponse;

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
            $response = $client->exchange(new HelloRequest(0), $cancellation);
            if (!$response instanceof HelloResponse) {
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
        $response = $this->exchange(new SendRequest($this->nextId, $queue, $delay, $body, $headers), $cancellation);
        if (!$response instanceof SentResponse) {
            $this->invalidReply();
        }

        return $response->messageId;
    }

    public function receive(string $queue, ?Cancellation $cancellation = null): ?Delivery
    {
        $response = $this->exchange(new ReceiveRequest($this->nextId, $queue), $cancellation);
        if ($response instanceof EmptyReceiveResponse) {
            return null;
        }
        if ($response instanceof ReceivedResponse) {
            return $response->delivery;
        }
        $this->invalidReply();
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
        $request = Operation::Acknowledge === $operation
            ? new AcknowledgeRequest($this->nextId, $receipt)
            : new RejectRequest($this->nextId, $receipt);
        $response = $this->exchange($request, $cancellation);
        if (!$response instanceof SettledResponse) {
            $this->invalidReply();
        }
    }

    private function exchange(Request $request, ?Cancellation $cancellation = null): Response
    {
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy) {
            throw new \LogicException('Only one outstanding request is permitted per client.');
        }
        // Local encoding never consumes the sequence: an oversize payload fails before it is sent.
        try {
            $bytes = RequestCodec::encode($request)->encode();
        } catch (\Throwable $error) {
            // A local protocol violation never reached the broker, so only a broken local
            // encoder closes the client; anything else propagates with the connection usable.
            if ($error instanceof ProtocolException) {
                throw $error;
            }
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $error);
        }
        ++$this->nextId;
        $deadline = new TimeoutCancellation($this->timeout);
        $cancellation = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
        $this->busy = true;
        try {
            Frame::write($this->socket, $bytes, $cancellation);
            $frame = Frame::read($this->socket, $cancellation);
            if (null === $frame) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
            }
            $response = ResponseCodec::decode($frame, $request);
        } catch (\Throwable $error) {
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $error);
        } finally {
            $this->busy = false;
        }
        if ($response instanceof FailedResponse) {
            throw $this->failure($response->code);
        }

        return $response;
    }

    private function failure(ErrorCode $code): \Throwable
    {
        // Recoverable replies keep the connection usable; anything else closes it.
        if (ErrorCode::StaleReceipt === $code) {
            return new InvalidReceipt('Broker rejected a stale or foreign receipt.');
        }
        if (ErrorCode::InvalidQueueName === $code) {
            return new \InvalidArgumentException('Broker rejected the queue name.');
        }
        $this->close();
        if (ErrorCode::BrokerShuttingDown === $code || ErrorCode::InternalStorageFailure === $code) {
            return new TransportException('Broker failed the request; outcome may be unknown. Client is closed.');
        }

        return new ProtocolException($code, 'Broker rejected the request.');
    }

    private function invalidReply(): never
    {
        $this->close();
        throw new TransportException('Invalid broker response; outcome may be unknown. Client is closed.');
    }
}
