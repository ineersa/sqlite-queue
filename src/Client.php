<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Protocol\ControlField;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ProtocolException;

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
        $client->exchange(
            Operation::Hello,
            static function (Frame $reply): void {
                $result = $reply->control[ControlField::Result->value];
                if (!\is_array($result)) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO result must be an object.');
                }
                if (($result[ControlField::MaxPayload->value] ?? null) !== Frame::MAX_PAYLOAD) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO result.max_payload must equal the client payload limit.');
                }
                self::assertNoPayload($reply, 'HELLO');
            },
            cancellation: $cancellation,
        );

        return $client;
    }

    public function send(string $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int
    {
        return $this->exchange(
            Operation::Send,
            static function (Frame $reply): int {
                $id = self::positiveId($reply->control[ControlField::Result->value], 'SEND result');
                self::assertNoPayload($reply, 'SEND');

                return $id;
            },
            [
                ControlField::Queue->value => $queue,
                ControlField::Delay->value => $delay,
            ],
            $body,
            $headers,
            $cancellation,
        );
    }

    public function receive(string $queue, ?Cancellation $cancellation = null): ?DeliveryDTO
    {
        return $this->exchange(
            Operation::Receive,
            static function (Frame $reply) use ($queue): ?DeliveryDTO {
                $result = $reply->control[ControlField::Result->value];
                if (null === $result) {
                    self::assertNoPayload($reply, 'empty RECEIVE');

                    return null;
                }
                if (!\is_array($result)) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'RECEIVE result must be an object or null.');
                }
                $id = self::positiveId($result[ControlField::Id->value] ?? null, 'RECEIVE result.id');
                if (($result[ControlField::Queue->value] ?? null) !== $queue) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'RECEIVE result.queue must equal the requested queue.');
                }
                $receipt = self::readString($result[ControlField::Receipt->value] ?? null, 'RECEIVE result.receipt');
                $availableAt = self::readInteger($result[ControlField::AvailableAt->value] ?? null, 'RECEIVE result.available_at');
                $reservedUntil = self::readInteger($result[ControlField::ReservedUntil->value] ?? null, 'RECEIVE result.reserved_until');

                return new DeliveryDTO($id, $queue, $reply->body, $reply->headers, $receipt, $availableAt, $reservedUntil);
            },
            [ControlField::Queue->value => $queue],
            cancellation: $cancellation,
        );
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
        $label = strtoupper($operation->value);
        $this->exchange(
            $operation,
            static function (Frame $reply) use ($label): void {
                if (null !== $reply->control[ControlField::Result->value]) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, $label.' result must be null.');
                }
                self::assertNoPayload($reply, $label);
            },
            [ControlField::Receipt->value => $receipt],
            cancellation: $cancellation,
        );
    }

    /**
     * Local encoding stays outside the remote cleanup boundary. After a write begins, one
     * boundary closes the client for protocol-validation and transport failures; recoverable
     * broker rejections stay usable and are never replayed.
     *
     * @template T
     *
     * @param \Closure(Frame): T   $decode
     * @param array<string, mixed> $params
     *
     * @return T
     */
    private function exchange(Operation $operation, \Closure $decode, array $params = [], string $body = '', string $headers = '', ?Cancellation $cancellation = null): mixed
    {
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy) {
            throw new \LogicException('Only one outstanding request is permitted per client.');
        }
        $id = $this->nextId;
        // Local encoding never consumes the sequence. An oversize payload throws ProtocolException
        // with the connection usable. Unencodable control is also a local validation failure:
        // nothing was written, so the client and request id stay intact.
        try {
            $bytes = (new Frame([
                ControlField::Version->value => Frame::VERSION,
                ControlField::Id->value => $id,
                ControlField::Operation->value => $operation->value,
            ] + $params, $body, $headers))->encode();
        } catch (\JsonException) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Request control is not valid UTF-8 JSON.');
        }
        ++$this->nextId;
        $deadline = new TimeoutCancellation($this->timeout);
        $cancellation = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
        $this->busy = true;
        $reply = null;
        $ioFailure = null;
        try {
            Frame::write($this->socket, $bytes, $cancellation);
            $reply = Frame::read($this->socket, $cancellation);
        } catch (\Throwable $error) {
            $ioFailure = $error;
        }
        $this->busy = false;
        if (null !== $ioFailure) {
            $this->close();
            if ($ioFailure instanceof TransportException || $ioFailure instanceof ProtocolException) {
                throw $ioFailure;
            }
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.', previous: $ioFailure);
        }
        if (null === $reply) {
            $this->close();
            throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.');
        }

        try {
            return $decode($this->successfulReply($reply, $id));
        } catch (InvalidReceiptException|\InvalidArgumentException $error) {
            throw $error;
        } catch (TransportException|ProtocolException $error) {
            $this->close();
            throw $error;
        }
    }

    private function successfulReply(Frame $reply, int $id): Frame
    {
        if (($reply->control[ControlField::Version->value] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Response v must equal protocol version 1.');
        }
        if (($reply->control[ControlField::Id->value] ?? null) !== $id) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Response id must equal the outstanding request id.');
        }
        $ok = $reply->control[ControlField::Ok->value] ?? null;
        if (!\is_bool($ok)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Response ok must be a boolean.');
        }
        if ($ok) {
            if (!\array_key_exists(ControlField::Result->value, $reply->control)) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Successful response must include result.');
            }

            return $reply;
        }
        if ('' !== $reply->body || '' !== $reply->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure response must not include a payload.');
        }
        if (\array_key_exists(ControlField::Result->value, $reply->control)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure response must not include result.');
        }
        $error = $reply->control[ControlField::Error->value] ?? null;
        if (!\is_array($error)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure response must include an error object.');
        }
        $code = $error[ControlField::Code->value] ?? null;
        if (!\is_string($code)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure response error.code must be a string.');
        }
        $known = ErrorCode::tryFrom($code);
        if (null === $known) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure response error.code is not a known protocol error.');
        }

        throw $this->brokerRejection($known);
    }

    private static function assertNoPayload(Frame $reply, string $context): void
    {
        if ('' !== $reply->body || '' !== $reply->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, $context.' must not include a payload.');
        }
    }

    private static function readInteger(mixed $value, string $field): int
    {
        if (!\is_int($value)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, $field.' must be an integer.');
        }

        return $value;
    }

    private static function readString(mixed $value, string $field): string
    {
        if (!\is_string($value)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, $field.' must be a string.');
        }

        return $value;
    }

    private static function positiveId(mixed $value, string $field): int
    {
        $id = self::readInteger($value, $field);
        if ($id < 1) {
            throw new ProtocolException(ErrorCode::InvalidRequest, $field.' must be a positive integer.');
        }

        return $id;
    }

    private function brokerRejection(ErrorCode $code): \Throwable
    {
        if (ErrorCode::StaleReceipt === $code) {
            return new InvalidReceiptException('Broker rejected a stale or foreign receipt.');
        }
        if (ErrorCode::InvalidQueueName === $code) {
            return new \InvalidArgumentException('Broker rejected the queue name.');
        }
        if (ErrorCode::BrokerShuttingDown === $code) {
            return new TransportException('Broker failed the request; outcome may be unknown. Client is closed.');
        }

        return new ProtocolException($code, 'Broker rejected the request.');
    }
}
