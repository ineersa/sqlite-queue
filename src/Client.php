<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Amp\ByteStream\ResourceStream;
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
use Ineersa\SqliteQueue\Protocol\KeepaliveStream;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ProtocolException;
use Ineersa\SqliteQueue\Protocol\ProtocolLimits;
use Ineersa\SqliteQueue\ValueObject\QueueName;

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
    private const string RECEIPT_KEY_PREFIX = 'receipt:';

    private int $nextId = 0;
    private bool $busy = false;
    private bool $closed = false;
    /** @var array<string, array{receipt: string, minimum: int, error: \Closure(\Throwable): void}> Coalesced only until the active exchange completes. */
    private array $pendingKeepalives = [];
    private bool $flushingKeepalives = false;

    private function __construct(
        private readonly Socket $socket,
        private readonly float $timeout,
        private readonly ProtocolLimits $limits,
    ) {
    }

    private function __clone(): void
    {
    }

    public function protocolLimits(): ProtocolLimits
    {
        return $this->limits;
    }

    /**
     * @param ?ProtocolLimits $expectedLimits when provided, HELLO must advertise these exact budgets
     */
    public static function connect(string $endpoint, float $timeout = 10, ?Cancellation $cancellation = null, ?ProtocolLimits $expectedLimits = null): self
    {
        if ($timeout <= 0 || !is_finite($timeout)) {
            throw new \InvalidArgumentException('Timeout must be positive and finite.');
        }
        try {
            $socket = connect('unix://'.$endpoint, (new ConnectContext())->withConnectTimeout($timeout), $cancellation);
        } catch (\Throwable $error) {
            throw new TransportException('Could not connect to queue broker.', previous: $error);
        }
        $deadline = new TimeoutCancellation($timeout);
        $bound = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
        try {
            // HELLO is empty and always fits the published default frame budget.
            Frame::write($socket, (new Frame([
                ControlField::Version->value => Frame::VERSION,
                ControlField::Id->value => 0,
                ControlField::Operation->value => Operation::Hello->value,
            ]))->encode(ProtocolLimits::defaults()), $bound);
            $reply = Frame::read($socket, $bound, ProtocolLimits::defaults());
            if (null === $reply) {
                throw new TransportException('Broker confirmation unavailable; outcome may be unknown. Client is closed.');
            }
            if (($reply->control[ControlField::Version->value] ?? null) !== Frame::VERSION) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Response v must equal protocol version 1.');
            }
            if (($reply->control[ControlField::Id->value] ?? null) !== 0) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'Response id must equal the outstanding request id.');
            }
            $ok = $reply->control[ControlField::Ok->value] ?? null;
            if (true !== $ok) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO must succeed.');
            }
            $result = $reply->control[ControlField::Result->value] ?? null;
            if (!\is_array($result)) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO result must be an object.');
            }
            $maxPayload = $result[ControlField::MaxPayload->value] ?? null;
            // Original v1 brokers advertise only max_payload and have a fixed
            // 64-connection limit. The new capacity field is additive metadata.
            $maxConnections = \array_key_exists(ControlField::MaxConnections->value, $result)
                ? $result[ControlField::MaxConnections->value]
                : Limits::MAX_CONNECTIONS;
            if (!\is_int($maxPayload) || !\is_int($maxConnections)) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO payload and connection limits must be integers.');
            }
            try {
                $limits = new ProtocolLimits($maxPayload, $maxConnections);
            } catch (\InvalidArgumentException) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO result limits are invalid.');
            }
            if (null !== $expectedLimits
                && ($expectedLimits->maxPayload !== $limits->maxPayload || $expectedLimits->maxConnections !== $limits->maxConnections)
            ) {
                throw new ProtocolException(ErrorCode::InvalidRequest, 'HELLO result limits must equal the expected client limits.');
            }
            self::assertNoPayload($reply, 'HELLO');
        } catch (\Throwable $error) {
            $socket->close();
            if ($error instanceof TransportException || $error instanceof ProtocolException) {
                throw $error;
            }
            throw new TransportException('Could not complete the broker handshake.', previous: $error);
        }

        $client = new self($socket, $timeout, $limits);
        // HELLO used request id 0 outside exchange(); later calls start at 1.
        $client->nextId = 1;

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

    /** Zero minimum selects the broker's reservation duration; cancellation is optional for callers. */
    public function keepalive(string $receipt, int $minimumMilliseconds = 0, ?Cancellation $cancellation = null): int
    {
        if ($minimumMilliseconds < 0) {
            throw new \InvalidArgumentException('Keepalive duration must be nonnegative milliseconds.');
        }

        return $this->exchange(
            Operation::Keepalive,
            self::keepaliveDeadline(...),
            [ControlField::Receipt->value => $receipt, ControlField::KeepaliveMilliseconds->value => $minimumMilliseconds],
            cancellation: $cancellation,
        );
    }

    /**
     * Native SIGALRM callbacks cannot switch fibers. Borrow this same socket for a bounded
     * renewal, or coalesce it until the interrupted exchange completes. Null means deferred.
     * The observer records deferred failures without throwing, so a confirmed exchange stays successful.
     * Closing discards coalesced renewals without notifying observers.
     *
     * @param \Closure(\Throwable): void $onDeferredError
     */
    public function keepaliveSignalSafe(string $receipt, int $minimumMilliseconds, \Closure $onDeferredError): ?int
    {
        if ($minimumMilliseconds < 0) {
            throw new \InvalidArgumentException('Keepalive duration must be nonnegative milliseconds.');
        }
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy || $this->flushingKeepalives) {
            // A prefix preserves opaque numeric strings instead of coercing them to integer keys.
            $key = self::RECEIPT_KEY_PREFIX.$receipt;
            $minimum = max($minimumMilliseconds, $this->pendingKeepalives[$key]['minimum'] ?? 0);
            $this->pendingKeepalives[$key] = ['receipt' => $receipt, 'minimum' => $minimum, 'error' => $onDeferredError];

            return null;
        }

        return $this->keepaliveWithoutSuspending($receipt, $minimumMilliseconds);
    }

    public function reject(string $receipt, ?Cancellation $cancellation = null): void
    {
        $this->settle(Operation::Reject, $receipt, $cancellation);
    }

    /**
     * Bounded readiness hint. True means try receive; it never grants a reservation.
     * False means the wait elapsed without a readiness hint.
     *
     * Bounds are validated locally before any bytes are written, so an out-of-range
     * timeout does not consume a request id. The exchange deadline is the requested
     * wait duration plus the client's normal transport allowance.
     */
    public function wait(string $queue, int $timeoutMilliseconds, ?Cancellation $cancellation = null): bool
    {
        if ($timeoutMilliseconds < 0 || $timeoutMilliseconds > Limits::MAX_WAIT_MILLISECONDS) {
            throw new \InvalidArgumentException(\sprintf('Wait timeout must be between 0 and %d milliseconds.', Limits::MAX_WAIT_MILLISECONDS));
        }

        return $this->exchange(
            Operation::Wait,
            static function (Frame $reply): bool {
                $result = $reply->control[ControlField::Result->value];
                if (!\is_bool($result)) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'WAIT result must be a boolean.');
                }
                self::assertNoPayload($reply, 'WAIT');

                return $result;
            },
            [
                ControlField::Queue->value => $queue,
                ControlField::WaitMilliseconds->value => $timeoutMilliseconds,
            ],
            cancellation: $cancellation,
            timeout: $this->timeout + ($timeoutMilliseconds / 1000),
        );
    }

    /**
     * Bounded multi-queue readiness hint. True means try receive on the selected queues; it never
     * grants a reservation. False means the shared wait elapsed without a readiness hint.
     *
     * Queue lists, each name, empty input, duplicates, count, and the timeout are validated
     * locally before any bytes are written. Unsupported peers reject wait_any explicitly; this
     * client never falls back to single-queue WAIT or silently truncates the list.
     *
     * @param list<string> $queues
     */
    public function waitAny(array $queues, int $timeoutMilliseconds, ?Cancellation $cancellation = null): bool
    {
        if ($timeoutMilliseconds < 0 || $timeoutMilliseconds > Limits::MAX_WAIT_MILLISECONDS) {
            throw new \InvalidArgumentException(\sprintf('Wait timeout must be between 0 and %d milliseconds.', Limits::MAX_WAIT_MILLISECONDS));
        }
        $selected = self::validatedWaitQueues($queues);

        return $this->exchange(
            Operation::WaitAny,
            static function (Frame $reply): bool {
                $result = $reply->control[ControlField::Result->value];
                if (!\is_bool($result)) {
                    throw new ProtocolException(ErrorCode::InvalidRequest, 'WAIT_ANY result must be a boolean.');
                }
                self::assertNoPayload($reply, 'WAIT_ANY');

                return $result;
            },
            [
                ControlField::Queues->value => $selected,
                ControlField::WaitMilliseconds->value => $timeoutMilliseconds,
            ],
            cancellation: $cancellation,
            timeout: $this->timeout + ($timeoutMilliseconds / 1000),
        );
    }

    public function close(): void
    {
        $this->closed = true;
        $this->pendingKeepalives = [];
        $this->socket->close();
    }

    private function keepaliveWithoutSuspending(string $receipt, int $minimumMilliseconds): int
    {
        if (!$this->socket instanceof ResourceStream) {
            throw new TransportException('Signal-safe keepalive requires a resource-backed operations socket.');
        }

        return $this->exchange(
            Operation::Keepalive,
            self::keepaliveDeadline(...),
            [ControlField::Receipt->value => $receipt, ControlField::KeepaliveMilliseconds->value => $minimumMilliseconds],
            keepaliveIo: new KeepaliveStream($this->socket, $this->timeout),
        );
    }

    private static function keepaliveDeadline(Frame $reply): int
    {
        $deadline = self::readInteger($reply->control[ControlField::Result->value], 'KEEPALIVE result');
        if ($deadline < 0) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'KEEPALIVE result must be nonnegative Unix milliseconds.');
        }
        self::assertNoPayload($reply, 'KEEPALIVE');

        return $deadline;
    }

    private function flushKeepalives(bool $signalSafe): void
    {
        if ($this->flushingKeepalives || $this->busy) {
            return;
        }
        $this->flushingKeepalives = true;
        try {
            // Detach in one operation so a signal cannot clear or replace half a dequeue.
            // New ticks stay in the live queue and cannot extend this caller's drain.
            $snapshot = array_splice($this->pendingKeepalives, 0);
            foreach ($snapshot as $pending) {
                if ($this->closed) {
                    break;
                }
                $receipt = $pending['receipt'];
                try {
                    if ($signalSafe) {
                        $this->keepaliveWithoutSuspending($receipt, $pending['minimum']);
                    } else {
                        // Ordinary API completion stays on Amp, including deferred renewal I/O.
                        $this->keepalive($receipt, $pending['minimum']);
                    }
                } catch (\Throwable $error) {
                    ($pending['error'])($error);
                }
            }
        } finally {
            $this->flushingKeepalives = false;
        }
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
     * @param float|null           $timeout     null uses the normal transport bound; WAIT adds its requested duration
     * @param KeepaliveStream|null $keepaliveIo null selects Amp I/O; native signal renewal borrows the stream
     *
     * @return T
     */
    private function exchange(Operation $operation, \Closure $decode, array $params = [], string $body = '', string $headers = '', ?Cancellation $cancellation = null, ?float $timeout = null, ?KeepaliveStream $keepaliveIo = null): mixed
    {
        if ($this->closed) {
            throw new TransportException('Client is closed; create a new connection explicitly.');
        }
        if ($this->busy) {
            throw new \LogicException('Only one outstanding request is permitted per client.');
        }
        // Protect encoding, sequence allocation, decoding and cleanup, not just socket I/O.
        $this->busy = true;
        try {
            return $this->performExchange($operation, $decode, $params, $body, $headers, $cancellation, $timeout, $keepaliveIo);
        } finally {
            if (Operation::Acknowledge === $operation || Operation::Reject === $operation) {
                unset($this->pendingKeepalives[self::RECEIPT_KEY_PREFIX.$params[ControlField::Receipt->value]]);
            }
            $this->busy = false;
            $this->flushKeepalives(null !== $keepaliveIo);
        }
    }

    /**
     * @template T
     *
     * @param \Closure(Frame): T   $decode
     * @param array<string, mixed> $params
     * @param ?KeepaliveStream     $keepaliveIo only the signal-safe renewal supplies native I/O
     *
     * @return T
     */
    private function performExchange(Operation $operation, \Closure $decode, array $params, string $body, string $headers, ?Cancellation $cancellation, ?float $timeout, ?KeepaliveStream $keepaliveIo): mixed
    {
        $id = $this->nextId;
        // Local encoding never consumes the sequence. An oversize payload throws ProtocolException
        // with the connection usable. Unencodable control is also a local validation failure:
        // nothing was written, so the client and request id stay intact.
        try {
            $bytes = (new Frame([
                ControlField::Version->value => Frame::VERSION,
                ControlField::Id->value => $id,
                ControlField::Operation->value => $operation->value,
            ] + $params, $body, $headers))->encode($this->limits);
        } catch (\JsonException) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Request control is not valid UTF-8 JSON.');
        }
        ++$this->nextId;
        $reply = null;
        $ioFailure = null;
        try {
            if (null === $keepaliveIo) {
                $deadline = new TimeoutCancellation($timeout ?? $this->timeout);
                $cancellation = null === $cancellation ? $deadline : new CompositeCancellation($deadline, $cancellation);
                Frame::write($this->socket, $bytes, $cancellation);
                $reply = Frame::read($this->socket, $cancellation, $this->limits);
            } else {
                $keepaliveIo->write($bytes);
                $reply = Frame::readUsing($keepaliveIo->read(...), $this->limits);
            }
        } catch (\Throwable $error) {
            $ioFailure = $error;
        }
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

    /**
     * @param list<mixed> $queues
     *
     * @return list<string>
     */
    private static function validatedWaitQueues(array $queues): array
    {
        if ([] === $queues) {
            throw new \InvalidArgumentException('WAIT_ANY requires at least one queue.');
        }
        if (\count($queues) > Limits::MAX_WAIT_QUEUES) {
            throw new \InvalidArgumentException(\sprintf('WAIT_ANY accepts at most %d queues.', Limits::MAX_WAIT_QUEUES));
        }
        $selected = [];
        $seen = [];
        foreach ($queues as $queue) {
            if (!\is_string($queue)) {
                throw new \InvalidArgumentException('WAIT_ANY queue names must be strings.');
            }
            $name = new QueueName($queue);
            if (isset($seen[$name->value])) {
                throw new \InvalidArgumentException('WAIT_ANY queue names must be unique.');
            }
            $seen[$name->value] = true;
            $selected[] = $name->value;
        }

        return $selected;
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
        $receiptError = $code->receiptException();
        if (null !== $receiptError) {
            return $receiptError;
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
