<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Ineersa\SqliteQueue\InvalidReceipt;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\ProtocolException;
use Ineersa\SqliteQueue\Queue;
use Revolt\EventLoop;

use function Amp\async;

final class Broker
{
    public const int MAX_CONNECTIONS = 64;
    private const int IDLE_READ_TIMEOUT = 5;
    private const int OPERATION_TIMEOUT = 30;
    private const int WRITE_TIMEOUT = 5;
    /** Total budget for one shutdown, in seconds. Every awaited shutdown step shares this deadline. */
    private const int SHUTDOWN_BUDGET_SECONDS = 5;
    /** @var array<int, array{socket: Socket, session: string, future: Future<void>}> */
    private array $clients = [];
    private bool $stopping = false;
    private bool $started = false;
    private bool $failed = false;

    /** Fully acquired by BrokerFactory; a constructed broker is ready to serve. */
    public function __construct(
        private readonly ServerSocket $server,
        private readonly Queue $queue,
        private readonly SqliteConnection $connection,
        private readonly Persistence $persistence,
        private readonly Ownership $ownership,
    ) {
    }

    /**
     * @param (\Closure(array<string, int|string>): void)|null $ready        optional observer notified once after the socket accepts work; process creation alone is not readiness
     * @param ?Cancellation                                    $cancellation optional cooperative cancellation for the serving loop; startup cancellation belongs to BrokerFactory
     */
    public function run(?\Closure $ready = null, ?Cancellation $cancellation = null): int
    {
        if ($this->started) {
            throw new \LogicException('A broker instance can only run once.');
        }
        $this->started = true;
        $subscription = $cancellation?->subscribe($this->stop(...));
        $monitor = async(function (): void {
            try {
                $this->persistence->awaitExit();
            } catch (\Throwable) {
                // A failed result channel also signals child death. Never log worker content.
            }
            if (!$this->stopping) {
                $this->failed = true;
                $this->stop();
            }
        });
        try {
            $cancellation?->throwIfRequested();
            if (!$this->stopping) {
                $ready?->__invoke(['event' => 'ready', 'pid' => getmypid(), 'persistence_pid' => $this->persistence->pid(), 'database' => $this->ownership->database, 'endpoint' => $this->ownership->endpoint]);
            }
            while (!$this->stopping && null !== ($socket = $this->server->accept())) {
                if (\count($this->clients) >= self::MAX_CONNECTIONS) {
                    $socket->close();
                    continue;
                }
                $session = $this->queue->openSession();
                $key = spl_object_id($socket);
                $future = async(function () use ($socket, $session, $key): void {
                    try {
                        $this->serve($socket, $session);
                    } finally {
                        $socket->close();
                        $this->queue->closeSession($session);
                        unset($this->clients[$key]);
                    }
                })->catch(function (): void {
                    $this->failed = true;
                    $this->stop();
                });
                $this->clients[$key] = ['socket' => $socket, 'session' => $session, 'future' => $future];
            }
        } finally {
            $this->stop();
            // One deadline for the whole shutdown: every awaited step below waits on this same
            // cancellation, so no step can start a fresh timer and stretch the budget. The
            // budget covers the client drain too, where in-flight SQL may stall. The timer is
            // referenced, so it keeps the loop alive until it fires: no step is ever released by
            // the loop running out of work instead of by this deadline.
            $deadline = new DeferredCancellation();
            $timer = EventLoop::delay(self::SHUTDOWN_BUDGET_SECONDS, function () use ($deadline): void {
                // Escalate before releasing the awaits, so the child is dead before any caller
                // resumes and a step stuck on that child cannot hold the shutdown. Killing the
                // owned child is the only action that releases a storage worker that stopped
                // answering: the driver marks its connection closed before its graceful close
                // returns, so repeating that close cannot interrupt such a worker.
                $this->failed = true;
                $this->persistence->forceStop();
                $deadline->cancel(new \RuntimeException('Broker shutdown budget exhausted.'));
            });
            $budget = $deadline->getCancellation();
            $failure = null;
            try {
                foreach ($this->clients as $client) {
                    $client['future']->await($budget);
                }
            } catch (\Throwable $error) {
                $failure = $error;
            }
            // Every step runs: an earlier failure must not leave the persistence child alive.
            foreach ($this->shutdown($budget) as $step) {
                try {
                    $this->awaitStep($step, $budget);
                } catch (\Throwable $error) {
                    $failure ??= $error;
                }
            }
            try {
                $monitor->await($budget);
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
            EventLoop::cancel($timer);
            if (null !== $subscription) {
                $cancellation?->unsubscribe($subscription);
            }
            // Ownership release is collected like every other step: a late release failure
            // must not mask the earlier error that actually failed the shutdown. It is not
            // budgeted, because the child is already force-stopped and the release must happen
            // before another broker may take the database or the endpoint.
            try {
                $this->ownership->close();
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
            if (null !== $failure) {
                throw $failure;
            }
        }

        return $this->failed ? 1 : 0;
    }

    public function stop(): void
    {
        $this->stopping = true;
        $this->server->close();
        foreach ($this->clients as $client) {
            $this->queue->closeSession($client['session']);
            $client['socket']->close();
        }
    }

    /** @return list<\Closure(): void> Storage steps whose failure must not skip later steps. */
    private function shutdown(Cancellation $budget): array
    {
        return [
            $this->queue->close(...),
            $this->connection->close(...),
            // The engine and the driver close through the same connection, so the persistence
            // pipes are observed with the shared budget as well.
            function () use ($budget): void {
                $this->persistence->close($budget);
            },
        ];
    }

    /**
     * Wait for one shutdown step under the shared budget.
     *
     * The engine and driver closes accept no Cancellation, so the step runs in its own fiber
     * and the caller waits on it with the shared deadline. A step abandoned at the deadline
     * keeps running; its later outcome is ignored because the caller has already escalated
     * and cannot act on it.
     *
     * @param \Closure(): void $step
     */
    private function awaitStep(\Closure $step, Cancellation $budget): void
    {
        $future = async($step);
        try {
            $future->await($budget);
        } catch (CancelledException $cancelled) {
            $future->ignore();
            throw $cancelled;
        }
    }

    private function serve(Socket $socket, string $session): void
    {
        $expected = 0;
        try {
            while (!$this->stopping) {
                $request = Frame::read($socket, new TimeoutCancellation(0 === $expected ? self::IDLE_READ_TIMEOUT : self::OPERATION_TIMEOUT));
                if (null === $request) {
                    return;
                }
                $this->assertRunning();
                try {
                    $operation = $this->validateRequest($request, $expected);
                } catch (ProtocolException $error) {
                    // Answer decode errors where they are caught: the reply below ends this
                    // session unless the queue name alone was bad.
                    $this->writeError($socket, $expected, $error->errorCode);
                    if (ErrorCode::InvalidQueueName !== $error->errorCode) {
                        return;
                    }
                    // A bad queue name stays recoverable: the session survives and the sequence advances.
                    ++$expected;
                    continue;
                }
                if (0 === $expected) {
                    $response = new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => Frame::MAX_PAYLOAD]]);
                } else {
                    try {
                        $response = $this->dispatch($request, $operation, $session, $expected);
                    } catch (InvalidReceipt) {
                        $response = self::error($expected, ErrorCode::StaleReceipt);
                    } catch (ProtocolException $error) {
                        if (ErrorCode::InvalidQueueName === $error->errorCode) {
                            // A bad queue name stays recoverable: the session survives and the sequence advances.
                            $response = self::error($expected, $error->errorCode);
                        } else {
                            $this->writeError($socket, $expected, $error->errorCode);

                            return;
                        }
                    } catch (\InvalidArgumentException) {
                        $this->writeError($socket, $expected, ErrorCode::InvalidRequest);

                        return;
                    } catch (\Throwable) {
                        $this->failed = true;
                        $this->stopping = true;
                        $this->server->close();
                        $this->writeError($socket, $expected, ErrorCode::InternalStorageFailure);

                        return;
                    }
                }
                Frame::write($socket, $response->encode(), new TimeoutCancellation(self::WRITE_TIMEOUT));
                ++$expected;
            }

            return;
        } catch (ProtocolException $error) {
            $response = self::error($expected, $error->errorCode);
        } catch (\Throwable) {
            // Read/write timeout, cancellation, or peer failure ends only this session.
            return;
        }
        try {
            Frame::write($socket, $response->encode(), new TimeoutCancellation(self::WRITE_TIMEOUT));
        } catch (\Throwable) {
            // A malformed or disconnected peer may be unable to receive its error.
        }
    }

    /** Validation runs before dispatch so every rejection reports its own error code. */
    private function validateRequest(Frame $request, int $expected): Operation
    {
        $control = $request->control;
        if (($control['v'] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::UnsupportedProtocolVersion, 'Unsupported protocol version.');
        }
        $name = $control['op'] ?? null;
        $operation = \is_string($name) ? Operation::tryFrom($name) : null;
        if (($control['id'] ?? null) !== $expected || null === $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid request sequence or operation.');
        }
        $fields = match ($operation) {
            Operation::Hello => [],
            Operation::Send => ['queue', 'delay'],
            Operation::Receive => ['queue'],
            Operation::Acknowledge, Operation::Reject => ['receipt'],
        };
        if ([] !== array_diff(array_keys($control), ['v', 'id', 'op', 'body_length', 'headers_length', ...$fields])) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported control field.');
        }
        if (0 === $expected && (Operation::Hello !== $operation || '' !== $request->body || '' !== $request->headers)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Handshake required.');
        }
        if (0 !== $expected && Operation::Hello === $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Handshake required.');
        }

        return $operation;
    }

    private function dispatch(Frame $request, Operation $operation, string $session, int $id): Frame
    {
        if (Operation::Send === $operation) {
            return $this->store($request, $id);
        }
        if (Operation::Receive === $operation) {
            return $this->claim($request, $session, $id);
        }
        if (Operation::Acknowledge === $operation || Operation::Reject === $operation) {
            return $this->settle($request, $operation, $session, $id);
        }
        throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported operation.');
    }

    private function store(Frame $request, int $id): Frame
    {
        $name = $this->queueName($request->control['queue'] ?? null);
        $delay = $request->control['delay'] ?? null;
        if (!\is_int($delay)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }
        if ($delay < 0) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }

        return self::ok($id, $this->queue->send($name, $request->body, $request->headers, $delay));
    }

    private function claim(Frame $request, string $session, int $id): Frame
    {
        $this->assertNoPayload($request);
        $name = $this->queueName($request->control['queue'] ?? null);
        $delivery = $this->queue->receive($name, $session);
        if (null === $delivery) {
            return self::ok($id, null);
        }

        return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => true, 'result' => [
            'id' => $delivery->id, 'queue' => $delivery->queue, 'receipt' => $delivery->receipt,
            'available_at' => $delivery->availableAt, 'reserved_until' => $delivery->reservedUntil,
        ]], $delivery->body, $delivery->headers);
    }

    private function settle(Frame $request, Operation $operation, string $session, int $id): Frame
    {
        $this->assertNoPayload($request);
        $receipt = $request->control['receipt'] ?? null;
        if (!\is_string($receipt)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Missing receipt.');
        }
        if (Operation::Acknowledge === $operation) {
            $this->queue->acknowledge($receipt, $session);
        } else {
            $this->queue->reject($receipt, $session);
        }

        return self::ok($id, null);
    }

    private function queueName(mixed $name): string
    {
        if (!\is_string($name)) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }
        if (1 !== preg_match(Queue::NAME_PATTERN, $name)) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }

        return $name;
    }

    private function assertNoPayload(Frame $request): void
    {
        if ('' !== $request->body || '' !== $request->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unexpected application payload.');
        }
    }

    private static function ok(int $id, ?int $result): Frame
    {
        return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => true, 'result' => $result]);
    }

    private static function error(int $id, ErrorCode $code): Frame
    {
        return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => false, 'error' => ['code' => $code->value]]);
    }

    /** Best-effort error reply: a malformed or disconnected peer may be unable to receive it. */
    private function writeError(Socket $socket, int $id, ErrorCode $code): void
    {
        try {
            Frame::write($socket, self::error($id, $code)->encode(), new TimeoutCancellation(self::WRITE_TIMEOUT));
        } catch (\Throwable) {
        }
    }

    private function assertRunning(): void
    {
        // Shutdown may have run in another fiber while a frame read was suspended.
        if ($this->stopping) {
            throw new ProtocolException(ErrorCode::BrokerShuttingDown, 'Broker is stopping.');
        }
    }
}
