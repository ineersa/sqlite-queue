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
    /**
     * One deadline for the whole shutdown, created with the broker so no shutdown path can arm a
     * second one. It is created here but armed on the first stop request: a serving broker must
     * not hold a loop watcher, so the timer is genuinely absent until shutdown is requested.
     */
    private readonly DeferredCancellation $deadline;
    /** Pending shutdown timer, or null before the first stop request and after the disarm. */
    private ?string $shutdownTimer = null;
    /** Optional non-payload lifecycle observer. Null, the default, writes nothing at all. */
    private ?\Closure $diagnostic = null;

    /** Fully acquired by BrokerFactory; a constructed broker is ready to serve. */
    public function __construct(
        private readonly ServerSocket $server,
        private readonly Queue $queue,
        private readonly SqliteConnection $connection,
        private readonly Persistence $persistence,
        private readonly Ownership $ownership,
    ) {
        $this->deadline = new DeferredCancellation();
    }

    /**
     * @param (\Closure(array<string, int|string>): void)|null                         $ready        optional observer notified once after the socket accepts work; process creation alone is not readiness
     * @param ?Cancellation                                                            $cancellation optional cooperative cancellation for the serving loop; startup cancellation belongs to BrokerFactory
     * @param (\Closure(array{event: string, pid: int, monotonic_ns: int}): void)|null $diagnostic   optional non-payload
     *                                                                                               lifecycle observer for the CLI trace file and for tests; the default emits nothing. It is a separate
     *                                                                                               observer rather than part of $ready, which is notified once and must not be completed twice.
     */
    public function run(?\Closure $ready = null, ?Cancellation $cancellation = null, ?\Closure $diagnostic = null): int
    {
        if ($this->started) {
            throw new \LogicException('A broker instance can only run once.');
        }
        $this->started = true;
        $this->diagnostic = $diagnostic;
        // Record delivery before stop() so a wedged trace can separate cancel() from the stop path.
        // Observation failures must never prevent stopping.
        $subscription = $cancellation?->subscribe(function (): void {
            $this->diagnose('cancellation-delivered');
            $this->stop();
        });
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
            // stop() arms the shared deadline on its first call, before it closes anything, so the
            // budget already covers every step below and this path must never arm a second timer.
            // The referenced timer keeps the loop alive until it fires or is disarmed, so no step
            // is ever released by the loop running out of work instead of by this deadline.
            $this->stop();
            $budget = $this->deadline->getCancellation();
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
            $this->disarmShutdownDeadline();
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
        if (!$this->stopping) {
            $this->stopping = true;
            // The deadline is armed on the first shutdown request, before the server or a client
            // socket closes, so the budget covers every step that follows. Later requests, including
            // the serving loop's own stop() in its finally, share that one deadline and never rearm it.
            $this->armShutdownDeadline();
        }
        $this->server->close();
        foreach ($this->clients as $client) {
            $this->queue->closeSession($client['session']);
            $client['socket']->close();
        }
    }

    /** Arms the single shutdown deadline; only the first stop request may reach this. */
    private function armShutdownDeadline(): void
    {
        // The timer is installed before any observer runs, so an observation can never prevent arming.
        $this->shutdownTimer = EventLoop::delay(self::SHUTDOWN_BUDGET_SECONDS, function (): void {
            $this->failed = true;
            $this->diagnose('deadline-fired');
            // The kill is the only action that releases a storage worker that stopped answering:
            // the driver marks its connection closed before its graceful close returns, so a
            // repeated close cannot interrupt such a worker. Escalate before releasing the awaits,
            // so the child is dead before any caller resumes.
            $this->releaseBudgetAfter($this->deadline, $this->persistence->forceStop(...));
        });
        $this->diagnose('shutdown-requested');
        $this->diagnose('deadline-armed');
    }

    /**
     * Runs the shutdown escalation, then releases the budget exactly once.
     *
     * A force-stop failure must not escape into the event loop, and it must not leave the budget
     * unreleased either: the failure becomes the cancellation cause, so the awaiters resume with
     * the real reason instead of waiting for a deadline that already fired. Releasing the budget
     * happens before the observation, so a broken observer cannot delay the awaiters.
     *
     * @param DeferredCancellation $deadline  the single shutdown budget to release
     * @param \Closure(): void     $forceStop
     */
    private function releaseBudgetAfter(DeferredCancellation $deadline, \Closure $forceStop): void
    {
        $cause = new \RuntimeException('Broker shutdown budget exhausted.');
        try {
            $forceStop();
        } catch (\Throwable $error) {
            $cause = $error;
        } finally {
            $deadline->cancel($cause);
            $this->diagnose('persistence-force-stop');
        }
    }

    /** The deadline has served its purpose once every awaited shutdown step has returned. */
    private function disarmShutdownDeadline(): void
    {
        if (null === $this->shutdownTimer) {
            return;
        }
        EventLoop::cancel($this->shutdownTimer);
        $this->shutdownTimer = null;
    }

    /** Reports one shutdown milestone to the optional observer; no payload, nothing when disabled. */
    private function diagnose(string $milestone): void
    {
        $observer = $this->diagnostic;
        if (null === $observer) {
            return;
        }
        try {
            $observer(['event' => $milestone, 'pid' => (int) getmypid(), 'monotonic_ns' => (int) hrtime(true)]);
        } catch (\Throwable) {
            // Observation only: a failing observer must never arm, release, or skip cleanup.
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
                        $this->writeError($socket, $expected, ErrorCode::InternalStorageFailure);
                        // Same shutdown entry as every other path, so exactly one place arms the
                        // shared deadline. The reply above is flushed before stop() closes sockets.
                        $this->stop();

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
