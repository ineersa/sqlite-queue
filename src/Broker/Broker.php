<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Protocol\ControlField;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Protocol\ProtocolException;
use Ineersa\SqliteQueue\Sqlite\Exception\StorageCapacityException;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueWorker;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Revolt\EventLoop;
use Symfony\Component\Filesystem\Filesystem;

use function Amp\async;

final class Broker
{
    public const int MAX_CONNECTIONS = Limits::MAX_CONNECTIONS;
    private const int HANDSHAKE_TIMEOUT = 5;
    private const int OPERATION_TIMEOUT = 30;
    private const int WRITE_TIMEOUT = 5;
    private const int OWNER_ID_BYTES = 32;
    /** Total budget for one shutdown, in seconds. Every awaited shutdown step shares this deadline. */
    private const int SHUTDOWN_BUDGET_SECONDS = 5;
    /** @var array<int, array{socket: Socket, ownerId: string, lifetime: DeferredCancellation, future: Future<void>}> */
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
    private readonly QueueNotifier $notifier;

    /** Fully acquired by BrokerFactory; a constructed broker is ready to serve. */
    public function __construct(
        private readonly ServerSocket $server,
        private readonly SqliteQueueWorker $worker,
        private readonly BrokerLifetimeLocks $locks,
        private readonly SocketIdentity $socketIdentity,
        private readonly \Closure $clock,
    ) {
        $this->deadline = new DeferredCancellation();
        $this->notifier = new QueueNotifier(
            $this->worker->earliestEligibility(...),
            $this->worker->awaitCapacity(...),
            $this->clock,
            function (\Throwable $error): void {
                $this->failed = true;
                $this->stop();
            },
        );
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
        $subscription = $cancellation?->subscribe(function (): void {
            $this->stop();
        });
        $monitor = async(function (): void {
            try {
                $this->worker->handle()->awaitExit();
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
                $ready?->__invoke([
                    'synchronous_effective' => $this->worker->synchronousMode()->value,
                    'event' => BrokerEventEnum::Ready->value,
                    'pid' => getmypid(),
                    'persistence_pid' => $this->worker->handle()->pid(),
                    'database' => $this->locks->database,
                    'endpoint' => $this->locks->endpoint,
                ]);
            }
            while (!$this->stopping && null !== ($socket = $this->server->accept())) {
                if (\count($this->clients) >= self::MAX_CONNECTIONS) {
                    $socket->close();
                    continue;
                }
                $ownerId = bin2hex(random_bytes(self::OWNER_ID_BYTES));
                $lifetime = new DeferredCancellation();
                $key = spl_object_id($socket);
                $future = async(function () use ($socket, $ownerId, $lifetime, $key): void {
                    try {
                        $this->serve($socket, $ownerId, $lifetime->getCancellation());
                    } finally {
                        $socket->close();
                        $lifetime->cancel();
                        unset($this->clients[$key]);
                    }
                })->catch(function (): void {
                    $this->failed = true;
                    $this->stop();
                });
                $this->clients[$key] = ['socket' => $socket, 'ownerId' => $ownerId, 'lifetime' => $lifetime, 'future' => $future];
            }
        } finally {
            // stop() arms the shared deadline on its first call, before it closes anything, so the
            // budget already covers every step below and this path must never arm a second timer.
            // The referenced timer keeps the loop alive until it fires or is disarmed, so no step
            // is ever released by the loop running out of work instead of by this deadline.
            $this->stop();
            $budget = $this->deadline->getCancellation();
            $failure = null;
            foreach ($this->clients as $client) {
                try {
                    $client['future']->await($budget);
                } catch (\Throwable $error) {
                    $failure ??= $error;
                }
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
            // Identity-checked socket removal and lock release are collected like every other
            // step: a late failure must not mask the earlier error that actually failed the
            // shutdown. They are not budgeted, because the child is already force-stopped and
            // the release must happen before another broker may take the database or endpoint.
            try {
                $this->releaseEndpointAndLocks();
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
            // Synchronously refuse later dispatch before tearing down waiters or sockets.
            $this->worker->beginClose();
        }
        $this->server->close();
        $this->notifier->close();
        foreach ($this->clients as $client) {
            $client['lifetime']->cancel();
            $client['socket']->close();
        }
    }

    /** Arms the single shutdown deadline; only the first stop request may reach this. */
    private function armShutdownDeadline(): void
    {
        $this->shutdownTimer = EventLoop::delay(self::SHUTDOWN_BUDGET_SECONDS, function (): void {
            $this->failed = true;
            // Escalate before releasing the awaits, so the child is dead before any caller resumes.
            $this->releaseBudgetAfter($this->deadline, $this->worker->handle()->forceStop(...));
        });
    }

    /**
     * Runs the shutdown escalation, then releases the budget exactly once.
     *
     * A force-stop failure must not escape into the event loop, and it must not leave the budget
     * unreleased either: the failure becomes the cancellation cause, so the awaiters resume with
     * the real reason instead of waiting for a deadline that already fired.
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

    /** @return list<\Closure(): void> Storage steps whose failure must not skip later steps. */
    private function shutdown(Cancellation $budget): array
    {
        return [
            function () use ($budget): void {
                $this->worker->close($budget);
            },
        ];
    }

    /**
     * Removes this broker's socket inode when it still matches, then releases the lifetime locks.
     *
     * A failed socket removal must not skip lock release.
     */
    private function releaseEndpointAndLocks(): void
    {
        $failure = null;
        if ($this->socketIdentity->matches($this->locks->endpoint)) {
            try {
                (new Filesystem())->remove($this->locks->endpoint);
            } catch (\Throwable $error) {
                $failure = $error;
            }
        }
        try {
            $this->locks->close();
        } catch (\Throwable $error) {
            $failure ??= $error;
        }
        if (null !== $failure) {
            throw $failure;
        }
    }

    /**
     * Wait for one shutdown step under the shared budget.
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

    private function serve(Socket $socket, string $ownerId, Cancellation $lifetime): void
    {
        $expected = 0;
        try {
            while (!$this->stopping) {
                $request = $this->readRequest($socket, $lifetime, $expected);
                if (null === $request) {
                    return;
                }
                $this->assertRunning();
                try {
                    $operation = $this->validateRequest($request, $expected);
                } catch (ProtocolException $error) {
                    $this->writeError($socket, $expected, $error->errorCode);

                    return;
                }
                if (0 === $expected) {
                    $response = new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => Frame::MAX_PAYLOAD]]);
                } else {
                    try {
                        $response = $this->dispatch($request, $operation, $ownerId, $lifetime, $expected, $socket);
                    } catch (ClientContextClosedException) {
                        // Disconnect and shutdown cancellation end the session, not the broker.
                        return;
                    } catch (StorageCapacityException) {
                        // Local admission refusal closes only this session. The storage lane stays up.
                        return;
                    } catch (InvalidReceiptException $error) {
                        $response = self::error($expected, ErrorCode::fromReceiptException($error));
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
                        // Prefer stopping over a potentially five-second error write. The protocol
                        // already allows storage failure to close sockets before an error reply.
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

    private function readRequest(Socket $socket, Cancellation $lifetime, int $expected): ?Frame
    {
        if (0 === $expected) {
            return Frame::read($socket, new CompositeCancellation($lifetime, new TimeoutCancellation(self::HANDSHAKE_TIMEOUT)));
        }

        // An established session may be idle or executing a handler. Bound partial frames,
        // not the time between requests: receipts must retain their original session owner.
        $prefix = $socket->read($lifetime, Limits::LENGTH_PREFIX_BYTES);
        if (null === $prefix) {
            return null;
        }

        return Frame::readAfterPrefix($socket, $prefix, new CompositeCancellation($lifetime, new TimeoutCancellation(self::OPERATION_TIMEOUT)));
    }

    /** Validation runs before dispatch so every rejection reports its own error code. */
    private function validateRequest(Frame $request, int $expected): Operation
    {
        $control = $request->control;
        if (($control[ControlField::Version->value] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::UnsupportedProtocolVersion, 'Unsupported protocol version.');
        }
        $name = $control[ControlField::Operation->value] ?? null;
        $operation = \is_string($name) ? Operation::tryFrom($name) : null;
        if (($control[ControlField::Id->value] ?? null) !== $expected || null === $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid request sequence or operation.');
        }
        $common = [
            ControlField::Version->value,
            ControlField::Id->value,
            ControlField::Operation->value,
            ControlField::BodyLength->value,
            ControlField::HeadersLength->value,
        ];
        if ([] !== array_diff(array_keys($control), [...$common, ...$operation->allowedFields()])) {
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

    private function dispatch(Frame $request, Operation $operation, string $ownerId, Cancellation $lifetime, int $id, Socket $socket): Frame
    {
        if (Operation::Send === $operation) {
            return $this->store($request, $lifetime, $id);
        }
        if (Operation::Receive === $operation) {
            return $this->claim($request, $ownerId, $lifetime, $id);
        }
        if (Operation::Acknowledge === $operation || Operation::Reject === $operation) {
            return $this->settle($request, $operation, $ownerId, $lifetime, $id);
        }
        if (Operation::Wait === $operation) {
            return $this->awaitReadiness($request, $socket, $lifetime, $id);
        }
        throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported operation.');
    }

    private function store(Frame $request, Cancellation $lifetime, int $id): Frame
    {
        $name = $this->queueName($request->control[ControlField::Queue->value] ?? null);
        $delay = $request->control[ControlField::Delay->value] ?? null;
        if (!\is_int($delay)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }
        if ($delay < 0) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }

        $inserted = $this->worker->send($name, $request->body, $request->headers, $delay, $lifetime);
        // Notify only after the committed mutation, even when the publisher session is already gone.
        $this->notifier->notify($name);

        return self::ok($id, $inserted);
    }

    private function claim(Frame $request, string $ownerId, Cancellation $lifetime, int $id): Frame
    {
        $this->assertNoPayload($request);
        $name = $this->queueName($request->control[ControlField::Queue->value] ?? null);
        $delivery = $this->worker->receive($name, $ownerId, $lifetime);
        if (null === $delivery) {
            return self::ok($id, null);
        }
        // A committed claim changes visibility readiness for other waiters.
        $this->notifier->notify($name);
        if ($lifetime->isRequested()) {
            // Keep the reservation until its original expiry; do not deliver to a cancelled session.
            throw new ClientContextClosedException('The client connection lifetime was cancelled.');
        }

        return new Frame([
            ControlField::Version->value => Frame::VERSION,
            ControlField::Id->value => $id,
            ControlField::Ok->value => true,
            ControlField::Result->value => [
                ControlField::Id->value => $delivery->id,
                ControlField::Queue->value => $delivery->queue,
                ControlField::Receipt->value => $delivery->receipt,
                ControlField::AvailableAt->value => $delivery->availableAt,
                ControlField::ReservedUntil->value => $delivery->reservedUntil,
            ],
        ], $delivery->body, $delivery->headers);
    }

    private function awaitReadiness(Frame $request, Socket $socket, Cancellation $lifetime, int $id): Frame
    {
        $this->assertNoPayload($request);
        $name = $this->queueName($request->control[ControlField::Queue->value] ?? null);
        $timeout = $request->control[ControlField::WaitMilliseconds->value] ?? null;
        if (!\is_int($timeout)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid wait timeout.');
        }
        if ($timeout < 0) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Wait timeout must be nonnegative milliseconds.');
        }
        if ($timeout > Limits::MAX_WAIT_MILLISECONDS) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Wait timeout exceeds the protocol maximum.');
        }

        $monitor = new DeferredCancellation();
        $waitCancellation = new CompositeCancellation($lifetime, $monitor->getCancellation());
        $monitorFuture = async(static function () use ($socket, $monitor, $lifetime): void {
            try {
                $chunk = $socket->read(new CompositeCancellation($lifetime, $monitor->getCancellation()));
            } catch (CancelledException) {
                return;
            } catch (\Throwable $error) {
                $monitor->cancel($error);

                return;
            }
            if (null === $chunk) {
                $monitor->cancel();

                return;
            }
            $monitor->cancel(new ProtocolException(ErrorCode::InvalidRequest, 'Pipelined bytes during WAIT are not allowed.'));
        });

        try {
            $ready = $this->notifier->wait($name, $timeout, $waitCancellation);
            $waitCancellation->throwIfRequested();
        } catch (CancelledException $error) {
            $previous = $error->getPrevious();
            if ($previous instanceof ProtocolException) {
                throw $previous;
            }
            if ($this->stopping) {
                throw new ClientContextClosedException('The broker stopped during WAIT.', previous: $error);
            }
            if ($lifetime->isRequested()) {
                throw new ClientContextClosedException('The client connection lifetime was cancelled.', previous: $error);
            }
            throw new ClientContextClosedException('The client connection closed during WAIT.', previous: $error);
        } finally {
            $monitor->cancel();
            try {
                $monitorFuture->await();
            } catch (\Throwable) {
                $monitorFuture->ignore();
            }
        }

        return self::ok($id, $ready);
    }

    private function settle(Frame $request, Operation $operation, string $ownerId, Cancellation $lifetime, int $id): Frame
    {
        $this->assertNoPayload($request);
        $receipt = $request->control[ControlField::Receipt->value] ?? null;
        if (!\is_string($receipt)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Missing receipt.');
        }
        if (Operation::Acknowledge === $operation) {
            $this->worker->acknowledge($receipt, $ownerId, $lifetime);
        } else {
            $this->worker->reject($receipt, $ownerId, $lifetime);
        }

        return self::ok($id, null);
    }

    private function queueName(mixed $name): QueueName
    {
        if (!\is_string($name)) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }
        try {
            return new QueueName($name);
        } catch (\InvalidArgumentException) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }
    }

    private function assertNoPayload(Frame $request): void
    {
        if ('' !== $request->body || '' !== $request->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unexpected application payload.');
        }
    }

    private static function ok(int $id, int|bool|null $result): Frame
    {
        return new Frame([
            ControlField::Version->value => Frame::VERSION,
            ControlField::Id->value => $id,
            ControlField::Ok->value => true,
            ControlField::Result->value => $result,
        ]);
    }

    private static function error(int $id, ErrorCode $code): Frame
    {
        return new Frame([
            ControlField::Version->value => Frame::VERSION,
            ControlField::Id->value => $id,
            ControlField::Ok->value => false,
            ControlField::Error->value => [ControlField::Code->value => $code->value],
        ]);
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
