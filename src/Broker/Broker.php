<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Ineersa\SqliteQueue\InvalidReceipt;
use Ineersa\SqliteQueue\Protocol\AcknowledgeRequest;
use Ineersa\SqliteQueue\Protocol\EmptyReceiveResponse;
use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\FailedResponse;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\HelloRequest;
use Ineersa\SqliteQueue\Protocol\HelloResponse;
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
    private const int SHUTDOWN_WATCHDOG_TIMEOUT = 5;
    private const int CLIENT_DRAIN_TIMEOUT = 6;
    private const int MONITOR_TIMEOUT = 5;
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
            // Total shutdown budget: the watchdog fires five seconds after shutdown starts no
            // matter which step is stuck — including the client drain, where in-flight SQL may
            // stall. The driver marks its connection closed before its graceful close returns,
            // so repeating that close cannot interrupt a worker that stopped answering; only
            // killing the owned child releases the database and endpoint locks on time. The
            // per-step timeouts below are backstops for the remainder, not budget extensions.
            $watchdog = EventLoop::delay(self::SHUTDOWN_WATCHDOG_TIMEOUT, function (): void {
                $this->failed = true;
                $this->persistence->forceStop();
            });
            $failure = null;
            try {
                foreach ($this->clients as $client) {
                    $client['future']->await(new TimeoutCancellation(self::CLIENT_DRAIN_TIMEOUT));
                }
            } catch (\Throwable $error) {
                $failure = $error;
            }
            // Every step runs: an earlier failure must not leave the persistence child alive.
            foreach ($this->shutdown() as $step) {
                try {
                    $step();
                } catch (\Throwable $error) {
                    $failure ??= $error;
                }
            }
            try {
                $monitor->await(new TimeoutCancellation(self::MONITOR_TIMEOUT));
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
            EventLoop::cancel($watchdog);
            if (null !== $subscription) {
                $cancellation?->unsubscribe($subscription);
            }
            // Ownership release is collected like every other step: a late release failure
            // must not mask the earlier error that actually failed the shutdown.
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

    /** @return list<callable(): void> Storage steps whose failure must not skip later steps. */
    private function shutdown(): array
    {
        return [
            $this->queue->close(...),
            $this->connection->close(...),
            $this->persistence->close(...),
        ];
    }

    private function serve(Socket $socket, string $session): void
    {
        $expected = 0;
        try {
            while (!$this->stopping) {
                $frame = Frame::read($socket, new TimeoutCancellation(0 === $expected ? self::IDLE_READ_TIMEOUT : self::OPERATION_TIMEOUT));
                if (null === $frame) {
                    return;
                }
                $this->assertRunning();
                try {
                    $request = RequestCodec::decode($frame, $expected);
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
                if ($request instanceof HelloRequest) {
                    $response = new HelloResponse($expected, Frame::MAX_PAYLOAD);
                } else {
                    try {
                        $response = $this->dispatch($request, $session);
                    } catch (InvalidReceipt) {
                        $response = new FailedResponse($expected, ErrorCode::StaleReceipt);
                    } catch (ProtocolException $error) {
                        $this->writeError($socket, $expected, $error->errorCode);

                        return;
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
                Frame::write($socket, ResponseCodec::encode($response)->encode(), new TimeoutCancellation(self::WRITE_TIMEOUT));
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

    private function dispatch(Request $request, string $session): Response
    {
        if ($request instanceof SendRequest) {
            return new SentResponse($request->id, $this->queue->send($request->queue, $request->body, $request->headers, $request->delay));
        }
        if ($request instanceof ReceiveRequest) {
            $delivery = $this->queue->receive($request->queue, $session);
            if (null !== $delivery) {
                return new ReceivedResponse($request->id, $delivery);
            }

            return new EmptyReceiveResponse($request->id);
        }
        if ($request instanceof AcknowledgeRequest) {
            $this->queue->acknowledge($request->receipt, $session);

            return new SettledResponse($request->id);
        }
        if ($request instanceof RejectRequest) {
            $this->queue->reject($request->receipt, $session);

            return new SettledResponse($request->id);
        }
        throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported operation.');
    }

    private static function error(int $id, ErrorCode $code): Frame
    {
        return ResponseCodec::encode(new FailedResponse($id, $code));
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
