<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use Ineersa\SqliteQueue\InvalidReceipt;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\ProtocolException;
use Ineersa\SqliteQueue\Queue;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\Socket\listen;

final class Broker
{
    public const int MAX_CONNECTIONS = 64;
    private ?ServerSocket $server = null;
    private ?Queue $queue = null;
    private ?SqliteConnection $connection = null;
    /** @var array<int, array{socket: Socket, session: string, future: Future<void>}> */
    private array $clients = [];
    private bool $stopping = false;
    private bool $started = false;
    private bool $failed = false;

    /** @param (\Closure(): int)|null $clock */
    public function __construct(
        private readonly string $database,
        private readonly string $endpoint,
        private readonly int $visibilityTimeout = 5000,
        private readonly ?\Closure $clock = null,
    ) {
    }

    /** @param (\Closure(array<string, int|string>): void)|null $ready */
    public function run(?\Closure $ready = null, ?Cancellation $cancellation = null): int
    {
        if ($this->started) {
            throw new \LogicException('A broker instance can only run once.');
        }
        $this->started = true;
        $ownership = null;
        $persistence = new Persistence();
        $monitor = null;
        $subscription = $cancellation?->subscribe($this->stop(...));
        try {
            $cancellation?->throwIfRequested();
            $ownership = new Ownership($this->database, $this->endpoint);
            $config = (new SqliteConfig($ownership->database))
                ->withJournalMode(SqliteJournalMode::Wal)
                ->withSynchronousMode(SqliteSynchronousMode::Full)
                ->withTransactionMode(SqliteTransactionMode::Immediate)
                ->withBusyTimeout(5000);
            $this->connection = (new SqliteConnector($persistence))->connect($config, $cancellation);
            $this->queue = new Queue($this->connection, $this->visibilityTimeout, $this->clock);
            $monitor = async(function () use ($persistence): void {
                try {
                    $persistence->awaitExit();
                } catch (\Throwable) {
                    // A failed result channel also signals child death. Never log worker content.
                }
                if (!$this->stopping) {
                    $this->failed = true;
                    $this->stop();
                }
            });
            $mask = umask(0077);
            try {
                $this->server = listen('unix://'.$ownership->endpoint);
            } finally {
                umask($mask);
            }
            $ownership->recordSocket();
            $cancellation?->throwIfRequested();
            if (!$this->stopping) {
                $ready?->__invoke(['event' => 'ready', 'pid' => getmypid(), 'persistence_pid' => $persistence->pid(), 'database' => $ownership->database, 'endpoint' => $ownership->endpoint]);
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
                        $this->queue?->closeSession($session);
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
            $deadline = EventLoop::delay(5, function (): void {
                $this->failed = true;
                $this->connection?->close();
            });
            try {
                foreach ($this->clients as $client) {
                    $client['future']->await(new TimeoutCancellation(6));
                }
                $this->queue?->close();
                $this->connection?->close();
                $persistence->close();
                $monitor?->await(new TimeoutCancellation(5));
            } finally {
                EventLoop::cancel($deadline);
                if (null !== $subscription) {
                    $cancellation?->unsubscribe($subscription);
                }
                $ownership?->close();
            }
        }

        return $this->failed ? 1 : 0;
    }

    public function stop(): void
    {
        $this->stopping = true;
        $this->server?->close();
        foreach ($this->clients as $client) {
            $this->queue?->closeSession($client['session']);
            $client['socket']->close();
        }
    }

    private function serve(Socket $socket, string $session): void
    {
        $expected = 0;
        try {
            while (!$this->stopping) {
                $request = Frame::read($socket, new TimeoutCancellation(0 === $expected ? 5 : 30));
                if (null === $request) {
                    return;
                }
                $this->assertRunning();
                $operation = $this->validateRequest($request, $expected);
                if (0 === $expected) {
                    $response = new Frame(['v' => Frame::VERSION, 'id' => 0, 'ok' => true, 'result' => ['max_payload' => Frame::MAX_PAYLOAD]]);
                } else {
                    try {
                        $response = $this->dispatch($request, $operation, $session, $expected);
                    } catch (InvalidReceipt) {
                        $response = self::error($expected, 'stale_receipt');
                    } catch (ProtocolException $error) {
                        if ('invalid_queue_name' !== $error->errorCode) {
                            throw $error;
                        }
                        $response = self::error($expected, $error->errorCode);
                    } catch (\InvalidArgumentException) {
                        throw new ProtocolException('invalid_request', 'Invalid operation arguments.');
                    } catch (\Throwable) {
                        $this->failed = true;
                        $this->stopping = true;
                        $this->server?->close();
                        throw new ProtocolException('internal_storage_failure', 'Storage unavailable; outcome may be unknown.');
                    }
                }
                Frame::write($socket, $response->encode(), new TimeoutCancellation(5));
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
            Frame::write($socket, $response->encode(), new TimeoutCancellation(5));
        } catch (\Throwable) {
            // A malformed or disconnected peer may be unable to receive its error.
        }
    }

    /** Validation runs before dispatch so every rejection reports its own error code. */
    private function validateRequest(Frame $request, int $expected): string
    {
        $control = $request->control;
        $operation = $control['op'] ?? null;
        if (($control['v'] ?? null) !== Frame::VERSION) {
            throw new ProtocolException('unsupported_protocol_version', 'Unsupported protocol version.');
        }
        if (($control['id'] ?? null) !== $expected || !\is_string($operation)) {
            throw new ProtocolException('invalid_request', 'Invalid request sequence or operation.');
        }
        $fields = match ($operation) {
            'hello' => [],
            'send' => ['queue', 'delay'],
            'receive' => ['queue'],
            'acknowledge', 'reject' => ['receipt'],
            default => throw new ProtocolException('invalid_request', 'Unsupported operation.'),
        };
        if ([] !== array_diff(array_keys($control), ['v', 'id', 'op', 'body_length', 'headers_length', ...$fields])) {
            throw new ProtocolException('invalid_request', 'Unsupported control field.');
        }
        if (0 === $expected && ('hello' !== $operation || '' !== $request->body || '' !== $request->headers)) {
            throw new ProtocolException('invalid_request', 'Handshake required.');
        }

        return $operation;
    }

    private function dispatch(Frame $request, string $operation, string $session, int $id): Frame
    {
        $control = $request->control;
        if ('send' !== $operation && ('' !== $request->body || '' !== $request->headers)) {
            throw new ProtocolException('invalid_request', 'Unexpected application payload.');
        }
        $queue = $this->queue ?? throw new \LogicException('Storage not initialized.');
        if (\in_array($operation, ['send', 'receive'], true)) {
            $name = $control['queue'] ?? null;
            if (!\is_string($name) || 1 !== preg_match('/\A[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}\z/D', $name)) {
                throw new ProtocolException('invalid_queue_name', 'Invalid queue name.');
            }
            if ('send' === $operation) {
                $delay = $control['delay'] ?? null;
                if (!\is_int($delay) || $delay < 0) {
                    throw new ProtocolException('invalid_request', 'Invalid delay.');
                }
                $result = $queue->send($name, $request->body, $request->headers, $delay);
            } else {
                $delivery = $queue->receive($name, $session);
                if (null !== $delivery) {
                    return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => true, 'result' => [
                        'id' => $delivery->id, 'queue' => $delivery->queue, 'receipt' => $delivery->receipt,
                        'available_at' => $delivery->availableAt, 'reserved_until' => $delivery->reservedUntil,
                    ]], $delivery->body, $delivery->headers);
                }
                $result = null;
            }
        } elseif (\in_array($operation, ['acknowledge', 'reject'], true)) {
            $receipt = $control['receipt'] ?? null;
            if (!\is_string($receipt)) {
                throw new ProtocolException('invalid_request', 'Missing receipt.');
            }
            if ('acknowledge' === $operation) {
                $queue->acknowledge($receipt, $session);
            } else {
                $queue->reject($receipt, $session);
            }
            $result = null;
        } else {
            throw new ProtocolException('invalid_request', 'Unsupported operation.');
        }

        return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => true, 'result' => $result]);
    }

    private static function error(int $id, string $code): Frame
    {
        return new Frame(['v' => Frame::VERSION, 'id' => $id, 'ok' => false, 'error' => ['code' => $code]]);
    }

    private function assertRunning(): void
    {
        // Shutdown may have run in another fiber while a frame read was suspended.
        if ($this->stopping) {
            throw new ProtocolException('broker_shutting_down', 'Broker is stopping.');
        }
    }
}
