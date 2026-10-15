<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\ByteStream\BufferedReader;
use Amp\ByteStream\StreamChannel;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Sqlite\QueueSql;
use Ineersa\SqliteQueue\Sqlite\QueueStorageOperation;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\DeadlineQueueWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\ProcessTestCase;
use Ineersa\SqliteQueue\Tests\Support\QueueFaultContextFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\TimerCallback;

use function Amp\async;
use function Amp\Socket\listen;

final class StorageDeadlineTest extends ProcessTestCase
{
    private const float STORAGE_SECONDS = 11;
    private const float SAFETY_SECONDS = 5;

    protected function setUp(): void
    {
        if (0 === posix_geteuid()) {
            $this->markTestSkipped('Worker termination tests cannot run as root.');
        }
        if (false !== getenv('HATFIELD_SESSION_ID')) {
            $this->markTestSkipped('Run worker termination tests with HATFIELD_SESSION_ID unset.');
        }
        parent::setUp();
    }

    public static function operations(): iterable
    {
        yield 'send' => [Operation::Send, QueueStorageOperation::Insert];
        yield 'receive' => [Operation::Receive, QueueStorageOperation::Claim];
        yield 'ack' => [Operation::Acknowledge, QueueStorageOperation::Settle];
        yield 'reject' => [Operation::Reject, QueueStorageOperation::Settle];
        yield 'keepalive' => [Operation::Keepalive, QueueStorageOperation::Keepalive];
        yield 'wait lookup' => [Operation::Wait, QueueStorageOperation::Eligibility];
        yield 'wait_any lookup' => [Operation::WaitAny, QueueStorageOperation::Eligibility];
    }

    #[DataProvider('operations')]
    public function testDeadlineStopsAdmissionsClosesWorkerAndPreservesConfirmedData(Operation $operation, QueueStorageOperation $storageOperation): void
    {
        $before = EventLoop::getIdentifiers();
        $broker = $this->broker();
        $running = async($broker->run(...));
        $running->ignore();
        $endpoint = $this->database->path('broker.sock');
        $client = Client::connect($endpoint);
        $queuedClient = Client::connect($endpoint);
        $storage = (new \ReflectionProperty($broker, 'storage'))->getValue($broker);
        $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
        $channel = (new \ReflectionProperty($connection, 'channel'))->getValue($connection);
        $context = (new \ReflectionProperty($channel, 'context'))->getValue($channel);
        $this->assertInstanceOf(ProcessContext::class, $context);
        $process = (new \ReflectionProperty(ProcessContext::class, 'process'))->getValue($context);
        $this->assertInstanceOf(Process::class, $process);
        $server = listen('127.0.0.1:0');
        try {
            $client->send('confirmed', 'prior-confirmation');
            $client->send('jobs', 'target');
            $receipts = [];
            if (\in_array($operation, [Operation::Acknowledge, Operation::Reject, Operation::Keepalive], true)) {
                $delivery = $client->receive('jobs');
                $this->assertNotNull($delivery);
                $receipts[] = $delivery->receipt;
            }
            $this->assertSame([], $this->timers(self::STORAGE_SECONDS), 'Completed operations must release their deadlines.');
            file_put_contents($this->database->path('control.json'), json_encode(['operation' => $storageOperation->value, 'uri' => 'tcp://'.$server->getAddress()->toString()], \JSON_THROW_ON_ERROR));
            $operationFuture = async(static fn () => match ($operation) {
                Operation::Send => $client->send('jobs', 'unconfirmed'),
                Operation::Receive => $client->receive('jobs'),
                Operation::Acknowledge => $client->acknowledge($receipts[0]),
                Operation::Reject => $client->reject($receipts[0]),
                Operation::Keepalive => $client->keepalive($receipts[0]),
                Operation::Wait => $client->wait('empty', 30_000),
                Operation::WaitAny => $client->waitAny(['empty', 'other'], 30_000),
                default => throw new \LogicException('Unsupported deadline case.'),
            });
            $operationFuture->ignore();
            $barrier = $server->accept(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertNotNull($barrier);
            $reader = new BufferedReader($barrier);
            $this->assertSame($storageOperation->value, $reader->readUntil("\n", new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertFalse($operationFuture->isComplete());
            $initialCalls = Operation::WaitAny === $operation ? 2 : 1;
            $this->until(fn (): bool => $initialCalls === \count($this->timers(self::STORAGE_SECONDS)));
            $timers = $this->timers(self::STORAGE_SECONDS);
            $this->assertCount($initialCalls, $timers);
            // A second public request must already be waiting for serial executor ownership.
            $queued = async(static fn (): int => $queuedClient->send('jobs', 'must-not-dispatch'));
            $queued->ignore();
            $executor = (new \ReflectionProperty($storage, 'executor'))->getValue($storage);
            $pending = new \ReflectionProperty($executor, 'pendingOperations');
            $this->until(static fn (): bool => $initialCalls + 1 === $pending->getValue($executor));
            $this->assertCount($initialCalls + 1, $this->timers(self::STORAGE_SECONDS), 'Executor ownership waiting consumes its own overall budget.');
            $this->fire(reset($timers));
            $this->assertSame(1, $running->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            foreach ([$operationFuture, $queued] as $future) {
                try {
                    $future->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                    $this->fail('A timed-out or queued request cannot receive confirmation.');
                } catch (TransportException) {
                    $this->addToAssertionCount(1);
                }
            }
            // Join the process, never the driver's already-owned Context result channel.
            $process->join(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertFalse($process->isRunning());
            $this->assertTrue($context->isClosed());
            $this->assertTrue($connection->isClosed());
            $this->assertTrue($executor->isClosed());
            $this->assertSame(0, $pending->getValue($executor));
            $this->assertNull($barrier->read(new TimeoutCancellation(self::SAFETY_SECONDS)), 'The worker exited without a release or replay.');
            $this->assertFileDoesNotExist($endpoint);
            $this->assertSame([], $this->timers(self::STORAGE_SECONDS));
            $this->assertNull((new \ReflectionProperty($broker, 'shutdownTimer'))->getValue($broker));
            $observer = new \SQLite3($this->database->path());
            try {
                $observer->exec('BEGIN IMMEDIATE');
                $this->assertSame(1, $observer->querySingle("SELECT count(*) FROM queue_messages WHERE CAST(body AS TEXT) = 'prior-confirmation'"));
                $this->assertSame(0, $observer->querySingle("SELECT count(*) FROM queue_messages WHERE CAST(body AS TEXT) IN ('unconfirmed', 'must-not-dispatch')"));
                $observer->exec('ROLLBACK');
            } finally {
                $observer->close();
            }
            $replacement = (new BrokerFactory($this->database->path(), $endpoint))->create();
            $replacement->stop();
            $this->assertSame(0, $replacement->run());
            $barrier->close();
        } finally {
            $client->close();
            $queuedClient->close();
            $broker->stop();
            $server->close();
            $running->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        }
        $this->turn();
        // Closed stream objects can retain disabled watcher IDs until these local spies die.
        $active = array_filter(array_diff(EventLoop::getIdentifiers(), $before), EventLoop::isEnabled(...));
        $this->assertSame([], array_values($active), 'No active operation, future or worker watcher may remain.');
    }

    public function testWaitAndIdleSessionDoNotHoldStorageDeadline(): void
    {
        $broker = $this->broker();
        $running = async($broker->run(...));
        $client = Client::connect($this->database->path('broker.sock'));
        $cancel = new DeferredCancellation();
        try {
            $this->assertSame([], $this->timers(self::STORAGE_SECONDS));
            $waiting = async(static fn (): bool => $client->wait('empty', 30_000, $cancel->getCancellation()));
            $waiting->ignore();
            $notifier = (new \ReflectionProperty($broker, 'notifier'))->getValue($broker);
            $watches = new \ReflectionProperty(QueueNotifier::class, 'watches');
            $this->until(static function () use ($notifier, $watches): bool {
                $current = $watches->getValue($notifier);

                return [] !== $current && !reset($current)->querying;
            });
            $this->assertFalse($waiting->isComplete());
            $this->assertSame([], $this->timers(self::STORAGE_SECONDS), 'WAIT duration is not a storage operation.');
            $cancel->cancel();
            try {
                $waiting->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            } catch (TransportException) {
                $this->addToAssertionCount(1);
            }
        } finally {
            $client->close();
            $broker->stop();
            $this->assertSame(0, $running->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
        }
    }

    public static function invalidBudgets(): iterable
    {
        yield 'zero' => [0.0, 'positive'];
        yield 'negative' => [-1.0, 'positive'];
        yield 'positive infinity' => [\INF, 'finite'];
        yield 'negative infinity' => [-\INF, 'finite'];
        yield 'not a number' => [\NAN, 'finite'];
    }

    #[DataProvider('invalidBudgets')]
    public function testInvalidBudgetFailsBeforeResourceAcquisition(float $seconds, string $constraint): void
    {
        $this->assertSame(10.0, Broker::STORAGE_OPERATION_TIMEOUT_SECONDS);
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage operation timeout must be '.$constraint.' seconds.');
        new BrokerFactory($this->database->path(), $this->database->path('broker.sock'), storageOperationTimeoutSeconds: $seconds);
    }

    public static function startupQueries(): iterable
    {
        yield 'readback' => ['PRAGMA journal_mode'];
        yield 'schema' => [QueueSql::SCHEMA_SQL];
    }

    #[DataProvider('startupQueries')]
    public function testStartupDeadlineClosesAnAcquiredWorkerDuringReadbackOrSchema(string $sql): void
    {
        $source = \dirname((new \ReflectionClass(SqliteConnector::class))->getFileName()).'/Internal';
        $worker = file_get_contents($source.'/worker.php');
        $anchor = "            \$channel->send(['id' => \$id, 'value' => \$worker->handle(\$request)]);";
        $this->assertSame(1, substr_count($worker, $anchor));
        $hook = "            \$value = \$worker->handle(\$request);\n"
            .'            if (($request[\'sql\'] ?? \'\') === '.var_export($sql, true).") {\n"
            ."                \$GLOBALS['queueFaultBarrier']->at('startup');\n            }\n"
            ."            \$channel->send(['id' => \$id, 'value' => \$value]);";
        file_put_contents($this->database->path('worker.php'), str_replace($anchor, $hook, $worker));
        copy($source.'/WorkerProcess.php', $this->database->path('WorkerProcess.php'));
        $server = listen('127.0.0.1:0');
        $factory = new QueueFaultContextFactory(new ProcessContextFactory(), $this->database->directory(), 'tcp://'.$server->getAddress()->toString());
        $connection = (new SqliteConnector($factory))->connect((new SqliteConfig($this->database->path()))->withJournalMode(SqliteJournalMode::Wal));
        $deadline = new TimeoutCancellation(self::STORAGE_SECONDS);
        $initializing = async(static fn () => SqliteQueueStorage::fromConnection($connection, SqliteSynchronousMode::Normal, $deadline));
        $initializing->ignore();
        try {
            $socket = $server->accept(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertNotNull($socket);
            $control = new StreamChannel($socket, $socket);
            $this->assertSame('startup', $control->receive(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertFalse($initializing->isComplete());
            $timers = $this->timers(self::STORAGE_SECONDS);
            $this->assertCount(1, $timers);
            $this->fire(reset($timers));
            try {
                $initializing->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                $this->fail('Startup cannot succeed after its deadline.');
            } catch (CancelledException) {
                $this->assertTrue($connection->isClosed());
            }
            $channel = (new \ReflectionProperty($connection, 'channel'))->getValue($connection);
            $context = (new \ReflectionProperty($channel, 'context'))->getValue($channel);
            $process = (new \ReflectionProperty(ProcessContext::class, 'process'))->getValue($context);
            $process->join(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertFalse($process->isRunning());
            $this->assertSame(1, $factory->starts);
            $control->close();
        } finally {
            $connection->close();
            $server->close();
        }
    }

    private function broker(): Broker
    {
        $previous = getenv(DeadlineQueueWorkerOperation::CONTROL_ENVIRONMENT);
        putenv(DeadlineQueueWorkerOperation::CONTROL_ENVIRONMENT.'='.$this->database->path('control.json'));
        try {
            return (new BrokerFactory($this->database->path(), $this->database->path('broker.sock'), workerOperation: DeadlineQueueWorkerOperation::class, storageOperationTimeoutSeconds: self::STORAGE_SECONDS))->create();
        } finally {
            putenv(false === $previous ? DeadlineQueueWorkerOperation::CONTROL_ENVIRONMENT : DeadlineQueueWorkerOperation::CONTROL_ENVIRONMENT.'='.$previous);
        }
    }

    /** @return array<string, TimerCallback> */
    private function timers(float $seconds): array
    {
        $driver = EventLoop::getDriver();
        $reflection = new \ReflectionObject($driver);
        do {
            if ($reflection->hasProperty('callbacks')) {
                $callbacks = $reflection->getProperty('callbacks')->getValue($driver);

                return array_filter($callbacks, static fn ($callback): bool => $callback instanceof TimerCallback && $callback->interval === $seconds);
            }
            $reflection = $reflection->getParentClass();
        } while (false !== $reflection);
        throw new \LogicException('Could not inspect Revolt callback registry.');
    }

    private function fire(TimerCallback $timer): void
    {
        EventLoop::cancel($timer->id);
        ($timer->closure)($timer->id);
    }

    /** Observable state is the barrier; this timeout only aborts a broken fixture. */
    private function until(\Closure $ready): void
    {
        $safety = new TimeoutCancellation(self::SAFETY_SECONDS);
        while (!$ready()) {
            $safety->throwIfRequested();
            $this->turn();
        }
    }

    private function turn(): void
    {
        $suspension = EventLoop::getSuspension();
        EventLoop::defer($suspension->resume(...));
        $suspension->suspend();
    }
}
