<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\ByteStream\BufferedReader;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function Amp\async;
use function Amp\ByteStream\buffer;

#[RequiresOperatingSystem('Linux')]
final class NativeConsoleProcessTest extends TestCase
{
    private const int SAFETY_SECONDS = 15;
    // Keep reservations live until explicit settlement, independent of subprocess scheduling.
    private const int BROKER_NOW_MILLISECONDS = 1_700_000_000_000;

    // Resources may be absent after failed setup; the CLI-broker case has no in-process broker.
    private ?IsolatedDatabase $fixture = null;
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerRun = null;
    /** @var list<Process> */
    private array $processes = [];
    /** @var list<int> */
    private array $ownedPids = [];
    /** @var array<int, Future<string>> */
    private array $errors = [];

    protected function setUp(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('Process ownership proof requires /proc.');
        }
        $this->fixture = new IsolatedDatabase();
        (new Filesystem())->mirror(__DIR__.'/Fixtures/NativeApp/config', $this->project().'/config');
    }

    protected function tearDown(): void
    {
        try {
            $this->broker?->stop();
            $this->brokerRun?->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        } finally {
            // Fallback only. Successful tests assert process absence before this cleanup.
            foreach (array_reverse($this->ownedPids) as $pid) {
                if (isset(ProcessTree::snapshot()[$pid])) {
                    @posix_kill($pid, \SIGKILL);
                }
            }
            foreach ($this->processes as $process) {
                if ($process->isRunning()) {
                    $process->kill();
                }
            }
            if (null !== $this->fixture) {
                (new Filesystem())->remove($this->project());
                $this->fixture->remove();
            }
        }
    }

    public function testApplicationBrokerCommandAndNativeConsumeAcknowledgeDurably(): void
    {
        $broker = $this->console(['sqlite-queue:broker', '--database='.$this->database(), '--endpoint='.$this->endpoint()]);
        $ready = (new BufferedReader($broker->getStdout()))->readUntil("\n", new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame('ready', json_decode((string) $ready, true, 16, \JSON_THROW_ON_ERROR)['event']);
        $this->trackDescendants($broker);

        $transport = (new \Ineersa\SqliteQueue\Messenger\TransportFactory())->createTransport(
            'sqlite-queue://jobs?endpoint='.rawurlencode($this->endpoint()), [], new PhpSerializer(),
        );
        try {
            $transport->send(new Envelope(new NativeProbeMessage('native-ack')));
        } finally {
            $transport->close();
        }

        $consumer = $this->console(['messenger:consume', 'async', '--limit=1', '--quiet']);
        $this->assertExit($consumer, 0);
        $this->assertSame("{\"body\":\"native-ack\"}\n", file_get_contents($this->project().'/handled.jsonl'));
        $this->assertSame(0, $this->messageCount());
        $this->assertTrue(posix_kill($broker->getPid(), \SIGTERM));
        $this->assertExit($broker, 0);
        $this->assertOwnedGone();
        $this->assertFileDoesNotExist($this->endpoint());
    }

    public function testNativeSigtermCancelsRegisteredWaitAndExitsCleanly(): void
    {
        $this->startBroker();
        $consumer = $this->console(['messenger:consume', 'async', '--quiet']);
        $this->awaitWaiters(1);
        $this->trackDescendants($consumer);
        $this->assertTrue(posix_kill($consumer->getPid(), \SIGTERM));
        $this->assertExit($consumer, 0);
        $this->awaitWaiters(0);
        $this->assertFileDoesNotExist($this->project().'/handled.jsonl');
        $this->assertSame(0, $this->messageCount());
        $this->assertOwnedGone();
        $this->broker->stop();
        $this->assertSame(0, $this->brokerRun->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
        $this->assertFileDoesNotExist($this->endpoint());
    }

    public function testBrokerDisconnectDuringNativeWaitFailsWithoutReconnect(): void
    {
        $this->startBroker();
        $consumer = $this->console(['messenger:consume', 'async', '--quiet']);
        $this->awaitWaiters(1);
        $this->broker->stop();
        $this->assertSame(0, $this->brokerRun->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
        $exit = $consumer->join(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertNotSame(0, $exit);
        $diagnostics = $this->errors[$consumer->getPid()]->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertStringContainsString('queue broker', $diagnostics);
        $this->assertFileDoesNotExist($this->project().'/handled.jsonl');
        $this->assertOwnedGone();
        $this->assertFileDoesNotExist($this->endpoint());
    }

    public function testSigtermDuringWaitFlushesDeferredBatchAndAcksDurably(): void
    {
        $this->startBroker();
        $publisher = (new \Ineersa\SqliteQueue\Messenger\TransportFactory())->createTransport(
            'sqlite-queue://jobs?endpoint='.rawurlencode($this->endpoint()), [], new PhpSerializer(),
        );
        $publisher->send(new Envelope(new Fixtures\NativeApp\Message\NativeBatchMessage('deferred')));
        $publisher->close();
        $consumer = $this->console(['messenger:consume', 'async', '--quiet']);
        $this->awaitWaiters(1);
        if (Fixtures\NativeApp\Handler\NativeBatchMessageHandler::supportsDeferredIdleFlush()) {
            $this->assertSame(1, $this->messageCount());
            $this->assertFileDoesNotExist($this->project().'/batch.jsonl');
        } else {
            // Messenger 8.0 flushes pending batches before dispatching its idle event.
            $this->assertSame(0, $this->messageCount());
            $this->assertFileExists($this->project().'/batch.jsonl');
        }
        $this->assertTrue(posix_kill($consumer->getPid(), \SIGTERM));
        $this->assertExit($consumer, 0);
        $this->assertSame("{\"body\":\"deferred\"}\n", file_get_contents($this->project().'/batch.jsonl'));
        $this->assertSame(0, $this->messageCount());
        $client = Client::connect($this->endpoint());
        try {
            $this->assertNull($client->receive('jobs'));
        } finally {
            $client->close();
        }
        $this->assertOwnedGone();
    }

    /** @param list<string> $arguments */
    private function console(array $arguments): Process
    {
        $process = Process::start([
            \PHP_BINARY, __DIR__.'/Fixtures/NativeApp/console.php', ...$arguments, '--no-interaction', '--no-ansi',
        ], environment: [
            ...getenv(),
            'NATIVE_PROJECT_DIR' => $this->project(),
            'NATIVE_ASYNC_DSN' => 'sqlite-queue://jobs?endpoint='.rawurlencode($this->endpoint()),
        ]);
        $this->processes[] = $process;
        $this->ownedPids[] = $process->getPid();
        $error = async(static fn (): string => buffer($process->getStderr()));
        $error->ignore();
        $this->errors[$process->getPid()] = $error;

        return $process;
    }

    private function assertExit(Process $process, int $expected): void
    {
        $exit = $process->join(new TimeoutCancellation(self::SAFETY_SECONDS));
        $diagnostics = $this->errors[$process->getPid()]->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame($expected, $exit, $diagnostics);
    }

    private function startBroker(): void
    {
        $broker = (new BrokerFactory(
            $this->database(),
            $this->endpoint(),
            clock: static fn (): int => self::BROKER_NOW_MILLISECONDS,
        ))->create();
        $this->broker = $broker;
        $ready = new DeferredFuture();
        $this->brokerRun = async(static fn (): int => $broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $ready->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
    }

    private function awaitWaiters(int $expected): void
    {
        $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
        $property = new \ReflectionProperty(QueueNotifier::class, 'waiters');
        $bound = new TimeoutCancellation(self::SAFETY_SECONDS);
        while (true) {
            $count = array_sum(array_map(\count(...), $property->getValue($notifier)));
            if ($expected === $count) {
                return;
            }
            $bound->throwIfRequested();
            $barrier = new DeferredFuture();
            EventLoop::delay(0, static fn () => $barrier->complete());
            $barrier->getFuture()->await($bound);
        }
    }

    private function trackDescendants(Process $process): void
    {
        $tree = ProcessTree::ownedBy($process->getPid());
        $this->ownedPids = [...$this->ownedPids, ...$tree['launchers'], ...$tree['workers']];
    }

    private function assertOwnedGone(): void
    {
        $snapshot = ProcessTree::snapshot();
        foreach ($this->ownedPids as $pid) {
            $this->assertArrayNotHasKey($pid, $snapshot, 'Owned process survived before fallback cleanup.');
        }
    }

    private function database(): string
    {
        return $this->fixture->path();
    }

    private function endpoint(): string
    {
        return $this->fixture->path('queue.sock');
    }

    private function project(): string
    {
        return $this->fixture->directory().'/app';
    }

    private function messageCount(): int
    {
        $database = new \SQLite3($this->database());
        try {
            return (int) $database->querySingle('SELECT count(*) FROM queue_messages');
        } finally {
            $database->close();
        }
    }
}
