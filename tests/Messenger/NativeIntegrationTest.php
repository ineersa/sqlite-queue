<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\ByteStream\BufferedReader;
use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\Stamp\DeliveryReceiptStamp;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\BrokenDecodeSerializer;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\ControllableClock;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler\NativeProbeMessageHandler;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeParallelMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeParallelOutcome;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Ineersa\SqliteQueue\Tests\Support\ClockFile;
use Ineersa\SqliteQueue\Tests\Support\ControlledQueueWorkerOperation;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ShortUnixEndpoint;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\TimerCallback;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Command\LazyCommand;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\Command\ConsumeMessagesCommand;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;
use Symfony\Component\Messenger\Event\WorkerMessageReceivedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageRetriedEvent;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMemoryLimitListener;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Execution\ParallelExecutionStrategy;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Worker;

use function Amp\async;
use function Amp\Socket\listen;

#[RequiresOperatingSystem('Linux')]
final class NativeIntegrationTest extends TestCase
{
    private const int SAFETY_SECONDS = 15;
    private const int VISIBILITY_MILLISECONDS = 5_000;

    private ?IsolatedDatabase $database = null;
    private string $projectDir = '';
    private string $endpoint = '';
    private ?ShortUnixEndpoint $endpointResource = null;
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerFuture = null;
    private int $now = 1_700_000_000_000 {
        set {
            $this->now = $value;
            ClockFile::set($this->database->path(), $value);
        }
    }
    private float $workerNow = 1_700_000_000.0;
    private ?ControllableClock $clock = null;
    /** No saved clock exists before setUp acquires the test-only global clock scope. */
    private ?\Symfony\Component\Clock\ClockInterface $previousClock = null;
    private ?NativeKernel $kernel = null;
    private ?Application $application = null;
    /** @var list<Client> */
    private array $clients = [];
    /** These resources exist only after a parallel scenario acquires its barrier and consumer. */
    private ?ShortUnixEndpoint $parallelEndpoint = null;
    private ?ServerSocket $parallelBarrier = null;
    private ?Worker $parallelWorker = null;
    /** @var Future<int>|null */
    private ?Future $parallelConsumer = null;
    /** @var list<Socket> */
    private array $parallelConnections = [];
    /** @var list<\Throwable> Native failures remain observable without interrupting settlement. */
    private array $parallelFailures = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->database = new IsolatedDatabase();
        $this->endpointResource = new ShortUnixEndpoint();
        $this->endpoint = $this->endpointResource->path();
        $this->projectDir = $this->database->directory().'/app';
        $this->clock = new ControllableClock($this->workerNow);
        $this->previousClock = \Symfony\Component\Clock\Clock::get();
        \Symfony\Component\Clock\Clock::set($this->clock);
        $this->materializeProject();
        NativeProbeMessageHandler::reset();
    }

    protected function tearDown(): void
    {
        try {
            $parallelError = null;
            try {
                $this->closeParallelConsumer();
            } catch (\Throwable $error) {
                $parallelError = $error;
            }
            foreach ($this->clients as $client) {
                try {
                    $client->close();
                } catch (\Throwable) {
                }
            }
            $this->clients = [];
            $this->application = null;
            $this->kernel?->shutdown();
            $this->broker?->stop();
            if (null !== $this->brokerFuture) {
                $this->assertSame(0, $this->brokerFuture->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            }
            if (null !== $parallelError) {
                throw $parallelError;
            }
        } finally {
            if (null !== $this->previousClock) {
                \Symfony\Component\Clock\Clock::set($this->previousClock);
                $this->previousClock = null;
            }
            $this->broker = null;
            $this->brokerFuture = null;
            $this->kernel = null;
            $this->clock = null;
            if ('' !== $this->projectDir && is_dir($this->projectDir)) {
                (new Filesystem())->remove($this->projectDir);
            }
            $this->database?->remove();
        }

        $this->endpointResource?->remove();
        $this->endpointResource = null;
        parent::tearDown();
    }

    public function testKernelBootAndBrokerHelpWorkWithoutLiveBroker(): void
    {
        $app = $this->application();
        $this->assertTrue($app->has('sqlite-queue:broker'));
        $this->assertTrue($app->has('messenger:consume'));

        $broker = $app->find('sqlite-queue:broker');
        if ($broker instanceof LazyCommand) {
            $broker = $broker->getCommand();
        }
        $this->assertInstanceOf(BrokerCommand::class, $broker);

        $consume = $app->find('messenger:consume');
        if ($consume instanceof LazyCommand) {
            $consume = $consume->getCommand();
        }
        $this->assertInstanceOf(ConsumeMessagesCommand::class, $consume);

        $output = new BufferedOutput();
        $exit = $app->run(new ArrayInput([
            'command' => 'sqlite-queue:broker',
            '--help' => true,
        ]), $output);
        $this->assertSame(0, $exit);
        $this->assertStringContainsString('--database', $output->fetch());

        $container = $this->kernel?->getContainer() ?? throw new \LogicException('Missing kernel.');
        $this->assertInstanceOf(TransportFactory::class, $container->get(TransportFactory::class));
        $this->assertInstanceOf(NativeConsumeWaitSubscriber::class, $container->get(NativeConsumeWaitSubscriber::class));
        $transport = $this->transport('async');
        $this->assertInstanceOf(Transport::class, $transport);
        $this->assertSame(0, $app->run(new ArrayInput([
            'command' => 'messenger:setup-transports',
            'transport' => 'async',
        ]), new BufferedOutput()));
    }

    public function testNativeConsumeLimitHandlesAndAcks(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $this->publisher()->send(new Envelope(new NativeProbeMessage('ack-me')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['ack-me'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], iterator_to_array($this->transport()->get()));
        });
    }

    public function testDefaultInvocationArmsWaitWithIdleTimeoutZeroAndWakesOnPublication(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $idleTimeout = null;
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use (&$idleTimeout): void {
                if (method_exists($event, 'getIdleTimeout')) {
                    $idleTimeout = $event->getIdleTimeout();
                }
            }, 100);

            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
            ]));

            $this->awaitNotifierWaiters(1);
            if (null !== $idleTimeout) {
                $this->assertSame(0, $idleTimeout);
            }
            $this->assertSame(1_000, $this->activeWaitDurationMilliseconds());
            $this->assertSame([], $this->clock?->sleepCalls ?? []);

            $this->publisher()->send(new Envelope(new NativeProbeMessage('woke')));
            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(['woke'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public static function literalReceiverInvocations(): iterable
    {
        yield 'hyphen, default sleep' => ['async-high', []];
        yield 'hyphen, zero sleep' => ['async-high', ['--sleep' => '0']];
        yield 'colon, default sleep' => ['async:high', []];
        yield 'colon, zero sleep' => ['async:high', ['--sleep' => '0']];
    }

    #[DataProvider('literalReceiverInvocations')]
    public function testLiteralReceiverRegistersWaitBeforePublication(string $receiver, array $options): void
    {
        $this->runAsync(function () use ($receiver, $options): void {
            $this->startBroker();
            $started = new DeferredFuture();
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use ($started): void {
                $started->complete($event->getWorker());
            });
            $original = $this->consumeCommand()->getDefinition()->getOption('sleep')->getDefault();
            $consume = async(fn (): int => $this->runConsume($options + [
                'command' => 'messenger:consume',
                'receivers' => [$receiver],
                '--limit' => '1',
            ]));
            try {
                $this->awaitNotifierWaiters(1);
                $this->assertSame(1, $this->notifierWaiterCount());
                $this->assertSame(1_000, $this->activeWaitDurationMilliseconds());
                $this->publisher()->send(new Envelope(new NativeProbeMessage($receiver)));
                $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
                $this->assertSame([$receiver], NativeProbeMessageHandler::$handled);
                $this->assertSame([], $this->clock->sleepCalls);
                $this->assertSame($original, $this->consumeCommand()->getDefinition()->getOption('sleep')->getDefault());
            } finally {
                if ($started->isComplete()) {
                    $started->getFuture()->await()->stop();
                }
                $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            }
        });
    }

    public function testExplicitFractionalSleepDoesNotArmWait(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            $this->publisher()->send(new Envelope(new NativeProbeMessage('fraction')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
                '--sleep' => '0.5',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['fraction'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $this->notifierWaiterCount());
            $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
        });
    }

    public function testExplicitPositiveSleepDoesNotArmWaitAndRestoresDefault(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            $this->publisher()->send(new Envelope(new NativeProbeMessage('poll')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
                '--sleep' => '2',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['poll'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $this->notifierWaiterCount());
            $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
        });
    }

    public static function foreignOnlySleepModes(): iterable
    {
        yield 'omitted sleep keeps native one-second budget' => [[], 1_000_000];
        yield 'explicit zero sleep keeps native zero budget' => [['--sleep' => '0'], 0];
    }

    #[DataProvider('foreignOnlySleepModes')]
    public function testForeignOnlyReceiverHandlesReadyWorkWithoutExtraFallbackSleep(array $options, int $expectedIdleTimeout): void
    {
        $this->runAsync(function () use ($options, $expectedIdleTimeout): void {
            $this->startBroker();
            $idleTimeout = null;
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use (&$idleTimeout): void {
                if (method_exists($event, 'getIdleTimeout')) {
                    $idleTimeout = $event->getIdleTimeout();
                }
            }, 100);
            $this->eventDispatcher()->addListener(WorkerRunningEvent::class, function (WorkerRunningEvent $event): void {
                if (!$event->isWorkerIdle()) {
                    return;
                }
                // Earlier idle listeners may already wait; advancing past 1s exposes any extra fallback sleep.
                $this->clock?->advance(1.5);
                $this->bootKernel()->getContainer()->get('messenger.transport.sync_probe')
                    ->send(new Envelope(new NativeProbeMessage('foreign-ready')));
            }, 0);

            $exit = $this->runConsume($options + [
                'command' => 'messenger:consume',
                'receivers' => ['sync_probe'],
                '--limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            if (null !== $idleTimeout) {
                $this->assertSame($expectedIdleTimeout, $idleTimeout);
            }
            $this->assertSame(['foreign-ready'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $this->notifierWaiterCount());
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public function testDelayedMessageWakesRegisteredWaitWithoutRemainderSleep(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $idleTimeout = null;
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use (&$idleTimeout): void {
                if (method_exists($event, 'getIdleTimeout')) {
                    $idleTimeout = $event->getIdleTimeout();
                }
            }, 100);

            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
            ]));

            $this->awaitNotifierWaiters(1);
            if (null !== $idleTimeout) {
                $this->assertSame(0, $idleTimeout);
            }
            $this->publisher()->send(new Envelope(new NativeProbeMessage('later'), [new DelayStamp(50)]));
            $timerId = $this->awaitNotifierDeadline($this->now + 50);
            $this->assertFalse(EventLoop::isEnabled($timerId));
            $this->setNow($this->now + 50);
            $this->fireNotifierTimer($timerId);

            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(['later'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public function testSameBrokerReceiversShareOneNotificationWaitOnDistinctQueueNames(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $idleTimeout = null;
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use (&$idleTimeout): void {
                if (method_exists($event, 'getIdleTimeout')) {
                    $idleTimeout = $event->getIdleTimeout();
                }
            }, 100);

            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async', 'reports'],
                '--limit' => '1',
            ]));

            $this->awaitNotifierWaiters(1);
            if (null !== $idleTimeout) {
                $this->assertSame(0, $idleTimeout);
            }
            $this->assertSame(1_000, $this->activeWaitDurationMilliseconds());
            $this->assertSame([], $this->clock?->sleepCalls ?? []);

            $reports = new Transport($this->connection(), new QueueName('reports'), new PhpSerializer(), $this->connection(), $this->endpoint);
            $reports->send(new Envelope(new NativeProbeMessage('shared-wait')));
            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(['shared-wait'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public function testRegexSelectionUsesMetadataAndFallsBackWhenMixedWithForeignReceivers(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $selected = [];
            $idleTimeout = null;
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, static function (WorkerStartedEvent $event) use (&$selected, &$idleTimeout): void {
                $selected = $event->getWorker()->getMetadata()->getTransportNames();
                if (method_exists($event, 'getIdleTimeout')) {
                    $idleTimeout = $event->getIdleTimeout();
                }
            }, 100);
            $this->publisher()->send(new Envelope(new NativeProbeMessage('regex')));
            $this->assertSame(0, $this->runConsume([
                'command' => 'messenger:consume', 'receivers' => ['async.alpha'], '--limit' => '1',
            ]));
            // Native receiver regex expansion was added in Symfony 8.1 and can include the
            // foreign in-memory asyncXalpha receiver. Metadata selection then uses the bounded
            // Clock fallback instead of WAIT. Symfony 8.0 keeps the literal dotted name alone.
            $expected = version_compare(\Composer\InstalledVersions::getVersion('symfony/messenger') ?? '0', '8.1.0', '>=')
                ? ['async.alpha', 'asyncXalpha']
                : ['async.alpha'];
            $this->assertSame($expected, $selected);
            if (null !== $idleTimeout) {
                $this->assertSame(0, $idleTimeout);
            }
            $this->assertSame(['regex'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $this->notifierWaiterCount());
            $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
        });
    }

    public function testInvalidNativeOptionRestoresSleepDefaultBeforeNextInvocation(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            try {
                $this->runConsume(['command' => 'messenger:consume', 'receivers' => ['async'], '--limit' => '0']);
                $this->fail('Native command must reject its invalid limit.');
            } catch (\Symfony\Component\Console\Exception\InvalidOptionException) {
                $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
            }
            $this->publisher()->send(new Envelope(new NativeProbeMessage('after-error')));
            $this->assertSame(0, $this->runConsume([
                'command' => 'messenger:consume', 'receivers' => ['async'], '--limit' => '1',
            ]));
            $this->assertSame(['after-error'], NativeProbeMessageHandler::$handled);
            $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
        });
    }

    public function testMessageLimitStopsWithoutAnotherIdleWait(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $idleEvents = 0;
            $this->eventDispatcher()->addListener(WorkerRunningEvent::class, static function (WorkerRunningEvent $event) use (&$idleEvents): void {
                if ($event->isWorkerIdle()) {
                    ++$idleEvents;
                }
            });
            $this->publisher()->send(new Envelope(new NativeProbeMessage('one')));
            $this->publisher()->send(new Envelope(new NativeProbeMessage('two')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['one'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $idleEvents);
            $remaining = iterator_to_array($this->transport()->get());
            $this->assertCount(1, $remaining);
            $this->transport()->ack($remaining[0]);
        });
    }

    public function testMemoryLimitStopsAfterHandlingWithoutConsumingMoreWork(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            // Same listener class messenger:consume registers for --memory-limit, with a fixed
            // usage callback so the proof stays deterministic. CLI byte parsing is Symfony's.
            $this->eventDispatcher()->addSubscriber(new StopWorkerOnMemoryLimitListener(
                1,
                null,
                static fn (): int => 2,
            ));
            $this->publisher()->send(new Envelope(new NativeProbeMessage('memory-one')));
            $this->publisher()->send(new Envelope(new NativeProbeMessage('memory-two')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['memory-one'], NativeProbeMessageHandler::$handled);
            $remaining = iterator_to_array($this->transport()->get());
            $this->assertCount(1, $remaining);
            $this->assertSame('memory-two', $remaining[0]->getMessage()->body);
            $this->transport()->ack($remaining[0]);
        });
    }

    public function testTimeLimitCapsWaitAndExitsWithoutNativeSleep(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--time-limit' => '1',
            ]));

            $this->awaitNotifierWaiters(1);
            $this->assertLessThanOrEqual(1_000, $this->activeWaitDurationMilliseconds());
            $this->clock?->advance(2.0);
            $timeoutId = $this->awaitNotifierWaitTimeout();
            $this->assertFalse(EventLoop::isEnabled($timeoutId));
            $this->fireNotifierTimer($timeoutId);

            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame([], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public function testRetryWithSubsecondDelayThenSucceedsThroughNativeConsume(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            NativeProbeMessageHandler::$mode = NativeProbeMessageHandler::MODE_FAIL_THEN_SUCCEED;
            $this->publisher()->send(new Envelope(new NativeProbeMessage('retry-me')));

            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '2',
            ]));

            $this->awaitNotifierWaiters(1);
            $timerId = $this->awaitNotifierDeadline($this->now + 50);
            $this->assertFalse(EventLoop::isEnabled($timerId));
            $this->setNow($this->now + 50);
            $this->fireNotifierTimer($timerId);

            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(2, NativeProbeMessageHandler::$attempts);
            $this->assertSame(['retry-me'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], iterator_to_array($this->transport()->get()));
        });
    }

    public function testRetryExhaustionRejectsTerminallyThroughNativeConsume(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            NativeProbeMessageHandler::$mode = NativeProbeMessageHandler::MODE_ALWAYS_FAIL;
            $this->publisher()->send(new Envelope(new NativeProbeMessage('terminal')));

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async_terminal'],
                '--limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(1, NativeProbeMessageHandler::$attempts);
            $this->assertSame([], iterator_to_array($this->transport('async_terminal')->get()));
        });
    }

    public function testDecodeFailureUsesMessengerFailurePathWithoutLeavingInvisibleReservation(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $broken = new Transport($this->connection(), new QueueName('jobs'), new BrokenDecodeSerializer(), $this->connection(), $this->endpoint);
            $broken->send(new Envelope(new NativeProbeMessage('ignored')));

            if (\is_callable([MessageDecodingFailedException::class, 'wrap'])) {
                $exit = $this->runConsume([
                    'command' => 'messenger:consume',
                    'receivers' => ['async'],
                    '--limit' => '1',
                ]);
                $this->assertSame(0, $exit);
            } else {
                try {
                    iterator_to_array($broken->get());
                    $this->fail('8.0 decode failures must throw after rejecting the claim.');
                } catch (MessageDecodingFailedException $error) {
                    $this->assertSame('native decode failure', $error->getMessage());
                }
            }

            $this->assertSame([], iterator_to_array($this->transport()->get()));
        });
    }

    public function testConfiguredFailureTransportExhaustionAndNativeRetryRecovery(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            NativeProbeMessageHandler::$mode = NativeProbeMessageHandler::MODE_ALWAYS_FAIL;
            $this->publisher()->send(new Envelope(new NativeProbeMessage('recover-failure')));
            $consume = async(fn (): int => $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async_failure'],
                '--limit' => '2',
            ]));
            $this->awaitNotifierWaiters(1);
            $timer = $this->awaitNotifierDeadline($this->now + 50);
            $this->setNow($this->now + 50);
            $this->fireNotifierTimer($timer);
            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(2, NativeProbeMessageHandler::$attempts);
            $this->assertSame([], iterator_to_array($this->transport('async_failure')->get()));
            $failed = $this->bootKernel()->getContainer()->get('messenger.transport.failed');
            $this->assertInstanceOf(\Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransport::class, $failed);
            $envelopes = iterator_to_array($failed->all());
            $this->assertCount(1, $envelopes);
            $envelope = $envelopes[0];
            $this->assertSame('async_failure', $envelope->last(\Symfony\Component\Messenger\Stamp\SentToFailureTransportStamp::class)->getOriginalReceiverName());
            $this->assertNotNull($envelope->last(\Symfony\Component\Messenger\Stamp\ErrorDetailsStamp::class));
            $this->assertSame(0, $envelope->last(\Symfony\Component\Messenger\Stamp\RedeliveryStamp::class)->getRetryCount());
            $this->assertNull($envelope->last(DeliveryReceiptStamp::class));
            $id = $envelope->last(\Symfony\Component\Messenger\Stamp\TransportMessageIdStamp::class)->getId();
            NativeProbeMessageHandler::$mode = NativeProbeMessageHandler::MODE_ACK;
            $this->assertSame(0, $this->runConsume([
                'command' => 'messenger:failed:retry',
                'id' => [$id],
                '--transport' => 'failed',
                '--force' => true,
            ]));
            $this->assertSame(['recover-failure'], NativeProbeMessageHandler::$handled);
            $this->assertCount(0, iterator_to_array($failed->all()));
            $this->assertSame([], iterator_to_array($this->transport('async_failure')->get()));
        });
    }

    public function testRestartPreservesDelayedDeliveryForNativeConsume(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $publisher = $this->publisher();
            $publisher->send(new Envelope(new NativeProbeMessage('across-restart'), [new DelayStamp(100)]));
            $this->assertSame([], iterator_to_array($publisher->get()));
            $availableAt = $this->now + 100;
            foreach ($this->clients as $client) {
                $client->close();
            }
            $this->clients = [];
            $this->application = null;
            $this->kernel?->shutdown();
            $this->kernel = null;
            $this->broker?->stop();
            $this->assertSame(0, $this->brokerFuture?->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->broker = null;
            $this->brokerFuture = null;

            $this->startBroker();
            $this->assertSame([], iterator_to_array($this->transport()->get()));
            $this->assertGreaterThan($this->now, $availableAt);
            $this->setNow($availableAt);

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['async'],
                '--limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame(['across-restart'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], iterator_to_array($this->transport()->get()));
        });
    }

    #[\PHPUnit\Framework\Attributes\RunInSeparateProcess]
    #[\PHPUnit\Framework\Attributes\PreserveGlobalState(false)]
    public function testNativeParallelChildCompletionRestoresParentReceiptAndAcknowledges(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $endpoint = $this->createParallelBarrier();
            $received = [];
            $handled = [];
            $this->eventDispatcher()->addListener(WorkerMessageReceivedEvent::class, static function (WorkerMessageReceivedEvent $event) use (&$received): void {
                $received[] = $event->getEnvelope()->last(DeliveryReceiptStamp::class);
            });
            $this->eventDispatcher()->addListener(WorkerMessageHandledEvent::class, static function (WorkerMessageHandledEvent $event) use (&$handled): void {
                $handled[] = $event->getEnvelope()->last(DeliveryReceiptStamp::class);
            });
            $this->publisher()->send(new Envelope(new NativeParallelMessage('child-ack', $endpoint, NativeParallelOutcome::Success)));
            $this->startParallelConsumer('async', 1);
            [$socket, $record] = $this->acceptParallelProbe();
            $this->assertNotSame(getmypid(), $record['pid']);
            $socket->write(NativeParallelMessage::RELEASE);
            $this->assertParallelConsumerCompleted();
            $this->assertCount(1, $received);
            $this->assertInstanceOf(DeliveryReceiptStamp::class, $received[0]);
            $this->assertSame($received, $handled, 'Native completion must restore the exact original stamp instance.');
            $this->assertSame($this->transport(), $handled[0]->transport);
            $this->setNow($this->now + self::VISIBILITY_MILLISECONDS);
            $this->assertSame([], iterator_to_array($this->publisher()->get()));
        });
    }

    // Native runtime/bootstrap state is process-global; do not inherit a preceding
    // consumer's event loop and worker-pool state for the retry lifecycle.
    #[\PHPUnit\Framework\Attributes\RunInSeparateProcess]
    #[\PHPUnit\Framework\Attributes\PreserveGlobalState(false)]
    public function testNativeParallelFailureRetriesAndRoutesToFailureTransport(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $endpoint = $this->createParallelBarrier();
            $received = [];
            $failedReceipts = [];
            $retried = 0;
            $this->eventDispatcher()->addListener(WorkerMessageReceivedEvent::class, static function (WorkerMessageReceivedEvent $event) use (&$received): void {
                $received[] = $event->getEnvelope()->last(DeliveryReceiptStamp::class);
            });
            $this->eventDispatcher()->addListener(WorkerMessageFailedEvent::class, static function (WorkerMessageFailedEvent $event) use (&$failedReceipts): void {
                $failedReceipts[] = $event->getEnvelope()->last(DeliveryReceiptStamp::class);
            });
            $this->eventDispatcher()->addListener(WorkerMessageRetriedEvent::class, function () use (&$retried): void {
                ++$retried;
                $this->setNow($this->now + 50);
            });
            $this->publisher()->send(new Envelope(new NativeParallelMessage('child-failure', $endpoint, NativeParallelOutcome::Failure)));
            $this->startParallelConsumer('async_failure', 2);
            [$first] = $this->acceptParallelProbe();
            $first->write(NativeParallelMessage::RELEASE);
            [$retry, $record] = $this->acceptParallelProbe();
            $this->assertSame('child-failure', $record['body']);
            $this->assertNotSame(getmypid(), $record['pid']);
            $retry->write(NativeParallelMessage::RELEASE);
            $this->assertParallelConsumerCompleted();
            $this->assertSame(1, $retried);
            $this->assertCount(2, $received);
            $this->assertSame($received, $failedReceipts);
            $this->assertNotSame($received[0], $received[1], 'Retry must acquire a fresh parent-owned receipt.');
            $failed = $this->bootKernel()->getContainer()->get('messenger.transport.failed');
            $this->assertInstanceOf(\Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransport::class, $failed);
            $envelopes = iterator_to_array($failed->all());
            $this->assertCount(1, $envelopes);
            $this->assertInstanceOf(NativeParallelMessage::class, $envelopes[0]->getMessage());
            $this->assertSame('async_failure', $envelopes[0]->last(\Symfony\Component\Messenger\Stamp\SentToFailureTransportStamp::class)->getOriginalReceiverName());
            $this->assertNotNull($envelopes[0]->last(\Symfony\Component\Messenger\Stamp\ErrorDetailsStamp::class));
            $this->assertNull($envelopes[0]->last(DeliveryReceiptStamp::class));
            $this->setNow($this->now + self::VISIBILITY_MILLISECONDS);
            $this->assertSame([], iterator_to_array($this->publisher()->get()));
        });
    }

    #[\PHPUnit\Framework\Attributes\RunInSeparateProcess]
    #[\PHPUnit\Framework\Attributes\PreserveGlobalState(false)]
    public function testNativeParallelWarmedUnderfilledPoolUsesNativeIdleCompletionWait(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $endpoint = $this->createParallelBarrier();
            $warm = new DeferredFuture();
            $firstHandled = new DeferredFuture();
            $handled = [];
            $dispatched = 0;
            $idle = new DeferredFuture();
            // Register before consume starts: its scoped limit listener snapshots
            // WorkerRunningEvent listeners from the application dispatcher.
            $this->eventDispatcher()->addListener(WorkerMessageReceivedEvent::class, static function () use (&$dispatched): void {
                ++$dispatched;
            });
            $this->eventDispatcher()->addListener(WorkerRunningEvent::class, function (WorkerRunningEvent $event) use ($idle, &$dispatched): void {
                if (!$event->isWorkerIdle() || 3 !== $dispatched || $idle->isComplete()) {
                    return;
                }
                $strategy = (new \ReflectionProperty(Worker::class, 'messageExecutionStrategy'))->getValue($event->getWorker());
                $this->assertInstanceOf(ParallelExecutionStrategy::class, $strategy);
                $subscriber = $this->bootKernel()->getContainer()->get(NativeConsumeWaitSubscriber::class);
                $sessions = (new \ReflectionProperty($subscriber, 'sessionsByWorker'))->getValue($subscriber);
                $this->assertCount(0, $sessions, 'Parallel idle passes must never activate a broker notification wait.');
                $this->assertSame(0, $this->notifierWaiterCount());
                $idle->complete();
            });
            $this->eventDispatcher()->addListener(WorkerMessageHandledEvent::class, function (WorkerMessageHandledEvent $event) use ($warm, $firstHandled, &$handled): void {
                $body = $event->getEnvelope()->getMessage()->body;
                $handled[] = $body;
                if ('warm' === $body) {
                    $warm->complete();
                } elseif ('first' === $body) {
                    $firstHandled->complete();
                } elseif ('second' === $body) {
                    $this->parallelWorker?->stop();
                }
            });
            $publisher = $this->publisher();
            $publisher->send(new Envelope(new NativeParallelMessage('warm', $endpoint, NativeParallelOutcome::Success)));
            // Leave room above the three receives so an idle pass remains reachable;
            // the final handled event, not a receive-limit event, stops this consumer.
            $this->startParallelConsumer('async', 4);
            [$warming] = $this->acceptParallelProbe();
            $warming->write(NativeParallelMessage::RELEASE);
            $warm->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            $publisher->send(new Envelope(new NativeParallelMessage('first', $endpoint, NativeParallelOutcome::Success)));
            $publisher->send(new Envelope(new NativeParallelMessage('second', $endpoint, NativeParallelOutcome::Success)));
            [$first, $firstRecord] = $this->acceptParallelProbe();
            [$second, $secondRecord] = $this->acceptParallelProbe();
            $children = [$firstRecord['body'] => [$first, $firstRecord], $secondRecord['body'] => [$second, $secondRecord]];
            $this->assertArrayHasKey('first', $children);
            $this->assertArrayHasKey('second', $children);
            [$first, $firstRecord] = $children['first'];
            [$second, $secondRecord] = $children['second'];
            $this->assertNotSame($firstRecord['pid'], $secondRecord['pid']);
            $this->assertSame(['warm'], $handled, 'Both child handlers must remain barrier-blocked with no queued jobs left.');
            $this->assertSame([], iterator_to_array($publisher->get()));
            $idle->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            $first->write(NativeParallelMessage::RELEASE);
            $firstHandled->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertSame(['warm', 'first'], $handled, 'Native completion must be processed while the other child remains blocked.');
            $second->write(NativeParallelMessage::RELEASE);
            $this->assertParallelConsumerCompleted();
            $this->assertSame(['warm', 'first', 'second'], $handled);
            $this->setNow($this->now + self::VISIBILITY_MILLISECONDS);
            $this->assertSame([], iterator_to_array($publisher->get()));
        });
    }

    private function createParallelBarrier(): string
    {
        $this->parallelEndpoint = new ShortUnixEndpoint('child.sock');
        $endpoint = 'unix://'.$this->parallelEndpoint->path();
        $this->parallelBarrier = listen($endpoint);

        return $endpoint;
    }

    private function startParallelConsumer(string $receiver, int $limit): void
    {
        $this->eventDispatcher()->addListener(WorkerMessageFailedEvent::class, function (WorkerMessageFailedEvent $event): void {
            $this->parallelFailures[] = $event->getThrowable();
        });
        $this->eventDispatcher()->addListener(WorkerStartedEvent::class, function (WorkerStartedEvent $event): void {
            $this->parallelWorker = $event->getWorker();
            $this->assertSame(1_000_000, $event->getIdleTimeout(), 'Omitted native sleep must not be suppressed for parallel consumers.');
        });
        $this->parallelConsumer = async(fn (): int => $this->runConsume([
            'command' => 'messenger:consume',
            'receivers' => [$receiver],
            '--concurrency' => '4',
            '--limit' => (string) $limit,
        ]));
    }

    /** @return array{Socket, array{body: string, pid: int}} */
    private function acceptParallelProbe(): array
    {
        $server = $this->parallelBarrier ?? throw new \LogicException('Parallel barrier was not acquired.');
        $consumer = $this->parallelConsumer ?? throw new \LogicException('Parallel consumer was not started.');
        $socket = \Amp\Future\awaitFirst([
            $consumer,
            async(static fn () => $server->accept(new TimeoutCancellation(self::SAFETY_SECONDS))),
        ]);
        if (!$socket instanceof Socket) {
            throw new \RuntimeException('Native parallel consumer exited before its handler reached the barrier.', previous: $this->parallelFailures[0] ?? null);
        }
        $this->assertInstanceOf(Socket::class, $socket);
        $this->parallelConnections[] = $socket;
        $record = json_decode((new BufferedReader($socket))->readUntil("\n", new TimeoutCancellation(self::SAFETY_SECONDS)), true, flags: \JSON_THROW_ON_ERROR);
        $this->assertIsArray($record);
        $this->assertIsString($record['body']);
        $this->assertIsInt($record['pid']);

        return [$socket, $record];
    }

    private function assertParallelConsumerCompleted(): void
    {
        $consumer = $this->parallelConsumer ?? throw new \LogicException('Parallel consumer was not started.');
        $this->assertSame(0, $consumer->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
    }

    private function closeParallelConsumer(): void
    {
        $this->parallelWorker?->stop();
        foreach ($this->parallelConnections as $socket) {
            $socket->close();
        }
        $this->parallelBarrier?->close();
        try {
            $this->parallelConsumer?->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        } catch (\Throwable $error) {
            if (null !== $this->parallelWorker) {
                $strategy = (new \ReflectionProperty(Worker::class, 'messageExecutionStrategy'))->getValue($this->parallelWorker);
                foreach ((new \ReflectionProperty($strategy, 'workers'))->getValue($strategy) as $worker) {
                    $worker->kill();
                }
            }
            throw $error;
        } finally {
            foreach ($this->parallelWorker?->getMetadata()->getTransportNames() ?? [] as $name) {
                $this->transport($name)->close();
            }
            $this->parallelEndpoint?->remove();
        }
    }

    /**
     * @param array<string, mixed> $arguments
     */
    private function runConsume(array $arguments): int
    {
        $output = new BufferedOutput();
        $input = new ArrayInput($arguments);
        $input->setInteractive(false);

        $exit = $this->application()->run($input, $output);
        if (0 !== $exit) {
            throw new \RuntimeException($output->fetch(), $exit);
        }

        return $exit;
    }

    private function application(): Application
    {
        if (null !== $this->application) {
            return $this->application;
        }
        $kernel = $this->bootKernel();
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
        $this->application = $application;

        return $application;
    }

    private function bootKernel(): NativeKernel
    {
        if (null !== $this->kernel) {
            return $this->kernel;
        }
        $dsn = 'sqlite-queue://jobs?endpoint='.rawurlencode($this->endpoint);
        putenv('NATIVE_ASYNC_DSN='.$dsn);
        $_ENV['NATIVE_ASYNC_DSN'] = $dsn;
        $_SERVER['NATIVE_ASYNC_DSN'] = $dsn;
        $reports = 'sqlite-queue://reports?endpoint='.rawurlencode($this->endpoint);
        putenv('NATIVE_REPORTS_DSN='.$reports);
        $_ENV['NATIVE_REPORTS_DSN'] = $reports;
        $_SERVER['NATIVE_REPORTS_DSN'] = $reports;

        $kernel = new NativeKernel($this->projectDir);
        $kernel->boot();
        $this->kernel = $kernel;

        return $kernel;
    }

    private function consumeCommand(): ConsumeMessagesCommand
    {
        $command = $this->application()->find('messenger:consume');
        if ($command instanceof LazyCommand) {
            $command = $command->getCommand();
        }
        $this->assertInstanceOf(ConsumeMessagesCommand::class, $command);

        return $command;
    }

    private function transport(string $name = 'async'): Transport
    {
        $transport = $this->bootKernel()->getContainer()->get('messenger.transport.'.$name);
        $this->assertInstanceOf(Transport::class, $transport);

        return $transport;
    }

    private function publisher(): Transport
    {
        return new Transport($this->connection(), new QueueName('jobs'), new PhpSerializer(), $this->connection(), $this->endpoint);
    }

    private function connection(): BrokerConnection
    {
        return new BrokerConnection(function (Cancellation $cancellation): Client {
            $client = Client::connect($this->endpoint, cancellation: $cancellation);
            $this->clients[] = $client;

            return $client;
        });
    }

    private function eventDispatcher(): object
    {
        return $this->bootKernel()->getContainer()->get('event_dispatcher');
    }

    private function setNow(int $value): void
    {
        $this->now = $value;
    }

    private function startBroker(): void
    {
        $database = $this->database?->path() ?? throw new \LogicException('Missing database.');
        $ready = new DeferredFuture();
        ClockFile::configure($this->database->path(), $this->now);
        $this->broker = (new BrokerFactory(
            $database,
            $this->endpoint,
            5_000,
            fn (): int => $this->now,
            workerOperation: ControlledQueueWorkerOperation::class,
        ))->create();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            if (!$ready->isComplete()) {
                $ready->complete($event);
            }
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
        $this->assertSame('ready', $event['event']);
    }

    private function materializeProject(): void
    {
        $fs = new Filesystem();
        $source = \dirname(__DIR__).'/Messenger/Fixtures/NativeApp';
        $fs->mkdir($this->projectDir.'/config/packages');
        $fs->mkdir($this->projectDir.'/var');
        $fs->copy($source.'/.env', $this->projectDir.'/.env');
        $fs->copy($source.'/config/bundles.php', $this->projectDir.'/config/bundles.php');
        $fs->copy($source.'/config/services.yaml', $this->projectDir.'/config/services.yaml');
        $fs->copy($source.'/config/packages/framework.yaml', $this->projectDir.'/config/packages/framework.yaml');
        $fs->copy($source.'/bin/console', $this->projectDir.'/bin/console');
    }

    private function awaitNotifierWaiters(int $count): void
    {
        $deadline = microtime(true) + self::SAFETY_SECONDS;
        do {
            if ($count === $this->notifierWaiterCount()) {
                return;
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Expected '.$count.' notifier waiters.');
    }

    private function notifierWaiterCount(): int
    {
        if (null === $this->broker) {
            return 0;
        }
        $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
        $waiters = (new \ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);
        $unique = [];
        foreach ($waiters as $list) {
            foreach ($list as $waiter) {
                $unique[spl_object_id($waiter)] = $waiter;
            }
        }

        return \count($unique);
    }

    private function activeWaitDurationMilliseconds(): int
    {
        $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
        $waiters = (new \ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);
        foreach ($waiters as $list) {
            foreach ($list as $waiter) {
                return $waiter->durationMilliseconds;
            }
        }

        $this->fail('No active WAIT duration is registered.');
    }

    private function awaitNotifierDeadline(int $readyAt): string
    {
        $deadline = microtime(true) + self::SAFETY_SECONDS;
        do {
            $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
            $watches = (new \ReflectionProperty(QueueNotifier::class, 'watches'))->getValue($notifier);
            foreach ($watches as $watch) {
                if (!$watch->querying && !$watch->dirty && $watch->timerReadyAt === $readyAt && null !== $watch->timerId) {
                    $timerId = $watch->timerId;
                    EventLoop::disable($timerId);

                    return $timerId;
                }
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Deadline timer was not scheduled at '.$readyAt.'.');
    }

    private function awaitNotifierWaitTimeout(): string
    {
        $deadline = microtime(true) + self::SAFETY_SECONDS;
        do {
            $notifier = (new \ReflectionProperty(Broker::class, 'notifier'))->getValue($this->broker);
            $waiters = (new \ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);
            foreach ($waiters as $list) {
                foreach ($list as $waiter) {
                    if (null !== $waiter->timeoutId) {
                        $timeoutId = $waiter->timeoutId;
                        EventLoop::disable($timeoutId);

                        return $timeoutId;
                    }
                }
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Waiter timeout timer was not armed.');
    }

    private function fireNotifierTimer(string $timerId): void
    {
        $driver = EventLoop::getDriver();
        $callbacks = null;
        $reflection = new \ReflectionObject($driver);
        while (null !== $reflection) {
            if ($reflection->hasProperty('callbacks')) {
                $callbacks = $reflection->getProperty('callbacks')->getValue($driver);
                break;
            }
            $reflection = $reflection->getParentClass() ?: null;
        }
        $this->assertIsArray($callbacks);
        $callback = $callbacks[$timerId] ?? null;
        $this->assertInstanceOf(TimerCallback::class, $callback);
        EventLoop::cancel($timerId);
        ($callback->closure)($timerId);
    }

    private function runAsync(\Closure $operation): void
    {
        async($operation)->await();
    }

    private function turn(): void
    {
        $suspension = EventLoop::getSuspension();
        $id = EventLoop::delay(0, static function () use ($suspension): void {
            $suspension->resume();
        });
        try {
            $suspension->suspend();
        } finally {
            EventLoop::cancel($id);
        }
    }
}
