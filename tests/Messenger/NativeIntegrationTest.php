<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\BrokenDecodeSerializer;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\ControllableClock;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler\NativeProbeMessageHandler;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\ValueObject\QueueName;
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
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMemoryLimitListener;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function Amp\async;

#[RequiresOperatingSystem('Linux')]
final class NativeIntegrationTest extends TestCase
{
    private const int SAFETY_SECONDS = 15;

    private ?IsolatedDatabase $database = null;
    private string $projectDir = '';
    private string $endpoint = '';
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerFuture = null;
    private int $now = 1_700_000_000_000;
    private float $workerNow = 1_700_000_000.0;
    private ?ControllableClock $clock = null;
    /** No saved clock exists before setUp acquires the test-only global clock scope. */
    private ?\Symfony\Component\Clock\ClockInterface $previousClock = null;
    private ?NativeKernel $kernel = null;
    private ?Application $application = null;
    /** @var list<Client> */
    private array $clients = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->database = new IsolatedDatabase();
        $this->endpoint = $this->database->path('queue.sock');
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
        $this->expectException(\Symfony\Component\Messenger\Exception\TransportException::class);
        $transport->get();
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
            $this->assertSame(0, $this->consumeCommand()->getDefinition()->getOption('sleep')->getDefault());
            $this->assertSame(1_000, $this->activeWaitDurationMilliseconds());
            $this->assertSame([], $this->clock?->sleepCalls ?? []);

            $this->publisher()->send(new Envelope(new NativeProbeMessage('woke')));
            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(['woke'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
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

    public function testUnrelatedReceiverLeavesSleepDefaultIntact(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            $this->eventDispatcher()->addListener(WorkerRunningEvent::class, static function (WorkerRunningEvent $event): void {
                $event->getWorker()->stop();
            });

            $exit = $this->runConsume([
                'command' => 'messenger:consume',
                'receivers' => ['sync_probe'],
                '--limit' => '1',
                '--time-limit' => '1',
            ]);
            $this->assertSame(0, $exit);
            $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
            $this->assertSame(0, $this->notifierWaiterCount());
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
            $this->assertSame(0, $this->consumeCommand()->getDefinition()->getOption('sleep')->getDefault());
            $this->publisher()->send(new Envelope(new NativeProbeMessage('later'), [new DelayStamp(50)]));
            $timerId = $this->awaitNotifierDeadline($this->now + 50);
            $this->assertFalse(EventLoop::isEnabled($timerId));
            $this->now += 50;
            $this->fireNotifierTimer($timerId);

            $this->assertSame(0, $consume->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $this->assertSame(['later'], NativeProbeMessageHandler::$handled);
            $this->assertSame([], $this->clock?->sleepCalls ?? []);
        });
    }

    public function testRegexLikeExistingNameLeavesNativeMultiReceiverPollingUntouched(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $selected = [];
            $command = $this->consumeCommand();
            $original = $command->getDefinition()->getOption('sleep')->getDefault();
            $this->eventDispatcher()->addListener(WorkerStartedEvent::class, function (WorkerStartedEvent $event) use (&$selected, $command, $original): void {
                $selected = $event->getWorker()->getMetadata()->getTransportNames();
                $this->assertSame($original, $command->getDefinition()->getOption('sleep')->getDefault());
                if (method_exists($event, 'getIdleTimeout')) {
                    $this->assertSame(1_000_000, $event->getIdleTimeout());
                }
            });
            $this->publisher()->send(new Envelope(new NativeProbeMessage('regex')));
            $this->assertSame(0, $this->runConsume([
                'command' => 'messenger:consume', 'receivers' => ['async.alpha'], '--limit' => '1',
            ]));
            // Native receiver regex expansion was added in Symfony 8.1. The conservative
            // activation guard also leaves literal dotted names on native polling in 8.0.
            $expected = version_compare(\Composer\InstalledVersions::getVersion('symfony/messenger') ?? '0', '8.1.0', '>=')
                ? ['async.alpha', 'asyncXalpha']
                : ['async.alpha'];
            $this->assertSame($expected, $selected);
            $this->assertSame(['regex'], NativeProbeMessageHandler::$handled);
            $this->assertSame(0, $this->notifierWaiterCount());
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
            $this->now += 50;
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
            $broken = new Transport($this->connection(), new QueueName('jobs'), new BrokenDecodeSerializer(), $this->connection());
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
            $this->now += 50;
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
            $this->assertNull($envelope->last(\Ineersa\SqliteQueue\Messenger\Stamp\DeliveryReceiptStamp::class));
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
            $this->now = $availableAt;

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

    /**
     * @param array<string, mixed> $arguments
     */
    private function runConsume(array $arguments): int
    {
        $output = new BufferedOutput();
        $input = new ArrayInput($arguments);
        $input->setInteractive(false);

        return $this->application()->run($input, $output);
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
        return new Transport($this->connection(), new QueueName('jobs'), new PhpSerializer(), $this->connection());
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

    private function startBroker(): void
    {
        $database = $this->database?->path() ?? throw new \LogicException('Missing database.');
        $ready = new DeferredFuture();
        $this->broker = (new BrokerFactory(
            $database,
            $this->endpoint,
            5_000,
            fn (): int => $this->now,
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
        $fs->copy($source.'/config/bundles.php', $this->projectDir.'/config/bundles.php');
        $fs->copy($source.'/config/services.yaml', $this->projectDir.'/config/services.yaml');
        $fs->copy($source.'/config/packages/framework.yaml', $this->projectDir.'/config/packages/framework.yaml');
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
        $total = 0;
        foreach ($waiters as $list) {
            $total += \count($list);
        }

        return $total;
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
