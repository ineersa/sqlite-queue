<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Clock\Clock;
use Symfony\Component\Clock\MockClock;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;

use function Amp\async;

final class BrokerVisibilityTest extends TestCase
{
    public static function configuredLeases(): iterable
    {
        yield 'bundle configuration' => [[], 15000, 15, 'full'];
        yield 'CLI synchronous override' => [['--synchronous' => 'normal'], 15000, 15, 'full'];
        yield 'CLI override' => [['--redeliver-timeout' => '20', '--synchronous' => 'full'], 20000, 15, 'normal'];
        yield 'default' => [[], 60000, null, 'normal'];
    }

    #[DataProvider('configuredLeases')]
    public function testNativeBrokerLeaseSurvivesSixSecondHandler(array $options, int $lease, ?int $configuredSeconds, string $configuredMode): void
    {
        $fixture = new IsolatedDatabase();
        $project = $fixture->directory().'/app';
        $filesystem = new Filesystem();
        $filesystem->mirror(__DIR__.'/Fixtures/NativeApp/config', $project.'/config');
        if (null !== $configuredSeconds) {
            $filesystem->dumpFile($project.'/config/packages/sqlite_queue.yaml', "sqlite_queue:\n    redeliver_timeout: $configuredSeconds\n    synchronous: $configuredMode\n");
        }
        $clock = new MockClock('2026-10-03 00:00:00 UTC');
        $previousClock = Clock::get();
        Clock::set($clock);
        $kernel = new NativeKernel($project);
        $kernel->boot();
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
        $this->assertSame(null === $configuredSeconds ? 'normal' : $configuredMode, $application->find('sqlite-queue:broker')->getDefinition()->getOption('synchronous')->getDefault());
        $ready = new DeferredFuture();
        $output = new class($ready) extends BufferedOutput {
            public function __construct(private readonly DeferredFuture $ready)
            {
                parent::__construct();
            }

            protected function doWrite(string $message, bool $newline): void
            {
                parent::doWrite($message, $newline);
                if (str_contains($message, '"event":"ready"')) {
                    $this->ready->complete();
                }
            }
        };
        $run = async(static fn (): int => $application->run(new ArrayInput($options + [
            'command' => 'sqlite-queue:broker',
            '--database' => $fixture->path(),
            '--endpoint' => $fixture->directory().'/queue.sock',
        ]), $output));
        try {
            $ready->getFuture()->await(new TimeoutCancellation(15));
            $owner = Client::connect($fixture->directory().'/queue.sock');
            $competitor = Client::connect($fixture->directory().'/queue.sock');
            try {
                $owner->send('jobs', 'handler-effect');
                $delivery = $owner->receive('jobs');
                $this->assertNotNull($delivery);
                $this->assertSame($lease, $delivery->reservedUntil - $delivery->availableAt);
                $handled = [];
                $bus = new MessageBus([new HandleMessageMiddleware(new HandlersLocator([
                    NativeProbeMessage::class => [function (NativeProbeMessage $message) use ($clock, $competitor, &$handled): void {
                        // Simulate a handler taking longer than the original five-second lease.
                        $clock->modify('+6 seconds');
                        $this->assertNull($competitor->receive('jobs'));
                        $handled[] = $message;
                    }],
                ]))]);
                $message = new NativeProbeMessage($delivery->body);
                $bus->dispatch($message);
                $this->assertSame([$message], $handled);
                $owner->acknowledge($delivery->receipt);
                $this->assertNull($competitor->receive('jobs'));
            } finally {
                $owner->close();
                $competitor->close();
            }
        } finally {
            if (!$run->isComplete()) {
                posix_kill(getmypid(), \SIGTERM);
            }
            $this->assertSame(0, $run->await(new TimeoutCancellation(15)));
            $kernel->shutdown();
            Clock::set($previousClock);
            $filesystem->remove($project);
            $fixture->remove();
        }
    }

    public static function invalidLeases(): iterable
    {
        foreach (['0', '-1', '1.5', 'invalid', (string) (intdiv(\PHP_INT_MAX, 1000) + 1)] as $value) {
            yield $value => [$value];
        }
    }

    public function testStandaloneDefaultKeepsSixtySecondRedelivery(): void
    {
        $this->assertSame('60', (new BrokerCommand())->getDefinition()->getOption('redeliver-timeout')->getDefault());
    }

    public function testCommandAcceptsBothSecondBoundaries(): void
    {
        $this->assertSame('1', (new BrokerCommand(1))->getDefinition()->getOption('redeliver-timeout')->getDefault());
        $maximum = intdiv(\PHP_INT_MAX, 1000);
        $this->assertSame((string) $maximum, (new BrokerCommand($maximum))->getDefinition()->getOption('redeliver-timeout')->getDefault());
    }

    public static function invalidConfiguredSeconds(): iterable
    {
        yield 'zero' => [0, 'Redelivery timeout must be positive seconds.'];
        yield 'negative' => [-1, 'Redelivery timeout must be positive seconds.'];
        yield 'overflow' => [intdiv(\PHP_INT_MAX, 1000) + 1, 'Redelivery timeout exceeds the supported milliseconds range.'];
    }

    #[DataProvider('invalidConfiguredSeconds')]
    public function testInvalidConfiguredSecondsAreRejected(int $seconds, string $message): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
        new BrokerCommand($seconds);
    }

    #[DataProvider('invalidLeases')]
    public function testInvalidCliLeaseFailsBeforeStartup(string $value): void
    {
        $tester = new CommandTester(new BrokerCommand());
        $this->assertSame(1, $tester->execute(['--redeliver-timeout' => $value]));
        $this->assertStringContainsString('InvalidArgumentException', $tester->getDisplay());
    }

    public static function invalidSynchronousModes(): iterable
    {
        foreach (['off', 'extra', 'automatic', 'invalid'] as $mode) {
            yield [$mode];
        }
    }

    #[DataProvider('invalidSynchronousModes')]
    public function testBundleRejectsInvalidSynchronousMode(string $mode): void
    {
        $fixture = new IsolatedDatabase();
        $project = $fixture->directory().'/app';
        $filesystem = new Filesystem();
        $filesystem->mirror(__DIR__.'/Fixtures/NativeApp/config', $project.'/config');
        $filesystem->dumpFile($project.'/config/packages/sqlite_queue.yaml', "sqlite_queue:\n    synchronous: $mode\n");
        $kernel = new NativeKernel($project);
        try {
            $this->expectException(\Symfony\Component\Config\Definition\Exception\InvalidConfigurationException::class);
            $this->expectExceptionMessage('sqlite_queue.synchronous');
            $kernel->boot();
        } finally {
            $kernel->shutdown();
            $filesystem->remove($project);
            $fixture->remove();
        }
    }
}
