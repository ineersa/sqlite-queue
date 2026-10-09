<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Ineersa\SqliteQueue\Messenger\DTO\ConsumeWaitSessionDTO;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\ControllableClock;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Clock\MockClock;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Worker;

final class NativeConsumeWaitSubscriberTest extends TestCase
{
    public static function invalidBudgets(): iterable
    {
        yield 'negative' => [-1, 'Wait budget must be positive milliseconds.'];
        yield 'zero' => [0, 'Wait budget must be positive milliseconds.'];
        yield 'above maximum' => [Limits::MAX_WAIT_MILLISECONDS + 1, 'Wait budget exceeds the protocol maximum.'];
    }

    #[DataProvider('invalidBudgets')]
    public function testInvalidBudgetIsRejectedBeforeDeadlineShortening(int $budget, string $message): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
        $this->timeout($budget, -1.0);
    }

    public function testBudgetBoundariesAreNotClamped(): void
    {
        $this->assertSame(1, $this->timeout(1, null));
        $this->assertSame(Limits::MAX_WAIT_MILLISECONDS, $this->timeout(Limits::MAX_WAIT_MILLISECONDS, null));
    }

    public function testDeadlineShortensValidBudget(): void
    {
        $this->assertSame(250, $this->timeout(1000, 0.25));
        $this->assertSame(0, $this->timeout(1000, 0.0));
    }

    public function testMetadataSelectionUsesFirstTransportNotificationOwnerAcrossDistinctQueues(): void
    {
        $factory = new TransportFactory();
        $serializer = new PhpSerializer();
        $first = $factory->createTransport('sqlite-queue://jobs?endpoint=/tmp/sqlite-queue-wait-a.sock', [], $serializer);
        $second = $factory->createTransport('sqlite-queue://reports?endpoint=/tmp/sqlite-queue-wait-a.sock', [], $serializer);
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator([
            'async' => static fn (): Transport => $first,
            'reports' => static fn (): Transport => $second,
        ]), new MockClock('2026-10-03 00:00:00 UTC'));

        $wait = (new \ReflectionMethod($subscriber, 'buildWait'))->invoke($subscriber, ['async', 'reports']);
        $variables = (new \ReflectionFunction($wait))->getStaticVariables();
        $this->assertSame($first, $variables['owner'] ?? null);
        $this->assertSame(['jobs', 'reports'], $variables['queues'] ?? null);
    }

    public function testMixedOrForeignBrokerSelectionsUseBoundedClockFallback(): void
    {
        $factory = new TransportFactory();
        $serializer = new PhpSerializer();
        $ours = $factory->createTransport('sqlite-queue://jobs?endpoint=/tmp/sqlite-queue-wait-a.sock', [], $serializer);
        $otherBroker = $factory->createTransport('sqlite-queue://jobs?endpoint=/tmp/sqlite-queue-wait-b.sock', [], $serializer);
        $clock = new ControllableClock(1_700_000_000.0);
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator([
            'async' => static fn (): Transport => $ours,
            'other' => static fn (): Transport => $otherBroker,
            'sync' => static fn (): object => new \stdClass(),
        ]), $clock);

        $foreign = (new \ReflectionMethod($subscriber, 'buildWait'))->invoke($subscriber, ['async', 'other']);
        $mixed = (new \ReflectionMethod($subscriber, 'buildWait'))->invoke($subscriber, ['async', 'sync']);
        $this->assertFalse($foreign(250, new NullCancellation()));
        $this->assertFalse($mixed(250, new NullCancellation()));
        $this->assertSame([0.25, 0.25], $clock->sleepCalls);
    }

    public function testOversizedDistinctQueueSetUsesBoundedClockFallback(): void
    {
        $factory = new TransportFactory();
        $serializer = new PhpSerializer();
        $locator = [];
        $names = [];
        for ($i = 0; $i <= Limits::MAX_WAIT_QUEUES; ++$i) {
            $name = 'q'.$i;
            $names[] = $name;
            $locator[$name] = static function () use ($factory, $serializer, $i): Transport {
                return $factory->createTransport(
                    'sqlite-queue://queue'.$i.'?endpoint=/tmp/sqlite-queue-wait-a.sock',
                    [],
                    $serializer,
                );
            };
        }
        $clock = new ControllableClock(1_700_000_000.0);
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator($locator), $clock);
        $wait = (new \ReflectionMethod($subscriber, 'buildWait'))->invoke($subscriber, $names);
        $this->assertFalse($wait(100, new NullCancellation()));
        $this->assertSame([0.1], $clock->sleepCalls);
    }

    public function testSelectedSqliteQueueReceiverActivatesOmittedSleepBeforeWorkerOptions(): void
    {
        $factory = new TransportFactory();
        $serializer = new PhpSerializer();
        $transport = $factory->createTransport('sqlite-queue://jobs?endpoint=/tmp/sqlite-queue-wait-a.sock', [], $serializer);
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator([
            'async' => static fn (): Transport => $transport,
            'sync' => static fn (): object => new \stdClass(),
        ]), new MockClock('2026-10-03 00:00:00 UTC'));
        $command = new \Symfony\Component\Messenger\Command\ConsumeMessagesCommand(
            new \Symfony\Component\Messenger\RoutableMessageBus(new ServiceLocator([]), new MessageBus()),
            new ServiceLocator([]),
            new \Symfony\Component\EventDispatcher\EventDispatcher(),
        );
        $input = new \Symfony\Component\Console\Input\ArrayInput([]);
        $input->bind($command->getDefinition());
        $event = new \Symfony\Component\Console\Event\ConsoleCommandEvent($command, $input, new \Symfony\Component\Console\Output\NullOutput());
        $subscriber->onConsoleCommand($event);
        $this->assertSame(1, $input->getOption('sleep'));
        $subscriber->onSelectedReceiver('sync');
        $this->assertSame(1, $input->getOption('sleep'));
        $subscriber->onSelectedReceiver('async');
        $this->assertSame(0, $input->getOption('sleep'));
    }

    private function timeout(int $budget, ?float $deadlineOffset): int
    {
        $clock = new MockClock('2026-10-03 00:00:00 UTC');
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator([]), $clock);
        $session = new ConsumeWaitSessionDTO(
            new Command('probe'),
            static fn (int $timeoutMilliseconds, $cancellation): bool => false,
            new DeferredCancellation(),
            $budget,
            null === $deadlineOffset ? null : $clock->now()->getTimestamp() + $deadlineOffset,
            new Worker([], new MessageBus()),
        );

        return (new \ReflectionMethod($subscriber, 'waitTimeoutMilliseconds'))->invoke($subscriber, $session);
    }
}
