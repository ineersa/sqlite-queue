<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Messenger\DTO\ConsumeWaitSessionDTO;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\Protocol\Limits;
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

    private function timeout(int $budget, ?float $deadlineOffset): int
    {
        $clock = new MockClock('2026-10-03 00:00:00 UTC');
        $subscriber = new NativeConsumeWaitSubscriber(new ServiceLocator([]), $clock);
        $transport = (new TransportFactory())->createTransport('sqlite-queue://jobs?endpoint=/tmp/unused-wait-budget.sock', [], new PhpSerializer());
        $session = new ConsumeWaitSessionDTO(
            new Command('probe'), $transport, 'jobs', 1000000, false,
            new DeferredCancellation(), $budget,
            null === $deadlineOffset ? null : $clock->now()->getTimestamp() + $deadlineOffset,
            new Worker([], new MessageBus()),
        );

        return (new \ReflectionMethod($subscriber, 'waitTimeoutMilliseconds'))->invoke($subscriber, $session);
    }
}
