<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Broker\QueueNotifierWaiter;
use Ineersa\SqliteQueue\Broker\QueueNotifierWatch;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Tests\Support\ProcessTestCase;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\TimerCallback;

use function Amp\async;

/**
 * Focused coordinator tests for the readiness-backed wait API.
 *
 * Delayed timer firing uses reflection against the live Revolt timer callback. That is test-only
 * and does not add production hooks.
 */
final class QueueNotifierTest extends ProcessTestCase
{
    /** @var list<SqliteQueueStorage> */
    private array $storages = [];
    private ?QueueNotifier $notifier = null;
    private int $now = 1_700_000_000_000;
    /** @var list<\Throwable> */
    private array $failures = [];

    protected function tearDown(): void
    {
        try {
            $this->notifier?->close();
            $this->notifier = null;
            foreach (array_reverse($this->storages) as $storage) {
                try {
                    $storage->close();
                } catch (\Throwable) {
                }
            }
            $this->storages = [];
        } finally {
            parent::tearDown();
        }
    }

    public function testZeroDurationReportsCurrentReadinessWithoutClaiming(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $this->assertFalse($notifier->wait($name, 0, new TimeoutCancellation(5)));
            $queue->send($name, 'ready');
            $this->assertTrue($notifier->wait($name, 0, new TimeoutCancellation(5)));
            $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages WHERE reserved_until IS NULL'));
            $this->assertSame([], $this->failures);
        });
    }

    public function testNotifyWakesRegisteredWaiter(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $this->awaitEmptyWatch($notifier, $name->value);
            $queue->send($name, 'published after wait registered');
            $notifier->notify($name);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testWaitAnyWakesForEitherSelectedQueueAndDetachesFromAll(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $jobs = new QueueName('jobs');
            $other = new QueueName('other');
            $waiting = async(static fn (): bool => $notifier->waitAny([$jobs, $other], 5_000, new TimeoutCancellation(5)));
            $this->awaitEmptyWatch($notifier, $jobs->value);
            $this->awaitEmptyWatch($notifier, $other->value);
            $this->assertSame(['jobs', 'other'], $this->waiterNames($notifier));
            $queue->send($other, 'wake other');
            $notifier->notify($other);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->waiterNames($notifier));
            $this->assertNull($this->watch($notifier, $jobs->value));
            $this->assertNull($this->watch($notifier, $other->value));
            $this->assertSame([], $this->failures);
        });
    }

    public function testZeroDurationWaitAnyRequiresEveryQueueNotReady(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $jobs = new QueueName('jobs');
            $other = new QueueName('other');
            $this->assertFalse($notifier->waitAny([$jobs, $other], 0, new TimeoutCancellation(5)));
            $queue->send($other, 'ready');
            $this->assertTrue($notifier->waitAny([$jobs, $other], 0, new TimeoutCancellation(5)));
            $this->assertSame([], $this->waiterNames($notifier));
            $this->assertSame([], $this->failures);
        });
    }

    public function testWaitAnyRejectsInvalidQueueListsLocally(): void
    {
        $notifier = $this->notifier($this->open());
        foreach ([
            [[], 'WAIT_ANY requires at least one queue.'],
            [[new QueueName('jobs'), new QueueName('jobs')], 'WAIT_ANY queue names must be unique.'],
            [array_fill(0, Limits::MAX_WAIT_QUEUES + 1, new QueueName('jobs')), \sprintf('WAIT_ANY accepts at most %d queues.', Limits::MAX_WAIT_QUEUES)],
        ] as [$queues, $message]) {
            try {
                $notifier->waitAny($queues, 0, new \Amp\NullCancellation());
                $this->fail('Invalid WAIT_ANY list must fail locally.');
            } catch (\InvalidArgumentException $error) {
                $this->assertSame($message, $error->getMessage());
            }
        }
        $this->assertSame([], $this->waiterNames($notifier));
    }

    public function testRegistrationBeforeRecheckCannotMissCommittedSend(): void
    {
        $this->runAsync(function (): void {
            $entered = new DeferredFuture();
            $release = new DeferredFuture();
            $armed = false;
            $queue = $this->open();
            $notifier = $this->notifier($queue, static function () use ($entered, $release, &$armed): void {
                if ($armed) {
                    $armed = false;
                    $entered->complete();
                    $release->getFuture()->await(new TimeoutCancellation(5));
                }
            });
            $name = new QueueName('jobs');
            $armed = true;
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->open()->send($name, 'committed during readiness query');
            $notifier->notify($name);
            $release->complete();
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testDelayedDeadlineFiresAfterControlledClockAdvance(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $queue->send($name, 'later', delay: 100);
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $timerId = $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 100);
            $this->now += 100;
            $this->fireTimer($timerId);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testEarlierInsertedDeadlineReschedulesWatch(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $queue->send($name, 'late', delay: 500);
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 500);
            $queue->send($name, 'earlier', delay: 50);
            $notifier->notify($name);
            $timerId = $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 50);
            $this->now += 50;
            $this->fireTimer($timerId);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testReservedMessageSchedulesVisibilityDeadlineNotAvailability(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open(visibility: 200);
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $queue->send($name, 'claimed');
            $delivery = $queue->receive($name, bin2hex(random_bytes(32)));
            $this->assertNotNull($delivery);
            $this->assertSame($delivery->reservedUntil, $queue->earliestEligibility($name));
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $timerId = $this->awaitDeadlineTimer($notifier, $name->value, $delivery->reservedUntil);
            $this->assertSame($delivery->reservedUntil, $this->watch($notifier, $name->value)?->timerReadyAt);
            $this->now = $delivery->reservedUntil;
            $this->fireTimer($timerId);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testRestartRebuildsDeadlineFromPersistedState(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $name = new QueueName('jobs');
            $queue->send($name, 'persisted delay', delay: 250);
            $deadline = $this->now + 250;
            $first = $this->notifier($queue);
            $waiting = async(static fn (): bool => $first->wait($name, 5_000, new TimeoutCancellation(5)));
            $this->awaitDeadlineTimer($first, $name->value, $deadline);
            try {
                $first->close();
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Closed notifier must fail outstanding waits.');
            } catch (CancelledException) {
                $this->addToAssertionCount(1);
            }
            $second = $this->notifier($queue);
            $waiting = async(static fn (): bool => $second->wait($name, 5_000, new TimeoutCancellation(5)));
            $timerId = $this->awaitDeadlineTimer($second, $name->value, $deadline);
            $this->now = $deadline;
            $this->fireTimer($timerId);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testCancellationRemovesWaiterAndTimerState(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $queue->send($name, 'future', delay: 1_000);
            $lifetime = new DeferredCancellation();
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, $lifetime->getCancellation()));
            $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 1_000);
            $lifetime->cancel();
            try {
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Cancelled wait must fail.');
            } catch (CancelledException) {
                $this->addToAssertionCount(1);
            }
            $this->assertNull($this->watch($notifier, $name->value));
            $this->assertSame([], $this->waiterNames($notifier));
            $this->assertSame([], $this->failures);
        });
    }

    public function testPositiveWaitIgnoresForwardWallClockJump(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $this->awaitEmptyWatch($notifier, $name->value);
            $timeoutId = $this->awaitWaiterTimeout($notifier, $name->value);
            $this->now += 60_000;
            $this->turn();
            $this->assertFalse($waiting->isComplete());
            $this->fireTimer($timeoutId);
            $this->assertFalse($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testBackwardWallClockJumpDoesNotHangZeroDurationWait(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $this->now = 2_000;
            $this->assertFalse($notifier->wait($name, 0, new TimeoutCancellation(5)));
            $this->now = 1_000;
            $this->assertFalse($notifier->wait($name, 0, new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testFinalWaiterDetachmentCancelsDeadlineDuringInFlightRefresh(): void
    {
        $this->runAsync(function (): void {
            $entered = new DeferredFuture();
            $release = new DeferredFuture();
            $armed = false;
            $queue = $this->open();
            $notifier = $this->notifier($queue, static function () use ($entered, $release, &$armed): void {
                if ($armed) {
                    $armed = false;
                    $entered->complete();
                    $release->getFuture()->await(new TimeoutCancellation(5));
                }
            });
            $name = new QueueName('jobs');
            $queue->send($name, 'future', delay: 1_000);
            $lifetime = new DeferredCancellation();
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, $lifetime->getCancellation()));
            $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 1_000);
            $armed = true;
            $notifier->notify($name);
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $lifetime->cancel();
            try {
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Cancelled wait must fail.');
            } catch (CancelledException) {
                $this->addToAssertionCount(1);
            }
            // Last waiter detachment cancels the deadline and drops the watch immediately,
            // even while the readiness query is still paused.
            $this->assertNull($this->watch($notifier, $name->value));
            $this->assertSame([], $this->waiterNames($notifier));
            $release->complete();
            $deadline = microtime(true) + 5;
            do {
                if (null === $this->watch($notifier, $name->value)) {
                    break;
                }
                $this->turn();
            } while (microtime(true) < $deadline);
            $this->assertNull($this->watch($notifier, $name->value), 'Stale readiness query must drop the idle watch.');
            $this->assertSame([], $this->waiterNames($notifier));
            $this->assertSame([], $this->failures);
        });
    }

    public function testCancelledWatchCannotCompleteAReplacementWaitFromItsOldQuery(): void
    {
        $this->runAsync(function (): void {
            $entered = [new DeferredFuture(), new DeferredFuture()];
            $release = [new DeferredFuture(), new DeferredFuture()];
            $reads = 0;
            $queue = $this->open();
            $name = new QueueName('jobs');
            $queue->send($name, 'ready');
            $notifier = $this->notifier($queue, static function () use ($entered, $release, &$reads): void {
                if ($reads < 2) {
                    $index = $reads++;
                    $entered[$index]->complete();
                    $release[$index]->getFuture()->await(new TimeoutCancellation(5));
                }
            });
            $cancel = new DeferredCancellation();
            $old = async(static fn (): bool => $notifier->wait($name, 5_000, $cancel->getCancellation()));
            try {
                $entered[0]->getFuture()->await(new TimeoutCancellation(5));
                $cancel->cancel();
                try {
                    $old->await(new TimeoutCancellation(5));
                    $this->fail('Cancelled wait completed.');
                } catch (CancelledException) {
                    $this->assertNull($this->watch($notifier, 'jobs'));
                }
                $replacement = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
                $replacement->ignore();
                $this->awaitWaiterTimeout($notifier, 'jobs');
                $release[0]->complete();
                $entered[1]->getFuture()->await(new TimeoutCancellation(5));
                $this->turn();
                $this->assertFalse($replacement->isComplete(), 'Only the replacement query may complete its wait.');
                $this->assertTrue($this->watch($notifier, 'jobs')->querying);
                $release[1]->complete();
                $this->assertTrue($replacement->await(new TimeoutCancellation(5)));
            } finally {
                foreach ($release as $barrier) {
                    if (!$barrier->isComplete()) {
                        $barrier->complete();
                    }
                }
                $old->ignore();
                $notifier->close();
            }
        });
    }

    public function testClosedNotifierRejectsWithoutRegisteringAWait(): void
    {
        $notifier = $this->notifier($this->open());
        $notifier->close();
        try {
            $notifier->wait(new QueueName('jobs'), 0, new \Amp\NullCancellation());
            $this->fail('Closed notifier accepted a wait.');
        } catch (\Ineersa\SqliteQueue\Exception\ClientContextClosedException $error) {
            $this->assertSame('Broker wait service is closed.', $error->getMessage());
        }
        $this->assertSame([], $this->waiterNames($notifier));
        $this->assertNull($this->watch($notifier, 'jobs'));
    }

    public function testNumericQueueWaitSurvivesCloseWithoutIntegerKeyCoercion(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $numeric = new QueueName('1');
            $padded = new QueueName('01');
            $queue->send($numeric, 'literal-one', delay: 1_000);
            $queue->send($padded, 'zero-padded', delay: 1_000);
            $waiting = async(static fn (): bool => $notifier->wait($numeric, 5_000, new TimeoutCancellation(5)));
            $timerId = $this->awaitDeadlineTimer($notifier, $numeric->value, $this->now + 1_000);
            $this->assertSame(['1'], $this->waiterNames($notifier));
            $this->assertSame('1', $this->watch($notifier, $numeric->value)?->queue->value);
            $this->assertNull($this->watch($notifier, $padded->value));
            $this->assertSame(1, $this->scalar("SELECT count(*) FROM queue_messages WHERE queue = '1'"));
            $this->assertSame(1, $this->scalar("SELECT count(*) FROM queue_messages WHERE queue = '01'"));
            $notifier->close();
            try {
                $waiting->await(new TimeoutCancellation(5));
                $this->fail('Close must cancel the numeric-queue wait.');
            } catch (CancelledException) {
                $this->addToAssertionCount(1);
            }
            $this->assertSame([], $this->waiterNames($notifier));
            $this->assertNull($this->watch($notifier, $numeric->value));
            $this->assertNotContains($timerId, EventLoop::getIdentifiers());
            $this->assertSame(1, $this->scalar("SELECT count(*) FROM queue_messages WHERE queue = '1'"));
            $this->assertSame(1, $this->scalar("SELECT count(*) FROM queue_messages WHERE queue = '01'"));
        });
    }

    public function testCapturedDeadlineIsDisabledBeforeReturnAndRemainsManuallyFireable(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $queue->send($name, 'later', delay: 50);
            $waiting = async(static fn (): bool => $notifier->wait($name, 5_000, new TimeoutCancellation(5)));
            $timerId = $this->awaitDeadlineTimer($notifier, $name->value, $this->now + 50);
            $this->assertFalse(EventLoop::isEnabled($timerId));
            $this->assertSame($timerId, $this->watch($notifier, $name->value)?->timerId);
            $this->turn();
            $this->assertFalse($waiting->isComplete(), 'A disabled deadline must not fire on its own.');
            $this->assertSame($timerId, $this->watch($notifier, $name->value)?->timerId);
            $this->now += 50;
            $this->fireTimer($timerId);
            $this->assertTrue($waiting->await(new TimeoutCancellation(5)));
            $this->assertSame([], $this->failures);
        });
    }

    public function testImmediateSubscriptionCancellationPreservesCauseAndLeavesNoTimers(): void
    {
        $notifier = $this->notifier($this->open());
        $cause = new \RuntimeException('Cancelled during subscription.');
        $unsubscribed = false;
        $cancellation = $this->createStub(Cancellation::class);
        $cancellation->method('subscribe')->willReturnCallback(static function (\Closure $callback) use ($cause): string {
            $callback(new CancelledException($cause));

            return 'subscription';
        });
        $cancellation->method('unsubscribe')->willReturnCallback(function (string $id) use (&$unsubscribed): void {
            $this->assertSame('subscription', $id);
            $unsubscribed = true;
        });
        $identifiers = EventLoop::getIdentifiers();
        try {
            $notifier->wait(new QueueName('jobs'), 5_000, $cancellation);
            $this->fail('Cancelled subscription accepted a wait.');
        } catch (CancelledException $error) {
            $this->assertSame($cause, $error->getPrevious());
        }
        $this->assertTrue($unsubscribed);
        $this->assertSame([], $this->waiterNames($notifier));
        $this->assertNull($this->watch($notifier, 'jobs'));
        $this->assertSame([], array_values(array_diff(EventLoop::getIdentifiers(), $identifiers)), 'Cancellation must not leave new watchers; unrelated watchers may finish.');
    }

    public function testQueryFailureFailsClosed(): void
    {
        $this->runAsync(function (): void {
            $queue = $this->open();
            $notifier = $this->notifier($queue);
            $name = new QueueName('jobs');
            $this->latestStorage()->close();
            try {
                $notifier->wait($name, 5_000, new TimeoutCancellation(5));
                $this->fail('Storage failure must reject the wait.');
            } catch (\Throwable $error) {
                $this->assertNotInstanceOf(CancelledException::class, $error);
            }
            $this->assertNotSame([], $this->failures);
            $this->assertNull($this->watch($notifier, $name->value));
        });
    }

    private function open(int $visibility = 5000): Queue
    {
        $storage = SqliteQueueStorage::open($this->database->path());
        $this->storages[] = $storage;

        return new Queue($storage, $visibility, fn (): int => $this->now);
    }

    private function notifier(Queue $queue, ?\Closure $onEligibility = null): QueueNotifier
    {
        $this->notifier?->close();
        $this->failures = [];
        $this->notifier = new QueueNotifier(
            static function (QueueName $name, ?Cancellation $cancellation = null) use ($queue, $onEligibility): ?int {
                if (null !== $onEligibility) {
                    $onEligibility();
                }

                return $queue->earliestEligibility($name, $cancellation);
            },
            fn (): int => $this->now,
            function (\Throwable $error): void {
                $this->failures[] = $error;
            },
        );

        return $this->notifier;
    }

    private function latestStorage(): SqliteQueueStorage
    {
        return $this->storages[array_key_last($this->storages)];
    }

    private function scalar(string $sql): mixed
    {
        $database = new \SQLite3($this->database->path());
        try {
            return $database->querySingle($sql);
        } finally {
            $database->close();
        }
    }

    private function awaitEmptyWatch(QueueNotifier $notifier, string $queue): void
    {
        $deadline = microtime(true) + 5;
        do {
            $watch = $this->watch($notifier, $queue);
            if (null !== $watch && !$watch->querying && !$watch->dirty && null === $watch->timerId) {
                return;
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Empty-queue watch was not established.');
    }

    private function awaitDeadlineTimer(QueueNotifier $notifier, string $queue, int $readyAt): string
    {
        $deadline = microtime(true) + 5;
        do {
            $watch = $this->watch($notifier, $queue);
            if (null !== $watch && !$watch->querying && !$watch->dirty && $watch->timerReadyAt === $readyAt && null !== $watch->timerId) {
                $timerId = $watch->timerId;
                EventLoop::disable($timerId);

                return $timerId;
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Deadline timer was not scheduled at '.$readyAt.'.');
    }

    private function awaitWaiterTimeout(QueueNotifier $notifier, string $queue): string
    {
        $deadline = microtime(true) + 5;
        do {
            /** @var array<string, list<QueueNotifierWaiter>> $waiters */
            $waiters = (new \ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);
            foreach ($waiters[$this->watchKey($queue)] ?? [] as $waiter) {
                if (null !== $waiter->timeoutId) {
                    $timeoutId = $waiter->timeoutId;
                    EventLoop::disable($timeoutId);

                    return $timeoutId;
                }
            }
            $this->turn();
        } while (microtime(true) < $deadline);

        $this->fail('Waiter timeout timer was not armed.');
    }

    private function watch(QueueNotifier $notifier, string $queue): ?QueueNotifierWatch
    {
        /** @var array<string, QueueNotifierWatch> $watches */
        $watches = (new \ReflectionProperty(QueueNotifier::class, 'watches'))->getValue($notifier);

        return $watches[$this->watchKey($queue)] ?? null;
    }

    /**
     * @return list<string>
     */
    private function waiterNames(QueueNotifier $notifier): array
    {
        /** @var array<string, list<QueueNotifierWaiter>> $waiters */
        $waiters = (new \ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);

        return array_map(
            static fn (string $key): string => str_starts_with($key, 'queue:') ? substr($key, \strlen('queue:')) : $key,
            array_keys($waiters),
        );
    }

    private function watchKey(string $queue): string
    {
        return 'queue:'.$queue;
    }

    private function fireTimer(string $timerId): void
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
        $this->turn();
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
