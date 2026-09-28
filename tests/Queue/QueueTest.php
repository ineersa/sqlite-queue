<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Fabpot\Amp\Sqlite\SqliteTransaction;
use Ineersa\SqliteQueue\Delivery;
use Ineersa\SqliteQueue\InvalidReceipt;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Tests\Driver\DriverTestCase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\DataProvider;

use function Amp\async;

final class QueueTest extends DriverTestCase
{
    private int $now = 1_700_000_000_000;
    /** @var list<Queue> */
    private array $queues = [];

    protected function tearDown(): void
    {
        try {
            foreach ($this->queues as $queue) {
                $queue->close();
            }
        } finally {
            parent::tearDown();
        }
    }

    public function testBinaryEmptyPayloadsAndNamedQueues(): void
    {
        $queue = $this->open();
        $session = $queue->openSession();
        $id = $queue->send('alpha', "\0\xffbody\0", "\xff\0headers");
        $otherId = $queue->send('beta', '', '');
        $delivery = $queue->receive('alpha', $session);
        $this->assertInstanceOf(Delivery::class, $delivery);
        $this->assertSame($id, $delivery->id);
        $this->assertSame("\0\xffbody\0", $delivery->body);
        $this->assertSame("\xff\0headers", $delivery->headers);
        $this->assertSame($this->now + 5000, $delivery->reservedUntil);
        $this->assertNull($queue->receive('alpha', $session));
        $queue->acknowledge($delivery->receipt, $session);
        $other = $queue->receive('beta', $session);
        $this->assertSame($otherId, $other->id);
        $this->assertSame('', $other->body);
        $this->assertSame('', $other->headers);
        $queue->reject($other->receipt, $session);
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->assertSame('wal', $this->scalar('PRAGMA journal_mode'));
    }

    public function testDelayedOrderingAndRestartPreserveOriginalDeadlines(): void
    {
        $queue = $this->open();
        $delayed = $queue->send('jobs', 'delayed', delay: 250);
        $ready = $queue->send('jobs', 'ready');
        $queue->send('elsewhere', 'independent');
        $session = $queue->openSession();
        $this->assertSame($ready, $queue->receive('jobs', $session)->id);
        $this->assertNull($queue->receive('jobs', $session));
        $queue->close();
        $this->now += 249;
        $queue = $this->open();
        $session = $queue->openSession();
        $this->assertNull($queue->receive('jobs', $session));
        $this->assertSame('independent', $queue->receive('elsewhere', $session)->body);
        ++$this->now;
        $delivery = $queue->receive('jobs', $session);
        $this->assertSame($delayed, $delivery->id);
        $this->assertSame(1_700_000_000_250, $delivery->availableAt);
        $queue->acknowledge($delivery->receipt, $session);
        $queue->send('overdue', 'persisted', delay: 1);
        $queue->close();
        $this->now += 1000;
        $queue = $this->open();
        $this->assertSame('persisted', $queue->receive('overdue', $queue->openSession())->body);
    }

    public function testEligibleMessagesUseInsertionOrderNotDeadlineOrder(): void
    {
        $queue = $this->open();
        $first = $queue->send('jobs', 'first', delay: 200);
        $second = $queue->send('jobs', 'second', delay: 100);
        $this->now += 200;
        $session = $queue->openSession();
        $this->assertSame($first, $queue->receive('jobs', $session)->id);
        $this->assertSame($second, $queue->receive('jobs', $session)->id);
    }

    public function testStaleForeignDisconnectedAndPreviousEpochReceipts(): void
    {
        $queue = $this->open(50);
        $queue->send('jobs', 'message');
        $owner = $queue->openSession();
        $foreign = $queue->openSession();
        $first = $queue->receive('jobs', $owner);
        $this->invalid(static fn () => $queue->acknowledge($first->receipt, $foreign));
        $this->invalid(static fn () => $queue->reject($first->receipt, $foreign));
        $this->now += 49;
        $this->assertNull($queue->receive('jobs', $foreign));
        ++$this->now;
        $this->invalid(static fn () => $queue->acknowledge($first->receipt, $owner));
        $next = $queue->receive('jobs', $foreign);
        $this->assertSame($first->id, $next->id);
        $this->assertNotSame($first->receipt, $next->receipt);
        $this->invalid(static fn () => $queue->reject($first->receipt, $owner));
        $queue->closeSession($foreign);
        $this->invalid(static fn () => $queue->acknowledge($next->receipt, $foreign));
        $this->assertNull($queue->receive('jobs', $owner));
        $queue->close();
        $queue = $this->open(50);
        $newOwner = $queue->openSession();
        $this->invalid(static fn () => $queue->reject($next->receipt, $newOwner));
        $this->assertNull($queue->receive('jobs', $newOwner));
        $this->now += 50;
        $last = $queue->receive('jobs', $newOwner);
        $queue->acknowledge($last->receipt, $newOwner);
        $this->invalid(static fn () => $queue->acknowledge($last->receipt, $newOwner));
        $newId = $queue->send('jobs', 'new identity');
        $this->assertGreaterThan($first->id, $newId);
        $this->invalid(static fn () => $queue->reject($last->receipt, $newOwner));
        $this->assertSame($newId, $queue->receive('jobs', $newOwner)->id);
    }

    public function testConcurrentReceiversAcrossConnectionsCannotShareDelivery(): void
    {
        $first = $this->open();
        $second = $this->open();
        $first->send('jobs', 'one');
        $sessions = [$first->openSession(), $second->openSession()];
        $futures = [];
        for ($i = 0; $i < 8; ++$i) {
            $engine = 0 === $i % 2 ? $first : $second;
            $session = $sessions[$i % 2];
            $futures[] = async(static fn () => $engine->receive('jobs', $session));
        }
        $deliveries = array_filter(array_map(static fn ($future) => $future->await(), $futures));
        $this->assertCount(1, $deliveries);
    }

    public static function mutations(): iterable
    {
        yield 'send' => ['send'];
        yield 'claim' => ['receive'];
        yield 'ack' => ['acknowledge'];
        yield 'reject' => ['reject'];
    }

    #[DataProvider('mutations')]
    public function testCommitFailureRollsBackAndReleasesOperation(string $operation): void
    {
        $real = $this->connection();
        $armed = false;
        $connection = $this->forward(SqliteConnection::class, $real, [
            'beginTransaction' => function () use ($real, &$armed) {
                $transaction = $real->beginTransaction();

                return $this->forward(SqliteTransaction::class, $transaction, [
                    'commit' => static function () use ($transaction, &$armed): void {
                        if ($armed) {
                            $armed = false;
                            $transaction->execute('INSERT INTO child_guard VALUES (123)')->close();
                        }
                        $transaction->commit();
                    },
                ]);
            },
        ]);
        $queue = $this->open(connection: $connection);
        $session = $queue->openSession();
        $receipt = $this->prepareOperation($queue, $session, $operation);
        $before = $this->scalar('SELECT count(*) FROM queue_messages');
        $this->exec('CREATE TABLE parent_guard (id INTEGER PRIMARY KEY); CREATE TABLE child_guard (id INTEGER REFERENCES parent_guard(id) DEFERRABLE INITIALLY DEFERRED);');
        $armed = true;
        try {
            $this->mutate($queue, $session, $receipt, $operation);
            $this->fail('A deferred foreign-key violation must fail COMMIT.');
        } catch (SqliteQueryError) {
            $this->assertSame($before, $this->scalar('SELECT count(*) FROM queue_messages'));
            $this->assertSame(0, $this->scalar('SELECT count(*) FROM child_guard'));
        }
        $result = $this->mutate($queue, $session, $receipt, $operation);
        if ('receive' === $operation) {
            $this->assertInstanceOf(Delivery::class, $result);
        }
        $this->assertGreaterThan(0, $queue->send('recovered', 'usable after rollback'));
    }

    #[DataProvider('mutations')]
    public function testNoConfirmationOrInterleavingBeforeCommit(string $operation): void
    {
        $real = $this->connection();
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $begins = 0;
        $connection = $this->forward(SqliteConnection::class, $real, [
            'beginTransaction' => function () use ($real, $entered, $release, &$armed, &$begins) {
                ++$begins;
                $transaction = $real->beginTransaction();

                return $this->forward(SqliteTransaction::class, $transaction, [
                    'commit' => static function () use ($transaction, $entered, $release, &$armed): void {
                        if ($armed) {
                            $armed = false;
                            $entered->complete();
                            $release->getFuture()->await(new TimeoutCancellation(5));
                        }
                        $transaction->commit();
                    },
                ]);
            },
        ]);
        $queue = $this->open(connection: $connection);
        $session = $queue->openSession();
        $receipt = $this->prepareOperation($queue, $session, $operation);
        $before = $this->scalar('SELECT count(*) FROM queue_messages');
        $armed = true;
        $future = async(fn () => $this->mutate($queue, $session, $receipt, $operation));
        $competitor = null;
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($future->isComplete());
            $this->assertSame($before, $this->scalar('SELECT count(*) FROM queue_messages'));
            if ('receive' === $operation) {
                $this->assertNull($this->scalar('SELECT reservation_token FROM queue_messages'));
            }
            $started = new DeferredFuture();
            $beforeBegins = $begins;
            $competitor = async(static function () use ($queue, $started): int {
                $started->complete();

                return $queue->send('other', 'must wait for whole transaction');
            });
            $started->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($competitor->isComplete());
            $this->assertSame($beforeBegins, $begins);
        } finally {
            $release->complete();
            $future->await(new TimeoutCancellation(5));
            $competitor?->await(new TimeoutCancellation(5));
        }
        $this->assertSame($beforeBegins + 1, $begins);
    }

    public function testDisconnectDuringReceiveKeepsCommittedReservationUntilOriginalExpiry(): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $queue = $this->open(50, $this->pauseAt('commit', $entered, $release, $armed));
        $queue->send('jobs', 'reserved');
        $session = $queue->openSession();
        $expiry = $this->now + 50;
        $armed = true;
        $receive = async(static fn () => $queue->receive('jobs', $session));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($receive->isComplete());
            $this->assertNull($this->scalar('SELECT reservation_token FROM queue_messages'));
            $this->now += 10;
            $queue->closeSession($session);
        } finally {
            $release->complete();
            $this->invalid(static fn () => $receive->await(new TimeoutCancellation(5)));
        }
        $this->assertSame($expiry, $this->scalar('SELECT reserved_until FROM queue_messages'));
        $this->assertNotNull($this->scalar('SELECT reservation_token FROM queue_messages'));
        $replacement = $queue->openSession();
        $this->now = $expiry - 1;
        $this->assertNull($queue->receive('jobs', $replacement));
        $this->now = $expiry;
        $this->assertSame('reserved', $queue->receive('jobs', $replacement)->body);
    }

    public static function settlements(): iterable
    {
        yield 'ack' => ['acknowledge'];
        yield 'reject' => ['reject'];
    }

    #[DataProvider('settlements')]
    public function testDisconnectAfterDeleteRollsBackSettlement(string $operation): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $queue = $this->open(50, $this->pauseAt('delete', $entered, $release, $armed));
        $session = $queue->openSession();
        $receipt = $this->prepareOperation($queue, $session, $operation);
        $token = $this->scalar('SELECT reservation_token FROM queue_messages');
        $expiry = $this->scalar('SELECT reserved_until FROM queue_messages');
        $armed = true;
        $settle = async(fn () => $this->mutate($queue, $session, $receipt, $operation));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($settle->isComplete());
            $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
            $queue->closeSession($session);
        } finally {
            $release->complete();
            $this->invalid(static fn () => $settle->await(new TimeoutCancellation(5)));
        }
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->assertSame($token, $this->scalar('SELECT reservation_token FROM queue_messages'));
        $this->assertSame($expiry, $this->scalar('SELECT reserved_until FROM queue_messages'));
        $replacement = $queue->openSession();
        $this->assertNull($queue->receive('jobs', $replacement));
        $this->now = $expiry;
        $this->assertSame('payload', $queue->receive('jobs', $replacement)->body);
    }

    public function testCloseWaitsForInFlightSendAndPreservesItsCommit(): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $connection = $this->pauseAt('commit', $entered, $release, $armed);
        $queue = $this->open(connection: $connection);
        $armed = true;
        $send = async(static fn () => $queue->send('jobs', 'durable before close'));
        $close = null;
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($send->isComplete());
            $started = new DeferredFuture();
            $close = async(static function () use ($queue, $started): void {
                $started->complete();
                $queue->close();
            });
            $started->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($close->isComplete());
            $this->assertFalse($connection->isClosed());
            $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
        } finally {
            $release->complete();
            $id = $send->await(new TimeoutCancellation(5));
            $close?->await(new TimeoutCancellation(5));
        }
        $this->assertTrue($connection->isClosed());
        try {
            $queue->send('jobs', 'after close');
            $this->fail('Closed engine accepted a send.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('closed', $error->getMessage());
        }
        $reopened = $this->open();
        $delivery = $reopened->receive('jobs', $reopened->openSession());
        $this->assertSame($id, $delivery->id);
        $this->assertSame('durable before close', $delivery->body);
    }

    public function testZeroRowClaimRollsBack(): void
    {
        $queue = $this->open();
        $queue->send('jobs', 'one');
        $this->exec('CREATE TRIGGER lose_claim BEFORE UPDATE ON queue_messages BEGIN SELECT RAISE(IGNORE); END;');
        $session = $queue->openSession();
        $this->assertNull($queue->receive('jobs', $session));
        $this->assertNull($this->scalar('SELECT reservation_token FROM queue_messages'));
        $this->exec('DROP TRIGGER lose_claim');
        $this->assertSame('one', $queue->receive('jobs', $session)->body);
    }

    public function testPersistenceWorkerDeathFailsWithoutLeavingOwnershipStuck(): void
    {
        $queue = $this->open();
        $queue->send('jobs', 'confirmed');
        $workers = ProcessTree::ownedBy(getmypid())['workers'];
        $this->assertCount(1, $workers);
        $pid = $workers[0];
        $this->assertNotSame(0, posix_geteuid());
        $this->assertSame(posix_geteuid(), fileowner('/proc/'.$pid));
        $this->assertTrue(posix_kill($pid, \SIGKILL));
        try {
            $queue->send('jobs', 'unconfirmed');
            $this->fail('Dead persistence worker must surface as an error.');
        } catch (SqliteConnectionException) {
            $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        }
        $this->expectException(\RuntimeException::class);
        $queue->send('jobs', 'closed');
    }

    public function testValidationAndClose(): void
    {
        $queue = $this->open();
        foreach (['', '../db', 'a/b', "nul\0", str_repeat('x', 256)] as $name) {
            try {
                $queue->send($name, 'body');
                $this->fail('Invalid queue name accepted.');
            } catch (\InvalidArgumentException) {
            }
        }
        foreach ([-1, \PHP_INT_MAX] as $delay) {
            try {
                $queue->send('jobs', 'body', delay: $delay);
                $this->fail('Invalid delay accepted.');
            } catch (\InvalidArgumentException) {
            }
        }
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
        $queue->close();
        $queue->close();
        $this->expectException(\RuntimeException::class);
        $queue->send('jobs', 'closed');
    }

    public function testDatabaseFullLeavesConfirmedDataAndFailedOrUsableOwnership(): void
    {
        $connection = $this->connection();
        $queue = $this->open(connection: $connection);
        $queue->send('jobs', 'confirmed');
        $pages = $connection->query('PRAGMA page_count');
        $count = $pages->fetchRow()['page_count'];
        $pages->close();
        $connection->query('PRAGMA max_page_count='.$count)->close();
        $failure = null;
        try {
            $queue->send('jobs', str_repeat('x', 1_000_000));
        } catch (\Throwable $error) {
            $failure = $error;
        }
        $this->assertNotNull($failure);
        $cause = $failure;
        while (null !== $cause->getPrevious()) {
            $cause = $cause->getPrevious();
        }
        $this->assertStringContainsString('full', strtolower($cause->getMessage()));
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        // A follow-up must either work or fail explicitly, never remain queued behind a lost lock.
        $followup = async(static fn () => $queue->send('other', 'small'));
        try {
            $followup->await(new TimeoutCancellation(5));
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('closed', $error->getMessage());
        }
        $queue->close();
        $reopened = $this->open();
        $this->assertSame('confirmed', $reopened->receive('jobs', $reopened->openSession())->body);
    }

    public function testInitializationFailureClosesTransferredConnection(): void
    {
        $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));
        try {
            new Queue($connection);
            $this->fail('NORMAL durability must not be accepted.');
        } catch (\InvalidArgumentException) {
            $this->assertTrue($connection->isClosed());
        } finally {
            $connection->close();
        }
        $this->assertGreaterThan(0, $this->open()->send('jobs', 'recovered'));
    }

    public function testSelectionUsesQueueIndexWithoutTemporaryOrdering(): void
    {
        $queue = $this->open();
        $queue->send('jobs', 'ready');
        $queue->send('jobs', 'future', delay: 100);
        $queue->receive('jobs', $queue->openSession());
        $database = new \SQLite3($this->database->path());
        try {
            $result = $database->query("EXPLAIN QUERY PLAN SELECT id FROM queue_messages WHERE queue = 'jobs' AND available_at <= 1700000000000 AND (reserved_until IS NULL OR reserved_until <= 1700000000000) ORDER BY id LIMIT 1");
            $details = [];
            while (false !== ($row = $result->fetchArray(\SQLITE3_ASSOC))) {
                $details[] = $row['detail'];
            }
            $result->finalize();
            $plan = implode('; ', $details);
            $this->assertStringContainsString('queue_messages_order', $plan);
            $this->assertStringNotContainsString('TEMP B-TREE', $plan);
            $this->assertStringNotContainsString('SCAN queue_messages', $plan);
        } finally {
            $database->close();
        }
    }

    private function open(int $visibility = 5000, ?SqliteConnection $connection = null): Queue
    {
        $queue = null === $connection
            ? Queue::open($this->database->path(), $visibility, fn (): int => $this->now)
            : new Queue($connection, $visibility, fn (): int => $this->now);
        $this->queues[] = $queue;

        return $queue;
    }

    private function connection(): SqliteConnection
    {
        return (new SqliteConnector())->connect((new SqliteConfig($this->database->path()))->withJournalMode(SqliteJournalMode::Wal)->withSynchronousMode(SqliteSynchronousMode::Full));
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

    private function exec(string $sql): void
    {
        $database = new \SQLite3($this->database->path());
        $database->enableExceptions(true);
        try {
            $database->exec($sql);
        } finally {
            $database->close();
        }
    }

    private function invalid(\Closure $operation): void
    {
        try {
            $operation();
            $this->fail('A stale or foreign receipt was accepted.');
        } catch (InvalidReceipt) {
            $this->addToAssertionCount(1);
        }
    }

    private function prepareOperation(Queue $queue, string $session, string $operation): ?string
    {
        if ('send' === $operation) {
            return null;
        }
        $queue->send('jobs', 'payload');

        return 'receive' === $operation ? null : $queue->receive('jobs', $session)->receipt;
    }

    private function mutate(Queue $queue, string $session, ?string $receipt, string $operation): mixed
    {
        return match ($operation) {
            'send' => $queue->send('jobs', 'payload'),
            'receive' => $queue->receive('jobs', $session),
            'acknowledge' => $queue->acknowledge($receipt, $session),
            'reject' => $queue->reject($receipt, $session),
        };
    }

    /** Pause once at a real transaction boundary, without adding production hooks. */
    private function pauseAt(string $boundary, DeferredFuture $entered, DeferredFuture $release, bool &$armed): SqliteConnection
    {
        $real = $this->connection();
        $pause = static function () use ($entered, $release, &$armed): void {
            if ($armed) {
                $armed = false;
                $entered->complete();
                $release->getFuture()->await(new TimeoutCancellation(5));
            }
        };

        return $this->forward(SqliteConnection::class, $real, [
            'beginTransaction' => function () use ($real, $pause, $boundary) {
                $transaction = $real->beginTransaction();

                return $this->forward(SqliteTransaction::class, $transaction, [
                    'commit' => static function () use ($transaction, $pause, $boundary): void {
                        if ('commit' === $boundary) {
                            $pause();
                        }
                        $transaction->commit();
                    },
                    'execute' => static function (string $sql, array $parameters = []) use ($transaction, $pause, $boundary) {
                        $result = $transaction->execute($sql, $parameters);
                        if ('delete' === $boundary && str_starts_with($sql, 'DELETE ')) {
                            $pause();
                        }

                        return $result;
                    },
                ]);
            },
        ]);
    }

    /** Test-only forwarding decorators keep all persistence in the real driver. */
    private function forward(string $interface, object $real, array $overrides): object
    {
        $double = $this->createStub($interface);
        foreach ((new \ReflectionClass($interface))->getMethods() as $method) {
            $name = $method->getName();
            $double->method($name)->willReturnCallback($overrides[$name] ?? $real->$name(...));
        }

        return $double;
    }
}
