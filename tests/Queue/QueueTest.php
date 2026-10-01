<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Amp\DeferredCancellation;
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
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\ClientContextClosedException;
use Ineersa\SqliteQueue\Exception\ExpiredReceiptException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\MalformedReceiptException;
use Ineersa\SqliteQueue\Exception\NoActiveReservationException;
use Ineersa\SqliteQueue\Exception\ReceiptEpochMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptOwnerMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptTokenMismatchException;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Tests\Driver\DriverTestCase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\Attributes\DataProvider;

use function Amp\async;

final class QueueTest extends DriverTestCase
{
    private int $now = 1_700_000_000_000;
    /** @var list<SqliteQueueStorage> */
    private array $storages = [];

    protected function tearDown(): void
    {
        try {
            foreach ($this->storages as $storage) {
                $storage->close();
            }
        } finally {
            parent::tearDown();
        }
    }

    public function testBinaryEmptyPayloadsAndNamedQueues(): void
    {
        $queue = $this->open();
        $session = $this->owner();
        $id = $queue->send($this->queueName('alpha'), "\0\xffbody\0", "\xff\0headers");
        $otherId = $queue->send($this->queueName('beta'), '', '');
        $delivery = $queue->receive($this->queueName('alpha'), $session);
        $this->assertInstanceOf(DeliveryDTO::class, $delivery);
        $this->assertSame($id, $delivery->id);
        $this->assertSame("\0\xffbody\0", $delivery->body);
        $this->assertSame("\xff\0headers", $delivery->headers);
        $this->assertSame($this->now + 5000, $delivery->reservedUntil);
        $this->assertNull($queue->receive($this->queueName('alpha'), $session));
        $queue->acknowledge($delivery->receipt, $session);
        $other = $queue->receive($this->queueName('beta'), $session);
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
        $delayed = $queue->send($this->queueName('jobs'), 'delayed', delay: 250);
        $ready = $queue->send($this->queueName('jobs'), 'ready');
        $queue->send($this->queueName('elsewhere'), 'independent');
        $session = $this->owner();
        $this->assertSame($ready, $queue->receive($this->queueName('jobs'), $session)->id);
        $this->assertNull($queue->receive($this->queueName('jobs'), $session));
        $this->latestStorage()->close();
        $this->now += 249;
        $queue = $this->open();
        $session = $this->owner();
        $this->assertNull($queue->receive($this->queueName('jobs'), $session));
        $this->assertSame('independent', $queue->receive($this->queueName('elsewhere'), $session)->body);
        ++$this->now;
        $delivery = $queue->receive($this->queueName('jobs'), $session);
        $this->assertSame($delayed, $delivery->id);
        $this->assertSame(1_700_000_000_250, $delivery->availableAt);
        $queue->acknowledge($delivery->receipt, $session);
        $queue->send($this->queueName('overdue'), 'persisted', delay: 1);
        $this->latestStorage()->close();
        $this->now += 1000;
        $queue = $this->open();
        $this->assertSame('persisted', $queue->receive($this->queueName('overdue'), $this->owner())->body);
    }

    public function testEligibleMessagesUseInsertionOrderNotDeadlineOrder(): void
    {
        $queue = $this->open();
        $first = $queue->send($this->queueName('jobs'), 'first', delay: 200);
        $second = $queue->send($this->queueName('jobs'), 'second', delay: 100);
        $this->now += 200;
        $session = $this->owner();
        $this->assertSame($first, $queue->receive($this->queueName('jobs'), $session)->id);
        $this->assertSame($second, $queue->receive($this->queueName('jobs'), $session)->id);
    }

    public function testStaleForeignDisconnectedAndPreviousEpochReceipts(): void
    {
        $queue = $this->open(50);
        $queue->send($this->queueName('jobs'), 'message');
        $owner = $this->owner();
        $foreign = $this->owner();
        $first = $queue->receive($this->queueName('jobs'), $owner);
        $this->invalid(static fn () => $queue->acknowledge($first->receipt, $foreign));
        $this->invalid(static fn () => $queue->reject($first->receipt, $foreign));
        $this->now += 49;
        $this->assertNull($queue->receive($this->queueName('jobs'), $foreign));
        ++$this->now;
        $this->invalid(static fn () => $queue->acknowledge($first->receipt, $owner));
        $next = $queue->receive($this->queueName('jobs'), $foreign);
        $this->assertSame($first->id, $next->id);
        $this->assertNotSame($first->receipt, $next->receipt);
        $this->invalid(static fn () => $queue->reject($first->receipt, $owner));
        $disconnected = $this->lifetime();
        $disconnected->cancel();
        $this->contextClosed(static fn () => $queue->acknowledge($next->receipt, $foreign, $disconnected->getCancellation()));
        $this->assertNull($queue->receive($this->queueName('jobs'), $owner));
        $this->latestStorage()->close();
        $queue = $this->open(50);
        $newOwner = $this->owner();
        $this->invalid(static fn () => $queue->reject($next->receipt, $newOwner));
        $this->assertNull($queue->receive($this->queueName('jobs'), $newOwner));
        $this->now += 50;
        $last = $queue->receive($this->queueName('jobs'), $newOwner);
        $queue->acknowledge($last->receipt, $newOwner);
        $this->invalid(static fn () => $queue->acknowledge($last->receipt, $newOwner));
        $newId = $queue->send($this->queueName('jobs'), 'new identity');
        $this->assertGreaterThan($first->id, $newId);
        $this->invalid(static fn () => $queue->reject($last->receipt, $newOwner));
        $this->assertSame($newId, $queue->receive($this->queueName('jobs'), $newOwner)->id);
    }

    public static function malformedReceipts(): iterable
    {
        $token = str_repeat('a', 64);
        yield 'empty' => [''];
        yield 'missing delimiter' => ['1'.$token];
        yield 'zero ID' => ['0:'.$token];
        yield 'negative ID' => ['-1:'.$token];
        yield 'leading zero' => ['01:'.$token];
        yield 'overflow ID' => [\PHP_INT_MAX.'0:'.$token];
        yield 'short token' => ['1:'.substr($token, 1)];
        yield 'long token' => ['1:'.$token.'a'];
        yield 'uppercase token' => ['1:'.strtoupper($token)];
        yield 'nonhex token' => ['1:'.str_repeat('g', 64)];
        yield 'trailing newline' => ['1:'.$token."\n"];
    }

    #[DataProvider('malformedReceipts')]
    public function testMalformedReceiptsAreNotReservationMismatches(string $receipt): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $this->receiptFailure(static fn () => $queue->acknowledge($receipt, $owner), new MalformedReceiptException());
        $this->receiptFailure(static fn () => $queue->reject($receipt, $owner), new MalformedReceiptException());
        $id = $queue->send($this->queueName(), 'still usable');
        $this->assertSame($id, $queue->receive($this->queueName(), $owner)->id);
    }

    #[DataProvider('settlements')]
    public function testMissingMessageHasNoActiveReservation(string $operation): void
    {
        $queue = $this->open();
        $receipt = '1:'.str_repeat('a', 64);
        $this->receiptFailure(fn () => $this->mutate($queue, $this->owner(), $receipt, $operation), new NoActiveReservationException());
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    #[DataProvider('settlements')]
    public function testUnreservedMessageHasNoActiveReservation(string $operation): void
    {
        $queue = $this->open();
        $id = $queue->send($this->queueName(), 'unreserved');
        $this->receiptFailure(fn () => $this->mutate($queue, $this->owner(), $id.':'.str_repeat('a', 64), $operation), new NoActiveReservationException());
        $this->assertSame($id, $queue->receive($this->queueName(), $this->owner())->id);
    }

    #[DataProvider('settlements')]
    public function testReceiptOwnerMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $this->receiptFailure(fn () => $this->mutate($queue, $this->owner(), $receipt, $operation), new ReceiptOwnerMismatchException());
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->mutate($queue, $owner, $receipt, $operation);
    }

    #[DataProvider('settlements')]
    public function testReceiptEpochMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $replacement = new Queue($this->latestStorage(), clock: fn (): int => $this->now);
        $this->receiptFailure(fn () => $this->mutate($replacement, $owner, $receipt, $operation), new ReceiptEpochMismatchException());
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->mutate($queue, $owner, $receipt, $operation);
    }

    #[DataProvider('settlements')]
    public function testReceiptTokenMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $wrong = $this->differentToken($receipt);
        $this->receiptFailure(fn () => $this->mutate($queue, $owner, $wrong, $operation), new ReceiptTokenMismatchException());
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->mutate($queue, $owner, $receipt, $operation);
    }

    #[DataProvider('settlements')]
    public function testReceiptExpiryIsSpecific(string $operation): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $this->now = $this->scalar('SELECT reserved_until FROM queue_messages');
        $this->receiptFailure(fn () => $this->mutate($queue, $owner, $receipt, $operation), new ExpiredReceiptException());
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->assertNotSame($receipt, $queue->receive($this->queueName(), $owner)->receipt);
    }

    public function testReceiptFailurePrecedence(): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, 'acknowledge');
        $wrong = $this->differentToken($receipt);
        $this->now = $this->scalar('SELECT reserved_until FROM queue_messages');
        $replacement = new Queue($this->latestStorage(), clock: fn (): int => $this->now);
        $this->receiptFailure(fn () => $replacement->acknowledge($wrong, $this->owner()), new ReceiptOwnerMismatchException());
        $this->receiptFailure(static fn () => $replacement->acknowledge($wrong, $owner), new ReceiptEpochMismatchException());
        $this->receiptFailure(static fn () => $queue->acknowledge($wrong, $owner), new ReceiptTokenMismatchException());
        $this->receiptFailure(static fn () => $queue->acknowledge($receipt, $owner), new ExpiredReceiptException());
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public function testIgnoredMatchingDeleteIsAStorageFailure(): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, 'acknowledge');
        $this->exec('CREATE TRIGGER ignore_settlement BEFORE DELETE ON queue_messages BEGIN SELECT RAISE(IGNORE); END');
        try {
            $queue->acknowledge($receipt, $owner);
            $this->fail('A matching reservation was not deleted.');
        } catch (\RuntimeException $error) {
            $this->assertNotInstanceOf(InvalidReceiptException::class, $error);
            $this->assertSame('Settlement DELETE failed despite a matching active reservation.', $error->getMessage());
        }
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->exec('DROP TRIGGER ignore_settlement');
        $queue->acknowledge($receipt, $owner);
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public static function invalidStoredReservations(): iterable
    {
        yield 'owner' => ["UPDATE queue_messages SET owner_id = X'00'", 'Stored reservation owner must be a string.'];
        yield 'epoch' => ["UPDATE queue_messages SET broker_epoch = X'00'", 'Stored reservation epoch must be a string.'];
        yield 'token' => ["UPDATE queue_messages SET reservation_token = X'00'", 'Stored reservation token must be a string.'];
        yield 'expiry' => ['UPDATE queue_messages SET reserved_until = 1.5', 'Stored reservation expiry must be an integer.'];
    }

    #[DataProvider('invalidStoredReservations')]
    public function testInvalidStoredReservationIsNotReportedAsAReceiptMismatch(string $sql, string $message): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, 'acknowledge');
        $this->exec($sql);
        try {
            $queue->acknowledge($receipt, $owner);
            $this->fail('Invalid stored reservation was accepted.');
        } catch (\RuntimeException $error) {
            $this->assertNotInstanceOf(InvalidReceiptException::class, $error);
            $this->assertSame($message, $error->getMessage());
        }
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    #[DataProvider('mutations')]
    public function testCancelledContextIsNotAReceiptFailure(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $count = $this->scalar('SELECT count(*) FROM queue_messages');
        $lifetime = $this->lifetime();
        $lifetime->cancel();
        $this->contextClosed(fn () => $this->mutate($queue, $owner, $receipt, $operation, $lifetime->getCancellation()));
        $this->assertSame($count, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public function testConcurrentReceiversAcrossConnectionsCannotShareDelivery(): void
    {
        $first = $this->open();
        $second = $this->open();
        $first->send($this->queueName('jobs'), 'one');
        $sessions = [$this->owner(), $this->owner()];
        $futures = [];
        for ($i = 0; $i < 8; ++$i) {
            $engine = 0 === $i % 2 ? $first : $second;
            $session = $sessions[$i % 2];
            $name = $this->queueName('jobs');
            $futures[] = async(static fn () => $engine->receive($name, $session));
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
        $session = $this->owner();
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
            $this->assertInstanceOf(DeliveryDTO::class, $result);
        }
        $this->assertGreaterThan(0, $queue->send($this->queueName('recovered'), 'usable after rollback'));
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
        $session = $this->owner();
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
            $other = $this->queueName('other');
            $competitor = async(static function () use ($queue, $started, $other): int {
                $started->complete();

                return $queue->send($other, 'must wait for whole transaction');
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
        $queue->send($this->queueName('jobs'), 'reserved');
        $session = $this->owner();
        $lifetime = $this->lifetime();
        $expiry = $this->now + 50;
        $armed = true;
        $name = $this->queueName('jobs');
        $cancellation = $lifetime->getCancellation();
        $receive = async(static fn () => $queue->receive($name, $session, $cancellation));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($receive->isComplete());
            $this->assertNull($this->scalar('SELECT reservation_token FROM queue_messages'));
            $this->now += 10;
            $lifetime->cancel();
        } finally {
            $release->complete();
            $this->contextClosed(static fn () => $receive->await(new TimeoutCancellation(5)));
        }
        $this->assertSame($expiry, $this->scalar('SELECT reserved_until FROM queue_messages'));
        $this->assertNotNull($this->scalar('SELECT reservation_token FROM queue_messages'));
        $replacement = $this->owner();
        $this->now = $expiry - 1;
        $this->assertNull($queue->receive($this->queueName('jobs'), $replacement));
        $this->now = $expiry;
        $this->assertSame('reserved', $queue->receive($this->queueName('jobs'), $replacement)->body);
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
        $session = $this->owner();
        $lifetime = $this->lifetime();
        $receipt = $this->prepareOperation($queue, $session, $operation);
        $token = $this->scalar('SELECT reservation_token FROM queue_messages');
        $expiry = $this->scalar('SELECT reserved_until FROM queue_messages');
        $armed = true;
        $settle = async(fn () => $this->mutate($queue, $session, $receipt, $operation, $lifetime->getCancellation()));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($settle->isComplete());
            $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
            $lifetime->cancel();
        } finally {
            $release->complete();
            $this->contextClosed(static fn () => $settle->await(new TimeoutCancellation(5)));
        }
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->assertSame($token, $this->scalar('SELECT reservation_token FROM queue_messages'));
        $this->assertSame($expiry, $this->scalar('SELECT reserved_until FROM queue_messages'));
        $replacement = $this->owner();
        $this->assertNull($queue->receive($this->queueName('jobs'), $replacement));
        $this->now = $expiry;
        $this->assertSame('payload', $queue->receive($this->queueName('jobs'), $replacement)->body);
    }

    public function testCloseWaitsForInFlightSendAndPreservesItsCommit(): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $connection = $this->pauseAt('commit', $entered, $release, $armed);
        $queue = $this->open(connection: $connection);
        $armed = true;
        $name = $this->queueName('jobs');
        $send = async(static fn () => $queue->send($name, 'durable before close'));
        $close = null;
        try {
            $entered->getFuture()->await(new TimeoutCancellation(5));
            $this->assertFalse($send->isComplete());
            $started = new DeferredFuture();
            $storage = $this->latestStorage();
            $close = async(static function () use ($storage, $started): void {
                $started->complete();
                $storage->close();
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
            $queue->send($this->queueName('jobs'), 'after close');
            $this->fail('Closed engine accepted a send.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('closed', $error->getMessage());
        }
        $reopened = $this->open();
        $delivery = $reopened->receive($this->queueName('jobs'), $this->owner());
        $this->assertSame($id, $delivery->id);
        $this->assertSame('durable before close', $delivery->body);
    }

    public function testZeroRowClaimRollsBack(): void
    {
        $queue = $this->open();
        $queue->send($this->queueName('jobs'), 'one');
        $this->exec('CREATE TRIGGER lose_claim BEFORE UPDATE ON queue_messages BEGIN SELECT RAISE(IGNORE); END;');
        $session = $this->owner();
        $this->assertNull($queue->receive($this->queueName('jobs'), $session));
        $this->assertNull($this->scalar('SELECT reservation_token FROM queue_messages'));
        $this->exec('DROP TRIGGER lose_claim');
        $this->assertSame('one', $queue->receive($this->queueName('jobs'), $session)->body);
    }

    public function testPersistenceWorkerDeathFailsWithoutLeavingOwnershipStuck(): void
    {
        $queue = $this->open();
        $queue->send($this->queueName('jobs'), 'confirmed');
        $workers = ProcessTree::ownedBy(getmypid())['workers'];
        $this->assertCount(1, $workers);
        $pid = $workers[0];
        $this->assertNotSame(0, posix_geteuid());
        $this->assertSame(posix_geteuid(), fileowner('/proc/'.$pid));
        $this->assertTrue(posix_kill($pid, \SIGKILL));
        try {
            $queue->send($this->queueName('jobs'), 'unconfirmed');
            $this->fail('Dead persistence worker must surface as an error.');
        } catch (SqliteConnectionException) {
            $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        }
        $this->expectException(\RuntimeException::class);
        $queue->send($this->queueName('jobs'), 'closed');
    }

    public function testValidationAndClose(): void
    {
        $queue = $this->open();
        foreach (['', '../db', 'a/b', "nul\0", str_repeat('x', 256)] as $name) {
            try {
                new QueueName($name);
                $this->fail('Invalid queue name accepted.');
            } catch (\InvalidArgumentException) {
            }
        }
        foreach ([-1, \PHP_INT_MAX] as $delay) {
            try {
                $queue->send($this->queueName('jobs'), 'body', delay: $delay);
                $this->fail('Invalid delay accepted.');
            } catch (\InvalidArgumentException) {
            }
        }
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
        $this->latestStorage()->close();
        $this->latestStorage()->close();
        $this->expectException(\RuntimeException::class);
        $queue->send($this->queueName('jobs'), 'closed');
    }

    public function testDatabaseFullLeavesConfirmedDataAndFailedOrUsableOwnership(): void
    {
        $connection = $this->connection();
        $queue = $this->open(connection: $connection);
        $queue->send($this->queueName('jobs'), 'confirmed');
        $pages = $connection->query('PRAGMA page_count');
        $count = $pages->fetchRow()['page_count'];
        $pages->close();
        $connection->query('PRAGMA max_page_count='.$count)->close();
        $failure = null;
        try {
            $queue->send($this->queueName('jobs'), str_repeat('x', 1_000_000));
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
        $name = $this->queueName('other');
        $followup = async(static fn () => $queue->send($name, 'small'));
        try {
            $followup->await(new TimeoutCancellation(5));
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('closed', $error->getMessage());
        }
        $this->latestStorage()->close();
        $reopened = $this->open();
        $this->assertSame('confirmed', $reopened->receive($this->queueName('jobs'), $this->owner())->body);
    }

    public function testInitializationFailureClosesTransferredConnection(): void
    {
        $connection = (new SqliteConnector())->connect(new SqliteConfig($this->database->path()));
        try {
            new SqliteQueueStorage($connection);
            $this->fail('NORMAL durability must not be accepted.');
        } catch (\InvalidArgumentException) {
            $this->assertTrue($connection->isClosed());
        } finally {
            $connection->close();
        }
        $this->assertGreaterThan(0, $this->open()->send($this->queueName(), 'recovered'));
    }

    public function testSelectionUsesQueueIndexWithoutTemporaryOrdering(): void
    {
        $queue = $this->open();
        $queue->send($this->queueName('jobs'), 'ready');
        $queue->send($this->queueName('jobs'), 'future', delay: 100);
        $queue->receive($this->queueName('jobs'), $this->owner());
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

    public function testReceiveSamplesVisibilityAfterTransactionAcquisition(): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $queue = $this->open(50, $this->pauseAt('begin', $entered, $release, $armed));
        $name = $this->queueName();
        $id = $queue->send($name, 'visible after acquisition');
        $owner = $this->owner();
        $armed = true;
        $receiving = async(static fn () => $queue->receive($name, $owner));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(10));
            $this->now += 100;
        } finally {
            $release->complete();
        }
        $delivery = $receiving->await(new TimeoutCancellation(10));
        $this->assertNotNull($delivery);
        $this->assertSame($id, $delivery->id);
        $this->assertSame($this->now + 50, $delivery->reservedUntil);
        $nextOwner = $this->owner();
        $this->assertNull($queue->receive($name, $nextOwner), 'The transaction wait must not consume visibility.');
        $this->now = $delivery->reservedUntil;
        $redelivery = $queue->receive($name, $nextOwner);
        $this->assertNotNull($redelivery);
        $this->assertSame($id, $redelivery->id);
        $this->assertNotSame($delivery->receipt, $redelivery->receipt);
    }

    #[DataProvider('settlements')]
    public function testSettlementCrossingExpiryDuringTransactionAcquisitionKeepsTheMessage(string $operation): void
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $armed = false;
        $queue = $this->open(50, $this->pauseAt('begin', $entered, $release, $armed));
        $name = $this->queueName();
        $owner = $this->owner();
        $queue->send($name, 'must survive expiry');
        $delivery = $queue->receive($name, $owner);
        $this->assertNotNull($delivery);
        $this->now = $delivery->reservedUntil - 1;
        $armed = true;
        $settling = async(fn () => $this->mutate($queue, $owner, $delivery->receipt, $operation));
        try {
            $entered->getFuture()->await(new TimeoutCancellation(10));
            $this->now = $delivery->reservedUntil + 1;
        } finally {
            $release->complete();
        }
        $this->invalid(static fn () => $settling->await(new TimeoutCancellation(10)));
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
        $redelivery = $queue->receive($name, $this->owner());
        $this->assertNotNull($redelivery);
        $this->assertSame($delivery->id, $redelivery->id);
        $this->assertSame('must survive expiry', $redelivery->body);
        $this->assertNotSame($delivery->receipt, $redelivery->receipt);
    }

    public function testStorageCloseRejectsTheOwningFiberAndPreservesUsability(): void
    {
        $queue = $this->open();
        $storage = $this->latestStorage();
        try {
            $storage->exclusive($storage->close(...));
            $this->fail('Closing from the owning fiber must not wait on its own mutex.');
        } catch (\LogicException $error) {
            $this->assertSame('Queue storage cannot close from its owning fiber.', $error->getMessage());
        }
        $queue->send($this->queueName(), 'still usable');
        $this->assertSame('still usable', $queue->receive($this->queueName(), $this->owner())->body);
        $storage->close();
    }

    public function testStorageOwnershipCannotBeBorrowedByAnotherFiber(): void
    {
        $queue = $this->open();
        $storage = $this->latestStorage();
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $owner = async(static fn () => $storage->exclusive(static function () use ($storage, $entered, $release): void {
            $entered->complete();
            $release->getFuture()->await(new TimeoutCancellation(10));
            $storage->insert('jobs', 'owned operation', '', 0);
        }));
        $entered->getFuture()->await(new TimeoutCancellation(10));
        try {
            try {
                $storage->insert('jobs', 'foreign operation', '', 0);
                $this->fail('Another fiber must not borrow storage ownership.');
            } catch (\LogicException $error) {
                $this->assertStringContainsString('require exclusive()', $error->getMessage());
            }
        } finally {
            $release->complete();
            $owner->await(new TimeoutCancellation(10));
        }
        $this->assertSame('owned operation', $queue->receive($this->queueName(), $this->owner())->body);
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public function testRecursiveStorageOwnershipFailsWithoutWaiting(): void
    {
        $this->open();
        $storage = $this->latestStorage();
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('cannot be acquired recursively');
        $storage->exclusive(static fn () => $storage->exclusive(static fn () => null));
    }

    private function owner(): string
    {
        return bin2hex(random_bytes(32));
    }

    private function lifetime(): DeferredCancellation
    {
        return new DeferredCancellation();
    }

    private function queueName(string $queue = 'jobs'): QueueName
    {
        return new QueueName($queue);
    }

    private function open(int $visibility = 5000, ?SqliteConnection $connection = null): Queue
    {
        $storage = null === $connection
            ? SqliteQueueStorage::open($this->database->path())
            : new SqliteQueueStorage($connection);
        $this->storages[] = $storage;

        return new Queue($storage, $visibility, fn (): int => $this->now);
    }

    private function latestStorage(): SqliteQueueStorage
    {
        if ([] === $this->storages) {
            throw new \LogicException('No storage opened by this test.');
        }

        return $this->storages[array_key_last($this->storages)];
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
        } catch (InvalidReceiptException) {
            $this->addToAssertionCount(1);
        }
    }

    private function contextClosed(\Closure $operation): void
    {
        try {
            $operation();
            $this->fail('Cancelled client context was accepted.');
        } catch (ClientContextClosedException $error) {
            $this->assertInstanceOf(\Amp\CancelledException::class, $error->getPrevious());
        }
    }

    private function differentToken(string $receipt): string
    {
        [$id, $token] = explode(':', $receipt);

        return $id.':'.('0' === $token[0] ? '1' : '0').substr($token, 1);
    }

    private function receiptFailure(\Closure $operation, InvalidReceiptException $expected): void
    {
        try {
            $operation();
            $this->fail('Invalid receipt was accepted.');
        } catch (InvalidReceiptException $error) {
            $this->assertSame($expected::class, $error::class);
            $this->assertSame($expected->getMessage(), $error->getMessage());
        }
    }

    private function prepareOperation(Queue $queue, string $ownerId, string $operation): ?string
    {
        if ('send' === $operation) {
            return null;
        }
        $queue->send($this->queueName(), 'payload');

        return 'receive' === $operation ? null : $queue->receive($this->queueName(), $ownerId)->receipt;
    }

    private function mutate(Queue $queue, string $ownerId, ?string $receipt, string $operation, ?\Amp\Cancellation $cancellation = null): mixed
    {
        return match ($operation) {
            'send' => $queue->send($this->queueName(), 'payload', cancellation: $cancellation),
            'receive' => $queue->receive($this->queueName(), $ownerId, $cancellation),
            'acknowledge' => $queue->acknowledge($receipt, $ownerId, $cancellation),
            'reject' => $queue->reject($receipt, $ownerId, $cancellation),
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
                if ('begin' === $boundary) {
                    $pause();
                }

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
