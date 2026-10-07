<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Amp\DeferredCancellation;
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
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Driver\DriverTestCase;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Pdo\Sqlite;
use PHPUnit\Framework\Attributes\DataProvider;

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
        $first = $queue->send($this->queueName('jobs'), 'first', delay: 10);
        $second = $queue->send($this->queueName('jobs'), 'second');
        $this->now += 10;
        $session = $this->owner();
        $this->assertSame($first, $queue->receive($this->queueName('jobs'), $session)->id);
        $this->assertSame($second, $queue->receive($this->queueName('jobs'), $session)->id);
    }

    public function testStaleForeignAndPreviousEpochReceipts(): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $foreign = $this->owner();
        $queue->send($this->queueName(), 'payload');
        $delivery = $queue->receive($this->queueName(), $owner);
        $this->assertNotNull($delivery);
        $this->invalid(static fn () => $queue->acknowledge($delivery->receipt, $foreign));
        $this->invalid(fn () => $queue->acknowledge($this->differentToken($delivery->receipt), $owner));
        $this->now = $delivery->reservedUntil;
        $this->invalid(static fn () => $queue->acknowledge($delivery->receipt, $owner));
        $replacement = $queue->receive($this->queueName(), $foreign);
        $this->assertNotNull($replacement);
        $this->assertSame($delivery->id, $replacement->id);
        $this->assertNotSame($delivery->receipt, $replacement->receipt);
        $this->latestStorage()->close();
        $queue = $this->open(50);
        $this->invalid(static fn () => $queue->acknowledge($replacement->receipt, $foreign));
    }

    public static function malformedReceipts(): iterable
    {
        yield 'empty' => [''];
        yield 'missing-token' => ['1'];
        yield 'uppercase' => ['1:'.str_repeat('A', 64)];
        yield 'short-token' => ['1:'.str_repeat('a', 63)];
        yield 'zero-id' => ['0:'.str_repeat('a', 64)];
    }

    #[DataProvider('malformedReceipts')]
    public function testMalformedReceiptsAreNotReservationMismatches(string $receipt): void
    {
        $queue = $this->open();
        $this->receiptFailure(
            static fn () => $queue->acknowledge($receipt, 'owner'),
            new MalformedReceiptException(),
        );
    }

    #[DataProvider('settlements')]
    public function testMissingMessageHasNoActiveReservation(string $operation): void
    {
        $queue = $this->open();
        $this->receiptFailure(
            fn () => $this->mutate($queue, $this->owner(), '1:'.str_repeat('a', 64), $operation),
            new NoActiveReservationException(),
        );
    }

    #[DataProvider('settlements')]
    public function testUnreservedMessageHasNoActiveReservation(string $operation): void
    {
        $queue = $this->open();
        $id = $queue->send($this->queueName(), 'payload');
        $this->receiptFailure(
            fn () => $this->mutate($queue, $this->owner(), $id.':'.str_repeat('a', 64), $operation),
            new NoActiveReservationException(),
        );
    }

    #[DataProvider('settlements')]
    public function testReceiptOwnerMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $this->receiptFailure(
            fn () => $this->mutate($queue, $this->owner(), $receipt, $operation),
            new ReceiptOwnerMismatchException(),
        );
    }

    #[DataProvider('settlements')]
    public function testReceiptEpochMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $replacement = new Queue($this->latestStorage(), clock: fn (): int => $this->now);
        $this->receiptFailure(
            fn () => $this->mutate($replacement, $owner, $receipt, $operation),
            new ReceiptEpochMismatchException(),
        );
    }

    #[DataProvider('settlements')]
    public function testReceiptTokenMismatchIsSpecific(string $operation): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $this->receiptFailure(
            fn () => $this->mutate($queue, $owner, $this->differentToken($receipt), $operation),
            new ReceiptTokenMismatchException(),
        );
    }

    #[DataProvider('settlements')]
    public function testReceiptExpiryIsSpecific(string $operation): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, $operation);
        $this->now += 50;
        $this->receiptFailure(
            fn () => $this->mutate($queue, $owner, $receipt, $operation),
            new ExpiredReceiptException(),
        );
    }

    public function testReceiptFailurePrecedence(): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, 'acknowledge');
        $wrong = $this->differentToken($receipt);
        $replacement = new Queue($this->latestStorage(), clock: fn (): int => $this->now);
        $this->now += 50;
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

    public function testCancelledContextDoesNotRevokeAnAlreadyStartedSettlement(): void
    {
        $queue = $this->open();
        $owner = $this->owner();
        $receipt = $this->prepareOperation($queue, $owner, 'acknowledge');
        $lifetime = $this->lifetime();
        // Admission cancellation is checked before storage begins. Once settle() is entered,
        // a later cancellation must not roll the delete back.
        $queue->acknowledge($receipt, $owner, $lifetime->getCancellation());
        $lifetime->cancel();
        $this->assertSame(0, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public function testConcurrentReceiversAcrossConnectionsCannotShareDelivery(): void
    {
        $first = $this->open();
        $second = $this->open();
        $first->send($this->queueName('jobs'), 'one');
        $sessions = [$this->owner(), $this->owner()];
        $deliveries = [];
        for ($i = 0; $i < 8; ++$i) {
            $engine = 0 === $i % 2 ? $first : $second;
            $session = $sessions[$i % 2];
            $delivery = $engine->receive($this->queueName('jobs'), $session);
            if (null !== $delivery) {
                $deliveries[] = $delivery;
            }
        }
        $this->assertCount(1, $deliveries);
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

    public function testValidationAndClose(): void
    {
        $storage = $this->openStorage();
        $this->storages[] = $storage;
        $this->expectException(\InvalidArgumentException::class);
        new Queue($storage, 0);
    }

    public function testClosedStorageRejectsOperations(): void
    {
        $queue = $this->open();
        $queue->send($this->queueName('jobs'), 'payload');
        $this->latestStorage()->close();
        $this->latestStorage()->close();
        $this->expectException(\RuntimeException::class);
        $queue->send($this->queueName('jobs'), 'closed');
    }

    public function testInitializationFailureReleasesConnection(): void
    {
        try {
            SqliteQueueStorage::open(':memory:');
            $this->fail('Memory databases must be rejected before acquisition.');
        } catch (\InvalidArgumentException $error) {
            $this->assertSame('Queue storage requires a database file path.', $error->getMessage());
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

    public function testEarliestEligibilityUsesReadyIndexAndAccountsForVisibility(): void
    {
        $queue = $this->open(50);
        $jobs = $this->queueName('jobs');
        $other = $this->queueName('other');
        $queue->send($jobs, 'future', delay: 100);
        $queue->send($other, 'ready-elsewhere');
        $this->assertSame($this->now + 100, $queue->earliestEligibility($jobs));
        $this->assertSame($this->now, $queue->earliestEligibility($other));
        $this->assertNull($queue->earliestEligibility($this->queueName('empty')));

        $queue->send($jobs, 'claim-me');
        $delivery = $queue->receive($jobs, $this->owner());
        $this->assertNotNull($delivery);
        $this->assertSame($delivery->reservedUntil, $queue->earliestEligibility($jobs));

        $database = new \SQLite3($this->database->path());
        try {
            $result = $database->query(
                "EXPLAIN QUERY PLAN SELECT max(available_at, coalesce(reserved_until, available_at)) AS ready_at
                 FROM queue_messages WHERE queue = 'jobs' ORDER BY ready_at LIMIT 1",
            );
            $details = [];
            while (false !== ($row = $result->fetchArray(\SQLITE3_ASSOC))) {
                $details[] = $row['detail'];
            }
            $result->finalize();
            $plan = implode('; ', $details);
            $this->assertStringContainsString('queue_messages_ready', $plan);
            $this->assertStringNotContainsString('TEMP B-TREE', $plan);
            $this->assertStringNotContainsString('SCAN queue_messages', $plan);
        } finally {
            $database->close();
        }
    }

    public function testSendSamplesAvailabilityAfterTransactionAcquisition(): void
    {
        $storage = $this->openStorage();
        $this->storages[] = $storage;
        $observed = null;
        $id = $storage->insert('jobs', 'delayed', '', function () use (&$observed): int {
            $this->now += 25;
            $observed = $this->now;

            return $this->now + 100;
        });
        $this->assertSame($observed + 100, $this->scalar('SELECT available_at FROM queue_messages WHERE id = '.$id));
    }

    public function testReceiveSamplesVisibilityInsideTransaction(): void
    {
        $storage = $this->openStorage();
        $this->storages[] = $storage;
        $storage->insert('jobs', 'visible', '', fn (): int => $this->now);
        $observed = null;
        $claimed = $storage->claim(
            'jobs',
            $this->owner(),
            str_repeat('e', 64),
            str_repeat('f', 64),
            function () use (&$observed): int {
                $this->now += 40;
                $observed = $this->now;

                return $this->now;
            },
            static fn (int $now): int => $now + 50,
        );
        $this->assertNotNull($claimed);
        $this->assertSame($observed + 50, $claimed['reserved_until']);
        $this->assertSame($observed + 50, $this->scalar('SELECT reserved_until FROM queue_messages'));
    }

    public function testSettlementCrossingExpiryUsesTransactionClock(): void
    {
        $queue = $this->open(50);
        $owner = $this->owner();
        $queue->send($this->queueName(), 'must survive expiry');
        $delivery = $queue->receive($this->queueName(), $owner);
        $this->assertNotNull($delivery);
        $this->now = $delivery->reservedUntil;
        $this->receiptFailure(
            static fn () => $queue->acknowledge($delivery->receipt, $owner),
            new ExpiredReceiptException(),
        );
        $this->assertSame(1, $this->scalar('SELECT count(*) FROM queue_messages'));
    }

    public function testDatabaseFullLeavesConfirmedDataAndAllowsReopen(): void
    {
        $storage = $this->openStorage();
        $queue = new Queue($storage, 5_000, fn (): int => $this->now);
        $this->storages[] = $storage;
        $queue->send($this->queueName('jobs'), 'confirmed');
        $connection = (new \ReflectionProperty($storage, 'connection'))->getValue($storage);
        $this->assertInstanceOf(Sqlite::class, $connection);
        $pages = (int) $connection->query('PRAGMA page_count')->fetchColumn();
        $connection->exec('PRAGMA max_page_count='.$pages);
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
        $storage->close();
        $reopened = $this->open();
        $this->assertSame('confirmed', $reopened->receive($this->queueName('jobs'), $this->owner())->body);
    }

    public static function settlements(): iterable
    {
        yield 'ack' => ['acknowledge'];
        yield 'reject' => ['reject'];
    }

    public static function mutations(): iterable
    {
        yield 'send' => ['send'];
        yield 'claim' => ['receive'];
        yield 'ack' => ['acknowledge'];
        yield 'reject' => ['reject'];
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

    private function open(int $visibility = 5000): Queue
    {
        $storage = $this->openStorage();
        $this->storages[] = $storage;

        return new Queue($storage, $visibility, fn (): int => $this->now);
    }

    private function openStorage(SqliteSynchronousMode $mode = SqliteSynchronousMode::Normal): SqliteQueueStorage
    {
        return SqliteQueueStorage::open($this->database->path(), $mode);
    }

    private function latestStorage(): SqliteQueueStorage
    {
        if ([] === $this->storages) {
            throw new \LogicException('No storage opened by this test.');
        }

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
}
