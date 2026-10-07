# SQLite queue engine

`Ineersa\SqliteQueue\Queue` owns message policy. `Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage` owns one native PDO SQLite connection, schema setup, fixed prepared statements, and transaction boundaries. SQLite is authoritative. The engine stores no message cache, tracks no live client sessions, performs no application deserialization, and has no Symfony dependency.

In production, both classes run inside the broker's persistent Amp worker. The parent proxy exchanges one complete operation at a time. This document describes the local policy and storage APIs that the worker invokes. It does not implement the broker socket loop, WAIT coordination, or Messenger transport. Broker waiting uses `earliestEligibility()` as a scheduling hint only.

## Opening and ownership

```php
use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\ValueObject\QueueName;

$storage = SqliteQueueStorage::open('/absolute/path/to/queue.sqlite');
$queue = new Queue($storage);
$ownerId = bin2hex(random_bytes(32));
$lifetime = new DeferredCancellation();
try {
    $id = $queue->send(new QueueName('emails'), $bodyBytes, $headerBytes, delay: 250, cancellation: $lifetime->getCancellation());
    $delivery = $queue->receive(new QueueName('emails'), $ownerId, $lifetime->getCancellation());
    if ($delivery !== null) {
        // Application processing happens outside every storage transaction.
        $queue->acknowledge($delivery->receipt, $ownerId, $lifetime->getCancellation());
    }
} finally {
    $lifetime->cancel();
    $storage->close();
}
```

The database directory must exist. Queue names are independent of the database path. Construct `QueueName` from untrusted strings; invalid names fail at construction. Names contain 1 to 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit. Paths, empty names, and NUL bytes are rejected. Bodies and headers can contain arbitrary bytes, including empty strings.

`SqliteQueueStorage::open(string $path, SqliteSynchronousMode $synchronous = SqliteSynchronousMode::Normal)` opens one PDO connection with exception error mode, associative fetches, and immediate transactions. Only NORMAL and FULL are supported. The NORMAL default is the product durability policy. Use `SqliteQueueStorage::open($path, SqliteSynchronousMode::Full)` for FULL. Initialization verifies WAL and the selected synchronous mode, enables foreign keys and extended result codes, sets `busy_timeout` to 5,000 milliseconds and `wal_autocheckpoint` to 1,000 pages, and creates the table and indexes if absent. In-memory databases are not supported. The database is dedicated queue storage. Do not modify its schema or open it independently while the broker owns it.

`new SqliteQueueStorage(Pdo\Sqlite $connection, SqliteSynchronousMode $synchronous)` takes exclusive ownership of an existing PDO connection. Initialization failure closes it. Do not use the transferred connection concurrently or change its schema or settings after construction.

`new Queue(SqliteQueueStorage $storage, int $visibilityTimeout = 60000, ?Closure $clock = null)` is message policy only. It does not open, configure, or close storage. Invalid visibility fails without closing the caller-owned storage dependency.

The optional clock is a `Closure(): int` returning nonnegative Unix wall-clock milliseconds. The default samples `floor(microtime(true) * 1000)`. Send, claim, and settlement sample deadlines after the write transaction begins. Queue supplies the clock and deadline calculation; storage invokes them inside the transaction. No monotonic deadline is persisted. Delays and visibility timeouts must fit the signed integer timestamp range. Visibility must be positive; delay may be zero but not negative.

Always close storage in `finally`. Close is idempotent, and closed storage rejects subsequent operations. Engines cannot be cloned. Production callers use the broker worker, not a second parent-process PDO connection.

## API

| Method | Result |
| --- | --- |
| `send(QueueName $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int` | Committed insertion ID. Persists the availability deadline sampled after transaction acquisition. |
| `receive(QueueName $queue, string $ownerId, ?Cancellation $cancellation = null): ?DeliveryDTO` | One committed reservation, or `null` when nothing can be claimed. Never waits for future work. |
| `earliestEligibility(QueueName $queue, ?Cancellation $cancellation = null): ?int` | Earliest effective eligibility deadline for one queue, or `null` when the queue has no rows. Scheduling hint only. |
| `acknowledge(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void` | Deletes the current delivery after validating its receipt and ownership. Returns only after commit. |
| `reject(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void` | The same fenced terminal deletion. Does not reschedule or retry. |

`DeliveryDTO` has readonly fields `id`, `queue`, `body`, `headers`, `receipt`, `availableAt`, and `reservedUntil`. The two timestamps are Unix wall-clock milliseconds. Treat the receipt as opaque, even though its current representation contains the insertion ID and a random reservation token.

Queue owns receipt generation and interpretation. It creates the storage epoch and reservation tokens, formats receipts, and interprets settlement outcomes. Storage receives those policy inputs and returns a `SettlementResultEnum` identifying success or the failed reservation predicate. Queue translates failures to specific exceptions.

Callers own client lifetime. Create a new owner identity for each connection and pass that identity into receive and settlement. Pass an Amp cancellation for the connection lifetime. Cancel it on disconnect. Queue maps a requested cancellation to `ClientContextClosedException`, with the original Amp cancellation as its cause. This is not a receipt rejection.

Cancellation can prevent a local call before storage begins. Once a storage operation starts, it finishes normally. In the broker, the equivalent boundary is admission before the worker channel send. A completed claim can retain its reservation without disclosing its receipt to a disconnected caller. A disconnect during an in-flight mutation cannot prove that the mutation did not commit.

## Schema and ordering

There is one application table, `queue_messages`, for every named queue. SQLite also maintains its internal `sqlite_sequence` table.

| Column | Meaning |
| --- | --- |
| `id INTEGER PRIMARY KEY AUTOINCREMENT` | Insertion sequence and message identity. Normal deletion cannot recycle it. |
| `queue TEXT NOT NULL` | Logical queue name from a validated `QueueName`. |
| `body BLOB NOT NULL`, `headers BLOB NOT NULL` | Opaque bytes, bound as `PDO::PARAM_LOB`. Type checks reject non-BLOB values. |
| `available_at INTEGER NOT NULL` | Original send deadline. Never reset by claim or restart. |
| `reserved_until INTEGER` | Current visibility expiry, initially null. |
| `reservation_token TEXT` | Random 256-bit token for this delivery, initially null. |
| `owner_id TEXT` | Owning client identity, initially null. |
| `broker_epoch TEXT` | Random 256-bit identity for the Queue policy instance, initially null. |

A constraint requires all four reservation fields to be null or all four to be populated. Deadlines cannot be negative.

`queue_messages_order(queue, id)` narrows selection to one queue in insertion order. Eligibility filters are applied during that index scan. Future or reserved rows can increase scan work, but do not block a later eligible row or another queue. There is no temporary ordering step in the verified query plan. This is a correctness-first index choice, not a claim about delayed-backlog performance.

`queue_messages_ready(queue, max(available_at, coalesce(reserved_until, available_at)))` supports the earliest-eligibility query used by WAIT scheduling. Effective eligibility is that same expression: unreserved rows use availability, and reserved rows use visibility expiry. The query returns the minimum ready deadline for one queue, or no row when the queue is empty. It does not claim a message and does not hold a transaction while waiting.

The claim selects the lowest eligible ID:

```sql
SELECT id FROM queue_messages
WHERE queue = ? AND available_at <= ?
  AND (reserved_until IS NULL OR reserved_until <= ?)
ORDER BY id LIMIT 1
```

The worker executes one operation at a time. Each mutation uses an immediate PDO transaction. Receive selects an ID, conditionally updates its reservation using the same eligibility predicate, reads the payload inside that transaction, and commits before returning it. A zero-row conditional update or empty selection rolls back and returns `null`. There is no DML `RETURNING`.

Concurrent writers acquire SQLite's write lock before selection. Committed insertions therefore define the ID order. This promises eligible-message ordering, not consumer completion order. The broker owns one database connection through its worker. SQLite serializes writers from independent connections to the same file.

## Visibility and receipts

Availability is sampled after send acquires its write transaction, before insertion. Receive samples eligibility and visibility expiry inside its write transaction. Deadlines have millisecond resolution and follow wall clock, so clock adjustments can advance or postpone eligibility. A positive subsecond delay is preserved, not rounded to seconds or reset after reopen.

A claim records its owner identity, Queue epoch, new random token, and expiry. Acknowledge and reject match all of those fields plus message ID. They also require `reserved_until > now`; a receipt expires at its deadline even if no consumer has reclaimed it yet.

Receipt rejections have distinct concrete exceptions under `Exception`, all extending the abstract `InvalidReceiptException` base. `MalformedReceiptException` identifies invalid syntax or an ID outside the supported positive integer range. A zero-row DELETE is diagnosed inside the same immediate transaction, using the same sampled clock. Failure precedence is `NoActiveReservationException`, `ReceiptOwnerMismatchException`, `ReceiptEpochMismatchException`, `ReceiptTokenMismatchException`, then `ExpiredReceiptException`. Multiple mismatches report the first in that order. These failures do not delete another reservation.

No active reservation means the message is absent or unreserved. Storage retains no settlement history, so it cannot distinguish an already-settled message from an ID that never existed. If every predicate matches but DELETE removes no row, storage reports an invariant failure instead of a receipt rejection. Wire codes and client exception mappings are listed in the [protocol reference](broker-protocol.md#receipt-errors).

Cancelling a connection lifetime before dispatch invalidates the call without starting storage work. A committed reservation expiry remains unchanged. Reopening creates a new Queue epoch and does not clear reservations. Old receipts cannot settle them. At the original expiry, a new receive may claim the row with a new token. There is no receive-count cap or lease extension. A slow consumer can still repeat an external effect after redelivery; receipt fencing does not provide exactly-once application execution.

## Failures and durability

Send, successful claim, acknowledge, and reject return only after commit. SQL errors propagate after rollback. A failed rollback closes storage and preserves the original operation error as the exception's previous cause, with rollback details in the outer message. Connection failures close storage. The engine never retries an uncertain mutation.

SQLite can commit before a process or connection fails to deliver confirmation. An exception therefore does not prove that the operation had no effect. The client does not replay uncertain operations.

NORMAL can lose recent committed sends or ACKs after OS crashes or power loss. FULL provides stronger commit durability if storage honors synchronization. See [storage and durability](contracts.md#storage-and-durability). Close/reopen and process-crash checks do not establish power-loss survival.
