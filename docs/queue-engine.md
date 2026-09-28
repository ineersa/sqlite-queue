# Async SQLite queue engine

`Ineersa\SqliteQueue\Queue` owns one `fabpot/amphp-sqlite3` connection and its persistence process. SQLite is authoritative. The engine stores no message cache, performs no application deserialization, and has no Symfony dependency.

This is the storage API for Task 04. It does not implement a broker, socket client, waiting consumers, notifications, or Messenger transport.

## Opening and ownership

```php
use Ineersa\SqliteQueue\Queue;

$queue = Queue::open('/absolute/path/to/queue.sqlite');
try {
    $session = $queue->openSession();
    $id = $queue->send('emails', $bodyBytes, $headerBytes, delay: 250);
    $delivery = $queue->receive('emails', $session);
    if ($delivery !== null) {
        // Application processing happens outside every storage transaction.
        $queue->acknowledge($delivery->receipt, $session);
    }
    $queue->closeSession($session);
} finally {
    $queue->close();
}
```

The database directory must exist. Queue names are independent of the database path. They contain 1 to 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit. Paths, empty names, and NUL bytes are rejected. Bodies and headers can contain arbitrary bytes, including empty strings.

`Queue::open(string $path, int $visibilityTimeout = 5000, ?Closure $clock = null)` starts the connection with explicit WAL, synchronous FULL, and immediate transactions. Initialization verifies the effective WAL/FULL settings and creates the table and index if absent. In-memory databases are not supported. Schema migrations and exclusive broker ownership belong to later tasks; the database is dedicated queue storage, not an application database to modify independently.

`new Queue(SqliteConnection $connection, int $visibilityTimeout = 5000, ?Closure $clock = null)` transfers exclusive ownership of an existing driver connection. It requires explicitly configured WAL/FULL, verifies effective settings, and sets immediate transaction mode. Do not use the transferred connection concurrently or change its schema or settings. Initialization failure closes it. The constructor accepts the existing driver interface for embedding and controlled storage-failure tests, not a multi-driver abstraction.

The optional clock is a `Closure(): int` returning nonnegative Unix wall-clock milliseconds. The default samples `floor(microtime(true) * 1000)`. No monotonic deadline is persisted. Delays and visibility timeouts must fit the signed integer timestamp range. Visibility must be positive; delay may be zero but not negative.

Always close the engine in `finally`. `close()` waits for operation ownership, invalidates all sessions, and closes the persistence connection. It is idempotent. Closed engines reject subsequent operations. Engines cannot be cloned.

## API

| Method | Result |
| --- | --- |
| `send(string $queue, string $body, string $headers = '', int $delay = 0): int` | Committed insertion ID. Persists the original availability deadline. |
| `openSession(): string` | New opaque context for one client connection. |
| `receive(string $queue, string $session): ?Delivery` | One committed reservation, or `null` when nothing can be claimed. Never waits for future work. |
| `acknowledge(string $receipt, string $session): void` | Deletes the current delivery after validating its receipt and ownership. Returns only after commit. |
| `reject(string $receipt, string $session): void` | The same fenced terminal deletion. Does not reschedule or retry. |
| `closeSession(string $session): void` | Invalidates the context without releasing or extending persisted reservations. |
| `close(): void` | Closes storage after preceding operations release ownership. |

`Delivery` has readonly fields `id`, `queue`, `body`, `headers`, `receipt`, `availableAt`, and `reservedUntil`. The two timestamps are Unix wall-clock milliseconds. Treat the receipt as opaque, even though its current representation contains the insertion ID and a random reservation token.

The broker must create a new session for each connection and pass the context it owns, not a context supplied by a client. It must call `closeSession()` on disconnect. Unknown and disconnected contexts fail explicitly with `InvalidReceipt`. A disconnect during an in-flight mutation cannot prove that the mutation did not commit.

## Schema and ordering

There is one application table, `queue_messages`, for every named queue. SQLite also maintains its internal `sqlite_sequence` table.

| Column | Meaning |
| --- | --- |
| `id INTEGER PRIMARY KEY AUTOINCREMENT` | Insertion sequence and message identity. Normal deletion cannot recycle it. |
| `queue TEXT NOT NULL` | Validated logical queue name. |
| `body BLOB NOT NULL`, `headers BLOB NOT NULL` | Opaque bytes, bound and read through the driver's `SqliteBlob`. Type checks reject non-BLOB values. |
| `available_at INTEGER NOT NULL` | Original send deadline. Never reset by claim or restart. |
| `reserved_until INTEGER` | Current visibility expiry, initially null. |
| `reservation_token TEXT` | Random 256-bit token for this delivery, initially null. |
| `owner_id TEXT` | Owning client context, initially null. |
| `broker_epoch TEXT` | Random 256-bit identity for the engine instance, initially null. |

A constraint requires all four reservation fields to be null or all four to be populated. Deadlines cannot be negative.

`queue_messages_order(queue, id)` narrows selection to one queue in insertion order. Eligibility filters are applied during that index scan. Future or reserved rows can increase scan work, but do not block a later eligible row or another queue. There is no temporary ordering step in the verified query plan. This is a correctness-first index choice, not a claim about delayed-backlog performance. Deadline notification queries and their indexes remain Task 05 work.

The claim selects the lowest eligible ID:

```sql
SELECT id FROM queue_messages
WHERE queue = ? AND available_at <= ?
  AND (reserved_until IS NULL OR reserved_until <= ?)
ORDER BY id LIMIT 1
```

A local Amp mutex serializes whole operations across fiber suspensions, including close. Each mutation uses the driver's immediate transaction. Receive selects an ID, conditionally updates its reservation using the same eligibility predicate, reads the payload inside that transaction, and commits before returning it. A zero-row conditional update or empty selection rolls back and returns `null`. There is no DML `RETURNING` and no connection-level query inside an active transaction.

Concurrent writers acquire SQLite's write lock before selection. Committed insertions therefore define the ID order. This promises eligible-message ordering, not consumer completion order. The broker will own one database connection; tests also verify fencing across two independent connections to the same file.

## Visibility and receipts

Availability is sampled after send acquires operation ownership, before insertion. Receive samples eligibility and visibility expiry inside its write transaction. Deadlines have millisecond resolution and follow wall clock, so clock adjustments can advance or postpone eligibility. A positive subsecond delay is preserved, not rounded to seconds or reset after reopen.

A claim records its session, engine epoch, new random token, and expiry. Acknowledge and reject match all of those fields plus message ID. They also require `reserved_until > now`; a receipt is stale at expiry even if no consumer has reclaimed it yet. Wrong, malformed, expired, already-settled, disconnected, and previous-epoch receipts raise `InvalidReceipt` without deleting another reservation.

Disconnect invalidates the session immediately but leaves the persisted expiry unchanged. Reopening creates a new epoch and does not clear reservations. Old receipts cannot settle them. At the original expiry, a new receive may claim the row with a new token. There is no receive-count cap or lease extension. A slow consumer can still repeat an external effect after redelivery; receipt fencing does not provide exactly-once application execution.

## Failures and durability

Send, successful claim, acknowledge, and reject return only after commit. SQL errors propagate after rollback. A failed rollback closes the engine and preserves the original operation error as the exception's previous cause, with rollback details in the outer message. Connection failures close the engine. Operation locks are released in `finally`, including these failure paths. The engine never retries an uncertain mutation.

SQLite can commit before a process or connection fails to deliver confirmation. An exception therefore does not establish that the operation had no effect. Task 04 must preserve that ambiguity at the client boundary.

WAL/FULL durability remains subject to SQLite, filesystem, and host guarantees. The tests prove commit visibility, close/reopen persistence, rollback, and process failure behavior; they do not simulate power-loss survival.

## Validation

Follow [AGENTS.md](../AGENTS.md) for QA and reports. The focused command is `vendor/bin/castor test --filter=QueueTest`.

The file-backed engine tests cover binary and empty data, named queues, insertion ordering, 250 ms and 1 ms delays, restart before and after deadlines, visibility boundaries, receipt ownership, concurrent receivers, and persistence-worker teardown. Test-only forwarding decorators pause real transactions before commit. They prove that another operation cannot enter and that neither the API result nor the mutation is visible before commit. Deferred foreign-key violations at commit exercise rollback for all four mutations. A `RAISE(IGNORE)` trigger exercises the zero-row claim branch. A per-connection page limit produces a real `SQLITE_FULL` failure without filling the host disk. Worker-death tests signal only the fixture's owned process.

Clock values and deferred futures control synchronization. Failure timeouts bound waits; arbitrary sleeps do not establish eligibility or transaction ordering.
