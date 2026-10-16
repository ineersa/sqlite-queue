# In-process queue engine

This is the advanced PHP API used by the broker. Most applications should use the [client](broker.md) or [Messenger transport](messenger.md) instead.

`Queue` owns message policy, receipt generation, and the Queue epoch. `SqliteQueueStorage` owns its driver connection, schema, SQL, and transaction boundary. Neither class owns a registry of live client sessions. See [storage architecture](sqlite-worker.md#process-model) for the worker execution model and [delivery contracts](contracts.md) for guarantees.

## Opening and ownership

The database directory must already exist. Open file-backed storage, construct the policy object, and close storage in `finally`:

```php
use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Queue;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\ValueObject\QueueName;

$bodyBytes = 'example message';
$headerBytes = '';
$ownerId = bin2hex(random_bytes(32));
$lifetime = new DeferredCancellation();
$storage = SqliteQueueStorage::open('/absolute/path/queue.sqlite');

try {
	$queue = new Queue($storage);
	$name = new QueueName('emails');
	$id = $queue->send($name, $bodyBytes, $headerBytes, cancellation: $lifetime->getCancellation());
	$delivery = $queue->receive($name, $ownerId, $lifetime->getCancellation());
	if ($delivery !== null) {
		// Process $delivery->body outside the storage transaction.
		$queue->acknowledge($delivery->receipt, $ownerId, $lifetime->getCancellation());
	}
} finally {
	$lifetime->cancel();
	$storage->close();
}
```

`open()` accepts named `synchronous` and `cancellation` arguments. The default is `SqliteSynchronousMode::Normal`; pass `Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode::Full` for FULL. In-memory databases and an empty path are rejected. Initialization verifies SQLite 3.31.0 or newer, WAL, and the selected synchronous mode, then creates the table and indexes if absent. The standard connection uses a 5,000-millisecond busy timeout and 1,000-page WAL autocheckpoint threshold.

`SqliteQueueStorage::fromConnection(SqliteWorkerOperationConnection $connection, SqliteSynchronousMode $synchronous, ?Cancellation $cancellation = null): self` transfers exclusive ownership of an acquired driver connection. The connection must already have the required configuration and queue worker-operation registration. Initialization verifies configuration and creates the schema; failure closes the transferred connection. Do not reuse that connection, change its configuration, or modify its schema concurrently.

`new Queue(SqliteQueueStorage $storage, int $visibilityTimeout = 60000)` requires positive visibility milliseconds. Queue does not acquire or close storage. Invalid visibility leaves caller-owned storage open. A Queue cannot be cloned; each new instance creates a fresh epoch.

**Direct engine use does not acquire the broker's lifetime locks, enforce its private-path checks, or install its startup and operation deadlines.** The caller owns those policies. An optional storage-initialization cancellation covers acquisition, readback, and schema setup as applicable; no overall initialization timeout is added by `open()` itself. Do not open the broker's database independently while the broker owns it.

A caller owns each client context: use a nonempty owner identity for receive, keepalive, ACK, and reject, and cancel its lifetime on disconnect. Reusing an owner string does not transfer a broker receipt to a new connection. A requested context cancellation raises `ClientContextClosedException`, with the Amp cancellation as its cause. The [uncertain-operation rules](contracts.md#uncertain-operations-and-cancellation) still apply after dispatch.

## API

The signatures below are methods on `Ineersa\SqliteQueue\Queue`. Types are `Amp\Cancellation`, `Ineersa\SqliteQueue\ValueObject\QueueName`, and `Ineersa\SqliteQueue\DTO\DeliveryDTO`.

| Method | Result |
| --- | --- |
| `send(QueueName $queue, string $body, string $headers = '', int $delay = 0, ?Cancellation $cancellation = null): int` | Committed positive insertion ID. Delay is nonnegative milliseconds, sampled after transaction acquisition. |
| `receive(QueueName $queue, string $ownerId, ?Cancellation $cancellation = null): ?DeliveryDTO` | A committed reservation, or `null` if no message can be claimed. Does not wait for future work. |
| `acknowledge(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void` | Fenced terminal deletion. |
| `reject(string $receipt, string $ownerId, ?Cancellation $cancellation = null): void` | The same terminal deletion, without retry or rescheduling. |
| `keepalive(string $receipt, string $ownerId, int $minimumMilliseconds = 0, ?Cancellation $cancellation = null): int` | Committed expiry in Unix milliseconds. Zero uses configured visibility; a positive minimum cannot shorten the current lease. |
| `earliestEligibility(QueueName $queue, ?Cancellation $cancellation = null): ?int` | Earliest effective eligibility timestamp, or `null` for an empty queue. A scheduling hint, not a claim or wait. |

`DeliveryDTO` is readonly. Its fields are integer `id`, string `queue`, byte strings `body` and `headers`, string `receipt`, and integer Unix-millisecond timestamps `availableAt` and `reservedUntil`.

Construct `QueueName` at the boundary for untrusted strings. Names contain 1 through 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit. Bodies and headers can be arbitrary bytes, including empty strings. The combined default maximum is 1,040,000 bytes; the active broker payload budget may raise or lower that bound. Validation rejects oversized payloads rather than truncating them. Delays and calculated deadlines must fit the PHP integer range.

Queue interprets receipt failures from storage and raises the [six concrete receipt exceptions](broker-protocol.md#receipt-errors). It retains no delivery history after deletion. There are no queue list, count, or purge methods.

### Closing storage

`SqliteQueueStorage::close(?Cancellation $cancellation = null): void` starts one close and rejects subsequent operations. Every caller awaits the same close completion and shares its result. The driver's own five-second close ceiling applies; supplied cancellation can tighten it. A later caller's cancellation also reaches the existing close, but cannot restart it, extend its budget, or abandon the cleanup await.

Configuration is available through `configuration(): array`, a startup snapshot containing `journal_mode`, `synchronous`, `busy_timeout`, `wal_autocheckpoint`, and `sqlite_version`, and through `synchronousMode(): SqliteSynchronousMode`. These do not query a live connection or grant concurrent access to it.

## Schema and ordering

`queue_messages` holds all queues. SQLite also maintains `sqlite_sequence` for AUTOINCREMENT.

| Column | Meaning |
| --- | --- |
| `id INTEGER PRIMARY KEY AUTOINCREMENT` | Insertion sequence and identity. Committed IDs are not reused after ordinary deletion. |
| `queue TEXT NOT NULL` | Logical queue name. QueueName validation belongs to the API boundary. |
| `body BLOB NOT NULL`, `headers BLOB NOT NULL` | Opaque payloads; CHECK constraints require BLOB storage types. |
| `available_at INTEGER NOT NULL` | Send deadline; a CHECK requires a nonnegative value. |
| `reserved_until INTEGER` | Current visibility expiry, initially null. |
| `reservation_token TEXT` | Random 256-bit delivery token, initially null. |
| `owner_id TEXT` | Caller-supplied owner identity, initially null. |
| `broker_epoch TEXT` | Random 256-bit Queue identity, initially null. |

A CHECK requires the four reservation fields to be either all null or all populated. The worker calculates nonnegative wall-clock milliseconds and rejects deadline overflow. Claim and renewal do not change `available_at`.

| Index | Purpose |
| --- | --- |
| `queue_messages_order(queue, id)` | Supports selection in insertion order within a queue. Future or reserved rows can increase scan work but do not block later eligible rows. |
| `queue_messages_ready(queue, max(available_at, coalesce(reserved_until, available_at)))` | Supports the earliest effective eligibility lookup for notifier scheduling. |

Receive selects the lowest eligible ID:

```sql
SELECT id, body, headers, available_at FROM queue_messages
WHERE queue = ? AND available_at <= ?
  AND (reserved_until IS NULL OR reserved_until <= ?)
ORDER BY id LIMIT 1
```

The complete claim runs inside one immediate transaction: acquire the write lock, sample time, select and validate the payload, conditionally reserve the row, then commit. Empty selection or a zero-row reservation update returns `null` after rollback. Driver materialization limits can reject oversized stored payloads before the reservation update; this is an error, not an empty-queue result. The operation does not use DML `RETURNING`.

Effective eligibility is `max(available_at, coalesce(reserved_until, available_at))`. `earliestEligibility()` returns the minimum for a queue, including a past timestamp when work is already ready. It does not hold a transaction while waiting for a future deadline. The [delivery contracts](contracts.md#eligibility-and-ordering) explain clock changes and the distinction between selection order and completion order.

## Visibility and receipts

The worker samples time after acquiring the write transaction. Claim writes a new token, owner, epoch, and expiry. Settlement and renewal match message ID and all reservation fields and require `reserved_until > now`.

A zero-row conditional mutation is diagnosed inside that transaction with the same clock sample. Missing or unreserved rows, owner mismatch, epoch mismatch, token mismatch, and expiry have distinct outcomes in that order. Matching all predicates without changing a row is an invariant error, not a receipt rejection. See [receipt errors](broker-protocol.md#receipt-errors) for wire codes and exception classes.

Reopening storage does not clear reservations. Constructing a new Queue changes the epoch, so old receipts cannot settle or renew those rows. At expiry, a new receive may claim them with a new token. [Reservation and keepalive contracts](contracts.md#reservations-and-receipts) define renewal, duplicate handling, and effect fencing.

## Failures and durability

Storage validates worker transaction outcomes and result shapes before confirming an operation. Contradictory or malformed application replies close storage. SQL and driver failures propagate; direct engine callers must handle them and own service shutdown. The broker adds its own fatal-failure and timeout policy, described under [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

Successful mutation results follow commit. An exception is not proof that a mutation had no effect; use the shared [uncertain-operation rules](contracts.md#uncertain-operations-and-cancellation). NORMAL and FULL have different machine-failure guarantees, detailed under [storage and durability](contracts.md#storage-and-durability).
