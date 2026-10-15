# Delivery contracts

This page defines message delivery and failure semantics. For interfaces, see the [wire protocol](broker-protocol.md), [in-process engine](queue-engine.md), and [Messenger adapter](messenger.md). For execution and timeout ownership, see [SQLite storage](sqlite-worker.md).

## Supported environment

| Requirement | Scope |
| --- | --- |
| PHP `^8.5`, `ext-sqlite3`, SQLite 3.31.0 or newer | All package use. |
| Symfony components `^8.0` | Console, Messenger, and other runtime components. FrameworkBundle is needed for bundle integration, not standalone use. |
| Unix-domain sockets and `ext-posix` | Broker execution and private file ownership checks. Windows is not supported. |
| `ext-pcntl` | Foreground broker command signal handling and native Messenger keepalive. |
| Private, user-owned directories and absolute database and socket paths | Broker resource ownership. See [broker operation](broker.md). |

The package provides a foreground broker, PHP client, Messenger transport, and an advanced in-process engine. It stores bodies and headers as opaque bytes. It does not execute handlers or deserialize application objects. Doctrine is not a runtime dependency; applications using a Doctrine failure transport install it separately.

The standalone command is `vendor/bin/sqlite-queue broker`; the bundle command is `bin/console sqlite-queue:broker`. Transport construction does not start or connect to the broker. Native consumers use Symfony's `messenger:consume`. Separate operation and notification connections let an idle WAIT be cancelled without discarding the owning settlement connection.

## Storage and durability

SQLite is the authoritative store. One table holds all named queues; publishing to a new name needs no migration. There is no message cache that can replace committed database state.

| Operation | Success means |
| --- | --- |
| Send | The insertion committed. |
| Receive with a delivery | A reservation committed before the delivery was returned. |
| ACK or reject | The fenced terminal deletion committed. Neither operation schedules a retry. |
| Keepalive | The reservation extension committed. |
| WAIT or WAIT_ANY | A readiness hint, not a reservation or a claim guarantee. |

The delivery model is at least once, not exactly once, subject to the selected durability mode:

| WAL synchronous mode | Guarantee and limitation |
| --- | --- |
| NORMAL, the default | Preserves commits across application or process crashes. Recent committed writes can be lost after an OS crash or power failure. A confirmed send can disappear; a lost ACK can make a message reappear. This mode does not guarantee at-least-once delivery across power loss. |
| FULL | Stronger commit durability across OS crashes and power loss, provided the filesystem and storage honor synchronization. It does not provide exactly-once handler effects. |

A process-crash or close/reopen check is not evidence of power-loss survival. Configure durability with the broker's `--synchronous` option, bundle `sqlite_queue.synchronous`, or `SqliteSynchronousMode` in PHP.

## Eligibility and ordering

A message is eligible when `available_at <= now` and its reservation is absent or expired. Receive immediately selects the lowest insertion ID among eligible messages in that queue. Reservation is atomic: concurrent receives cannot reserve the same delivery. Delayed or reserved messages do not block later eligible messages. There is no priority scheduling or consumer completion-order guarantee.

Send delay is a nonnegative integer number of milliseconds. The worker samples wall-clock time after acquiring the write transaction, then persists `available_at = now + delay`. Positive subsecond delays are retained. Restart does not reset availability or reservation deadlines.

Clock adjustments can advance or postpone eligibility. WAIT scheduling uses duration timers derived from persisted wall-clock deadlines. An already armed timer can deliver a late hint after a forward clock jump. Receive still checks persisted eligibility inside its transaction. See [schema and ordering](queue-engine.md#schema-and-ordering).

## Reservations and receipts

The default reservation is **60 seconds**. The broker CLI's `--redeliver-timeout` and bundle `sqlite_queue.redeliver_timeout` use positive integer seconds. An explicit CLI value overrides bundle configuration; the standalone command does not load that configuration. PHP `Queue` and `BrokerFactory::visibilityTimeout` use milliseconds, defaulting to **60,000**.

A receipt identifies a delivery and is fenced by its message ID, owning connection, Queue epoch, reservation token, and unexpired deadline. Treat it as opaque. A new connection cannot reuse an old connection's receipts. Restart creates a new epoch without clearing existing reservations.

| Event | Reservation behavior |
| --- | --- |
| Consumer disconnects or crashes | The persisted reservation remains until its last committed expiry. |
| Receipt expires | ACK, reject, and renewal fail, even if no other consumer has reclaimed the message. |
| Message is reclaimed | A new token identifies the new delivery. Old receipts cannot modify it. |
| ACK or reject succeeds | The row is deleted. Reject is terminal, not a request for redelivery. |
| Messenger retries | Messenger publishes a new message with its own delay and identity. |

Consumer-initiated keepalive extends an unexpired reservation to `max(existing expiry, now + max(configured reservation duration, requested minimum))`. It keeps the receipt and token unchanged, never shortens expiry, and cannot revive an expired, settled, or reclaimed delivery. Successful renewals allow indefinite processing; there is no cumulative processing or receive-count cap.

Native Messenger `--keepalive` uses a **5-second interval** when no interval is supplied. The lease remains 60 seconds by default. A handler running for hours needs continued successful renewals, not a longer communication timeout. Losing renewal does not stop application code. After expiry, another consumer can claim the message while the original handler is still running. Make external effects idempotent or fence them against stale workers.

The six concrete receipt exceptions and their precedence are listed under [receipt errors](broker-protocol.md#receipt-errors). `ClientContextClosedException` reports engine-context cancellation and does not extend `InvalidReceiptException`.

## Uncertain operations and cancellation

A mutation can commit before its confirmation reaches the caller. Missing confirmation or an exception therefore does not prove rollback. This applies to send, claim, renewal, and settlement.

Cancellation can prevent work before storage begins. It does not revoke dispatched SQL: an ACK may commit after disconnect, and an unconfirmed claim may retain its reservation until expiry. Forced driver termination also does not establish whether a write committed.

After an I/O failure, timeout, or exchange cancellation, the PHP client is unusable. It raises `TransportException`, performs no automatic reconnect or replay, and requires the caller to create a new connection explicitly. Malformed remote replies instead raise `ProtocolException` and also invalidate the client. Recoverable receipt and queue-name rejections are different; see [client recovery](broker-protocol.md#client-recovery).

The package keeps no operation deduplication history. An application that republishes an uncertain send may create a duplicate. A deliberate Messenger retry is a new publication, not a retransmission of the failed exchange. Applications decide how to recover from uncertainty and whether to restart their workers.

## Waits and resource bounds

WAIT and WAIT_ANY return booleans, not deliveries. Their public bound is **0 through 30,000 milliseconds**; zero is an immediate probe. WAIT_ANY accepts **1 through 16 distinct queues** and does not choose which queue the consumer should receive from.

Native Messenger uses up to **1,000 milliseconds** per idle wait for selected queues on one broker. Mixed transports, different endpoints, or larger selections containing sqlite-queue use a bounded sleep fallback. Consumers without sqlite-queue receivers retain native idle handling. See [Messenger integration](messenger.md).

The broker admits **64 connections**, including notification connections. Frame and payload bounds are defined in [framing and limits](broker-protocol.md#framing-and-limits). Storage startup, individual operations, and shutdown have separate [budgets](sqlite-worker.md#cancellation-and-budgets). Handler execution and established-session idle time are not storage operations.

The broker owns its database and socket paths for its lifetime. Readiness is a JSON `ready` event after locks, storage initialization, and socket binding succeed; process creation alone is not readiness. The package does not daemonize or supervise itself. There are no public queue list, count, or purge APIs.
