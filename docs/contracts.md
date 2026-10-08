# Package contracts

This reference describes the supported APIs and delivery guarantees. The [protocol reference](broker-protocol.md), [engine reference](queue-engine.md), and [Messenger reference](messenger.md) give the detailed interfaces.

## Supported environment

The package requires PHP `^8.5`, `ext-sqlite3`, and SQLite 3.31.0 or newer. Symfony components use `^8.0`. FrameworkBundle is required for bundle integration, not for standalone use. Broker execution requires Unix sockets and `ext-posix`; its command also requires `ext-pcntl`.

The broker uses one asynchronous SQLite connection through the `ineersa/amp-sqlite3` fork. The driver owns its child process and internal IPC. This package owns SQL and queue policy, not a worker protocol. Waiting for storage suspends a fiber without blocking sockets or WAIT callbacks. See [SQLite storage](sqlite-worker.md).

Doctrine DBAL and Doctrine Messenger are not runtime dependencies. Applications that choose a Doctrine failure transport install those dependencies themselves.

## Public boundary

The package provides a queue engine, a foreground broker, a socket protocol and PHP client, and a Symfony Messenger transport. The standalone command is `vendor/bin/sqlite-queue broker`. The bundle command is `bin/console sqlite-queue:broker`.

Client operations are send, immediate receive, acknowledge, reject, bounded wait, and close. The broker stores bodies and headers as opaque bytes. It does not run handlers, accept SQL from clients, or deserialize application objects.

The Messenger bundle uses native `messenger:consume`. Transport construction does not connect or start the broker. Separate operation and notification sockets preserve settlement capability when an idle wait is cancelled.

## Storage and durability

SQLite is the only authoritative message store. One table holds all named queues, and a new queue name needs no migration. A message is eligible when its availability deadline has passed and its reservation has expired or is absent.

Receive reserves one eligible message in a write transaction and returns it only after commit. Concurrent receives cannot reserve the same delivery. Send, ACK, and reject also return success only after commit. A rollback or storage error does not produce success.

The writing connection uses WAL and synchronous NORMAL by default. NORMAL preserves committed changes across application or process crashes, but recent committed writes can be lost after an OS crash or power failure. A confirmed send may disappear without delivery, and a committed ACK may be lost so the message reappears. NORMAL does not guarantee at-least-once delivery across power loss.

Choose synchronous FULL for stronger commit durability across OS crashes and power loss, assuming storage honors synchronization. Durability still depends on SQLite, the filesystem, and the host. A process-crash or reopen check does not simulate power loss.

Transactions remain short. They do not include handlers, client socket I/O, or waits for future messages. Claim selects the ID and payload, then conditionally updates the reservation in one immediate transaction. See [schema and ordering](queue-engine.md#schema-and-ordering).

Eligible messages are selected in insertion-sequence order. Delayed or reserved rows do not block a later eligible message or another queue. Concurrent consumers have no promised completion order. There is no priority scheduling.

## Reservations and receipts

The default reservation duration is **60 seconds**. `--redeliver-timeout` and bundle `sqlite_queue.redeliver_timeout` use positive integer seconds. An explicit CLI value overrides bundle configuration. The standalone command does not load bundle configuration.

The PHP engine and `BrokerFactory::visibilityTimeout` use milliseconds, default **60,000**. Client communication timeouts are separate from reservations.

- The claim persists its expiry. Disconnect does not release it early; restart does not reset or extend it.
- There is no lease renewal or maximum receive count. A message is not deleted because it was received too often.
- A receipt is tied to the delivery token, owning connection, and issuing engine epoch.
- ACK and reject check every reservation field and require an unexpired receipt. An expired receipt fails even before another worker reclaims the message.
- ACK deletes the delivery. Reject is also terminal. Messenger owns retry publication and backoff.
- Receipts from another connection, broker run, or delivery cannot mutate the current reservation.

Receipt failures distinguish malformed syntax, no active reservation, owner mismatch, epoch mismatch, token mismatch, and expiry. They leave the client session usable. See [receipt errors](broker-protocol.md#receipt-errors) for wire codes and failure precedence.

Delivery is at least once, not exactly once. Receipt checks protect queue state, but cannot undo an external effect performed by a slow or stale worker. Make external effects safe to repeat.

## Uncertain operations and cancellation

A mutation can commit before its reply reaches the client. Missing confirmation does not prove that the mutation failed.

On an uncertain send, receive, ACK, or reject, the client raises a transport exception and becomes unusable. It does not reconnect or replay. The caller decides how to handle the unknown outcome before creating a new connection. A new connection cannot reuse old receipts.

Cancellation can stop an operation before storage begins. Connection cancellation does not revoke dispatched SQL. A valid ACK may therefore commit after the client disconnects. A cancelled claim can retain its reservation until the original visibility expiry without delivering the receipt to the cancelled session. Send delay starts after transaction acquisition. Forced driver shutdown can interrupt outstanding work, but does not prove rollback.

The package stores no operation IDs or deduplication history. A deliberate Messenger retry publishes a new message; it does not retransmit the old exchange. Applications decide whether to stop or restart workers after transport failure.

## Delays and waits

Delay uses integer milliseconds, including positive subsecond values. Send persists the availability deadline as Unix wall-clock milliseconds sampled after transaction acquisition. No code path makes a message eligible before that deadline. Restart keeps future deadlines and makes overdue messages eligible immediately.

Clock changes can advance or postpone eligibility. Duration timers are not a realtime guarantee; an already armed wake timer can run late after a forward wall-clock jump. Storage eligibility checks still decide whether receive can claim a message.

WAIT is a readiness hint, not a reservation. The public bound is **0 through 30,000 milliseconds**. Zero probes immediately. Messenger's single-receiver idle wait uses at most **1,000 milliseconds**. Its multiple-receiver and regex-like selections retain polling.

The default communication allowance is **10 seconds** for connection establishment and exchanges. WAIT adds its requested duration to that allowance. This communication setting has no 30-second cap and does not change message visibility. The broker's frame-read deadline is a separate protocol limit.

## Protocol and ownership

Frames are versioned and length-prefixed. Control JSON and binary payload bytes are separate. Each connection permits one outstanding request, and replies echo its ID. Limits reject invalid input rather than truncating messages.

The broker allows 64 connections. Payload, control, and frame limits are listed in [framing and limits](broker-protocol.md#framing-and-limits). Malformed traffic ends the session when it cannot be recovered safely. A storage failure stops service and leaves affected operations uncertain.

The broker exclusively locks its database and socket paths for its lifetime. Directories and files must be private. Startup refuses any existing endpoint. Normal shutdown removes only the broker's own socket. After an abrupt exit, verify that no listener owns a leftover socket before removing it.

Readiness is a JSON `ready` event after locks, storage initialization, and socket binding succeed. Process creation alone is not readiness. The package does not daemonize or supervise itself.

The former package-worker startup and exchange deadlines do not apply. Shutdown shares a five-second cleanup budget across client draining and driver close. The driver terminates an unresponsive child when that budget expires. See [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).
