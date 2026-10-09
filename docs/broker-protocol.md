# Broker protocol v1

The v1 Unix-socket protocol covers immediate queue operations and bounded WAIT and WAIT_ANY. No operation executes application handlers or accepts SQL or database paths over the socket.

## Framing and limits

Each frame is a four-byte unsigned big-endian length followed by a four-byte control length, UTF-8 JSON control, and raw body and header bytes. The outer length excludes its own prefix. Control includes integer `body_length` and `headers_length`; their sum must equal the raw payload length. JSON never contains serialized application bytes.

Fixed v1 limits are 1,048,576 frame bytes, 8,192 control bytes, 1,040,000 combined body and header bytes, and 30,000 milliseconds for WAIT. Each buffered request or reply is limited to roughly 1 MiB. The broker accepts at most 64 connections, executes one request per connection at a time, and retains at most one pending response per connection. Amp handles partial reads and write backpressure. Oversized lengths are rejected before reading their content.

Handshake reads have a five-second deadline. After the handshake, an established connection can remain idle until disconnect or broker shutdown. Its thirty-second frame deadline starts when the first prefix bytes arrive and covers the rest of that frame. Idle time between requests does not consume the deadline or invalidate receipt ownership. Writes have a five-second deadline. A WAIT exchange remains open for the requested bound plus the client's transport allowance. The client still permits one outstanding call. Cancellation or timeout after starting an exchange invalidates the client. Larger frame limits are not negotiated in v1.

## Exchanges

The first request is `{"v":1,"id":0,"op":"hello"}`. The response confirms v1 and returns `result: {"max_payload":1040000}`. Later requests use consecutive positive integer IDs starting at 1. Every response echoes `v` and `id`, plus `ok: true` and `result`, or `ok: false` and `error: {"code":"..."}`. Framing adds the two payload-length fields to every control object.

Operations are `send` with `queue` and nonnegative millisecond `delay`, `receive` with `queue`, `acknowledge` or `reject` with `receipt`, and `wait` with `queue` and integer `wait_ms`. Send uses the raw payload blocks and returns the insertion ID. Receive remains immediate: it returns null or delivery metadata with raw payload blocks and never accepts `wait_ms`. Delivery metadata contains `id`, `queue`, `receipt`, `available_at`, and `reserved_until`. Both timestamps use Unix milliseconds. Settlement returns null. Closing the socket ends a session. Unknown operations and control fields are rejected.

`wait_any` carries `queues` instead of `queue`, plus integer `wait_ms`. It accepts a JSON list of 1 through 16 distinct queue names. Each name follows the same validation as `queue`; empty input, duplicates, non-list input, invalid names, and larger lists are rejected. Sixteen maximum-length names fit within the existing 8,192-byte control limit. Payload blocks must be empty.

### WAIT

A WAIT request carries `queue` and `wait_ms` only. Both body and headers must be empty. `wait_ms` must be an integer from `0` through `30_000`. Zero is an immediate readiness probe. The successful result is a boolean: `true` is a receive hint, never a reservation; `false` means the bound elapsed without a readiness hint. The reply carries no payload.

WAIT-specific validation rejects negative, oversized, non-integer, or missing `wait_ms`, and rejects any payload bytes. Putting `wait_ms` on `receive` remains `invalid_request`. While WAIT is outstanding, the broker monitors the socket for unexpected bytes, cancels that wait, and replies with `invalid_request` for pipelined traffic. The monitor is cancelled and awaited before the WAIT reply is written. A normal timeout leaves the session reusable. Disconnect, session cancellation, and shutdown end that WAIT without a durable reservation change.

Persisted availability and visibility deadlines use Unix wall-clock milliseconds. WAIT bounds and per-queue deadline delays use Revolt duration timers. The broker converts a future wall-clock deadline into a duration under normal forward clock progression. A backward jump can postpone eligibility and force rearming. A forward jump can make work eligible on the next recheck, but an already armed monotonic timer is not moved earlier, so a wake hint may arrive late until another recheck, mutation, or WAIT. This is not a realtime or latency guarantee, and there is no separate scheduler service.

The broker schedules wakeups for watched queues from persisted eligibility deadlines. Committed mutations trigger a readiness recheck. Restart rebuilds scheduling from storage. Timeout, cancellation, disconnect, and shutdown remove the affected waiters and timers.

WAIT_ANY uses the same timeout, boolean response, disconnect handling, and per-queue timers as WAIT. It registers every selected queue before checking readiness. A ready queue completes the whole request and removes every registration. Zero duration returns false only after all selected queues have been checked without a readiness hint. The reply does not identify a queue or choose one to claim. Consumers retain their own receive order.

Single-queue WAIT remains unchanged. Older v1 brokers reject `wait_any` as `invalid_request`; the client fails explicitly and does not retry or downgrade to polling. Upgrade the broker before enabling multi-queue notification consumers.

General errors are `invalid_request`, `unsupported_protocol_version`, `frame_too_large`, `invalid_queue_name`, and `broker_shutting_down`. Receipt errors are listed below. Malformed traffic ends the connection after a bounded error response when framing permits one. Storage failures are treated conservatively as uncertain outcomes, terminate service, and never trigger replay. Default diagnostics contain error categories, not payloads or worker exception text.

Invalid queue names and receipt rejections leave the session usable. Other protocol errors end it. At the connection limit, the broker closes the new socket without a handshake. Shutdown and storage failure can close sockets before an error response is delivered.

### Receipt errors

Each receipt error maps to a concrete exception under `Exception`. All six extend `InvalidReceiptException` and leave the client usable.

| Wire code | Exception | Established failure |
| --- | --- | --- |
| `malformed_receipt` | `MalformedReceiptException` | Invalid receipt syntax or message ID outside the supported positive integer range. |
| `no_active_reservation` | `NoActiveReservationException` | The message is absent or unreserved. |
| `receipt_owner_mismatch` | `ReceiptOwnerMismatchException` | The reservation belongs to another client connection. |
| `receipt_epoch_mismatch` | `ReceiptEpochMismatchException` | The reservation belongs to another Queue epoch. |
| `receipt_token_mismatch` | `ReceiptTokenMismatchException` | The supplied token differs from the current reservation token. |
| `expired_receipt` | `ExpiredReceiptException` | The reservation deadline is at or before the sampled transaction time. |

Syntax is checked before storage access. After a zero-row conditional DELETE, storage diagnoses failures in table order inside the same immediate transaction. It uses the same clock sample as DELETE. When several fields mismatch, the first failure wins. Missing rows have no history to distinguish never-created from already-settled messages.

## Ownership and readiness

The foreground command is `bin/sqlite-queue broker --database=/absolute/path/queue.sqlite --endpoint=/private/directory/queue.sock`. Both parent directories must exist, belong to the current user, and have no group or other permissions. Existing database and lock files must also be private and owned by that user. Socket paths are limited to 100 bytes. New files and sockets use mode 0600.

The broker holds two exclusive nonblocking Symfony FlockStore locks for its lifetime, one keyed by the canonical database path and one by the canonical endpoint path. Parent-directory aliases resolve to the same paths, so a second broker using a different endpoint for the same database still conflicts. The store manages its own sidecar files under the resource parent directories. Those directories must exist, belong to the current user, and deny group and other access, which protects the sidecars without this package naming or touching them. The database file itself is created mode 0600 or strictly validated: symlinks, multiply linked files, foreign owners, and group or other access are rejected. Lock sidecars stay on disk after shutdown to preserve the lock namespace.

Startup refuses every existing endpoint, including a leftover socket. It does not infer that a socket is abandoned from its existence or from a failed probe. Normal shutdown removes only the socket inode created by this broker. After an abrupt broker exit, restarting at that endpoint requires operator verification and removal of the leftover socket, or a different endpoint. Other tools must respect the broker's ownership and must not alter its files while it runs.

Readiness is a JSON `ready` event on stdout after lifetime locks, storage initialization, and socket binding succeed. The broker owns client-session lifetime and cancels those contexts on disconnect or shutdown. Queue and storage retain delivery owner identifiers, not a registry of live sessions. Shutdown stops accepting, closes clients, and closes local storage before releasing the socket and locks. SIGINT and SIGTERM request shutdown. The package does not detach or manage an application service.

The readiness event includes `pid`, `storage_execution: "fabpot"`, `database`, `endpoint`, and `synchronous_effective`. It omits `persistence_pid`; the driver owns its child process. Normal stop exits 0. Startup failures, observed storage failures, and exhaustion of the broker's shutdown budget exit nonzero. Driver close may force termination after a close-protocol failure without reporting that failure to the broker; exit 0 does not certify graceful driver shutdown.

The former package-worker startup and exchange deadlines do not apply. Shutdown has one five-second budget starting when the broker processes its first stop request. Driver close receives the same cancellation and terminates an unresponsive child. Fatal storage failure stops service without a separate error-frame write. Cleanup attempts every step and retains the first failure.

SQL executes in the driver's child process. Waiting for its results suspends a fiber, leaving the broker's event loop available for sockets, WAIT timers, and signals. A client timeout does not revoke dispatched SQL or prove rollback. SQLite's 5,000-millisecond busy timeout is not an overall call bound. Use an external supervisor for a hard termination deadline.

## Client recovery

`Client` connects once and performs no automatic reconnect or retry. A transport error means the caller must discard it. An absent confirmation does not establish whether send, claim, or settlement committed. SQLite can commit before its result reaches the broker or before the broker's confirmation reaches the client. A connection failure in either interval leaves the caller unable to distinguish a committed operation from a failure before commit. Constructing a new client is explicit and does not reuse the old session or receipts. Malformed remote responses raise `ProtocolException` with the violated field, invalidate the client, and never replay. Known receipt rejections and invalid-queue responses keep the session usable. A receipt cannot move to a different connection.

The default client timeout is ten seconds per exchange and for connection establishment. A WAIT exchange uses that transport allowance plus the requested `wait_ms`. Concurrent calls on the same client raise `LogicException` rather than queueing more requests. A frame rejected locally before writing, including unencodable UTF-8 control or an out-of-range WAIT bound, does not consume a request ID and leaves the client open. `TransportException` invalidates the client for EOF, timeout, cancellation, and socket I/O failure. A known queue-name error raises `InvalidArgumentException`. Receipt rejections raise the specific exceptions listed above.

The client does not open the queue database. Standalone engine and client use do not require a Symfony application kernel. The executable requires Symfony Console, Unix sockets, `ext-sqlite3`, `ext-posix`, and `ext-pcntl`. See [Run the broker and use the client](broker.md) for setup. See [SQLite storage](sqlite-worker.md) for driver ownership and shutdown behavior.
