# Broker protocol v1

The v1 Unix-socket protocol covers immediate queue operations and bounded WAIT. No operation executes application handlers or accepts SQL or database paths over the socket.

## Framing and limits

Each frame is a four-byte unsigned big-endian length followed by a four-byte control length, UTF-8 JSON control, and raw body and header bytes. The outer length excludes its own prefix. Control includes integer `body_length` and `headers_length`; their sum must equal the raw payload length. JSON never contains serialized application bytes.

Fixed v1 limits are 1,048,576 frame bytes, 8,192 control bytes, 1,040,000 combined body/header bytes, and 30,000 milliseconds for WAIT. The payload limit supports the existing 16 KiB benchmark payload with room for larger messages, while limiting each buffered request or reply to roughly 1 MiB. It is not a throughput claim. The broker accepts at most 64 connections, executes one request per connection at a time, and retains at most one pending response per connection. Amp handles partial reads and write backpressure. Oversized lengths are rejected before reading their content.

Handshake reads have a five-second deadline. Subsequent non-WAIT frame reads have a thirty-second deadline, including idle time. Writes have a five-second deadline. A WAIT exchange remains open for the requested bound plus the client's transport allowance. The client still permits one outstanding call. Cancellation or timeout after starting an exchange invalidates the client. Large limits and indefinite idle connections are not negotiated in v1.

## Exchanges

The first request is `{"v":1,"id":0,"op":"hello"}`. The response confirms v1 and returns `result: {"max_payload":1040000}`. Later requests use consecutive positive integer IDs starting at 1. Every response echoes `v` and `id`, plus `ok: true` and `result`, or `ok: false` and `error: {"code":"..."}`. Framing adds the two payload-length fields to every control object.

Operations are `send` with `queue` and nonnegative millisecond `delay`, `receive` with `queue`, `acknowledge` or `reject` with `receipt`, and `wait` with `queue` and integer `wait_ms`. Send uses the raw payload blocks and returns the insertion ID. Receive remains immediate: it returns null or delivery metadata with raw payload blocks and never accepts `wait_ms`. Delivery metadata contains `id`, `queue`, `receipt`, `available_at`, and `reserved_until`. Both timestamps use Unix milliseconds. Settlement returns null. Closing the socket ends a session. Unknown operations and control fields are rejected.

### WAIT

A WAIT request carries `queue` and `wait_ms` only. Both body and headers must be empty. `wait_ms` must be an integer from `0` through `30_000`. Zero is an immediate readiness probe. The successful result is a boolean: `true` is a receive hint, never a reservation; `false` means the bound elapsed without a readiness hint. The reply carries no payload.

WAIT-specific validation rejects negative, oversized, non-integer, or missing `wait_ms`, and rejects any payload bytes. Putting `wait_ms` on `receive` remains `invalid_request`. While WAIT is outstanding, the broker monitors the socket for unexpected bytes, cancels that wait, and replies with `invalid_request` for pipelined traffic. The monitor is cancelled and awaited before the WAIT reply is written. A normal timeout leaves the session reusable. Disconnect, session cancellation, and shutdown end that WAIT without a durable reservation change.

Persisted availability and visibility deadlines use Unix wall-clock milliseconds. WAIT bounds and per-queue deadline delays use Revolt duration timers. The broker converts a future wall-clock deadline into a duration under normal forward clock progression. A backward jump can postpone eligibility and force rearming. A forward jump can make work eligible on the next recheck, but an already armed monotonic timer is not moved earlier, so a wake hint may arrive late until another recheck, mutation, or WAIT. This is not a realtime or latency guarantee, and there is no separate scheduler service.

`QueueNotifier` keeps derived waiter and deadline state only for watched queues. It rebuilds deadlines lazily from storage after restart or when a watch is created. After each committed send or claim that can change readiness, the broker posts a hint and dirty-rechecks the earliest effective eligibility for that queue. Watches are identified by object identity so a stale in-flight query cannot re-arm timers for a replaced or idle watch. Timeout, cancellation, disconnect, and shutdown clear waiters and timers for the affected queues.

General errors are `invalid_request`, `unsupported_protocol_version`, `frame_too_large`, `invalid_queue_name`, and `broker_shutting_down`. Receipt errors are listed below. Malformed traffic ends the connection after a bounded error response when framing permits one. Storage failures are treated conservatively as uncertain outcomes, terminate service, and never trigger replay. Default diagnostics contain error categories, not payloads or driver exception text.

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

Syntax is checked before storage access. After a zero-row conditional DELETE, storage diagnoses failures in table order inside the same immediate transaction. It uses the same clock sample as DELETE. When several fields mismatch, the first failure wins. Missing rows have no history to distinguish never-created from already-settled messages. The former generic `stale_receipt` code is removed from this unreleased protocol.

## Ownership and readiness

The foreground command is `bin/sqlite-queue broker --database=/absolute/path/queue.sqlite --endpoint=/private/directory/queue.sock`. Both parent directories must exist, belong to the current user, and have no group or other permissions. Existing database and lock files must also be private and owned by that user. Socket paths are limited to 100 bytes. New files and sockets use mode 0600.

The broker holds two exclusive nonblocking Symfony FlockStore locks for its lifetime, one keyed by the canonical database path and one by the canonical endpoint path. Parent-directory aliases resolve to the same paths, so a second broker using a different endpoint for the same database still conflicts. The store manages its own sidecar files under the resource parent directories. Those directories must exist, belong to the current user, and deny group and other access, which protects the sidecars without this package naming or touching them. The database file itself is created mode 0600 or strictly validated: symlinks, multiply linked files, foreign owners, and group or other access are rejected. Lock sidecars stay on disk after shutdown to preserve the lock namespace.

Startup refuses every existing endpoint, including a leftover socket. It does not infer that a socket is abandoned from its existence or from a failed probe. Normal shutdown removes only the socket inode created by this broker. After an abrupt broker exit, restarting at that endpoint requires operator verification and removal of the leftover socket, or a different endpoint. Other tools must respect the broker's ownership and must not alter its files while it runs.

Readiness is a JSON `ready` event on stdout after lifetime locks, storage initialization, and socket binding succeed. The broker owns client-session lifetime and cancels those contexts on disconnect or shutdown. Queue and storage retain delivery owner identifiers, not a registry of live sessions. Shutdown stops accepting, closes clients, drains or fails storage work, and closes the worker before releasing the socket and locks. SIGINT and SIGTERM request shutdown. The package does not detach or manage an application service.

The command registers a one-second event-loop timer to prevent an indefinite select wait when a signal arrives between the driver's signal check and its select call. This does not poll storage or guarantee progress if the event loop is stalled. Production output contains ready, stopped, and failed events from the shared `Broker\BrokerEventEnum`, without a trace-file option. Normal SIGTERM shutdown and SIGKILL worker failure/recovery are release requirements. The deliberately SIGSTOPped-worker scenario is excluded by scope decision; its historical intermittent hang remains unexplained.

The readiness event includes `pid`, `persistence_pid`, `database`, and `endpoint`. Amp may also create a shell launcher. The broker detects idle SQLite-worker exit through EOF on the child's output pipes. It drains those pipes without logging their contents. The readiness field `persistence_pid` remains the wire key for that worker PID. The SQLite driver alone joins its process context; the broker never joins it a second time. Normal stop exits 0; storage or startup failure exits nonzero.

Shutdown has one five-second budget, armed on the first transition to stopping and before the server or a client socket closes. Repeated stop requests share that deadline and never re-arm it. Every awaited step shares the deadline: the client drain, storage close, worker pipe observation, and child monitor. `SqliteQueueStorage` owns the SQLite connection. A step that takes no cancellation runs in its own fiber and is awaited with the same budget. When the budget expires, the broker kills its worker through the process context. A kill failure becomes the cancellation cause instead of an unhandled loop error. Repeating graceful close cannot interrupt a stalled worker because the driver marks the connection closed before awaiting completion. The deadline is disarmed after the awaiting steps return. Fatal storage failure stops the service without a separate error-frame write. Cleanup attempts every step and preserves the first failure.

The budget bounds what the broker waits for, not the exact wall-clock duration of the exit. Signal delivery, process teardown, and reaping are scheduled by the operating system. Pipes can outlive the killed child when a shell launcher still holds their write ends, so pipe observation is bounded by the same budget rather than by a fresh timeout. Fatal storage failure stops the service without a dedicated wire error code.

## Client recovery

`Client` connects once and performs no automatic reconnect or retry. A transport error means the caller must discard it. An absent confirmation does not establish whether send, claim, or settlement committed. SQLite can commit before its result reaches the broker or before the broker's confirmation reaches the client. A connection failure in either interval leaves the caller unable to distinguish a committed operation from a failure before commit. Constructing a new client is explicit and does not reuse the old session or receipts. Malformed remote responses raise `ProtocolException` with the violated field, invalidate the client, and never replay. Known receipt rejections and invalid-queue responses keep the session usable. A receipt cannot move to a different connection.

The default client timeout is ten seconds per exchange and for connection establishment. A WAIT exchange uses that transport allowance plus the requested `wait_ms`. Concurrent calls on the same client raise `LogicException` rather than queueing more requests. A frame rejected locally before writing, including unencodable UTF-8 control or an out-of-range WAIT bound, does not consume a request ID and leaves the client open. `TransportException` invalidates the client for EOF, timeout, cancellation, and socket I/O failure. A known queue-name error raises `InvalidArgumentException`. Receipt rejections raise the specific exceptions listed above.

Operations and error codes are backed string enums (`Operation`, `ErrorCode`). Control field names live in `ControlField`, and `Operation::allowedFields()` owns the request allow lists. The client and broker build and check `Frame` control arrays directly, with small validation next to each use. `ProtocolException` carries a typed `ErrorCode`. Delivery timestamps are signed Unix-millisecond integers, mapped without added semantics. The wire format is unchanged v1, and the no-replay rule still holds: an uncertain outcome never triggers an automatic retry.

The engine and client use Amp and Revolt without Symfony or an application kernel. The broker requires `symfony/lock:^8.0` and `symfony/filesystem:^8.0` at runtime for exclusive ownership. The executable uses Symfony Console `^8.0`, an optional CLI dependency. Broker execution requires Unix sockets and `ext-posix`; signal handling in the executable also requires `ext-pcntl`. The client does not open the queue database or start a persistence process. See [Run the broker and use the client](broker.md) for setup.
