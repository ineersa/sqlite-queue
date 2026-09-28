# Broker protocol v1

Task 04 exposes immediate queue operations over a local Unix socket. Notification waiting remains Task 05 work. No operation executes application handlers or accepts SQL or database paths over the socket.

## Framing and limits

Each frame is a four-byte unsigned big-endian length followed by a four-byte control length, UTF-8 JSON control, and raw body and header bytes. The outer length excludes its own prefix. Control includes integer `body_length` and `headers_length`; their sum must equal the raw payload length. JSON never contains serialized application bytes.

Fixed v1 limits are 1,048,576 frame bytes, 8,192 control bytes, and 1,040,000 combined body/header bytes. The payload limit supports the existing 16 KiB benchmark payload with room for larger messages, while limiting each buffered request or reply to roughly 1 MiB. It is not a throughput claim. The broker accepts at most 64 connections, executes one request per connection at a time, and retains at most one pending response per connection. Amp handles partial reads and write backpressure. Oversized lengths are rejected before reading their content.

Handshake reads have a five-second deadline. Subsequent frame reads have a thirty-second deadline, including idle time. Writes have a five-second deadline. The client bounds each complete exchange and permits one outstanding call. Cancellation or timeout after starting an exchange invalidates the client. Large limits and indefinite idle connections are not negotiated in v1.

## Exchanges

The first request is `{"v":1,"id":0,"op":"hello"}`. The response confirms v1 and returns `result: {"max_payload":1040000}`. Later requests use consecutive positive integer IDs starting at 1. Every response echoes `v` and `id`, plus `ok: true` and `result`, or `ok: false` and `error: {"code":"..."}`. Framing adds the two payload-length fields to every control object.

Operations are `send` with `queue` and nonnegative millisecond `delay`, `receive` with `queue`, and `acknowledge` or `reject` with `receipt`. Send uses the raw payload blocks and returns the insertion ID. Receive returns null or delivery metadata with raw payload blocks. Delivery metadata contains `id`, `queue`, `receipt`, `available_at`, and `reserved_until`. Both timestamps use Unix milliseconds. Settlement returns null. Closing the socket ends a session. Unknown operations and control fields are rejected, including WAIT and `wait_ms`.

Errors are `invalid_request`, `unsupported_protocol_version`, `frame_too_large`, `invalid_queue_name`, `stale_receipt`, `broker_shutting_down`, and `internal_storage_failure`. Malformed traffic ends the connection after a bounded error response when framing permits one. Storage failures are treated conservatively as uncertain outcomes, terminate service, and never trigger replay. Default diagnostics contain error categories, not payloads or driver exception text.

`stale_receipt` also covers unknown deliveries and foreign receipts. Invalid queue names and stale receipts leave the session usable. Other protocol errors end it. At the connection limit, the broker closes the new socket without a handshake. Shutdown and storage failure can close sockets before an error response is delivered.

## Ownership and readiness

The foreground command is `bin/sqlite-queue broker --database=/absolute/path/queue.sqlite --endpoint=/private/directory/queue.sock`. Both parent directories must exist, belong to the current user, and have no group or other permissions. Existing database and lock files must also be private and owned by that user. Socket paths are limited to 100 bytes. New files and sockets use mode 0600.

The broker holds two exclusive nonblocking Symfony FlockStore locks for its lifetime, one keyed by the canonical database path and one by the canonical endpoint path. Parent-directory aliases resolve to the same paths, so a second broker using a different endpoint for the same database still conflicts. The store manages its own sidecar files under the resource parent directories. Those directories must exist, belong to the current user, and deny group and other access, which protects the sidecars without this package naming or touching them. The database file itself is created mode 0600 or strictly validated: symlinks, multiply linked files, foreign owners, and group or other access are rejected. Lock sidecars stay on disk after shutdown to preserve the lock namespace.

Startup refuses every existing endpoint, including a leftover socket. It does not infer that a socket is abandoned from its existence or from a failed probe. Normal shutdown removes only the socket inode created by this broker. After an abrupt broker exit, restarting at that endpoint requires operator verification and removal of the leftover socket, or a different endpoint. Other tools must respect the broker's ownership and must not alter its files while it runs.

Readiness is a JSON `ready` event on stdout after ownership, engine initialization, and socket binding succeed. Shutdown stops accepting, closes client sockets, invalidates their engine sessions, drains or fails in-flight storage, and closes the persistence child before releasing ownership. SIGINT and SIGTERM request shutdown. The broker runs in the foreground; a caller may supervise it, but the package does not detach or manage an application service.

The readiness event includes `pid`, `persistence_pid`, `database`, and `endpoint`. Amp may also create a shell launcher. The broker detects idle persistence-child exit through EOF on the child's output pipes. It drains those pipes without logging their contents. The SQLite driver alone joins its process context; the broker never joins it a second time. Normal stop exits 0; storage or startup failure exits nonzero. Shutdown has a five-second total budget: if any step stalls, a watchdog kills the owned persistence child directly through its process context. A repeated graceful connection close cannot do this, because the driver marks its connection closed before that close returns. Every shutdown step still runs after an earlier failure, and the first failure is what the caller receives.

## Client recovery

`Client` connects once and performs no automatic reconnect or retry. A transport error means the caller must discard it. An absent confirmation does not establish whether send, claim, or settlement committed. Constructing a new client is explicit and does not reuse the old session or receipts. Known invalid-request and stale-receipt responses can be reported without replay. A receipt cannot move to a different connection.

The default client timeout is ten seconds per exchange and for connection establishment. Concurrent calls on the same client raise `LogicException` rather than queueing more requests. A frame rejected locally before writing does not consume a request ID. `TransportException` invalidates the client. A known queue-name error raises `InvalidArgumentException`, and a stale receipt raises `InvalidReceipt`.

Operations and error codes are backed string enums (`Operation`, `ErrorCode`). The client and broker build and check `Frame` control arrays directly, with small validation next to each use. `ProtocolException` carries a typed `ErrorCode`. Delivery timestamps are signed Unix-millisecond integers, mapped without added semantics. The wire format is unchanged v1, and the no-replay rule still holds: an uncertain outcome never triggers an automatic retry.

The engine and client use Amp and Revolt without Symfony or an application kernel. The broker requires `symfony/lock:^8.0` and `symfony/filesystem:^8.0` at runtime for exclusive ownership. The executable uses Symfony Console `^8.0`, an optional CLI dependency. Broker execution requires Unix sockets and `ext-posix`; signal handling in the executable also requires `ext-pcntl`. The client does not open the queue database or start a persistence process. See [Run the broker and use the client](broker.md) for setup.
