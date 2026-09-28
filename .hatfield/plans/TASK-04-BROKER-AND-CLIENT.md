# Task 04: foreground broker, socket protocol, and PHP client

Status: DONE, immediate socket service implemented; notification waiting remains Task 05
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 03](TASK-03-ASYNC-SQLITE-QUEUE.md)
Read first: [PLAN.md](PLAN.md), sections 3, 5–7, and 10–11.

## Goal

Expose the real queue engine through a bounded local socket service and PHP client, with explicit ownership and failure behavior. Do not add an application-specific supervisor.

## Scope

- Implement the agreed protocol using existing Amp stream/socket facilities and Revolt, with lossless payload framing and request/reply correlation.
- Support send, immediate receive, ACK, and terminal reject through the actual engine. Forward delayed-send metadata without changing deadlines.
- Start a foreground broker with explicit database and endpoint paths, private permissions, positive readiness, and exclusive database/socket ownership.
- Bound frames, connections, pending requests, and output buffers. Handle partial I/O, truncated/invalid frames, slow readers, cancellation, and disconnects.
- Keep socket waits/writes outside storage transactions and operation locks.
- Implement the agreed uncertain-outcome and client-reconnect contract without hidden mutation replay.
- Own startup and shutdown of the async SQLite persistence child; handle partial startup and child failure.
- Provide basic package-local broker/client usage documentation as the API becomes usable.

## Boundaries

No detached shared-service daemon, application lifecycle manager, custom persistence-worker implementation, handler execution, or arbitrary SQL/path RPC. One outstanding request per connection is sufficient unless the approved contract requires more.

Notification waiting is completed in Task 05. This intermediate immediate-receive service is not the finished MVP or a performance acceptance result.

## Acceptance criteria

- [x] Independent PHP processes communicate with the broker through the real client without booting a consuming application.
- [x] Only the broker persistence path accesses the queue database; another broker cannot take ownership of the same database/endpoint.
- [x] Positive readiness follows ownership, schema, database, and socket readiness, not mere process creation.
- [x] Real send/receive/ACK/reject preserve engine durability, receipts, body/headers, and delay metadata.
- [x] Malformed, truncated, oversized, or unsupported traffic fails explicitly with bounded memory use.
- [x] Slow or disconnected clients cannot hold a database transaction or block unrelated socket progress through unbounded output.
- [x] Lost confirmation and reconnect follow the approved contract, without treating unknown outcome as known failure.
- [x] Normal shutdown, cancellation, partial startup, and persistence-child failure clean up the complete owned tree while preserving the database.
- [x] Default logs contain no message payloads, serialized envelopes, or credentials.

## Validation and handoff

Test framing and bounds in process, then use the smallest real client/broker subprocess proof for ownership, readiness, persistence, disconnect, and teardown. Track descendants before harness cleanup so a cleanup helper cannot hide a product leak. Use positive events/barriers rather than elapsed-time races.

Document protocol versioning/errors, operational bounds, startup/stop behavior, and client recovery semantics. Failure coverage starts here; Task 07 is not permission to defer known safety failures.

## Completion record

- `bin/sqlite-queue broker` is a foreground Symfony Console command. Console is an optional CLI dependency; the broker API, engine, and PHP client remain Symfony-free.
- The broker uses the existing `Queue` engine, one session per socket, Amp socket backpressure, and Revolt signals. `Client` supports send, immediate receive, ACK, terminal reject, cancellation, and explicit close without automatic replay or reconnect.
- Protocol v1 separates bounded JSON control from opaque body and header bytes. It limits frames to 1,048,576 bytes, payloads to 1,040,000 bytes, control to 8,192 bytes, and accepted connections to 64. Each connection has one active request and one bounded reply.
- Database and endpoint ownership use private paths and nonblocking locks. Startup refuses existing endpoints, including stale sockets. Normal shutdown removes only the owned socket. Abnormal-exit recovery requires operator verification or a different endpoint.
- Readiness follows database, schema, and socket initialization. The driver owns its persistence process; the broker observes pipe EOF without joining the context a second time. Child output is drained without content logging.
- [Usage](../../docs/broker.md) and the [protocol reference](../../docs/broker-protocol.md) document API behavior, limits, errors, and recovery.
- `vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa` passes on PHP 8.5.10: 115 tests, 638 assertions. The 59 new cases cover framing, malformed traffic, no replay, cancellation, receipt fencing, delayed availability across restart, connection limits, slow readers, ownership conflicts, partial startup, and idle persistence-child death. Process tests assert observed descendants are gone before fallback cleanup.
- Independent review approved with no blockers. Notification waiting, Messenger integration, and broker performance acceptance remain Tasks 05, 06, and 08.

## Review follow-up (PR #4, after the record above)

The counts in the completion record above describe the Task 04 implementation at review time. They are historical, not current. The follow-up below changed the implementation again.

- `BrokerFactory::listen()` acquires the required resources and constructs a ready-to-run `Broker`. Startup failures release acquired steps in reverse order. `Persistence` holds a required `ProcessContext`, and `PersistenceFactory` tracks the handles it creates. `Ownership` permits a null socket identity before binding and after close, when there is no owned socket to remove.
- Shutdown runs under a five-second total watchdog that kills the owned persistence child directly through its process context. A repeated graceful connection close cannot interrupt a wedged worker, because the driver marks its connection closed before that close returns. The driver alone joins the context. A live regression test stops the idle worker, stops the broker, and proves bounded exit, released ownership, and restart recovery. The test fails before the fix and passes after it.
- Ownership uses Symfony `FlockStore` locks keyed by canonical database and endpoint paths. The store owns its sidecar files under the verified private resource directories. This package no longer names lock files, so no `<endpoint>.lock` claim remains. The database file is created mode 0600 or strictly validated, and the bound socket is mode 0600.
- Protocol operations are typed request objects and typed response objects with `Operation` and `ErrorCode` backed enums. `RequestCodec` and `ResponseCodec` validate untrusted frames at the boundary. `ProtocolException` carries a typed `ErrorCode`. The v1 wire format is unchanged, and uncertain outcomes still never replay.
- Runtime dependencies now include `symfony/lock:^8.0` and `symfony/filesystem:^8.0` for the broker. `symfony/console:^8.0` stays an optional CLI dependency. Engine and client code do not use Symfony components.
- `vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa` passes on PHP 8.5.10: 200 tests, 779 assertions. A separate readiness-based persistence probe fails when force-stop is disabled. The global code standards in `AGENTS.md` require justified nullability, fully initialized dependencies, typed protocol objects with enums, and named limits throughout the repository.
- Two validation runs timed out before the broker entered shutdown after a forwarded SIGTERM. The trigger remains unexplained. The regression now sends SIGTERM directly with `posix_kill`, matching the reported failure scenario, and saves process and signal state on timeout. Three consecutive broker-process suite runs and full QA passed afterward. No speculative signal-handler changes were retained in production.
