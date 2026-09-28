# Task 04: foreground broker, socket protocol, and PHP client

Status: TODO
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

- [ ] Independent PHP processes communicate with the broker through the real client without booting a consuming application.
- [ ] Only the broker persistence path accesses the queue database; another broker cannot take ownership of the same database/endpoint.
- [ ] Positive readiness follows ownership, schema, database, and socket readiness, not mere process creation.
- [ ] Real send/receive/ACK/reject preserve engine durability, receipts, body/headers, and delay metadata.
- [ ] Malformed, truncated, oversized, or unsupported traffic fails explicitly with bounded memory use.
- [ ] Slow or disconnected clients cannot hold a database transaction or block unrelated socket progress through unbounded output.
- [ ] Lost confirmation and reconnect follow the approved contract, without treating unknown outcome as known failure.
- [ ] Normal shutdown, cancellation, partial startup, and persistence-child failure clean up the complete owned tree while preserving the database.
- [ ] Default logs contain no message payloads, serialized envelopes, or credentials.

## Validation and handoff

Test framing and bounds in process, then use the smallest real client/broker subprocess proof for ownership, readiness, persistence, disconnect, and teardown. Track descendants before harness cleanup so a cleanup helper cannot hide a product leak. Use positive events/barriers rather than elapsed-time races.

Document protocol versioning/errors, operational bounds, startup/stop behavior, and client recovery semantics. Failure coverage starts here; Task 07 is not permission to defer known safety failures.
