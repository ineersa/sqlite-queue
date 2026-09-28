# Task 03: durable async SQLite queue engine

Status: DONE, storage engine implemented; broker and notifications remain out of scope
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 01](TASK-01-SETUP-AND-CONTRACTS.md)
Read first: [PLAN.md](PLAN.md), sections 4–6 and 10–11.

## Goal

Implement the queue engine on one async SQLite connection. Durable delayed availability is part of this engine from the start, not a later schema retrofit.

## Scope

- Implement one message table for named queues using the approved minimal schema and indexes.
- Implement durable send, immediate receive, ACK, and terminal reject with lossless opaque body/headers.
- Store the original availability deadline at send time and exclude future messages from receive. Preserve deadlines across close/reopen.
- Serialize whole storage operations and transactions across async waits. Use a supported atomic-claim operation rather than copying unsupported `RETURNING` SQL.
- Implement delivery receipts/reservation metadata and the approved visibility/redelivery policy. Prevent stale or foreign ACK/reject from mutating another delivery.
- Confirm operations only after commit. Propagate storage errors and release operation/transaction ownership on all paths.
- Document initialization and durability settings. Keep queue identity independent of filesystem path or application identity.

## Boundaries

No socket server, worker execution, priorities, batch APIs, detailed stats, dead-letter administration, extra drivers, or in-memory queue mirror. Broker notifications arrive in Task 05, but delayed persistence and receive eligibility must work here.

Task 02 need not block correctness implementation, but its baseline must be fixed before performance tuning. Do not change delivery policy to make an engine test pass.

## Acceptance criteria

- [x] Named queues share one table and remain isolated without per-queue migrations.
- [x] Empty and binary messages plus headers round-trip correctly.
- [x] Real concurrent calls cannot acquire the same current delivery or interleave another request inside an active transaction.
- [x] Send/claim/ACK/reject cannot report success before commit or after rollback.
- [x] Receipt validation follows the approved contract, including stale/foreign reservations and restart implications.
- [x] Positive subsecond delays are not made immediate; ready/delayed ordering and eligibility follow the documented contract.
- [x] Closing/reopening storage preserves future deadlines and makes overdue work eligible without restarting its delay.
- [x] Persistence errors are visible and leave consistent state and usable or explicitly failed ownership, not a stuck mutex or leaked transaction.
- [x] The engine has no Symfony application-kernel or consuming-application dependency.

## Validation and handoff

Use actual file-backed SQLite for persistence, transaction, and concurrency proof. Control eligibility clocks and concurrency with deterministic facilities, not sleeps. Include rollback/storage failure, separate queues, stale receipts, and persistence-worker cleanup. Do not treat mocked storage as durability evidence.

Document the actual schema/query behavior, receipt contract, public engine API, and focused validation commands. Task 04 consumes this implementation rather than duplicating queue SQL.

## Completion record

- `Ineersa\SqliteQueue\Queue` owns one WAL/FULL async connection and serializes complete operations. `Delivery` contains opaque bytes and an opaque fenced receipt; `InvalidReceipt` reports stale, foreign, expired, or disconnected ownership.
- The engine persists original millisecond availability and visibility expiry. Restart creates a new epoch without resetting deadlines or releasing reservations. Terminal ACK/reject deletes only the matching current delivery.
- The [engine reference](../../docs/queue-engine.md) records initialization, schema, query plan, public API, timing, error handling, and Task 04 integration responsibilities.
- `vendor/bin/castor cs:fix` and `vendor/bin/castor qa` pass. The full suite has 52 tests and 264 assertions. The 19 new engine cases use file-backed SQLite, controlled clocks, and commit barriers rather than sleeps.
- Failure evidence includes real deferred-constraint commit errors for send/claim/ACK/reject, a zero-row conditional claim, real `SQLITE_FULL` under a fixture page limit, and an owned persistence-worker kill. Teardown checks for surviving persistence processes.
- Independent code review approved the implementation. The original baseline archive is unchanged; no performance claim is made.

Task 04 must give each socket connection a new engine session, invalidate it on disconnect, map storage errors without replay, and enforce exclusive broker/database ownership. Task 05 still owes deadline scheduling and race-free notifications. No blocker remains within Task 03.
