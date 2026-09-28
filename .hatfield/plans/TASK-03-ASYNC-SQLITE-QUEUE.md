# Task 03: durable async SQLite queue engine

Status: TODO
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

- [ ] Named queues share one table and remain isolated without per-queue migrations.
- [ ] Empty and binary messages plus headers round-trip correctly.
- [ ] Real concurrent calls cannot acquire the same current delivery or interleave another request inside an active transaction.
- [ ] Send/claim/ACK/reject cannot report success before commit or after rollback.
- [ ] Receipt validation follows the approved contract, including stale/foreign reservations and restart implications.
- [ ] Positive subsecond delays are not made immediate; ready/delayed ordering and eligibility follow the documented contract.
- [ ] Closing/reopening storage preserves future deadlines and makes overdue work eligible without restarting its delay.
- [ ] Persistence errors are visible and leave consistent state and usable or explicitly failed ownership, not a stuck mutex or leaked transaction.
- [ ] The engine has no Symfony application-kernel or consuming-application dependency.

## Validation and handoff

Use actual file-backed SQLite for persistence, transaction, and concurrency proof. Control eligibility clocks and concurrency with deterministic facilities, not sleeps. Include rollback/storage failure, separate queues, stale receipts, and persistence-worker cleanup. Do not treat mocked storage as durability evidence.

Document the actual schema/query behavior, receipt contract, public engine API, and focused validation commands. Task 04 consumes this implementation rather than duplicating queue SQL.
