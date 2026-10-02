# Task 05: consumer notifications and delayed-message wakeups

Status: IMPLEMENTED; PR #6 open for review
Repository: `/home/ineersa/projects/sqlite-queue`
Branch: `task-05-delayed-wakeups`
Dependencies: [Task 04](TASK-04-BROKER-AND-CLIENT.md)
Read first: [PLAN.md](PLAN.md), sections 5.5, 7, and 10.

## Goal

Complete the mandatory event-driven delivery path, including delayed messages becoming eligible on an otherwise idle queue and after restart.

## Scope

- Implement bounded, cancellable waiting through the real client/protocol and Revolt loop.
- Coordinate an empty receive and waiter registration so a concurrent committed publication cannot be missed.
- Wake appropriate consumers after committed sends and at persisted availability deadlines. A wakeup must not grant a reservation.
- Maintain minimal derived waiter/deadline state rather than mirroring all queue bodies or scanning every queue continuously.
- Reschedule when a newly inserted deadline is earlier; avoid due-but-claimed work creating a timer/busy-loop problem.
- Rebuild delayed scheduling after restart. Already-overdue work is eligible; future work retains its original deadline.
- Resolve waits and remove timers/resources during timeout, cancellation, disconnect, shutdown, or broker failure.

## Boundaries

Delayed storage was required in Task 03. This task adds notification and scheduling, not a new interpretation of delay. No priorities, generic scheduler framework, batch receive, or speculative multi-queue subscriptions.

Do not imitate notifications with an unbounded high-frequency database polling loop. Do not hold a transaction while waiting for a deadline or consumer.

## Acceptance criteria

- [x] An idle consumer wakes for a committed immediate message without another client action.
- [x] Waiter registration cannot miss work published between empty receive and waiting.
- [x] A delayed message cannot be received early and wakes an idle consumer when due, without another publication.
- [x] Positive subsecond deadlines are preserved; inserting an earlier deadline updates scheduling.
- [x] Restart before a deadline preserves it; restart after it exposes overdue work without restarting the delay.
- [x] Multiple consumers still reserve exclusively; notification never substitutes for a committed claim.
- [x] Ready work in another queue is not blocked by a future or reserved message.
- [x] Cancellation, disconnect, timeout, and shutdown leave no orphan waiters/timers or persistence children.
- [x] Idle operation yields without a busy loop and does not require a second supervisor/service.

## Implementation notes

Public wait API:

- `Client::wait(string $queue, int $timeoutMilliseconds, ?Cancellation $cancellation = null): bool`
- Bounds `0..30_000`. Zero is an immediate readiness probe.
- `true` is only a receive hint. `false` is a normal timeout or empty probe.
- No reservation. One outstanding exchange.
- Invalid local bounds raise before any write and preserve the request sequence.
- The exchange deadline adds the client's transport allowance to the requested wait.
- Cancellation during WAIT invalidates the connection with no replay.

Protocol:

- Operation `wait` with `queue` and `wait_ms`.
- Payload-free request and boolean result.
- WAIT-specific validation. Pipelined bytes are rejected.
- Dedicated socket monitor is cancelled and awaited before the reply.
- Normal timeout leaves the session reusable.
- Immediate `receive` stays immediate. `receive` plus `wait_ms` remains invalid.

Scheduling:

- Earliest effective eligibility is `max(available_at, coalesce(reserved_until, available_at))`, indexed by `queue_messages_ready`.
- Per-watched-queue timers. Lazy rebuild after restart or first watch.
- Visibility deadlines and dirty rechecks against committed changes.
- Post-commit hints after send and claim. No body mirror and no DB polling loop.
- Watch identity is object identity. Stale in-flight queries cannot re-arm a replaced or idle watch.
- Timeout, cancellation, disconnect, and shutdown clear waiters and timers.

Clock assumptions:

- Persisted availability and visibility use wall-clock milliseconds.
- WAIT bounds and deadline delays use Revolt duration timers.
- Normal forward wall-clock progression is assumed when converting a deadline to a duration.
- A backward jump can postpone eligibility and force rearming.
- A forward jump can make work eligible on recheck, but an already armed monotonic timer is not moved earlier, so a wake hint may be late until another recheck, mutation, or WAIT.
- No realtime or latency guarantee. No separate scheduler service.

## Validation and handoff

Proof shape in this branch:

- Real `bin/sqlite-queue` process tests cover idle publication wakeups and a real 200 ms delayed timer without another publication.
- The controlled-clock subprocess fixture `tests/Broker/Fixtures/broker-controlled-clock-probe.php` drives a real `Broker`/`Client`/socket protocol for exact before/after restart, deadline preservation, registered-wait cancellation/disconnect, and shutdown with a pending WAIT. Fixture clock injection is not a CLI option.
- In-process barriers in `QueueNotifierTest` and `BrokerTest` prove stale query replacement, missed-registration protection, earlier-deadline reschedule, visibility scheduling, exclusive claims after wake, zero-duration reuse, and pipelined-byte rejection.

Document waiting and deadline behavior in `docs/broker.md`, `docs/broker-protocol.md`, `docs/queue-engine.md`, and `docs/contracts.md`. Hand Task 06 the actual client wait/cancellation API and worker-idle integration obligation. Do not claim Messenger implementation.

PR #6 review follow-ups:

- Notifier maps use prefixed string keys. Watches retain the original `QueueName` for storage queries. Regressions cover an active WAIT on `"1"` followed by notifier close and broker shutdown, with `"01"` remaining a distinct queue.
- Controlled-clock helpers disable captured timers before returning their IDs, including before the subprocess control reply. Assertions verify disabled state and explicit firing. Production timer scheduling and real CLI timer tests are unchanged.
- `vendor/bin/castor cs:fix` and `vendor/bin/castor qa` pass after these fixes: 282 tests, 1,763 assertions on PHP 8.5.10. These are local Castor results, not attached GitHub CI checks. PR review remains open.

The Task 04 SIGSTOP-worker shutdown caveat remains intact and outside this task's release claims.
