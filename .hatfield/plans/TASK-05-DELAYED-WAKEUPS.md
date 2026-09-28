# Task 05: consumer notifications and delayed-message wakeups

Status: TODO
Repository: `/home/ineersa/projects/sqlite-queue`
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

- [ ] An idle consumer wakes for a committed immediate message without another client action.
- [ ] Waiter registration cannot miss work published between empty receive and waiting.
- [ ] A delayed message cannot be received early and wakes an idle consumer when due, without another publication.
- [ ] Positive subsecond deadlines are preserved; inserting an earlier deadline updates scheduling.
- [ ] Restart before a deadline preserves it; restart after it exposes overdue work without restarting the delay.
- [ ] Multiple consumers still reserve exclusively; notification never substitutes for a committed claim.
- [ ] Ready work in another queue is not blocked by a future or reserved message.
- [ ] Cancellation, disconnect, timeout, and shutdown leave no orphan waiters/timers or persistence children.
- [ ] Idle operation yields without a busy loop and does not require a second supervisor/service.

## Validation and handoff

Use controlled clocks and deterministic publication/registration barriers for scheduling correctness. Add bounded real-process evidence for delayed restart, idle delivery, and wait cancellation using actual protocol endpoints. No arbitrary sleeps to manufacture races or production test-only APIs.

Document waiting and deadline behavior, including wall-clock assumptions and limits. Hand Task 06 the actual client wait/cancellation API and its proof.
