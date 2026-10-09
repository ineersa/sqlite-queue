# Task 09: notification waits across multiple queues

Status: Implemented; pending user review.
Dependencies: Task 05 notification handling and Task 06 native Messenger integration.
Requested during Task 08. Creating this task does not authorize changing the ongoing benchmark method or implementing it in the benchmark branch.

## Goal

Allow one consumer to wait until any queue in a selected set has eligible work. Native `messenger:consume queue_a queue_b` must not need idle polling when both receivers use the same broker.

This is an API limitation, not a SQLite table limitation. Multiple consumers can already wait independently on the same or different queues. The missing operation waits across several queues on one notification connection.

## Scope

- Add a bounded multi-queue readiness request to the protocol and PHP client. Keep single-queue WAIT usable and define compatibility with peers that do not support the new operation.
- Validate every queue name, the queue count, duplicates, empty input, and the requested timeout. Define limits within existing frame/control bounds. Reject invalid input instead of silently truncating it.
- Register interest in all selected queues before checking readiness. Wake when any has eligible work, including immediate publication, delayed eligibility, and reservation expiry.
- Return a readiness hint, not a reservation. Keep claims, queue selection order, and receipt ownership unchanged.
- Remove all registrations when any queue wakes the request, the timeout expires, the client disconnects, or cancellation/shutdown occurs. Share existing per-queue timers and persisted deadline queries.
- Extend native Messenger idle integration to the receiver set actually selected by the command, including regex and --all selection where supported. Preserve Messenger's queue priority/order and unrelated receiver behavior.
- Use one separate notification connection per participating broker, not one connection per queue or a SQLite connection per consumer. Retain the original operation connections for final batch ACKs.
- For receivers from different brokers or mixed transport types, retain a documented bounded fallback unless cross-broker waiting is separately approved. Do not silently turn an unsupported --sleep=0 selection into busy polling.
- Update benchmark pickup modes after implementation. Preserve older polling captures and identify the method/configuration change explicitly.

## Boundaries

No custom consume command or Worker replacement. No atomic multi-queue receive, schema redesign, priority scheduler, broadcast payload system, receipt rebinding, or replay of uncertain operations. Reuse the existing notifier instead of introducing a second notification service.

## Acceptance criteria

- [x] Waiting on queues A and B wakes for publication to either queue without an extra idle sleep.
- [x] A pre-existing ready message and publication during registration/recheck cannot be missed.
- [x] Delayed messages and expired reservations wake the wait without another publication, including after restart.
- [x] Overlapping waits from multiple consumers remain safe. A hint can lose the claim race without losing future wakeups.
- [x] Completion removes registrations from every queue, with no leaked timers, reads, or futures.
- [x] Cancellation during notification acquisition or WAIT preserves connections needed for batch settlement.
- [x] Native multi-receiver consumption handles both queues, preserves native selection order, and respects shutdown and worker limits.
- [x] Single-queue callers remain compatible; unsupported peers and malformed inputs fail explicitly.
- [x] Tests use controlled clocks and observable barriers rather than sleeps or elapsed-time thresholds.

## Validation

Run focused notifier, protocol/client, and native Messenger tests, then Castor formatting and QA. Verify both supported Symfony 8.0 and 8.1 selection behavior. Run an explicit multi-queue benchmark smoke check and label notification mode in its report; do not compare it silently with earlier polling captures.

Validation completed:

- Castor formatting and full QA pass: 415 tests, 2,401 assertions, 25.69 seconds.
- Isolated Symfony 8.0.15 native integration and subscriber checks pass: 30 tests, 215 assertions. Dependency downgrades were not committed.
- `vendor/bin/castor bench --smoke --workload=multi-queue` passes both backends with complete accounting and process cleanup. Broker pickup mode is `notification-wait-any`; Doctrine uses polling. Historical captures remain unchanged.
- Independent review approves the implementation and follow-up fixes. Cross-broker waits remain outside scope.
