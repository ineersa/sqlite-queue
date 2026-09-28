# Task 06: Symfony Messenger transport adapter

Status: TODO
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 05](TASK-05-DELAYED-WAKEUPS.md)
Read first: [PLAN.md](PLAN.md), sections 5–8 and 10.

## Goal

Make the standalone broker usable through Symfony Messenger without replacing Messenger's bus, worker, handlers, or explicit retry policy.

## Scope

- Implement the supported transport/factory and relevant contracts using Symfony facilities and the real PHP client.
- Preserve configured serialized body/headers, retry information, and transport identity. Keep application deserialization in the adapter/consumer boundary, not the broker.
- Map receive to a committed reservation and carry its delivery receipt through ACK/reject.
- Map DelayStamp without truncating positive subsecond delays or recomputing deadlines during reconnect/restart.
- Keep reject terminal. Messenger's retry listener/backoff must not compete with a second broker retry loop.
- Handle decode failures and ambiguous transport failures according to the approved contracts rather than leaving stuck invisible claims or silently repeating work.
- Integrate bounded notification waits with the real worker's idle lifecycle, remaining sleep, shutdown, and limits.
- Add a standalone Messenger configuration example and adapter documentation.

## Boundaries

No consuming-application imports/configuration, application supervision, changes to unrelated transports, custom Doctrine connection shim, or broker job runner. Batch receive remains deferred unless a newly approved supported framework contract genuinely requires it.

The engine must remain usable without booting a Symfony application. Choose the smallest actual dependency/API boundary, not a speculative multi-framework adapter layer.

## Acceptance criteria

- [ ] A normal Symfony Messenger worker sends, receives, handles, and ACKs through the actual broker adapter.
- [ ] Serializer body/headers and retry/delay stamps round-trip without corruption or silent loss.
- [ ] Stale/foreign receipts cannot ACK/reject another reservation through the adapter.
- [ ] Delayed messages wake idle workers and retain availability across broker restart.
- [ ] Handler failure/retry/reject follows the documented Messenger mapping without duplicate retry mechanisms.
- [ ] Decode and transport failures produce the supported visible failure/cleanup behavior.
- [ ] A notification is not followed by an unnecessary full/remainder idle sleep; waiting still respects cancellation, shutdown, and worker limits without busy spinning.
- [ ] The documented example and tests run in this package without a consuming application installed.

## Validation and handoff

Use focused adapter tests plus real Messenger worker/broker integration, not only manually invoked listeners or a fake transport. Assert the specific envelope/delivery behavior, not handler prose. Verify delayed retry mapping and cancellation at their actual boundaries.

Provide the concrete adapter configuration to Tasks 07 and 08. Keep task-local tests green before the broader failure proof.
