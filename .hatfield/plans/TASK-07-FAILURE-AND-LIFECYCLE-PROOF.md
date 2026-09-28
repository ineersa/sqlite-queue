# Task 07: cross-component failure and lifecycle proof

Status: TODO
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 06](TASK-06-MESSENGER-ADAPTER.md)
Read first: [PLAN.md](PLAN.md), sections 5–8 and 10–11.

## Goal

Verify the remaining cross-component failure contracts of the real engine, broker/client, and adapter. This is a final gap audit, not a separate testing phase that permits earlier tasks to omit their own proofs.

## Scope

Map existing test evidence to PLAN section 10 before adding cases. Reuse adequate proof and implement only missing cases or fixes for failures found.

Cover the agreed outcomes for:
- Commit completed but send/claim confirmation lost.
- Consumer exit after a possible external effect.
- Broker or persistence-child death while a handler or request is active.
- Stale receipts and competing broker startup after restart.
- Delayed work across restart, including overdue eligibility and earlier deadline rescheduling.
- Storage failures, including disk-full/I/O failure behavior.
- Slow/truncated/malformed socket traffic and pending waits during shutdown.
- Repeated start/stop and partial startup with explicit descendant ownership.

## Boundaries

Use the policies settled in Task 01. If they do not specify an observed failure, stop that change and obtain a decision. Do not add automatic recovery, short leases, request deduplication infrastructure, timeout increases, or compatibility fallbacks as an unapproved fix.

No hostile live-session experiments, content inspection, generic fuzzing platform, or production-only test hooks. Test failures must be reproduced in disposable owned resources.

## Acceptance criteria

- [ ] Each applicable PLAN failure contract points to concrete automated evidence and its actual tested boundary.
- [ ] No successful mutation reply precedes commit, follows rollback, or hides a storage failure.
- [ ] Uncertain deliveries and possible external effects follow the approved policy without silent redrive.
- [ ] Receipt/owner identity remains safe across disconnect and restart.
- [ ] Delays retain their original durable deadline and still notify consumers after recovery.
- [ ] Slow clients and failed persistence cannot leak transactions, locks, timers, buffers, or child processes.
- [ ] Every spawned resource has deterministic teardown and the tests detect product leaks before forced harness cleanup.
- [ ] No flaky timing-window assertions, arbitrary sleeps, or retries-until-green remain.
- [ ] Operational/error documentation reflects the verified behavior and does not claim exactly-once effects or untested power-loss guarantees.

## Validation and handoff

Run the relevant concurrent correctness lanes when concurrency is part of the contract. Use barriers and bounded process events, preserving diagnostics on failure. A solo green run cannot resolve a known contention flake.

Record the completed contract-to-test map, commands/results, and remaining limitations. Obtain independent review of failure semantics and implementation scope before performance acceptance in Task 08.
