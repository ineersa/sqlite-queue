# Task 07: cross-component failure and lifecycle proof

Status: IMPLEMENTED — pending independent review
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

## Audit evidence

Four cases fill the two missing cross-component boundaries. No production changes were needed.

| Contract | Automated evidence and tested boundary |
| --- | --- |
| Lost send/claim confirmation | `BrokerTest::testCommittedMutationSurvivesLostReply`, send and claim cases: gate the real broker reply after commit, inspect persisted state, cancel the client before releasing the reply. The failed client stays closed; one sent row survives; a lost claim stays reserved until controlled expiry and then redelivers. Existing `ClientTest::testServerCloseWithoutConfirmationFailsTheCallAndNeverReplays` and `TransportTest::testPeerFailureDoesNotReplaySend` cover wire/adapter no-replay. |
| Consumer exit after effect | `NativeConsoleProcessTest::testActiveHandlerDeathPreservesEffectAndUnsettledDelivery`, consumer case: native handler records an effect and signals a pipe barrier, consumer receives SIGKILL before returning, reservation survives until controlled expiry. Redelivery does not erase the effect. |
| Persistence death during handler | The same test's persistence case kills the recorded SQLite worker after the handler effect barrier. Broker exits nonzero and releases endpoint/descendants. Releasing the handler exposes failed ACK and nonzero native consumer exit. Reopening the broker preserves the reservation until its original expiry. |
| Commit ordering, serialization, rollback | `QueueTest::testNoConfirmationOrInterleavingBeforeCommit` and `testCommitFailureRollsBackAndReleasesOperation` cover all mutation variants with real SQLite and deterministic commit barriers/failures. |
| Atomic claim and receipt identity | `QueueTest::testConcurrentReceiversAcrossConnectionsCannotShareDelivery`, `testStaleForeignDisconnectedAndPreviousEpochReceipts`, and receipt-specific validation cases; `BrokerTest::testSpecificReceiptRejectionsPreserveTheSession` and transport receipt tests cover public propagation. |
| Storage failures and close | `QueueTest::testDatabaseFullLeavesConfirmedDataAndFailedOrUsableOwnership`, worker-death, initialization-close, in-flight close, and recursive-close tests; `BrokerTest::testStorageFailureStopsWithoutAPreBudgetErrorWrite`. FULL uses real SQLite's page limit and validates reopen consistency. |
| Durable delayed eligibility and notifier races | `QueueTest::testDelayedOrderingAndRestartPreserveOriginalDeadlines`; notifier registration, earlier deadline, cancellation, stale-query, and rebuild tests; `BrokerProcessTest::testControlledClockRestartPreservesFutureAndOverdueDeadlines`; `NativeIntegrationTest::testRestartPreservesDelayedDeliveryForNativeConsume`. |
| Socket limits and resource teardown | Frame truncation/bounds tests; broker malformed-session and gated slow-writer tests; process startup/conflicting-owner/shutdown cases; notifier timer cleanup; `BrokerProcessTest::testGracefulShutdownWithPendingWaitPreservesFutureMessage`. |
| Native Messenger failures/shutdown | `NativeConsoleProcessTest::testSigtermDuringWaitFlushesDeferredBatchAndAcksDurably`; native retry/decode-failure tests; configured Doctrine failure transport exhaustion and native retry recovery. |

### Validation

- `vendor/bin/castor test --filter='BrokerTest|NativeConsoleProcessTest'`: passed, 62 tests / 594 assertions; both full affected classes, not isolated new cases.
- `vendor/bin/castor cs:fix && vendor/bin/castor qa`: passed, 368 tests / 2,210 assertions; zero PHPStan errors.
- No broad experiments, production hooks, sleep-based correctness assertions, reviewer launch, or benchmark run.

### Limits

The effect is an observable file write, not proof of an external service transaction or exactly-once behavior. Consumer death and persistence-child death are tested; abrupt broker SIGKILL during a handler is not separately duplicated. Existing receipt fencing tests cover prior epoch/owner identity. Disk-full evidence uses SQLite `max_page_count`, not physical disk exhaustion or arbitrary OS I/O faults. Power-loss guarantees and the historical SIGSTOP-worker signal caveat remain outside this proof. Existing graceful stop/restart and startup rollback cases are reused rather than adding a timing-based stress loop. Independent review remains pending before performance acceptance.
