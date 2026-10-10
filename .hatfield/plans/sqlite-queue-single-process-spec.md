# SQLite queue: single-process broker with local PDO

**Status:** Implementation specification for a separate single-process experiment. Not a performance result or release approval.  
**Date:** 8 October 2026.  
**Repository:** `ineersa/sqlite-queue`.  
**Implementation base:** `d37c9ab00290324578ae78c0127b307b3433ed88`, the `main` base reported by PR #10 when inspected.  
**Comparison reference:** PR #10, `87164c72fd59bdf8966a9302b5e8f9e0632d08ec`, the depth-four worker-pipelining POC.  
**Suggested branch:** `experiment/single-process`.  
**Suggested repository path:** `.hatfield/plans/single-process-broker-spec.md`.

## 1. Decision and goal

Run the broker's sockets, notification coordinator, queue policy, and existing PDO storage in **one PHP process using one Revolt event loop**. Remove the internal persistence worker entirely. Keep the client-to-broker Unix-socket protocol.

```text
Application processes                         One broker process
  Messenger publishers/consumers              Revolt event loop
           │                                    ├─ client sessions and socket I/O
           └──── existing Unix socket ──────────┤
                                                ├─ WAIT notifications and timers
                                                └─ Broker → Queue → SqliteQueueStorage
                                                                      │
                                                               one PDO connection
                                                                      │
                                                               existing SQLite file
```

This is an execution-boundary change, not a queue-storage redesign. `Broker` continues coordinating the runtime; `Queue` continues owning policy; `SqliteQueueStorage` continues owning SQL, transactions, prepared statements, and the connection. Do not combine these classes into a single large class. The existing local `Queue` already supports synchronous storage and pre-operation cancellation. [R1–R3]

**Accepted trade-off:** synchronous PDO work pauses this process's event loop. The new implementation loses isolation from database stalls in exchange for fewer process boundaries and less coordination code. Fibers do not make synchronous PDO nonblocking. [E1]

The supplied attribution report supports investigating this boundary, but does not establish a single-process speedup or a bound on database pauses. Its exchange-minus-worker aggregate includes scheduling and transfer, not serialization alone. PR #10's positive pipelining measurements are a separate result. [D1, R1]

Requirements below are proposed implementation decisions. References identify the inspected baseline or supporting external documentation; they are not claims that the proposed implementation already exists.

## 2. Branch scope and non-goals

Create a sibling experiment from the pinned implementation base. Do not merge PR #10 merely to remove its new pipeline. Preserve its branch for comparison. The selected base already contains the PDO rewrite and the `23c41b1` claim simplification. If implementation starts from a newer revision, record its SHA and inspect intervening changes rather than silently substituting it.

The finished branch has **one storage execution mode**: in-process PDO. Do not retain a selectable worker backend, dormant pipeline, compatibility proxy, or worker feature flag. Existing NORMAL/FULL durability selection remains supported.

Do not introduce:

- An in-memory command queue, per-database-operation future, mutex, scheduler, or “local worker” imitating the removed IPC layer.
- Message prefetch, batching, group commit, new SQL, new indexes, a schema migration, a second journal, or another production SQLite connection.
- Automatic replay/retry, worker fallback, threads, an extension-based asynchronous SQLite replacement, or a new process supervisor.
- Changes to Messenger dispatch, public frame format, receipt format, notification semantics, or benchmark workloads beyond necessary topology/accounting adaptation.

A cooperative checkpoint between completed **public requests** is required in §6. It is not a replacement command queue or a polling delay.

## 3. Component and deletion map

### Retain and rewire

| Component | Target responsibility/change |
| --- | --- |
| `BrokerFactory` | Acquire locks and paths, open local PDO storage, construct local `Queue`, bind the socket, transfer initialized resources to `Broker`. |
| `Broker` | Call typed `Queue` methods directly. Own runtime shutdown and storage disposal. No SQL or worker protocol. |
| `Queue` | Retain message policy, epoch, receipt tokens, cancellation checks, transaction-time clocks, and domain exceptions. Restore necessary validation formerly performed only by the proxy. |
| `SqliteQueueStorage` | Keep the six prepared statements, current claim SQL, transaction wrapper, configuration readback, and connection ownership. |
| `QueueNotifier` | Query the local queue. Remove worker-admission/capacity waiting; retain register-before-recheck and deadline-based WAIT behavior. |
| `BrokerCommand` and bundle wiring | Remove worker-factory injection; continue configuring paths, redelivery, clock, and durability. |
| Client, protocol, Messenger adapter | Retain public behavior and bounds. Only update documentation/tests that assume a persistence child exists. |

A direct constructor arrangement is sufficient:

```text
Broker(server, queue, storage, lifetimeLocks, socketIdentity, clock)
```

`Queue` borrows the same storage object that `Broker` closes. Storage alone owns PDO; no second connection is opened to verify readiness.

### Remove the obsolete implementation

Delete the worker proxy/runtime and their now-unused support:

```text
src/Sqlite/SqliteQueueWorker.php
src/Sqlite/SqliteWorkerContextFactory.php
src/Sqlite/SqliteWorkerHandle.php
src/Sqlite/SqliteWorkerRuntime.php
src/Sqlite/SqliteWorkerOperationEnum.php
src/Sqlite/SqliteWorkerStatusEnum.php
src/Sqlite/SqliteWorkerDiagnostic.php       # when no surviving consumer needs it
src/Sqlite/worker.php
```

If applying onto the pipelining branch instead, also remove `SqliteWorkerRequestEntry`, `SqliteWorkerLifecycleEnum`, the FIFO, credits, response reader, writer, and internal ID/deadline machinery. Do not port these into `Broker`.

Remove `StorageCapacityException` and `StorageFailureException` if their remaining uses concern only the deleted worker layer. Existing queue/domain exceptions and PDO failures suffice for the local path; do not add a new error hierarchy solely to preserve obsolete names. Retain small diagnostic sanitization code only where a surviving output boundary actually needs it.

Remove the direct `amphp/parallel` dependency once production and retained tests no longer use it. Keep Amp, Revolt, socket support, and dependencies still used elsewhere, including synchronization facilities used by client code. Keep `ext-pdo_sqlite`. Development `ext-sqlite3` is not automatically obsolete: existing audits or tests may still use it. Update the lock file without unrelated dependency upgrades. [R8]

## 4. Storage, durability, and validation invariants

### 4.1 Preserve storage behavior

Preserve the database schema and the current operation sequences:

| Operation | Required local behavior |
| --- | --- |
| Send | Begin IMMEDIATE, sample availability, INSERT, obtain identity, COMMIT, return identity. |
| Claim | Begin IMMEDIATE, sample time/expiry, SELECT ID plus payload, conditional reservation UPDATE, verify row count, validate payload, COMMIT, return delivery. |
| Empty or zero-row claim | ROLLBACK and return null. Retain zero-row precedence over selected-payload validation. |
| ACK/reject | Begin IMMEDIATE, sample time, fenced DELETE; diagnose a zero-row result in the same transaction; preserve existing settlement outcome/error behavior. Reject remains deletion, not requeue. |
| Earliest eligibility | Existing indexed read; return a scheduling hint, not a reservation. |

Return successful mutations only after commit. Preserve conditional predicates and failure precedence: no active reservation, owner, epoch, token, then expiry. Preserve ID ordering, queue isolation, visibility expiry, and receipt fencing. Keep the current binary body/header representation and LOB bindings. [R2, R4]

Keep the same production settings for the experiment:

```text
PHP:                  existing ^8.5 requirement
Connection:           one Pdo\Sqlite instance
Journal:              WAL
Synchronous:          NORMAL default; FULL explicitly selectable
Transaction mode:     Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE
busy_timeout:         5,000 ms
wal_autocheckpoint:    1,000 pages
foreign_keys:         ON
trusted_schema:       OFF
Prepared statements:  six, reused
```

Keep PDO's native transaction API and its transaction-mode attribute; do not replace it with manually mixed transaction-control mechanisms. Preserve exception mode, fetch/type behavior, rollback handling, and configuration readback. [R4, E3]

**Do not lower `busy_timeout`, disable checkpoints, or weaken synchronous mode to improve the first comparison.** A shorter lock timeout would not bound every filesystem delay and would change failure behavior. Any later tuning is a separate experiment. [E4]

### 4.2 Preserve validation when deleting IPC

Audit the removed proxy and child boundary before deleting their checks. Separate genuine queue preconditions from validation of serialized internal replies.

Keep or move genuine preconditions into the appropriate surviving boundary: public frame checks in the protocol; `QueueName` validation in the value object; body-plus-headers size checks and non-empty owner checks in `Queue` where needed. In particular, the local `Queue::send()` must not become an unbounded route around the proxy's former payload limit. Retain delay validation, timestamp overflow checks, malformed receipt rejection, and validation of stored data before delivery. [R2, R9]

Delete validation that exists only to parse a worker response: internal response IDs, operation/status arrays, serialized-result reconstruction, and internal error envelopes. A typed local return is not an untrusted IPC response. Do not recreate that protocol in arrays between PHP objects.

Maintain client-facing error distinctions. A malformed request or invalid receipt must not kill the broker merely because there is no worker to classify it. Conversely, unexpected PDO, storage-corruption, or invariant failures remain fatal to the serving broker. Stop accepting work and close sessions; do not convert fatal storage errors to an empty receive or automatically retry them. [R3]

### 4.3 Clocks

Use one injected non-suspending wall-clock function for local queue policy and notifier wall-time calculations. Keep absolute queue deadlines in Unix milliseconds and duration/deadline accounting monotonic where already applicable.

Sample send availability, claim expiry, and settlement time **after transaction acquisition**, as today. Do not move these samples to socket-read time. Retain existing expiry comparisons and token generation behavior. The factory's injected clock will now govern both policy and notifier time; update tests and parameter documentation, which currently describe a parent-only clock. [R2, R5]

A clock or validation callback called inside a transaction must never suspend, pump the event loop, perform socket I/O, or call back into the broker. Controlled clocks in tests must observe the same rule.

## 5. Direct execution and cancellation

Replace calls to `$this->worker->send/receive/acknowledge/reject/earliestEligibility()` with the corresponding local `Queue` methods. Retain stopping checks at the dispatch boundary and pass the existing session-lifetime cancellation where relevant.

The new execution boundary is **entry into the local queue operation**, not a send on an internal channel:

| Stage | Required behavior |
| --- | --- |
| Cancellation/stopping observed before operation starts | Do not begin storage work. End the session with existing cancellation behavior. |
| Operation has started | Complete it synchronously, committing or rolling back according to SQL outcome. Do not add a mid-transaction parent cancellation check. |
| Commit succeeds | Preserve the known result even if shutdown/disconnect is observed afterward. |
| Client response cannot be delivered | Do not undo/replay the committed operation. The client may have an unknown outcome. |
| Claim committed but client cannot receive it | Keep the reservation until its original expiry; do not release or extend it merely because delivery failed. |

There is no concurrent callback execution during the non-suspending local operation. A peer can nevertheless disconnect at the OS level while PDO is running, and the broker may only discover it afterward. Do not claim immediate disconnect detection.

For sends and successful claims, apply the existing post-commit notifier action **before any optional cooperative yield** and before returning/writing success. Retain the claim's post-operation lifetime check before delivering its payload. If shutdown has already closed the notifier, its normal no-op behavior applies; the committed database state remains authoritative.

Across sessions, execution order is the order in which the event loop invokes the guarded local operations. Preserve per-session request order. The deleted worker FIFO no longer defines cross-session admission order; do not promise a global arrival-order guarantee or create a queue to simulate it. Queue-message selection still follows the existing SQL ordering.

## 6. Event-loop safety and fairness

### 6.1 Transactions never suspend

From entering storage through commit/rollback and statement cleanup, execution stays synchronous. Do not call `Amp\async()`, `Future::await()`, `Amp\delay()`, `Suspension::suspend()`, socket methods, or event-loop re-entry inside that path.

A database mutex is unnecessary under this invariant: no other fiber can use the connection until the running fiber yields. This does not remove SQLite's transaction or lock requirements against other processes. [E1, E5]

Do not execute database work from a native asynchronous signal handler. Continue using Revolt signal callbacks, which act between synchronous operations. Handler/business execution remains in the external Messenger consumers, not in the broker.

### 6.2 Yield between completed public requests

Removing worker awaits can remove the only suspension from a fast request path. Buffered socket reads/writes must not let one active session run indefinitely without returning control.

For the first experiment, require **one cooperative checkpoint after each completed request/response and before the next request in that session**. Apply it to recoverable error responses and handshakes too. Drop large request/response references before yielding. A `WAIT` must still suspend through its existing notification mechanism rather than poll.

Use a small private helper built from `EventLoop::defer()` and the current fiber's suspension, or an equivalent established Amp primitive with verified next-iteration behavior. `defer()` runs on the next iteration; do not substitute an endlessly chained microtask queue. No positive sleep interval is added. [E2]

Illustrative helper, not a new service:

```php
private function yieldToEventLoop(): void
{
    $suspension = EventLoop::getSuspension();
    $callback = EventLoop::defer(static function () use ($suspension): void {
        $suspension->resume();
    });
    try {
        $suspension->suspend();
    } finally {
        EventLoop::cancel($callback);
    }
}
```

Skip the checkpoint when the session is already terminating. Recheck stopping/cancellation before starting another operation after any suspension. Verify watcher cleanup against the locked Revolt version when implementing the helper.

Keep notifier refresh work coalesced per watched queue. Each scheduled refresh performs its local read and returns; it must not drain arbitrary work in a tight loop. Retain the existing once-per-second signal-dispatch workaround in `BrokerCommand`: it is not a persistence-worker heartbeat and its rationale is independent of removing the worker. Reword it so it does not promise progress while PDO is blocked. [R6]

This fairness rule limits uninterrupted application work between requests. It **cannot preempt one slow PDO call**, and is not a millisecond responsiveness guarantee.

## 7. WAIT and notification simplification

Inject the local `Queue` into `QueueNotifier`, or retain one clearly typed eligibility callable if necessary for existing tests. Remove the worker `awaitCapacity` dependency, `StorageCapacityException` retry path, and internal capacity notification machinery. Do not replace them with a no-op capacity implementation.

Keep these behaviors intact:

- Register the waiter before scheduling/rechecking readiness, so a committed send between an empty receive and WAIT is not lost.
- Use persisted availability/visibility deadlines; WAIT success remains a hint to call receive.
- Preserve zero-duration probes, positive WAIT duration handling, cancellation, and delayed/expired-reservation wakeups.
- Coalesce scheduled refreshes and discard obsolete watch/timer callbacks. Remove timers when the last waiter detaches. Preserve numeric queue names without array-key coercion.

The eligibility query is now synchronous. Remove scaffolding used exclusively to wait for worker capacity, but do not redesign the notifier's wakeup policy in this experiment. Late timers caused by blocked PDO are a documented limitation, not a reason to introduce periodic database polling. Baseline watch/capacity behavior is documented in [R7].

## 8. Memory bounds and slow clients

Remove the internal admission counters, in-flight windows, pending result maps, and pipeline byte credits. There are no worker requests or replies to account for.

Retain public limits: at most 64 live client connections, the existing frame/control limits, and the existing 1,040,000-byte combined body/header limit. Process at most one request and its response at a time per session. Do not pre-read a list of commands or retain completed responses in an unbounded output queue.

Await each bounded socket write before that session processes its next request. A slow reader can suspend its own session outside the transaction; other clients must still make progress between local SQL operations. No database transaction may remain open while a response is written or WAIT is suspended.

Explicitly release completed request/delivery/frame references before parking on the next read or fairness checkpoint. Continue clearing retained LOB parameter values and closing statement cursors. Bound notifier state by live waiters/watches, not historical queue names.

These are logical buffering limits, not an exact RSS cap: PHP object, stream, encoding, and kernel buffers add overhead. Do not retain obsolete worker credits as a substitute for checking real ownership of payloads.

## 9. Startup, shutdown, and failure contracts

### 9.1 Startup

`BrokerFactory::create()` must:

1. Validate configuration and extension support; acquire the existing database/endpoint lifetime locks and private-file protections.
2. Open/configure local `SqliteQueueStorage`, prepare statements, and read configuration from that actual connection.
3. Construct `Queue` with the configured visibility timeout and shared clock; construct/bind the server using existing private endpoint handling.
4. Check cooperative cancellation between stages and before handing over readiness. Allow one safe event-loop checkpoint before readiness so queued signal callbacks can be observed; do not yield while any transaction is open.
5. On failure, release acquired resources in reverse ownership order. Close storage before releasing lifetime locks. Preserve the original error while attempting the remaining safe cleanup.

The returned broker must be fully initialized. Do not introduce nullable half-built runtime dependencies. Remove worker creation hooks, bootstrap IPC, worker PID discovery, pipe drains, and the worker-specific 15-second startup and five-second failed-worker-release machinery. Factory cancellation remains cooperative; it is not able to interrupt a blocked local PDO initialization.

### 9.2 Shutdown

Keep `stop()` idempotent. Set the stopping state before closing the listener, notifier, and sessions. Cancel session lifetimes so callbacks resumed later cannot begin additional queue work. Do not close storage reentrantly from `stop()`; disposal belongs to the existing runtime's finalization path.

Preserve a **single five-second cooperative shutdown budget for asynchronous session cleanup**, starting at the first observed stop request. Its expiry releases asynchronous waits and marks failure. Delete worker-kill/close-exchange/join actions and `awaitStep()` wrappers whose sole job was awaiting worker teardown.

After session work has unwound, close local storage synchronously, then remove only the owned socket inode and release lifetime locks. Cancel owned timers and subscriptions on every exit path. Preserve original failures; a cleanup error must not turn shutdown into success or skip other safe cleanup.

Ensure storage disposal drops statement and PDO references even if one cursor cleanup fails: use `finally` for reference release where necessary. With no external PDO owner and no active transaction/callback stack, no later resumed broker task may access that connection. If disposal blocks, execution cannot skip ahead and release lifetime locks underneath it.

### 9.3 Explicit guarantee changes

| Area | Single-process contract |
| --- | --- |
| Internal exchange timeout | Removed; no internal exchange exists. A ten-second wrapper around local PDO would not interrupt it. |
| Startup | Local initialization may block. Cooperative cancellation is checked between stages, not during a stuck native call. |
| Socket/WAIT timers | Keep existing nominal values. Callback execution may be delayed by synchronous database work. |
| SIGTERM/SIGINT | Graceful shutdown is handled when the event loop regains control. The one-second signal timer does not preempt PDO. |
| Five-second shutdown budget | Bounds cooperative waits after stop is observed; **not** total wall time from OS signal delivery through native database cleanup. |
| External termination | Existing SIGKILL/crash recovery expectations remain; there is no persistence child to orphan. Hard process deadlines require an external supervisor. |

Do not install a signal trick, another child watchdog, a database thread, or an automatic worker fallback to conceal this trade-off. Do not advertise the old database-stall isolation. SQLite lock waiting and checkpoints can delay synchronous calls; a busy timeout is not a universal storage-operation timeout. [E1, E4, E6]

Preserve the distinction between application/process-crash recovery and power-loss durability. NORMAL and FULL keep their existing meanings; successful benchmark runs and process-kill tests do not prove power-loss behavior. [E6]

## 10. Readiness and benchmark topology

The public messaging protocol is unchanged. The operational readiness record changes deliberately:

```json
{
  "event": "ready",
  "pid": 12345,
  "storage_execution": "in_process",
  "synchronous_effective": "normal",
  "database": "/absolute/path/queue.sqlite",
  "endpoint": "/absolute/path/queue.sock"
}
```

The example PID/paths are illustrative. **Omit `persistence_pid`**. Do not set it to zero, null, or the broker PID. `storage_execution` distinguishes intentional absence from broken readiness.

Update existing benchmark consumers of readiness, including `Runner`, `ConcurrentRun`, resource registration, cleanup checks, and topology-dependent report/tests. Search all active code for `persistence_pid`, `Role::Persistence`, worker-handle assumptions, and child-count assertions.

For the in-process candidate:

- Register the broker PID/start-time once. Its CPU and memory include PDO; do not add a second persistence role for the same process.
- Report the persistence process as **not present**, not missing telemetry or measured zero CPU. Missing expected broker data is still an error.
- Verify effective SQLite settings from local owning-connection readback, not a separate observer connection.
- Verify broker and actor cleanup; retain storage integrity/accounting audits after measurement. Auditors/test helpers may remain separate processes.

When reading old/reference captures, a valid distinct persistence PID still identifies the process-worker topology. Do not infer “in-process” from an unexpectedly absent PID in a legacy ready record. Keep historical captures unchanged.

Update constructor/CLI readiness annotations and documentation. Do not add a topology registry, new benchmark engine, or selectable runtime mode. These are small adaptations to the existing two-backend harness.

## 11. Tests and minimal acceptance

Use existing suites and repository QA conventions. Preserve behavioral coverage while deleting tests of removed machinery. Test-only processes are allowed; “single process” describes the deployed broker and its storage, not the PHPUnit runner, publisher/consumer processes, or fault-injection helpers.

### 11.1 Required correctness coverage

| Area | Required proof |
| --- | --- |
| Single-process wiring | Running broker owns no persistence child; readiness identifies in-process storage and actual owning mode. |
| Message behavior | Send/claim/ACK/reject, delayed messages, multiple queues, ordering, expiry, and receipt fencing retain their existing behavior. |
| Claim regression | Initial payload SELECT, conditional UPDATE, zero-row rollback, and oversized-payload validation precedence remain correct. |
| Transaction completion | Commit precedes success; SQL/commit failure rolls back or fails closed; no open transaction survives an operation. |
| Cancellation | Already-cancelled calls do not mutate. Cancellation/disconnect after commit does not undo/replay send, claim, or settlement. |
| WAIT | No lost wakeup; idle and long-handler sessions remain usable; delayed/expiry wakeups and cancellation still work. |
| Binary/bounds | Binary data, NUL, maximum payloads, oversize rejection, receipt/header/frame validation remain covered. |
| Fairness/backpressure | A pre-buffered hot session yields between completed requests; another ready session/callback progresses. Slow readers hold no transaction or unbounded reply list. |
| Shutdown/recovery | Normal SIGTERM, startup failure cleanup, repeated stop, socket identity safety, process kill/reopen, and no post-stop mutation. |
| Retention | After repeated drain/disconnect cycles, client/watch/deferred-callback collections return to their expected empty state. |

Use controlled clocks, scripted sockets, explicit events, and observable operation boundaries. Do not add sleeps or elapsed-time thresholds as correctness proofs. A test safety timeout aborts a hang; it does not establish a performance guarantee.

For cancellation-after-commit and response failures, synchronize at the socket write boundary **after** local SQL returns. Do not suspend a production-style PDO transaction to simulate another event-loop callback executing concurrently.

A same-thread sequential loop over two local connections is not a concurrency test. Retain real multi-client broker tests; where cross-connection database contention is needed, use a separate helper process.

### 11.2 Blocking and contention tests

A helper process must explicitly acquire and acknowledge an external SQLite write lock before the broker attempts a mutation. That helper, not a timer on the blocked broker loop, releases the lock. This prevents a test deadlock caused by asking the stalled loop to unblock itself.

Check both a controlled release and a definite busy-error path. A focused test may set a zero busy timeout on its test-owned connection to force the latter; the primary benchmark and production default remain at 5,000 ms. Verify no false success, replay, or partially committed mutation. Keep the broker's existing fail-closed policy on unexpected PDO errors.

Do not require the broker to answer another socket while it is inside a deliberately blocked PDO call. The required behavior is progress after the call returns, or externally enforced termination and subsequent recovery. Observe and report the pause separately from correctness assertions.

### 11.3 Delete or translate obsolete tests

Delete worker protocol sequencing, pipe/environment propagation, child-kill/join, internal capacity credits, early worker replies, and pipeline-close race tests. They test removed components.

Translate any queue, binary-data, cancellation, shutdown, or resource-cleanup assertion embedded in those tests to the surviving local/socket boundary. Record the coverage mapping in the PR. A lower test count is not a defect if obsolete tests are removed; silently dropping behavior tests is.

Keep tests for the signal-dispatch workaround and real broker SIGTERM/SIGKILL behavior. Do not revive a deliberately stopped persistence-worker scenario when there is no persistence worker.

## 12. Performance and responsiveness evaluation

### 12.1 Primary comparisons

Use fresh captures, not historical rates. Compare this candidate with pinned PR #10 `87164c7`, the best measured worker variant supplied so far. A fresh non-pipelined base comparison is optional and separate; do not require another full matrix.

Freeze three concurrent pair orders before execution:

```text
reference → candidate
candidate → reference
reference → candidate
```

Each invocation uses the existing 3,000-message workload, three publishers, two consumers, 256-byte/16-KiB payload mix, WAL/NORMAL, 50 ms Doctrine polling, and Xdebug off. Retain the existing Doctrine-then-broker order within each capture and identify that limitation. No concurrent QA, benchmark, or diagnostic job.

Run one 60-second roundtrip and one 60-second idle capture per revision. Keep FULL smoke separate. Reuse the existing retention workload for the candidate rather than creating a new retention engine. Readiness/resource adaptation must not change the message workload or timed completion boundaries.

```sh
XDEBUG_MODE=off vendor/bin/castor bench --workload=concurrent --synchronous=normal --polling-ms=50
XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=60 --synchronous=normal --polling-ms=50
XDEBUG_MODE=off vendor/bin/castor bench --workload=idle --duration=60 --synchronous=normal --polling-ms=50
XDEBUG_MODE=off vendor/bin/castor bench --smoke --workload=roundtrip --synchronous=full --polling-ms=50
```

Keep dependency versions common to both revisions pinned. Record necessary dependency removals, PHP/extensions, event-loop driver, storage configuration, revision, topology, and recording policy. No retries-until-green; retain failures and explain any separately requested rerun.

### 12.2 One focused responsiveness diagnostic

Throughput alone cannot justify removing stall isolation. In a separate diagnostic worktree/pass, measure event-loop delay and independent socket pickup during ordinary traffic and checkpoint-producing/FULL traffic; include the explicitly controlled external-lock case.

A small, fixed-interval monotonic timer with bounded aggregate counters is sufficient to observe delayed callback execution. An independent client can measure the existing HELLO exchange on a second connection; do not add a production ping protocol. Report count, maximum and clearly labelled latency distributions/buckets, capture duration, and measured transaction/traffic conditions. Do not manufacture missed tick timestamps or treat timer drift as exact SQLite time.

Keep diagnostic hooks out of production and primary timed captures. No per-message diagnostic file writes, no telemetry-mode matrix, and no tracing subsystem. FULL smoke alone is not evidence about FULL tail latency or checkpoint pauses. Do not label a pause “checkpoint-caused” without identifying that cause; a FULL workload can measure observed pauses without such attribution.

For the blocked-call case, record the observed delay and eventual progress/failure. Expected blocking is not an integrity failure, but is essential decision evidence. No worst-case pause bound is established by means or a short run.

### 12.3 Report and decision

Report every cohort duration, public send/receive/ACK/full-cycle distribution, idle pickup result, observed CPU, memory, and cleanup status. Compare candidate broker CPU/memory with the reference's **broker plus worker**, without duplicating PIDs. Preserve partial-resource-coverage caveats and distinguish baseline memory from retention/peak memory.

Do not subtract concurrent public-call timings or old exchange aggregates from wall time to invent an IPC percentage. The candidate removes communication **and** changes scheduling/parallel execution, so performance need not improve by the old measured exchange-minus-worker difference.

There is no promised throughput multiplier. Correctness and the explicit operational contract are required. A deployment decision must consider observed pauses and complexity reduction, not just average throughput. This spec supplies no user-approved hard latency SLO; report that limitation rather than inventing one. If local PDO regresses badly, retain the experiment and its results instead of silently reintroducing a child.

## 13. Implementation sequence

**Step 1 — Local wiring and semantic preservation.** Construct `Queue`/storage in `BrokerFactory`; route broker operations locally; preserve proxy-only genuine preconditions; use the shared clock. Keep SQL and settings unchanged.

**Step 2 — Runtime simplification.** Remove worker lifecycle/IPC/dependencies; simplify notifier capacity handling; add per-request cooperative checkpoints; implement local startup/shutdown cleanup and document reduced timeout guarantees.

**Step 3 — Tests and topology.** Translate surviving behavior tests, remove worker-only tests, add single-process/fairness checks, and adapt readiness/resource accounting. Validate no false worker PID or duplicate CPU accounting.

**Step 4 — Evaluate.** Run QA, then the frozen benchmark comparisons and separate responsiveness diagnostic. Produce one ignored Markdown report, not a new measurement framework. Keep failures and limitations visible.

Before submission:

```sh
vendor/bin/castor cs:fix
vendor/bin/castor qa
```

Use the existing Castor result files to inspect failures; do not add PHPStan suppressions, a baseline, or Composer QA scripts.

## 14. Definition of done

The implementation is complete when the branch has one broker process, one PDO connection, no internal worker protocol or hidden command queue; preserves the specified queue/protocol/durability semantics; yields between requests without yielding inside transactions; cleans up local resources safely; and reports the new topology honestly.

The PR must include the actual base/candidate SHAs, files/components removed, surviving validation/test coverage, QA outcome, fresh comparison results, observed responsiveness limitations, and any uncompleted acceptance work. Explicitly identify removed startup/exchange isolation guarantees. Existing messaging clients must not need a new DSN or frame format; scripts consuming readiness may need the documented topology change.

No merge/release recommendation follows solely from creating this spec or from a faster benchmark.

## References and evidence

Repository URLs are pinned to inspected revisions. External documentation supplies API/behavior facts, not measured results for this project.

- **[R1]** PR #10 metadata, scope, revisions and supplied pipelining results: https://github.com/ineersa/sqlite-queue/pull/10 ; comparison revision `87164c72fd59bdf8966a9302b5e8f9e0632d08ec`.
- **[R2]** Local queue policy and cancellation: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/src/Queue.php
- **[R3]** Broker wiring, dispatch errors, readiness and shutdown: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/src/Broker/Broker.php
- **[R4]** PDO storage/claim implementation: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/src/Sqlite/SqliteQueueStorage.php ; related supplied claim-attribution report [D1].
- **[R5]** Factory acquisition and clock semantics: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/src/Broker/BrokerFactory.php
- **[R6]** CLI setup, signal callbacks and periodic signal-dispatch workaround: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/src/Command/BrokerCommand.php
- **[R7]** Notifier capacity path and watch/timer behavior: https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Broker/QueueNotifier.php
- **[R8]** Dependency requirements: https://github.com/ineersa/sqlite-queue/blob/d37c9ab00290324578ae78c0127b307b3433ed88/composer.json
- **[R9]** Removed proxy preconditions and internal exchange: https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteQueueWorker.php
- **[D1]** User-supplied “Claim SQL simplification and worker attribution,” measured 8 October 2026 (`Pasted markdown(2).md`). Diagnostic boundaries/counts and 0.998 s versus 1.894 s aggregates; no independently rerun measurements. Local linked raw captures were not accessed.
- **[E1]** AMPHP cooperative coroutines, blocking I/O, cancellation and utilities: https://amphp.org/amp
- **[E2]** Revolt deferred callbacks and event-loop scheduling: https://revolt.run/timers ; https://revolt.run/fundamentals
- **[E3]** PHP PDO SQLite transaction-mode attribute: https://www.php.net/manual/en/class.pdo-sqlite.php
- **[E4]** SQLite busy timeout: https://www.sqlite.org/c3ref/busy_timeout.html
- **[E5]** SQLite transactions, IMMEDIATE acquisition and single-writer behavior: https://www.sqlite.org/lang_transaction.html
- **[E6]** SQLite WAL checkpointing and synchronous durability behavior: https://www.sqlite.org/wal.html
