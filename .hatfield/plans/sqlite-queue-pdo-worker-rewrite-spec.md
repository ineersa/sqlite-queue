# SQLite queue: operation-level PDO worker rewrite

**Status:** Approved for implementation  
**Date:** 7 October 2026  
**Repository:** `ineersa/sqlite-queue`  
**Source baseline:** PR #9, `1d8f6f8bd34376a675cdd768be9daf51573ef17c`  
**Suggested repository location:** `.hatfield/plans/pdo-worker-rewrite-spec.md`

This specifies a rewrite, not an implementation or a new benchmark result. Requirements below are proposed design decisions unless explicitly identified as existing behavior. Rebase against the actual integration branch before implementing; do not accidentally lose PR #9's idle-session fix or durability configuration. [R1]

## 1. Objective

Replace `fabpot/amphp-sqlite3` with a single, long-lived process that executes complete queue operations using one native PDO SQLite connection.

The broker remains asynchronous for sockets, notifications, process supervision, and waiting for storage results. SQLite calls are synchronous **inside the worker**, never inside the broker event loop.

```text
Application / Messenger
        │ existing public Unix-socket protocol
        ▼
Broker process
  Broker + sessions + QueueNotifier
        │
  SqliteQueueWorker: asynchronous, operation-level proxy
        │ one request → one terminal response
        ▼
Persistent worker process
  worker.php: receive and dispatch one operation at a time
        │
  Queue: message policy, clocks, receipts, validation
        │ local calls only
  SqliteQueueStorage: native PDO, statements, transactions
        │
  SQLite database + WAL
```

AMPHP provides process contexts and channels for running blocking work outside the parent. Create the database connection inside the child; transmit data, not PDO resources or closures. Use `ProcessContextFactory` explicitly, not an environment-dependent process/thread choice. [E1]

The existing successful message lifecycle uses five application SQL statements and three transactions. This rewrite initially preserves those statements. Its structural target is **three operation exchanges per successful publish → claim → ACK lifecycle**, rather than approximately eleven statement/control exchanges. Notification probes, startup, errors, and shutdown are separate operations, not part of that count. [R2]

**No throughput multiplier is promised.** Measure the new implementation against the same workload after preserving correctness.

## 2. Scope and deliberate non-goals

Implement operation-level IPC, worker-local PDO and prepared statements, lifecycle integration, dependency removal, migrated correctness tests, and a small before/after measurement.

Do not add message batching, group commit, prefetch, multiple SQLite workers, a worker pool, an in-memory message store, a second journal, automatic replay, worker auto-restart, heartbeats, or a generic asynchronous SQL API. Keep the public wire protocol, Messenger routing, WAIT behavior, queue ordering, schema, and durability options unchanged.

Do not merge the claim's two SELECTs or introduce `RETURNING` in this rewrite. Moving the existing SQL unchanged makes the driver/IPC change easier to evaluate. A later SQL optimization can have its own tests and measurement.

**Two approved contract changes:** cancellation stops an operation before dispatch but does not revoke an already-dispatched database operation (§6); send delay starts after transaction acquisition (§7). This is not transparent compatibility with the current pre-commit cancellation behavior. Record both in the design decision and release notes before integration. Receipt fencing and commit-before-success remain required.

## 3. Ownership and code organization

| Component | Process | Responsibility |
|---|---|---|
| `Broker` | Parent | Public protocol, session identity, result routing, commit-based notifications, shutdown coordination. |
| `QueueNotifier` | Parent | WAIT registrations, wakeups, deadline scheduling and coalesced eligibility probes. No SQL. |
| `SqliteQueueWorker` — new | Parent | Typed operation methods, serialized channel exchange, admission cancellation, response validation and failed/closing state. No SQL or policy clock. |
| `SqliteWorkerContextFactory` — adapted | Parent | Start exactly one package-owned worker; retain its lifecycle handle; unwind failed startup. |
| `SqliteWorkerHandle` — adapted | Parent | PID, pipe drains, exit observation, force termination and sole ownership of context joining. No queue operations. |
| `Sqlite/worker.php` — new | Child | Bootstrap, receive/validate requests, invoke `Queue`, send terminal responses, exit on fatal failure/EOF. |
| `Queue` — retained, moved into child runtime | Child | Queue policy, validated names, receipt parsing, epoch/token generation, deadline arithmetic and domain exceptions. |
| `SqliteQueueStorage` — rewritten | Child | PDO ownership, schema initialization, fixed prepared statements, transactional SQL and rollback. |

Change `Broker` and `QueueNotifier` to depend on the parent proxy rather than a parent-resident `Queue`. The child constructs exactly one `Queue` and one storage instance. A worker lifetime equals one broker generation because automatic worker replacement is prohibited.

Keep policy separate from persistence as required by `AGENTS.md`. Moving both into one process is not permission to put SQL into `Broker`, the proxy, or a large protocol switch. Local clock/deadline callbacks between `Queue` and storage are acceptable; they never cross IPC. [R3]

Remove the worker-local `LocalMutex`/`FiberLocal` ownership mechanism: the child executes one operation synchronously at a time. Keep one parent-side exchange mutex. The old arbitrary `exclusive(Closure)` interface is not a remote API.

Use a small internal operation enum and explicit array shapes at the channel boundary. Do not create a class hierarchy, codec framework, or command bus for each operation. Retain existing DTOs and domain exceptions where useful. New exceptions use the `Exception` suffix; new DTOs use the `DTO` suffix. [R3]

## 4. Startup, PDO configuration and readiness

### 4.1 Startup sequence

1. Validate capabilities and configuration before taking resources. Require PHP 8.5+, `ext-pdo_sqlite`, and the existing foreground-broker process/ownership capabilities.
2. Acquire the existing database and endpoint lifetime locks. Preserve canonical-path, private-file, symlink, hardlink, ownership and socket-identity checks.
3. Start one worker through AMPHP, inheriting the required PHP environment. Start bounded/discarding stdout and stderr drains immediately.
4. Send one initialization payload containing the database path, synchronous mode and visibility timeout. Other fixed connection policies can be package constants. There is no arbitrary-PRAGMA or arbitrary-bootstrap option exposed to clients.
5. In the child: open PDO, configure it, initialize the existing schema, prepare the fixed statements, and construct `Queue` with a worker-local clock and new epoch.
6. Child returns a validated readiness response. Only then finish public socket setup and publish broker readiness. Every partial-startup failure terminates the created worker before releasing ownership.

Use the actual child connection for configuration evidence, not a read-only audit connection. Preserve the public readiness fields, including `persistence_pid` and `synchronous_effective`. The proxy can retain immutable startup readback for readiness reporting; it must not open another connection.

Use one 15-second monotonic startup budget beginning before worker spawn. It covers process startup, initialization, schema/configuration and the readiness response. Do not reset it between steps. Failed startup must stop the acquired worker and release resources.

### 4.2 Connection policy

Use exception error mode, associative fetches, non-stringified numeric fetches, and explicit parameter types. Keep one ordinary connection alive through object ownership; PDO persistent-connection mode is unnecessary.

Use PHP 8.5's `Pdo\Sqlite::ATTR_TRANSACTION_MODE = Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE`, then use PDO's `beginTransaction()`, `commit()`, `rollBack()` and `inTransaction()` consistently. Do not mix raw `BEGIN` with a different transaction-state mechanism. This transaction-mode attribute is available in PHP 8.5. [E2]

Configure and verify:

| Setting | Required policy |
|---|---|
| `journal_mode` | WAL, effective file-backed database |
| `synchronous` | NORMAL by default; FULL remains selectable |
| `busy_timeout` | 5,000 milliseconds |
| `foreign_keys` | ON |
| `trusted_schema` | OFF |
| `wal_autocheckpoint` | 1,000 pages, explicitly recorded |
| Extended SQLite error codes | Enabled through the supported PDO SQLite attribute |

Foreign keys, untrusted schema and extended codes preserve the locked driver's configuration rather than accidentally changing them during removal. Preserve its SQLite minimum of 3.31.0 for this migration; detect the actual library linked to PDO. [R4]

NORMAL and FULL keep their existing durability meanings. In WAL mode, FULL synchronizes the WAL at transaction commits; NORMAL may lose recent transactions after power loss or an OS crash. Do not describe NORMAL as guaranteeing confirmed messages across those failures. Do not change mode to make the benchmark faster. [E3]

Close statements/cursors and drop all PDO references during orderly shutdown. PDO resources must not escape the child storage object into process-global caches.

## 5. Internal operation protocol

Use AMPHP's existing channel serialization/framing. Do not add JSON/base64 around message bodies. Internal payloads must preserve arbitrary bytes, including malformed UTF-8 and NUL. Keep Messenger header validation at its existing adapter boundary.

One monotonically increasing request ID is sufficient per worker lifetime. Include the operation in request and response; validate the response ID, operation, status and result shape before releasing the exchange lane. IDs correlate exchanges; they are **not** persisted deduplication keys.

| Operation | Data sent to child | Successful result |
|---|---|---|
| `send` | Queue name, body bytes, header bytes, nonnegative delay in ms | Positive integer message ID, after commit |
| `claim` | Queue name, broker-assigned owner ID | Delivery fields after commit, or an explicit empty result |
| `settle` | Receipt, owner ID | Settlement result; ACK and reject share the existing delete behavior |
| `earliest_eligibility` | Queue name | Absolute eligibility timestamp in ms, or null |
| `close` | No queue data | Storage closed; worker then exits |

Initialization is a separate startup exchange. No steady-state `execute`, `prepare`, `fetch`, `begin`, `commit`, `rollback`, `runClosure`, clock-override, or debug commands exist.

A response is exactly one of: successful result, expected domain failure, or storage failure. Use finite status/error enums. The worker sends no partial delivery or provisional success.

Validate request shape and bounds on both sides, without reparsing opaque payloads. The child reconstructs `QueueName`; it does not trust arbitrary callers to bypass domain validation. Never accept a database path or owner replacement inside a normal queue operation.

For `claim`, check positive identity, receipt structure, expected queue, bounded payload sizes and correctly typed deadlines in the parent response validator. Malformed worker replies fail the storage lane closed, with a specific diagnostic. Do not continue consuming replies to guess which operation they belong to.

## 6. Admission and cancellation: selected semantics

### 6.1 Why this decision is explicit

Current settlement runs a parent-visible cancellation check after DELETE and before COMMIT. `testDisconnectAfterDeleteRollsBackSettlement` explicitly requires rollback when cancellation is observed at that point. The current receive test also expects a cancellation exception while retaining an already-committed reservation. [R5]

Moving a complete operation into a blocking child removes that shared cancellation state. This specification chooses **cancellable admission, non-revocable dispatch**, rather than adding a cross-process commit-approval exchange.

This preserves receipt fencing and commit-before-success, but changes the cancellation boundary. Do not claim all previous cancellation behavior is unchanged.

### 6.2 State transitions

```text
waiting for exchange lane
    ├─ cancelled / closing before dispatch → not sent, no DB operation
    └─ lane acquired + final checks pass
             ↓ immediately before channel send begins
         dispatch attempted
             ├─ valid terminal result → known result
             └─ send/receive failure, timeout, worker death → potentially unknown
```

Check caller cancellation before waiting and immediately before sending. There must be no yield between that final check and marking dispatch attempted.

Once sending begins, do not attach the caller's cancellation token to the channel receive. A channel-send error may occur after some or all bytes reached the child; it is not automatically proof of non-execution.

The exchange owner remains responsible for receiving the result even if the requesting public socket closes. It retains the mutex until the response is validated or the storage lane has irreversibly failed. A cancelled waiter must never release the lane while a late response can still arrive.

Each dispatched exchange has one 10-second monotonic deadline starting immediately before channel send. It covers send and terminal response validation, not admission queue time. This deadline is independent of caller cancellation. On expiry, classify the dispatched outcome as unknown, fail the lane, terminate the worker, and release all waiting callers with exceptions. Never reuse the channel or replay an operation. Shutdown can shorten this budget through its shared five-second deadline.

The parent may learn that an operation committed after its caller disconnected. It must not replay, compensate, delete a newly claimed message, clear a reservation, or recreate an acknowledged message.

### 6.3 Operation-specific consequences

**Send:** after dispatch, it may commit despite cancellation. On a successful worker response, process the publish notification even if the publisher can no longer receive confirmation.

**Claim:** after dispatch, it may commit a reservation. If the broker has observed session cancellation, do not write the delivery to that session. Keep the reservation until its original visibility expiry. Process the committed claim's readiness update before suppressing the response.

**ACK/reject:** a dispatched, valid settlement may commit after session cancellation. An invalid or expired receipt still cannot delete the message. A lost response makes the caller's outcome uncertain; it does not undo a successful settlement.

**Low-level proxy calls:** before dispatch, cancellation throws `ClientContextClosedException`. After dispatch, return the actual terminal result/domain failure, or an explicit storage failure. Do not throw a misleading cancellation exception instead of a known successful commit. The broker separately decides whether its public session is still writable.

“Cancellation observed” means the broker/lifetime has detected it, not instantaneous knowledge of a physical peer disconnect. This rewrite does not add a new socket-disconnect detection protocol.

### 6.4 Compatibility alternative, not selected

Strict preservation of the parent-side pre-commit cancellation check would need an explicit mechanism such as worker → parent commit approval → worker. That keeps SQL inside the worker but adds a second phase and a transaction held open across IPC. It is a different design from this one-request/one-response specification. Do not silently introduce that mechanism or silently omit the contract change.

## 7. Clock, identity and receipt rules

Keep absolute Unix wall-clock milliseconds for persisted `available_at` and `reserved_until`; use monotonic time for parent deadlines and shutdown budgets.

Calculate availability, expiry and settlement validity **inside the worker after the relevant transaction has been acquired**. Pass a relative send delay, not an absolute timestamp calculated while the request waits in the broker. Pass owner identity, not a precomputed claim expiry or parent “now”.

For send, this deliberately makes delay start at the operation's acquired transaction rather than including parent/IPC queue time. Document this timing definition with the cancellation change. Claim and settlement must retain the existing transaction-acquisition clock guarantees. [R6]

Keep the existing receipt format `positive-id:64-lowercase-hex-token`, 32-byte random reservation tokens, and fresh 32-byte broker epoch. Generate one epoch when the worker's `Queue` is constructed. Never regenerate it per operation. Owner IDs remain assigned by the broker's public sessions; reconnects do not inherit previous ownership. [R2]

A fresh claim writes owner, epoch, token and visibility deadline atomically. Receipt failure precedence remains: no active reservation → owner mismatch → epoch mismatch → token mismatch → expiry. Expiry uses the existing strict test: settlement requires `reserved_until > now`; equality is expired.

Keep `ORDER BY id` among eligible rows. Keep delay and visibility arithmetic overflow checks. Do not extend reservations on disconnect or reset all reservations at restart.

Tests must inject clocks **where they are sampled**. A mutable closure in the parent is not a shared clock in another process. Retain local policy/storage tests with controlled clocks; use a test worker bootstrap with its own controlled clock for cross-process barriers. No clock controls are added to the production IPC or CLI.

## 8. Worker-local SQL operations

Preserve `queue_messages`, its constraints, AUTOINCREMENT identities, and both indexes. Existing databases require no data migration. Startup schema checks are allowed; repeated schema introspection on message paths is not. [R2]

Prepare the seven fixed application statements once per connection: insert; eligible-ID select; reservation update; payload select; fenced delete; settlement-diagnosis select; earliest-eligibility select. Rebind every parameter on each use. Do not build an unbounded SQL-keyed statement cache.

### 8.1 Send

Inside one immediate transaction: sample time; validate the resulting delay deadline; insert the row; capture and validate the positive `lastInsertId()`; close the cursor; commit; then return the ID.

Bind body and headers explicitly as `PDO::PARAM_LOB`, including empty strings. PDO SQLite accepts strings for LOB parameters. Bind numeric timestamps/IDs as integers and textual names/tokens as strings. Do not rely on an untyped `execute($params)` call for the BLOB columns. [E4]

### 8.2 Claim

Keep the initial SQL sequence unchanged:

```sql
SELECT id
FROM queue_messages
WHERE queue = ? AND available_at <= ?
  AND (reserved_until IS NULL OR reserved_until <= ?)
ORDER BY id
LIMIT 1;

UPDATE queue_messages
SET reserved_until = ?, reservation_token = ?, owner_id = ?, broker_epoch = ?
WHERE id = ? AND queue = ? AND available_at <= ?
  AND (reserved_until IS NULL OR reserved_until <= ?);

SELECT body, headers, available_at
FROM queue_messages
WHERE id = ?;
```

Acquire the transaction before sampling `now`; derive visibility from that sample. On no eligible row, roll back and return empty. If the conditional UPDATE affects zero rows, roll back and return empty, preserving the existing zero-row-claim test. Any unexpected affected-row count is a storage error.

Validate fetched payloads/deadlines. Materialize bounded body/header bytes in the child; never return PDO cursors or LOB resources over IPC. Commit before returning a delivery.

Keep the eligibility predicate even with a single worker. SQLite transaction protection, not an assumption about exclusive application access, must prevent two independent connections from claiming the same row.

### 8.3 Settlement

Acquire an immediate transaction; sample `now`; execute the existing fenced DELETE:

```sql
DELETE FROM queue_messages
WHERE id = ? AND reservation_token = ? AND owner_id = ?
  AND broker_epoch = ? AND reserved_until > ?;
```

One affected row means commit and return success. Zero rows means diagnose using the existing SELECT **inside the same transaction and with the same sampled time**; finish the transaction before returning the domain failure. Preserve the existing error precedence. More than one affected row is an invariant violation.

Do not simplify this to deletion by ID. ACK and reject retain identical storage deletion semantics; retry routing remains outside this worker.

### 8.4 Earliest eligibility

Retain the indexed effective-deadline query:

```sql
SELECT max(available_at, coalesce(reserved_until, available_at)) AS ready_at
FROM queue_messages
WHERE queue = ?
ORDER BY ready_at
LIMIT 1;
```

Return a validated integer or null; close the cursor. It is a scheduling hint, not a claim. Do not add a write transaction, periodic table scan, or worker-owned polling timer.

### 8.5 Statement and transaction hygiene

Close every cursor on success and failure; clear references that retain large parameter values/results after an operation. `closeCursor()` releases a result for statement reuse. Keep PDO and prepared statements private and non-cloneable. [E5]

No per-operation `EXPLAIN`, `sqlite_schema` lookup, table-definition parser or `total_changes()` probes. Use the known schema, PDO insert identity and DML affected-row counts. Do not use `rowCount()` to decide whether a SELECT returned a row.

## 9. Errors, commit ambiguity and fail-closed behavior

The storage implementation must track whether the transaction began and whether commit returned successfully. On an exception, close cursors and attempt rollback if the connection reports an active transaction. Preserve the primary error and separately retain rollback failure information.

Do not assume a throwing COMMIT automatically ended the transaction: SQLite documents cases where a failed commit leaves it active. No automatic statement or transaction retry is added. SQLite's configured busy wait is distinct from replaying an entire operation. [E6]

Classify evidence, not guesses:

| Evidence | Outcome |
|---|---|
| Rejected before channel dispatch | Not applied |
| Worker reports domain failure or verified rollback before any successful commit | Not applied |
| Valid success response after commit | Committed |
| Commit/channel outcome cannot be established | Unknown |

A cancellation or timeout is not rollback evidence. A malformed result after dispatch is not rollback evidence. A successful commit followed by response failure remains committed in storage even though the parent/client may not know it.

Expected receipt/argument failures return domain errors without killing the broker. Local admission-capacity refusal follows section 11 and is not a storage failure. Actual storage exceptions, corrupt rows, rollback failures, malformed internal protocol, IPC loss and unexpected worker exit fail the parent storage lane and trigger the existing broker-failure path. Never convert them into “empty queue”. No automatic worker restart or operation replay.

After fatal failure, stop admission before releasing any waiting caller. Resolve every outstanding wait with a result or explicit exception; no future may remain behind an abandoned mutex.

Carry only bounded diagnostic fields: operation, phase, exception category, SQLSTATE/native code when available, and outcome evidence. Cap textual diagnostics at a named small limit, for example 1,024 bytes, and ensure diagnostic text is valid UTF-8. Do not echo SQL parameters, body/header bytes, arbitrary exception objects, or worker stderr to public clients.

## 10. Notifications and public transport behavior

Keep WAIT in the broker. A worker `claim` or eligibility lookup returns immediately when its database work finishes; it never waits for future messages.

Preserve notification ordering: a successful send or claim notifies only after its worker success response establishes commit. Never notify optimistically when queuing the request. A lost response/fatal lane closes WAIT clients through broker shutdown rather than pretending readiness is known.

Do not add a parent-side SQLite connection to improve notifications or inspect worker state. Retain `QueueNotifier`'s existing race handling for a mutation arriving during an eligibility probe. Coalesce probes so each watched queue has at most one outstanding storage lookup. [R7]

After a committed response, update notification state even when the originating session has been cancelled; suppress only that session's reply. After global shutdown begins, notifier shutdown supersedes wakeups.

Preserve the established-session idle fix: connection idleness is not an incomplete-frame timeout. Keep separate operation and notification connections and receipt ownership. This rewrite does not add reconnect/replay or turn WAIT into payload push. [R1]

## 11. Backpressure and bounded resource use

Allow **one in-flight storage exchange**. Do not pipeline commands into AMPHP's channel or create another queue of serialized payloads. Waiting calls retain their existing request data; serialize only after acquiring the exchange lane.

Enforce explicit admission limits in the proxy, including the active operation:

- At most **128 admitted operations**: a budget derived from the current 64 public connections plus up to 64 eligibility probes.
- At most **`64 × Limits::MAX_PAYLOAD` body/header bytes** across admitted operations. This is a logical payload budget, not a bound on PHP allocation or serialization overhead.

These are internal named limits, not new user-facing tuning options. Reserve count/bytes synchronously before waiting for the mutex, and release them in `finally` on every path. Do not create an unlimited list of waiters outside this accounting.

A full admission budget produces a local, **not-dispatched** `StorageCapacityException`; it does not fail the database lane. For a public request, close only that session without a success response, using the existing transport-failure behavior. Do not kill the broker or retry the request automatically. The public client cannot infer non-execution solely from the connection closure.

For an eligibility probe, capacity exhaustion instead leaves its existing live watch dirty. Retry on a single coalesced capacity-available notification, not a timer, polling loop, or new future per attempt. This reschedules a probe that was never dispatched; it does not replay an uncertain operation.

Preserve at most one outstanding probe per queue, and count probes belonging to detached watch generations until they are actually released. Live-connection limits alone are insufficient: repeated WAIT timeout/cancellation can abandon old probe fibers while new watches are created. Test that case explicitly. Remove obsolete, not-dispatched work where practical; otherwise keep it accounted for until it reaches the gate and is skipped. Never let stale completion re-arm a detached watch.

Broker admission remains capped at 64 connections, one outstanding public request per connection, with protocol payload limits unchanged. At the current maximum, 64 retained publish payloads alone can occupy up to `64 × 1,040,000` bytes, before PHP/framing/serialization overhead. Public frames waiting before storage admission are a separate, connection-bounded allocation. Do not advertise one-message memory usage or a 64 MiB total-process bound. [R7, R8]

The proxy is an internal service, not an unbounded job-submission API. Cancellation while queued is checked again at admission; it need not remove a mutex waiter instantaneously, but must prevent later dispatch. Shutdown/fatal failure must eventually release all accounted waiters.

The worker retains no historical request list, delivery graph, response cache or completed-task list. Reuse only its fixed statements/configuration. Release operation arrays, bound values and large payload references after sending the response.

## 12. Worker lifecycle and shutdown

Retain process supervision independently of the request-response lane. Do not remove force-stop capability merely because the vendor driver is gone. The existing handle currently relies on driver-owned joining; update that responsibility explicitly. [R9]

While serving, retain the existing pipe-drain exit observation as a failure signal; it is not proof that a process has been reaped. Do not run a second `receive()` consumer on the operation channel. Lifecycle ownership moves to `SqliteWorkerHandle`; repeated cleanup shares one outcome. Graceful close joins the context once. Forced termination uses `ProcessContext::close()` and tolerates the expected missing-result/channel failure instead of retrying `join()`. AMPHP owns underlying process-exit tracking and reaping. Do not add a custom reaper, call `waitpid()` behind AMPHP's back, or copy its internals. Cancel pending exchanges and owned pipe reads on every shutdown path. Verify child cleanup in lifecycle tests.

On ordinary SIGTERM, use the existing single five-second shutdown budget from the first stop request:

1. Stop accepting clients; mark storage closing synchronously; close WAIT registrations. Operations not yet dispatched must not start later.
2. Allow the one dispatched operation to reach a terminal result while the shared budget permits. Caller cancellation does not revoke it.
3. When the lane is idle, send `close`; child closes statements/PDO, replies, and returns from its bootstrap.
4. Join/reap through the designated owner and finish/cancel pipe drains.
5. On deadline or fatal failure, terminate the child through the retained process handle, settle blocked exchanges, then perform identity-checked socket cleanup and lock release.

Do not create a fresh five-second budget for each step. Repeated stop/close calls are idempotent. A timeout on an await is not child termination. Do not release ownership on the assumption that killing was successful while a worker is known to remain live; report cleanup failure honestly.

On worker SIGKILL, fail the broker; do not create a replacement behind existing sessions. A newly started broker gets a fresh epoch and uses the existing database recovery behavior.

On broker SIGKILL, application-level cleanup cannot run. The worker must exit on channel EOF/failure after its current finite database call finishes. Test ordinary parent-death cleanup, but do not promise an instantaneous bound for a child stuck in uninterruptible kernel I/O. Operations whose reply was lost remain uncertain.

Preserve normal SIGTERM and relevant SIGKILL coverage. Do **not** reinstate the deliberately SIGSTOPped-worker scenario as a release requirement; the user explicitly removed that condition. Historical discussion remains historical, not a claim that it was fixed. [R10]

## 13. Dependency, configuration and API migration

Remove `fabpot/amphp-sqlite3` from production dependencies and update the lock file. Promote `ext-pdo_sqlite` to production requirements. Remove `ext-sqlite3` from production only after checking all runtime references; retaining it as a development dependency is acceptable while audit/test code still uses `SQLite3`.

Keep AMPHP/Revolt dependencies actually used by the broker and process channel. Do not remove `amphp/parallel` or copy its internals.

Introduce package-owned `SqliteSynchronousMode` with only `Normal = 'normal'` and `Full = 'full'`. Replace vendor enum/config/result/blob/exception references in production, CLI, bundle configuration, benchmark and tests. Do not leave a vendor import solely to preserve an internal type. Preserve external option names and accepted `normal|full` strings.

Document low-level PHP API changes: constructing storage from a vendor connection, injecting a parent clock into child policy, and the vendor synchronous enum are not transparently compatible. Prefer one clean implementation over a runtime dual-driver feature flag or compatibility shim that keeps the dependency installed.

Keep the public `Client`, protocol version, receipt shape, Messenger transport API and DSNs unchanged. Update package extension documentation, PHPStan includes and Castor task wiring as necessary; do not add Composer QA scripts or suppressions. [R3]

## 14. Correctness tests

Use controlled clocks, barriers and explicit events; safety timeouts may abort a hang but elapsed-time thresholds do not prove correctness. Reuse the existing test support rather than creating another large harness. [R3]

| Area | Required cases |
|---|---|
| PDO/storage parity | Existing DB opens; WAL/mode readback; empty and binary body/headers including invalid UTF-8; BLOB types; delay/overflow; ordering; empty receive; zero-row claim; rollback; database-full failure. |
| Receipts | Owner/epoch/token mismatch and precedence; equality at expiry; old receipt after reclaim/restart; ACK/reject only delete the matching reservation. |
| Clock placement | Advance child-local time while waiting for transaction acquisition; full claim visibility remains; expired settlement fails; send delay follows the specified transaction-time definition. |
| Transaction isolation | No result before commit; no second operation enters the worker during a transaction; independent connections cannot claim the same message. |
| IPC | One operation request/terminal response; binary transport; wrong ID/op/type; truncated/closed channel; failure during send; no reuse after desynchronization. |
| Cancellation | Before admission: no dispatch. After dispatch: actual result consumed. Committed send still notifies. Cancelled claim retains original expiry without delivery to cancelled session. Dispatched settlement may commit. |
| Failures | Kill before transaction, during transaction, and after commit before reply; distinguish stored state from caller uncertainty; no replay; all waiters released. |
| Responsiveness | Hold the child at a controlled barrier; explicitly demonstrate that parent timers, cancellation and socket/WAIT lifecycle callbacks still execute. |
| Lifecycle | Partial startup cleanup, ordinary SIGTERM, worker SIGKILL, parent SIGKILL, one worker only, idempotent close, one join owner, pipes/watchers released, locks/socket ownership preserved. |
| Boundedness | Admission count/byte caps; capacity recovery without broker failure; stale WAIT/watch-generation churn while storage is held; no pipelined worker backlog; payload/statement references released across repeated cycles. |

Rename and replace `testDisconnectAfterDeleteRollsBackSettlement` with a dispatched-settlement cancellation test that verifies the newly selected boundary. Preserve its original requirement in the migration notes rather than silently deleting a failing assertion. Adapt the committed-receive cancellation test's API expectation while preserving its reservation/expiry assertions. [R5]

Keep deterministic transaction hooks/barriers in test fixtures or a narrow test seam. Do not expose fault injection through production CLI/environment/protocol, add per-message marker files, or install general profiling hooks in the hot path.

Vendor-driver conformance tests may be removed only when they test the removed dependency itself. Broker/storage guarantees previously exercised through that driver must be moved to equivalent PDO/worker tests. A lower test count is not proof of simplification if contract coverage vanished.

## 15. Implementation sequence

### Step A — Record decisions and freeze the comparison

Add the short design decision covering process ownership, dispatched-operation cancellation, and send-delay timing. Inventory vendor imports and affected tests. Make a separate benchmark-config change using Doctrine `--sleep=0.05` as the primary Hatfield reference and record it in captures. Capture the old broker and Doctrine under this configuration before the rewrite.

Keep historic 1 ms results clearly labelled. Do not compare old 1 ms idle CPU against new 50 ms CPU and attribute the difference to PDO.

### Step B — Implement local PDO storage

Rewrite `SqliteQueueStorage`, retain the SQL/schema, introduce the package durability enum and migrate local `Queue` policy tests. Remove only asynchronous ownership machinery that is redundant in the single worker. Confirm binary binding, transaction-time clocks, cursor cleanup and rollback with real PDO.

**Exit:** local storage/policy guarantees pass without the vendor SQL driver. No claim of broker performance yet.

### Step C — Implement the package worker and parent proxy

Add the child bootstrap and small internal operation protocol. Move policy/storage construction into the child. Adapt factory/handle lifecycle ownership. Implement one serialized exchange, cancellation at admission, terminal response validation, and fatal-lane behavior.

**Exit:** real-process operation, cancellation, ambiguity and child-death tests pass; one connection and one child are observed.

### Step D — Integrate broker, notifier and Messenger

Replace parent `Queue` access with the proxy; retain notification races, protocol limits, receipt ownership and idle-session behavior. Apply post-result session checks only after commit-based notification handling. Wire one shutdown budget and one join owner.

Remove vendor dependencies/imports, obsolete tests and stale documentation. Do not retain two production implementations behind a flag.

**Exit:** complete QA passes with migrated cancellation expectations explicitly documented.

### Step E — Measure and report

Run the same primary 50 ms comparison with the rewritten broker. Report exact source/configuration, integrity, public operation timing, finite-cohort completion and idle behavior. Retain failed runs and report them; do not automatically retry until a favorable number appears.

If results remain poor, profile the new path separately. Do not add batching or rewrite the benchmark within this step to conceal the result.

## 16. Measurement and acceptance

Use the existing native Messenger workloads and fixed recording policy; no new benchmark framework or calibration matrix.

Primary performance evidence: the existing 3,000-message concurrent NORMAL cohort, existing roundtrip workload, and idle/pickup at Doctrine's 50 ms setting. Check FULL as a separate durability mode; do not pool it with NORMAL. Retain application/retention workloads as existing regression checks rather than adding more scenarios. [R11]

Report public send/receive/ACK durations, completion accounting, elapsed cohort time, observed process CPU, effective pragmas, errors, worker count and source revision. Do not sum overlapping operation means into wall time, claim tails from two pickups, or call a finite drain test sustained capacity.

Verify the structural changes using deterministic tests or a separate diagnostic run: one worker request per queue operation, no per-SQL IPC, fixed explicit preparations and no hot-path driver metadata queries. Profiling durations do not replace unprofiled measurements.

Correctness and structural acceptance are mandatory. A performance claim additionally requires observed improvement under matched conditions; no fabricated percentage or universal “faster than Doctrine” requirement is part of this specification. If throughput does not improve, report that result and investigate before presenting the rewrite as a performance success.

## 17. Definition of done

The broker performs no SQLite calls; its only production database owner is one persistent PDO worker. All queue SQL and transactions are worker-local, policy remains separated from storage, and each operation has one terminal exchange. Success follows commit; unknown outcomes are never replayed. Receipt fencing, binary storage, durability modes, notifications and supported shutdown behavior are preserved, with cancellation/send-delay changes documented and tested.

Vendor runtime references are removed. Existing databases work without conversion. Production memory has no history-growing request/result cache. Relevant deterministic tests and full QA pass. Before/after captures use matched 50 ms polling and selected durability, with limitations and failures retained.

Run the repository's required validation sequence:

```bash
vendor/bin/castor cs:fix
vendor/bin/castor qa
```

Read and resolve reported failures; do not add baselines, ignored errors, disabled strict rules or performance thresholds to correctness QA. [R3]

## Sources and implementation anchors

Repository references are pinned to the reviewed revision; external documentation was checked on 4 October 2026. They support the baseline and API facts, not a claim that the proposed implementation exists.

- **R1 — PR baseline and idle/durability changes:** [PR #9](https://github.com/ineersa/sqlite-queue/pull/9), head `1d8f6f8bd34376a675cdd768be9daf51573ef17c`.
- **R2 — Existing policy and SQL:** [Queue.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Queue.php), [SqliteQueueStorage.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Sqlite/SqliteQueueStorage.php).
- **R3 — Repository engineering and test conventions:** [AGENTS.md](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/AGENTS.md).
- **R4 — Locked driver behavior and defaults:** [WorkerProcess.php](https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/src/Internal/WorkerProcess.php), [SqliteConfig.php](https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/src/SqliteConfig.php).
- **R5 — Existing cancellation/commit tests:** [QueueTest.php, lines 300–505](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/tests/Queue/QueueTest.php#L300-L505).
- **R6 — Transaction-acquisition clock tests:** [QueueTest.php, lines 700–773](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/tests/Queue/QueueTest.php#L700-L773).
- **R7 — Broker operations, shutdown and notification ordering:** [Broker.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Broker/Broker.php), [BrokerFactory.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Broker/BrokerFactory.php), [QueueNotifier.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Broker/QueueNotifier.php).
- **R8 — Current payload/frame bounds:** [Limits.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Protocol/Limits.php).
- **R9 — Existing process lifetime integration:** [SqliteWorkerContextFactory.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Sqlite/SqliteWorkerContextFactory.php), [SqliteWorkerHandle.php](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/src/Sqlite/SqliteWorkerHandle.php).
- **R10 — Prior responsibility and SIGSTOP-scope decisions:** [broker-responsibilities.md](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/.hatfield/discussions/broker-responsibilities.md).
- **R11 — Existing measurement method:** [benchmark-method.md](https://github.com/ineersa/sqlite-queue/blob/1d8f6f8bd34376a675cdd768be9daf51573ef17c/docs/benchmark-method.md).
- **E1 — AMPHP process contexts/channels:** [Parallel processing](https://amphp.org/parallel).
- **E2 — PDO SQLite transaction-mode attribute:** [Pdo\Sqlite manual](https://www.php.net/manual/en/class.pdo-sqlite.php).
- **E3 — SQLite WAL durability:** [WAL documentation](https://www.sqlite.org/wal.html), [synchronous pragma](https://www.sqlite.org/pragma.html#pragma_synchronous).
- **E4 — Explicit PDO binding and binary strings:** [PDO SQLite driver](https://www.php.net/manual/en/ref.pdo-sqlite.php), [PDOStatement::bindValue](https://www.php.net/manual/en/pdostatement.bindvalue.php).
- **E5 — Statement result release:** [PDOStatement::closeCursor](https://www.php.net/manual/en/pdostatement.closecursor.php).
- **E6 — SQLite transaction and failed-commit behavior:** [Transactions](https://www.sqlite.org/lang_transaction.html).
