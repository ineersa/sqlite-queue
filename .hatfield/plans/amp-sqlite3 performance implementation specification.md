Improve repeated small-query throughput while retaining the dedicated SQLite worker, responsive parent event loop, transaction ownership, and existing result semantics. Begin with the current driver, reduce work inside each request, and add batching only after measurements establish that IPC remains material.

The first implementation series is Steps 0 through 4 below. Step 5 is an optional row-processing improvement. Step 6 is a separately justified API extension. More workers, a replacement channel, and a worker-side programming language are outside the first series.

## Reviewed baseline

Analysis date: 8 October 2026.

| Component | Revision and role |
| - | - |
| Driver under review | ineersa/amp-sqlite3 main at `3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d` |
| Equivalent upstream release | fabpot/amp-sqlite3 v1.1.0 resolves to that same commit |
| Historical release | v1.0.0 resolves to `1ee168273e29af037a5c4576349ff89e3a53b5fd` |
| Historical queue source used to understand call shape | ineersa/sqlite-queue `6d492bf7ae0cb0ed408c53b38d1a7667e7a45eb0`, src/Queue.php |
| Native diagnostic | Local SQLite 3.45.1 through its C API, reproducing the PHP execute and fetch sequence |

The supplied benchmark counts describe an older cohort and include startup and warmup. They are not measurements of the current fork. The historical queue source establishes an eleven-statement successful lifecycle; it is not asserted to be the exact source revision of that profiled run. Its WAL/FULL setting also differs from the previously discussed WAL/NORMAL benchmark. Pin the actual benchmark caller and its effective settings before measuring an implementation. [S1] [S2] [S3]

The source review covers the worker, channel, connection and transaction leases, statements and statement pooling, result fetching, connection pooling, configuration, and existing regression tests. The native probe establishes one engine mechanism. No PHP test suite or PHP throughput benchmark was executed in this analysis because the execution environment has no PHP binary.

## Findings from the historical profile

### The reported preparation totals closely match the old code

The supplied counts are:

| Activity | Reported calls |
| - | -: |
| Worker execute dispatches | 33,248 |
| Explicit native statement preparations | 54,309 |
| Native statement executions | 54,309 |
| querySingle calls | 60,336 |
| EXPLAIN analyses | 18,052 |
| Table-definition parses | 3,006 |

There is a useful accounting check:

`33,248 + 18,052 + 3,006 = 54,306`

That is only three below the reported 54,309 preparations. One preparation for each directly executed statement, another for EXPLAIN, and another for insert-target inspection therefore explain almost the entire reported preparation count. This is a strong consistency check, not a time attribution or proof of the remaining three calls.

In v1.0.0, execute reads total_changes before every execution and again for commands without result columns. It also changes the authorizer around metadata preparation and analyzes non-read-only statements using EXPLAIN. BEGIN IMMEDIATE qualifies as non-read-only, so the old implementation can analyze transaction starts as well as DML. Insert attribution reads and parses the target table definition. These costs occur inside the worker. [S2] [S4]

The eleven-request shape is also understandable from the historical caller:

| Queue operation | Successful SQL sequence | Driver requests |
| - | - | -: |
| Send | BEGIN, INSERT, COMMIT | 3 |
| Claim | BEGIN, SELECT identity, UPDATE reservation, SELECT payload, COMMIT | 5 |
| Settle | BEGIN, DELETE, COMMIT | 3 |
| Total | One successful message lifecycle | 11 |

One application execute call emits one execute request. The driver does not expand it into eleven IPC messages. The application lifecycle supplies those SQL requests; metadata adds native work within requests. Empty claims, failed operations, startup, and resource traffic add further work. Dividing the mixed cohort by 3,000 is descriptive, not an exact steady-state cost per message. [S3] [S5]

### Several historical costs are already fixed

| Historical cost | Current v1.1.0 behavior |
| - | - |
| Two total_changes queries around commands | Replaced with changes-based DML row counts |
| Reinstalling and removing the authorizer | One permanent authorizer |
| Reanalyzing prepared write metadata on every execution | Metadata retained and marked stale when authorization activity indicates possible recompilation |
| EXPLAIN for each direct write | DML row-production decision cached for up to 256 SQL texts, each at most 4,096 bytes |
| Table-definition parsing | Replaced by schema and index metadata |
| Repeating rowid-table classification | Cached with schema-version and invalidation checks |
| Removing the first array element for each returned row | Position-based iteration |

The row-count change is also a semantic correction: v1.1.0 counts direct DML changes, excluding trigger writes and virtual-table internals. Retain these semantics when upgrading and optimizing. Do not reimplement the improvements already present. [S1] [S6]

## Remaining execution costs

### Direct execution still prepares user SQL every time

Connection::run sends SQL and parameters for every direct call. WorkerProcess::execute calls prepareSingleStatement and normally closes that native statement after the command or result finishes. The DML analysis cache stores a boolean, not a reusable native user statement. StatementPool manages explicitly prepared pooled wrappers; it is not a cache for direct execute calls. [S5] [S6] [S7]

Under stable schema and a warm metadata cache, the approximate PHP-level worker path is:

| Statement | User preparation | User execution | Additional scalar helper executions |
| - | -: | -: | - |
| Transaction control | 1 | 1 | Normally none |
| SELECT | 1 | 1 | Normally none |
| UPDATE or DELETE | 1 | 1 | count_changes guard |
| Successful ordinary INSERT | 1 | 1 | count_changes guard and schema_version check |
| Reused explicit prepared DML | 0 per execution | 1 | count_changes guard, plus insert attribution where applicable |

These are source-derived warm-path descriptions. They exclude SQLite automatic repreparation, PHP's internal stepping behavior, cold EXPLAIN, cold table classification, trigger work, and exceptional paths.

### Public prepared statements do not solve repeated short transactions

A statement prepared through a transaction belongs to that transaction and is closed when it finishes. A statement prepared on the connection acquires nontransactional connection ownership when executed; attempting to use it while a transaction holds the connection can wait behind that transaction.

Consequently, preparing queue statements once on the connection and then calling them inside successive transaction objects is not a valid drop-in optimization. Preparing them afresh inside every short transaction adds prepare and close traffic. A worker-private cache can reuse native handles across transaction lifetimes while preserving public statement ownership. [S7] [S8]

### A cached scalar check still recompiles internally

Current queryInternal caches a prepared PRAGMA count_changes statement, executes it, fetches its scalar value, and finalizes the result. PHP's SQLite3Stmt::execute steps the statement and resets it; fetchArray steps it again. SQLite 3.45.1 marks most PRAGMA programs for one execution, including count_changes. Its schema-version reads have an explicit reusable-program exception. [S6] [S9] [S10]

A C-API probe using the PHP-style reset and step sequence produced:

| Ten logical scalar reads on SQLite 3.45.1 | Explicit preparations | Automatic repreparations | SQLite step calls |
| - | -: | -: | -: |
| Cached count_changes with PHP-style execute and fetch | 1 | 19 | 20 |
| Single-shot count_changes prepare, step, finalize | 10 | 0 | 10 |
| Cached schema_version with PHP-style execute and fetch | 1 | 0 | 20 |

Automatic repreparations were read from SQLITE_STMTSTATUS_REPREPARE, not inferred solely from function names. The single-shot read also observed OFF, executed ON, executed OFF, and prepared-only ON as 0, 1, 0, 1.

This supports a small optimization experiment: use querySingle for the count_changes scalar and retain the cached schema-version helper. It does not establish a PHP speedup. A profiler could show more querySingle calls after a successful optimization while the engine performs fewer compilations.

### Parent and worker rendezvous remain serialized

WorkerChannel holds a mutex across send and receive. ConnectionLeases separately protects connection and transaction ownership; unfinished cursors retain a lease. Transaction operations and completion also use a state mutex. Removing only the channel mutex leaves the other serialization gates; removing all of them changes correctness and lifecycle contracts. [S5] [S8] [S11]

The current implementation already includes the initial row batch in the execute response, uses lookahead at batch boundaries, and finalizes exhausted native cursors. The remaining row path rebuilds the column-name/type lookup on every row. There is also redundant closeResult traffic when a fully buffered result is explicitly closed or dropped before all its local rows are consumed. [S6] [S12]

## Invariants for every implementation step

1. SQLite execution and lock waits stay in the worker process. Parent event-loop work remains runnable while the worker is blocked.

2. Public query, execute, prepared-statement, transaction, result, BLOB, and pool behavior remain compatible unless an API change is explicitly documented.

3. Preserve result-specific nullable insert IDs and direct-DML row counts. The historical queue send path requires a non-null integer insert ID.

4. Preserve rejection of row-producing DML before its execution. RETURNING and count_changes can expose PHP's repeated-execution behavior; removing the guard is not a performance optimization.

5. Preserve transaction, savepoint, result-lease, and statement ownership. Unrelated fibers must not enter another transaction.

6. Preserve binary values, omitted-parameter behavior, duplicate column names, numeric-looking column keys, and per-row SQLite types.

7. Never retry an application write automatically after an uncertain error, worker loss, or lost commit response.

8. Bound newly retained statements, SQL text, batch input, and diagnostic data. Clear parameter values before retaining idle cached statements.

9. Keep original SQL errors and primary/extended codes through cleanup. Close a connection whose transaction state cannot be recovered.

10. Preserve durability configuration and transaction boundaries in driver-only comparisons.

SQLite documents that changes and last_insert_rowid have different scopes and exceptions; neither a global insert-ID read nor a total_changes delta substitutes for the existing result contract. [S13] [S14]

## Step 0 Establish the baseline

### Implementation

Create a small benchmark runner under bench with documented commands, machine-readable output, and bounded aggregate diagnostics. Keep this specific to driver performance; a general telemetry framework is unnecessary.

Pin the current driver commit and the actual async queue caller commit. Record the Composer lock hash and any dirty diff. First compare the historical release and current release with the same caller where the historical release is runnable; subsequent candidates compare against the pinned v1.1.0 baseline.

Separate startup, schema creation, warmup, measured execution, drain, verification, and shutdown. Capture phase deltas, not only process-lifetime totals.

Collect:

- Requests by operation, with SQL classified as query, DML, transaction control, or diagnostic work.

- User preparations and executions; internal preparations and executions split into EXPLAIN, count_changes, schema-version reads, table classification, and metadata refresh.

- Cache hits, misses, evictions, retained entries and SQL bytes, and active statements/results.

- Rows and payload bytes transferred; connection/transaction lease wait; channel wait; parent request duration; worker handling duration.

- Parent and child user/system CPU, memory measurements with their definitions, busy/locked errors, and unexpected failures.

Counter hooks must be internal and disabled in normal operation. Keep reports outside timed regions, avoid synchronous per-query logging, and separate instrumented diagnostic runs from throughput runs. Count engine recompilation where the runtime permits it; otherwise use a small native companion probe or explicitly label authorizer activity as a proxy. PHP prepare-call counts alone are insufficient.

### Acceptance

- A benchmark result identifies both caller and driver and records effective owning-connection pragmas.

- The runner distinguishes initial and steady-state work and publishes failed runs.

- The same deterministic query sequences produce the same rows, IDs, row counts, and database contents in each variant.

- Diagnostic counters reconcile with the known workload; timing runs report whether instrumentation is enabled.

- The baseline includes an external-lock responsiveness test and an unchanged durability configuration.

## Step 1 Make the count_changes check single shot

### Scope

WorkerProcess::queryInternal, producesRows, and assertExecutable. Preserve the existing safety decision and its timing.

Add one dedicated helper for reading count_changes with SQLite3::querySingle. Use it at the two current guard sites. Keep schema_version on the existing cached prepared path; the native probe demonstrates why these scalars should not be optimized identically. [S6] [S9] [S10]

Do not cache the count_changes value in a PHP boolean. A PRAGMA setter can take effect during prepare, including before a request subsequently fails validation. Tracking successful execute calls alone is insufficient.

This change must preserve handling of an unavailable deprecated PRAGMA and all existing exceptions. Keep the long-SQL and row-producing-DML fallback behavior. It must not weaken RETURNING detection or alter the user SQL.

### Tests and performance gate

- Read OFF, set ON, read ON, set OFF, read OFF.

- Prepare a setter without executing it, and prove the next guard observes the effective state.

- Exercise script, direct, prepared, EXPLAIN PRAGMA, schema-qualified, and rejected-request paths that can alter or expose the state.

- Reuse existing direct/prepared DML-becoming-row-producing tests; assert no write occurs before rejection.

- Compare scalar-only and DML loops on PHP 8.4 and 8.5 with the relevant linked SQLite versions.

Merge if the PHP measurements confirm a useful reduction in scalar/DML overhead without semantic regressions. Retain the existing implementation if the target runtime does not benefit. The native mechanism is established; its share of end-to-end latency is still to be measured.

## Step 2 Reuse native statements across direct executions

### Configuration and boundaries

Add withStatementCacheSize and getStatementCacheSize to SqliteConfig and pass the value through SqliteConnector and startup validation. Proposed initial defaults are 64 retained idle statements and a 4,096-byte maximum cacheable SQL text; size 0 disables the cache. These are implementation defaults to validate, not measured optimal values.

Use exact SQL bytes as the key. Do not normalize whitespace, comments, placeholder styles, or literal values. Parameterized statements should provide reuse naturally. Cap entries and cacheable SQL length, giving at most 256 KiB of retained key text at the proposed defaults. That is not a strict native-memory cap: SQLite VM objects have additional size, which must be measured.

Cache only ordinary SELECT, INSERT, REPLACE, UPDATE, DELETE, and applicable WITH statements after the existing single-statement validation. Exclude PRAGMA, transaction control, DDL, ATTACH/DETACH, VACUUM, and EXPLAIN from this first cache. Most PRAGMAs do not obtain the expected compilation saving, and their prepare-time effects require separate care.

### Ownership

Keep cached implicit statements separate from the publicly prepared statement-ID map. Use one small worker-local cache abstraction if that simplifies ownership; do not build a generic cache framework.

The lifecycle is:

1. Validate request types, NUL restrictions, parameters, and query-versus-execute rules. An exact full-SQL cache hit reuses the entry's established single-statement validation. On a miss, perform native preparation and consumed-tail validation before admitting the statement; do not prepare again merely to validate a hit.

2. Remove a borrowed handle from the idle cache.

3. Reset and clear bindings before binding the new parameter set.

4. Run the same executable guard used for reused prepared DML, including count_changes changes since its original preparation.

5. Execute with existing authorization and stale-metadata tracking.

6. For a command, capture row count and insert ID, finalize the result, clear/reset the handle, and return it to the idle cache.

7. For a row result, transfer ownership to the worker result resource. Return the handle only after that cursor is finalized.

8. If the cache is disabled or the statement is ineligible, close the idle handle. Otherwise return it as the most recently used idle entry. At capacity, evict and close the least recently used idle entry before admission. If the same key already has an idle handle, retain one and close the duplicate.

Evict only idle handles. An active result must never be reset or closed to make space. The current connection serialization limits ordinary overlap, but the cache's ownership rule must stand independently of that optimization.

On cache release, clear SQLite bindings and PHP-held parameter values, including large BLOBs. Preserve omitted parameters becoming NULL; a later call must never inherit previous bindings.

### Error and schema handling

Introduce explicit try/finally ownership from handle acquisition until it is either released or transferred to results. Binding, execution, insert attribution, column metadata, and first fetch can all fail. Every path must dispose of its handle exactly once.

On an error, discard the borrowed implicit handle in the first implementation. That is simpler than recovering it for reuse. Capture the original error before reset/finalization can disturb native error state. Do not repeat user SQL to recover a cache entry.

Keep the permanent authorizer, stale-metadata refresh, schema-version checks, and rowid-table invalidation logic. SQLite can automatically reprepare retained statements; a retained PHP object does not guarantee unchanged metadata.

Flush idle implicit handles around restore and conservative schema/configuration boundaries, including scripts and ATTACH/DETACH attempts. Do not flush merely because BEGIN, COMMIT, ROLLBACK, SAVEPOINT, or RELEASE runs; retained handles must survive ordinary short transactions. Closing a cached statement must not step it. A conservative schema/configuration flush may be broader than necessary initially; optimize invalidation only after it becomes a measured cost. Continue to rely on SQLite recompilation and existing metadata refresh for external schema changes, not only local SQL classification.

Explicit public prepared statements keep their current identity and transaction lifetime. Do not return a handle still owned by a public Statement object to the implicit cache.

### Tests and acceptance

- Repeated eligible SQL after warmup uses no additional driver user-statement preparations while resident and schema-stable.

- The same SQL can be reused across successive committed and rolled-back transaction objects.

- Cache-disabled behavior remains equivalent to the current driver.

- Capacity overflow, SQL longer than the cap, unique-SQL churn, and duplicate idle-key handling stay bounded.

- BLOB, NULL, boolean, integer, float, string, named, numeric, and omitted bindings remain correct.

- A query call with placeholders still rejects them after the same SQL was cached by a parameterized execute call; cache hits do not bypass query-versus-execute validation.

- Partially read, exhausted, explicitly closed, and dropped results release resources exactly once.

- Binding failure, query failure, busy/locked failure, and later-fetch failure do not leak or double-close handles.

- Preserve external table recreation, temporary table shadowing, failed-execution recompilation, schema rollback, reattach, restore, trigger, UPSERT, WITHOUT ROWID, and virtual-table tests.

- An error followed by a fresh execution succeeds where it does today, without replay of the failed write.

The performance claim is fewer user preparations and less repeated metadata capture. IPC request count is expected to remain unchanged.

## Step 3 Give transaction control a small private path

### Implementation

Change Connection::executeControl and its transaction call sites to send a typed internal control request. The worker constructs a single command from a validated action, transaction mode, and driver-generated savepoint identifier. Execute it through SQLite3::exec and return a void response.

Supported actions are begin, commit, rollback, savepoint, release-savepoint, and rollback-to-savepoint. Validate mode against the existing enum and savepoint names against the driver's generated identifier form. This is not an arbitrary-SQL fast execution endpoint.

Retain the current transaction-idle checks and all connection/state locks. Include the new request in worker.php's SQL-operation error classification so failures remain SqliteQueryError with the correct SQL and codes.

This removes generic PHP statement/result-metadata handling and its result-payload validation. SQLite still compiles and executes the control SQL internally. Request count remains unchanged; changing a profiler's method label does not prove less engine work.

### Tests and acceptance

- Top-level and nested commit/rollback preserve order and callbacks.

- A failed explicit commit leaves the transaction active, as it does today.

- Commit cannot race an executing transaction statement.

- Unread result/BLOB ownership still blocks or rejects completion according to the current contract.

- A blocked BEGIN leaves the parent responsive and connection close settles pending operations.

- Invalid typed payloads fail through the protocol contract rather than reaching arbitrary SQL execution.

- Each control operation returns a void payload and avoids the generic user-result path.

Measure short-transaction workloads after the statement-cache step. Retain the change for demonstrated overhead reduction and simpler control handling; do not attribute its unchanged round-trip count to batching.

## Step 4 Avoid remote closure of an exhausted cursor

WorkerProcess has already finalized a native cursor when it returns exhausted=true. Result::close and the destructor cleanup path should send closeResult only while a remote cursor can still exist. Continue local closure, buffer disposal, lease release, and completion callbacks exactly once.

Use the existing exhausted flag. Do not encode exhausted row results with result_id=null: the current response validator uses that shape to identify command results.

Required cases:

- Close a fully buffered result before consuming any row.

- Consume only part of a fully buffered result, then close or drop it.

- Exhaust a later fetch batch and then close.

- Close a genuinely streaming result; it still sends one necessary close request.

- Preserve local buffered-row readability after connection close where currently supported.

- Verify pool release and destructor behavior.

Acceptance is zero unnecessary remote close requests for exhausted cursors, unchanged public behavior, and no abandoned live worker results.

The historical queue's one-row SELECTs are normally consumed to exhaustion by fetchRow. Do not claim that this optimization removes a close request from every queue SELECT; its benefit depends on result-consumption patterns.

## Step 5 Reduce work per returned row when profiling justifies it

Use the column names already collected for a native result to build a name-to-last-column-index mapping once per execution. Preserve PHP's numeric-key coercion and duplicate-name behavior.

For each fetched row, call columnType only for string values that may be either TEXT or BLOB. Non-string values do not need BLOB classification. Never cache a column's storage type across rows, and never reuse a result-shape map across statement executions.

Test mixed NULL, integer, real, text, and BLOB values in one result column; conflicting duplicate aliases; numeric-looking aliases; binary bytes; changed SELECT-star shape after schema changes; and all fetch-batch boundaries.

Keep the existing initial batch, lookahead, exhaustion detection, and binary protocol representation. Benchmark narrow one-row queries separately from wide/many-row results. Merge only if row-processing CPU materially falls for a relevant workload.

Byte-bounded fetching is a separate feature. Current batch size limits row count, not bytes. If added later, define an oversized-single-row policy explicitly; a byte target that permits one large row is not a strict memory limit.

## Step 6 Add bounded command batches only if IPC still dominates

### Decision gate

Reprofile the combined earlier steps first. If time is now mainly SQLite execution or durable commit, batching driver messages may have limited value. If parent/worker rendezvous and parent-side bookkeeping remain substantial, implement a narrow parameterized command-batch API.

Existing executeScript already executes a parameterless script atomically in one request. It is useful for setup and migrations but provides neither bound parameters nor per-command results. Do not interpolate queue payloads into scripts or create a second SQL parser to retrofit them. [S6]

### Proposed public contract

Add one executeBatch method to SqliteExecutor, implemented by connection, transaction, pool, and pooled-transaction adapters:

```php
/**
 * @param list<array{
 *     sql: string,
 *     params?: array<array-key, null|bool|int|float|string|SqliteBlob>
 * }> $commands
 * @return list<SqliteResult>
 */
public function executeBatch(
    #[SensitiveParameter] array $commands,
): array;
```

The attribute above refers to PHP's built-in SensitiveParameter. Each returned result is a completed command result with its own row count and attributed insert ID, no rows, no live cursor, and no retained connection lease.

Reuse the existing SqliteResult implementation initially. Do not release the batch lease once per constructed result; the operation acquires and releases one lease in total.

Adding a method to a public interface affects external implementers and mocks. Document that compatibility effect and select an appropriate release strategy.

### Supported input

The first batch supports only non-row-producing INSERT, REPLACE, UPDATE, and DELETE, including WITH forms that resolve to those operations. Keep the current RETURNING and count_changes rejection.

Reject SELECT, DDL, PRAGMA, ATTACH/DETACH, VACUUM, EXPLAIN, user transaction control, mixed result streaming, callbacks, and result-dependent parameter references. All parameters must be supplied before dispatch. An empty list returns an empty list without dispatch.

Use configurable limits with conservative initial values of 64 commands, 4,096 total parameters, and 1 MiB of logical input bytes, validated before dispatch and again in the worker. Define logical bytes as SQL text, parameter names, string/BLOB bytes, and documented fixed accounting for scalars. The aggregate parameter-count cap prevents many NULL parameters from defeating the byte budget. These limits bound input, not exact allocator or serializer overhead.

Never silently split an atomic batch to satisfy a cap. Reject it before execution. Avoid adding a family of execution policies or an atomic boolean with differing meanings on different executors.

### Connection and pool semantics

Acquire one connection for the full batch. The worker:

1. Validates the complete payload and rejects prohibited first-keyword classes before native preparation, because a disallowed PRAGMA setter can change connection state during prepare.

2. Begins using the configured transaction mode.

3. Prepares or borrows each command in order, establishes native DML eligibility including WITH forms, binds its parameters, and executes it through the shared execution/cache path.

4. Captures each command's result metadata immediately.

5. Commits.

6. Returns the ordered result list only after successful commit.

On command or commit failure, roll back. No completed-success list is returned on failure.

Preflight guarantees no command execution for malformed/oversized input and prohibited first-keyword classes. SQL, binding, and native eligibility errors discovered in a later allowed command follow ordered execution and roll back earlier SQLite effects. Do not promise that every SQL error is discovered before the first command, and do not add an all-statements preparation pass merely to make that promise.

### Transaction semantics

Acquire the existing transaction state lock and one transaction lease. Use a driver-generated savepoint around the batch. Release it on success; on failure, roll back to it and release it. This provides atomicity for the batch without committing the caller's outer transaction.

The additional savepoint commands are native work inside the same request. Existing nested-transaction ownership rules still apply.

OR ROLLBACK and some SQLite errors can remove the enclosing transaction. If rollback/savepoint cleanup cannot restore a known state, close the connection and invalidate dependent transaction objects. Never return uncertain state to the pool.

### Errors and resources

Capture the failing phase, command index where applicable, original SQL, primary SQLite code, and extended code before rollback cleanup. The current worker.php handler reads extended state after handle throws, which is too late if handle already executed cleanup SQL. Introduce a small internal captured-error representation and a validated batch error response.

Represent BEGIN and COMMIT failures as phases without inventing a command index. A command failure carries its zero-based index. Public error SQL must identify the actual failing SQL, not the concatenation of the entire batch. Parameter values remain sensitive.

Expose those fields through a small SqliteBatchQueryError subclass of SqliteQueryError with getPhase returning begin, command, or commit, and getCommandIndex returning an integer only for the command phase. For a transaction-scoped batch, savepoint creation maps to begin and successful-scope release maps to commit. Keep inherited SQL and result-code accessors. Cleanup failure or connection loss remains a connection exception, with the captured original batch error retained as its cause where available.

Close/evict or return every borrowed statement exactly once. Completed command results retain no native cursor. If worker loss occurs during execution or commit, preserve an unknown-outcome connection error and do not retry automatically.

Bounded batch size does not bound lock-wait time or SQL runtime. The parent should remain responsive, but other users of that same worker can wait behind the batch. Report both properties separately.

### Batch acceptance tests

- Ordered rows-changed and insert-ID metadata match equivalent individual commands.

- Failed middle command rolls back the owned batch; the outer transaction remains usable only when savepoint recovery succeeded.

- Deferred constraint failure at COMMIT returns failure and leaves no committed partial batch.

- Malformed, oversized, or lexically prohibited input causes no command execution; a later SQL/binding/native-eligibility error rolls back earlier SQLite effects.

- BEGIN, command, COMMIT, rollback, malformed response, worker-loss, and connection-close paths settle all callers without replay.

- Binary values and parameter binding remain equivalent.

- Pool size one can execute successive batches without retaining connections in returned results.

- One eligible batch produces one request and response, excluding initialization/diagnostics.

- Compare with identical transaction grouping. Fewer commits from grouping multiple messages is a separate application change.

### Integration limits for this queue

The historical send requires an integer insert ID, and claims use a SELECT result to form later parameters. Settlement checks row counts and session state before committing. The historical async send samples availability before BEGIN, while its claim and settlement sample inside the transaction. The separately reviewed PDO storage revision b1834387 samples insert, claim, and settlement time after BEGIN has acquired the SQLite write lock, so moving those calculations before a worker-side BEGIN changes behavior under contention. Preserve the actual pinned caller's clock contract. The newer PDO source is evidence of integration requirements, not the async benchmark baseline. [S3] [S17]

A generic prebound command list cannot preserve all of those decisions automatically. Do not promise that this API reduces every send, claim, and acknowledgement to one request.

Any queue migration must explicitly verify:

- When availability and reservation clocks are sampled.

- Whether row-count validation must happen before commit.

- Whether a session may disconnect between dispatch and completion.

- How empty claims and invalid receipts roll back.

- Whether a SELECT-to-UPDATE dependency can be removed by SQL changes.

Keep that integration in a separate queue PR. A command batch can be useful without encoding queue names, clocks, branches, or callbacks inside the generic driver. If the real workload needs whole-operation IPC, assess a separate operation-oriented adapter after measuring the optimized driver.

## Changes to defer

### Omitting insert-ID metadata

A future explicit execution policy could omit insert attribution when an application never needs it, with current behavior as the default. It must be selected before execution and return deliberately absent metadata. A lazy getLastInsertId query is incorrect after other statements have run.

This is not a primary improvement for the supplied queue: send needs the ID, while ordinary UPDATE and DELETE already avoid rowid-table attribution. Preserve the current API until another measured workload justifies an opt-out.

### Tracking count_changes or schema state without reading SQLite

The single-shot count_changes check avoids a mutable-state tracking system. Eliminating the read entirely would require handling startup, successful and failed prepares, scripts, EXPLAIN PRAGMA, prepared-only setters, and supported custom behavior. Never query SQLite from inside its authorizer.

Schema-version checks protect insert attribution against external DDL, temporary shadowing, rollback, reattachment, and restore. Retain them until their remaining cost justifies a separately designed invalidation mechanism.

### Channel pipelining

The current request ID is validated for one active exchange; it is not a multiplexed dispatcher. A future pipeline needs one reader, bounded pending requests and bytes, response dispatch, failure fanout, close ordering, and explicit transaction/cursor ownership.

Implementing only the channel portion will not remove the higher-level serialization. Do not include that rewrite in the initial performance series.

### Backend and durability changes

Keep SQLite3 and AMPHP for the initial series. A PDO worker experiment changes available APIs and result semantics, including BLOB handling, callbacks, backup/restore, and RETURNING behavior. Treat it as a separate backend project.

More connections may help independent readers, but SQLite WAL still has one writer at a time. Changing synchronous mode, removing transactions, adding group commit, or introducing a different writer architecture changes the comparison and potentially durability. None should be presented as an intrinsic driver optimization. [S15]

## Benchmark matrix and release gates

### Runtime and workload controls

Record PHP version and extensions, linked SQLite version and compile options, event-loop driver, OS, CPU, storage/filesystem, assertions, JIT/OPcache, profiler state, and worker/connection count.

Read effective journal_mode, synchronous, busy_timeout, foreign_keys, cache_size, and wal_autocheckpoint from the owning connection. Keep WAL/NORMAL and WAL/FULL in separate result sets.

| Workload | Purpose |
| - | - |
| SELECT 1 with direct and explicit prepared calls | Real request/response floor |
| Repeated INSERT, UPDATE, DELETE and no-op DML | Preparation, scalar guards, and result metadata |
| Identical DML within one long transaction and across many short transactions | Distinguish preparation from transaction/commit overhead |
| Historical eleven-operation lifecycle with fixed SQL and data | Comparable queue-shaped driver path |
| Actual pinned async queue integration | End-to-end impact without caller changes |
| Empty claims and failed settlements | Non-happy-path cost and correctness |
| 0, 1, batch-minus-one, batch, batch-plus-one, and twice-batch rows | Result boundaries and cleanup traffic |
| 256-byte and 16-KiB payloads, plus an explicitly labeled large-payload stress case | Copying, serialization, binding retention |
| SQL working set below capacity and churn above capacity | Cache effectiveness and bounds |
| Competing fibers on one connection | Lease wait and transaction ordering |
| Parent heartbeat while another process holds the SQLite write lock | Event-loop responsiveness |

Use native ext-sqlite3 as the first local diagnostic reference because it matches the binding. PDO and the previous coarse worker are additional architectural references, not automatically equivalent baselines.

Preserve the 3,000-message finite cohort for continuity. Count unique confirmed acknowledgement completions, with cohort time measured from the release barrier through the final required acknowledgement. Publish attempts, send confirmations, duplicate IDs, unfinished IDs, and uncertain outcomes separately. A failed or partial cohort has no healthy completed-cohort throughput result. If claiming sustained capacity, add a separate sustained run; cohort drain rate does not establish it.

### Experimental sequence

Run paired baseline/candidate comparisons in alternating or randomized order. Schedule seven pairs in advance and retain/report every failure; do not replace failed pairs to obtain seven successful ones. Expand only for a declared noise investigation, a specific unresolved risk, or a separately recorded rerun after a fix. Calibrate short microbenchmarks to a useful duration instead of reporting one fast iteration.

Measure each change independently, then the combination:

1. v1.1.0 baseline.

2. Single-shot scalar check.

3. Implicit statement cache, including enabled/disabled comparisons.

4. Private transaction-control path.

5. Exhausted-cursor cleanup.

6. Optional row conversion.

7. Optional batch API and its separate caller integration.

Publish elapsed time, completed operations, paired median change, dispersion, user/system CPU per operation, memory definition, request counts, and preparation/execution counts by purpose. Report latency distributions from samples; do not average per-run p99s and call the result an overall p99.

Do not add overlapping parent wait, worker time, and wall time as though they were independent totals. Diagnostic timing should explain the critical path; uninstrumented runs establish throughput.

### Mechanism gates

| Change | Required evidence |
| - | - |
| Scalar check | Same observed flag and rejection behavior; fewer native steps/compilations for the helper on the tested runtime |
| Native cache | Preparations approach distinct resident SQL count after warmup; bounded retained handles and cleared bindings |
| Control path | Correct void protocol and less generic PHP handling; unchanged transaction semantics |
| Cursor cleanup | Zero remote closes for already exhausted cursors |
| Row conversion | Column-name mapping built per result, with reduced conversion CPU and preserved types |
| Batch | One request per eligible batch, correct atomicity, and identical transaction grouping in comparisons |

The proposed decision policy is to require a repeatable targeted throughput or CPU improvement for added complexity. A 5 percent paired median improvement is a useful initial materiality threshold, not a prediction. Reject unexplained repeatable regressions in relevant non-target workloads. Tiny simplifications may still be worthwhile when they clearly remove redundant work and preserve behavior.

### Responsiveness and failure gates

Hold a write transaction in an independent process for 500 ms while a driver write waits. Run a 10-ms periodic timer and an unrelated parent-side control operation during the interval. Assert that callbacks/control work complete before lock release; report timer lateness and control latency against the unloaded baseline. A query on the blocked SQLite connection is not an event-loop responsiveness probe.

Set practical latency thresholds on the dedicated benchmark host before comparing candidates. The essential failure condition is callbacks stalling for the SQLite lock interval; do not disguise that with an overall average.

Also test:

- Closing the connection while a child operation is blocked settles pending work and terminates the child.

- Worker failure before commit leaves no confirmed partial transaction after reopen.

- Data from an acknowledged commit survives a process-kill/reopen test under the preserved configuration.

- A lost response around commit is treated as an unknown outcome, without automatic replay.

- Database integrity, exact IDs, payloads, row counts, and fenced settlement remain correct.

- Failed startup, dropped results/statements/transactions, and pool closure leave no newly introduced leaks.

Process-kill tests do not prove survival of a machine power loss. Keep the documented synchronous configuration and its guarantees unchanged.

### Existing regression anchors

Retain the current composer check gate, including coding style, PHPStan, and PHPUnit. Run the existing supported PHP 8.4/8.5 and platform/dependency CI matrix. Add focused regressions to the existing suites rather than mirroring implementation internals.

| Concern | Existing test anchors |
| - | - |
| Result metadata | SqliteQueryTest methods for trigger changes, non-rowid inserts, UPSERTs, zero command counts after DML |
| Schema changes | Prepared-insert table recreation, temporary shadowing, failed recompilation, schema rollback and reattach cases |
| Unsafe row-producing DML | Direct and prepared DML-becoming-row-producing tests; EXPLAIN-of-DML test |
| Parameter and statement reuse | SqliteStatementTest reset-bindings, reuse-after-error, and reexecute-closes-result tests |
| Transaction ownership | Commit-versus-execute, statement-versus-commit, failed-commit-stays-active, nested-savepoint tests |
| Shutdown and protocol | SqliteConnectionTest blocked-close and stopped-child tests; WorkerResponseTest |
| Result and pool release | Immediate command release, last-row release, sequential prepared executions, destructor tests |
| Restore | SqliteBackupTest restored-table-storage insert-ID test |

Existing testReusesNativeStatement verifies behavior, not native preparation counts. Use benchmark diagnostics to establish the reuse mechanism. [S16]

## Implementation sequence and delivery

Each step should be a reviewable commit or small PR with a baseline SHA, focused change, relevant tests, diagnostic counter delta, and paired performance result.

Primary edit locations:

| Step | Main production locations |
| - | - |
| 0 | bench runner and narrow optional diagnostics in WorkerProcess and WorkerChannel |
| 1 | Internal/WorkerProcess.php scalar helper and guard sites |
| 2 | SqliteConfig.php, SqliteConnector.php, Internal/WorkerProcess.php, optional small Internal/StatementCache.php |
| 3 | Internal/Connection.php, Internal/Transaction.php, Internal/WorkerProcess.php, Internal/worker.php |
| 4 | Internal/Result.php |
| 5 | Internal/WorkerProcess.php result state and conversion |
| 6 | SqliteExecutor.php, connection/transaction/pool adapters, worker request/response handling, and new SqliteBatchQueryError.php |

Production paths are relative to src. Update phpstan.neon.dist aliases whenever startup, statement ownership, or protocol payload shapes change. Add configuration and startup-validation tests for every new limit. The batch extension must cover Internal/PooledTransaction.php and SqliteConnectionPool.php, not only the concrete connection and worker.

| Order | Deliverable | Proceed when |
| - | - | - |
| 0 | Pinned baseline and benchmark harness | Current runtime and cost attribution are known |
| 1 | Single-shot count_changes helper | PHP measurements confirm the native-mechanism benefit |
| 2 | Worker-private implicit statement cache | Ownership, invalidation, and memory tests pass |
| 3 | Private transaction-control protocol | Transaction regressions remain green |
| 4 | Exhausted-cursor cleanup | Remote-close counts fall for affected cases |
| 5 | Optional result conversion change | Row processing is a measured cost |
| 6 | Optional bounded batch API | IPC remains material and a real caller can use the contract |

At the end of Step 4, reprofile and decide whether further work is warranted. A large improvement over v1.0.0 does not, by itself, justify every optional stage. No specific final messages-per-second target is asserted from source inspection.

Keep the architecture as an asynchronous parent and a dedicated synchronous SQLite worker. The initial objective is to make that worker execute less redundant work for the same application operations.

## Sources

- [S1 Current changelog and release baseline](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/CHANGELOG.md). Upstream [v1.1.0 tag object](https://api.github.com/repos/fabpot/amp-sqlite3/git/tags/e68aa52b93f46f95734db5f820ce9c3aa76acee0) resolves to the reviewed SHA.

- [S2 Historical v1.0.0 WorkerProcess](https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/src/Internal/WorkerProcess.php#L550-L598). The [v1.0.0 tag object](https://api.github.com/repos/fabpot/amp-sqlite3/git/tags/2d13ac3b5f01b00e6ed379e3e1a8a5348b0f8d75) resolves to this revision.

- [S3 Historical async queue source](https://github.com/ineersa/sqlite-queue/blob/6d492bf7ae0cb0ed408c53b38d1a7667e7a45eb0/src/Queue.php). Used for operation shape and required IDs, not as an exact benchmark-revision claim.

- [S4 SQLite statement read-only semantics](https://www.sqlite.org/c3ref/stmt_readonly.html). Includes the BEGIN IMMEDIATE and EXPLAIN distinctions.

- [S5 Current connection request and control paths](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/Connection.php).

- [S6 Current WorkerProcess](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/WorkerProcess.php). Execute 601–673; fetch/conversion 703–763; preparation/guards 765–848; metadata and scalar helpers 854–1069.

- [S7 Current Statement](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/Statement.php) and [StatementPool](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/StatementPool.php).

- [S8 Current Transaction](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/Transaction.php) and [ConnectionLeases](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/ConnectionLeases.php).

- [S9 PHP 8.4 SQLite extension source](https://github.com/php/php-src/blob/PHP-8.4/ext/sqlite3/sqlite3.c#L1834-L1889) and [PHP 8.5 source](https://github.com/php/php-src/blob/PHP-8.5/ext/sqlite3/sqlite3.c#L1899-L1954). Branch snapshots inspected on the analysis date; both execute implementations reset after their first native step. PHP 8.5 querySingle is at 692–765. [PHP issue 13587](https://github.com/php/php-src/issues/13587) documents repeated execution through result fetching.

- [S10 SQLite 3.45.1 PRAGMA implementation](https://github.com/sqlite/sqlite/blob/version-3.45.1/src/pragma.c#L401): run-once default at 401 and reusable header-value read at 2281. [Statement counter documentation](https://www.sqlite.org/c3ref/c_stmtstatus_counter.html) defines REPREPARE.

- [S11 Current WorkerChannel](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/WorkerChannel.php) and [worker loop](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/worker.php).

- [S12 Current Result](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/Result.php), [WorkerResponse](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/src/Internal/WorkerResponse.php), and [PHP result ownership warning](https://www.php.net/manual/en/sqlite3stmt.execute.php).

- [S13 SQLite changes](https://www.sqlite.org/c3ref/changes.html) and [last insert rowid](https://www.sqlite.org/c3ref/last_insert_rowid.html).

- [S14 SQLite PRAGMA behavior](https://www.sqlite.org/pragma.html) and [authorizer restrictions](https://www.sqlite.org/c3ref/set_authorizer.html).

- [S15 SQLite WAL concurrency and durability](https://www.sqlite.org/wal.html).

- [S16 Current test suites](https://github.com/ineersa/amp-sqlite3/tree/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/tests) and [CI configuration](https://github.com/ineersa/amp-sqlite3/blob/3e31dd28dc8f4c14e4739f81f0ca5f6d3c3f148d/.github/workflows/ci.yml).

- [S17 Queue storage timing and dependencies](https://github.com/ineersa/sqlite-queue/blob/b183438749bdfb38aea07b7c83afe6bb6fb37597/src/Sqlite/SqliteQueueStorage.php#L183-L320). This PDO implementation is separate from the historical async caller.

## Appendix Native scalar diagnostic reproduction

This probe requires Python and a discoverable libsqlite3. It measures SQLite engine counters and native step counts, not PHP wall-clock performance. It deliberately follows the inspected PHP execute, fetch, and result-finalize reset sequence.

```python
import ctypes as c
import ctypes.util
import json

s = c.CDLL(ctypes.util.find_library("sqlite3"))
s.sqlite3_libversion.restype = c.c_char_p
s.sqlite3_open.argtypes = [c.c_char_p, c.POINTER(c.c_void_p)]
s.sqlite3_prepare_v2.argtypes = [
    c.c_void_p, c.c_char_p, c.c_int,
    c.POINTER(c.c_void_p), c.POINTER(c.c_char_p),
]
for name in ["sqlite3_step", "sqlite3_reset", "sqlite3_finalize",
             "sqlite3_close"]:
    getattr(s, name).argtypes = [c.c_void_p]
s.sqlite3_stmt_status.argtypes = [c.c_void_p, c.c_int, c.c_int]
s.sqlite3_column_int.argtypes = [c.c_void_p, c.c_int]
db = c.c_void_p()
assert s.sqlite3_open(b":memory:", c.byref(db)) == 0

def prepare(sql):
    stmt = c.c_void_p()
    assert s.sqlite3_prepare_v2(
        db, sql.encode(), -1, c.byref(stmt), None
    ) == 0
    return stmt

def step_row(stmt):
    assert s.sqlite3_step(stmt) == 100  # SQLITE_ROW
    return s.sqlite3_column_int(stmt, 0)

def cached(sql, n=10):
    stmt = prepare(sql)
    values = []
    for _ in range(n):
        assert s.sqlite3_reset(stmt) == 0
        step_row(stmt)                 # PHP execute
        assert s.sqlite3_reset(stmt) == 0
        values.append(step_row(stmt)) # PHP fetchArray
        assert s.sqlite3_reset(stmt) == 0  # result finalize
    automatic = s.sqlite3_stmt_status(stmt, 5, 0)  # REPREPARE
    assert s.sqlite3_finalize(stmt) == 0
    return dict(explicit=1, automatic=automatic, steps=2*n,
                values=values)

def single(sql, n=10):
    values, automatic = [], 0
    for _ in range(n):
        stmt = prepare(sql)
        values.append(step_row(stmt))
        automatic += s.sqlite3_stmt_status(stmt, 5, 0)
        assert s.sqlite3_finalize(stmt) == 0
    return dict(explicit=n, automatic=automatic, steps=n,
                values=values)

report = {
    "sqlite": s.sqlite3_libversion().decode(),
    "cached_count_changes": cached("PRAGMA count_changes"),
    "single_count_changes": single("PRAGMA count_changes"),
    "cached_schema_version": cached("PRAGMA main.schema_version"),
}
assert s.sqlite3_close(db) == 0
print(json.dumps(report, indent=2))
```
