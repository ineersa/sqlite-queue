# sqlite-queue: PR #9 review and benchmark measurement specification v4

**Status:** Proposed measurement work, not an implemented benchmark or an optimization claim.
**Review date:** 2026-10-03.
**Reviewed head:** `632efd338b32af30665d8c93880389e8082160cd`.
**PR:** https://github.com/ineersa/sqlite-queue/pull/9
**Existing method:** `paired-v3-immediate`.
**Proposed method:** `diagnostic-v4`; comparisons must never pool v3 and v4 observations.

## 1. Executive decision

Retain PR #9 as historical integration and comparison evidence. Do not treat it as performance acceptance, a sustained-capacity measurement, or a memory-leak diagnosis. Repair the measurement system before changing the product.

The existing harness has useful foundations: real Messenger transports and a real Worker, matching serializers and payloads, explicit WAL/FULL settings, native PDO IMMEDIATE transactions, retained failed repetitions, alternating backend order, correlation-based integrity checks, and explicit caveats about short runs and delayed-message precision. Preserve those features. [S1–S3]

The important limitations are not merely insufficient sample counts. The harness performs its own database reads on the measured path, uses per-message file communication in roundtrip, flushes telemetry on each record, does not attribute resource costs to phases and roles, and does not record enough internal failure information to explain the Doctrine failures. Some limitations predate PR #9; this document describes the reviewed head, not exclusively newly introduced defects. [S4–S10]

The broker currently remains a durable SQLite-backed queue service. Publish, successful reservation, and successful ACK each commit a separate transaction. Centralizing database ownership can remove competing application writers without removing the durable-write cost. The benchmark does not establish that this architecture leaks memory or that it cannot provide useful benefits. [S11–S12]

### Evidence boundary

This review inspected the PR metadata, committed benchmark method and comparison, benchmark orchestration, receiver/publisher/session/recording/statistics/resource code, comparison logic, selected benchmark tests, and relevant queue/storage/notification paths. The GitHub connector did not expose the bytes of the compressed results archive, and direct repository download was unavailable. Therefore the numeric outcomes below are the **committed report's observations**, not independently recalculated raw samples. The benchmark and repository QA were not executed during this review. The exact failing SQL operations and the cause of the additional memory remain unverified.

## 2. Interpretation of the current reported results

The committed comparison reports the following completed measured-run ranges. [S2]

| Workload | Doctrine completed messages/s | Broker completed messages/s | Supported interpretation |
|---|---:|---:|---|
| Roundtrip | 112–117 | 105–108 | Broker is slower in this particular closed-loop harness. |
| Concurrent, 3 publishers / 2 consumers / 3,000 messages | 119 in the only clean measured repetition | 138–140 | Interesting completed-run difference, but not a repeatable healthy-baseline comparison. |
| Four publishers / one consumer | 155–248 | 177–223 | Ranges overlap; finite batch is too short for a capacity claim. |
| Three-queue backlog / two consumers | 188–242 | 199–233 | Ranges overlap; this is a small drain workload. |

All scheduled broker repetitions reportedly completed. Two measured concurrent Doctrine repetitions failed with SQLite lock errors. This is relevant reliability evidence for this workload and configuration, not proof of universal broker reliability or reproduction of the original long-handler/keepalive incident. The current handler only validates payloads, visibility is 3,600 seconds, and the benchmark does not exercise the original long-handler keepalive scenario. [S1–S2]

The reported extra approximately 90–100 MiB is a difference in sampled whole-process-tree RSS peaks. It is not a measured increase in live message objects or post-drain retained memory. [S2, S8]

### What the current ACK/s field actually calculates

`Stats::throughput()` calculates the handling rate as:

```text
number of delivery records with outcome=ack
-----------------------------------------------------------
latest successful ACK return − earliest successful handler entry
```

It is aggregate across consumers. It is not ACK RPC capacity, an individual consumer rate, or the reciprocal of ACK-call latency. The initial claim/pickup interval is excluded. Between the first and last message, handler work, receive calls, database diagnostics, recording overhead, scheduling, producer pacing, and idle gaps can all influence this window. Duplicate ACK records can inflate the raw numerator in an invalid run; existing integrity checks separately make such a run incomplete. New reports must use unique valid message IDs for goodput. [S7]

Keep the old field for historical compatibility, but label it `legacy_handling_window_acks_per_second`. Do not silently redefine it.

## 3. Required findings and fixes

### F1 — Remove database observation from the capacity/latency data path

**Priority:** P1 for measurement validity.

`Receiver::finish()` reads the stored deadline using SQL before every ACK, including nondelayed workloads. Broker consumer processes consequently open a separate diagnostic PDO/DBAL connection to the database that production transport SQL is supposed to access through the broker. Every publisher also opens a DBAL connection during `Session::execute()`. The coordinator queries database inventory repeatedly while draining; the existing baseline inventory path also checks schema existence. [S4–S6, S13]

These operations are not part of normal message delivery. They consume time and resources, and their placement and connection topology differ between backends. Equal-looking diagnostic code is not evidence of equal overhead. It is not established that these reads caused the reported failures.

**Required changes:**

- No diagnostic SQL connection in broker publishers or consumers in headline performance mode.
- No per-delivery deadline SELECT in ordinary latency or throughput workloads.
- No coordinator inventory query during the measured interval.
- Use local counters and a separate bounded control channel for completion/progress. Reconcile message identities and database inventory after the measured work and drain have ended.
- Measure stored deadlines in a separate diagnostic scenario. Prefer metadata already present in a delivery or a read-only instrumentation hook at the existing persistence boundary; do not add a database roundtrip to the headline path.
- Attribute all diagnostic operations to the observer rather than the product's logical-operation counts.
- Preserve diagnostic mode separately and measure its effect with on/off calibration runs.

### F2 — Make failed attempts and handler execution observable

**Priority:** P1 for reliability interpretation.

`Receiver::get()` updates counters only after the real transport returns. A thrown receive operation contributes neither a completed poll count nor its duration. An empty array also cannot distinguish genuine emptiness from a retryable database failure swallowed by the underlying receiver. The committed method acknowledges that internal retries are not counted. [S1, S4]

In `finish()`, the deadline query occurs before the ACK try/finally. If it throws, the handler may already have run, but no `deliver` record is emitted. More generally, delivery/handler observation is currently coupled to settlement completion. This weakens stage attribution without necessarily allowing the run to pass: the outer lifecycle/error checks can still mark it incomplete. [S4]

**Required changes:**

- Give every public operation attempt an ID and an outcome recorded through try/finally.
- Track `receive_attempt`, `receive_empty`, `receive_success`, and `receive_error` separately.
- Record a claim/delivery observation and handler entry independently of later ACK success.
- Track handler success/failure separately from settlement success/failure.
- Put failures from optional instrumentation in a distinct `observer_error` category.
- Instrument the DBAL/driver boundary in diagnostic mode, below public receiver retry suppression. Do not modify retry policy, exception conversion, SQL, or transaction mode while observing it.
- Capture operation stage, connection role, primary and extended SQLite code where available, SQLSTATE, exception chain, transaction state, duration, and attempt/retry number. Unsupported observations must be null/unknown, never fabricated zeros.
- Distinguish application message retries, transport retries, SQLite busy waits, and unknown-outcome operations.

### F3 — Replace per-message control files and calibrate telemetry cost

**Priority:** P1 before interpreting small roundtrip differences.

Roundtrip creates an ACK marker file for each message and polls file existence with `usleep(1000)`. `SampleStore::append()` JSON-encodes, writes, and calls `fflush()` for every record. The recorder writes on both publish and delivery paths. These operations are outside some narrow RPC timers but still affect pacing, CPU, filesystem activity, and aggregate throughput. `fflush()` must not be described as an `fsync()` or a durable disk flush. [S5, S9–S10]

**Required changes:**

- Use a bounded pipe or Unix-domain control socket for roundtrip completion signalling.
- Keep control signalling outside public-call latency spans and measure its own cost.
- Use bounded recording buffers and a declared flush policy; never grow an in-memory list with total messages processed.
- Keep detailed traces separate from the low-overhead run. Exact final counters and integrity reconciliation remain mandatory.
- Do not silently drop samples: report dropped records and invalidate affected metric claims.
- Calibrate recording, resource sampling, control-channel, serializer, and payload-validation overhead independently.
- Predeclare an instrumentation-overhead budget. A useful initial engineering gate is at most 5% throughput perturbation in a controlled calibration; this is a proposed benchmark-quality threshold, not a measured property or a universal standard. Report latency perturbation separately.

### F4 — Replace lifetime peak-only resource reporting with phase/role accounting

**Priority:** P1 for the user's memory/CPU/I/O questions.

`Resources` stores samples privately but returns aggregate peak tree RSS, accumulated sampled CPU, process count, and sample count. It does not export a phase-aligned per-process series. Its CPU delta accumulation omits CPU before a process's first observation and after its last observation. The short idle CPU interval and coarse OS accounting are insufficient for a strong low-utilization comparison. These are measurement limitations, not evidence that the broker itself retains message data. [S8]

`ProcessTree::snapshot()` scans all system PIDs and reads their metadata at roughly every 20 ms coordinator tick. This observer cost can perturb the host even though the coordinator is excluded from product-tree totals. `Resources::$samples` grows with duration, and `SampleStore::read()` materializes entire files for analysis; those designs must not be carried unchanged into long soaks. Do not misattribute coordinator allocations to the reported extra broker RSS. [S8, S10, S14]

**Required changes:** See sections 7 and 8 for the normative replacement.

### F5 — Separate the questions being tested

**Priority:** P1 for conclusions; P2 for existing historical artifacts.

The current many-to-one/backlog/idle budgets are 160 messages, 240 messages, and 1.5 seconds of empty time plus 20 publications. Fresh processes are created for measured repetitions, so a previous warmup repetition does not warm the same long-lived runtimes that are then measured. Saturated finite-batch publish-to-handler percentiles include queue residence; they are not isolated notification/pickup latency. [S1, S3, S5]

The delayed scenario adds a probe queue, which selects the multi-queue polling fallback. It does not measure notification-driven delayed pickup. The synthetic receiver also rotates queues inside one wrapper rather than exercising the native multi-receiver selection path. These are documented scenario properties, not necessarily incorrect implementations. [S1, S4, S15]

**Required changes:** Separate empty pickup, steady offered load, maximum drain/capacity, delay semantics, multi-queue behavior, native integration, and resource retention. Do not use one scenario as a proxy for all of them.

### F6 — Replace the single all-or-nothing comparison verdict

**Priority:** P2.

`Comparison::build()` immediately becomes inconclusive if any scheduled run is incomplete, then considers only concurrent payload-specific p95/p99 pickup and cycle ranges. This is appropriately cautious about a speed-win claim, but it hides independent information and labels mixed directions as neutral. Neutral must not be interpreted as equivalent or acceptable. [S16]

**Required changes:** Report separate statuses for integrity, completion/reliability, latency, sustained throughput, idle efficiency, resource footprint, and memory retention. Incomplete baseline performance evidence must not suppress useful broker-only diagnostics or observed baseline failure counts. Report successful-run speed comparisons explicitly as conditional on successful completion.

## 4. Scope and non-goals for measurement work

The first implementation tranche changes benchmark and observational code only. It must not optimize SQL, alter indexes, reduce durability, change visibility/reclaim policy, introduce ACK retries, or change the protocol.

Preserve commit-before-reply, committed reservation before delivery, fencing by receipt/session/epoch/expiry, conservative handling of uncertain operations, and existing cancellation/shutdown guarantees. Do not introduce automatic reclaim or keepalive policy changes as benchmark fixes. [S11–S12]

The reference pair remains:

1. `doctrine-stock-immediate`: ordinary Doctrine transport using the approved native PDO IMMEDIATE setting.
2. `broker-durable-current`: current real broker plus current Messenger adapter.

An as-deployed application profile, a DEFERRED failure-reproduction profile, a direct-SQLite cost-floor probe, and internal async-storage/client microbenchmarks may be added under distinct names. They must never silently replace the reference pair. A non-durable echo probe can measure IPC overhead, but must never be presented as a durability-equivalent queue comparison.

## 5. Experiment lifecycle and workload contracts

### 5.1 Explicit phases

Use these independently timed phases:

```text
boot -> connected baseline -> same-process warmup -> reset counters
     -> measure -> bounded drain -> post-drain settling -> audit -> shutdown
```

Record each boundary in the common monotonic clock domain. Keep startup, measured interval, drain, settling, audit, and shutdown CPU/I/O/memory separate.

Warmup must exercise the same processes and relevant data paths used during measurement: serialization, publish, successful receive, ACK, WAIT, and both payload strata where applicable. Completely drain warmup work and verify its IDs before resetting counters. Do not restart the broker or workers between warmup and the main measured phase.

For backlog-drain scenarios, warm the processes first, then prefill the measured backlog outside the drain timer. Record prefill as its own workload rather than folding prefill costs into consumer capacity.

### 5.2 Load models

**Closed-loop:** Each actor sends its next operation only after the previous prescribed completion. Use for roundtrip or isolated-call latency. Clearly report the in-flight limit. Do not label this a fixed offered-rate test.

**Open-loop:** Schedule arrivals independently of service completion. Record the planned arrival time, actual invocation time, and scheduling lag. Use bounded dispatch queues and enough generator capacity. If the generator cannot keep up, record late/unstarted arrivals and mark the offered-load objective unmet. Do not shift future arrivals to hide delays, and do not build an unbounded client-side queue.

**Backlog drain:** Prefill a known durable inventory, release consumers together, and time from release to the last required confirmed ACK. Include first receive. Require the backlog not to empty prematurely during a fixed-window saturation subtest; if it does, that window is supply-limited.

**Application-like handlers:** Distinguish no-op/light verification, CPU work, and simulated external waiting. State whether the wait blocks a synchronous Messenger consumer or permits concurrent execution; those are different workloads. Preserve a concurrency limit and report handlers active over time.

### 5.3 Durability and environment

For each real database-owning connection, record effective journal mode, synchronous setting, busy timeout, transaction mode, automatic checkpoint setting, page size, cache settings, SQLite library version, and file location. Read settings before the measured phase. Observer connections must not silently become additional transport writers.

Record the PHP executable, PHP/extension versions and effective debug/opcache/JIT settings by role, exact dependency lock, source SHA, benchmark configuration hash, payload seed, filesystem/mount/storage information, CPU affinity/available cores, limits, host load, and relevant throttling/pressure observations.

Formal paired captures must use a clean recorded source revision. Diagnostic dirty captures must preserve the patch and untracked source files, not just report that the worktree was dirty. Preserve all v3 archives unchanged.

Do not run compared backends simultaneously against the same storage device. Alternate or seed-randomize pair order, retain the schedule, and declare repetitions before looking at outcomes. Different configuration/tuning experiments require a new experiment ID and a fresh complete planned schedule.

## 6. Metrics and event schema

### 6.1 Required public timestamps

All elapsed-time fields use a verified monotonic clock. Retain wall-clock anchors only for persisted wall-clock eligibility semantics.

| Event | Definition |
|---|---|
| `t_scheduled` | Planned arrival time in an offered-load scenario. |
| `t_send_call` | Immediately before invoking the real transport/client send API. |
| `t_send_return` | Successful return/confirmation to the publisher; record an error-return timestamp separately. |
| `t_receive_call` | Before each receive attempt. |
| `t_delivery_return` | Message/envelope is available to the consumer from the public receive boundary. |
| `t_handler_enter` | Immediately before handler work. |
| `t_handler_exit` | Handler finishes or throws. |
| `t_ack_call` | Before the real transport/client ACK call. |
| `t_ack_return` | Successful ACK return; record an error-return timestamp separately. |

For iterable receivers, specify exactly when iteration starts and when the envelope is yielded. Merely timing creation of an unconsumed generator is invalid.

### 6.2 Required derived metrics

| Metric | Formula or interpretation |
|---|---|
| Publish confirmation latency | `t_send_return - t_send_call`. |
| Delivery latency | `t_delivery_return - t_send_call`. Includes queue residence. |
| Publish-to-handler latency | `t_handler_enter - t_send_call`. |
| Confirmation-to-delivery gap | `t_delivery_return - t_send_return`; may legitimately be negative. |
| Scheduling lag | `t_send_call - t_scheduled`. |
| Arrival-to-handler latency | `t_handler_enter - t_scheduled`; exposes generator/backpressure delay. |
| Successful receive-call latency | `t_delivery_return - t_receive_call` for the successful attempt. |
| Empty/error receive latency | Duration of the corresponding attempt, reported separately. |
| Handler service time | `t_handler_exit - t_handler_enter`. |
| ACK confirmation latency | `t_ack_return - t_ack_call`. |
| Full-cycle latency | `t_ack_return - t_send_call`. |
| Empty-queue pickup | Delivery or handler latency only when the experiment establishes an already waiting consumer and no prior eligible backlog. |
| Delayed lateness | Delivery/handler time minus the stored eligibility deadline, with explicit clock mapping and precision. |
| Requested-deadline error | Delivery/handler time minus the original requested deadline; report separately from stored-deadline lateness. |

Do not clamp negative confirmation gaps. Do not compare second-precision and millisecond-precision delayed delivery as though they satisfy identical semantics. Missing stored-deadline evidence is a missing diagnostic measurement, not an on-time result.

### 6.3 Rates and backlog

For a declared measured window `[T0, T1)`, publish:

```text
scheduled arrivals/s
actual publish attempts/s
confirmed publications/s
unique valid ACK completions/s
handler starts/s
error and retry rates
backlog at start and end, plus its trend
```

Rate denominators use the same fixed window, not the first/last observed successes. Also report full cohort completion time from workload release or first scheduled arrival to final required ACK, with drain separate.

A completion in the rate window can belong to a message sent before that window; preserve cohort membership separately. A finite-batch completion rate, a steady-window rate, and a publish-cohort completion percentage are distinct statistics.

Backlog observations must not add database queries to the hot path. Use explicitly scoped instrumentation/counters during the run and reconcile against storage after draining. When retries, unknown outcomes, or receipt changes prevent exact derived backlog, mark the derived value uncertain and retain the independent post-run inventory.

### 6.4 Bounded internal diagnostics

In diagnostic mode, add timestamps/counters for:

- Broker request decoded; serialized-storage wait begins and ownership acquired.
- Request/response communication with the SQLite worker, including messages and bytes.
- Transaction begin, SQL execution, commit, rollback, and failure.
- Broker reply enqueue/flush and client reply decode, when observable.
- WAIT registration, readiness query, timeout, readiness wake, and cancellation.
- Event-loop callback lateness under load, with probe overhead recorded.

Use operation IDs for correlation, but do not use message IDs or receipt tokens as unbounded metric labels. Do not expose message bodies or secrets in trace output.

Nested spans overlap. Do not add p99 values from different spans or subtract independent percentiles to claim an overhead component. Use correlated traces or separate controlled experiments for attribution.

## 7. Resource measurement specification

### 7.1 Process identities and scopes

Record PID **and process start time**, role, parent identity, launch time, exit time, and whether accounting covers self or descendants. The scopes are:

- Publisher processes.
- Consumer processes.
- Broker process.
- SQLite persistence worker.
- Other persistent product children, if any.
- Observer/coordinator/control processes, separately.

Report both product-total resource cost and per-role breakdown. The broker-only comparison includes persistent infrastructure plus incremental client-side cost; it must not hide costs merely by moving them to consumers or to the SQLite child.

Prefer a dedicated delegated cgroup v2 for the product tree where available. It retains aggregate CPU/I/O accounting after children exit and provides a second memory view. Keep the observer outside it. Record cgroup scope and page-cache accounting; cgroup memory and summed PSS are different metrics. Without usable cgroups, use phase snapshots plus final self-usage/reaping records without double-counting child totals.

### 7.2 CPU

Report user seconds, system seconds, total core-seconds, CPU milliseconds per unique valid ACK, and idle CPU core-seconds per second. Define 100% as one fully used logical CPU; additionally reporting host-capacity percentage is optional.

Take cumulative readings at phase boundaries and final process exit. Sample trends during measurement at a declared low cadence, initially around 100–250 ms. Discover descendants using a known-role registry and a slower discovery path rather than reading every host process at every sample.

Optional diagnostics include context switches, scheduler delay, page faults, and throttling. A blocked storage operation does not necessarily consume CPU; interpret CPU alongside latency, off-CPU waits, queue depth, and synchronization time.

### 7.3 Memory

Required metrics, by process role and product tree:

- RSS, PSS, and private resident pages where supported.
- PHP current used memory and allocator-reserved memory separately.
- PHP peak memory separately from current values.
- Connected-idle baseline, post-warmup baseline, measured peak, and post-drain retained memory.
- Queue-ready and in-flight message counts and logical payload bytes at each comparison point.
- Broker gauges for live sessions, WAIT registrations, watched queues, timers, pending operations, and buffered bytes where observable.

Sample cheaper memory counters with the regular sampler. Sample PSS/smaps and detailed gauges at a slower declared cadence, initially about once a second and at phase boundaries. Calibrate overhead.

Do not add each PID's independent peak to invent a simultaneous tree peak. Do not interpret summed RSS as unique physical memory: shared mappings can be counted in several processes. PSS proportionally attributes shared resident pages. PHP memory does not account for every allocation in native extensions or every OS cache. [E3, E6]

### 7.4 SQLite activity and operating-system I/O

Keep three layers distinct:

1. **Logical queue operations:** publish, receive attempt, successful reservation, ACK, reject, WAIT, timeout, readiness refresh.
2. **SQLite operations:** SELECT/INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK, empty receive transactions, failed attempts, busy waits/retries, and checkpoints.
3. **OS/storage activity:** read/write system calls, synchronization calls and time, bytes attributed to storage, file sizes, and checkpoint/WAL growth.

Required derived metrics include SQL statements per completed message, committed transactions per completed message, empty receives per second, readiness SQL per idle second, CPU per completed message, and storage bytes per completed logical payload byte.

Use in-process counters at existing database-call boundaries without extra SQL. Distinguish statement executions from rows examined, VM work, worker IPC operations, and physical disk I/O. A SELECT count is not a disk-read count. `/proc/PID/io` syscall/character counters and `read_bytes`/`write_bytes` describe different layers; telemetry files can also contribute to process I/O. [E4]

For a separate short diagnostic run, capture `fsync`/`fdatasync`, file descriptor/path attribution, and timing using an appropriate syscall tracer or lower-overhead tracing facility. Do not use traced throughput as the headline result. Store benchmark telemetry separately from the queue database storage where practical and declare any observer memory/filesystem cost.

Record database/WAL size and normal checkpoint activity. Perform any final explicit checkpoint as a separate phase. In failure diagnosis, preserve relevant database/WAL evidence before destructive checkpoint/truncation or deletion, once owned processes are safely stopped. Do not delete a still-live broker's storage to tidy a report.

## 8. Memory-retention and leak investigation protocol

The current peak-RSS delta cannot distinguish fixed runtime overhead from a leak. Perform the following with the same long-lived broker and persistence process:

1. Start and connect clients. Record connected-idle baseline.
2. Warm all relevant paths and record a new baseline.
3. Publish a fixed cohort, consume it, ACK all required messages, and verify the queue and in-flight state are empty.
4. Keep the same client topology, allow a declared settling interval, and sample current memory and live-state gauges.
5. Repeat the same cycle at least 20 times without restarting the measured processes.
6. Separately repeat client-connect/disconnect, WAIT/timeout/cancel, and many-distinct-queue churn cycles.
7. Run a bounded backlog-size experiment using queued **bytes** as well as message count, then drain and compare against the same-topology baseline.
8. Run a separate deliberately slow/blocked-consumer diagnostic with bounded input to test buffer limits and backpressure. This is not the normal throughput scenario.

Plot matched-state post-drain PSS, private pages, PHP used/reserved memory, and live-object gauges against both completed messages and cycle number. Also compare repeated workloads with identical current state but increasing historical work.

Interpretation:

- Flat post-drain current memory with an initially higher baseline supports fixed overhead or bounded cache/high-water allocation, not a demonstrated leak.
- Increasing PHP used memory and retained live-state gauges after identical drained cycles supports a retention investigation.
- Increasing native/PSS memory without PHP-used growth points to a different attribution problem, not automatic proof of a PHP object leak.
- Memory that grows with active queued/in-flight bytes and falls or stabilizes after draining is different from memory that grows with total historical completions.

Record normal behavior without forcibly collecting cycles. A separate diagnostic may collect cycles and compare memory before/after; it must not mask normal production retention in the headline result.

Do not claim leak-free operation from one flat short run. Require stable live-state invariants, declared soak duration/message volume, and a reported upper bound or uncertainty on retained-memory growth. Resource-acceptance budgets must be frozen before claiming product acceptance; the initial v4 capture is for characterization, not post-hoc goal setting.

## 9. Workload suite and execution tiers

Do not execute an enormous full Cartesian product by default. Implement independently selectable scenarios and a small frozen core matrix, then extend only the dimensions under investigation.

| Scenario | Starting shape | Main question |
|---|---|---|
| Lifecycle smoke | Tiny fixed cohorts for each backend/mode | Are wiring, counters, cleanup, and integrity correct? Never capacity evidence. |
| Closed-loop roundtrip | 1 publisher / 1 consumer / one in flight | Public latency with no accumulated backlog; control overhead separately. |
| Empty-to-message pickup | 1 and several waiting consumers; isolated arrivals | WAIT versus polling latency and wake behavior without queue residence. |
| Idle | Empty queue, connected clients, no publications for a fixed long interval | CPU, SQL, timers, IPC, and memory while there is no useful work. |
| Fixed-rate load | Same predeclared offered rates for both backends | Latency/backlog/error curve at equal demand. |
| Backlog drain | One consumer then multiple consumers; sized durable backlog | Sustainable reservation/handler/settlement path with input already available. |
| Concurrent publishing | Multiple publishers; consumers absent or separately controlled | Publish confirmation and writer-contention behavior. |
| Burst and recovery | Fixed bursts separated by quiet periods | Tail latency, recovery/drain time, and return to idle costs. |
| Long-handler integration | Fixed synchronous waiting/CPU profiles with declared concurrency | Transport overhead versus application-bound completion and receipt safety. |
| Delayed single queue | Separate precision probe, one notification-driven queue | Due-time wakeups and stored/requested deadline semantics. |
| Multi-queue | Native selection and separately labelled synthetic selection | Fallback polling today; multi-queue WAIT later under a new version. |
| Retention/churn | Repeated equal-state cycles in one process lifetime | Fixed overhead, high-water memory, leaked sessions/timers/buffers. |

Retain 256-byte and 16-KiB payload strata for continuity. Add a larger payload stratum for buffer/backpressure diagnostics, for example 256 KiB, only within existing protocol limits. Report original payload, serialized body, protocol frame, and stored-byte sizes separately.

Use the current 1-ms polling profile as a named reference. Add realistic deployment polling intervals under explicit profile names, for example 10 ms and 50 ms where relevant. A zero-sleep busy-poll run is a diagnostic ceiling experiment, not an efficiency result or an unannounced default. Preserve actual broker WAIT timeout behavior and count timeout-driven rechecks; notification-driven does not mean literally zero idle SQL.

### Suggested initial budgets

These are proposed implementation defaults, not empirical conclusions:

- Smoke: tiny deterministic cohorts and bounded teardown.
- Core characterization: same-process warmup followed by a fixed 60–120 second measured interval, five predeclared paired repetitions for selected core cases.
- Idle characterization: at least 60 seconds per measured interval, with stable connected topology.
- Targeted capacity/tail investigation: longer fixed intervals chosen after a separate pilot and frozen before the comparison.
- Retention: at least 20 full publish/drain cycles in one lifetime, plus a separately declared longer steady/churn soak.

A pilot may select feasible rates, generator concurrency, backlog bytes, and duration. It may not select only favorable backend results. Keep pilot captures separate from the frozen comparison. Short/low-rate scenarios may never produce enough observations for strong p99 claims; show counts and uncertainty rather than extending selected runs until the result looks favorable.

## 10. Failure diagnosis and reliability reporting

### 10.1 Required classification

Each scheduled run has independently reported:

```text
execution_status: complete | failed | invalid | aborted
failure_stage: generator | send | receive | handler | ack | observer | shutdown | unknown
integrity_status: pass | fail | unknown
accounting_status: complete | partial | unavailable
```

A successful worker replacing a failed worker does not turn the run into a clean repetition. Do not change consumer count mid-run to obtain a passing benchmark. Record active consumer count over time.

Keep every planned repetition. Report successes/attempts, messages completed by the fixed deadline, unknown outcomes, pending work, time to first failure, and recovery/drain behavior where the scenario explicitly includes recovery.

For a failed run, observed successful-operation latencies are partial observations. Do not merge them into clean-run percentiles without an explicit label. Do not assign an invented latency to unfinished messages; report them as unfinished/censored with their elapsed lower bounds where available. A separate deadline-goodput metric can count valid completions over the full declared window even when a process fails, without pretending the run was healthy.

### 10.2 Doctrine diagnosis sequence

Keep the approved stock IMMEDIATE configuration. First run without harness SQL and capture stage-level observations. Then use a controlled two-connection reproducer with explicit barriers to establish where lock contention occurs. The existing test that holds a writer with timeout zero is a useful foundation. [S17]

For actual failures, record whether contention occurred at transaction begin, a read, a write, commit, ACK retry, setup, checkpoint, or observer query. Record the owning connection role and effective timeout/transaction mode. IMMEDIATE addresses deferred read-to-write upgrade problems; it does not make a held writer or an expired busy timeout disappear. [E1–E2]

Do not blanket-retry publish or ACK solely to obtain a clean comparison. A thrown response is not always proof that a write did not commit. Any policy change requires a separate explicit configuration and an unknown-outcome correctness contract.

The original application keepalive/signal failure requires its own native-command long-handler reliability scenario. Reproduce the old behavior/configuration explicitly and preserve the existing conservative lease/reclaim choices. A payload-verification loop cannot validate that incident's resolution.

## 11. Statistics and report format

For each backend, workload, phase, payload stratum, and relevant concurrency setting, report count, duration, p50, p95, p99, maximum, mean where useful, completed/failed/unfinished counts, and sample coverage. Keep per-run distributions. Do not average p99 values and call the result an overall p99.

The experimental unit for a backend comparison is the run or a declared paired run, not every message treated as an independent experiment. Summarize paired effects with uncertainty. Resampling, if used, must respect run/pair dependence; a tiny number of pairs does not support impressive precision merely because each pair processed thousands of messages.

Use separate conclusion fields:

- Correctness and delivery/settlement integrity.
- Completion and failure behavior under the declared workload.
- Publish, delivery, and settlement latency.
- Sustained-window throughput and backlog stability.
- Idle CPU/SQL/I/O efficiency.
- Persistent footprint and peak footprint.
- Post-drain retained-memory trend and lifecycle gauges.

Missing resource data is unknown, not zero. Mixed improvements/regressions remain mixed; do not collapse them into "neutral" or a weighted score. A failing Doctrine baseline can support the observation that it failed under that configuration, but cannot support a fabricated healthy-baseline speed estimate.

Publish a human-readable table answering: at the same offered rate and comparable handler work, which system meets the predeclared latency/error/resource objectives? Separately show maximum characterized capacity and its failure boundary. Adoption objectives must come from the application's needs and declared resource budget, not whichever metric happens to improve.

## 12. Artifact contract

Keep small text summaries available outside the compressed archive:

```text
manifest.json
report.md
summary.json
runs/<id>/config.json
runs/<id>/phases.json
runs/<id>/processes.json
runs/<id>/operations.*
runs/<id>/resources.*
runs/<id>/counters.json
runs/<id>/integrity.json
runs/<id>/errors.jsonl
runs/<id>/stderr/
runs/<id>/diagnostics/       # only in explicitly traced runs
```

The manifest includes schema/method version, revision/config/lock hashes, full planned schedule, random seeds, environment/provenance, tracing/sampling settings, and checksums. Compact text summaries should be inspectable without fetching a multi-megabyte binary archive.

Raw event schema must include run ID, role/process identity, monotonic timestamp, operation kind and attempt, correlation/message identity, payload stratum, phase, outcome, and optional error code/stage. Keep high-cardinality raw records out of metrics labels. Use bounded buffering and streaming/offline aggregation so a long benchmark does not become a coordinator memory benchmark.

Preserve error evidence and failed-run traces. Database artifacts must have an explicit retention policy and be handled only after safe process cleanup. Unknown or partial telemetry must propagate to report coverage flags.

## 13. Deterministic benchmark tests

Ordinary QA must test correctness of the harness, not wall-clock performance thresholds. Add tests for:

1. Correct timestamp boundaries, including iterable receive and delivery before send confirmation.
2. Handler entry retained when diagnostic reads or ACK fail.
3. Receive throws counted as attempts with error outcomes and durations.
4. Retry suppression distinguished through lower-layer diagnostic instrumentation without policy changes.
5. Unique-completion accounting with duplicates, rejects, missing records, unknown sends, and ACK failures.
6. Fixed-window/cohort boundary membership and failed-run denominator handling.
7. Scheduled versus actual arrivals, generator lag, bounded dispatch, and generator insufficiency.
8. No hot-path diagnostic database reads/connections in headline mode.
9. Bounded telemetry buffers, explicit overflow/write failure handling, and streaming analysis.
10. Phase-aligned per-role resource accounting, PID reuse, process exit, and no self/child double counting.
11. WAIT wake/timeout/cancel accounting and same-topology post-drain gauge cleanup.
12. Missing metrics represented as unavailable rather than zero; per-metric verdicts preserved after unrelated failures.
13. Historical v3 artifacts remain readable without schema reinterpretation.
14. Full scheduled repetition retention and no automatic retry-until-green.
15. Bounded shutdown and no deletion of live-owned storage.

Timing/throughput thresholds belong only in explicitly invoked performance characterization jobs. Fault tests should use controlled barriers, clocks, and injected failures rather than hoping for a random lock collision.

## 14. Implementation plan and acceptance gates

### Tranche A — Repair telemetry and preserve evidence

Modify the benchmark receiver, recorder, publisher/session control path, run-phase accounting, and report schema. Remove hot-path observer SQL in headline mode. Add exact failure-stage and completion accounting. Expose text reports outside archives. Keep the product implementation unchanged.

**Gate:** Deterministic tests pass; smoke captures reconcile all expected IDs; intentional failures remain visible; no unbounded recorder state; old v3 reports remain readable.

### Tranche B — Add phase/role resource attribution

Implement process identity registry, phase boundaries, CPU/PSS/current-PHP snapshots, logical SQL counters, and diagnostic I/O tracing support. Keep observer costs separate. Add baseline/drain/churn memory scenarios.

**Gate:** Resource accounting coverage is explicit; phase totals reconcile without double counting; sampler overhead is measured; matched-state retention plots and live-state gauges can be generated.

### Tranche C — Capture the frozen v4 core suite

Run an independent pilot, freeze workload/rate/duration/repetition settings, then capture the planned reference pairs. Add native Symfony integration and single-queue delayed notification as separate scenarios. Diagnose Doctrine failures without replacing failed repetitions.

**Gate:** The report can explain whether each scenario is generator-, handler-, transport-, or storage-limited, or explicitly mark attribution unknown. Reliability, latency, capacity, idle cost, and retained memory each have separate outcomes.

### Tranche D — Optimize only the measured dominant cost

Examples of hypotheses to evaluate, not authorized changes:

| Measured result | Candidate investigation | Required guardrail |
|---|---|---|
| Commit/synchronization dominates | Transaction execution overhead; carefully designed grouping of independently ready operations if justified | Preserve each operation's durability/atomicity and reply-after-commit contract; separate design review. |
| Async-worker communication dominates | Execute a full logical persistence operation in fewer worker exchanges | Preserve SQL transaction semantics, cancellation boundaries, and uncertainty handling. |
| Claim SQL dominates with growing backlog/in-flight prefix | Query plan, indexes, rows scanned, or an equivalent atomic claim/RETURNING design | Preserve eligible FIFO/order policy, payload handling, and receipt fencing. |
| Notification/readiness work dominates | Refresh coalescing, timeout policy, multi-queue WAIT | Preserve register-before-recheck, persisted deadlines, and WAIT-as-hint. |
| Fixed process footprint dominates | Runtime bootstrap, loaded extensions, worker configuration, topology | Demonstrate same correctness and resource ownership; do not call a baseline reduction a leak fix. |
| Matched-state memory rises repeatedly | Retained sessions, watches, futures, result sets, queues, native buffers | Reproduce with bounded live state; prove cleanup without forced restarts masking growth. |
| Application handler dominates | Evaluate whether transport overhead already meets application objectives | Do not optimize headline ACK/s that is fundamentally limited by handler concurrency/work. |

Change one major variable at a time, rerun the relevant fixed cases, and retain unfavorable results. No optimization qualifies as a win solely because it makes a tiny unrepresentative batch faster.

## 15. Source references

Repository sources below are pinned to the reviewed head. They support observed implementation facts, not the newly proposed thresholds or workload choices.

- **S1:** `docs/benchmark-method.md` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/docs/benchmark-method.md
- **S2:** `docs/benchmark-comparison.md` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/docs/benchmark-comparison.md
- **S3:** `bench/src/Config.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Config.php
- **S4:** `bench/src/Child/Receiver.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Child/Receiver.php
- **S5:** `bench/src/Child/Session.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Child/Session.php
- **S6:** `bench/src/Runner.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Runner.php
- **S7:** `bench/src/Stats.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Stats.php
- **S8:** `bench/src/Resources.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Resources.php
- **S9:** `bench/src/Child/Publisher.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Child/Publisher.php
- **S10:** `bench/src/SampleStore.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/SampleStore.php
- **S11:** `src/Sqlite/SqliteQueueStorage.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/src/Sqlite/SqliteQueueStorage.php
- **S12:** `src/Queue.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/src/Queue.php
- **S13:** `bench/src/Baseline.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Baseline.php
- **S14:** `bench/src/ProcessTree.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/ProcessTree.php
- **S15:** `bench/src/Child/Consumer.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Child/Consumer.php
- **S16:** `bench/src/Comparison.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/bench/src/Comparison.php
- **S17:** `tests/Bench/PairedBenchmarkTest.php` — https://github.com/ineersa/sqlite-queue/blob/632efd338b32af30665d8c93880389e8082160cd/tests/Bench/PairedBenchmarkTest.php
- **E1:** SQLite transaction semantics — https://www.sqlite.org/lang_transaction.html
- **E2:** PHP native SQLite transaction-mode attributes — https://www.php.net/manual/en/class.pdo-sqlite.php
- **E3:** Linux process memory/PSS accounting — https://docs.kernel.org/filesystems/proc.html
- **E4:** Linux process I/O accounting — https://cdn.kernel.org/doc/html/latest/filesystems/proc.html
- **E5:** SQLite WAL and synchronous durability — https://sqlite.org/pragma.html and https://www.sqlite.org/wal.html

- **E6:** PHP current/reserved memory and native-allocation limitations — https://www.php.net/manual/en/function.memory-get-usage.php
