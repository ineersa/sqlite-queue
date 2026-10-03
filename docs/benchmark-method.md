# Paired benchmark method v3: immediate Doctrine transactions

The runner compares the standard Symfony Doctrine SQLite transport with the real broker Messenger adapter. Both use `PhpSerializer`, a real Messenger `Worker`, and the same payload-verification handler. Neither backend requires an application kernel or custom claim SQL. Historical baseline captures use method v1 and are not paired comparison evidence.

The current revision is `paired-v3-immediate`. Doctrine uses PHP 8.5's native `Pdo\Sqlite::ATTR_TRANSACTION_MODE` option set to `TRANSACTION_MODE_IMMEDIATE`. The runner reads back that attribute on every DBAL connection. The standard transport and its empty-poll receive path remain unchanged. There is no custom middleware or advisory empty-queue optimization.

Immediate transactions reserve SQLite's writer slot at BEGIN rather than upgrading a deferred read snapshot later. This is an explicitly approved baseline configuration, not a change to durability. The broker already serializes writes and uses immediate transactions. Both configurations retain WAL, FULL synchronization, and the same lock timeout.

Preserve failed DEFERRED captures from paired v2. Do not combine their metrics with v3 or replace their failed repetitions with new successes. Every v3 report records its method revision and observed transaction modes.

## Runtime profile

Production-like capacity runs use `XDEBUG_MODE=off` for both backends. The process launcher passes only this debug override through its otherwise isolated environment. Effective modes come from `xdebug_info('mode')`, not `ini_get('xdebug.mode')`. Reports retain the INI default and the override separately. A default of `develop` with an effective empty mode list means Xdebug is off.

The coordinator, publishers, consumers, and broker record their effective modes. A runtime mismatch fails the repetition. The vendor SQLite worker exposes no PHP diagnostic RPC. At broker startup, an Amp context inheritance probe verifies effective modes through the same process factory, and the actual SQLite worker's interpreter is checked. Reports identify this indirect verification and the probe PID. The probe exits before the workload barrier. Its cost belongs to startup and lifetime resource accounting, not steady-state transport calls.

Earlier develop-mode captures remain diagnostic evidence, not production-capacity measurements. Keep their failures and do not pool them with the off-profile matrix. Run one fresh fixed matrix with debugging off, retaining every failed repetition. Workload budgets and comparison criteria do not change with this runtime correction.

## Workloads and budgets

Every backend has one retained warmup repetition and three measured repetitions per workload. Each repetition uses a fresh database. Backend order alternates within each pair. Warmup samples do not enter measured-run variation. Operation latency excludes startup. Reports retain `startup_ms` and process lifetime resource costs.

| Name | Publishers | Consumers | Messages per repetition | Scheduling |
| --- | ---: | ---: | ---: | --- |
| roundtrip | 1 | 1 | 150 | Publisher waits for the preceding ACK before the next send |
| concurrent | 3 | 2 | 3000 | 1000 per publisher, unpaced saturation |
| many-to-one | 4 | 1 | 160 | 40 per publisher, unpaced saturation |
| backlog | 1 prefill | 2 | 240 | 80 per queue across three queues, prefilled before consumer release |
| idle | 1 | 1 | 20 | Empty-poll readiness, 1.5 s idle, then fixed 100 ms publication targets |
| delayed | 1 | 1 | 40 plus one probe | Empty-poll readiness, 2000 ms DelayStamp, 50 ms publication targets, plus a separate 300 ms probe |

Messages alternate between 256-byte and 16,384-byte deterministic binary payloads. Every delivery regenerates the bytes from its correlation ID and verifies the digest. There is no simulated handler work beyond verification. Named queues rotate at the receiver wrapper. The wrapper delegates receive and ACK to the selected real transport and does not retry transport errors.

Doctrine polling is 1000 microseconds, with one message per receive. Single-queue broker consumers instead use a bounded 1000 ms WAIT on idle worker events, with native sleep disabled. Multi-queue backlog and delayed consumers retain polling because the broker has no multi-queue WAIT. Reports record pickup mode, poll counts, and wait counts and durations. Both backends use a 3600-second visibility timeout. No keepalive is needed for the verification handler. The runner does not restart failed workers. Doctrine's internal receive retries remain unchanged. Exhausted retries appear as failures, not silently repeated runs. Internal retry counts are not exposed by its public interface.

The idle workload is the controlled offered-load case. Targets advance from one monotonic start timestamp, not from completion of the previous send. Scheduling lag and unfinished sends remain visible when the sender falls behind. Saturation workloads do not claim a fixed offered rate.

## Durability and ownership

Every DBAL connection explicitly sets WAL, synchronous FULL, a 5000 ms busy timeout, and a 1000-page auto-checkpoint. The runner reads back settings on each child connection and the actual broker SQLite worker. A broker mismatch aborts the repetition. Broker readback uses startup-only reflection to inspect the initialized worker connection, without a production diagnostic API. Schema setup and transport acquisition finish before the go barrier. An explicit final `wal_checkpoint(TRUNCATE)` runs outside measured operations.

Confirmed send and ACK are measured at the commit boundary. This is not a power-loss test; SQLite and the filesystem still determine power-loss guarantees.

Children receive a minimal environment, explicit file paths, and no inherited application DSN. Readiness files include their actual PIDs. Start files carry parent clock readings. Every child has a finite runtime; the coordinator has a repetition deadline and a TERM/KILL cleanup path. Database cleanup names only the files it created. Raw evidence remains under the unique run directory.

## Timing definitions

All elapsed durations use `hrtime(true)`. A 20-round parent/child clock probe brackets each child reading between parent request and reply readings. Cross-process durations require an offset interval containing zero with width below 2 ms, plus per-child start-order checks. Linux supplies the common `CLOCK_MONOTONIC` domain. Probe brackets, source hashes, and clocks remain in the report. Values below the measured bracket width need caution; the probe is not a nanosecond calibration.

| Metric | Start | End |
| --- | --- | --- |
| send_ms | Publish invocation | Transport send returns after commit |
| publish_to_handler_ms | Publish invocation | Handler entry |
| confirmation_to_handler_ms | Publish confirmation | Handler entry, possibly before confirmation |
| ack_ms | ACK invocation | ACK returns |
| full_cycle_ms | Publish invocation | Correlated ACK confirmation |
| delayed_lateness_ms | Stored eligibility deadline | Handler entry |
| requested_deadline_lateness_ms | Requested wall-clock deadline | Handler entry |

Both receivers read stored eligibility after the handler and before ACK using a DBAL diagnostic query outside the claim transaction. That query contributes to the full cycle but not ACK duration. Doctrine reuses its worker connection. Broker workers use a separate diagnostic connection because transport SQL belongs to the broker. Broker deadlines retain millisecond precision. Doctrine deadlines retain second precision. The roundtrip publisher waits on an ACK marker outside the timed transport calls.

Doctrine truncates `DelayStamp` milliseconds to whole seconds and stores datetimes at second resolution. Even a whole-second delay can become eligible before the requested millisecond deadline. The report retains stored-deadline lateness, requested-deadline lateness, and their quantization difference. The 300 ms probe is separately recorded and excluded from measured latency distributions. It remains included in send, delivery, and ACK integrity counts. No delay-performance comparison may call this precision equivalent to the broker's required millisecond semantics.

Wall clocks must remain stable during delayed tests. A clock adjustment can affect persisted eligibility and lateness. The runner does not claim immunity to host clock changes.

## Statistics, failures, and resources

Percentiles use nearest rank with no trimming. Reports retain p50, p95, p99, maximum, count, and every scheduled repetition. Metrics with fewer than 1000 samples are labeled tail-inconclusive. The v2 concurrent budget has 1500 samples per payload size per repetition. Other workload budgets remain unchanged and cannot support payload-specific p99 claims.

A send can commit before its confirmation reaches the publisher. Negative confirmation-to-handler durations remain negative. Missing, corrupt, duplicate, rejected, unacknowledged, and unknown messages make the repetition incomplete. Failed runs keep their observed distributions, clearly labeled incomplete. Unfinished counts are based on unique acknowledged IDs rather than raw ACK count. No report removes outliers or selects favorable repetitions.

The coordinator samples descendant CPU, resident memory, and process count, including the broker and SQLite worker. Sampled peaks are lower bounds, not exact simultaneous peaks. Publisher and consumer footers record final `getrusage()` and PHP peak memory. Broker facts record its final usage and reaped-child usage separately. Do not sum final usage with sampled CPU as if they were disjoint. Missing final records make accounting incomplete. The coordinator's usage is separate. Idle CPU is the sampled child-tree delta during the empty interval.

The coordinator checks child progress and samples resources at roughly 20 ms intervals. After publishers finish, it also queries the shared database inventory at that interval until the queue drains. These reads add measurement overhead. A comparison must match this cadence and diagnostic cost, or revise the method and capture both backends again.

The machine record includes PHP/SQLite/dependency versions, source revision and dirty state, per-file benchmark hashes, lock hash, CPU, storage mount, workload settings, and debug extensions. Artifact retention includes every raw sample and child error.

## Comparison decision

The paired-v2 concurrent budget of 3000 messages was declared before the first paired measurements. Method v3 changes only Doctrine transaction mode with explicit approval. Workloads, budgets, backend order, and the comparison decision remain unchanged. Older DEFERRED captures do not substitute for the new paired baseline.

All scheduled runs must complete before a win claim. Each concurrent payload size and measured repetition must have at least 1000 successful observations. For both pickup and full-cycle latency, every broker p95 and p99 must be below the best Doctrine repetition. Reverse separation across all these metrics is a regression. Overlapping or mixed ranges are neutral. Missing runs, failures, or insufficient samples are inconclusive.

This conservative range comparison is not a significance test. Resource costs remain separate tradeoffs, not part of a hidden score. Neutral, regressing, or inconclusive results pause adoption for review. No result justifies weaker durability or removal of failed repetitions. Delayed results describe different timestamp precision and do not claim equivalent millisecond semantics.
