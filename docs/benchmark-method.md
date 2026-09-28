# Baseline method v1

This reference fixes the Task 02 measurement method before a broker candidate exists. The runner uses the standard Symfony Doctrine transport, `PhpSerializer`, a real Messenger `Worker`, and a payload-verification handler. There is no application kernel, fake candidate, or custom claim SQL.

`bin/benchmark` registers Symfony Console commands through Composer's development PSR-4 mapping. `RunCommand` handles options and cancellation; `Benchmark` owns the capture schedule, and `Runner` owns one repetition. Worker commands delegate to publisher or consumer classes. Symfony Process starts and stops children with an explicitly isolated environment. The internal clock command exists to verify cross-process timestamp subtraction, not to measure queue throughput.

## Workloads and budgets

Every workload has one retained warmup repetition and three measured repetitions. Each repetition uses a fresh database. Warmup covers the same workload, but its samples do not enter measured-run variation. Each measured repetition has its own startup; operation latency excludes startup. Reports retain startup metadata and process lifetime resource costs.

| Name | Publishers | Consumers | Messages per repetition | Scheduling |
| --- | ---: | ---: | ---: | --- |
| roundtrip | 1 | 1 | 150 | Publisher waits for the preceding ACK before the next send |
| concurrent | 3 | 2 | 150 | 50 per publisher, unpaced saturation |
| many-to-one | 4 | 1 | 160 | 40 per publisher, unpaced saturation |
| backlog | 1 prefill | 2 | 240 | 80 per queue across three queues, prefilled before consumer release |
| idle | 1 | 1 | 20 | Empty-poll readiness, 1.5 s idle, then fixed 100 ms publication targets |
| delayed | 1 | 1 | 40 plus one probe | Empty-poll readiness, 2000 ms DelayStamp, 50 ms publication targets, plus a separate 300 ms probe |

Messages alternate between 256-byte and 16,384-byte deterministic binary payloads. Every delivery regenerates the bytes from its correlation ID and verifies the digest. There is no simulated handler work beyond verification. Named queues rotate at the receiver wrapper; the wrapper delegates each receive and ACK to the standard Doctrine transport. It does not retry transport errors.

Worker polling is 1000 microseconds, with one message per receive. Doctrine's visibility timeout is explicitly 3600 seconds. No keepalive is needed for this short handler. The runner does not restart a failed worker. Doctrine's internal receive retries remain unchanged; exhausted retries appear as failures, not silently repeated runs. Internal retry counts are not exposed by its public interface and are not separately measured.

The idle workload is the controlled offered-load case. Targets advance from one monotonic start timestamp, not from completion of the previous send. Scheduling lag and unfinished sends remain visible when the sender falls behind. Saturation workloads do not claim a fixed offered rate.

## Durability and ownership

Every DBAL connection explicitly sets WAL, synchronous FULL, a 5000 ms busy timeout, and a 1000-page auto-checkpoint. The runner reads back effective settings on each child connection. Schema setup finishes before children start. An explicit final `wal_checkpoint(TRUNCATE)` runs outside measured operations and its result is recorded.

Focused tests verify that a second connection observes a confirmed send and the deletion after a confirmed ACK. This is commit-boundary evidence, not a power-loss test. SQLite and the filesystem still determine power-loss guarantees.

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

The receiver reads stored eligibility after the handler and before ACK. That diagnostic query contributes to the full cycle but not the ACK duration. The future candidate must use equivalent diagnostics or a new matched baseline. The roundtrip publisher waits on a test-owned ACK marker outside the timed transport calls.

Doctrine truncates `DelayStamp` milliseconds to whole seconds and stores datetimes at second resolution. Even a whole-second delay can become eligible before the requested millisecond deadline. The report retains stored-deadline lateness, requested-deadline lateness, and their quantization difference. The 300 ms probe is separately recorded and excluded from measured latency distributions. It remains included in send, delivery, and ACK integrity counts. No delay-performance comparison may call this precision equivalent to the broker's required millisecond semantics.

Wall clocks must remain stable during delayed tests. A clock adjustment can affect persisted eligibility and lateness. The runner does not claim immunity to host clock changes.

## Statistics, failures, and resources

Percentiles use nearest rank with no trimming. Reports retain p50, p95, p99, maximum, count, and every scheduled repetition. Metrics with fewer than 1000 samples are labeled tail-inconclusive. The initial budgets therefore describe variability and failure behavior, not a defensible p99 win.

A send can commit before its confirmation reaches the publisher. Negative confirmation-to-handler durations remain negative. Missing, corrupt, duplicate, rejected, unacknowledged, and unknown messages make the repetition incomplete. Failed runs keep their observed distributions, clearly labeled incomplete. Unfinished counts are based on unique acknowledged IDs rather than raw ACK count. No report removes outliers or selects favorable repetitions.

The coordinator samples descendant CPU, resident memory, and process count. Sampled tree peaks are lower bounds, not exact simultaneous peaks. Each child's final `getrusage()` records CPU time and high-water RSS, and its footer records PHP peak memory. Missing footers mean final resource totals are incomplete. The coordinator's CPU and PHP peak memory are reported separately. Idle CPU is the sampled child-tree CPU delta during the explicit empty interval. The baseline owns no broker or persistence worker; the candidate must include both.

The coordinator checks child progress and samples resources at roughly 20 ms intervals. After publishers finish, it also queries the shared database inventory at that interval until the queue drains. These reads add measurement overhead. Task 08 must match this cadence and diagnostic cost, or revise the method and capture both backends again.

The machine record includes PHP/SQLite/dependency versions, source revision and dirty state, per-file benchmark hashes, lock hash, CPU, storage mount, workload settings, and debug extensions. Artifact retention includes every raw sample and child error.

## Later comparison

Task 08 must use the real adapter, matched durability, payloads, serialization, handler, counts, and diagnostics. Pickup differs intentionally: baseline polling versus candidate notifications. Record both configurations.

Use alternating paired backend order across repetitions. Keep startup separate, preserve failures, and compare individual-run variation rather than only pooled percentiles. Before a tail claim, collect at least 1000 successful samples per workload, payload size, and repetition on both backends under a declared revised budget. Re-run the baseline with that budget before candidate tuning. A failure-heavy or statistically inconclusive baseline cannot establish a speedup. Neutral or regressing evidence pauses adoption rather than justifying weaker durability.
