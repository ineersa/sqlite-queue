# Native Messenger measurement method

Method `native-messenger-diagnostic` measures a package-owned Symfony application using configured buses, real transports, and stock `messenger:consume`. Use the [benchmark guide](../bench/README.md) for commands and capture storage. [Historical observations](benchmark-comparison.md) describe older builds, not the current build's performance. Synthetic-runner results are a separate method and must not be pooled with native runs.

## Reference pair

Doctrine uses its stock Messenger transport with PHP 8.5 PDO SQLite immediate transactions. The broker uses the current package adapter, native bundle integration, and an asynchronous driver's SQLite child process. There is no custom Doctrine empty-poll optimization or automatic replay of failed publish or ACK operations. Public timings include caller-side framework, serialization, and adapter work, plus the broker and driver path where applicable. They are not isolated SQL or socket-call timings.

Both backends use WAL, immediate transactions, a 5000ms busy timeout, and a 3600-second redelivery interval. `--synchronous=normal|full` selects the same mode for both, defaulting to NORMAL. Before measurement, the actual Doctrine transport connections read back their pragmas, and broker readiness reports readback from its owning storage connection. Desired and effective modes are recorded in configuration, manifest, and per-run results. The read-only audit connection is not authoritative for synchronous mode. Do not pool NORMAL and FULL captures.

NORMAL preserves commits across application or process crashes, but OS crashes or power loss can lose recent committed publications or ACKs. Confirmed sends may vanish and ACKed messages may reappear. NORMAL does not guarantee at-least-once across power loss. FULL retains stronger commit durability if storage honors synchronization. Payloads alternate between 256 bytes and 16 KiB. The serializer and payload-validation handler are shared. Broker workers use native WAIT integration, or WAIT_ANY for the explicit multi-queue workload. Doctrine workers use the recorded `--polling-ms` interval, defaulting to 50 milliseconds.

Outside the concurrent workload, the publisher and observer share a process. Concurrent publishers run in three separate processes. Broker consumers and publishers have no diagnostic SQL connections. Read-only storage audits run outside message paths after drain, including between retention cycles. Resource reports cannot isolate shared publisher and observer costs as product costs.

## Scenarios

| Workload | Declared question |
| --- | --- |
| `roundtrip` | Unloaded public latency with one message in flight. Socket control signals completion without per-message marker files. |
| `idle` | Connected empty-queue CPU, I/O, and receive activity. Two isolated pickups follow in a separate phase. |
| `multi-queue` | One stock worker selects two queues in order. An empty interval precedes one isolated arrival per queue. Broker pickup mode is `notification-wait-any`; Doctrine uses polling. |
| `concurrent` | A released 3,000-message cohort with three publishers and two consumers. Finite drain time, not sustained capacity. |
| `application` | Execution-to-control result routing with two consumers and declared synchronous handler waiting. |
| `retention` | Twenty cycles of one hundred messages with the same processes and clients, comparing audited-empty memory observations. |

Application offers five workflows/s with 100ms handler work and at most sixteen outstanding workflows. Stalled sends do not trigger catch-up bursts. Actual attempts and completions are recorded; this is not a configurable offered-rate capacity test.

Multi-queue captures explicitly change receiver topology and pickup mode. Do not pool them with historical single-queue or polling-only captures. Smoke checks the control path and accounting, not a latency guarantee or throughput advantage.

Application handlers dispatch correlated results through Messenger. Reports distinguish message ACKs from workflows whose root and result both complete cleanly. The workflow is synthetic. It does not reproduce real LLM latency, external side effects, keepalive incidents, or multi-receiver native selection.

## Phases and schedule

The lifecycle records boot, connected baseline, same-process warmup, measurement, bounded drain, settling, audit, and shutdown. Scenario-specific idle, pickup, and retention-cycle boundaries remain separate. Warmup exercises both payloads and the relevant routes without restarting workers.

Each invocation freezes settings and runs Doctrine first, then the broker. Defaults are 60 seconds per backend and one pair, about two minutes plus setup for a time-based workload. A recorded revision identifies the source only when changes are committed before measurement. There is no pilot or repetition option. Retention is cycle based rather than a duration-based capacity test. Non-smoke uses twenty cycles of one hundred messages. Retention smoke uses two cycles of two messages and does not establish retention behavior. Concurrent smoke uses twelve messages. Smoke captures prove wiring and accounting only.

Failed repetitions remain in place. A later clean repetition does not replace them. Changes to offered rates or instrumentation create a separate experiment.

## Time and completion accounting

Same-host `hrtime` observations record send invocation/return, receive invocation and yielded delivery, handler entry/exit, and ACK invocation/return. These are monotonic elapsed-time boundaries, not wall-clock timestamps. Iterable lifetime and active receive time are distinct so handler execution between yields is not attributed to transport work.

Public-operation failures retain their stage and exception details. A throwing send or ACK has an uncertain commit outcome unless the evidence establishes otherwise. Handler-entry observations survive settlement failure. Missing terminal telemetry, recorder losses, and write failures prevent complete accounting; observer errors also invalidate affected performance claims. Do not turn a failed backend into a healthy throughput estimate.

Fixed-window goodput counts unique valid ACK completions in the declared half-open interval. Cohort completion also includes later drain ACKs. Workflow completion requires both root and result settlement; result arrival alone is insufficient. Time-based rates use declared windows. Concurrent rates use the finite cohort duration. Publish attempts, confirmations, completions, and derived backlog remain separate.

Offline disk-backed analysis joins streamed observations and expected IDs after drain. It reports counts, missing boundaries, p50, p95, p99, means, and payload-specific distributions. Confirmation-to-delivery gaps may be negative. Tail adequacy is explicit. Do not average per-run p99 values or interpret short smoke percentiles as capacity evidence.

## Observer cost and resources

Telemetry and control buffers have fixed bounds. Recording is buffered rather than flushed for every event. Offline correlation uses disk-backed storage; raw streams are not loaded wholesale into PHP arrays.

Resources register PID/start-time identities. Current broker readiness supplies the broker PID and `storage_execution: "fabpot"`, not a persistence PID. The benchmark discovers exactly one driver worker through a bounded traversal of that broker's descendants in Linux `/proc`. It reports the child under the `persistence` role; this is driver-owned storage, not a package-owned PDO worker. Phase snapshots include cumulative user/system CPU, RSS, PSS, private memory, and Linux process-I/O fields where accessible. Consumer PHP used/reserved memory comes from control observations. Broker and driver-worker PHP gauges are unavailable.

Resource coverage is partial:

- Snapshots bracket phases; they are not continuous active-peak measurements.
- Work before first observation and after the last shutdown observation is missing.
- Publisher and observer costs share a role.
- Process I/O includes telemetry/control work and is not SQLite-only physical I/O.
- Summed RSS can count shared mappings more than once. PSS is a different measurement.

Retention compares audited-empty points with unchanged topology and process identities. Reports show memory endpoints, ranges, and deltas against completed historical work. No forced garbage collection or restart hides growth. Internal session/watch/timer/buffer gauges are unavailable. Stable short observations do not establish leak-free operation.

Retention audits empty inventory between cycles. Full offline correlation runs once after the cohort, rather than rescanning cumulative traces after every cycle. Retention elapsed rates include between-cycle observation work and are not transport capacity measurements.

There is one recording policy: operation records use bounded buffers, empty receives use phase aggregates, resources are sampled at phase boundaries, and SQL auditing and correlation run after drain. Buffering bounds memory, not observer cost: recording, control traffic, and snapshots still consume CPU and I/O. There are no telemetry-profile or calibration controls. The historical one-pass observer check does not certify zero observer effect. Material observer distortion invalidates the affected performance interpretation.

## Evidence and limits

Each capture contains a manifest with the revision, Composer-lock hash, settings, result paths, and owning synchronous-mode evidence. Record hardware, filesystem, PHP, SQLite, dependency versions, and competing host activity when publishing a comparison; results depend on that environment. Reports and raw captures are not distributed with the repository. Capture availability depends on the operator. The runner retains failed database evidence after owned-process cleanup; the [storage guidance](../bench/README.md#manage-capture-storage) explains how to manage accumulation.

Separate conclusions cover completion, integrity, latency, idle cost, footprint, and retention. Failed baseline runs do not erase broker-only measurements or become fabricated healthy-baseline estimates.

Lower-driver SQL/transaction/retry counters, traced synchronization costs, periodic resource peaks, exact WAIT registration/wake counts, and native broker lifecycle gauges are not implemented. Public exception details cannot always identify the internal SQL operation that failed. Report unavailable attribution as unknown. These measurements support characterization of the declared application and topology, not an unconditional performance win or universal production reliability claim.
