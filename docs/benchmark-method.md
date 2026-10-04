# Native Messenger measurement method

Method `native-messenger-diagnostic` measures a package-owned Symfony application using configured buses, real transports, and stock `messenger:consume`. Historical synthetic-runner measurements are separate experiments and must not be pooled with these observations.

## Reference pair

Doctrine uses its stock Messenger transport with PHP 8.5 PDO SQLite immediate transactions. The broker uses the current package adapter and native bundle integration. There is no custom Doctrine empty-poll optimization or automatic replay of failed publish or ACK operations.

Both backends use WAL, immediate transactions, a 5000ms busy timeout, and a 3600-second redelivery interval. `--synchronous=normal|full` selects the same mode for both, defaulting to NORMAL. Before measurement, the actual Doctrine transport connections read back their pragmas, and broker readiness reports readback from its owning storage connection. Desired and effective modes are recorded in configuration, manifest, and per-run results. The read-only audit connection is not authoritative for synchronous mode. Do not pool NORMAL and FULL captures.

NORMAL preserves commits across application or process crashes, but OS crashes or power loss can lose recent committed publications or ACKs. Confirmed sends may vanish and ACKed messages may reappear. NORMAL does not guarantee at-least-once across power loss. FULL retains stronger commit durability if storage honors synchronization. Payloads alternate between 256 bytes and 16 KiB. The serializer and payload-validation handler are shared. Broker workers use native single-receiver WAIT integration. Doctrine workers use the named 1ms polling reference.

Outside the concurrent workload, the publisher and observer share a process. Concurrent publishers run in three separate processes. Broker consumers and publishers have no diagnostic SQL connections. The observer audits storage through a separate read-only process after measurement and drain. Resource reports do not pretend that shared publisher and observer costs are isolated product costs.

## Scenarios

| Workload | Declared question |
| --- | --- |
| `roundtrip` | Unloaded public latency with one message in flight. Socket control signals completion without per-message marker files. |
| `idle` | Connected empty-queue CPU, I/O, and receive activity. Two isolated pickups follow in a separate phase. |
| `concurrent` | A released 3,000-message cohort with three publishers and two consumers. Finite drain time, not sustained capacity. |
| `application` | Execution-to-control result routing with two consumers and declared synchronous handler waiting. |
| `retention` | At least twenty publish/drain cycles with the same processes and clients, followed by audited-empty memory observations. |

Application offers five workflows/s with 100ms handler work and at most sixteen outstanding workflows. Stalled sends do not trigger catch-up bursts. Actual attempts and completions are recorded; this is not a configurable offered-rate capacity test.

Application handlers dispatch correlated results through Messenger. Reports distinguish message ACKs from workflows whose root and result both complete cleanly. The workflow is synthetic. It does not reproduce real LLM latency, external side effects, keepalive incidents, or multi-receiver native selection.

## Phases and schedule

The lifecycle records boot, connected baseline, same-process warmup, measurement, bounded drain, settling, audit, and shutdown. Scenario-specific idle, pickup, and retention-cycle boundaries remain separate. Warmup exercises both payloads and the relevant routes without restarting workers.

Each invocation freezes settings and runs Doctrine first, then the broker. Defaults are 60 seconds per backend and one pair, about two minutes plus setup for a time-based workload. A recorded revision identifies the source only when changes are committed before measurement. There is no pilot or repetition option. Retention is cycle based rather than a duration-based capacity test. Non-smoke uses twenty cycles of one hundred messages. Retention smoke uses two cycles of two messages and does not establish retention behavior. Concurrent smoke uses twelve messages. Smoke captures prove wiring and accounting only.

Failed repetitions remain in place. A later clean repetition does not replace them. Changes to offered rates or instrumentation create a separate experiment.

## Time and completion accounting

Monotonic observations record send invocation/return, receive invocation and yielded delivery, handler entry/exit, and ACK invocation/return. Iterable lifetime and active receive time are distinct so handler execution between yields is not attributed to transport work.

Public-operation failures retain their stage and exception details. A throwing send or ACK has an uncertain commit outcome unless the evidence establishes otherwise. Handler-entry observations survive settlement failure. Missing terminal telemetry or recorder losses make accounting partial.

Fixed-window goodput counts unique valid ACK completions in the declared half-open interval. Cohort completion also includes later drain ACKs. Workflow completion requires both root and result settlement; result arrival alone is insufficient. Time-based rates use declared windows. Concurrent rates use the finite cohort duration. Publish attempts, confirmations, completions, and derived backlog remain separate.

Offline disk-backed analysis joins streamed observations and expected IDs after drain. It reports counts, missing boundaries, p50, p95, p99, means, and payload-specific distributions. Confirmation-to-delivery gaps may be negative. Tail adequacy is explicit. Do not average per-run p99 values or interpret short smoke percentiles as capacity evidence.

## Observer cost and resources

Telemetry and control buffers have fixed bounds. Recording is buffered rather than flushed for every event. Offline correlation uses disk-backed storage; raw streams are not loaded wholesale into PHP arrays.

Resources use known PID/start-time identities, including the persistence PID from broker readiness. Phase snapshots include cumulative user/system CPU, RSS, PSS, private memory, and Linux process-I/O fields where accessible. Consumer PHP used/reserved memory comes from control observations. Broker and persistence PHP gauges are unavailable.

Resource coverage is partial:

- Snapshots bracket phases; they are not continuous active-peak measurements.
- Work before first observation and after the last shutdown observation is missing.
- Publisher and observer costs share a role.
- Process I/O includes telemetry/control work and is not SQLite-only physical I/O.
- Summed RSS can count shared mappings more than once. PSS is a different measurement.

Retention compares audited-empty points with unchanged topology and process identities. Reports show memory endpoints, ranges, and deltas against completed historical work. No forced garbage collection or restart hides growth. Internal session/watch/timer/buffer gauges are unavailable. Stable short observations do not establish leak-free operation.

Retention audits empty inventory between cycles. Full offline correlation runs once after the cohort, rather than rescanning cumulative traces after every cycle. Retention elapsed rates include between-cycle observation work and are not transport capacity measurements.

There is one recording policy: all operation records use bounded buffers, resources are sampled at phase boundaries, and SQL auditing and correlation run after measurement/drain. No telemetry profiles or calibration workload remain. The historical one-pass observer check is recorded in the comparison. It does not certify zero observer effect. Material observer distortion invalidates the affected performance interpretation; removing configuration choices does not itself establish measurement neutrality.

## Evidence and limits

Each capture contains a manifest with the revision, Composer-lock hash, settings, result paths, and owning synchronous-mode evidence. Reports and per-run raw records remain outside Git. Failed database evidence is retained after safe owned-process cleanup.

Separate conclusions cover completion, integrity, latency, idle cost, footprint, and retention. Failed baseline runs do not erase broker-only measurements or become fabricated healthy-baseline estimates.

Lower-driver SQL/transaction/retry counters, traced synchronization costs, periodic resource peaks, exact WAIT registration/wake counts, and native broker lifecycle gauges are not implemented. Public exception details cannot always identify the internal SQL operation that failed. Report unavailable attribution as unknown. These measurements support characterization of the declared application and topology, not an unconditional performance win or universal production reliability claim.
