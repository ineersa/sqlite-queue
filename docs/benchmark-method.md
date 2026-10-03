# Native Messenger measurement method

Method `native-messenger-diagnostic` measures a package-owned Symfony application using configured buses, real transports, and stock `messenger:consume`. Historical synthetic-runner measurements are separate experiments and must not be pooled with these observations.

## Reference pair

Doctrine uses its stock Messenger transport with PHP 8.5 PDO SQLite immediate transactions. The broker uses the current package adapter and native bundle integration. There is no custom Doctrine empty-poll optimization or automatic replay of failed publish or ACK operations.

Both backends use WAL/FULL, a 5000ms busy timeout, and a 3600-second redelivery interval. Payloads alternate between 256 bytes and 16 KiB. The serializer and payload-validation handler are shared. Broker workers use native single-receiver WAIT integration. Doctrine workers use the named 1ms polling reference.

The publisher and observer share a process. Broker consumers and publishers have no diagnostic SQL connections. The observer audits storage through a separate read-only process after measurement and drain. Resource reports do not pretend that shared publisher/observer costs are isolated product costs.

## Scenarios

| Workload | Declared question |
| --- | --- |
| `roundtrip` | Unloaded public latency with one message in flight. Socket control signals completion without per-message marker files. |
| `idle` | Connected empty-queue CPU, I/O, and receive activity. Two isolated pickups follow in a separate phase. |
| `fixed-rate` | Latency and completions at an absolute offered schedule with one synchronous publisher and one consumer. |
| `application` | Execution-to-control result routing with two consumers and declared synchronous handler waiting. |
| `delayed` | Single-queue DelayStamp delivery through native consumption and broker WAIT. |
| `retention` | At least twenty publish/drain cycles with the same processes and clients, followed by audited-empty memory observations. |
| `calibration` | Detailed versus essential telemetry crossed with resource snapshots enabled or disabled. |

Fixed-rate capacity bounds outstanding work. An overloaded generator retains original scheduled timestamps and reports unstarted or late arrivals. Generator insufficiency is not transport loss or a healthy capacity result. This single synchronous publisher can become the bottleneck before the transport does.

Application handlers dispatch correlated results through Messenger. Reports distinguish message ACKs from workflows whose root and result both complete cleanly. The workflow is synthetic. It does not reproduce real LLM latency, external side effects, keepalive incidents, or multi-receiver native selection.

Delayed reports distinguish requested-deadline error from unavailable stored-deadline lateness. Wall/monotonic anchors are recorded per send. Negative errors remain visible. Doctrine's second-precision delay semantics differ from the broker's millisecond semantics.

## Phases and schedule

The lifecycle records boot, connected baseline, same-process warmup, measurement, bounded drain, settling, audit, and shutdown. Scenario-specific idle, pickup, and retention-cycle boundaries remain separate. Warmup exercises both payloads and the relevant routes without restarting workers.

Formal runs require a clean revision and freeze configuration and the full alternating backend schedule before execution. Default duration is 60 seconds and default repetition count is five pairs. Retention is cohort/cycle based rather than a duration-based capacity test. Pilots preserve source snapshots and are labelled separately. Smoke captures prove wiring and accounting only.

Failed repetitions remain in place. A later clean repetition does not replace them. Changes to offered rates or instrumentation create a separate experiment.

## Time and completion accounting

Monotonic observations record send invocation/return, receive invocation and yielded delivery, handler entry/exit, and ACK invocation/return. Iterable lifetime and active receive time are distinct so handler execution between yields is not attributed to transport work.

Public-operation failures retain their stage and exception details. A throwing send or ACK has an uncertain commit outcome unless the evidence establishes otherwise. Handler-entry observations survive settlement failure. Missing terminal telemetry or recorder losses make accounting partial.

Fixed-window goodput counts unique valid ACK completions in the declared half-open interval. Cohort completion also includes later drain ACKs. Workflow completion requires both root and result settlement; result arrival alone is insufficient. Rates use declared windows, not first and last successes. Publish attempts, confirmations, scheduling lag, unfinished arrivals, and derived backlog remain separate.

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

Retention's offline audit work occurs between cycles. Re-reading cumulative traces makes observer work grow with cycle count, although PHP correlation memory is bounded. Large retention settings therefore require a separate observer-cost assessment.

Calibration reports paired perturbation and a proposed 5% quality budget. Essential telemetry preserves exact completion accounting while suppressing detailed empty-receive records. Calibration does not isolate serializer, payload-validation, control-channel, journal, or whole-system overhead. A passing recorder comparison is not proof that all instrumentation is negligible.

## Evidence and limits

Each capture contains a manifest with source/configuration/lock hashes, planned runs, and environment provenance, plus readable reports and per-run raw evidence. Raw captures stay outside Git. Failed database evidence is retained only after safe owned-process cleanup.

Separate conclusions cover completion, integrity, latency, generator sufficiency, idle cost, footprint, and retention. Failed baseline runs do not erase broker-only measurements or become fabricated healthy-baseline estimates.

Lower-driver SQL/transaction/retry counters, traced synchronization costs, periodic resource peaks, exact WAIT registration/wake counts, and native broker lifecycle gauges are not implemented. Public exception details cannot always identify the internal SQL operation that failed. Report unavailable attribution as unknown. These measurements support characterization of the declared application and topology, not an unconditional performance win or universal production reliability claim.
