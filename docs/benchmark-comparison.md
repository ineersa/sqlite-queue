# Messenger benchmark comparison

The native benchmark measures configured Symfony buses and stock `messenger:consume`. The [method](benchmark-method.md) defines its workloads, completion windows, resource coverage, and limitations. Its results are not comparable to the old synthetic runner by treating the two captures as interchangeable repetitions.

## Historical paired capture

The `paired-v3-immediate` capture at implementation revision `632efd3` was inconclusive. Every broker repetition completed; two measured Doctrine concurrent repetitions recorded SQLite lock failures. Roundtrip throughput was lower for the broker, other ranges overlapped, and broker sampled process-tree RSS was approximately 90–100 MiB higher.

Those measurements included observer SQL on message paths, per-message file coordination, and incomplete phase/resource attribution. They do not establish a leak or a performance advantage and are not the basis for optimization decisions.

The originating machine retains `bench/results/paired-v3-20261003.tar.gz`. Raw archives are ignored and no longer tracked in the current repository. The old archive remains available in historical commits. Its SHA-256 is:

```text
7057bc1ed886fa1b47310feccf82225dafadba9541a6b0a9d0742b74e49bc36a
```

## Native characterization schedule

The formal schedule is frozen before running the comparison. Each workload alternates backend order across five pairs. Xdebug is disabled. Time-based workload windows are 60 seconds unless noted below.

| Capture | Configuration |
| --- | --- |
| Roundtrip | One publisher, one consumer, one message in flight. |
| Idle | Connected empty interval, followed by two isolated pickups per repetition. |
| Fixed rate, moderate | 20 scheduled arrivals/s, capacity 64, one consumer. |
| Fixed rate, higher demand | 80 scheduled arrivals/s, capacity 64, one consumer. |
| Application | 5 workflows/s, capacity 16, 100ms synchronous execution handler, separate result consumer. |
| Delayed | One queue, 300ms requested delay, no precision-probe queue. |
| Retention | 20 cycles, 20 messages per cycle, 250ms settling per cycle, unchanged processes/connections. |
| Calibration | Four telemetry/snapshot profiles per backend, 60-second windows, five paired repetitions. |

All seven scenarios first passed separate two-second pilots, with twenty short cycles for retention. Pilot captures are excluded from the formal comparison. Fixed-rate rates were selected before formal outcomes, not adjusted to obtain clean results. A single synchronous generator may fail its offered-load objective at higher demand.

The schedule characterizes these configurations. It does not reproduce the original keepalive incident or claim maximum transport capacity. Internal SQL profiling, continuous resource peaks, and native broker lifecycle gauges remain unavailable. Calibration covers only the implemented recorder/snapshot profiles.

Formal result tables will be recorded after this schedule completes. Every failed scheduled repetition will remain visible. Raw captures stay under `var/bench/`; only compact findings and capture identifiers are published here.
