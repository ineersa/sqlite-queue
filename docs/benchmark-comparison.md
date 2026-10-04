# Messenger benchmark observations

The native benchmark measures configured Symfony buses and stock `messenger:consume`. The [method](benchmark-method.md) defines its workload, completion boundaries, and coverage limits. Observations from the earlier synthetic runner are not interchangeable repetitions of this experiment.

## One-pass instrumentation check

At the user's request, revision `292ca71` ran one 60-second window for each of four recording/snapshot combinations on both backends. Xdebug was disabled. All eight runs completed with passing integrity and complete telemetry accounting.

Capture: `native-20261003-231251-841d1a35`. Raw evidence remains locally under `var/bench/`, outside Git. This table preserves the complete observed configuration set without selecting only favorable runs.

| Historical setup | Doctrine unique ACK/s | Broker unique ACK/s | Doctrine full-cycle p50, ms | Broker full-cycle p50, ms |
| --- | ---: | ---: | ---: | ---: |
| Detailed records, snapshots on | 87.32 | 107.55 | 10.909 | 8.959 |
| Reduced records, snapshots off | 88.53 | 108.23 | 10.905 | 8.955 |
| Detailed records, snapshots off | 84.65 | 104.72 | 10.919 | 9.004 |
| Reduced records, snapshots on | 82.95 | 105.40 | 10.928 | 8.999 |

These are fixed-window valid completions for a one-message-in-flight roundtrip, not ACK RPC capacity. Reduced recording still retained all message identities and failures; it aggregated empty receives. It was not an uninstrumented reference.

Compared with reduced recording and no snapshots, detailed recording with snapshots completed 1.37% fewer messages/s for Doctrine and 0.63% fewer for the broker. However, removing snapshots from the detailed setup also reduced throughput. The observed changes are not monotonic with instrumentation. One sequential pass cannot separate observer effects from order effects, storage variation, and scheduling variation. No zero-distortion or 5% budget certification follows from these observations. The report correctly labelled its budget evidence insufficient.

Broker roundtrip throughput was higher in each observed setup. That is a descriptive result for this capture, not an established application-wide or repeatable performance advantage. Full-cycle medians changed little across setups. Small throughput differences must not be attributed to diagnostics without further causal evidence.

After this check, the instrumentation profiles and calibration workload were removed. Normal runs have one fixed bounded recording policy and phase-boundary resource snapshots. Defaults are 60 seconds per backend and one pair, with explicit options for additional repetitions. No calibration matrix runs implicitly.

## Capture history

The earlier five-repetition calibration at `native-20261003-225957-84a8f205` was interrupted at the user's request. Its queued roundtrip command was cancelled before starting. Neither is a completed comparison, and neither is pooled with the one-pass check. A proposed three-second replacement was never executed.

Short pilots covered native idle/pickup, fixed-rate load, execution-to-control application routing, delayed single-queue delivery, and same-process retention. Those pilots validated wiring and accounting. They are not completed 60-second application/resource comparisons.

## Historical synthetic-runner capture

The `paired-v3-immediate` capture at implementation revision `632efd3` was inconclusive. Every broker repetition completed; two measured Doctrine concurrent repetitions recorded SQLite lock failures. Roundtrip throughput was lower for the broker, other ranges overlapped, and broker sampled process-tree RSS was approximately 90–100 MiB higher.

Those measurements included observer SQL on message paths, per-message file coordination, and incomplete phase/resource attribution. They do not establish a leak or a performance advantage and are not the basis for optimization decisions.

The originating machine retains `bench/results/paired-v3-20261003.tar.gz`. Raw archives are ignored and no longer tracked in the current repository. The old archive remains in historical commits. Its SHA-256 is:

```text
7057bc1ed886fa1b47310feccf82225dafadba9541a6b0a9d0742b74e49bc36a
```
