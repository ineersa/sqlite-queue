# Historical Messenger benchmark observations

These results describe older builds, not the current broker's performance. Use the [current method](benchmark-method.md) and [run guide](../bench/README.md) for a new comparison. Native Messenger and earlier synthetic-runner captures use different boundaries and must not be pooled.

Capture identifiers below are provenance, not download links. Raw captures and archives are not distributed with the repository; availability depends on the operator. Their continued presence on the originating machine is not assumed.

## Native measurements on 4 October 2026

Revision `1b1dac2` ran Doctrine first and the broker second on the same host, with `XDEBUG_MODE=off`, 60-second time-based windows, and verified owning-connection synchronous modes. Doctrine used 1 ms polling, not the current 50 ms default. Each command produced one comparison; failures were not retried. This page does not record that host's hardware or filesystem, so the September baseline's machine description must not be assumed to apply.

Thirteen of fourteen backend runs passed completion, integrity, and accounting. All owned-process cleanup checks passed. The broker idle run failed on publication after its empty interval.

### Roundtrip and concurrent results

| Roundtrip mode | Doctrine completions/s | Broker completions/s | Doctrine send p50, ms | Broker send p50, ms |
| --- | ---: | ---: | ---: | ---: |
| NORMAL | 367.88 | 573.32 | 2.875 | 0.566 |
| FULL | 95.80 | 113.23 | 4.882 | 2.982 |

Broker roundtrip throughput was 5.1 times higher in NORMAL than FULL; Doctrine improved 3.8 times. This suggests synchronization cost but does not measure synchronization calls or isolate their duration. NORMAL send-call-to-ACK p50 was 0.876 ms for Doctrine and 1.505 ms for the broker; p99 was 4.551 and 8.881 ms. FULL p50 was 10.868 versus 8.855 ms. Delivery and ACK can precede send return, while the loop waits for both confirmation and completion. Those latency spans therefore do not independently determine throughput.

The concurrent cohort used three publisher processes, two native consumers, and 3,000 messages alternating 256-byte and 16 KiB payloads. Both backends completed all messages without recorded public-operation errors.

| Concurrent mode | Doctrine cohort, s | Broker cohort, s | Doctrine completions/s | Broker completions/s |
| --- | ---: | ---: | ---: | ---: |
| NORMAL | 1.075 | 3.457 | 2,790 | 868 |
| FULL | 21.426 | 22.474 | 140 | 133 |

Doctrine completed this NORMAL cohort 3.2 times faster. This is finite-cohort drain performance, not sustained capacity. NORMAL median send time was 0.050 ms versus 1.025 ms; median ACK time was 0.038 versus 0.464 ms. Median send-call-to-delivery was 426 ms versus 1,690 ms, including backlog residence rather than unloaded pickup alone. These results did not isolate SQL, IPC, or serialization costs.

### Application, idle, and retention

The application workload offered five workflows/s with 100 ms handler work and a separate result consumer. Both backends completed all 299 attempted workflows and 598 message ACKs, including drain. Median root-send-to-result-ACK was 102.320 ms for Doctrine and 104.464 ms for the broker. One broker workflow completed during drain, outside the window. Neither retained unfinished workflows. This describes that demand, not maximum capacity; 299 samples do not support a strong tail claim.

After 60 seconds idle, Doctrine completed both isolated pickups. The broker's first publish failed with `errno=32 Broken pipe`; its commit outcome remained unknown and no replay occurred. At this revision, `Broker::serve()` applied its 30-second operation timeout to established connections awaiting another request. The transport neither reconnected nor replayed. The later fix and successful rerun are recorded below; this failure is not a current-build claim.

Idle phase counts were 56,775 empty receives and 4.60 consumer CPU core-seconds for Doctrine, versus 61 empty receives and 0.08 observed core-seconds across the broker consumer, broker, and storage worker. Both recorded zero kernel-attributed physical read/write bytes for those roles. These boundary observations are not exact WAIT counters or complete product CPU totals. Low idle CPU did not make the failed pickup a healthy comparison.

Both retention runs completed twenty cycles of one hundred messages, with 21 matched empty-state observations, unchanged process identities, and no forced collection or restart.

| Role | Private memory after warmup, MiB | Final private memory, MiB | Change, KiB |
| --- | ---: | ---: | ---: |
| Doctrine consumer | 36.07 | 36.14 | +68 |
| Broker consumer | 34.55 | 34.66 | +108 |
| Broker | 14.50 | 14.54 | +40 |
| SQLite worker | 12.32 | 12.32 | 0 |

The broker and storage worker added about 88.5 MiB of summed RSS at baseline, but 28.6 MiB of summed PSS and 26.8 MiB of private memory. RSS double-counts shared mappings. The shared publisher/observer process is excluded from those role comparisons. This short 2,000-message screen neither established a leak nor proved leak-free operation. Broker PHP memory and internal live-state gauges were unavailable; retention elapsed rates included observation work.

| Workload | Mode | Capture identifier |
| --- | --- | --- |
| Roundtrip | NORMAL | `native-20261004-171839-c414ddcb` |
| Roundtrip | FULL | `native-20261004-172105-5f24a158` |
| Concurrent | NORMAL | `native-20261004-172321-ad05b950` |
| Concurrent | FULL | `native-20261004-172418-92d5515f` |
| Idle | NORMAL | `native-20261004-172518-223d3659` |
| Application | NORMAL | `native-20261004-172745-0d0d2fde` |
| Retention | NORMAL | `native-20261004-173014-708cd7de` |

## Idle-session fix and unprofiled rerun

Revision `e3b371a` removed the established-session idle timeout while keeping the five-second handshake bound and a thirty-second partial-frame bound starting at the first prefix bytes. The NORMAL concurrent rerun used the same three publishers, two consumers, and 3,000-message cohort. Doctrine completed in 1.062 seconds, or 2,825 messages/s; the broker took 3.839 seconds, or 781 messages/s. Both passed integrity, accounting, and process cleanup. A single before-and-after pair does not isolate a performance effect of the idle fix.

Both backends passed a 60-second idle check and both pickups. Doctrine recorded 56,731 empty receives and 5.01 consumer CPU core-seconds. The broker recorded 61 empty receives and 0.11 observed core-seconds across consumer, broker, and storage worker. Send-call-to-delivery ranged from 1.030 to 1.147 ms for Doctrine and 1.288 to 1.820 ms for the broker. Two arrivals cannot establish a tail-latency advantage or replace the earlier failed capture.

Rerun identifiers: concurrent `native-20261004-184418-bb24a5c7`; idle `native-20261004-184439-48ee784d`. Both used `XDEBUG_MODE=off` and verified NORMAL owning connections. No throughput optimization accompanied this rerun.

Public-operation distributions from the concurrent rerun each contain 3,000 delivered-message samples. Receive measures active fetching, excluding handler execution and idle WAIT. Empty receives are separate aggregates.

| Operation | Doctrine mean, ms | Broker mean, ms | Doctrine p50, ms | Broker p50, ms | Doctrine p95, ms | Broker p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Send | 0.341 | 1.580 | 0.051 | 1.165 | 0.139 | 2.970 |
| Receive | 0.426 | 1.014 | 0.145 | 0.699 | 2.974 | 2.196 |
| Handler | 0.007 | 0.008 | 0.006 | 0.007 | 0.011 | 0.015 |
| ACK | 0.199 | 0.781 | 0.039 | 0.477 | 0.122 | 1.708 |

Publishers and consumers overlap these spans. Summing their means is not an elapsed-time decomposition.

## Historical resource and driver diagnostics

CPU deltas for the original unprofiled `1b1dac2` concurrent measurement through drain were:

| Role | Doctrine CPU core-seconds | Broker CPU core-seconds |
| --- | ---: | ---: |
| Broker | Not present | 1.69 |
| SQLite worker | Not present | 2.37 |
| Both consumers together | 0.88 | 0.70 |
| Coordinator and observer | 0.07 | 0.09 |
| Publishers | Unavailable | Unavailable |

The broker and storage worker consumed 4.06 core-seconds during the 3.457-second cohort. Publishers exited before the final snapshot; their missing CPU cannot be treated as zero or used to construct complete product totals.

Diagnostic capture `native-20261004-182616-0f3bbe28`, revision `07c2897`, used `XDEBUG_MODE=profile` before the idle fix. Both backends completed the same NORMAL cohort cleanly, but took 7.463 seconds for Doctrine and 28.449 seconds for the broker. These are profiled durations, not substitute throughput measurements. Worker observations cover startup, six warmup messages, measurement, and shutdown.

| Worker activity | Calls | Recorded duration, s |
| --- | ---: | ---: |
| Worker `execute` dispatches | 33,248 | Not additive across fibers |
| Native `SQLite3Stmt::execute` | 54,309 | 1.246 |
| Native `SQLite3::prepare` | 54,309 | 0.579 self time |
| Native `SQLite3::querySingle` | 60,336 | 0.307 |
| Native `SQLite3Result::fetchArray` | 631,650 | 0.328 |
| Driver `analyzeStatement` | 18,052 | 0.700 self time |
| Driver `isOrdinaryTableDefinition` | 3,006 | 0.803 self time |

The locked driver was `fabpot/amphp-sqlite3` v1.0.0, source `1ee168273e29af037a5c4576349ff89e3a53b5fd`. It probed `total_changes()`, prepared statements, inspected non-read-only statements with `EXPLAIN`, and parsed table definitions for insert identities. Those were driver operations, not benchmark diagnostic SQL. The broker path used three transactions and five application statements: INSERT, SELECT, UPDATE, SELECT, DELETE. At that revision the driver rejected row-producing DML.

Including setup and warmup, counts were roughly eleven worker execute dispatches, eighteen native prepares, and eighteen native executions per completed message. These diagnose that historical implementation, not current-driver behavior or an outstanding optimization plan.

Xdebug fiber suspend/resume times overlap. Summed worker self time was 166.7 seconds against a 33.6-second process profile; broker summed self time was 460.9 seconds. Neither is a valid CPU total. Native SQL profile timings cannot be subtracted from the unprofiled cohort, and serialization alone was not established as the bottleneck. Compressed profiles, an analysis script, and `profile-summary.json` were associated with this capture outside Git; no profiling hooks were added to production or the benchmark.

## Historical observer-cost check

Revision `292ca71` ran one sequential 60-second window for each of four recording/snapshot combinations on both backends, with Xdebug disabled. All eight runs passed completion, integrity, and telemetry accounting. Capture identifier: `native-20261003-231251-841d1a35`. The original summary did not state the synchronous mode or host hardware; do not infer either from current defaults.

| Historical setup | Doctrine unique ACK/s | Broker unique ACK/s | Doctrine full-cycle p50, ms | Broker full-cycle p50, ms |
| --- | ---: | ---: | ---: | ---: |
| Detailed records, snapshots on | 87.32 | 107.55 | 10.909 | 8.959 |
| Reduced records, snapshots off | 88.53 | 108.23 | 10.905 | 8.955 |
| Detailed records, snapshots off | 84.65 | 104.72 | 10.919 | 9.004 |
| Reduced records, snapshots on | 82.95 | 105.40 | 10.928 | 8.999 |

These are fixed-window valid completions with one message in flight, not ACK RPC capacity. Reduced recording retained identities and failures while aggregating empty receives; it was not an uninstrumented reference. Detailed recording with snapshots completed 1.37% fewer messages/s for Doctrine and 0.63% fewer for the broker than reduced recording without snapshots, but removing snapshots from detailed recording also reduced throughput. That non-monotonic single pass cannot separate observer cost from ordering, storage, or scheduling variation. It established neither zero distortion nor a 5% observer budget; small differences and similar medians are not causal evidence.

Recording profiles and calibration controls were subsequently removed. Current runs use one bounded recording policy and phase-boundary snapshots, not an implicit calibration matrix. The earlier five-repetition calibration, `native-20261003-225957-84a8f205`, was interrupted; its queued roundtrip command never started. Neither is a completed comparison. Short idle/pickup, fixed-rate, application, delayed-delivery, and retention pilots checked wiring and accounting, not completed 60-second resource comparisons.

## Earlier synthetic-runner comparison

The `paired-v3-immediate` capture at implementation revision `632efd3` was inconclusive. Every broker repetition completed; two measured Doctrine concurrent repetitions recorded SQLite lock failures. Broker roundtrip throughput was lower, other ranges overlapped, and broker sampled process-tree RSS was approximately 90–100 MiB higher. Observer SQL on message paths, per-message file coordination, and incomplete phase/resource attribution prevent pooling these results with the native method. They established neither a leak nor a performance advantage.

The historical archive was named `paired-v3-20261003.tar.gz`. Its recorded SHA-256 was:

```text
7057bc1ed886fa1b47310feccf82225dafadba9541a6b0a9d0742b74e49bc36a
```
