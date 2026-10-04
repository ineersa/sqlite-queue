# Messenger benchmark observations

The native benchmark measures configured Symfony buses and stock `messenger:consume`. The [method](benchmark-method.md) defines its workload, completion boundaries, and coverage limits. Observations from the earlier synthetic runner are not interchangeable repetitions of this experiment.

## Measurements on 4 October 2026

Source revision: `1b1dac2`. Each command ran Doctrine first and the broker second, sequentially on the same host, with `XDEBUG_MODE=off`. Both owning connections confirmed the selected synchronous mode. These are single comparisons, not repeated estimates. Time-based windows were 60 seconds per backend. No failed comparison was retried.

Thirteen of fourteen backend runs completed with passing integrity and complete accounting. All owned-process cleanup checks passed. The broker idle run failed when publishing after the empty interval.

### NORMAL removes much of the synchronization cost

| Roundtrip | Doctrine completions/s | Broker completions/s | Doctrine send p50, ms | Broker send p50, ms |
| --- | ---: | ---: | ---: | ---: |
| NORMAL | 367.88 | 573.32 | 2.875 | 0.566 |
| FULL | 95.80 | 113.23 | 4.882 | 2.982 |

Broker roundtrip throughput was 5.1 times higher in NORMAL than FULL. Doctrine improved 3.8 times. This contrast strongly implicates commit synchronization in the earlier low throughput, but does not measure synchronization calls or isolate their duration.

Higher roundtrip throughput does not mean every latency improved. In NORMAL, send-call-to-ACK p50 was 0.876 ms for Doctrine and 1.505 ms for the broker. Corresponding p99 values were 4.551 and 8.881 ms. In FULL, p50 was 10.868 versus 8.855 ms. Delivery and ACK can precede the publisher's send return. The loop waits for both confirmation and completion, so these latency spans do not independently determine loop throughput.

### The broker loses the NORMAL contention comparison

Three publisher processes and two native consumers handled 3,000 measured messages, alternating 256-byte and 16 KiB payloads.

| Mode | Doctrine cohort, s | Broker cohort, s | Doctrine completions/s | Broker completions/s |
| --- | ---: | ---: | ---: | ---: |
| NORMAL | 1.075 | 3.457 | 2,790 | 868 |
| FULL | 21.426 | 22.474 | 140 | 133 |

Both backends completed all messages without recorded public-operation errors. Doctrine completed the NORMAL cohort 3.2 times faster. FULL brought their completion rates much closer. This is finite-cohort drain performance, not sustained capacity.

In NORMAL, median send time was 0.050 ms for Doctrine and 1.025 ms for the broker. Median ACK time was 0.038 versus 0.464 ms. Extra broker processing and persistence-worker IPC are candidates for this remaining overhead, not measured causes. Lower-driver SQL counters and IPC timing are unavailable. Delivery latency here includes backlog residence and must not be presented as unloaded pickup latency.

### Application work dominates at the declared demand

The application workload offered five workflows/s. An execution consumer waited 100 ms, then published a correlated result to a second native consumer. Both backends completed all 299 attempted workflows and 598 message ACKs, including drain.

Median root-send-to-result-ACK latency was 102.320 ms for Doctrine and 104.464 ms for the broker. The broker finished one workflow during drain, outside the measurement window. Neither backend retained unfinished workflows after drain. This run shows similar behavior at that demand, not maximum application capacity. The 299 samples are insufficient for a strong tail claim.

### Idle pickup exposes a session-lifetime bug

After the 60-second idle interval, Doctrine completed both isolated pickups. The broker's first pickup publish failed with `errno=32 Broken pipe`. Its send outcome is conservatively recorded as unknown; no replay occurred.

`Broker::serve()` applies the 30-second `OPERATION_TIMEOUT` while waiting for every request after the handshake. A healthy established connection with no requests therefore expires before the 60-second pickup. The transport keeps its original operations connection and does not reconnect. This is a functional blocker for long-idle use, not a throughput limitation. The same read policy also warrants a regression test for long handlers followed by ACK.

During the idle phase, Doctrine recorded 56,775 empty receives and its consumer used 4.60 CPU core-seconds. The broker recorded 61 empty receives; its consumer, broker, and persistence worker together used 0.08 observed CPU core-seconds. These are phase aggregates and boundary samples, not exact WAIT counters or complete product CPU totals. Both showed zero kernel-attributed physical read/write bytes for those roles during idle. Lower polling and CPU do not establish a healthy idle comparison while pickup fails.

### Retention does not show large historical growth

Both backends completed twenty cycles of one hundred messages. Each had 21 matched empty-state observations, unchanged process identities, and no forced collection or restart.

| Role | Private memory after warmup, MiB | Final private memory, MiB | Change, KiB |
| --- | ---: | ---: | ---: |
| Doctrine consumer | 36.07 | 36.14 | +68 |
| Broker consumer | 34.55 | 34.66 | +108 |
| Broker | 14.50 | 14.54 | +40 |
| SQLite worker | 12.32 | 12.32 | 0 |

The broker and SQLite worker added about 88.5 MiB of summed RSS at baseline, but only 28.6 MiB of summed PSS and 26.8 MiB of private memory. Shared mappings make summed RSS a poor measure of additional private memory. Publisher and observer share a process and are excluded from these role comparisons.

This short 2,000-message screen does not establish a leak or prove leak-free operation. Broker PHP memory and internal live-state gauges remain unavailable. Retention elapsed rates include between-cycle observation work and are not capacity results.

### What to fix next

Fix established-session idle handling first, without weakening partial-frame limits or replaying uncertain operations. Then use a small profile of the NORMAL concurrent workload to separate SQL, worker IPC, and broker scheduling costs. The current results support neither an overall broker speed win nor a memory-leak claim.

### Capture identifiers

Raw evidence and archived task reports remain under `var/bench/`, outside Git.

| Workload | Mode | Capture |
| --- | --- | --- |
| Roundtrip | NORMAL | `native-20261004-171839-c414ddcb` |
| Roundtrip | FULL | `native-20261004-172105-5f24a158` |
| Concurrent | NORMAL | `native-20261004-172321-ad05b950` |
| Concurrent | FULL | `native-20261004-172418-92d5515f` |
| Idle | NORMAL | `native-20261004-172518-223d3659` |
| Application | NORMAL | `native-20261004-172745-0d0d2fde` |
| Retention | NORMAL | `native-20261004-173014-708cd7de` |

## Historical one-pass instrumentation check

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

After this check, the instrumentation profiles and calibration workload were removed. Normal runs have one fixed bounded recording policy and phase-boundary resource snapshots. Defaults are 60 seconds per backend and one pair. No calibration matrix runs implicitly.

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
