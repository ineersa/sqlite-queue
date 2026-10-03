# Doctrine SQLite baseline results

The baseline capture contains six workloads, each with one warmup and three measured repetitions. It measures the Doctrine SQLite transport only, not this broker. The results establish no broker performance advantage.

## Revision and reproduction

Historical capture command at revision `13a43bc`:

```sh
	php bench/run.php
```

The benchmark source hashes match revision `13a43bc`. Capture metadata records parent revision `688353e` and a dirty worktree; the recorded file hashes, rather than that parent revision alone, identify the measured source. The archive also records the Composer lock hash.

The runner was later refactored into a Symfony Console application. The current command is `php bin/benchmark run`. The archived capture remains evidence for `13a43bc`, not a measurement of the refactored CLI. Workload budgets and timing definitions are unchanged, but new measurements must record the new runner and dependency hashes. Do not relabel the old timings as results from the refactor.

The original artifact directory is `var/bench/20260928-010402-4cda6aa2/`. The [retained archive](../bench/results/baseline-v1-20260928.tar.gz) contains all 24 repetition directories, raw JSONL samples, logs, configuration, `summary.json`, and `report.md`. It contains no queue database files.

Archive SHA-256:

```text
b3fab5e7acbc1042ab034c6719d50f9dc4d16a94017951ef1a522db8ca7ba756
```

The capture used PHP 8.5.10, SQLite 3.45.1, Symfony Messenger and Doctrine Messenger 8.1.7, and Doctrine DBAL 4.5.0. The machine had an Intel Core i5-13400F, with the database directory on ext4 at `/dev/nvme0n1p2`. Xdebug 3.5.3 was loaded in `develop` mode; CLI OPcache was disabled. Full configuration and machine observations are in `summary.json`.

See [method v1](benchmark-method.md) for sample budgets, timing definitions, polling, durability, and fairness requirements. This was a local development-machine capture, not a controlled hardware certification.

## Outcomes

The command exited 1 because 14 of 24 repetitions were incomplete. No repetition was removed or retried. All recorded owned-process survivor lists were empty, and all disposable queue database files were removed.

| Workload | Warmup | Measured runs 1, 2, 3 | Observation |
| --- | --- | --- | --- |
| roundtrip | Complete | Complete, complete, complete | 150 ACKs per repetition |
| concurrent | Incomplete | Incomplete, incomplete, incomplete | Consumers exhausted Doctrine lock retries; unfinished work retained in accounting |
| many-to-one | Incomplete | Incomplete, incomplete, incomplete | Consumer lock failure during publication |
| backlog | Incomplete | Incomplete, incomplete, incomplete | All 240 messages ACKed, but one consumer failed in every repetition |
| idle | Complete | Complete, complete, complete | 20 ACKs per repetition |
| delayed | Complete | Incomplete, complete, incomplete | Lock failures in measured runs 1 and 3; the successful repetitions ACKed 41 messages including the probe |

The underlying error was `SQLSTATE[HY000]: General error: 5 database is locked`. The runner does not add retries around standard Doctrine or restart failed workers. It stops remaining work when all consumers have exited and records the unattempted or unacknowledged work. This measures the standard transport's failure behavior under the declared workload, not a recovered-service throughput rate.

## Observed variation

These are individual-run observations, not pooled values. Every p99 remains tail-inconclusive because the per-run sample budgets are below 1000.

| Metric, ms | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| Roundtrip send p50 | 2.502 | 2.528 | 2.508 |
| Roundtrip publish-to-handler p95 | 6.536 | 6.494 | 5.860 |
| Roundtrip full-cycle p95 | 9.385 | 9.414 | 8.744 |
| Idle publish-to-handler p95 | 17.124 | 9.678 | 7.325 |
| Idle full-cycle p95 | 20.064 | 12.560 | 10.175 |

Roundtrip full-cycle p95 spanned about 0.67 ms. Idle full-cycle p95 varied by almost a factor of two. That variability and the concurrent failures rule out a useful speedup claim from one favorable run. A broker comparison needs repeated paired runs and larger matched budgets, as specified in method v1.

Idle child-tree CPU was about 22–24% of one CPU during the sampled empty interval at 1 ms worker polling. Peak sampled child counts were two for roundtrip/idle/delayed, five for concurrent/many-to-one, and three for backlog including prefill. These counts exclude the separately reported coordinator. Raw child footers retain final CPU time and high-water RSS.

The clock probe bounded the possible offset to -3781 through 4223 ns and passed. Negative confirmation-to-handler durations remain valid observations rather than being clipped.

## Delayed-delivery limitation

Doctrine floors delay milliseconds to seconds and stores eligibility at second resolution. Stored-deadline lateness and requested-deadline lateness are therefore different measurements. The successful measured delayed repetition had stored-deadline p95 lateness of about 112.84 ms. That does not establish millisecond-delay compatibility. The separate 300 ms probe and quantization fields preserve the discrepancy.

An early baseline delivery is not a valid speed advantage over a correctly delayed broker delivery. The concurrent failures, insufficient tail samples, and delay-precision mismatch remain explicit limits on later comparison, not reasons to weaken broker durability or discard failed runs.
