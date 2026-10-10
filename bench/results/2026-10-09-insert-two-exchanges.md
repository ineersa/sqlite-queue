# Insert-only two-exchange POC

## Result

The POC removes one worker exchange from insertion without merging transactions or changing checkpoint settings. It produces a small roundtrip improvement, but **no demonstrated concurrent-throughput improvement**.

| Workload | Baseline median msg/s | POC median msg/s | Median paired change | Improving pairs |
| --- | ---: | ---: | ---: | ---: |
| Concurrent | 948.3 | 932.5 | -3.54% | 3 of 7 |
| Roundtrip | 563.1 | 586.0 | +3.30% | 6 of 7 |

The percentage column is the median of `100 × (B / A - 1)` within matched pairs, not the percentage difference between the two rate medians. All **28 invocations and 56 backend outcomes** pass integrity, accounting, and cleanup. No failed slots are removed or replaced.

Keep this as an experiment. The data does not justify promoting the change as a general throughput optimization. Independent review did not complete because the reviewer reached its usage limit. This report is intended for advisor review.

## Compared builds

- **A:** accepted driver baseline [`401460a`](https://github.com/ineersa/amp-sqlite3/commit/401460af087ab6d8c23c990ddf09bedcb23d32aa), with conventional queue insertion.
- **B:** driver POC [`27371fb`](https://github.com/ineersa/amp-sqlite3/commit/27371fbdcddb39eed0e4449af467adb4b3b6b2b0), plus the queue insertion change on [`experiment/insert-two-exchanges`](https://github.com/ineersa/sqlite-queue/tree/experiment/insert-two-exchanges).
- [Driver patch relative to the accepted baseline](https://github.com/ineersa/amp-sqlite3/compare/401460af087ab6d8c23c990ddf09bedcb23d32aa...27371fbdcddb39eed0e4449af467adb4b3b6b2b0).

This comparison is against `401460a`, not the older driver `593218d` pinned by queue main. The queue branch's lock diff against main therefore includes the earlier driver optimizations as well as this POC. Those earlier optimizations are shared by both measured arms.

The frozen locks differ only in the `fabpot/amphp-sqlite3` package record. The measured queue code differs only in `SqliteQueueStorage.php`; benchmark code remains identical. The POC is not merged into either repository's main or driver development branch.

Use `composer install` on the queue branch to install its locked prototype. An unqualified `composer update` can resolve the development branch instead and remove the experimental method.

## Execution change

Conventional insertion has three exchanges:

1. Parent sends BEGIN IMMEDIATE and awaits its reply.
2. Parent samples the availability clock, sends INSERT, receives its result, and validates a positive insert ID.
3. Parent sends COMMIT and awaits its reply.

The POC has two:

1. Parent sends BEGIN IMMEDIATE and awaits its reply.
2. Parent samples the availability clock, then sends one `executeInsertAndCommit` request. The worker executes the same INSERT through the existing implicit statement cache, validates its positive native ID, performs COMMIT, and returns the ID only after COMMIT succeeds.

A successful insert, claim, and settlement lifecycle falls from **10 to 9 exchanges**. It still has **three native COMMITs**. Claim remains four exchanges and settlement remains three. Their SQL, transaction boundaries, and wire requests are unchanged.

### Source changes

The driver adds an internal root-only `Transaction::executeInsertAndCommit()` method, a Connection helper, and one WorkerProcess operation. The worker classifies failures through the existing SQL-error path. No public connection or transaction interface changes.

Existing commit finalization is extracted into `Transaction::finishCommit()`. Both conventional and combined commits use it to finalize transaction state, close statements, complete callbacks, and release the connection transaction lease. Conventional commits therefore gain a private helper call, even though their wire sequence is unchanged.

The queue callback still runs after BEGIN succeeds and checks a nonnegative availability deadline. Its insertion callback invokes the experimental method. A private `commitInWorker` flag prevents the storage wrapper from issuing another COMMIT after successful insertion. Other callbacks keep their existing commit or rollback path.

The positive-ID restriction is the queue's existing identity contract. It is not a proposed general-purpose SQLite insertion API: SQLite can accept row IDs that this queue rejects.

This patch also avoids constructing a parent-side `SqliteResult` for successful insertion and sends a smaller response. The experiment is consequently not a pure measurement of one event-loop round trip.

### Failure semantics

- SQL failure or invalid ID before COMMIT leaves the transaction active. The queue's existing catch path rolls it back before returning the failure.
- COMMIT failure follows the same active-transaction rollback path when the connection remains usable.
- A lost or malformed reply triggers the existing fail-close path. The transaction becomes inactive locally; the queue does not issue another rollback or replay on that closed connection.
- A missing confirmation means an unknown outcome. The insert may have committed. Local cleanup callbacks do not prove database rollback.

The lost-response behavior was checked by source inspection, not by fault injection. No new deadline, automatic retry, transaction grouping, or checkpoint operation is introduced.

## Measurement method

Seven pairs per workload run serially on the same host. Odd pairs run A then B; even pairs run B then A. Each invocation runs the unchanged Doctrine control followed by the broker. `XDEBUG_MODE=off` applies to both arms. No timing probes or mechanism counters run inside measured builds.

- **Concurrent:** three publishers, two native consumers, 3,000 measured messages on one queue. Throughput is unique completions divided by cohort elapsed time through the final required ACK. This is a finite-cohort result, not sustained capacity.
- **Roundtrip:** unchanged sequential coordination workload with a 30-second measurement window per backend. Throughput counts unique completions inside that window.
- **Storage:** WAL/NORMAL, statement cache capacity 64, automatic checkpoint threshold 1,000 pages. SQL, payload policy, and transaction boundaries stay unchanged.
- **Doctrine control:** 50 ms polling in every run. Its median paired change is -1.69% concurrent and -0.50% roundtrip.

The `--duration=60` argument on concurrent runs does not turn the fixed 3,000-message cohort into a 60-second sustained run.

## Pair results

Rates are rounded to two decimals here. Paired percentages use unrounded captured rates.

| Pair | Arm order | Concurrent A msg/s | Concurrent B msg/s | Change | Roundtrip A msg/s | Roundtrip B msg/s | Change |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | A then B | 936.87 | 986.43 | +5.29% | 563.07 | 595.60 | +5.78% |
| 2 | B then A | 958.90 | 924.78 | -3.56% | 579.43 | 598.57 | +3.30% |
| 3 | A then B | 948.31 | 841.19 | -11.30% | 560.27 | 563.93 | +0.65% |
| 4 | B then A | 920.27 | 873.29 | -5.11% | 568.17 | 585.97 | +3.13% |
| 5 | A then B | 966.73 | 932.54 | -3.54% | 565.27 | 612.53 | +8.36% |
| 6 | B then A | 960.88 | 1007.94 | +4.90% | 479.00 | 563.67 | +17.68% |
| 7 | A then B | 934.55 | 942.24 | +0.82% | 544.17 | 521.80 | -4.11% |

Concurrent paired changes range from -11.30% to +5.29%; roundtrip changes range from -4.11% to +17.68%. No confidence interval or significance claim is made. The roundtrip median is not the large gain from pair 6.

## Latency and CPU

The table contains medians of the seven run-level percentiles. It does not pool individual samples across runs. All latency units are milliseconds.

| Workload and metric | Baseline p50 / p95 / p99 | POC p50 / p95 / p99 |
| --- | ---: | ---: |
| Concurrent send | 0.642 / 1.437 / 9.670 | 0.580 / 1.273 / 9.604 |
| Concurrent receive | 0.553 / 1.167 / 7.158 | 0.678 / 1.170 / 7.431 |
| Concurrent ACK | 0.372 / 0.852 / 6.820 | 0.470 / 0.845 / 6.633 |
| Concurrent full cycle | 1582.069 / 2065.597 / 2110.060 | 1588.417 / 2225.186 / 2285.350 |
| Roundtrip send | 0.495 / 0.982 / 2.442 | 0.443 / 0.907 / 1.927 |
| Roundtrip receive | 0.392 / 0.874 / 4.891 | 0.387 / 0.842 / 4.974 |
| Roundtrip ACK | 0.243 / 0.495 / 2.788 | 0.243 / 0.495 / 2.788 |
| Roundtrip full cycle | 1.403 / 2.630 / 8.870 | 1.335 / 2.552 / 8.795 |

Median observed concurrent CPU is 2.00 → 2.01 core-seconds for the broker and 1.92 → 1.94 for the driver child. No concurrent CPU reduction is demonstrated. These figures cover observed measure and drain intervals for those two roles, not total application CPU.

Insertion latency improves in both workloads, but concurrent receive and median ACK latency worsen. Full-cycle concurrent tails worsen too. This run does not establish whether the throughput result comes from scheduling changes, COMMIT tails, host noise, or another cause. COMMIT distributions and request-boundary timings were not reprofiled for this POC.

## Mechanism and correctness checks

A separate standalone check uses one real SQLite child and observes IPC through Amp's existing channel parser. It executes conventional and combined insertions with identical queue-shaped BLOB SQL, then representative claim and settlement transactions.

It verifies:

- Actual insertion requests change from `executeControl.begin → execute → executeControl.commit` to `executeControl.begin → executeInsertAndCommit`.
- BEGIN uses IMMEDIATE. Its successful reply precedes the controlled clock marker, which precedes the INSERT send.
- The positive returned ID, availability value, and exact BLOB bytes are visible after commit. The transaction is inactive.
- Representative lifecycle requests change from 10 to 9. No `closeResult` request occurs.

The three native COMMITs are established by source inspection, not native runtime counters. The representative lifecycle check is separate from the measured 3,000-message broker workload.

Driver validation passes 402 tests with 896 assertions, including 10 focused cases with 38 assertions. The focused cases cover committed data, released leases and callbacks, invalid IDs, SQL and COMMIT failures, active-result restrictions, and nested transaction rejection. Queue formatting, PHPStan, and correctness QA pass. No independent review verdict is available yet.

## Questions for the advisor

1. Does the modest unloaded improvement justify further work when the concurrent result is negative and no CPU saving is demonstrated?
2. If continuing, should the next step attribute the concurrent change before extending the prototype to claim and settlement?
3. Is the failure contract sufficient for a larger experiment, given that lost replies were source-reviewed but not injected?

There is no evidence here for changing durability or checkpoint policy. Extending claim and settlement could remove additional exchanges, but its performance and correctness have not been tested by this POC.

## Evidence and reproduction

Local raw evidence is retained under `var/bench/insert-two-exchanges-20261009/`: `plan.json`, `schedule.json`, frozen A/B sources and locks, `runs.json`, `analysis.json`, and per-slot task reports. Each slot records its capture path, command, source hash, and exit code. Captures remain under `var/bench/native-*`; raw captures are not committed with this report.

The standalone mechanism source and result are under `/tmp/insert-poc-mechanism/`. This temporary check is not installed as production code.

Reproduce candidate workloads from the pushed queue branch after `composer install`:

```sh
XDEBUG_MODE=off vendor/bin/castor bench --workload=concurrent --duration=60 --synchronous=normal --polling-ms=50
XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=30 --synchronous=normal --polling-ms=50
```

The local `run-pairs.py` freezes both arms, runs all alternating pairs, and restores B. It has already completed and refuses to overwrite existing slots. Use a new evidence directory for another comparison. The pair table above retains the rates independently of local raw files.

### Capture inventory

All entries below are directory names under `var/bench/`.

| Workload | Pair | A capture | B capture |
| --- | ---: | --- | --- |
| Concurrent | 1 | `native-20261009-224930-9d924b36` | `native-20261009-224940-33b69253` |
| Concurrent | 2 | `native-20261009-225000-b4baba1a` | `native-20261009-224950-0e7fd68f` |
| Concurrent | 3 | `native-20261009-225010-d4557b45` | `native-20261009-225020-2fa2e971` |
| Concurrent | 4 | `native-20261009-225041-479b3a6d` | `native-20261009-225030-faabc9da` |
| Concurrent | 5 | `native-20261009-225051-fbced7a1` | `native-20261009-225101-58619bc7` |
| Concurrent | 6 | `native-20261009-225121-18f938fa` | `native-20261009-225111-e6b05a9b` |
| Concurrent | 7 | `native-20261009-225131-199fb8da` | `native-20261009-225141-89b862ce` |
| Roundtrip | 1 | `native-20261009-225151-13c1a825` | `native-20261009-225301-9da4e871` |
| Roundtrip | 2 | `native-20261009-225520-519cc118` | `native-20261009-225410-c91a02ed` |
| Roundtrip | 3 | `native-20261009-225630-5ede2a83` | `native-20261009-225740-8745e722` |
| Roundtrip | 4 | `native-20261009-230000-f470649a` | `native-20261009-225850-38379669` |
| Roundtrip | 5 | `native-20261009-230110-f260b01d` | `native-20261009-230220-6c9c444f` |
| Roundtrip | 6 | `native-20261009-230443-e7ee3d68` | `native-20261009-230330-2782c829` |
| Roundtrip | 7 | `native-20261009-230552-5e5b9849` | `native-20261009-230702-a47e7c1b` |
