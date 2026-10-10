# INSERT error and concurrent-attribution follow-up

## Decision

Keep **`401460a` as the accepted baseline**. Leave `experiment/insert-two-exchanges` experimental. Do not extend the compound operation to claim or settlement.

The two error-reporting defects are fixed, and deterministic fault tests cover both sides of COMMIT. The ACK summaries reconcile with raw records. One separate concurrent A/B diagnostic confirms insertion saves exchange time, but unchanged claim and settlement paths absorb that saving in this sample. It does not establish the cause of the earlier seven-pair slowdown.

The [original throughput report](2026-10-09-insert-two-exchanges.md) still reports -3.54% concurrent and +3.30% roundtrip at driver `27371fb`. **No new throughput comparison was run.** The timings below come from instrumented diagnostic builds, not release-performance measurements.

## Error-contract fixes

Driver [`bbc1053`](https://github.com/ineersa/amp-sqlite3/commit/bbc1053c4ee4ce630876999414dab1d976547711) remains on the experimental branch. [Patch against the first POC](https://github.com/ineersa/amp-sqlite3/compare/27371fbdcddb39eed0e4449af467adb4b3b6b2b0...bbc1053c4ee4ce630876999414dab1d976547711).

### Preserve the failing query

The worker wraps a native COMMIT exception as the existing `SqliteQueryError`, carrying query `COMMIT`, primary code, and extended code. The worker error envelope carries this query, and `WorkerResponse` validates and preserves it. Native INSERT errors keep the existing INSERT query attribution.

Tests assert that an INSERT uniqueness failure reports the INSERT SQL with primary code 19 and extended code 2067. A deferred foreign-key COMMIT failure reports `COMMIT` with primary code 19 and extended code 787. The latter remains rollbackable, and neither failure invokes a successful commit callback.

### Separate validation from native SQLite errors

Positive-ID validation now throws `RuntimeException`, not a fabricated `SQLite3Exception`. The existing generic SQL-error path returns null native primary and extended codes. Invalid-ID cases assert both null codes, the original query, active transaction state, successful rollback, and no successful commit callback.

No production SQL, clock placement, transaction boundary, or success-path commit count changes in this fix.

## Deterministic failure-boundary tests

Tests use a real child worker and a file-backed database. A control channel parks the worker at an exact stage. Generated test-local source copies add barrier hooks; installed production code has no hooks or injection framework. Safety cancellations abort a hung test but do not establish correctness. There are no sleeps or elapsed-time assertions.

| Fault | Barrier and observations |
| --- | --- |
| Worker exits after INSERT, before COMMIT | Caller fails; connection and local transaction become unusable; reopening finds zero rows. A new IMMEDIATE transaction succeeds, proving writer ownership was released. |
| COMMIT succeeds, then the worker exits before replying | While parked after native COMMIT, an independent read already sees one row. Caller receives failure, not successful confirmation. Reopening still finds exactly one row; the old connection is unusable and ownership is released. |
| COMMIT succeeds, then success contains a malformed ID payload | Parent rejects the payload, fails the connection, releases ownership, and reports no successful completion. Reopening finds exactly one row. |

All cases assert no successful commit callback, cleared local transaction ownership and leases, rejection of further rollback or query use on the closed connection, and no replacement worker start. Row counts and the source's single-send path provide no-replay evidence. The tests exercise the driver operation directly, not the public queue socket protocol.

These are observed failure cases. A worker that remains connected but silently withholds its reply still inherits an unbounded operation receive. Local rollback cleanup callbacks on a broken connection do not establish database rollback.

Driver formatting, static analysis, and tests pass: **409 tests, 989 assertions**, approximately 19 seconds. Queue Castor formatting and QA also pass. The driver and queue main branches and driver development branch are unchanged.

## Existing ACK-summary audit

No benchmark rerun was needed. All 14 roundtrip broker captures were checked independently from raw successful ACK spans for expected message IDs. Nearest-rank per-run percentiles were recomputed, matched to each capture's summary, and then reduced to the median over seven runs. All raw capture hashes are distinct.

| Median of run-level ACK percentiles | Baseline ms | POC ms | POC minus baseline µs |
| --- | ---: | ---: | ---: |
| p50 | 0.243112 | 0.242707 | -0.405 |
| p95 | 0.494961 | 0.495460 | +0.499 |
| p99 | 2.788066 | 2.788368 | +0.302 |

Both arms round to `0.243 / 0.495 / 2.788` at three decimal places. The extraction is correct; the displayed equality is rounding, not identical raw results. These are not paired latency estimates and do not prove unchanged ACK behavior.

## Concurrent diagnostic method

One serial A then B diagnostic uses accepted baseline `401460a` and error-fixed prototype `bbc1053`. Each invocation uses the existing three-publisher, two-consumer, 3,000-message workload, unchanged SQL and payload policy, WAL/NORMAL, cache capacity 64, and the 1,000-page automatic checkpoint threshold. Both retain the normal 50 ms Doctrine control. All four backend outcomes pass integrity, accounting, and cleanup.

The probes record bounded in-memory arrays, then write them at process shutdown. They add no wire requests and retain no SQL, parameters, or payloads. Temporary copies are restored byte-for-byte afterward. No checkpoint observation pragma or syscall trace is run.

Collected evidence:

- Two monotonic-clock reads bracket only native `SQLite3::exec('COMMIT')`, tagged insert, claim, or settlement.
- Driver parent and worker timestamps correlate by request ID. Every request's clock order is checked on the same-host Linux monotonic clock.
- Storage mutex queue, acquisition, and release timestamps identify operation kind and associate driver requests with the owning storage operation.
- Full acquisition order, native commit order, request labels, and gaps between exchanges are retained.

Native and wire totals use the benchmark's exact measurement start through final-completion observation. Storage operations are selected by acquisition in that interval and retain their complete lifetime. One B empty claim crosses the end boundary: its final rollback request starts after the interval. It is included in complete owner statistics, not in the window's wire count. Startup, warmup, and cleanup are excluded from the tables below.

The probes add clock, classification, and array-recording overhead. Different runs can also encounter different COMMIT tails and host conditions. This single pair cannot isolate a causal performance effect or reverse the prior throughput conclusion.

## Native COMMIT distributions

Each arm has **9,000 measured native COMMITs**, exactly 3,000 per kind. Whole-process totals are 3,006 per kind, including six warmup messages.

| Arm and kind | Total ms | p50 µs | p95 µs | p99 ms | Maximum ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| A insert | 267.673 | 25.391 | 52.166 | 2.121 | 8.526 |
| B insert | 293.050 | 25.984 | 51.769 | 2.206 | 11.803 |
| A claim | 285.674 | 30.172 | 64.488 | 2.402 | 6.958 |
| B claim | 325.329 | 31.697 | 68.589 | 2.709 | 7.385 |
| A settlement | 288.557 | 26.927 | 54.991 | 2.534 | 10.691 |
| B settlement | 255.748 | 28.496 | 57.227 | 2.447 | 9.019 |
| A all | 841.904 | 27.354 | 60.169 | 2.328 | 10.691 |
| B all | 874.127 | 28.814 | 62.525 | 2.414 | 11.803 |

The slowest 1% consumes 58.96% of A native COMMIT time and 57.74% of B. Total native COMMIT time rises 32.223 ms in B, but this is not uniform: insert rises 25.377 ms, claim rises 39.654 ms, and settlement falls 32.809 ms. No new trace establishes which of these particular commits performed checkpoint work.

## Storage ownership and actual request counts

| Kind | A owners | B owners | A mean wait µs | B mean wait µs | A mean hold µs | B mean hold µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Insert | 3,000 | 3,000 | 759.326 | 660.772 | 323.586 | 294.858 |
| Claim | 3,001 | 3,002 | 306.475 | 293.559 | 438.296 | 476.290 |
| Settlement | 3,000 | 3,000 | 242.342 | 240.926 | 322.984 | 328.219 |
| Eligibility | 1,241 | 1,371 | 700.960 | 611.899 | 83.937 | 94.066 |

Wait totals overlap between callers and must not be added to cohort elapsed time. Hold intervals do not overlap. There is no concurrent storage owner in either arm.

All 3,000 successful inserts have three exchanges in A and two in B. Successful claims have four in both; settlements have three in both. Empty claims use BEGIN, SELECT, and ROLLBACK.

| Window request accounting | A | B |
| --- | ---: | ---: |
| Successful insert, claim, settlement requests | 30,000 | 27,000 |
| Eligibility requests | 1,241 | 1,371 |
| Empty-claim requests starting inside window | 3 | 5 |
| Total parent requests starting inside window | 31,244 | 28,376 |

The 3,000 removed exchanges become a net reduction of 2,868 window requests because eligibility increases by 130 and empty-claim requests by two. B's additional rollback after the window explains why two complete empty claims contribute five window requests rather than six. All whole-process parent requests correlate to worker IDs; no probe records are dropped.

## Exchange and hold-time attribution

The next table sums complete selected storage-owner lifetimes. Parent exchange time partitions into outbound boundary, worker handler, and return boundary. The boundaries mix serialization, transport, dispatch, and scheduling; they are not pure IPC or pure CPU.

| Holding-time component, milliseconds | A insert | B insert | A claim | B claim | A settlement | B settlement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Parent send start to handler start | 250.625 | 205.692 | 279.970 | 301.598 | 210.965 | 227.174 |
| Worker handler | 388.375 | 419.821 | 504.784 | 561.320 | 404.884 | 379.254 |
| Handler return to parent receive return | 223.497 | 178.747 | 367.716 | 393.844 | 239.946 | 256.566 |
| Gaps between exchanges within owner | 76.983 | 44.256 | 129.085 | 137.233 | 79.856 | 85.983 |
| Before first and after last exchange | 31.277 | 36.056 | 33.770 | 35.828 | 33.302 | 35.682 |
| Total hold time | 970.758 | 884.573 | 1315.325 | 1429.821 | 968.953 | 984.657 |

Components partition hold time before independent rounding. Driver mutex waits are small: roughly 0.25–0.30 µs median in these paths. The contested ownership lane is the broker storage mutex, which stays held across exchanges.

Insertion saves **86.185 ms** of total holding time. Its outbound and return boundaries together fall by 89.683 ms, and between-exchange gaps fall by 32.727 ms. Its worker handler rises by 31.446 ms, mostly accounted for by the higher native insert COMMIT time in this pair.

Claim holding rises **114.496 ms**. Of that, 104.291 ms is exchange time: 21.628 ms more before the handler, 56.536 ms more inside it, and 26.128 ms more after it. The native claim COMMIT increase accounts for 39.654 ms of the handler increase. The unchanged four-request path is slower in multiple components, not only in storage-mutex waiting.

Settlement holding rises 15.704 ms; eligibility holding rises 24.798 ms. Including these paths, total storage holding is 3,359.202 ms in A and 3,428.015 ms in B. Time between owners rises from 218.087 to 237.673 ms. These are diagnostic timing budgets, not a new throughput estimate.

## Operation ordering

The maximum same-kind acquisition streak remains three inserts, two claims, and two settlements in both arms. There is no evidence here of an unbounded publisher streak. However, producers and consumers switch less often in B: insert-to-claim transitions fall from 159 to 44, and claim-to-insert transitions from 159 to 43.

Successful commit order provides a backlog view without inventory SQL. At the final insert, A has committed 159 claims and 158 settlements; B has committed 86 claims and 85 settlements. Peak committed-but-unclaimed work is 2,841 in A and 2,914 in B. Peak committed-but-unsettled work is 2,842 and 2,915 respectively. Both end with zero measured backlog.

This is consistent with publication shifting the arrival pattern. It does not prove that this pattern caused the original -3.54% result. Native COMMIT variation and slower unchanged exchanges also appear in the sample. No policy or notifier algorithm was changed to force the ordering.

## Conclusion and retained evidence

The fault boundary is now exercised rather than only source-reviewed. The ACK extraction is verified. The removed INSERT exchange yields a real timing saving in this diagnostic, but the diagnostic also shows where that saving is offset. It does not resolve the concurrent effect enough to justify expanding the API.

Stop the expansion here. Preserve the accepted baseline and the experimental branch. If a later task requires causal isolation, control the publication arrival pattern or the shared-path comparison in a separate experiment rather than changing transactions or checkpoint policy.

Evidence remains under `var/bench/insert-followup-20261010/`: `ack-audit.json`, frozen locks and manifests, original and diagnostic source copies, raw parent/worker records, `analysis.json`, task logs, and `restore.json`. The captures are `native-20261010-020226-7843b1f2` for A and `native-20261010-020236-52d3faa2` for B. Raw evidence is not committed as benchmark output.

Recompute diagnostic statistics with `python3 var/bench/insert-followup-20261010/analyze.py`. It reads retained artifacts only and runs no SQLite operations. The `prepare.py` and `run.py` files describe the source hooks and execution; `run.py` refuses to overwrite completed evidence. Do not rerun it in the same directory.
