# Task 08: real backend comparison and package MVP acceptance

Status: IMPLEMENTED AND MEASURED — performance acceptance inconclusive; advisor review pending
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 02](TASK-02-BENCHMARK-BASELINE.md), [Task 07](TASK-07-FAILURE-AND-LIFECYCLE-PROOF.md)
Read first: [PLAN.md](PLAN.md), sections 9–10 and 12.

## Goal

Complete the standalone A/B benchmark with the actual broker adapter and assess the package MVP on correctness, reproducibility, and measured benefit.

## Scope

- Connect Task 02's runner to the real Messenger broker adapter. Keep the same portable workload and matching serializers/handler work for both backends.
- Re-run the standard Doctrine baseline in the same environment, rather than comparing a new candidate with an old unrelated timing sample.
- Verify effective durability and record versions, configurations, payloads, concurrency, polling/notification behavior, and storage.
- Execute the predetermined repeated paired runs, preserving raw samples, failed operations, outliers, and per-run variation.
- Count all socket/persistence IPC and the complete process tree in latency/resource accounting.
- Finish package documentation for installation, foreground lifecycle, PHP client, Messenger integration, failure/delay contracts, and benchmark reproduction.
- Check every package MVP item in PLAN section 12 against actual code and evidence.

## Boundaries

No consuming-application checkout, test kernel, custom transport, runtime journey, or deployment topology is required to run the package benchmark. Do not tune the success criterion after seeing candidate results or weaken durability to create a speedup.

A direct client microbenchmark must remain separate from the equivalent Messenger comparison. No benchmark of deferred priority/batch APIs. No downstream application migration or package publishing side effect is authorized by this task.

## Acceptance criteria

- [ ] One documented standalone command runs real standard-Doctrine and broker-adapter backends with matching workload/durability and bounded cleanup.
- [ ] Reports retain raw data and per-run p50/p95/p99/max/counts, throughput, failure/unfinished counts, and full-tree resource cost.
- [ ] Cold startup is separate from warm operation; delayed lateness is measured from eligibility; sample sizes support the stated tail estimates.
- [ ] Integrity checks account for all expected IDs, payloads, delivery and ACK outcomes.
- [ ] The report applies the predeclared comparison method and states win, neutral, regression, or inconclusive honestly.
- [ ] A performance-win claim is supported by repeatable concurrent-delivery tail gains beyond baseline variation. Neutral/regressing/inconclusive evidence pauses adoption for review without disguising itself as a correctness failure.
- [ ] All mandatory MVP correctness, delay, failure, lifecycle, and documentation requirements are satisfied without consuming-application dependencies or deferred features.
- [ ] Independent review covers benchmark fairness and specification fidelity, with unresolved findings reported explicitly.

## Validation and handoff

Run package QA and the explicit long benchmark separately. Keep timing-sensitive thresholds out of ordinary unit tests. Preserve report artifacts and exact reproduction commands.

Provide a package-use recommendation with measured limits, not blanket shipping approval. This is the final package task; downstream application integrations are outside its scope.

## Implementation evidence

The runner now schedules real Doctrine and broker Messenger backends in alternating pairs. Method v2 increases only the concurrent workload to 3000 messages, declared before measured candidate runs. One warmup and three measured repetitions per backend produce 48 repetitions for the full matrix. The conservative payload-specific concurrent tail decision is fixed in `docs/benchmark-method.md` and `Comparison`.

Both backends retain `PhpSerializer`, the same handler, matching visibility and durability, original stored deadlines, one diagnostic eligibility query before ACK, and completed-ACK integrity. Broker readiness reads effective PRAGMAs from its actual SQLite worker through startup-only benchmark reflection. Owned-tree sampling includes broker and persistence. Broker final usage and reaped-child usage are also retained. Short socket paths use successfully acquired private directories under `/tmp`; database storage stays under `var/bench`.

Notification waits apply to single-queue consumers. Multi-queue backlog and delayed consumers retain polling, matching the product's lack of multi-queue WAIT. Startup acquisition runs before the go barrier. The benchmark constructs the standard Messenger Worker without an application kernel or replacement consume command.

- `vendor/bin/castor test:bench`: 19 tests / 103 assertions passed.
- `vendor/bin/castor cs:fix && vendor/bin/castor qa`: 374 tests / 2241 assertions passed, zero PHPStan findings.
- `vendor/bin/castor bench --smoke --workload=roundtrip`: both real backends complete. Latest capture `var/bench/20261003-031925-a8352132`.
- `vendor/bin/castor bench --smoke`: all six broker workloads complete. Doctrine concurrent, many-to-one, and backlog preserve upstream SQLite `database is locked` failures. Capture `var/bench/20261003-031024-d8e162fa` is deliberately retained, not rerun to obtain a green result. This is execution evidence, not performance acceptance.
- Full measured matrix: not run during implementation. Run `vendor/bin/castor bench`, or one `--workload=<name>` invocation for each declared workload. Each invocation keeps its alternating order and all three pairs. Do not select only successful repetitions.

Pending work remains the measured baseline and candidate capture, result interpretation, final MVP checklist, and independent fairness review. Inconclusive baseline evidence cannot establish a performance advantage. The existing Doctrine lock failures must not be hidden by retries or durability changes.

## Approved transaction-mode correction

Method `paired-v3-immediate` uses PHP 8.5 native PDO SQLite immediate transactions through DBAL `driverOptions`. The reference application was read only. No middleware, custom transport, or empty-poll optimization was copied. This explicit user-approved correction moves Doctrine writer contention to BEGIN, matching the broker's immediate transaction policy without weakening WAL/FULL durability. Effective PDO mode and broker driver isolation are recorded. All workloads and comparison criteria are unchanged.

The paired-v2 DEFERRED captures above remain preserved and must not be combined with the new revision. Their lock failures remain evidence for the earlier configuration, not failed repetitions to replace inside a v3 comparison.

- Native attribute plus two-connection held-writer BEGIN contention, rollback, and release are tested without sleeps or timing assertions. Timeout zero makes the contention outcome deterministic.
- `vendor/bin/castor test:bench`: 20 tests / 115 assertions passed.
- `vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa`: 375 tests / 2253 assertions passed, zero PHPStan findings.
- `vendor/bin/castor bench --smoke --workload=concurrent`: both real backends complete with 12 confirmed ACKs each and no survivors. Capture `var/bench/20261003-033934-510b5eb9` verifies `method_revision`, all native PDO mode observations, broker isolation, and disabled empty-poll optimization.
- Full measured runs remain unexecuted during this correction. Use fresh v3 paired captures for acceptance.

## Production-like runtime correction

The parent's first full immediate matrix remains under `var/bench/task08-v3-results/*.log`. It included Doctrine concurrent ACK/send failures and Xdebug develop mode. Preserve every capture as diagnostic evidence, not production capacity. No budgets, transaction policies, or comparison thresholds changed in this correction.

`Process::environment` now propagates caller `XDEBUG_MODE` explicitly while continuing to remove unrelated inherited variables. Machine metadata separates effective `xdebug_info('mode')` from the INI default and the environment override. Publisher, prefill, consumer, and broker runtime profiles are recorded and checked against the coordinator.

The vendor SQLite worker has no direct PHP diagnostic RPC. `/proc/<pid>/environ` proved unsuitable after Amp changes the process title. Instead, startup verifies Xdebug modes through an unmodified Amp context inheritance probe inside the broker and checks the actual SQLite worker's interpreter. Reports retain the probe PID and this verification provenance. The extra probe exits before the go barrier, and its cost is included in startup/lifetime resources.

- `vendor/bin/castor test:bench`: 23 tests / 129 assertions passed, including environment isolation, effective-mode metadata overriding a develop INI default, mismatch rejection, and real broker/worker inheritance.
- `vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa`: 378 tests / 2267 assertions passed, zero PHPStan findings.
- `XDEBUG_MODE=off vendor/bin/castor bench --smoke --workload=concurrent`: both real backends complete with 12 ACKs each and no survivors. Capture `var/bench/20261003-040111-54720f25` records effective mode off despite INI develop, six publisher and four consumer profiles, and verified broker/worker context inheritance.
- Next measured command: `XDEBUG_MODE=off vendor/bin/castor bench`, or one unchanged per-workload invocation for each of the six workloads. Run one fixed fresh matrix, retain failures, and do not retry until green. No full measured runs were executed during this correction.

## Final measured matrix

`XDEBUG_MODE=off vendor/bin/castor bench` executed all 48 scheduled backend repetitions in `20261003-040336-da069e7f`. All broker repetitions completed. Two measured Doctrine concurrent repetitions failed with SQLite lock errors; the remaining Doctrine repetitions completed. The command correctly exited 1. There was no retry-until-green.

The final capture is committed as `bench/results/paired-v3-20261003.tar.gz`, including failed repetitions. `docs/benchmark-comparison.md` records configuration, observed ranges, archive checksum, and limitations. Performance acceptance is inconclusive, not a win. Advisor fairness review and the final MVP acceptance decision remain pending. Task 09 records the requested multi-queue WAIT extension separately.
