# Task 08: real backend comparison and package MVP acceptance

Status: TODO
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
