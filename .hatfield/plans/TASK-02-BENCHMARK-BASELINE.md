# Task 02: portable benchmark runner and SQLite baseline

Status: DONE, baseline captured with failures retained; no A/B result
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: [Task 01](TASK-01-SETUP-AND-CONTRACTS.md)
Read first: [PLAN.md](PLAN.md), sections 9–11.

## Goal

Create the package-owned benchmark and capture the standard Messenger SQLite baseline before candidate tuning. This task does not claim a broker comparison exists yet.

## Scope

- Add a small standalone runner under `bench/` with one documented invocation, inspired by sqliteq's runner rather than a general benchmark framework.
- Use standard Symfony Messenger and Doctrine SQLite as development/benchmark dependencies. Do not import a consuming application or its custom transport, kernel, or fixtures.
- Define portable workloads: send/receive/ACK, concurrent publishers and consumers including many-to-one, backlog across named queues, idle pickup, delayed eligibility, and fixed small/larger payloads.
- Use separate processes where measuring interprocess competition. Use explicit isolated databases, sockets, environments, positive readiness, hard run bounds, and owned-tree teardown.
- Record effective durability, versions, offered load, payloads, worker counts, polling settings, warmup, and machine/storage information.
- Produce raw correlated samples, per-run percentiles/counts, throughput, errors/unfinished operations, scheduling lag, and resource accounting.
- Establish repetition/sample budgets and the comparison method from baseline variability before measuring the candidate.

## Boundaries

The command runs with this package and its declared dependencies alone. The baseline is standard Doctrine transport, not an application-specific wrapper. A portable development task wrapper may invoke the standalone command, but is not its runtime dependency.

Do not invent a mock broker protocol, unimplemented adapter, batch/priority workload, or timing-sensitive unit-test gate. Candidate support is completed in Task 08 against the real adapter.

## Acceptance criteria

- [x] A documented command runs every baseline workload against real file-backed SQLite and cleans up only its owned resources.
- [x] Reports explicitly identify baseline-only runs as incomplete for A/B comparison.
- [x] Equivalent-durability requirements and polling configuration are recorded rather than inferred from defaults.
- [x] Message IDs, payload integrity, deliveries, ACK outcomes, failures, and unfinished work are accounted for.
- [x] Raw samples and machine-readable/readable summaries include p50/p95/p99/max with counts; insufficient tail samples are labeled inconclusive.
- [x] Delayed lateness is measured from eligibility; cross-process clock assumptions and publisher-confirmation ordering are handled correctly.
- [x] Repeated baseline runs preserve outliers and failures and establish variation without favorable-run selection.
- [x] There is no consuming-application dependency or fake successful candidate result.

## Validation and handoff

Add deterministic checks for metric calculations, percentiles, report generation, missing/incomplete results, and process cleanup. Keep long performance runs outside ordinary unit tests. Record baseline commands and artifacts with exact versions/configuration so Task 08 can repeat the same workloads.

## Completion record

- Implementation revision `13a43bc`. Capture source hashes match that revision; capture metadata records the parent revision and dirty state because commit signing initially failed.
- `composer qa` passed with 27 tests and 123 assertions on PHP 8.5.10.
- `php bench/run.php` executed all 24 scheduled repetitions. Exit 1 correctly reports 14 incomplete repetitions, primarily exhausted Doctrine SQLite lock retries. No failed run was discarded.
- All owned-process survivor lists were empty. No queue database files remained in the capture directory.
- [Run instructions](../../bench/README.md), [method v1](../../docs/benchmark-method.md), and [capture with retained raw archive](../../docs/benchmark-baseline.md) record configuration, versions, workload budgets, outcomes, and observed variation.
- The standalone runner requires Linux process accounting facilities. The package runtime requirements are unchanged.

Task 08 still owes the real candidate, equivalent diagnostics/durability, paired runs, and sufficient tail samples. Doctrine's second-resolution delayed eligibility is not equivalent to the broker's required millisecond deadline behavior. The lock failures and small sample counts do not support a concurrent-tail performance claim.

## PR review follow-up

PR #2 review replaced the custom autoloader and procedural scripts with Composer PSR-4 autoloading and Symfony Console commands. `php bin/benchmark run` is the current entry point. Publisher and consumer classes own worker behavior; Symfony Process owns subprocess execution with an isolated environment. The original archive remains a historical capture of `13a43bc`.

`composer qa` passes with 31 tests and 136 assertions, including Console help/options, worker argument validation, the clock responder, and inherited-environment isolation. `composer dump-autoload --strict-psr --optimize` passes. The refactored CLI's all-workload smoke run retained three complete and three incomplete repetitions from Doctrine lock failures. No smoke timing is promoted to performance evidence.
