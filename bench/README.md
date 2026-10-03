# Compare the SQLite transports

Install development dependencies with `composer install`. Use PHP 8.5 on Linux with `/proc`, `getconf`, `ext-pdo_sqlite`, `ext-posix`, and `ext-pcntl` available.

Run all six workloads from the checkout:

```sh
	XDEBUG_MODE=off php bin/benchmark run
```

Do not run other benchmarks or builds at the same time. The command creates a private, unique directory under `var/bench/` and prints its path. It uses fresh file-backed databases on that filesystem, never a database supplied through environment variables. It removes its database files after each repetition and retains evidence files.

Read `report.md` for per-run percentiles. Read `summary.json` for configuration, versions, integrity, resources, throughput, and individual-run variation. Each repetition has `config.json`, `result.json`, child logs, and raw `samples/*.jsonl` files. Keep the whole directory to reproduce the analysis.

The command runs paired Doctrine and broker backends with alternating order, one warmup, and three measured repetitions each. Exit code 0 means every scheduled repetition completed without accounting errors. Exit code 1 means a repetition failed or was interrupted. A successful exit does not itself establish a speedup. Read the comparison verdict and resource tradeoffs.

For a short execution check, use:

```sh
	php bin/benchmark run --smoke
```

Smoke uses four messages per publisher or prefill queue and one pair per workload. Do not use smoke results as performance evidence. To run one workload, add `--workload=idle`, or another name from [the method](../docs/benchmark-method.md). Use `--workload=concurrent` for the tail-capable 3000-message workload. A full capture has 48 backend repetitions.

For repository task reports, use `vendor/bin/castor bench` with the same options. Keep every repetition, including SQLite lock failures. The runner does not retry an incomplete repetition to obtain a favorable result.

Single-queue broker consumers use notification waits. Multi-queue consumers retain polling. Both use the same serializer, payload verification, visibility timeout, and WAL/FULL durability. Broker startup and its SQLite child are included in process accounting. Socket files use an owned private directory under `/tmp` to stay below Unix path limits. Databases remain under `var/bench/` for both backends.

Current reports use method `paired-v3-immediate`. Standard Doctrine receives use immediate transactions through PHP 8.5's native PDO SQLite option. The runner verifies and records the effective mode. No custom empty-poll optimization is enabled. Keep older DEFERRED captures separate, including their lock failures. Do not mix revisions in a comparison.

For production-like measurements through Castor, use `XDEBUG_MODE=off vendor/bin/castor bench`. The override reaches both backends and their children. Reports distinguish effective modes from the INI default and verify worker inheritance at startup. Keep earlier develop-mode captures as diagnostic evidence, not production capacity. Do not rerun failed repetitions until they pass.

Use `php bin/benchmark run --help` for options. Performance workloads are separate from correctness tests. See [the method](../docs/benchmark-method.md) for timing definitions and [the recorded baseline](../docs/benchmark-baseline.md) for results and comparison limits.

On SIGINT or SIGTERM, the coordinator stops its children and writes the available results. Unexecuted repetitions remain counted as missing. A forced kill of the coordinator cannot provide completed cleanup evidence.
