# Run the SQLite baseline

Install development dependencies with `composer install`. Use PHP 8.5 on Linux with `/proc`, `getconf`, `ext-pdo_sqlite`, `ext-posix`, and `ext-pcntl` available.

Run all six workloads from the checkout:

```sh
	php bin/benchmark run
```

Do not run other benchmarks or builds at the same time. The command creates a private, unique directory under `var/bench/` and prints its path. It uses fresh file-backed databases on that filesystem, never a database supplied through environment variables. It removes its database files after each repetition and retains evidence files.

Read `report.md` for per-run percentiles. Read `summary.json` for configuration, versions, integrity, resources, throughput, and individual-run variation. Each repetition has `config.json`, `result.json`, child logs, and raw `samples/*.jsonl` files. Keep the whole directory to reproduce the analysis.

Exit code 0 means every scheduled baseline repetition completed without accounting errors. Exit code 1 means at least one repetition failed or was interrupted. Both outcomes describe the Doctrine baseline only, not a broker comparison.

For a short execution check, use:

```sh
	php bin/benchmark run --smoke
```

Smoke uses four messages per publisher or prefill queue and one repetition. Do not use smoke results as performance evidence. To run one workload, add `--workload=idle`, or another name from [the method](../docs/benchmark-method.md).

Use `php bin/benchmark run --help` for options. Performance workloads are separate from correctness tests. See [the method](../docs/benchmark-method.md) for timing definitions and [the recorded baseline](../docs/benchmark-baseline.md) for results and comparison limits.

On SIGINT or SIGTERM, the coordinator stops its children and writes the available results. Unexecuted repetitions remain counted as missing. A forced kill of the coordinator cannot provide completed cleanup evidence.
