# Run the SQLite baseline

Install development dependencies with `composer install`. Use PHP 8.5 on Linux with `/proc`, `getconf`, `ext-pdo_sqlite`, `ext-posix`, and `ext-pcntl` available.

Run all six workloads from the checkout:

```sh
	php bin/benchmark run
```

Do not run other benchmarks or builds at the same time. The command creates a private, unique directory under `var/bench/` and prints its path. It uses fresh file-backed databases on that filesystem, never a database supplied through environment variables. It removes its database files after each repetition and retains evidence files.

Read `report.md` for per-run percentiles. Read `summary.json` for configuration, versions, integrity, resources, throughput, and individual-run variation. Each repetition has `config.json`, `result.json`, child logs, and raw `samples/*.jsonl` files. Keep the whole directory to reproduce the analysis.

Exit code 0 means every scheduled baseline repetition completed without accounting errors. Exit code 1 means at least one repetition failed or was interrupted. Both outcomes remain **baseline only**. Neither outcome means a broker comparison succeeded.

For a short execution check, use:

```sh
	php bin/benchmark run --smoke
```

Smoke uses four messages per publisher or prefill queue and one repetition. Do not use smoke results as performance evidence. To run one workload, add `--workload=idle`, or another name from [the method](../docs/benchmark-method.md).

Use `php bin/benchmark run --help` for options. Composer maps `Ineersa\SqliteQueue\Bench\` to `bench/src/` through `autoload-dev`. The executable only loads Composer and registers Symfony Console commands. Hidden `worker` and `clock-probe` commands serve the coordinator; they are not separate scripts to run by hand.

Run deterministic checks separately:

```sh
	composer qa
```

QA does not run the performance workloads. `composer test:bench` runs only benchmark correctness checks. `composer bench` invokes the full standalone runner.

On SIGINT or SIGTERM, the coordinator stops its children and writes the available results. Unexecuted repetitions remain counted as missing. A forced kill of the coordinator cannot provide completed cleanup evidence.
