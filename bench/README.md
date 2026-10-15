# Run the Messenger benchmark

The benchmark compares Symfony's Doctrine SQLite transport with sqlite-queue through configured buses and stock `messenger:consume`. Each invocation runs Doctrine first, then the broker, once each. It characterizes that application and machine, not universal transport capacity.

## Requirements

Use PHP 8.5 with the development dependencies installed by `composer install`. The benchmark needs Linux with readable owned-process entries in `/proc`, `getconf`, and the `pcntl`, `posix`, `pdo_sqlite`, and `sqlite3` extensions. Resource accounting discovers the driver's child through the broker's bounded descendant tree.

These are benchmark requirements, not a Linux requirement for the package's Unix-socket runtime. The broker uses `sqlite3` through its driver; PDO SQLite is needed for the Doctrine comparison. The foreground broker command also requires `pcntl` and `posix`.

## Check the wiring

Install dependencies with `composer install`, then run:

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --smoke --workload=concurrent
```

Smoke checks wiring, payload validation, accounting, and owned-process cleanup with small cohorts. It is not performance evidence.

## Measure a workload

| Workload | Question and measured budget |
| --- | --- |
| `roundtrip` | Unloaded public latency with one message in flight, over a timed window. This is the default workload. |
| `concurrent` | Drain a released 3,000-message cohort with three publishers and two consumers. This is not a sustained-capacity test. |
| `application` | Five workflows/s, two consumers, 100 ms handler work, and at most sixteen outstanding workflows. Completion requires root and result ACKs. |
| `idle` | Empty-queue activity over a timed interval, followed by two isolated pickups in a separate phase. |
| `multi-queue` | One consumer selects `async results`; an empty interval precedes an isolated arrival on each queue. |
| `retention` | Twenty cycles of one hundred messages with the same processes and clients, comparing audited-empty memory points. |

Time-based workloads default to 60 seconds per backend, about two minutes plus setup and drain. `--duration` selects a positive finite window up to 3600 seconds; it does not resize the concurrent cohort or retention cycles. Smoke substitutes small budgets, including twelve concurrent messages and two retention cycles of two messages.

`--polling-ms` sets Doctrine's idle poll interval to an integer from 1 to 1000 milliseconds, defaulting to 50. Broker consumers use WAIT, or WAIT_ANY for `multi-queue`, rather than that polling interval. Each worker resolves only its selected transports; the multi-queue worker intentionally opens both. Do not pool its changed topology with single-queue measurements.

`--synchronous=normal|full` selects the same WAL synchronous mode for both backends, defaulting to NORMAL. Owning connections confirm the effective mode. NORMAL can lose recent commits after OS failure or power loss; do not combine NORMAL and FULL results.

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=60 --synchronous=normal
	XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=60 --synchronous=full
	XDEBUG_MODE=off vendor/bin/castor bench --workload=concurrent --synchronous=normal
	XDEBUG_MODE=off vendor/bin/castor bench --workload=application --duration=60
	XDEBUG_MODE=off vendor/bin/castor bench --workload=idle --duration=60
	XDEBUG_MODE=off vendor/bin/castor bench --workload=multi-queue --polling-ms=1000
	XDEBUG_MODE=off vendor/bin/castor bench --workload=retention
```

The supported controls are `--workload`, `--smoke`, `--duration`, `--synchronous`, and `--polling-ms`. There is no repetition, calibration, or pipeline-depth option. Commit source changes before publishing measurements so the manifest revision identifies the measured code. Run one command at a time without competing builds or benchmarks. Preserve failures rather than retrying until green.

## Inspect results

Castor prints a task-result JSON line. Find the capture path in `var/qa/bench/stdout.log`, then read `report.md`, `summary.json`, `manifest.json`, and each backend's `result.json` under that `var/bench/` directory. Inspect task diagnostics in `var/qa/bench/` if startup fails. Raw captures and `bench/results/` are ignored by Git.

Check execution, integrity, accounting, and cleanup before interpreting rates or percentiles. Recorder losses, write failures, missing boundaries, or observer errors invalidate affected performance claims. Bounded recording and phase snapshots still cost time and I/O. The [measurement method](../docs/benchmark-method.md) explains completion boundaries, role attribution, sample limits, and retention caveats. [Historical observations](../docs/benchmark-comparison.md) are not current-build measurements.

## Manage capture storage

Captures accumulate and can contain queue databases, analysis databases, logs, JSONL traces, and diagnostic profiles. There is no automatic capture-retention policy. If these directories exist, inspect their usage before choosing what to keep:

```sh
	du -sh var/bench var/qa
	du -h --max-depth=1 var/bench
```

Archive only evidence you need, outside the checkout. Delete an individual disposable capture only after confirming its run has finished, all owned processes have exited, and its evidence is no longer needed. Keep failed runs when investigating a comparison. Never delete production database directories or clear all of `var/` as benchmark cleanup.

The latest task reports under `var/qa/` are separate from benchmark captures. Each task invocation replaces its own reports; reviewed reports from completed tasks can be discarded independently. Removing them does not remove captures under `var/bench/`, and an interrupted task is not a completed or passing run.
