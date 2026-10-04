# Run the Messenger benchmark

The benchmark publishes through configured Symfony buses and starts stock `messenger:consume`. Each invocation compares Doctrine and the broker once, using the same WAL synchronous mode. NORMAL is the default; FULL is available explicitly.

## Check the wiring

Install dependencies with `composer install`, then run:

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --smoke --workload=concurrent
```

Smoke verifies wiring, payloads, accounting, and cleanup. It is not performance evidence.

## Measure a workload

The available workloads are `roundtrip`, `concurrent`, `application`, `idle`, and `retention`. Controls are `--workload`, `--smoke`, `--duration`, and `--synchronous`.

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=60 --synchronous=normal
	XDEBUG_MODE=off vendor/bin/castor bench --workload=roundtrip --duration=60 --synchronous=full
	XDEBUG_MODE=off vendor/bin/castor bench --workload=concurrent --synchronous=normal
	XDEBUG_MODE=off vendor/bin/castor bench --workload=application --duration=60
	XDEBUG_MODE=off vendor/bin/castor bench --workload=idle --duration=60
	XDEBUG_MODE=off vendor/bin/castor bench --workload=retention
```

Time-based workloads default to 60 seconds per backend, about two minutes plus setup. Concurrent uses a fixed 3000-message cohort. Retention uses twenty cycles of one hundred messages. Smoke substitutes explicitly small cohorts.

Commit the source before publishing measurements so the recorded revision identifies the code. Run one command at a time without competing builds or benchmarks. Do not retry failed comparisons until green.

## Inspect results

Each command prints its capture directory under `var/bench/`. Read `report.md`, `summary.json`, and `manifest.json`, then the per-backend records. Raw captures and `bench/results/` are ignored by Git. Databases and failure evidence remain after owned processes stop.

The [method](../docs/benchmark-method.md) defines the metrics and limits. NORMAL can lose recent commits after machine failure. Resource accounting uses phase snapshots, not continuous peaks. A stable short retention run does not prove leak-free operation.
