# Run the Messenger benchmark

The benchmark boots a Symfony application, publishes through its configured message bus, and starts stock `messenger:consume` workers. It compares the current broker transport with stock Doctrine using native PDO SQLite immediate transactions. It does not change production queue behavior.

## Check the wiring

Install dependencies with `composer install`, then run a small correctness capture:

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --smoke --workload=roundtrip
```

Smoke runs are not performance evidence. Other selectable workloads are `idle`, `fixed-rate`, `application`, `delayed`, and `retention`.

## Run a pilot

Pilots allow a dirty checkout and preserve a source snapshot. They are separate from formal comparisons.

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --pilot --duration=2 --repetitions=1 --workload=application --rate=5 --capacity=16 --handler-ms=100
```

The application workload has an execution consumer that waits synchronously for the configured handler duration and publishes a correlated result. A second consumer handles the result. This models execution-to-control routing without calling an external service.

## Run a declared comparison

Formal runs require a clean Git checkout. Choose the complete configuration before looking at outcomes. Defaults are one pair and 60 seconds per backend, about two minutes plus setup for time-based workloads. Recording and resource collection use one fixed policy, without instrumentation profiles.

```sh
	XDEBUG_MODE=off vendor/bin/castor bench --workload=fixed-rate --duration=60 --repetitions=1 --rate=20 --capacity=64
	XDEBUG_MODE=off vendor/bin/castor bench --workload=application --duration=60 --repetitions=1 --rate=5 --capacity=16 --handler-ms=100
	XDEBUG_MODE=off vendor/bin/castor bench --workload=retention --cycles=20 --cycle-messages=20 --settling=0.25 --repetitions=1
```

Do not run compared backends concurrently or run builds during measurement. Every planned repetition remains in the report. Exit code 1 can mean a public operation failed, integrity or telemetry coverage failed, or the generator could not maintain the planned arrivals. Do not retry until green.

Use `vendor/bin/castor bench --help` for task options or `php bin/benchmark run --help` for the underlying command.

## Inspect a capture

Each command prints its directory under `var/bench/`. Start with `report.md`, `summary.json`, and `manifest.json`. Per-run artifacts include configuration, operation streams, phase/resource records, expected cohorts, errors, and post-drain analysis. Database evidence is retained after owned processes stop. Remove old captures yourself when no longer needed.

Raw captures and `bench/results/` are ignored by Git. Older archives remain on the originating machine and in historical commits, not in new checkouts. Publish compact readable findings and capture identifiers in [the comparison](../docs/benchmark-comparison.md), rather than committing binary archives.

Read [the measurement method](../docs/benchmark-method.md) before interpreting the numbers. Resource accounting is partial, internal SQLite diagnostics are unavailable, and a short stable retention series is not proof of leak-free operation.
