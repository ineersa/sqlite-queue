# Agent instructions

Use Castor for development and validation. Run `composer install` to install the locked task runner at `vendor/bin/castor`. Do not add Composer QA scripts.

Before submitting changes, run `vendor/bin/castor cs:fix`, then `vendor/bin/castor qa`. Read the result files for any failed task and fix the findings. Do not add PHPStan baselines, ignores, or disable strict rules to make QA pass.

Keep console output brief and machine-readable. Write diagnostics to report files instead of dumping them into the conversation. Tasks live in `.castor/`, imported by `castor.php`.

## Commands

| Command | Scope |
| --- | --- |
| `vendor/bin/castor composer:validate` | Validate Composer metadata and lock consistency. |
| `vendor/bin/castor cs:check` | Check formatting without edits. Include diffs in the JSON report. |
| `vendor/bin/castor cs:fix` | Apply the configured formatting rules. |
| `vendor/bin/castor phpstan` | Analyze implementation and tooling code. Optional `--path=bench/src` overrides configured paths. |
| `vendor/bin/castor test` | Run all correctness tests. Optional `--filter=TaskReportsTest` selects tests. |
| `vendor/bin/castor test:driver` | Run driver tests, with optional `--filter`. |
| `vendor/bin/castor test:bench` | Run benchmark correctness tests, with optional `--filter`. |
| `vendor/bin/castor qa` | Run validation, formatting checks, PHPStan, and tests in that order. No formatting edits. |
| `vendor/bin/castor bench --smoke --workload=roundtrip` | Run a smoke benchmark. Omit `--smoke` for measured runs; omit `--workload` for all workloads. |

QA runs all four checks even when one fails and returns the first nonzero exit code. Performance workloads are not part of QA. Individual tasks preserve tool exit codes. A task-runner exception returns 1 and writes its details to the stderr log.

## Reports

Tool output has no colors or progress indicators. Each task prints one JSON line with `task`, `status`, `exit_code`, and `result`. It does not print tool diagnostics to the console.

`qa` prints one line per check, then one aggregate status line.

Each invocation replaces its task's files under `var/qa/<task>/`. Colons in task names become hyphens, so `cs:check` writes to `var/qa/cs-check/`.

| File | Content |
| --- | --- |
| `result.json` | Command arguments, timestamps, status, exit code, duration, and output paths. Paths are relative to the repository root. |
| `stdout.json` | Native PHPStan or CS Fixer JSON output. |
| `stdout.log` | Standard output for other tools. |
| `stderr.log` | Diagnostics from stderr, including task-runner exceptions. |
| `junit.xml` | PHPUnit's native report, when generated. |

`var/qa/result.json` contains the aggregate QA status and a result path for each check. Each result starts with `status: running` and a null exit code. An interrupted run must not be treated as a pass. A rerun clears old output and removes its old JUnit report before starting. Native reports can be missing or incomplete after startup errors; inspect `result.json` and `stderr.log` first.

Reports retain only the latest invocation per task. Do not run the same task concurrently in one checkout. Archive `var/qa/` before another run if you need to preserve evidence.

The benchmark keeps its timestamped captures under `var/bench/`. `var/qa/bench/stdout.log` records the capture directory. Each capture contains `summary.json`, `report.md`, and raw repetition artifacts. A lock failure still produces a nonzero task result; it is not hidden by report generation.

## Configuration

`.php-cs-fixer.dist.php` applies Symfony and Symfony risky rules, strict types, binary `fopen` flags, `$this` PHPUnit calls, and the configured class-element order. It covers source, benchmark classes, tests, Castor tasks, and PHP entry points.

`protected_to_private` is disabled as requested.

`phpstan.neon.dist` uses level 6, strict rules, the PHPUnit extension, and `treatPhpDocTypesAsCertain: false`. It analyzes `src/`, `bench/src/`, Castor tasks, and entry points, but not test files. Castor's extension supplies its context type alias. No baseline, error ignores, or disabled strict rules are configured.
