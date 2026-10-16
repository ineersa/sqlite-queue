# Develop and validate the package

Use PHP `^8.5` with Composer and the extensions declared in `composer.json`. Development dependencies include PDO SQLite for the Doctrine comparison; the broker itself uses `ext-sqlite3`. Run process and signal tests as a non-root user on Unix.

## Install dependencies

From the repository root:

```sh
	composer install
```

Install the locked dependencies rather than updating them as part of an unrelated change. The task runner is `vendor/bin/castor`; Composer scripts are not used for QA.

## Run the checks

Apply formatting, then run all checks:

```sh
	env -u HATFIELD_SESSION_ID XDEBUG_MODE=off vendor/bin/castor cs:fix
	env -u HATFIELD_SESSION_ID XDEBUG_MODE=off vendor/bin/castor qa
```

`qa` runs Composer validation, formatting checks, PHPStan, and correctness tests. It runs every check even when an earlier one fails, then returns the first nonzero exit code. It does not run performance workloads or change formatting.

Unsetting `HATFIELD_SESSION_ID` gives disposable test children a separate environment. Worker-termination regressions skip when that variable is present or the tests run as root. Disabling Xdebug keeps ordinary correctness runs separate from profiling.

Use focused tasks while changing one component:

| Command | Scope |
| --- | --- |
| `vendor/bin/castor cs:check` | Check formatting without edits. |
| `vendor/bin/castor phpstan` | Analyze implementation and tooling. |
| `vendor/bin/castor test --filter=StorageDeadlineTest` | Select tests by name. |
| `vendor/bin/castor test:sqlite` | Queue engine and storage tests. |
| `vendor/bin/castor test:broker` | Protocol, client, and broker tests. |
| `vendor/bin/castor test:bench` | Benchmark correctness tests, not performance measurements. |

Use the same environment prefix for focused test commands. `vendor/bin/castor test:flex` separately installs the package into a no-dev Flex application; it needs network access and is not part of ordinary QA.

## Read reports

Each task prints one JSON status line and saves diagnostics under `var/qa/<task>/`. Colons become hyphens: `cs:check` writes to `var/qa/cs-check/`. Start with `result.json` and `stderr.log`; other files include tool-native JSON, stdout logs, and PHPUnit's `junit.xml` when generated. `var/qa/result.json` is the aggregate QA result.

A `running` status or missing report is not a pass. Each task replaces its previous reports, so preserve evidence you need before rerunning it. Do not run the same task concurrently in one checkout.

Benchmark captures have a separate lifecycle and accumulate under `var/bench/`. See [the benchmark guide](../bench/README.md) for measurement requirements and [capture storage](../bench/README.md#manage-capture-storage) for safe cleanup. Do not use a blanket deletion of `var/` as a development step.
