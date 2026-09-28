# Task 01: package setup and delivery contracts

Status: DONE
Repository: `/home/ineersa/projects/sqlite-queue`
Dependencies: none
Read first: [PLAN.md](PLAN.md), especially sections 3–8, 11, and 13.

## Goal

Establish the smallest independent package foundation and settle the contracts needed by the queue engine. Do not turn unresolved product decisions into implementation defaults.

## Scope

- Inspect the current checkout before editing. Preserve `ineersa/sqlite-queue`, namespace `Ineersa\SqliteQueue\`, the MIT license, and unrelated workspace changes.
- Verify PHP, SQLite, extension, Amp/Revolt, async-driver, and Symfony compatibility. Establish a documented supported version range without speculative compatibility branches.
- Evaluate the actual selected `fabpot/amphp-sqlite3` version. Prove process startup, explicit durability settings, transaction behavior across async waits, and whether data-changing `RETURNING` is supported. Use isolated disposable databases.
- Establish package-local autoloading, development QA, tests, and the minimum direct dependencies. Keep benchmark-only Doctrine dependencies out of the broker's runtime requirements where possible.
- Record the public client/adapter boundary and minimal protocol/storage contract. Reuse existing library facilities instead of building replacements.
- Resolve the package-level policy gates in PLAN section 6: visibility/redelivery, ambiguous operations and reconnect, receipt validity after broker restart, and portable broker-failure behavior. Settle eligible-message ordering and durability promises as part of that contract.
- Inspect and pin reference revisions and retain required attribution when adapting code.

## Boundaries

The baseline direction is one async writable connection behind a Revolt broker. Do not fall back to synchronous Doctrine storage or introduce multiple drivers because compatibility work is inconvenient. Bring a genuine incompatibility back for a decision.

Portable failure/receipt semantics block the engine paths that depend on them. No downstream application integration or migration policy is part of package setup.

## Acceptance criteria

- [x] The package boots and its test/QA entry points work without a consuming application installed.
- [x] Required extensions and supported dependency versions are documented from observed compatibility evidence.
- [x] An isolated driver check opens, transacts, commits, closes, and reaps its persistence process without leaks.
- [x] A supported atomic-claim design is recorded, including serialization of complete operations across awaits and the `RETURNING` constraint.
- [x] Required public behavior is approved and recorded. Unresolved choices remain explicit blockers, not silently selected defaults.
- [x] Tests, examples, and scripts use explicit isolated database paths and connection settings rather than inheriting live-service configuration.
- [x] No queue implementation, fake broker adapter, deferred feature, or downstream application change is bundled into setup.

## Validation and handoff

Run the new package's focused setup/driver checks through its declared QA runner. Record exact commands, dependency revisions, observed limitations, and cleanup evidence. Avoid assertions that merely test class existence or repeat configuration values.

Update PLAN with the settled contracts and link to the resulting package documentation. The handoff must identify any remaining policy blocker by the later task it affects.

## Completion record

Delivered:

- Package skeleton: `composer.json` (PHP `^8.5`, `ext-sqlite3`, `fabpot/amphp-sqlite3 ^1.0`,
  `revolt/event-loop ^1.0`, `phpunit/phpunit ^13.2`, `symfony/messenger ^8.0` declared in
  `suggest`), `composer.lock`, `phpunit.xml.dist`, and the `qa` script.
- Driver evidence: `tests/Driver/` (six test classes, 18 tests, 64 assertions) with
  `tests/Support/IsolatedDatabase.php` and `tests/Support/ProcessTree.php`.
- Contracts: `docs/contracts.md`. Measurements and reproduction commands:
  `docs/driver-verification.md`. `README.md` records the support matrix and package status.
- PLAN updates: task index, section 1 status, section 5.1 ordering and durability, section 6
  decisions, section 13 pinned reference revisions.

Validation:

- `composer qa` on PHP 8.5.10: PASS, 18 tests, 64 assertions. The script validates
  `composer.json` first, then runs the suite.
- `composer update --no-interaction` after the constraint change: PASS, lock unchanged apart
  from the content hash; `symfony/messenger` resolves to v8.1.7.
- Platform probe: `config.platform.php` 8.4.25 rejected by the root `^8.5` requirement; 8.5.10
  and 8.6.0 resolve.
- Out of range but observed: PHP 8.4.25 passed the same suite while the constraint allowed it.
  The committed lock requires PHP >= 8.5, so the suite now refuses to start there.
- Post-run process audit: PASS; no `amp-process` survivors and no files left under
  `var/tests/`.
- Review follow-up: every driver test closes its connections in `finally`, and a
  `TransactionOutcomeTest` case proves `close()` is idempotent and force-closes an open
  transaction. The `/proc` leak proof skips the suite before creating fixtures when `/proc` is
  unreadable. `ClaimTransactionTest` drives the zero-row-count lost-claim rollback through a
  test-only seam. A `DurabilitySettingsTest` case covers the `:memory:` row of the documented
  durability table.

Remaining blockers by later task, none blocking setup:

- Concrete frame, connection, and buffer limits: Task 04. The contract names the categories
  and the fail-explicitly rule, not the numbers.
- Messenger adapter behavior, including decode failure and idle integration with the worker
  sleep: Task 06.
- Measured cost of synchronous FULL at equivalent durability: Task 02 for the baseline and
  Task 08 for the comparison. The durability policy stands until that evidence exists.
- `symfony/messenger` moves from development and suggestion to an optional runtime
  requirement when the adapter lands in Task 06.
