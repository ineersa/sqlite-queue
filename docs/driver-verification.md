# Driver verification

Reference for the async SQLite driver behavior the queue engine depends on. Every result here
comes from `tests/Driver/` running against real file databases on this machine.

## Versions observed

| Component | Version | Source |
| --- | --- | --- |
| PHP | 8.4.25 and 8.5.10 | `php -v`, `php8.4 -v` |
| SQLite library | 3.45.1 | `SQLite3::version()` |
| `fabpot/amphp-sqlite3` | v1.0.0, revision `1ee168273e29af037a5c4576349ff89e3a3b5fd` | `composer show` |
| `amphp/amp` | v3.1.3 | resolved lock |
| `amphp/parallel` | v2.4.0 | resolved lock |
| `revolt/event-loop` | v1.0.9 | resolved lock |
| `phpunit/phpunit` | 13.3.5 | `vendor/bin/phpunit --version` |
| `symfony/messenger` | v8.1.7 by default, v7.4.19 under `^7.4` | composer resolution probe |

## Commands

Run the suite on the supported PHP version:

```bash
composer qa                                 # PHP 8.5.10 -> OK (18 tests, 64 assertions)
php vendor/bin/phpunit --no-coverage        # same suite
```

PHP 8.4.25 passed the same suite while the constraint allowed it, before the supported range
was narrowed to 8.5. The committed lock now requires PHP >= 8.5, so the suite refuses to start
there: `Composer detected issues in your platform: Your Composer dependencies require a PHP
version ">= 8.5.0"`.

Probe dependency resolution for a target PHP version by overriding the platform:

```bash
composer update --dry-run   # with config.platform.php set to the target version
```

## Process model

The driver starts one process per connection:

```text
sh -c { '/usr/bin/php8.5' '-dhtml_errors=0' ... '<vendor>/amphp/parallel/src/Context/Internal/process-runner.php'
        'unix:///tmp/amp-parallel-ipc-<id>.sock' '64' '5'
        '<vendor>/fabpot/amphp-sqlite3/src/Internal/worker.php' } ... ; wait $pid
```

`process-runner.php` retitles the running process to `amp-process`, so the worker's command
line no longer contains the script path. Tests find it by walking `/proc` from the test
process, not by script name.

Facts the queue engine relies on:

- A connection runs in a separate OS process. It uses the parent's `PHP_BINARY`, and it
  inherits the parent's INI settings (`-dxdebug.mode=develop` and the other flags the parent
  had). The default async client topology is one parent plus one child per connection.
- `close()` sends a `close` request, waits for the child to exit, and closes the context.
  No launcher or worker process survives, including after `close()` during an open
  transaction.
- If the worker dies, an operation fails with
  `Fabpot\Amp\Sqlite\SqliteConnectionException: The SQLite child process stopped unexpectedly`.
  `close()` on that connection stays safe.

Proven by `BootstrapProcessTest`.

The leak check reads `/proc`, so the evidence exists only on Linux. `DriverTestCase` skips the
driver suite before it creates any fixture when `/proc/self` is unreadable, instead of
reporting a pass that never observed a process.

## Durability settings

The driver sets journal and synchronous mode from the connection configuration. Read back on
the live database:

| Configuration | `PRAGMA journal_mode` | `PRAGMA synchronous` |
| --- | --- | --- |
| Explicit `SqliteJournalMode::Wal` + `SqliteSynchronousMode::Full` | `wal` | `2` (FULL) |
| File database, default `Automatic` | `wal` | `1` (NORMAL) |
| `:memory:`, default `Automatic` | `memory` | `2` (FULL) |

`SqliteConfig::withPragma('journal_mode', ...)` throws `InvalidArgumentException: Pragma
'journal_mode' has a dedicated configuration option`. The same guard covers `synchronous`,
`busy_timeout`, `foreign_keys`, and `trusted_schema`.

The queue engine must pass `SqliteSynchronousMode::Full` explicitly. The default for a file
database is NORMAL, which survives a process crash but can lose the most recent commit after
a power loss. `PRAGMA wal_checkpoint(TRUNCATE)` returns `busy`, `log`, and `checkpointed`
counts and worked here with `busy = 0`.

Proven by `DurabilitySettingsTest`, one case per table row. The `:memory:` row comes from
`testMemoryDatabaseUsesMemoryJournalAndFullSynchronous`. The reopen check is
`testCommittedWriteSurvivesCloseAndReopenWithExplicitWalAndFull`; it proves a committed write
survives close and reopen, and does not simulate power loss.

## Transactions

- `commit()` persists changes that survive `close()` and a reopen with a new connection.
- `rollback()` discards the uncommitted changes.
- A nested `beginTransaction()` creates a savepoint. Rolling back the nested transaction keeps
  the outer write, and the outer `commit()` persists it.
- `close()` is idempotent and force-closes an active transaction. The open write is discarded,
  so a reopened connection never sees a half-finished delivery.

Proven by `TransactionOutcomeTest`.

## Serializing complete operations

`beginTransaction()` holds the connection's mutex until the transaction ends. Consequences:

- A second operation on the same connection waits for the open transaction and never observes
  uncommitted rows. A test that issues a connection-level read while a transaction is open on
  the same fiber deadlocks. Read inside a transaction through the `SqliteTransaction` object.
- A reader on a second connection to the same WAL database sees only committed rows and is not
  blocked by the open writer transaction.

The broker keeps one writable connection, so it must hold a transaction only around storage
work. Waiting for a client, a socket write, or a future message while a transaction is open
stalls every other storage operation on that connection.

Proven by `OperationSerializationTest`.

## Missing `RETURNING` on data-changing statements

The driver rejects data-changing statements that return rows:

```text
Fabpot\Amp\Sqlite\SqliteQueryError: Row-producing DML statements are not supported by the PHP SQLite3 extension
```

This applies to `INSERT ... RETURNING` and `UPDATE ... RETURNING`. The queue engine cannot
claim a message with a single `UPDATE ... WHERE id = (SELECT ...) RETURNING ...` statement.

The supported replacement is a transactionally protected claim. The test proves this
sequence against real storage:

1. `beginTransaction()` with `SqliteTransactionMode::Immediate`.
2. Parameterized `SELECT id, body ... WHERE queue = ? AND available_at <= ? AND
   reservation_token IS NULL ORDER BY id LIMIT 1`. Parameters require `execute()`;
   `query()` rejects them with `SqliteQueryError: Parameters are not allowed in direct queries`.
3. Conditional `UPDATE message SET reservation_token = ?, reserved_at = ? WHERE id = ? AND
   reservation_token IS NULL`.
4. `SqliteResult::getRowCount()` returns `1` on a won claim and `0` when another claim already
   reserved the row. The transaction rolls back on `0`.
5. Read the claimed row back inside the same transaction, then `commit()`.

The zero-row-count result is the lost-claim branch. `ClaimTransactionTest` drives it directly:
a test-only seam writes a competing reservation between the select and the conditional update,
so `getRowCount()` returns `0`, the transaction rolls back, and the reservation fence is left
untouched. The seam runs only in the test; production code passes no callback.

Proven by `ClaimTransactionTest`.

## Support range

Observed compatibility:

| Target | Result |
| --- | --- |
| PHP 8.3.30 | Rejected. `fabpot/amphp-sqlite3` requires PHP >= 8.4. |
| PHP 8.4.25 | Passed the suite while the constraint allowed it. The committed lock requires PHP >= 8.5, so the suite does not run there. |
| PHP 8.5.10 | Resolves and the suite passes. |
| `symfony/messenger` `^7.4` | Resolves to v7.4.19 on both PHP versions. |
| `symfony/messenger` `^8.0` | Resolves to v8.1.7 on both PHP versions. |
| PHP 8.6.0 | Resolves, not tested. |

Supported range, approved:

- `"php": "^8.5"`. PHP 8.5 is the supported target. PHP 8.4 passed the suite while the
  constraint allowed it but is outside the range, and PHP 8.6 is unverified.
- `"symfony/messenger": "^8.0"` in `require-dev`, declared in `suggest`, and moved to an
  optional runtime requirement when the adapter lands.

Confirm the range on the local platform:

```bash
composer config platform.php 8.4.25 && composer update --dry-run   # rejected: root requires php ^8.5
composer config platform.php 8.5.10 && composer update --dry-run   # resolves
composer config --unset platform.php
```

The range comes from dependency resolution and the suite result, not from the driver's own
minimum version. See [contracts.md](contracts.md) for the full matrix.

## Not verified here

- Behavior under a real power loss or a filesystem without WAL support.
- Concurrency across processes with a shared database file under load. Task 02 owns the
  benchmark, Task 03 owns atomic claims under concurrent receivers.
- Whether the child process inheriting the parent's INI settings (including Xdebug) needs to
  be suppressed for broker startup time.
