# SQLite driver behavior

The queue engine uses `fabpot/amphp-sqlite3`. This reference describes the driver behavior that affects embedding, transaction use, and durability.

These observations came from real file databases with driver v1.0.0, revision `1ee168273e29af037a5c4576349ff89e3a3b5fd`, and SQLite 3.45.1. They describe that driver version, not a guarantee for arbitrary future versions.

## Compatibility

PHP 8.5.10 was used for the supported driver checks. PHP 8.4.25 also passed earlier checks, but is outside this package's `^8.5` range. PHP 8.6 resolved dependencies but was not tested in those checks. The package supports Symfony `^8.0`, not Symfony 7.x.

The observations used Amp 3.1.3, Amp Parallel 2.4.0, and Revolt 1.0.9. Process-tree observations used Linux `/proc`. They do not establish equivalent process cleanup on every operating system.

## Process model

A connection starts one SQLite worker process using the parent's `PHP_BINARY`. The worker inherits the parent's environment and INI configuration, including enabled debug extensions. Amp may start a shell launcher around the worker.

`close()` sends a close request, waits for the worker, and closes its process context. It is idempotent. Closing during a transaction discards the uncommitted write. A dead worker causes operations to fail with `SqliteConnectionException`, and the connection can still be closed.

The broker stops service when its worker dies. It does not silently replace the connection. Its shutdown can escalate to terminating the worker; see [broker shutdown](broker.md#stop-or-restart) for supervisor requirements.

## Durability settings

| Connection configuration | Journal mode | Synchronous mode |
| --- | --- | --- |
| Explicit `SqliteJournalMode::Wal` and `SqliteSynchronousMode::Full` | `wal` | `2`, FULL |
| File database with default `Automatic` | `wal` | `1`, NORMAL |
| `:memory:` with default `Automatic` | `memory` | `2`, FULL |

The queue engine sets FULL explicitly. NORMAL can lose the most recent committed transaction after power loss. FULL durability remains subject to SQLite, filesystem, and host guarantees. Reopen and process-crash checks do not simulate power loss or prove behavior on filesystems without WAL support.

`SqliteConfig::withPragma()` rejects settings with dedicated options, including `journal_mode`, `synchronous`, `busy_timeout`, `foreign_keys`, and `trusted_schema`. Use the corresponding configuration methods.

`PRAGMA wal_checkpoint(TRUNCATE)` returns `busy`, `log`, and `checkpointed` counts. The observed checkpoint completed with `busy = 0`.

## Transactions and concurrent operations

`commit()` persists changes across close and reopen. `rollback()` discards uncommitted changes. A nested `beginTransaction()` creates a savepoint; rolling it back leaves the outer transaction active.

`beginTransaction()` holds the connection mutex until the transaction ends. A second operation on that connection waits. A connection-level read from the same fiber while its transaction is open deadlocks; use the transaction object's read methods instead.

A reader on a second WAL connection sees only committed rows and can read while another connection holds an open write transaction.

The broker uses one writable connection. Holding its transaction while waiting for a client or future message would block other storage work, so handlers and socket waits remain outside transactions.

## Data-changing statements with `RETURNING`

The driver rejects `INSERT ... RETURNING` and `UPDATE ... RETURNING` with:

```text
Row-producing DML statements are not supported by the PHP SQLite3 extension
```

The engine therefore claims a message within one immediate transaction:

1. Select an eligible message ID.
2. Conditionally update that row's reservation.
3. Check the affected-row count. Roll back if it is zero.
4. Read the payload through the transaction object.
5. Commit before returning the delivery.

Parameterized reads use `execute()`. Direct `query()` rejects parameters. `SqliteResult::getRowCount()` reports the affected rows for the conditional update.

These restrictions explain the engine's claim implementation. They do not establish its throughput under load. See the [benchmark method](benchmark-method.md) and [baseline results](benchmark-baseline.md) for measured workloads and limitations.
