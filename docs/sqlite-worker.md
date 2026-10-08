# SQLite storage

The broker uses one asynchronous connection from the `ineersa/amp-sqlite3` fork. This package owns queue policy, SQL, and transaction boundaries. The driver owns SQLite execution, its child process, and internal IPC. There is no package-owned worker protocol, pipeline, or connection pool.

## Process model

`BrokerFactory` opens storage, verifies WAL and the selected synchronous mode, creates the schema, and constructs `Queue` with a fresh epoch. Queue and notifier share the factory's clock.

An Amp mutex serializes complete storage operations on the writing connection. Contention and driver calls suspend fibers without blocking the broker's event loop. Sockets, WAIT timers, and signals remain available while SQLite runs. Transactions do not contain handlers or client socket I/O.

Readiness reports `storage_execution: "fabpot"` and the effective synchronous mode. It omits `persistence_pid` because the package does not supervise the driver's child. Driver failures stop service when a storage operation observes them. The broker does not independently monitor idle child exits or replace failed connections.

## Cancellation and budgets

Startup cancellation reaches the driver's connection handshake. Subsequent schema queries have no package-wide startup deadline. SQLite `busy_timeout` remains 5,000 milliseconds; it is not an overall operation deadline.

Connection cancellation can prevent storage work before it begins. It does not revoke dispatched SQL. Missing confirmation remains an unknown outcome, even if shutdown later terminates the driver child. The package never replays an operation automatically.

Shutdown shares one five-second budget across client draining and driver close. Storage passes that cancellation to the driver's `SqliteCancellableConnection`. Driver close also has its own five-second ceiling. Expiry interrupts pending close writes, reads, or join and forces child termination through Amp. Cleanup removes the owned socket and releases locks afterward.

Use an external supervisor for a hard process-exit deadline. Configure it to clean up the entire process group after an abrupt broker exit.

## Durability

| Mode | Meaning |
| --- | --- |
| NORMAL | Default. Preserves commits across application or process crashes. Recent commits can be lost after an OS crash or power failure. |
| FULL | Stronger commit durability across OS crashes and power loss when storage honors synchronization. |

Use `--synchronous=normal|full`, bundle `sqlite_queue.synchronous`, or `Sqlite\SqliteSynchronousMode`. NORMAL is not power-loss durability. Prefer FULL or a durable upstream outbox when a confirmed send must survive machine failure.

## Embedding

Applications use the same broker command, clients, DSNs, and frames. Existing databases need no schema migration. Receive selects the ID and payload, conditionally reserves the row, and commits in one immediate transaction. It does not use data-changing `RETURNING`.
