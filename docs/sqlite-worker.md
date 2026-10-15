# SQLite storage

The broker uses one asynchronous connection from the `ineersa/amp-sqlite3` fork. This package owns queue policy, SQL, and transaction boundaries. The driver owns SQLite execution, its child process, and internal IPC. There is no package-owned worker protocol, pipeline, or connection pool.

## Process model

`BrokerFactory` opens storage, verifies WAL and the selected synchronous mode, creates the schema, and constructs `Queue` with a fresh epoch. The notifier uses the factory's clock. Mutations sample wall-clock milliseconds inside the SQLite worker after transaction acquisition, using the operation's injected `QueueClock`.

The driver owns an exclusive serial operation executor on the writing connection. Each request runs a complete queue operation, including its transaction and outcome validation, in the worker. The broker has no outer storage mutex or operation pipeline. Contention and driver calls suspend fibers without blocking the broker's event loop. Sockets, WAIT timers, and signals remain available while SQLite runs. Transactions do not contain handlers or client socket I/O.

Readiness reports `storage_execution: "fabpot"` and the effective synchronous mode. It omits `persistence_pid` because the package does not supervise the driver's child. Driver failures stop service when a storage operation observes them. The broker does not independently monitor idle child exits or replace failed connections.

## Cancellation and budgets

`BrokerFactory` gives storage startup one 10-second deadline covering connection acquisition, configuration readback, and schema setup. Failure closes the acquired connection before releasing lifetime locks. The factory's `storageOperationTimeoutSeconds` argument changes this budget and the serving-operation budget; it must be positive and finite.

Each storage-backed SEND, RECEIVE, ACK, REJECT, KEEPALIVE, and notifier eligibility lookup has its own overall 10-second deadline. It starts before waiting for serial-executor ownership and covers the worker reply. WAIT and WAIT_ANY durations, idle sessions, and handler execution do not consume that budget. SQLite `busy_timeout` remains 5,000 milliseconds and is independent of this deadline and the client's communication timeout.

Expiry stops admissions, closes client sessions, and exits nonzero. The broker closes storage before draining sessions so stalled SQL cannot hold shutdown open. Closing the driver prevents queued callers from executing new work on the failed connection. A timed-out write may have committed; expiry neither proves rollback nor triggers replay. Earlier confirmed commits remain subject to the selected durability mode.

Connection cancellation can prevent storage work before it begins. It does not revoke dispatched SQL. Missing confirmation remains an unknown outcome, even if shutdown later terminates the driver child. The package never replays an operation automatically.

Shutdown shares one five-second budget across driver close and client draining, starting at the first stop request. Storage passes that cancellation to the driver's `SqliteCancellableConnection`. Driver close also has its own five-second ceiling. Expiry interrupts pending close writes, reads, or join and forces child termination through Amp. Cleanup removes the owned socket and releases locks afterward.

Use an external supervisor for a hard process-exit deadline. Configure it to clean up the entire process group after an abrupt broker exit.

## Durability

| Mode | Meaning |
| --- | --- |
| NORMAL | Default. Preserves commits across application or process crashes. Recent commits can be lost after an OS crash or power failure. |
| FULL | Stronger commit durability across OS crashes and power loss when storage honors synchronization. |

Use `--synchronous=normal|full`, bundle `sqlite_queue.synchronous`, or `Sqlite\SqliteSynchronousMode`. NORMAL is not power-loss durability. Prefer FULL or a durable upstream outbox when a confirmed send must survive machine failure.

## Embedding

Applications use the same broker command, clients, DSNs, and frames. Existing databases need no schema migration. Receive selects the ID and payload, conditionally reserves the row, and commits in one immediate transaction. It does not use data-changing `RETURNING`.
