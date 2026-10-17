# SQLite storage architecture

The broker uses one asynchronous connection from the `ineersa/amp-sqlite3` fork, installed as `fabpot/amphp-sqlite3`. The driver owns SQLite execution in a child process and the IPC that carries its requests. The package owns the queue's SQL and policy.

## Process model

| Component | Responsibility |
| --- | --- |
| `BrokerFactory` | Acquires lifetime locks, prepares the database, initializes storage, and binds the socket before handing over a ready broker. Failed startup cleans up acquired resources. Accepts an optional `Fabpot\Amp\Sqlite\SqliteConnector`; the default keeps the driver's process isolation. |
| `Broker` | Owns client sessions, admissions, storage deadlines, and shutdown. |
| `QueueNotifier` | Coordinates WAIT/WAIT_ANY and rechecks watched queues against persisted eligibility. |
| `Queue` | Validates message policy, creates the epoch and delivery tokens, and interprets receipts. |
| `SqliteQueueStorage` | Owns one driver connection and exclusive operation executor; initializes the schema and validates operation results. `open()` accepts an optional connector for the same reason. |
| `QueueWorkerOperation` | Runs queue SQL and samples mutation time inside worker-owned transactions. |
| Driver | Executes SQLite calls, serializes complete operations, carries results over IPC, and closes or terminates its child. |

Each driver request performs a complete insert, claim, settlement, renewal, or eligibility lookup. Mutations use immediate transactions; eligibility is read-only. Worker operations return explicit commit, rollback, or read-only outcomes, which storage checks before returning data. The executor exclusively owns the connection and runs operations serially. There is no package operation pipeline or connection pool.

Waiting for driver ownership or a result suspends a fiber rather than running blocking SQLite calls on the broker's event loop. Sockets, notifier timers, and signal callbacks can continue while SQLite executes. Transactions do not contain handlers, client socket I/O, or waits for future messages.

Mutation clocks are sampled in the worker after transaction acquisition. The optional `BrokerFactory` clock controls notifier scheduling, not mutation timestamps. Restart leaves persisted deadlines unchanged and creates a fresh Queue epoch.

Readiness reports `storage_execution: "fabpot"` and the effective synchronous mode. The broker does not independently monitor idle child exits or replace failed connections; an observed driver failure during storage work stops service. Readiness does not expose a driver-child PID.

## Connection configuration

| Setting | Standard storage configuration |
| --- | --- |
| Database | File-backed; in-memory storage is rejected. |
| SQLite version | At least 3.31.0, checked during initialization. |
| Journal mode | WAL, verified by readback. |
| Synchronous mode | NORMAL by default, or FULL; verified against the requested mode. |
| SQLite busy timeout | 5,000 milliseconds. |
| WAL autocheckpoint | 1,000 pages. |
| Write transaction mode | Immediate. |

The public broker configuration selects synchronous mode with `--synchronous=normal|full` or bundle `sqlite_queue.synchronous`. PHP callers use `SqliteSynchronousMode`.

The advanced [engine API](queue-engine.md#opening-and-ownership) can take exclusive ownership of an already configured driver connection. It does not grant concurrent access to that connection or make independent database access safe while a broker owns the file.

## Cancellation and budgets

These bounds have different owners. Neither a long handler nor a long reservation requires increasing every timeout.

| Bound | Default or limit | What it covers |
| --- | --- | --- |
| Client connect and exchange allowance | 10 seconds | Connection establishment and each exchange. WAIT/WAIT_ANY add the requested wait duration to the exchange allowance. Configurable through `Client::connect()`'s positive, finite `timeout`; there is no 30-second client-setting cap. |
| Broker handshake read | 5 seconds | The initial HELLO frame. |
| Established-session idle time | Unbounded | Waiting for the first bytes of the next request does not expire the owning connection. |
| Established partial-frame read | 30 seconds | Starts after the first one through four outer-prefix bytes arrive; covers the remaining prefix, control, and payload. |
| Broker response write | 5 seconds | A normal or best-effort error frame write. |
| Public WAIT/WAIT_ANY | 0 through 30,000 milliseconds | Readiness waiting, not reservation or handler execution. |
| Native Messenger idle wait | Up to 1,000 milliseconds | Each adapter idle wait or bounded fallback. |
| SQLite busy timeout | 5,000 milliseconds | SQLite lock contention, not a whole-operation deadline. |
| Broker storage startup | 10 seconds overall | One deadline for connection acquisition, configuration readback, and schema initialization. |
| Broker storage operation | 10 seconds per operation | SEND, RECEIVE, ACK, REJECT, KEEPALIVE, and notifier eligibility lookups, including serial ownership waiting and worker reply. |
| Broker shutdown | 5 seconds shared | Starts at the first stop request; covers graceful draining and driver close. |
| Driver close ceiling | 5 seconds | Driver cleanup, additionally constrained by the supplied cancellation. It does not renew the broker's budget. |

`BrokerFactory::storageOperationTimeoutSeconds` configures both the startup storage deadline and individual storage-operation deadlines. It must be positive and finite. There is no corresponding broker CLI option. Lock acquisition and socket binding are outside the startup **storage** timer.

WAIT/WAIT_ANY durations, established-session idle time, and handler execution are excluded from storage-operation deadlines. Eligibility queries made for a wait are included. Each lookup has its own storage deadline; the wait as a whole is not a ten-second storage call. Direct `Queue` and `SqliteQueueStorage` use do not install the broker's overall deadlines.

### Shutdown and escalation

Normal stop closes admissions and client sockets, then lets already-dispatched storage work drain within the original shared budget. Closing a socket does not itself cancel dispatched SQL. After draining, storage closes before endpoint removal and lifetime-lock release.

A storage failure or operation-deadline expiry is fatal: it starts storage close immediately, even during an existing graceful drain. Exhausting the shared shutdown budget also forces that escalation. The driver prevents queued callers from starting new work on the failed connection and terminates an unresponsive child. No separate error-frame write delays fatal storage shutdown.

Storage distinguishes close-started from close-completed. All close callers await one completion result. Cancellation from a later caller reaches the existing driver close and can tighten its budget, including a close already started while validating an invalid application reply. It cannot restart close, extend the deadline, or abandon cleanup. The broker retains its endpoint and lifetime locks until actual storage cleanup completes.

Driver close can force termination after a close-protocol failure without reporting that failure to the broker. Exit zero therefore does not certify that every driver shutdown was graceful. Use an external supervisor for a hard process-exit deadline and cleanup of the process group after an abrupt broker exit.

For cancellation, mutation uncertainty, and explicit recovery, use the shared [uncertain-operation contract](contracts.md#uncertain-operations-and-cancellation).

## Durability

The configured synchronous mode applies to writes for every queue on the connection. See [storage and durability](contracts.md#storage-and-durability) for NORMAL and FULL guarantees across process crashes and power loss, and for their limits on delivery guarantees.
