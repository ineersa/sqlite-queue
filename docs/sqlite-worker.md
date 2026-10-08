# SQLite worker

The broker keeps sockets, WAIT, and process supervision asynchronous in the parent process. One persistent child owns native PDO SQLite and runs complete queue operations. Amp starts that child and carries one request and one terminal response per operation.

## Process model

`SqliteWorkerContextFactory` starts exactly one package-owned worker. The child opens the database, configures WAL and the selected synchronous mode, prepares the fixed statements, and constructs queue policy with a fresh epoch.

The parent proxy admits at most one in-flight exchange. Caller cancellation can stop work before dispatch. After the channel send begins, the worker finishes the operation and the parent consumes the terminal result. Domain receipt failures remain ordinary responses. Storage, IPC, timeout, and malformed-reply failures fail the storage lane and stop the broker. The broker does not replace the worker behind live sessions.

Readiness reports the worker PID as `persistence_pid` and the effective synchronous mode from the owning child connection. The parent does not open a second SQLite connection for configuration evidence.

## Budgets

| Phase | Budget | Starts |
| --- | ---: | --- |
| Startup | 15 seconds | Before spawning the worker |
| Dispatched exchange | 10 seconds | Immediately before channel send |
| Shutdown | 5 seconds total | At the first stop request |

SQLite `busy_timeout` remains 5,000 milliseconds. An exchange timeout marks the outcome unknown, fails the lane, terminates the worker, and releases waiting callers. The broker does not reuse the channel or replay the operation.

Shutdown shares one budget across client drain, the close exchange, and child teardown. Graceful close joins the Amp process context once. Forced termination uses `ProcessContext::close()` and tolerates the expected missing-result failure. Amp owns process-exit tracking. Pipe EOF alone is not proof of reaping.

## Durability

| Mode | Meaning |
| --- | --- |
| NORMAL | Default. Preserves commits across application or process crashes. Recent commits can be lost after an OS crash or power failure. |
| FULL | Stronger commit durability across OS crashes and power loss when storage honors synchronization. |

Use `--synchronous=normal|full`, bundle `sqlite_queue.synchronous`, or `Sqlite\SqliteSynchronousMode`. NORMAL is not power-loss durability. Prefer FULL or a durable upstream outbox when a confirmed send must survive machine failure.

## Embedding

Applications should use the broker and client. Local `Queue` and `SqliteQueueStorage` remain package-owned policy and PDO storage for the worker. They are not a second public network API. Existing databases need no schema migration. Claim still uses select, conditional update, and payload read in one immediate transaction; the worker does not use data-changing `RETURNING`.
