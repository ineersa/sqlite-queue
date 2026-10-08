# SQLite worker

The broker keeps sockets, WAIT, and process supervision asynchronous in the parent process. One persistent child owns native PDO SQLite and runs complete queue operations. Amp starts that child and carries one request and one terminal response per operation.

## Process model

`SqliteWorkerContextFactory` starts exactly one package-owned worker. The child opens the database, configures WAL and the selected synchronous mode, prepares the fixed statements, and constructs queue policy with a fresh epoch.

This experimental parent proxy pipelines up to four operations through one FIFO writer and one independent response reader. The child still executes each operation and transaction sequentially. It never waits to fill a batch.

The in-flight payload budget is twice the maximum message payload. Total admission is limited to 128 operations and 64 maximum payloads, including queued work and retained completions. Sends reserve their actual payload size; claims reserve the maximum possible reply. Credits remain charged until the result is consumed and the sender releases its data.

Caller cancellation removes queued work without consuming a wire ID. After the channel send begins, the worker finishes the operation and the parent consumes its terminal result. Domain receipt failures affect only that request. Storage, IPC, timeout, and malformed-reply failures fail the lane and stop the broker. Several dispatched operations can have unknown outcomes after one channel failure. Known terminal results remain known. The broker does not replace the worker or replay operations.

Readiness reports the worker PID as `persistence_pid` and the effective synchronous mode from the owning child connection. The parent does not open a second SQLite connection for configuration evidence.

## Budgets

| Phase | Budget | Starts |
| --- | ---: | --- |
| Startup | 15 seconds | Before spawning the worker |
| Dispatched exchange | 10 seconds | Immediately before channel send |
| Shutdown | 5 seconds total | At the first stop request |

SQLite `busy_timeout` remains 5,000 milliseconds. Each dispatched request has its own exchange deadline, including time behind earlier requests in the child. Other replies do not extend it. A timeout fails the lane, terminates the worker, and releases waiting callers. It does not retract an already validated response.

Shutdown stops admission, removes queued work, and drains dispatched operations before sending one Close through the same writer and reader. It shares one budget across client drain, close, and child teardown. Graceful close joins the Amp process context once. Forced termination uses `ProcessContext::close()` and tolerates the expected missing-result failure. Amp owns process-exit tracking. Pipe EOF alone is not proof of reaping.

## Durability

| Mode | Meaning |
| --- | --- |
| NORMAL | Default. Preserves commits across application or process crashes. Recent commits can be lost after an OS crash or power failure. |
| FULL | Stronger commit durability across OS crashes and power loss when storage honors synchronization. |

Use `--synchronous=normal|full`, bundle `sqlite_queue.synchronous`, or `Sqlite\SqliteSynchronousMode`. NORMAL is not power-loss durability. Prefer FULL or a durable upstream outbox when a confirmed send must survive machine failure.

## Embedding

Applications should use the broker and client. Local `Queue` and `SqliteQueueStorage` remain package-owned policy and PDO storage for the worker. They are not a second public network API. Existing databases need no schema migration. Claim selects the payload and conditionally reserves that row in one immediate transaction; the worker does not use data-changing `RETURNING`.
