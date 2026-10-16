# Operate the queue

This guide covers ownership, supervision, failure recovery and offline backup. For the first working connection, use [the PHP client guide](broker.md) or [the Symfony setup guide](messenger-setup.md). For delivery guarantees and limits, use [the contracts](contracts.md).

## Process ownership and supervision

One broker owns one database and endpoint for its lifetime. The asynchronous SQLite driver owns a child process that executes SQL. Publishers and consumers are separate application processes; a transport connection does not launch or supervise a broker.

Run the foreground broker under your process supervisor with explicit database and endpoint paths. For example, the managed command can be:

```sh
/path/to/application/vendor/bin/sqlite-queue broker \
    --database=/srv/myapp/queue/queue.sqlite \
    --endpoint=/srv/myapp/queue/queue.sock \
    --redeliver-timeout=60 \
    --synchronous=full
```

Create the private parent directory first. Choose NORMAL or FULL deliberately rather than copying the example's durability choice without considering your failure model.

Configure supervision to:

- Run the broker and clients as the same dedicated Unix user. Keep the broker in the foreground rather than backgrounding it inside the managed command.
- Restart the broker after a failure, with a bounded restart policy and diagnostics. Repeated startup failure needs investigation, not an endless rapid restart loop.
- Capture stdout and stderr. Gate application startup on the JSON `ready` event from the current broker process, not just a PID or an existing socket file. The package has no separate HTTP health endpoint or systemd notification protocol.
- Send SIGTERM or SIGINT for graceful stop. Give the supervisor an explicit forced-stop policy beyond the broker's shared five-second cleanup budget. Do not assume a worker time limit interrupts a running application handler.
- Clean up the broker's **whole owned process group or cgroup** after an abrupt exit. Stopping only the parent can leave its SQLite child alive. Apply equivalent owned-tree cleanup to application workers that spawn subprocesses. Verify cleanup before allowing a replacement to own the database.
- Restart affected client applications after broker failure once the replacement is ready. They do not automatically reconnect or replay failed operations.

Do not add an unconditional `rm` of the socket to startup. Startup intentionally refuses existing endpoints. A failed restart is safer than deleting a socket still owned by a live listener.

For planned shutdown, stop admitting publications and let consumers finish and settle work while the broker remains available. Stop consumers before stopping the broker if you want them to retain their settlement connections. If handlers cannot finish, apply your application's cancellation and duplicate-effect policy rather than assuming broker shutdown stops those handlers.

### Paths and permissions

Use persistent local storage for the database, not the demonstration `/tmp` directory. The parent directories must exist, belong to the broker's effective Unix user, and deny group and other access. Use `0700` directories and `0600` files. Keep clients under the same user so they can reach the private socket.

Use a local filesystem that supports SQLite WAL. Do not place the live database on a network filesystem. The Unix socket and filesystem permissions are the access boundary; the protocol has no user authentication or per-queue access controls. Treat applications with socket access as trusted, especially when using PHP object serialization.

The canonical Unix socket path is limited to **100 bytes**. Canonicalization resolves the parent directory, so a short symlink spelling does not necessarily shorten the checked path. Keep the endpoint distinct from the database path. Use absolute paths in both commands and transport configuration.

Startup checks that an existing database is private, owned by the broker's user and singly linked; a symlink is not accepted as the database file. Keep SQLite companion files private as well. Do not loosen permissions to make another user connect. See [protocol ownership requirements](broker-protocol.md#ownership-and-readiness).

The lifetime locks use persistent sidecar files. **Never delete lock sidecars**, including during restarts and restores. Removing a lock file can create a second lock namespace while an existing owner still holds the old inode.

## Shutdown and deadlines

Normal broker stop closes client sockets immediately, then drains already-dispatched SQL within the shared five-second shutdown budget before closing storage. A healthy transaction can commit even though its client has disconnected and received no confirmation.

Storage failure or expiry of the ten-second storage-operation deadline is fatal. It stops service and starts driver close immediately, including during a graceful drain. Expiry does not prove that a write rolled back. Neither the broker nor adapter automatically retries it.

Repeated close calls await one actual driver cleanup. A later caller's cancellation can tighten the existing close to the original shutdown budget; it cannot restart close or grant a new budget. The broker retains its socket ownership and lifetime locks until storage cleanup completes. Use the supervisor for a hard process-tree termination deadline.

Normal stop removes the owned socket and preserves the database. A nonzero exit can indicate startup failure, observed storage failure or exhausted shutdown budget. An exit code of zero does not certify that the driver completed its graceful close protocol; it may have needed forced child termination internally. Capture diagnostics and supervise the whole process tree. See [driver cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

## Recover after broker or consumer failure

Start by separating unavailable communication from a rejected business operation. A missing send, claim, renewal or settlement confirmation is an **unknown outcome**: the transaction may have committed. Do not infer rollback or immediately replay the request.

### Broker failure

1. Stop or isolate affected publishers and consumers. Preserve logs and the application's record of unconfirmed work.
2. Verify that the failed broker and its owned SQLite child have exited. Investigate a remaining endpoint using the procedure below.
3. Correct the cause, such as storage exhaustion, permissions, an unavailable directory or a repeated storage deadline. Investigate a slow or blocked database before increasing operation budgets.
4. Start the replacement broker with the intended paths, reservation duration and synchronous mode. Wait for its `ready` event.
5. Restart affected application workers and publishers with new connections. Reconcile unconfirmed operations using application identifiers and business state before deciding whether to submit replacement work.

A new connection cannot use the old connection's receipts, and a new broker epoch cannot revive them. Persisted reservations become eligible according to their deadlines. Let the broker's reservation policy recover unsettled deliveries; do not invent SQL to clear receipts during restart.

### Consumer failure or ownership loss

A dead consumer stops renewing its lease. The message can be claimed again after the persisted reservation deadline, not necessarily as soon as the process exits. This also applies to a disconnected consumer that is still running application code.

Native Messenger workers should use an interval such as `--keepalive=5` with the default 60-second reservation. Renewal reduces accidental expiry during long work, but does not stop a handler after ownership loss. Protect external effects with idempotency keys, application uniqueness constraints or fencing appropriate to the effect. Receipt fencing protects queue mutations, not unrelated databases or services.

Use Messenger retry and failure routing for application handling failures. If operators need native failure inspection commands, choose a separate listable failure transport. The sqlite-queue adapter does not implement that inspection interface. See [failure transports](messenger.md#retry-and-failure-transports).

## Recover a stale endpoint

A failed connection attempt does not prove a socket is stale. The broker may still be starting, shutting down, or temporarily unable to accept clients. Startup refuses existing sockets, regular files and symlinks rather than removing them.

Before removing a leftover socket:

1. Stop automatic restarts and client connection attempts while investigating.
2. Verify through the supervisor and operating-system process inspection that the old broker and its whole owned process tree have exited.
3. Verify that no other listener owns the endpoint and that the path is the expected socket, not another user's file or a symlink. If ownership is uncertain, do not remove it; resolve the conflict or choose another private endpoint.
4. Remove only the confirmed stale socket. Leave the database, SQLite companions and lock sidecars alone.
5. Start the broker and wait for its new `ready` event before restarting clients.

Do not make this a blind pre-start deletion script. Do not remove locks to bypass an ownership failure.

## Watch capacity and storage health

Budget disk space for the main database, its `-wal` and `-shm` companions, and retained backups. Monitor free space, file growth, broker failures, application publication and completion rates, and the age of unfinished business work. A broker's continued readiness does not establish that consumers are keeping up.

A growing backlog needs consumer or application investigation: handler latency, errors, stalled downstream services, or insufficient processing capacity. The package does not provide a queue administration CLI, built-in backlog metrics or a purge API. Build application observability around stable message identifiers and completion state. If you inspect storage, use the documented [schema](queue-engine.md#schema-and-ordering) and avoid interfering with active transactions.

WAL growth can reflect sustained writes or readers holding snapshots. Avoid leaving ad hoc database readers open for long periods. Investigate persistent growth rather than deleting WAL files or changing checkpoint settings as an incident reflex. Do not run maintenance writes against a database the broker owns. Plan invasive maintenance during a controlled outage.

Decide whether NORMAL's power-loss risk is acceptable. It can lose a confirmed send, so it does not promise at-least-once delivery across an OS crash or power failure. It can also lose a confirmed settlement, leading to repeat handling. FULL is stronger when storage honors synchronization, but cannot make application effects exactly once. See [storage and durability](contracts.md#storage-and-durability).

## Back up offline

Use a controlled outage unless you have separately designed and tested an online SQLite backup process. A filesystem copy of only a **live** database file is not a safe backup: committed data can still reside in its WAL.

1. Pause publishers and stop new consumer work. Allow active handlers to finish and settle, or apply your application's explicit cancellation policy.
2. Stop publishers and consumers, then stop the broker gracefully. Verify that all of these processes and the broker's owned SQLite child have exited. Prevent supervisors from restarting them during the backup.
3. Preserve the database and any `-wal` and `-shm` companions together until a SQLite-consistent backup has been created. Do not delete a leftover WAL to make the directory look clean.
4. Use SQLite's backup facility to produce a consistent database snapshot. For example, if the separate `sqlite3` command-line utility is installed:

   ```sh
   install -d -m 700 /srv/myapp/queue-backups
   umask 077
   sqlite3 /srv/myapp/queue/queue.sqlite ".backup '/srv/myapp/queue-backups/queue.snapshot.sqlite'"
   sqlite3 /srv/myapp/queue-backups/queue.snapshot.sqlite 'PRAGMA integrity_check;'
   ```

   Choose a fresh backup filename for each capture rather than overwriting the only known-good copy. SQLite's backup operation reads the database consistently, including committed WAL content. The integrity check should return `ok`; it does not prove application-level completeness or that the backup meets your recovery objectives.
5. Keep the backup private and record its capture time and deployed application/serializer version. Resume the broker, wait for readiness, then restart clients.

The `sqlite3` utility in this example is an operator tool, not a package runtime dependency. SQLite's native backup API is another option. Do not substitute a live database-only file copy.

## Restore offline

Restoration can resurrect messages whose effects already happened after the backup was taken. Plan reconciliation and duplicate-effect protection before restarting consumers.

1. Stop publishers, consumers and the broker. Verify the whole owned process tree has exited and prevent automatic restarts.
2. Preserve the current database and its matching SQLite `-wal` and `-shm` companions as an incident copy in a separate private directory. Leave lifetime-lock sidecars in place. Do not mix an older restored database with the previous database's WAL.
3. Restore the verified snapshot to the configured database path. Restore ownership to the broker's Unix user, private parent permissions of `0700`, and file permissions of `0600`. Keep a copy of the backup rather than moving your only recovery image into service.
4. Check the endpoint separately. If a stale socket remains, follow [stale endpoint recovery](#recover-a-stale-endpoint); never delete it unconditionally.
5. Start the broker with the intended durability and reservation settings. Wait for `ready` and check its reported database and endpoint before admitting clients.
6. Restart clients with fresh connections. Reconcile business effects and resumed backlog; do not reuse old receipts or assume that a valid database snapshot represents exactly-once business execution.

Test this procedure on a separate private database and endpoint before depending on it for recovery. Include serializer compatibility and application reconciliation in the restore test, not just database integrity.
