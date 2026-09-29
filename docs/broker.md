# Run the broker and use the client

Task 04 provides immediate receive. Notification waiting and the Messenger adapter are not implemented yet.

## Start the foreground broker

1. Run `composer install` in this checkout. Check that CLI PHP has `sqlite3`, `pcntl`, and `posix` enabled.
2. Create private storage and socket directories. Keep the absolute socket path under 101 bytes.

   ```sh
   install -d -m 700 /tmp/sqlite-queue-demo
   ```

3. Start the broker in the foreground.

   ```sh
   php bin/sqlite-queue broker --database=/tmp/sqlite-queue-demo/queue.sqlite --endpoint=/tmp/sqlite-queue-demo/queue.sock
   ```

4. Wait for a JSON line whose `event` is `ready` before starting clients. Process creation alone is not readiness.

The executable uses Symfony Console `^8.0`. Development installation includes it. In a consuming project without development dependencies, install `symfony/console:^8.0` to use `vendor/bin/sqlite-queue`. The broker requires `symfony/lock:^8.0` and `symfony/filesystem:^8.0` at runtime for exclusive ownership. The `Queue` and `Client` APIs need no Symfony packages, and no Symfony application boot is involved anywhere.

## Send and settle a message

Run this code in a separate PHP process with the package autoloader:

```php
<?php

require 'vendor/autoload.php';

use Ineersa\SqliteQueue\Client;

$client = Client::connect('/tmp/sqlite-queue-demo/queue.sock');
try {
	$id = $client->send('jobs', "body\0bytes", "opaque headers\xff");
	$delivery = $client->receive('jobs');
	if (null !== $delivery) {
		// Process the message here, outside the broker.
		$client->acknowledge($delivery->receipt);
	}
} finally {
	$client->close();
}
```

Use `reject($delivery->receipt)` for terminal disposal instead of acknowledgment. Use the same client connection that received the delivery. Neither operation schedules retries.

Pass `delay: 1500` to `send()` to make a message unavailable for 1,500 milliseconds. `receive()` returns null immediately when no message is eligible. Do not build a busy polling loop as a substitute for Task 05 notifications.

The default visibility timeout is 5,000 milliseconds. An unacknowledged reservation becomes eligible again at its persisted deadline. Closing a client does not shorten that deadline. For custom visibility, construct a `BrokerFactory` with that timeout and call `listen()` to receive a fully initialized `Broker`. For an application-owned cancellation token, pass it to `Broker::run()` rather than adding a supervisor to this package.

## Recover from a failed exchange

Catch `TransportException` at the application boundary. Discard that client and decide how to handle the uncertain operation before creating a replacement with `Client::connect()`. Do not automatically resend it: the broker may have committed before the connection failed.

Client methods accept an optional Amp `Cancellation`. Cancellation during an exchange also closes the connection and reports an uncertain outcome. A new client cannot settle receipts from the old connection. Concurrent calls on one client are rejected; use separate clients for concurrent operations.

## Stop or restart

Send SIGTERM or SIGINT, or press Ctrl-C in the foreground terminal. Wait for process exit before restarting. Normal stop removes the owned socket but preserves the database. Symfony FlockStore lock sidecars stay on disk under the private resource directories, which preserves the lock namespace.

The command registers a one-second event-loop timer to prevent an indefinite select wait when a signal arrives between the driver's signal check and its select call. This timer does not poll storage.

The five-second budget is armed on the first shutdown request, inside `stop()` and before the server or any client socket closes, so the deadline covers every step that follows. Repeated requests, including the serving loop's own, share that one deadline and never re-arm it. The deadline is disarmed once every awaited step has returned; a shutdown that finishes inside the budget never fires it.

Shutdown runs under one five-second budget shared by every step it waits for: the client drain, the queue close, the persistence pipe observation, and the child monitor. No step starts a fresh clock, so a stalled step cannot stretch the total. When the budget expires, the broker kills the owned persistence child directly through its process context instead of repeating the graceful connection close. That close cannot interrupt a worker that stopped answering, because the driver marks its connection closed before the close returns. The SQLite driver alone joins that context, so no second join can wedge the shutdown. Queue owns the SQLite connection; the broker does not close it separately.

Steps that take no cancellation, such as the engine close, run in their own fiber and are awaited with the same budget. A step abandoned at the deadline is left to finish or fail on its own, and its outcome is ignored. Ownership is released after the child is force-stopped, so no storage writer survives to hold the database. Every shutdown step still runs after an earlier failure, and the first failure is what the caller receives, with a nonzero exit.

The budget bounds what the broker waits for, not the exact wall-clock duration of the exit. Signal delivery, process teardown, and reaping are scheduled by the operating system. The broker owns the persistence child it starts, not the shell launcher Amp creates around it: a launcher that is stopped rather than killed is left for its supervisor to reap.

An embedded broker can retain pending close fibers and pipe watchers after the deadline if that launcher remains stopped. There is also an unresolved intermittent SIGTERM timeout in validation. Do not rely on bounded signal-driven exit until it is understood; see the Task 04 review record.

If an endpoint remains after an abrupt exit, do not remove it merely because a connection attempt fails. Verify that no broker or other listener owns it before removing it, or choose a new endpoint. Startup refuses existing sockets, regular files, and symlinks.

## Shutdown trace

Pass `--trace-file=PATH` to record non-payload shutdown milestones as JSON lines: `signal-dispatched`, `cancellation-requested`, `cancellation-delivered`, `shutdown-requested`, `deadline-armed`, `deadline-fired`, and `persistence-force-stop`, each with the process id and a monotonic timestamp captured when the event happens. The command creates a new exclusive regular file before it acquires any broker resource, and it writes the buffered lines only after cleanup and ownership release. Lines are ordered by those captured timestamps, because storage failure can start shutdown before a later signal. An existing path is refused without modifying it. A FIFO, a device, or an unwritable path fails the command before startup. An empty on-disk file during a hang no longer identifies the missing stage, because flushing is deferred. The optional write happens after resource cleanup and can itself delay process exit on a slow filesystem. A write failure never undoes completed shutdown work. Without the option the broker writes nothing.

See the [protocol reference](broker-protocol.md) for exact framing, deadlines, errors, and ownership requirements. See [AGENTS.md](../AGENTS.md) for validation commands and report locations.
