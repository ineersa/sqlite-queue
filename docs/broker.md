# Run the broker and use the client

The broker supports immediate receive and bounded WAIT. The Symfony Messenger adapter remains Task 06 work.

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

## Run under a supervisor in production

1. Run the foreground command under systemd, supervisord, Docker, PM2, or another external supervisor. Do not daemonize it or put it in the background inside the managed command.
2. Configure restart after failure and wait for the `ready` event before admitting clients. The broker exits nonzero on fatal storage or worker failure instead of replacing SQLite connections inside the running process.
3. Send SIGTERM for graceful shutdown. Allow more than the broker's five-second cleanup budget before forced termination.
4. Configure the supervisor to clean up the whole process tree, including the SQLite worker and any Amp shell launcher, before starting a replacement broker.
5. Make client applications reconnect explicitly after failure. Do not replay unconfirmed operations automatically or reuse receipts from an old connection.

After an abrupt exit, follow the endpoint verification instructions in [Stop or restart](#stop-or-restart). Do not add an unconditional socket deletion to the supervisor's startup command.

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

Pass `delay: 1500` to `send()` to make a message unavailable for 1,500 milliseconds. `receive()` still returns null immediately when no message is eligible. It never waits.

## Wait for readiness without busy polling

Use `Client::wait(string $queue, int $timeoutMilliseconds, ?Cancellation $cancellation = null): bool` after an empty receive. Bounds are `0` through `30_000` milliseconds. Zero is an immediate readiness probe. Out-of-range local bounds raise `InvalidArgumentException` before any bytes are written, so the request sequence is unchanged. The exchange deadline is the requested wait plus the client's normal transport allowance. One outstanding exchange still applies.

`true` means try `receive()` again. It never grants a reservation. `false` means the bound elapsed without a readiness hint, including an empty zero-duration probe. Do not sleep after a hint. Cancellation during WAIT closes the connection, reports an uncertain outcome, and never replays.

```php
$delivery = $client->receive('jobs');
if (null === $delivery) {
	if (!$client->wait('jobs', 5_000)) {
		// Bound elapsed with no readiness hint. Decide whether to wait again.
		return;
	}
	$delivery = $client->receive('jobs');
}
if (null !== $delivery) {
	$client->acknowledge($delivery->receipt);
}
```

The default visibility timeout is 5,000 milliseconds. An unacknowledged reservation becomes eligible again at its persisted deadline. Closing a client does not shorten that deadline. For custom visibility, construct a `BrokerFactory` with that timeout and call `create()` to receive a fully initialized `Broker`. For an application-owned cancellation token, pass it to `Broker::run()` rather than adding a supervisor to this package.

Task 06 must map this wait API onto Symfony Messenger worker idle handling so a readiness hint is not followed by an unnecessary remaining sleep, while still respecting cancellation, shutdown, and worker limits. That adapter is not implemented here.

## Recover from a failed exchange

Catch `Exception\TransportException` for connection failures and `Protocol\ProtocolException` for malformed broker responses. Both invalidate the client after an exchange starts. The protocol exception identifies the violated field or constraint. Decide how to handle the uncertain operation before creating a replacement with `Client::connect()`. Do not automatically resend it. The broker may have committed before the connection failed.

Client methods accept an optional Amp `Cancellation`. Cancellation during an exchange also closes the connection and reports an uncertain outcome. A new client cannot settle receipts from the old connection. Concurrent calls on one client are rejected; use separate clients for concurrent operations.

## Stop or restart

Send SIGTERM or SIGINT, or press Ctrl-C in the foreground terminal. Wait for process exit before restarting. Normal stop removes the owned socket but preserves the database. Symfony FlockStore lock sidecars stay on disk under the private resource directories, which preserves the lock namespace.

The command registers a one-second event-loop timer to prevent an indefinite select wait when a signal arrives between the driver's signal check and its select call. This timer does not poll storage.

The five-second budget is armed on the first shutdown request, inside `stop()` and before the server or any client socket closes, so the deadline covers every step that follows. Repeated requests, including the serving loop's own, share that one deadline and never re-arm it. The deadline is disarmed once every awaited step has returned; a shutdown that finishes inside the budget never fires it.

Shutdown shares one five-second budget across the client drain, storage close, SQLite worker pipe observation, and child monitor. No step starts a fresh clock. `SqliteQueueStorage` owns the connection; `Queue` supplies message policy without SQL or connection lifecycle code. The broker closes storage during shutdown.

When the budget expires, the broker terminates the worker through `SqliteWorkerHandle`. Repeating the driver's graceful close cannot interrupt it because the driver marks the connection closed before awaiting completion. Vendor `SqliteConnector` calls `SqliteWorkerContextFactory`, which retains a handle to the same worker Amp starts. The driver alone joins that context. This workaround remains until the driver offers bounded close or abort support.

Steps that take no cancellation, such as storage close, run in their own fiber and are awaited with the same budget. A step abandoned at the deadline can finish or fail later; its outcome is ignored. The broker attempts every cleanup step and preserves the first failure. After worker shutdown, it removes the endpoint only if its device and inode still match, then releases `BrokerLifetimeLocks`. The lock component does not prepare files or remove sockets.

The budget bounds what the broker waits for, not the exact wall-clock duration of the exit. Signal delivery, process teardown, and reaping are scheduled by the operating system. The broker owns the persistence child it starts, not the shell launcher Amp creates around it: a launcher that is stopped rather than killed is left for its supervisor to reap.

Worker pipe reads are cancellable, so an expired budget releases their event-loop watchers. A pending driver close can remain until an external launcher resumes or exits. Normal SIGTERM shutdown and SIGKILL worker failure/recovery have process coverage. Shutdown with a deliberately SIGSTOPped worker is excluded from the release requirements by explicit scope decision. Its historical intermittent hang was not diagnosed or fixed by removing that test.

If an endpoint remains after an abrupt exit, do not remove it merely because a connection attempt fails. Verify that no broker or other listener owns it before removing it, or choose a new endpoint. Startup refuses existing sockets, regular files, and symlinks.

See the [protocol reference](broker-protocol.md) for exact framing, deadlines, errors, and ownership requirements. See [AGENTS.md](../AGENTS.md) for validation commands and report locations.
