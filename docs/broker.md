# Run the broker and use the client

The broker supports immediate receive and bounded WAIT. The [Symfony Messenger adapter](messenger.md) integrates with native `messenger:consume`.

## Start the foreground broker

1. Run `composer install` in this checkout. Check that CLI PHP has `pdo_sqlite`, `pcntl`, and `posix` enabled.
2. Create private storage and socket directories. Keep the absolute socket path under 101 bytes.

   ```sh
   install -d -m 700 /tmp/sqlite-queue-demo
   ```

3. Start the broker in the foreground.

   ```sh
   php bin/sqlite-queue broker --database=/tmp/sqlite-queue-demo/queue.sqlite --endpoint=/tmp/sqlite-queue-demo/queue.sock
   ```

4. Wait for a JSON line whose `event` is `ready` before starting clients. Process creation alone is not readiness.

The executable uses the package's runtime Symfony Console dependency. A no-dev installation includes `vendor/bin/sqlite-queue`. With the bundle enabled, `php bin/console sqlite-queue:broker` accepts the same options. Standalone `Queue` and `Client` APIs do not boot a Symfony application.

## Run under a supervisor in production

1. Run the foreground command under systemd, supervisord, Docker, PM2, or another external supervisor. Do not daemonize it or put it in the background inside the managed command.
2. Configure restart after failure and wait for the `ready` event before admitting clients. The broker exits nonzero on fatal storage failure instead of replacing SQLite connections inside the running process.
3. Send SIGTERM for graceful shutdown. Set the supervisor's forced-termination deadline explicitly.
4. Verify broker process exit before starting a replacement. The driver owns its SQLite child; configure the supervisor to clean up the whole process group after an abrupt broker exit.
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

The default reservation lasts 60 seconds. Both broker commands accept `--redeliver-timeout=60` in positive integer seconds. Choose a duration that covers your handlers; there is no lease renewal. For the bundle command, an explicit CLI value overrides `sqlite_queue.redeliver_timeout`, which defaults to 60. The standalone command does not load bundle configuration.

The PHP `BrokerFactory::visibilityTimeout` argument uses milliseconds and defaults to 60,000. Client communication timeouts are separate and do not change reservation expiry. The default communication allowance is 10 seconds, plus the requested duration for WAIT.

An unacknowledged reservation becomes eligible again at its persisted deadline. Closing a client does not shorten that deadline. PHP callers can construct `BrokerFactory` with `visibilityTimeout` and call `create()`. For an application-owned cancellation token, pass it to `Broker::run()`.

The Messenger bundle maps WAIT onto native consumer idle events for one literal receiver when `--sleep` is omitted or zero. See [Messenger idle behavior and limits](messenger.md#idle-waits).

## Recover from a failed exchange

Catch `Exception\TransportException` for connection failures and `Protocol\ProtocolException` for malformed broker responses. Both invalidate the client after an exchange starts. The protocol exception identifies the violated field or constraint. Decide how to handle the uncertain operation before creating a replacement with `Client::connect()`. Do not automatically resend it. The broker may have committed before the connection failed.

Client methods accept an optional Amp `Cancellation`. Cancellation during an exchange also closes the connection and reports an uncertain outcome. A new client cannot settle receipts from the old connection. Concurrent calls on one client are rejected; use separate clients for concurrent operations.

## Stop or restart

Send SIGTERM or SIGINT, or press Ctrl-C in the foreground terminal. Wait for process exit before restarting. Normal stop removes the owned socket but preserves the database. Symfony FlockStore lock sidecars stay on disk under the private resource directories, which preserves the lock namespace.

Shutdown shares one five-second budget across client draining and driver close. The broker passes the remaining budget to the driver, which terminates an unresponsive child. Cleanup removes only the broker's own socket and then releases the database and endpoint locks.

SQLite's 5,000-millisecond busy timeout does not bound a whole operation. Waiting for SQL does not block socket or signal handling. Use an external supervisor for a hard process-exit deadline. See [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

If an endpoint remains after an abrupt exit, do not delete it just because a connection attempt fails. Verify that no broker or other listener owns it before removal, or choose another endpoint. Startup refuses existing sockets, regular files, and symlinks.

See the [protocol reference](broker-protocol.md) for framing, deadlines, errors, and ownership requirements.

## WAL synchronous mode

Both broker commands accept `--synchronous=normal|full`. Only these two modes are supported. The default is `normal`. For the bundle command, precedence is CLI > `sqlite_queue.synchronous` > `normal`. The standalone command does not load bundle configuration.

```sh
vendor/bin/sqlite-queue broker --database=/private/queue.db --endpoint=/private/queue.sock --synchronous=full
```

```yaml
sqlite_queue:
    synchronous: full
```

PHP callers use the package enum:

```php
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;

$broker = (new BrokerFactory($database, $endpoint, synchronous: SqliteSynchronousMode::Full))->create();
```

NORMAL preserves commits across process crashes, but an OS crash or power failure can lose recent publications or ACKs. FULL provides stronger commit durability if storage honors synchronization. See [storage and durability](contracts.md#storage-and-durability).
