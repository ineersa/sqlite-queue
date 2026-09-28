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

The executable uses Symfony Console `^8.0`. Development installation includes it. In a consuming project without development dependencies, install `symfony/console:^8.0` to use `vendor/bin/sqlite-queue`. The `Broker`, `Queue`, and `Client` APIs do not require Symfony.

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

The default visibility timeout is 5,000 milliseconds. An unacknowledged reservation becomes eligible again at its persisted deadline. Closing a client does not shorten that deadline. For custom visibility or an application-owned cancellation token, use `Broker::run()` and its constructor options rather than adding a supervisor to this package.

## Recover from a failed exchange

Catch `TransportException` at the application boundary. Discard that client and decide how to handle the uncertain operation before creating a replacement with `Client::connect()`. Do not automatically resend it: the broker may have committed before the connection failed.

Client methods accept an optional Amp `Cancellation`. Cancellation during an exchange also closes the connection and reports an uncertain outcome. A new client cannot settle receipts from the old connection. Concurrent calls on one client are rejected; use separate clients for concurrent operations.

## Stop or restart

Send SIGTERM or SIGINT, or press Ctrl-C in the foreground terminal. Wait for process exit before restarting. Normal stop removes the owned socket but preserves the database and endpoint lock file.

If an endpoint remains after an abrupt exit, do not remove it merely because a connection attempt fails. Verify that no broker or other listener owns it before removing it, or choose a new endpoint. Startup refuses existing sockets, regular files, and symlinks.

See the [protocol reference](broker-protocol.md) for exact framing, deadlines, errors, and ownership requirements. See [AGENTS.md](../AGENTS.md) for validation commands and report locations.
