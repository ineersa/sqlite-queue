# Run the broker and use the client

Use the PHP client when you do not need Symfony Messenger. One broker owns a SQLite database and a Unix socket; applications send messages and claim deliveries through that socket. The broker stores body and header bytes without interpreting them.

For a Symfony application, start with [Configure and run Messenger](messenger-setup.md). For supervision, backups and incident recovery, see [Operate the queue](operations.md).

## Install the package

Use PHP 8.5 or later within the Composer `^8.5` constraint, with `ext-sqlite3`. The broker runs on Unix and requires `posix` for ownership checks. The foreground command also requires `pcntl` for signal handling. PDO SQLite is not the storage driver.

In your application's directory, configure the required driver fork and install the released broker from [Packagist](https://packagist.org/packages/ineersa/sqlite-queue). The driver fork still uses the package name `fabpot/amphp-sqlite3`:

```sh
	composer config repositories.amp-sqlite3 vcs https://github.com/ineersa/amp-sqlite3
	composer require ineersa/sqlite-queue fabpot/amphp-sqlite3:dev-development
```

Composer does not inherit repository declarations from dependencies. The explicit driver requirement permits its development stability without lowering your application's global stability setting. Commit your application's Composer lock file for reproducible deployments.

The following commands assume an installed package and use `vendor/bin/sqlite-queue`. If you are working in this repository instead, the executable is `php bin/sqlite-queue`.

## Start the foreground broker

See the [broker command options](configuration.md#broker-command-options) for the complete option list, descriptions, defaults, and bundle precedence.

1. Create a private directory owned by the Unix user that will run the broker and clients:

   ```sh
   install -d -m 700 /tmp/sqlite-queue-demo
   ```

2. Start the broker in a terminal:

   ```sh
   vendor/bin/sqlite-queue broker \
       --database=/tmp/sqlite-queue-demo/queue.sqlite \
       --endpoint=/tmp/sqlite-queue-demo/queue.sock \
       --redeliver-timeout=60 \
       --synchronous=normal
   ```

3. Wait for a JSON line with `"event":"ready"` before starting clients. A running PID alone does not establish readiness.

Both paths must be absolute. The canonical socket path must be no longer than **100 bytes**, including the directory. Use `0700` parent directories and `0600` files; keep the database, its SQLite companions, and the socket private to the same Unix user. Startup refuses an existing endpoint rather than removing it. Use `/tmp` only for this demo; choose persistent storage for production.

`--redeliver-timeout` is the reservation duration in positive integer **seconds**, not a handler execution limit. It defaults to 60. `--synchronous` accepts `normal` and `full`, defaulting to `normal`. The standalone command does not read Symfony bundle configuration.

With the bundle enabled, `php bin/console sqlite-queue:broker` accepts the same options. That command uses `sqlite_queue.redeliver_timeout` and `sqlite_queue.synchronous` unless a CLI value overrides them. Neither command starts automatically when a client connects.

## Send and settle a message

Save this as `publish.php` beside your application's `vendor/` directory:

```php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use Ineersa\SqliteQueue\Client;

$client = Client::connect('/tmp/sqlite-queue-demo/queue.sock');
try {
	$id = $client->send('jobs', 'Hello from the publisher', '{}');
	echo 'Committed message '.$id.PHP_EOL;
} finally {
	$client->close();
}
```

Save this as `consume-once.php` in the same directory:

```php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use Ineersa\SqliteQueue\Client;

$client = Client::connect('/tmp/sqlite-queue-demo/queue.sock');
try {
	$delivery = $client->receive('jobs');
	if (null === $delivery && $client->wait('jobs', 5_000)) {
		$delivery = $client->receive('jobs');
	}
	if (null === $delivery) {
		echo 'No delivery available.'.PHP_EOL;
	} else {
		// Replace this output with your application handler.
		echo $delivery->body.PHP_EOL;
		$client->acknowledge($delivery->receipt);
	}
} finally {
	$client->close();
}
```

With the broker still running, execute:

```sh
php publish.php
php consume-once.php
```

The publisher prints a committed message ID; the consumer prints `Hello from the publisher` and deletes the delivery by acknowledging it. A second consumer invocation reports no available delivery after its bounded wait.

Body and headers are opaque byte strings, not a prescribed JSON format. The `{}` in this example is an application choice. Use `send('jobs', $body, $headers, delay: 1_500)` for a delay of 1,500 milliseconds. A send confirms only after its transaction commits.

`receive()` claims at most one eligible message and returns immediately with a `DeliveryDTO` or `null`. ACK deletes a reserved message. `reject($delivery->receipt)` also deletes it; rejection is terminal disposal, not a request for retry. Both require the same live connection that claimed the delivery. Put application retry policy outside these operations.

## Wait for readiness without busy polling

After an empty receive, call `wait('jobs', 5_000)`. A `true` result means try receiving again, not that a message is reserved for you. Another consumer may claim it first. A `false` result means the wait completed without a readiness hint. Do not add a polling sleep after a hint.

To watch multiple queues through one client:

```php
if ($client->waitAny(['jobs', 'reports'], 5_000)) {
	// Probe the queues in your application's preferred order.
	$delivery = $client->receive('jobs') ?? $client->receive('reports');
}
```

`waitAny()` accepts 1 through 16 distinct queue names and returns a boolean, not a queue name. Both wait methods accept durations from 0 through **30,000 milliseconds**. Zero is a readiness probe. A wait does not claim a message or choose your receive priority.

The client communication allowance defaults to **10 seconds**. WAIT adds its requested duration to that allowance. This is separate from the broker's **10-second storage-operation deadline**, which bounds SQL work, including eligibility lookups, but does not count idle time or the requested WAIT duration. See [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

Use separate clients for concurrent operations. One client permits only one outstanding exchange. Cancellation during a wait closes that client's connection, so do not cancel a wait on a connection whose receipts you still need to settle. The Messenger adapter manages separate operation and notification connections for this reason.

## Renew a reservation

An unacknowledged message becomes eligible for redelivery when its reservation expires. Disconnecting does not immediately release it. Without renewal, choose a reservation long enough for all processing, including deferred work before ACK.

For work performed in bounded chunks, renew on the receiving client before the current lease expires:

```php
$newDeadline = $client->keepalive($delivery->receipt);
```

The default minimum of zero selects the broker's configured reservation duration. A successful renewal returns the committed Unix-millisecond deadline, never shortens the existing lease, and keeps the receipt unchanged. An optional minimum is expressed in milliseconds: `keepalive($receipt, 120_000)` requests at least two minutes from the renewal's clock sample.

The standalone client does not schedule heartbeats for your handler. Arrange timely renewals in your application, or use [Messenger's native keepalive](messenger.md#keep-long-running-handlers-alive). Renewals cannot revive expired, reclaimed or settled deliveries. Long processing has no cumulative broker time cap while renewal succeeds, but a lost lease does not stop application code automatically.

## Recover from a failed exchange

Treat a missing confirmation as an **unknown outcome**. A send, claim, renewal, ACK or reject may have committed before the connection failed. Do not turn an exception into an automatic resend or assume rollback.

- Catch `Ineersa\SqliteQueue\Exception\TransportException` for unavailable confirmations and connection failures.
- Catch `Ineersa\SqliteQueue\Protocol\ProtocolException` for protocol violations. Its diagnostic identifies the violated constraint.
- Receipt failures use specific subclasses of `Ineersa\SqliteQueue\Exception\InvalidReceiptException`. An expired receipt is not repaired by reconnecting or retrying ACK.

I/O failure or a malformed broker reply invalidates the connection. Local validation before writing a request and explicit receipt errors can fail without doing so. Methods accept optional Amp cancellation; cancellation after dispatch does not revoke SQL. See the precise [uncertainty contract](contracts.md#uncertain-operations-and-cancellation) and [receipt errors](broker-protocol.md#receipt-errors).

After deciding how to handle the uncertain operation, create a replacement with `Client::connect()`. There is no automatic reconnect or replay. A new connection cannot settle receipts issued to the old one. Make business effects idempotent or fenced because a disconnected handler can continue while another consumer receives an expired delivery.

## Run under a supervisor in production

Keep the broker in the foreground under a process supervisor. Supervise applications separately, gate their startup on the broker's ready event, and arrange cleanup of the broker's whole process group after an abrupt exit. See [process ownership and supervision](operations.md#process-ownership-and-supervision) for deployment and restart policy.

## Stop or restart

Send SIGTERM or SIGINT, or press Ctrl-C in the foreground terminal. Normal stop closes client sockets immediately but lets already-dispatched SQL drain within one shared **five-second** shutdown budget before closing storage. A transaction may finish without its client receiving confirmation.

Fatal storage failure or deadline expiry begins storage close immediately, including during an existing graceful drain. Repeated close calls wait for actual cleanup; later cancellation can tighten the existing close budget, not restart it. The broker removes its owned socket and releases lifetime locks only after storage cleanup completes.

Wait for the broker and its owned child process to exit before starting a replacement. Restart affected clients explicitly; old receipts are not transferable. Normal stop preserves the database and lock sidecars. Never delete lock sidecars, and never automatically delete an endpoint that remains after an abrupt exit. Follow [stale endpoint recovery](operations.md#recover-a-stale-endpoint).

## WAL synchronous mode

Choose durability before starting the broker:

| Mode | Behavior |
| --- | --- |
| `normal` | Default. Preserves commits across ordinary process crashes, but an OS crash or power loss can lose recent confirmed sends or settlements. |
| `full` | Stronger commit durability across OS crashes and power loss if the storage stack honors synchronization. |

NORMAL does not provide an at-least-once guarantee across power loss. Loss of a confirmed ACK can also cause duplicate handling. FULL does not make external application effects exactly once.

For example, add `--synchronous=full` to either broker command. To set the bundle command's default:

```yaml
sqlite_queue:
    synchronous: full
```

For an embedded broker, pass `synchronous: SqliteSynchronousMode::Full` to `BrokerFactory`. Its `visibilityTimeout` argument uses milliseconds, unlike the CLI's seconds. See [storage configuration](sqlite-worker.md#connection-configuration) and [storage and durability](contracts.md#storage-and-durability) for the lower-level contracts.
