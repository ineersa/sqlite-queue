# Configure and run Messenger

Use this guide to connect an existing Symfony 8 application to two queues on one broker. Keep your normal `messenger:consume` command, message bus, handlers and retry configuration. You do not need a custom consumer command.

The broker is a separate process. Registering a transport does not start it.

## Install the package

1. Use PHP `^8.5` with `ext-sqlite3`. Run the broker on Unix with `posix` enabled. Enable `pcntl` in CLI PHP for broker signals and native consumer keepalive. This package uses the SQLite3 extension, not PDO, for queue storage.
2. With Messenger and FrameworkBundle installed in your Symfony application, add both VCS repositories and require the package and driver:

   ```sh
   composer config repositories.sqlite-queue vcs https://github.com/ineersa/sqlite-queue
   composer config repositories.amp-sqlite3 vcs https://github.com/ineersa/amp-sqlite3
   composer require ineersa/sqlite-queue:dev-main fabpot/amphp-sqlite3:dev-development
   ```

   Composer does not inherit repository declarations from dependencies. Requiring the driver's development branch explicitly permits that stability without changing your global minimum stability. Commit the resulting Composer lock file.
3. If Symfony Flex is installed, check its bundle registration in `config/bundles.php`. Otherwise add this entry to the returned array:

   ```php
   Ineersa\SqliteQueue\SqliteQueueBundle::class => ['all' => true],
   ```

The bundle registers the transport factory, native idle-wait integration, and `sqlite-queue:broker` command.

## Configure two queues

Copy [the complete messenger.yaml example](examples/messenger.yaml) into `config/packages/messenger.yaml`, or merge its entries with your existing configuration:

```yaml
sqlite_queue:
    redeliver_timeout: 60
    synchronous: normal

framework:
    messenger:
        transports:
            email:
                dsn: 'sqlite-queue://emails'
                options:
                    endpoint: '/tmp/sqlite-queue-demo/queue.sock'
                    timeout: 10
                serializer: messenger.transport.native_php_serializer
            reports:
                dsn: 'sqlite-queue://reports'
                options:
                    endpoint: '/tmp/sqlite-queue-demo/queue.sock'
                    timeout: 10
                serializer: messenger.transport.native_php_serializer
        routing:
            'App\Message\SendWelcomeEmail': email
            'App\Message\GenerateMonthlyReport': reports
```

Replace the message class names with classes in your application and register their handlers through normal Messenger service configuration. This configuration does not create those application classes.

`email` is a Messenger transport alias. `emails`, the authority in `sqlite-queue://emails`, is the literal broker queue name, not a hostname. Both transports point to the same absolute Unix socket path and share the broker's database. `options.timeout: 10` is the client communication allowance in seconds, not the reservation duration or a handler execution limit.

The example uses Messenger's native PHP serializer. Use it only with trusted messages. The broker stores serialized bytes without loading your message classes.

## Start the broker

1. Create a private directory owned by the user running the broker and application:

   ```sh
   install -d -m 700 /tmp/sqlite-queue-demo
   ```

2. Run the bundle command in a separate terminal:

   ```sh
   php bin/console sqlite-queue:broker \
       --database=/tmp/sqlite-queue-demo/queue.sqlite \
       --endpoint=/tmp/sqlite-queue-demo/queue.sock
   ```

3. Wait for the JSON event with `"event":"ready"` before sending messages or starting consumers.

Both paths must be absolute. Keep the canonical socket path at most 100 bytes long. Use persistent storage rather than `/tmp` in production, and keep its parent directory private. See [production ownership and supervision](operations.md#process-ownership-and-supervision).

The bundle command reads `sqlite_queue.redeliver_timeout` and `sqlite_queue.synchronous` from the configuration above. `--redeliver-timeout=60` and `--synchronous=normal|full` override those values. The reservation option is in positive integer seconds; the default is 60. NORMAL is the default durability mode and can lose recent commits after OS failure or power loss. Use FULL for stronger durability when storage honors synchronization. See [durability guarantees](contracts.md#storage-and-durability).

If you prefer the standalone executable, use `vendor/bin/sqlite-queue broker` with the same database and endpoint options. It **does not read bundle configuration**: pass any nondefault reservation or durability settings explicitly. Schema creation belongs to broker startup; no separate transport setup step is needed.

## Run consumers

Start one native worker for both transports:

```sh
php bin/console messenger:consume email reports --keepalive=5
```

Leave `--sleep` omitted. The adapter waits for readiness on either queue when the worker is idle. Messenger still checks `email` before `reports`; the wait does not change receive priority or guarantee fairness.

The five-second keepalive interval renews the default 60-second reservation. Keep the interval comfortably below the lease duration. Enable `pcntl` in the consumer's CLI PHP. Scheduling and communication can delay renewals, so neither an exact five-second tick nor uninterrupted ownership is guaranteed. See [long-running handlers](messenger.md#keep-long-running-handlers-alive).

You can instead dedicate workers to each transport:

```sh
php bin/console messenger:consume email --keepalive=5
php bin/console messenger:consume reports --keepalive=5
```

Dispatch the configured messages through your application's injected `MessageBusInterface`, for example from an application service:

```php
$bus->dispatch(new \App\Message\SendWelcomeEmail(/* your constructor arguments */));
$bus->dispatch(new \App\Message\GenerateMonthlyReport(/* your constructor arguments */));
```

The routing sends each envelope to its configured queue. Registered handlers run in the consumers. For a local smoke check, have a handler log a stable message identifier and verify it handles a dispatched message. Do not interpret duplicate handler output as proof of transport corruption: this is not an exactly-once transport.

For handlers that finish within a reservation, keepalive is optional. Without `--keepalive`, reservations do not renew automatically. With it, a handler can run for hours while renewal succeeds; the broker imposes no cumulative handler time cap. Application and supervisor execution limits remain separate.

## Choose idle and failure behavior

The native idle wait uses WAIT for one queue and WAIT_ANY for 2 through 16 distinct queues on the same configured endpoint, with a default one-second wait budget. Explicit positive `--sleep` keeps native polling. Mixed transport selections, different endpoints, or oversized queue sets containing sqlite-queue use a bounded fallback sleep. Consumers selecting only other transports retain their native behavior. See [idle waits](messenger.md#idle-waits) for selection and shutdown details.

Keep Messenger's normal retry policy, or configure it for your application. ACK and reject delete a delivery; the adapter does not itself republish retries. A lost confirmation may follow a committed operation, so automatic transport-level replay would be unsafe.

If you need native failure inspection, configure a **separate listable failure transport**, such as Doctrine with its required packages and connection. The sqlite-queue adapter does not implement `ListableReceiverInterface`; do not configure it as a failure store expecting `messenger:failed:show` to inspect it. See [failures and recovery](messenger.md#failures-and-recovery).

Supervise the broker and consumers as separate processes. Gate startup on broker readiness, restart affected workers after broker failure, and never reuse receipts from their old connections. Before production use, test consumer death, reservation expiry, redelivery and your application's duplicate-effect protection. See [Operate the queue](operations.md).
