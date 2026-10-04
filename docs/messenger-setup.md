# Configure and run Messenger

Use an existing Symfony 8 application with Messenger and FrameworkBundle installed. The package is not published on Packagist, so installation requires a Composer VCS or path repository pointing to this source.

## Install the package

1. Add the source repository to your application's Composer configuration. Require the branch or local version available in that repository, then run `composer install`.
2. Check `config/bundles.php` for Flex's registration of `SqliteQueueBundle`. Without Flex, add this entry to the returned array:

   ```php
   Ineersa\SqliteQueue\SqliteQueueBundle::class => ['all' => true],
   ```

## Configure two queues

1. Copy [messenger.yaml](examples/messenger.yaml) to `config/packages/messenger.yaml`.
2. Replace `App\Message\SendWelcomeEmail` and `App\Message\GenerateMonthlyReport` with your message classes. Register their handlers through your normal Messenger service configuration.
3. Set both endpoints to the broker's absolute socket path.

The example routes email messages to the Messenger transport `email`, whose DSN selects the broker queue `emails`. Report messages use the transport `reports` and queue `reports`. Transport names are Messenger aliases; the DSN authority is the broker queue name. Both queues use one broker socket and database.

The example uses native PHP serialization. Accept only trusted messages with this serializer.

## Start the broker

1. Check that CLI PHP has `sqlite3`, `pcntl`, and `posix` enabled.
2. Create a private directory owned by the user running the broker and consumers:

   ```sh
   mkdir -m 700 /tmp/sqlite-queue-demo
   ```

3. Start the foreground broker:

   ```sh
   vendor/bin/sqlite-queue broker \
       --database=/tmp/sqlite-queue-demo/queue.sqlite \
       --endpoint=/tmp/sqlite-queue-demo/queue.sock \
       --redeliver-timeout=60
   ```

4. Wait for the JSON `ready` event before starting consumers.

With the bundle enabled, `php bin/console sqlite-queue:broker` accepts the same options and loads `sqlite_queue.redeliver_timeout` and `sqlite_queue.synchronous`. The standalone command does not read bundle configuration. An explicit CLI value overrides the bundle value. `--synchronous=normal|full` selects WAL commit durability, defaulting to NORMAL. See [durability guarantees](contracts.md#storage-and-durability).

Use absolute paths and a socket path no longer than 100 bytes. See [broker startup and recovery](broker.md) for permissions and supervisor configuration.

The default reservation lasts 60 seconds. Choose a longer timeout if processing or deferred batch flushes can exceed it. There is no automatic renewal. `options.timeout: 10` is the communication timeout in seconds, not the reservation duration.

## Run consumers

Run one native consumer per queue to use notification waits:

```sh
php bin/console messenger:consume email
```

In another terminal:

```sh
php bin/console messenger:consume reports
```

Leave `--sleep` omitted. Each idle consumer waits for a broker notification for up to 1,000 milliseconds instead of polling. Dispatch each message through your Messenger bus; its configured transport publishes it to the matching queue.

If one worker must consume both transports, use:

```sh
php bin/console messenger:consume email reports
```

That mixed worker uses native polling, not multi-queue WAIT. Explicit positive `--sleep`, `--all`, and regex-like selections also retain polling. See [idle waits](messenger.md#idle-waits).

Transport service resolution works before broker startup, but actual operations require the broker. Use an external supervisor for restarts. After a transport failure, do not replay an unconfirmed operation or reuse its receipt.
