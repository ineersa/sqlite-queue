# Configure and run Messenger

Use an existing Symfony 8 application with Messenger and FrameworkBundle installed. This package is unpublished; the commands below require a Composer VCS or path repository pointing to this source, or an already installed copy. Do not assume a Packagist release exists.

1. Add this repository through Composer's VCS or path repository configuration. Require the available branch or local package version, then run `composer install`. For this development branch, the Composer version is `dev-task-06-messenger-adapter`.
2. Check `config/bundles.php` for Flex's automatic registration of this `symfony-bundle` package. Without Flex, add this entry to the returned array:

```php
	Ineersa\SqliteQueue\SqliteQueueBundle::class => ['all' => true],
```

3. Copy [docs/examples/messenger.yaml](examples/messenger.yaml) into your application's `config/packages/messenger.yaml`. Replace the message class with your application's class. Register its Messenger handler using the application's normal service configuration. The example uses trusted PHP serialization.
4. Create a private directory owned by the user running both broker and consumer:

```sh
	mkdir -m 700 /tmp/sqlite-queue-demo
```

5. Start the foreground broker in one terminal:

```sh
	vendor/bin/sqlite-queue broker \
		--database=/tmp/sqlite-queue-demo/queue.sqlite \
		--endpoint=/tmp/sqlite-queue-demo/queue.sock \
		--visibility-timeout=60000
```

With the bundle registered, `php bin/console sqlite-queue:broker` accepts the same options instead. Use absolute paths and keep the socket path under 101 bytes. The broker requires CLI `sqlite3`, `pcntl`, and `posix` extensions.

The example sets a 60,000-millisecond lease. Choose a value for your handler duration; this value does not guarantee safety for every handler. The CLI value overrides `sqlite_queue.visibility_timeout` from the configuration file. Without either setting, visibility defaults to 5,000ms. The standalone command does not load bundle configuration. `options.timeout` controls communication in seconds, not visibility. There is no automatic lease renewal.

6. In another terminal, run the native consumer:

```sh
	php bin/console messenger:consume async
```

7. Dispatch your configured message through the application's Messenger bus. The `async` transport sends it to the broker, and the consumer handles and acknowledges it.

Leave `--sleep` omitted for bounded notification waits on this single receiver. See the [transport reference](messenger.md) for polling exceptions, stop limits, failure handling, and visibility. Use an external supervisor if the broker or consumer must restart after failure.

Transport service resolution works before broker startup because initial acquisition is lazy. Actual sends, receives, and waits require the broker. A failed connection is not retried automatically.
