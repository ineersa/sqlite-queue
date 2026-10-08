# sqlite-queue

A PHP queue broker backed by SQLite, with a socket client and a Symfony Messenger transport.

Run the broker as a separate process. Applications send messages through the PHP client or Messenger. The broker stores messages in SQLite, supports millisecond delays, and redelivers messages that are not acknowledged before their reservation expires.

## Requirements

- PHP `^8.5` and `ext-pdo_sqlite`, with SQLite 3.31.0 or newer.
- Unix sockets and `ext-posix` for the broker. The broker command also requires `ext-pcntl`.
- Symfony `^8.0`. FrameworkBundle is required for Symfony application integration, but not for standalone client use.

Composer installs the runtime dependencies, including Amp, Revolt, and Symfony Messenger, Console, Clock, Lock, and Filesystem. Doctrine is not a runtime dependency.

The package is not published on Packagist. Install it through a Composer VCS or path repository. See [Configure and run Messenger](docs/messenger-setup.md).

## Use with Symfony Messenger

The bundle registers the transport factory and `sqlite-queue:broker` command. Symfony Flex enables the bundle during installation. Configure a `sqlite-queue://<queue>` transport in `messenger.yaml`, then use the stock `messenger:consume` command.

[Configure and run Messenger](docs/messenger-setup.md) includes a two-queue example and broker startup commands. The [transport reference](docs/messenger.md) describes serialization, retries, idle waits, and failures.

## Use the PHP client

Start the broker with `vendor/bin/sqlite-queue broker`, then connect with `Client::connect()`. Use `send()`, `receive()`, `acknowledge()`, `reject()`, and `wait()` without a Symfony application kernel.

[Run the broker and use the client](docs/broker.md) provides working examples and production restart guidance.

## Delivery guarantees

- A successful send or settlement is confirmed only after SQLite commits, using WAL and synchronous NORMAL by default. Choose FULL for stronger durability across OS crashes or power loss. See [durability guarantees](docs/contracts.md#storage-and-durability).
- Delivery is at least once, not exactly once. Make external effects safe to repeat.
- Reservations expire after 60 seconds by default. Configure `--redeliver-timeout` for your handlers. There is no automatic lease renewal.
- A connection failure can hide a committed operation. The client does not reconnect or replay it automatically.
- A receipt belongs to the connection that received it. A replacement connection cannot acknowledge an old receipt.
- Cancellation stops an operation before the broker dispatches it to the SQLite worker. After dispatch, the operation may still commit. An ACK can therefore succeed after the client disconnects. Send delay starts after the worker acquires its write transaction.

See [Package contracts](docs/contracts.md) for the full guarantees and limits.

## Reference

- [Broker protocol](docs/broker-protocol.md): framing, limits, errors, and connection recovery.
- [Queue engine](docs/queue-engine.md): PHP APIs, schema, and reservation checks.
- [SQLite worker](docs/sqlite-worker.md): process model, budgets, and durability modes.
- [Benchmark runner](bench/README.md): run paired Doctrine SQLite and broker Messenger workloads.
- [Benchmark method](docs/benchmark-method.md) and [baseline results](docs/benchmark-baseline.md): measurement definitions and limitations. These results do not establish a broker performance advantage.
- [Doctrine and broker comparison](docs/benchmark-comparison.md): measured results, failures, and the archived paired capture.

## Development

Run `composer install` in this checkout. See [AGENTS.md](AGENTS.md) for Castor commands and development rules. Benchmarks require Linux, `/proc`, and the additional extensions listed in [the benchmark guide](bench/README.md).

## License

MIT. See [LICENSE](LICENSE).
