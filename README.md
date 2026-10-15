# sqlite-queue

A PHP queue broker backed by SQLite, with a socket client and a Symfony Messenger transport.

Run the broker as a separate process. Applications send messages through the PHP client or Messenger. The broker stores messages in SQLite, supports millisecond delays, and redelivers messages that are not acknowledged before their reservation expires.

Use it for local messaging when prompt wakeups and centralized database access matter more than maximum burst throughput. The broker does not run application handlers.

## Requirements

- PHP `^8.5` and `ext-sqlite3`, with SQLite 3.31.0 or newer.
- Unix sockets and `ext-posix` for the broker. The broker command also requires `ext-pcntl`.
- Symfony `^8.0`. FrameworkBundle is required for Symfony application integration, but not for standalone client use.

Composer installs the required Symfony components, Amp, and Revolt. Doctrine is not a runtime dependency. Native Messenger keepalive also requires `ext-pcntl` in the consumer.

## Install

Install the released broker from [Packagist](https://packagist.org/packages/ineersa/sqlite-queue). It still requires the `ineersa/amp-sqlite3` driver's development branch, installed under the package name `fabpot/amphp-sqlite3`. Configure that fork's repository in your application before installing:

```sh
	composer config repositories.amp-sqlite3 vcs https://github.com/ineersa/amp-sqlite3
	composer require ineersa/sqlite-queue fabpot/amphp-sqlite3:dev-development
```

The explicit driver requirement allows its development branch without lowering your application's global stability setting. Commit your application's Composer lock file and deploy with `composer install`. See the [Symfony setup guide](docs/messenger-setup.md) or [standalone client guide](docs/broker.md) for the next steps.

## Use with Symfony Messenger

The bundle registers the transport factory and `sqlite-queue:broker` command. Configure a `sqlite-queue://<queue>` transport with an absolute socket endpoint, start the broker, then use the stock consumer:

```sh
php bin/console messenger:consume email reports --keepalive=5
```

The [setup guide](docs/messenger-setup.md) defines those two transports, their routing, and broker startup. Idle workers wait for broker notifications instead of repeatedly polling when their selected queues share one endpoint. The [Messenger guide](docs/messenger.md) explains serialization, retries, idle selection, and failures.

## Use the PHP client

Start the broker with `vendor/bin/sqlite-queue broker`, supplying its database and socket paths, then connect with `Client::connect()`. The client supports sending, immediate receive, settlement, reservation renewal, and single- or multi-queue readiness waits without a Symfony application kernel.

[Run the broker and use the client](docs/broker.md) provides copyable publisher and consumer examples.

## Delivery guarantees

- A successful send or settlement is confirmed only after SQLite commits. WAL synchronous NORMAL is the default, but can lose recent confirmed writes after an OS crash or power loss. Choose FULL for stronger durability when storage honors synchronization.
- Delivery is at least once within the selected durability guarantees, not exactly once. NORMAL does not guarantee delivery across power loss. Make external effects safe to repeat.
- Reservations expire after 60 seconds by default. Native Messenger consumers can renew them with `--keepalive`, whose default tick is 5 seconds. Processing can continue for hours while renewals succeed. The package has no handler execution-time setting; see the [configuration reference](docs/configuration.md).
- A connection failure can hide a committed operation. The client does not reconnect or replay it automatically.
- A receipt belongs to the connection that received it. A replacement connection cannot acknowledge an old receipt.
- Cancellation can stop an operation before storage begins. Dispatched mutations may commit after disconnect. A failed renewal does not itself stop a running handler.

See [delivery contracts](docs/contracts.md) for ordering, durability, cancellation, and receipt ownership.

## Run in production

Supervise the broker and consumers separately. Keep the database and socket private, wait for the broker's `ready` event before admitting clients, and configure cleanup of its whole process group after an abrupt exit. Storage operations have bounded deadlines; missing confirmations still leave their outcomes unknown.

[Operate the queue](docs/operations.md) covers shutdown, failure recovery, disk growth, stale sockets, and offline backup and restore. Test application recovery and duplicate-effect protection before deployment.

## Reference

- [Configuration](docs/configuration.md): every broker option, bundle setting, transport timeout, and consumer control, with units, defaults, and precedence.
- [Broker protocol](docs/broker-protocol.md): framing, limits, errors, and connection recovery.
- [Queue engine](docs/queue-engine.md): PHP APIs, schema, and reservation checks.
- [SQLite storage](docs/sqlite-worker.md): driver ownership, shutdown budgets, and durability modes.
- [Benchmark runner](bench/README.md): run paired Doctrine SQLite and broker Messenger workloads.
- [Benchmark method](docs/benchmark-method.md): measurement boundaries and limitations.
- [Historical comparisons](docs/benchmark-comparison.md) and [Doctrine baseline](docs/benchmark-baseline.md): older measurements, not current-build performance claims.

## Development

See the [development guide](docs/development.md) for Castor checks, focused tests, and reports. Benchmark captures accumulate on disk; the [benchmark guide](bench/README.md#manage-capture-storage) explains how to manage them safely.

## License

MIT. See [LICENSE](LICENSE).
