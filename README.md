# sqlite-queue

A reusable PHP queue broker and client backed by SQLite, with a Symfony Messenger transport adapter.

This checkout includes the async SQLite engine, foreground broker, socket client with bounded WAIT, Messenger adapter, driver verification, and a standalone Doctrine SQLite benchmark baseline.
See [docs/contracts.md](docs/contracts.md) for the settled package contracts and [docs/driver-verification.md](docs/driver-verification.md) for the measured driver behavior later tasks depend on. The [queue engine reference](docs/queue-engine.md) describes the storage API, schema, and receipt lifecycle. See [Run the broker and use the client](docs/broker.md) for foreground startup, receive, and WAIT. See [docs/messenger.md](docs/messenger.md) for the native Messenger transport reference and [setup guide](docs/messenger-setup.md).

## Requirements

- PHP 8.5. The package is verified on PHP 8.5.10. PHP 8.4.25 passed the driver suite while the
  constraint allowed it but is outside the supported range, and PHP 8.6 and later are
  unverified.
- `ext-sqlite3`, with SQLite 3.31.0 or newer. The SQLite library here is 3.45.1.
- The broker also requires Unix sockets, `ext-posix`, and `symfony/lock:^8.0` with `symfony/filesystem:^8.0` as runtime dependencies. The broker executable additionally requires `ext-pcntl` and `symfony/console:^8.0`.
- Development installation also requires `ext-pdo_sqlite`, `ext-posix`, and `ext-pcntl` for the benchmark. The benchmark requires Linux `/proc`.

`fabpot/amphp-sqlite3` requires PHP 8.4, and the supported range starts at 8.5. Symfony
Messenger, Console, and Clock are runtime dependencies. Only Symfony 8.x is supported. FrameworkBundle is optional unless using the Symfony application integration.

## Install

The package is not published. For application installation, configure a Composer VCS or path repository first. See [Messenger setup](docs/messenger-setup.md). Install dependencies in this checkout:

```bash
	composer install
```

## Development

See [AGENTS.md](AGENTS.md) for Castor commands, validation rules, and report paths.

`vendor/bin/castor test:flex` runs an opt-in real no-dev Flex installation probe. It requires network access and is not part of ordinary QA. Reports are under `var/qa/test-flex/`.

The `symfony-bundle` package registers its bundle automatically with Flex. Transport clients connect lazily, so container boot and transport resolution work before broker startup. Messenger uses separate operation and notification socket clients to preserve final batch ACKs after WAIT cancellation. They share the broker's SQLite storage, not separate SQLite connections. See the [setup guide](docs/messenger-setup.md) for configurable visibility and native consumption.

## Status

| Area | State |
| --- | --- |
| Package setup, dependency matrix, driver verification | Done in Task 01 |
| Queue engine and schema | Done in Task 03, including durable delayed availability |
| Foreground `sqlite-queue broker` and PHP client | Done in Task 04 for the approved SIGTERM/SIGKILL scope. Immediate receive, ACK, reject, and close. |
| Delayed wakeups | Done in Task 05. Bounded WAIT, deadline scheduling, and restart/cancellation proof. |
| Symfony Messenger adapter | Implemented in Task 06, PR #7 awaiting user review, not merged. Native consume integration; QA 350 tests/2,098 assertions. Isolated Symfony 8.0 adapter/native tests: 68/327. |
| Benchmark baseline | Task 02 complete, including recorded lock failures and comparison limits |
| A/B comparison | Not started, Task 08. No broker performance result. |

## Dependencies

Runtime:

| Package | Constraint | Role |
| --- | --- | --- |
| `fabpot/amphp-sqlite3` | `^1.0` | Async SQLite client. One process per connection. |
| `revolt/event-loop` | `^1.0` | Event loop used by the driver and broker. |
| `amphp/sync` | `^2.3` | Whole-operation mutex for queue storage. |
| `amphp/amp` | `^3.1` | Futures and cancellation. |
| `amphp/socket` | `^2.4` | Unix sockets, partial I/O, and backpressure. |
| `amphp/parallel` | `^2.3` | Observe the persistence process supplied by the SQLite driver. |
| `symfony/lock` | `^8.0` | Exclusive database and endpoint ownership. Required by the broker. |
| `symfony/filesystem` | `^8.0` | Private file and socket handling for broker ownership. Required by the broker. |
| `symfony/console` | `^8.0` | Broker CLI. |
| `symfony/messenger` | `^8.0` | Transport and native consumer integration. |
| `symfony/clock` | `^8.0` | Consumer time-limit clock. |

Development:

| Package | Constraint | Role |
| --- | --- | --- |
| `phpunit/phpunit` | `^13.2` | Test runner. |
| `symfony/doctrine-messenger` | `^8.0` | Standard SQLite benchmark transport. |
| `symfony/event-dispatcher` | `^8.0` | Worker lifecycle events in the benchmark. |
| `doctrine/dbal` | `^4.3` | Benchmark SQLite connections. |
| `symfony/framework-bundle` | `^8.0` | Optional application integration; native-command test kernel. |
| `symfony/process` | `^8.0` | Owned benchmark subprocesses. |

Doctrine DBAL, the Symfony Messenger SQLite transport, and its benchmark dependencies stay
out of the runtime requirements.

## Layout

```text
	src/            queue engine, broker, framing, PHP client, and Messenger adapter
	tests/Queue/    file-backed storage-engine tests
	tests/Broker/   protocol, client, ownership, and broker lifecycle tests
	tests/Messenger Messenger transport and native command process tests
	tests/Driver/   async-driver verification tests
	tests/Support/  isolated test database and process-tree helpers
	tests/Bench/    deterministic benchmark accounting and process checks
	bench/          standalone Doctrine SQLite baseline runner
	bin/benchmark   Symfony Console entry point
	bin/sqlite-queue foreground broker command
	docs/           package contracts and driver verification evidence
```

## Documentation

- [docs/contracts.md](docs/contracts.md): support matrix, public boundary, storage and
  delivery contracts, protocol shape.
- [docs/broker.md](docs/broker.md): run the broker and use the PHP client.
- [docs/messenger.md](docs/messenger.md): native transport, idle waits, serializers, and failure behavior.
- [docs/messenger-setup.md](docs/messenger-setup.md): install through a VCS/path repository, configure, start, and consume.
- [docs/broker-protocol.md](docs/broker-protocol.md): wire format, limits, ownership, and recovery semantics.
- [docs/driver-verification.md](docs/driver-verification.md): measured async-driver behavior
  and the commands that reproduce it.
- [bench/README.md](bench/README.md): run the standalone baseline with `php bin/benchmark run`.
- [docs/benchmark-method.md](docs/benchmark-method.md): fixed workloads, timing definitions, and comparison limits.
- [docs/benchmark-baseline.md](docs/benchmark-baseline.md): the first capture and retained raw evidence.

## License

MIT. See [LICENSE](LICENSE).
