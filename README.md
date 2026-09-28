# sqlite-queue

A reusable PHP queue broker and client backed by SQLite. Symfony Messenger integration is planned.

This checkout includes the async SQLite engine, foreground broker, socket client, driver verification, and a standalone Doctrine SQLite benchmark baseline.
Notification waiting and the Messenger adapter are not implemented yet. See
[docs/contracts.md](docs/contracts.md) for the settled package contracts and
[docs/driver-verification.md](docs/driver-verification.md) for the measured driver behavior
the later tasks depend on. The [queue engine reference](docs/queue-engine.md) describes the storage API, schema, and receipt lifecycle.
See [Run the broker and use the client](docs/broker.md) for foreground startup and a client example.

## Requirements

- PHP 8.5. The package is verified on PHP 8.5.10. PHP 8.4.25 passed the driver suite while the
  constraint allowed it but is outside the supported range, and PHP 8.6 and later are
  unverified.
- `ext-sqlite3`, with SQLite 3.31.0 or newer. The SQLite library here is 3.45.1.
- The broker also requires Unix sockets, `ext-posix`, and `symfony/lock:^8.0` with `symfony/filesystem:^8.0` as runtime dependencies. The broker executable additionally requires `ext-pcntl` and `symfony/console:^8.0`.
- Development installation also requires `ext-pdo_sqlite`, `ext-posix`, and `ext-pcntl` for the benchmark. The benchmark requires Linux `/proc`.

`fabpot/amphp-sqlite3` requires PHP 8.4, and the supported range starts at 8.5. Symfony
Messenger is needed only for the transport adapter, and only Symfony 8.x is supported.

## Install

The package is not published. Install dependencies in this checkout:

```bash
composer install
```

## Development

See [AGENTS.md](AGENTS.md) for Castor commands, validation rules, and report paths.

## Status

| Area | State |
| --- | --- |
| Package setup, dependency matrix, driver verification | Done in Task 01 |
| Queue engine and schema | Done in Task 03, including durable delayed availability |
| Foreground `sqlite-queue broker` and PHP client | Implemented in Task 04. Immediate receive only. |
| Delayed wakeups | Not started, Task 05 |
| Symfony Messenger adapter | Not started, Task 06 |
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

Development:

| Package | Constraint | Role |
| --- | --- | --- |
| `phpunit/phpunit` | `^13.2` | Test runner. |
| `symfony/messenger` | `^8.0` | Symfony compatibility probe, and the adapter in a later task. |
| `symfony/doctrine-messenger` | `^8.0` | Standard SQLite benchmark transport. |
| `symfony/event-dispatcher` | `^8.0` | Worker lifecycle events in the benchmark. |
| `doctrine/dbal` | `^4.3` | Benchmark SQLite connections. |
| `symfony/console` | `^8.0` | Benchmark commands and the optional broker CLI. |
| `symfony/process` | `^8.0` | Owned benchmark subprocesses. |

Doctrine DBAL, the Symfony Messenger SQLite transport, and its benchmark dependencies stay
out of the runtime requirements.

## Layout

```text
src/            queue engine, broker, framing, and PHP client
tests/Queue/    file-backed storage-engine tests
tests/Broker/   protocol, client, ownership, and broker lifecycle tests
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
- [docs/broker-protocol.md](docs/broker-protocol.md): wire format, limits, ownership, and recovery semantics.
- [docs/driver-verification.md](docs/driver-verification.md): measured async-driver behavior
  and the commands that reproduce it.
- [bench/README.md](bench/README.md): run the standalone baseline with `php bin/benchmark run`.
- [docs/benchmark-method.md](docs/benchmark-method.md): fixed workloads, timing definitions, and comparison limits.
- [docs/benchmark-baseline.md](docs/benchmark-baseline.md): the first capture and retained raw evidence.

## License

MIT. See [LICENSE](LICENSE).
