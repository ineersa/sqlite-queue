# sqlite-queue

A reusable PHP queue broker backed by SQLite, with a Symfony Messenger transport adapter.

This checkout holds the package setup and the async-driver verification. The queue engine,
broker, client, and Messenger adapter do not exist yet. See
[docs/contracts.md](docs/contracts.md) for the settled package contracts and
[docs/driver-verification.md](docs/driver-verification.md) for the measured driver behavior
the later tasks depend on.

## Requirements

- PHP 8.5. The package is verified on PHP 8.5.10. PHP 8.4.25 passed the driver suite while the
  constraint allowed it but is outside the supported range, and PHP 8.6 and later are
  unverified.
- `ext-sqlite3`, with SQLite 3.31.0 or newer. The SQLite library here is 3.45.1.
- `ext-posix` for one optional driver test that kills the persistence worker. The test skips
  when the extension is missing.

`fabpot/amphp-sqlite3` requires PHP 8.4, and the supported range starts at 8.5. Symfony
Messenger is needed only for the transport adapter, and only Symfony 8.x is supported.

## Install

The package is not published. Use a path repository while it stays local:

```bash
composer install
```

## Development

```bash
composer install
composer qa          # validates composer.json, then runs the driver test suite
composer test        # the driver test suite, direct phpunit call
composer test:driver # same suite, explicit testsuite name
```

`composer qa` runs `composer validate --no-check-publish` and then PHPUnit with
`phpunit.xml.dist`. Each test opens its own file database under `var/tests/` and runs its
async work on a fresh event loop. On Linux, `/proc` lets the suite prove on teardown that no
persistence worker process is left behind. On a platform where `/proc` is unreadable, the
driver suite skips before it creates any fixture instead of passing without that evidence.
`var/` and `.phpunit.cache/` are not committed.

## Status

| Area | State |
| --- | --- |
| Package setup, dependency matrix, driver verification | Done in Task 01 |
| Queue engine and schema | Not started, Task 03 |
| Foreground `sqlite-queue broker` and PHP client | Not started, Task 04 |
| Delayed wakeups | Not started, Task 05 |
| Symfony Messenger adapter | Not started, Task 06 |
| Benchmark baseline and A/B comparison | Not started, Task 02 and Task 08 |

## Dependencies

Runtime:

| Package | Constraint | Role |
| --- | --- | --- |
| `fabpot/amphp-sqlite3` | `^1.0` | Async SQLite client. One process per connection. |
| `revolt/event-loop` | `^1.0` | Event loop used by the driver and the future broker. |

Development:

| Package | Constraint | Role |
| --- | --- | --- |
| `phpunit/phpunit` | `^13.2` | Test runner. |
| `symfony/messenger` | `^8.0` | Symfony compatibility probe, and the adapter in a later task. |

Doctrine DBAL, the Symfony Messenger SQLite transport, and its benchmark dependencies stay
out of the runtime requirements.

## Layout

```text
src/            package namespace Ineersa\SqliteQueue\ (empty until the queue engine lands)
tests/Driver/   async-driver verification tests
tests/Support/  isolated test database and process-tree helpers
docs/           package contracts and driver verification evidence
```

## Documentation

- [docs/contracts.md](docs/contracts.md): support matrix, public boundary, storage and
  delivery contracts, protocol shape.
- [docs/driver-verification.md](docs/driver-verification.md): measured async-driver behavior
  and the commands that reproduce it.

## License

MIT. See [LICENSE](LICENSE).
