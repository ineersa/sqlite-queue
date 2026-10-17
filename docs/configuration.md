# Configuration reference

This page lists deployment settings, their owners, units, defaults, and precedence. Broker configuration does not configure consumers or application handlers. For installation and runnable examples, see [Messenger setup](messenger-setup.md) or [the PHP client guide](broker.md).

## Symfony bundle settings

Put broker defaults in your application's `config/packages/sqlite_queue.yaml`:

```yaml
sqlite_queue:
    redeliver_timeout: 60
    synchronous: normal
    # Optional. Defaults preserve the published 1,040,000-byte / 64-connection budgets.
    # max_payload: 1040000
    # max_connections: 64
```

These are the **only four `sqlite_queue` configuration keys**. They configure the bundle's `sqlite-queue:broker` command, not the standalone executable or individual transports.

| Key | Type and allowed values | Units | Default | Effect |
| --- | --- | --- | --- | --- |
| `redeliver_timeout` | Positive integer, at most `intdiv(PHP_INT_MAX, 1000)` | Seconds | `60` | Reservation duration for all queues served by this broker. Expired reservations become eligible for redelivery. Not a handler execution limit. |
| `synchronous` | String: `normal` or `full` | None | `normal` | WAL commit durability for this broker's database. |
| `max_payload` | Positive integer at most `4,294,967,295 - 4 - 8,192 - 380` | Bytes | `1,040,000` | Combined body and header budget for every queue served by this broker. The frame budget is derived from this value. Do not lower it below retained messages. |
| `max_connections` | Positive integer | Connections | `64` | Maximum simultaneous broker client sessions, including notification connections. |

**Precedence:** explicit broker CLI option, then bundle YAML, then the package default. The standalone `vendor/bin/sqlite-queue broker` does not load Symfony configuration; it uses its CLI options or package defaults.

NORMAL preserves commits across application or process crashes, but can lose recent confirmed writes after an OS crash or power failure. FULL provides stronger commit durability when the storage honors synchronization. Neither provides exactly-once handler effects. See [durability guarantees](contracts.md#storage-and-durability).

## Broker command options

Both commands define the same six broker-specific options:

- Symfony application: `php bin/console sqlite-queue:broker`.
- Standalone installation: `vendor/bin/sqlite-queue broker`.

Every option takes a value. There are no broker-specific positional arguments.

| Option | Type and allowed values | Units | Default | Description |
| --- | --- | --- | --- | --- |
| `--database=PATH` | Required absolute filesystem path to a private, file-backed SQLite database | None | None; required on every start | The database shared by this broker's named queues. Startup creates the file and schema if absent, but not its parent directory. Existing files must pass ownership and private-file checks. |
| `--endpoint=PATH` | Required absolute Unix socket path; canonical path at most 100 bytes | Bytes for the path limit | None; required on every start | Socket on which the broker accepts clients. Must match the clients' endpoint. Startup refuses any existing entry at this path rather than removing it. |
| `--redeliver-timeout=SECONDS` | Positive integer, at most `intdiv(PHP_INT_MAX, 1000)` | Seconds | Bundle YAML value, otherwise `60` | Overrides the reservation duration. It does not limit total processing time while keepalive renewals succeed. |
| `--synchronous=MODE` | `normal` or `full` | None | Bundle YAML value, otherwise `normal` | Overrides SQLite WAL synchronous mode. Applies to every queue in this database. |
| `--max-payload=BYTES` | Positive integer at most `4,294,967,295 - 4 - 8,192 - 380` | Bytes | Bundle YAML value, otherwise `1,040,000` | Overrides the combined body and header budget. The frame budget is derived from this value. |
| `--max-connections=COUNT` | Positive integer | Connections | Bundle YAML value, otherwise `64` | Overrides the simultaneous client-session budget, including notification connections. |

For the standalone command, the last four defaults are always `60`, `normal`, `1,040,000`, and `64` unless explicitly supplied. Database and endpoint paths have no bundle YAML equivalents. Keep both parent directories private, owned by the broker's Unix user, and already present. See [path and permission requirements](operations.md#paths-and-permissions).

For example, after preparing the private directory:

```sh
	php bin/console sqlite-queue:broker \
		--database=/absolute/private/queue.sqlite \
		--endpoint=/absolute/private/queue.sock \
		--redeliver-timeout=60 \
		--synchronous=normal
```

The broker stays in the foreground. Wait for its JSON `ready` event before starting clients. SIGINT and SIGTERM request shutdown; neither command supervises or restarts itself.

Inspect the installed command's help for these options and Symfony's common console flags, such as `--help`, `--no-interaction`, and verbosity controls:

```sh
	php bin/console sqlite-queue:broker --help
	vendor/bin/sqlite-queue broker --help
```

There are no broker CLI flags for keepalive, handler execution time, client communication timeout, or storage-operation timeout. Payload and connection budgets are broker settings, not consumer settings. Keepalive is a [consumer option](#native-consumer-options).

## Messenger transport settings

Configure each transport under `framework.messenger.transports` in `config/packages/messenger.yaml`:

```yaml
framework:
    messenger:
        transports:
            email:
                dsn: 'sqlite-queue://emails'
                options:
                    endpoint: '/absolute/private/queue.sock'
                    timeout: 10
```

| Setting | Type and allowed values | Units | Default | Effect |
| --- | --- | --- | --- | --- |
| `dsn` queue | Required string: `sqlite-queue://<queue>` with a valid, case-sensitive queue name | None | None; required | Selects a broker queue. `email` above is a Messenger alias; `emails` is the actual queue name. |
| `options.endpoint` | Required absolute Unix socket path | None | None; required through options or DSN query | Selects the broker to connect to. Does not start a broker or select a database file. |
| `options.timeout` | Positive, finite number; numeric strings are also accepted | Seconds | `10` | Client connection and exchange allowance. Not the reservation duration or a handler execution limit. |

**Precedence:** explicit `options.endpoint` and `options.timeout` override values in the DSN query, then defaults apply where available. For example, `sqlite-queue://emails?endpoint=%2Fabsolute%2Fprivate%2Fqueue.sock&timeout=10` supplies both through the query. URL-encode query values. The queue name comes from the DSN authority, not an option.

`endpoint` and `timeout` are the only package transport options. Symfony's `transport_name` metadata is accepted but is not a user setting. Credentials, ports, URL paths, fragments, and unknown options are rejected. Queue-name constraints are in the [protocol limits](broker-protocol.md#framing-and-limits).

WAIT and WAIT_ANY add their requested wait duration to the client's exchange allowance. Increasing `timeout` does not increase the broker's storage deadline. A timeout can leave a mutation's outcome unknown; the transport does not reconnect or replay automatically.

Serializer selection, retries, routing, and failure transports are Symfony Messenger settings, not `sqlite_queue` keys. See [Messenger behavior](messenger.md).

## Native consumer options

These options belong to Symfony's `messenger:consume`, not bundle YAML or broker startup:

| Option | Value and units | Default when omitted | Effect |
| --- | --- | --- | --- |
| `--keepalive[=SECONDS]` | Positive integer seconds between renewal attempts | Disabled | With the bare flag, Symfony uses `5` seconds. An explicit value such as `--keepalive=15` selects that interval. Requires `pcntl` in consumer CLI PHP. |
| `--sleep=SECONDS` | Nonnegative number of seconds | `0` when the adapter activates for selected sqlite-queue receivers; otherwise Symfony's native `1` second | For sqlite-queue selections, omission or explicit zero enables adapter idle handling. An explicit positive value keeps native polling. Consumers without sqlite-queue receivers keep native idle handling. |
| `--time-limit=SECONDS` | Positive seconds for the worker's intake lifetime | No worker time limit | Requests a worker stop rather than interrupting an already-running handler. Not a hard per-handler timeout. |

CLI options apply to that consumer process. They do not override broker YAML or transport settings; they control different behavior. Other native worker options remain Symfony's responsibility; use `php bin/console messenger:consume --help` for the installed version's full list.

For a five-second renewal interval:

```sh
	php bin/console messenger:consume email reports --keepalive=5
```

Keep the interval comfortably below the reservation duration. Bare `--keepalive` enables renewal; omitting it does not. Successful renewals can support hours of processing with no cumulative broker time cap. Scheduling and communication can delay renewal, and renewal failure does not itself stop a handler. See [long-running handlers](messenger.md#keep-long-running-handlers-alive).

When eligible selected queues share one endpoint, native idle handling uses WAIT for one distinct queue and WAIT_ANY for 2 through 16. Mixed transports, different endpoints, or larger queue sets containing sqlite-queue use bounded fallback sleep. The default adapter wait budget is 1,000 milliseconds and has no YAML or CLI setting. See [idle selection](messenger.md#idle-waits).

## PHP-only settings

These constructor and method arguments do not have corresponding bundle keys. They apply when your code constructs the broker or client directly; they do not automatically override command-created objects.

| Argument | Type and units | Default | Configurability and scope |
| --- | --- | --- | --- |
| `BrokerFactory::visibilityTimeout` | Positive integer milliseconds | `60_000` | PHP equivalent of the CLI reservation setting, but with different units. |
| `BrokerFactory::synchronous` | `SqliteSynchronousMode` enum | `SqliteSynchronousMode::Normal` | PHP equivalent of `--synchronous`; `SqliteSynchronousMode::Full` selects FULL. |
| `BrokerFactory::storageOperationTimeoutSeconds` | Positive, finite float seconds | `10` | Configurable in PHP only. Sets both the overall storage-startup deadline and each storage-operation deadline. No YAML key or CLI flag exists. |
| `BrokerFactory::limits` | `ProtocolLimits` | `new ProtocolLimits()` | PHP equivalent of `--max-payload` and `--max-connections`. Defaults preserve the published 1,040,000-byte and 64-connection budgets. |
| `BrokerFactory::connector` | `Fabpot\Amp\Sqlite\SqliteConnector` | `new SqliteConnector()` | Optional driver connector. The default keeps the driver's `ProcessContextFactory` isolation. Pass a configured connector when the host must control Amp worker process launch. The Symfony bundle registers this class and injects it into `sqlite-queue:broker`; replace that service in the host container. There is no YAML key or CLI flag. |
| `Client::connect()` argument `timeout` | Positive, finite float seconds | `10` | Configures the direct PHP client's connection and exchange allowance. Messenger passes its resolved transport timeout to this API. |

Advanced engine ownership, cancellation, and per-operation arguments are documented in [the queue engine reference](queue-engine.md) and [the client guide](broker.md).

## Fixed bounds and unsupported settings

The standard broker exposes no YAML or CLI settings for these remaining bounds. Payload and connection budgets are listed above.

| Bound | Value | Can it be configured? |
| --- | --- | --- |
| Broker handshake read | 5 seconds | No public broker setting. |
| Established partial-frame read | 30 seconds after the first prefix bytes arrive | No public broker setting. Established-session idle time is unbounded. |
| Broker response write | 5 seconds | No public broker setting. |
| Broker shutdown | 5 seconds shared across draining and cleanup | No public broker setting. |
| Driver close ceiling | 5 seconds | No package YAML or CLI setting. Cancellation can tighten cleanup's budget, not extend it. |
| Public WAIT and WAIT_ANY duration | 0 through 30,000 milliseconds | Each call chooses a duration within this fixed limit. Not a global timeout setting. |
| Standard SQLite busy timeout | 5,000 milliseconds | No broker YAML or CLI setting. Not a whole-operation deadline. |
| Standard WAL autocheckpoint | 1,000 pages | No broker YAML or CLI setting. |
| Handler execution time | No package-imposed limit | No sqlite-queue handler timeout setting exists. Enforce application deadlines or process-supervisor limits yourself. |

The [storage budget reference](sqlite-worker.md#cancellation-and-budgets) explains deadline ownership and shutdown escalation. The [protocol reference](broker-protocol.md#framing-and-limits) lists payload, frame, and queue-selection bounds. Advanced callers transferring an already configured driver connection own that configuration; this does not add settings to the standard broker commands.
