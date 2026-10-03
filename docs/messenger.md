# Symfony Messenger transport reference

The adapter uses Symfony's `messenger:consume` command and normal handler and retry configuration. See [Configure and run Messenger](messenger-setup.md) for installation and examples.

## Registration

The package requires PHP `^8.5` and Symfony `^8.0`. Enable `Ineersa\SqliteQueue\SqliteQueueBundle` in a FrameworkBundle application. Symfony Flex enables this `symfony-bundle` package automatically; applications without Flex register the bundle in `config/bundles.php`.

The bundle registers the transport factory and `sqlite-queue:broker` command. The standalone command is `vendor/bin/sqlite-queue broker`. Neither a transport nor a consumer starts the broker.

## Transport configuration

| Setting | Meaning |
| --- | --- |
| DSN | `sqlite-queue://<queue>`. The authority is a case-sensitive queue name, not a Messenger transport name. |
| `endpoint` | Required absolute Unix socket path, supplied in options or as a URL-encoded query value. |
| `timeout` | Positive finite communication timeout in seconds, default `10`. It does not change reservation expiry. |
| Precedence | Explicit options override DSN query values. |

Credentials, ports, fragments, nonempty URL paths, and unknown options are rejected. Messenger's `transport_name` metadata is accepted. Queue names follow the [queue-name rules](queue-engine.md#opening-and-ownership).

In the [configuration example](examples/messenger.yaml), `email` and `reports` are Messenger aliases. Their DSNs select the broker queues `emails` and `reports` on the same socket.

Transport construction validates configuration without connecting. Container boot, transport resolution, and `messenger:setup-transports` do not require a live broker. The adapter does not implement transport setup; the broker creates its schema on startup. Actual sends, receives, and waits require the broker.

The adapter opens separate socket connections for queue operations and notification waits. Cancelling a wait does not close the connection needed to acknowledge messages during the worker's final batch flush. Both sockets use the same broker and SQLite storage.

For direct PHP construction, `Transport` requires distinct operation and notification `BrokerConnection` owners. Each receives a closure that accepts Amp `Cancellation` and returns a fresh `Client`. `TransportFactory` supplies these owners for normal Messenger use.

## Serialization and settlement

The configured Messenger serializer handles envelopes. The broker stores opaque bytes and never deserializes application objects. The adapter encodes serializer headers as JSON string values. Headers must be valid UTF-8; NUL characters are preserved.

The example uses Messenger's `PhpSerializer`. Use it only with trusted messages. Configure another Messenger serializer if required by your application.

Received envelopes carry Symfony's `TransportMessageIdStamp` and the adapter's non-sendable `DeliveryReceiptStamp`. The receipt stamp identifies the delivery and the transport that received it. ACK and reject require that transport and receipt. Resending removes both delivery-specific stamps before serialization.

`get()` returns at most one message and does not wait. ACK deletes the delivery. Reject also deletes it, without rescheduling. Messenger owns retry publication and backoff. `DelayStamp` retains integer milliseconds, including subsecond delays.

## Idle waits

A native consumer of one literal sqlite-queue receiver uses broker WAIT when `--sleep` is omitted or zero. Each idle wait lasts at most **1,000 milliseconds**, shortened by the native time limit. A readiness reply means try receiving again; it does not reserve a message.

Explicit positive sleep, such as `--sleep=0.5`, retains native polling. Multiple receivers, `--all`, regex-like names, and unrelated transports also retain polling. Dotted names retain polling on both Symfony 8.0 and 8.1. There is no multi-queue WAIT.

Console stop signals cancel an active notification wait. `SIGALRM` remains under native handling. Other idle stop listeners may take up to one additional 1,000-millisecond wait before the worker exits. Message, memory, and time limits remain Messenger's responsibility.

The public client WAIT limit is **30,000 milliseconds**. This is separate from the adapter's 1,000-millisecond idle wait and the **10-second** default communication timeout. A WAIT exchange adds the requested wait duration to the communication allowance.

Batch flush timing differs by Symfony version. Symfony 8.0 uses the idle sleep threshold; Symfony 8.1 can flush on its first idle iteration with zero sleep. Both versions force pending batches to flush during native shutdown.

## Redelivery timeout

Both broker commands accept `--redeliver-timeout` in positive integer **seconds**, default **60**. The bundle setting is `sqlite_queue.redeliver_timeout`. An explicit CLI value overrides the bundle setting. The standalone command does not load bundle configuration.

The PHP engine and `BrokerFactory::visibilityTimeout` use **milliseconds**, default **60,000**. Transport `options.timeout` remains a separate communication setting.

There is no lease renewal. Choose a reservation duration that covers your handler, including deferred batch processing. After expiry, another worker can reclaim the message and the original ACK fails, even if no worker has reclaimed it yet. A 60-second lease is not safe for every handler. Make external effects idempotent.

## Failures and recovery

Broker and protocol failures become Symfony `TransportException`. A mutation may have committed before its confirmation was lost. The adapter does not reconnect or replay automatically, including after an initial connection failure. Restart the worker or construct a new transport after handling the uncertain outcome. A new connection cannot settle old receipts.

Configure a failure transport through Messenger as usual. Applications using a Doctrine failure queue must install its dependencies. Use native `messenger:failed:show` and `messenger:failed:retry` for inspection and recovery; the broker adapter does not add a separate failure workflow.

Decode failure differs by Symfony version:

- On Symfony 8.0, the adapter rejects the committed claim and throws `MessageDecodingFailedException`.
- On Symfony 8.1 and later, it returns a failure envelope through `MessageDecodingFailedException::wrap()` for Messenger to handle.

Malformed stored headers remain available as opaque bytes under `Transport::RAW_HEADERS_HEADER`.
