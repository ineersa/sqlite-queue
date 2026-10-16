# Use Symfony Messenger with sqlite-queue

The adapter connects native `messenger:consume` workers to a separately supervised broker. Messenger owns handler execution, receive priority, retry publication and failure routing. The broker owns storage, reservations and readiness notifications.

Start with [Configure and run Messenger](messenger-setup.md) for installation and a two-queue configuration. This guide explains the settings and behavior that matter when adapting an existing application.

The [configuration reference](configuration.md) lists supported keys and command options with their types, units, defaults, and precedence. Handler execution deadlines are application-owned, not a package setting.

## Registration

Use PHP `^8.5`, Symfony `^8.0`, and `ext-sqlite3`. Enable `Ineersa\SqliteQueue\SqliteQueueBundle` in your FrameworkBundle application. Check Flex's bundle registration, or add the class entry manually in `config/bundles.php`.

The bundle registers the transport factory, `sqlite-queue:broker` command and native idle-wait integration. It does not start a broker or replace the consumer command. The standalone executable, `vendor/bin/sqlite-queue broker`, does not load bundle configuration.

## Transport configuration

| Setting | Meaning |
| --- | --- |
| DSN | `sqlite-queue://<queue>`. The authority is the literal, case-sensitive broker queue name, not a hostname or Messenger alias. |
| `options.endpoint` | Required absolute Unix socket path. It can also be supplied as a URL-encoded DSN query value. |
| `options.timeout` | Positive finite client communication allowance in seconds, default `10`. It does not change the lease or handler budget. |
| Precedence | Explicit options override DSN query values. |

Credentials, ports, fragments, nonempty URL paths and unknown options are rejected. Messenger's `transport_name` metadata is accepted. See [queue-name rules](queue-engine.md#opening-and-ownership) for the allowed names.

In [the configuration example](examples/messenger.yaml), the `email` transport uses the `emails` broker queue. The `reports` transport uses the `reports` queue. Configure exactly the same endpoint value for transports that should share notification waits.

Transport construction validates settings but defers connecting until the first operation. Container boot and transport service resolution do not require a live broker. The adapter has no transport setup interface; the broker initializes its schema at startup. Do not rely on `messenger:setup-transports` to start or provision a broker.

Normal Messenger configuration creates the required connection owners for you. For direct PHP construction, `Transport` takes a `BrokerConnection` for operations, a `QueueName`, the Messenger serializer, a distinct `BrokerConnection` for notifications, and the absolute endpoint path. Each connection owner receives a closure accepting Amp `Cancellation` and returning a fresh `Client`. Do not give both owners the same client: cancelling a wait must not close the connection needed for settlement.

## Serialization and settlement

The configured Messenger serializer encodes and decodes envelopes. The broker stores opaque bytes and never deserializes application objects. Adapter headers are a JSON object of UTF-8 string values; NUL bytes are preserved. The native PHP serializer in the example is suitable only for trusted messages. Use another Messenger serializer if your application's format or trust boundary requires it.

`get()` claims at most one message and does not wait for new work. Received envelopes carry Symfony's `TransportMessageIdStamp` and the adapter's non-sendable `DeliveryReceiptStamp`. That receipt belongs to the receiving transport and its operations connection. Do not acknowledge an envelope through another transport or reconnect and reuse its receipt. Resending an envelope removes delivery-specific stamps before serialization.

ACK deletes the reserved message. Reject also deletes it, without rescheduling. Messenger handles retry publication, backoff and failure routing around those operations. `DelayStamp` uses integer milliseconds, including subsecond delays.

A successful storage operation is confirmed after commit, but its crash durability depends on the selected synchronous mode. NORMAL can lose recent sends or settlements after an OS crash or power loss. See [storage and durability](contracts.md#storage-and-durability).

## Idle waits

Leave `--sleep` omitted or set it to zero to enable broker notification waits. Selection determines the behavior:

| Selected receivers | Idle behavior |
| --- | --- |
| One distinct sqlite-queue queue | WAIT on that queue. |
| 2 through 16 distinct sqlite-queue queues, all with the same configured endpoint | WAIT_ANY across those queues. |
| A selection containing sqlite-queue and foreign transports, different endpoints, or more than 16 distinct queues | Bounded Clock sleep fallback. |
| No sqlite-queue receivers | Native Messenger sleep options and idle listeners remain unchanged. |
| Explicit positive `--sleep`, such as `--sleep=0.5` | Native polling, without notification waits. |

The default notification and fallback budget is **1,000 milliseconds**, shortened by the native time limit. Aliases of the same queue are deduplicated. A readiness response means try receiving again; it neither reserves a message nor changes Messenger's receive order. A busy higher-priority queue can still delay a lower-priority queue.

The adapter reads the receiver set after native selection. Literal receiver names work on Symfony 8.0. Regex expansion, exclusions, `--all` and interactive selection follow the installed Symfony version's own command support; the adapter does not add those options.

Each selected transport retains its own operations connection. The group shares the first selected transport's separate notification connection. Cancelling an idle wait leaves operations connections available for final batch ACKs.

Console stop signals cancel an active notification wait. The fallback Clock sleep is not cancellation-aware while sleeping; stop handling can wait for that bounded sleep to return. Other idle listeners can also cause an additional wait before worker exit. `SIGALRM` remains under native Symfony keepalive handling. Messenger still owns message, memory and time limits.

The public client's wait limit is **30,000 milliseconds**, distinct from the adapter's one-second idle budget. WAIT adds its requested duration to the client's default **10-second communication allowance**. The broker's separate **10-second storage deadline** covers eligibility queries, not the requested wait or idle time. See [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

Cross-broker notification waiting is not implemented. A broker that does not support WAIT_ANY rejects it; the adapter does not silently downgrade to polling.

Batch handlers can retain deliveries until a flush. Messenger controls idle flush timing and forces pending batches to flush during normal worker shutdown. Size the lease or enable keepalive for time spent waiting to flush, not only active handler work.

## Redelivery timeout

The reservation duration is a broker setting, shared by the queues it serves. Both broker commands accept `--redeliver-timeout` in positive integer **seconds**, default **60**. The bundle command reads `sqlite_queue.redeliver_timeout`; a CLI value overrides it. The standalone command ignores bundle settings.

Without keepalive, choose a duration covering processing and deferred batch flushes. After expiry, another worker can reclaim the message. The original ACK fails even if no other worker has reclaimed it yet. Disconnecting does not release the reservation early.

The PHP `BrokerFactory::visibilityTimeout` argument uses **milliseconds**, default **60,000**. Transport `options.timeout` is a separate communication setting. None of these values imposes a cumulative handler execution limit.

## Keep long-running handlers alive

Enable Symfony's native consumer keepalive:

```sh
php bin/console messenger:consume email reports --keepalive=5
```

Enable `pcntl` in the consumer's CLI PHP. The bare `--keepalive` flag also selects Symfony's default five-second interval. Choose an interval comfortably shorter than the reservation duration, which defaults to 60 seconds. The interval is a scheduling target, not a guarantee that every renewal occurs exactly on time.

The consumer initiates renewal through the same operations connection that received the delivery. The broker validates receipt ownership, epoch, token and unexpired reservation, then commits a deadline extension. It does not change the receipt or shorten the lease. ACK, reject and transport close end renewal work for the delivery.

A handler can run for hours while renewals succeed. There is **no cumulative broker handler time cap**. Keepalive neither enforces nor extends an application execution budget. Configure application limits and supervision separately, and do not assume the worker's ordinary time limit can interrupt arbitrary running handler code.

Symfony invokes keepalive through `SIGALRM`. The adapter performs bounded signal-safe I/O, or defers a renewal when another exchange is active. Multiple pending ticks for the same receipt are coalesced. Deferred renewal errors surface on a later transport operation or close, without replacing an already confirmed send or settlement. For protocol details, see [KEEPALIVE](broker-protocol.md#keepalive).

A failed renewal may have committed without confirmation. There is no automatic reconnect or retry. An expired, reclaimed or settled reservation cannot be revived. A broker restart or replacement connection also loses the old receipt's ownership.

**Renewal failure does not itself stop a running handler.** Symfony can report a keepalive failure while application code continues. Decide how your application detects ownership loss and stops or fences further effects. Make external effects idempotent or fenced: a disconnected handler and a newly assigned handler can overlap after lease expiry. Keepalive is not an exactly-once guarantee.

## Failures and recovery

Broker communication and protocol failures surface as Symfony Messenger `TransportException`. Invalid receipt failures also identify lost ownership. A missing confirmation means the operation's outcome may be unknown; do not infer rollback from an exception.

The adapter does not reconnect or replay automatically, including after initial connection acquisition fails. After assessing an uncertain operation, restart the affected worker or construct a new transport. New connections cannot settle old receipts. Recovery of an unsettled delivery normally waits for reservation expiry and redelivery, rather than trying to repair its old receipt. See [application and broker recovery](operations.md#recover-after-broker-or-consumer-failure).

### Retry and failure transports

Configure Messenger's retry policy and failure routing as usual. Business failures, worker crashes and transport failures are different events; do not use retry policy as a blanket replay mechanism for uncertain sends or settlements.

The sqlite-queue adapter **does not implement `ListableReceiverInterface`**. It cannot serve as an inspectable failure store for native `messenger:failed:show` and related inspection commands. If you need that workflow, choose a separate listable failure transport, such as Symfony's Doctrine transport, and install its transport and database dependencies. Configure `failure_transport` to that transport and follow its own setup and inspection instructions.

Retrying a failed message can repeat external effects already performed. Preserve business identifiers and duplicate-effect protection through the failure workflow.

### Message decoding failures

Version-dependent handling matters when changing serializers or message classes:

- Symfony 8.0: the adapter rejects the committed claim and throws `MessageDecodingFailedException`.
- Symfony 8.1 and later: the adapter uses `MessageDecodingFailedException::wrap()` to return a failure envelope for Messenger to handle.

Malformed stored headers are preserved as opaque bytes under `Transport::RAW_HEADERS_HEADER` in the failure-envelope path. Treat serializer changes as a deployment compatibility concern; retain the classes and serializer support needed to read queued messages.
