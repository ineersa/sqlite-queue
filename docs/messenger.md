# Symfony Messenger transport reference

The adapter uses the native `messenger:consume` command. It does not replace Messenger's worker, handlers, or retry policy. See [Configure and run Messenger](messenger-setup.md) for installation and startup.

## Dependencies and registration

PHP `^8.5` is required. Symfony Console, Messenger, and Clock `^8.0` are runtime dependencies. FrameworkBundle is optional for standalone PHP APIs and required for Symfony application integration.

`Ineersa\SqliteQueue\SqliteQueueBundle` registers the transport factory, native consume event subscriber, and `sqlite-queue:broker` command. Symfony Flex can discover and register the bundle during installation. Without Flex, or if registration did not occur, add the bundle to `config/bundles.php`.

The standalone command is `vendor/bin/sqlite-queue broker`. The registered application command is `bin/console sqlite-queue:broker`. Neither transport creation nor consumption starts the broker.

## Configuration

The [copyable messenger.yaml example](examples/messenger.yaml) uses `sqlite-queue://jobs` and `options.endpoint`.

| Setting | Contract |
| --- | --- |
| DSN | `sqlite-queue://<queue>`; authority is a validated, case-sensitive queue name |
| `endpoint` | Required absolute Unix socket path, supplied through options or a URL-encoded query value |
| `timeout` | Positive finite seconds, default `10`; transport allowance, not visibility timeout |
| Option precedence | Explicit options override query values |
| Rejected DSN fields | Credentials, port, fragment, and nonempty URL path |
| Unknown options | Rejected; Messenger's `transport_name` metadata is accepted |

`TransportFactory` connects when the transport service is resolved. Kernel boot and broker help do not require a live broker, but resolving the transport does. Database and endpoint directories must be private and paths absolute. See [broker path requirements](broker.md).

## Serialization and delivery stamps

The configured Messenger serializer encodes and decodes envelopes. The broker stores opaque body and header bytes. The adapter encodes serializer headers as JSON with UTF-8 string values, preserving NUL characters and rejecting invalid UTF-8.

The example selects Messenger's native `PhpSerializer`, which is suitable only for trusted messages. Applications can select another Messenger serializer.

Received envelopes carry `DeliveryReceiptStamp`, a non-sendable stamp containing the receipt and owning transport instance, plus Symfony's `TransportMessageIdStamp`. ACK and reject require the original transport and receipt. Resending strips these delivery-specific stamps before serialization. Symfony supplies delay, retry, and other envelope stamps; the adapter does not duplicate them.

`get()` is nonblocking and claims at most one delivery. `DelayStamp` retains integer milliseconds, including positive subsecond delays. ACK removes the delivery. Reject is terminal and removes it too. Messenger's retry listener owns republishing and backoff; the broker has no second retry loop.

## Native consume idle behavior

For one literal receiver from this transport, omitting `--sleep` enables bounded broker WAIT. The subscriber temporarily changes the native command's sleep default to zero and restores it on stop, error, or termination. An idle event waits up to 1,000 milliseconds, capped by the native time limit. Readiness is only a hint to receive again, not a reservation.

Explicit `--sleep=0` also enables WAIT. Explicit positive sleep, including `--sleep=0.5`, retains native polling. Multiple receivers, `--all`, regex-like names, and unrelated transports retain native polling. There is no multi-queue WAIT. Dotted receiver names also retain polling on Symfony 8.0, where native receiver selection is literal rather than regex-based.

Console stop signals cancel an active WAIT, except `SIGALRM`, which the subscriber leaves to native handling. Other idle stop listeners may wait for one 1,000-millisecond budget because Worker has no public stopping-state getter. This is a consumer idle limitation, separate from the broker's shutdown budget.

Message, memory, and time limits remain Messenger's responsibility. No custom consume command or manual Worker construction is required.

## Failures and visibility

Broker and protocol failures become Symfony `TransportException`. The client does not reconnect or replay uncertain operations automatically. A mutation may have committed before its confirmation was lost.

On Symfony 8.0, a decoding failure rejects the committed claim and throws `MessageDecodingFailedException`. On Symfony 8.1+, the adapter uses `MessageDecodingFailedException::wrap()` to return a failure envelope for Messenger's decoding-failure handling. Opaque malformed headers remain available through `Transport::RAW_HEADERS_HEADER`.

There is no lease keepalive. The broker's default visibility timeout is 5,000 milliseconds. A handler that runs beyond visibility can overlap a redelivery, and its old receipt can no longer settle the reservation. Select suitable broker visibility through `BrokerFactory` when using the PHP API, and make external effects idempotent. The CLI does not expose a visibility option.

## Verified evidence

Current-tree `vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa` passed with 335 tests and 2,028 assertions. Isolated Symfony 8.0 adapter and native-command tests passed with 53 tests and 257 assertions; production-source PHPStan also passed. The isolated matrix used FrameworkBundle, Console, Messenger, and DependencyInjection 8.0.15, and Clock 8.0.8.

Task 06 is implemented awaiting PR and user review, not merged. The package is unpublished. These results establish correctness coverage, not benchmark performance or completion of the Task 07 failure audit.
