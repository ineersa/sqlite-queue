# Symfony Messenger transport reference

The adapter uses the native `messenger:consume` command. It does not replace Messenger's worker, handlers, or retry policy. See [Configure and run Messenger](messenger-setup.md) for installation and startup.

## Dependencies and registration

PHP `^8.5` is required. Symfony Console, Messenger, and Clock `^8.0` are runtime dependencies. FrameworkBundle is optional for standalone PHP APIs and required for Symfony application integration.

`Ineersa\SqliteQueue\SqliteQueueBundle` registers the transport factory, native consume event subscriber, and `sqlite-queue:broker` command. Composer package type `symfony-bundle` enables Flex discovery. Real installation into a fresh Flex application with `composer install --no-dev` verifies automatic registration without a pre-registered bundle. Without Flex, add the bundle to `config/bundles.php`.

The standalone command is `vendor/bin/sqlite-queue broker`. The application command is `bin/console sqlite-queue:broker`. Neither transport creation nor consumption starts the broker.

## Configuration and acquisition

The [copyable messenger.yaml example](examples/messenger.yaml) uses `sqlite-queue://jobs` and `options.endpoint`.

| Setting | Contract |
| --- | --- |
| DSN | `sqlite-queue://<queue>`; authority is a validated, case-sensitive queue name |
| `endpoint` | Required absolute Unix socket path, supplied through options or a URL-encoded query value |
| `timeout` | Positive finite seconds, default `10`; communication allowance, not visibility timeout |
| Option precedence | Explicit options override query values |
| Rejected DSN fields | Credentials, port, fragment, and nonempty URL path |
| Unknown options | Rejected; Messenger's `transport_name` metadata is accepted |

`TransportFactory` validates configuration without connecting. Kernel boot, help, and transport service resolution work without a live broker. The first operation acquires its client lazily; the first notification wait acquires a separate client. These are two broker socket clients, not two SQLite connections. See [broker path requirements](broker.md).

Direct `Transport` construction requires distinct operation and notification `BrokerConnection` owners. Each takes a connector closure accepting an Amp `Cancellation` and returning a fresh `Client`. Reusing an owner is rejected. Closing an owner cancels initial connection and handshake acquisition. A late connector result cannot reopen it. The factory supplies these owners automatically.

## Serialization and delivery stamps

The configured Messenger serializer encodes and decodes envelopes. The broker stores opaque body and header bytes. The adapter encodes serializer headers as JSON with UTF-8 string values, preserving NUL characters and rejecting invalid UTF-8.

The example selects Messenger's native `PhpSerializer`, suitable only for trusted messages. Applications can select another Messenger serializer.

Received envelopes carry `DeliveryReceiptStamp`, a non-sendable stamp containing the receipt and owning transport instance, plus Symfony's `TransportMessageIdStamp`. ACK and reject require the original transport and receipt. Resending strips these delivery-specific stamps before serialization. Symfony supplies delay, retry, and other envelope stamps.

`get()` is nonblocking and claims at most one delivery. `DelayStamp` retains integer milliseconds, including positive subsecond delays. ACK removes the delivery. Reject is terminal and removes it too. Messenger's retry listener owns republishing and backoff; the broker has no second retry loop.

## Native consume idle behavior

For one literal receiver from this transport, omitting `--sleep` enables bounded broker WAIT. The subscriber temporarily changes the native command's sleep default to zero and restores it on stop, error, or termination. An idle event waits up to 1,000 milliseconds, capped by the native time limit. Readiness is only a hint to receive again, not a reservation.

Explicit `--sleep=0` also enables WAIT. Explicit positive sleep, including `--sleep=0.5`, retains native polling. Multiple receivers, `--all`, regex-like names, and unrelated transports retain native polling. There is no multi-queue WAIT. Dotted receiver names also retain polling on Symfony 8.0, where native receiver selection is literal rather than regex-based.

Console stop signals cancel an active WAIT, except `SIGALRM`, which the subscriber leaves to native handling. Cancellation closes only the notification client, preserving the operation client for final batch ACKs during worker shutdown. Other idle stop listeners may wait for one 1,000-millisecond budget because Worker has no public stopping-state getter. This limitation is separate from the broker's shutdown budget.

Batch idle flushing is version-dependent. Symfony 8.0 flushes pending batches after its idle sleep threshold. Symfony 8.1 flushes on the first idle iteration when sleep is zero. Native shutdown flushes pending batches. SIGTERM tests verify that this final flush can acknowledge after notification cancellation.

Message, memory, and time limits remain Messenger's responsibility. No custom consume command or manual Worker construction is required.

## Failures and visibility

Broker and protocol failures become Symfony `TransportException`. There is no automatic reconnect or replay. A mutation may have committed before its confirmation was lost. Initial connection failure also closes its owner permanently. Applications must explicitly construct a new transport after failure.

A configured failure transport follows Messenger policy. Integration tests use a real Doctrine SQLite failure queue and stock `messenger:failed:retry` to recover a failed message through its original broker transport.

On Symfony 8.0, a decoding failure rejects the committed claim and throws `MessageDecodingFailedException`. On Symfony 8.1+, the adapter uses `MessageDecodingFailedException::wrap()` to return a failure envelope. Opaque malformed headers remain available through `Transport::RAW_HEADERS_HEADER`.

There is no lease keepalive. A handler that runs beyond visibility can overlap a redelivery, and its old receipt can no longer settle the reservation.

Both broker commands accept `--visibility-timeout` in positive integer milliseconds. Bundle configuration is `sqlite_queue.visibility_timeout`. Precedence is explicit CLI value, then bundle value, then 5,000ms. The standalone command has no bundle configuration, so it uses the CLI value or default. PHP callers can supply `BrokerFactory::visibilityTimeout`.

The example configures 60,000ms, not a universal safety guarantee. Choose a lease for your handler duration and make external effects idempotent. Transport `options.timeout` is a separate communication allowance in seconds and does not change visibility.

## Verified evidence

`vendor/bin/castor cs:fix` followed by `vendor/bin/castor qa` passed with 350 tests and 2,098 assertions. Isolated Symfony 8.0 adapter and native-command tests passed with 68 tests and 327 assertions, including configured visibility; production-source PHPStan passed. Compatibility evidence is under `var/task06-review-connection-compat/`.

`vendor/bin/castor test:flex` verifies actual no-dev Flex installation, bundle discovery, native broker command registration, and offline DSN resolution. It requires network access and is excluded from ordinary QA. Task reports are under `var/qa/test-flex/`; its stdout log identifies the timestamped installation capture under `var/flex-install/`.

Task 06 is implemented in PR #7 awaiting user review, not merged. The package is unpublished. These results establish correctness coverage, not benchmark performance or completion of the Task 07 failure audit.
