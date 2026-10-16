# Broker protocol v1

The broker accepts framed requests over a private Unix-domain socket. It exposes message operations and readiness hints, not SQL, database paths, handlers, or administrative queue queries. This reference describes the wire boundary; use the [PHP client guide](broker.md) for application code.

## Framing and limits

All integers in the frame envelope are unsigned, four-byte, big-endian values:

```text
[length L][control length C][C bytes of UTF-8 JSON][body bytes][header bytes]
```

`L = 4 + C + body_length + headers_length`. The outer length excludes its own four-byte prefix. `body_length` and `headers_length` are required nonnegative JSON integers in the control object. Their sum must exactly match the remaining payload bytes. Application bytes are not embedded in JSON and need not be UTF-8.

| Resource | Default limit | Configurable? |
| --- | --- | --- |
| Frame length `L`, excluding the outer prefix | Derived from the payload budget as `max_payload + 4 + 8,192 + 380`. The published default is 1,048,576 bytes. | Derived from `max_payload`. |
| Control JSON | 2 through 8,192 bytes; decoding depth limit 32. | Fixed. |
| Combined body and headers | 1,040,000 bytes, including empty strings. | Yes. BrokerFactory, `--max-payload`, or bundle `max_payload`. |
| Broker connections | 64 total, including notification connections. | Yes. BrokerFactory, `--max-connections`, or bundle `max_connections`. |
| Outstanding exchanges | One per connection. Requests must wait for the preceding reply. | Fixed. |
| WAIT duration | Integer 0 through 30,000 milliseconds. | Fixed. |
| WAIT_ANY queue selection | JSON list of 1 through 16 distinct valid names. | Fixed. |
| Queue name | 1 through 255 ASCII letters, digits, `.`, `_`, or `-`, starting with a letter or digit. | Fixed. |

Published defaults preserve the v0.1.0 budgets. Operators may raise or lower only the payload and connection budgets. The frame budget expands or contracts with the payload budget so a worst-case control block still fits. The HELLO response advertises the active payload and connection budgets; clients adopt those budgets after HELLO. Unexpected expected-limit mismatches reject the connection. Absent `max_connections` in an original v1 HELLO defaults to 64; an explicit null is rejected. The broker rejects excessive frame lengths before reading their contents. At the configured connection limit it closes a new socket without a handshake. Connection read and write deadlines are listed in [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

## Exchanges

Every request control object contains integer `v: 1`, integer `id`, string `op`, and the two payload-length fields. The first request must be `hello` with ID 0 and empty payloads. Later IDs are consecutive positive integers starting at 1. Repeating HELLO or sending an unexpected ID is invalid.

Successful responses contain `v`, the matching `id`, `ok: true`, and `result`. Failure responses contain `v`, `id`, `ok: false`, and `error: {"code":"..."}` instead of `result`. All responses include the payload-length fields. Failure responses have no payload. Error text and worker exception details are not wire fields.

### Operations

The fields below are additional to the common request fields. Unknown request fields and unknown operations are rejected. Only SEND accepts request payload bytes.

| `op` | Required fields | Request payload | Successful `result` and response payload |
| --- | --- | --- | --- |
| `hello` | None. | Empty. | `{"max_payload":1040000,"max_connections":64}` for the published defaults, or the configured values; empty payload. |
| `send` | `queue`: valid name; `delay`: nonnegative integer milliseconds. | Raw body and headers. | Positive integer insertion ID; empty payload. |
| `receive` | `queue`: valid name. | Empty. | `null`, or delivery metadata with the raw body and headers. Never waits for future work. |
| `acknowledge` | `receipt`: string. | Empty. | `null` after terminal deletion. |
| `reject` | `receipt`: string. | Empty. | `null` after terminal deletion. No requeue. |
| `keepalive` | `receipt`: string; `keepalive_ms`: nonnegative integer milliseconds. | Empty. | Committed reservation expiry as an integer Unix timestamp in milliseconds; empty payload. |
| `wait` | `queue`: valid name; `wait_ms`: integer within the WAIT bound. | Empty. | Boolean readiness hint; empty payload. |
| `wait_any` | `queues`: list within the selection limit; `wait_ms`: integer within the WAIT bound. | Empty. | Boolean readiness hint; empty payload. Does not identify a queue. |

A populated RECEIVE result has `id`, `queue`, `receipt`, `available_at`, and `reserved_until`. `id` is the positive message identity, distinct from the outer request ID. Both timestamps are Unix wall-clock milliseconds. Empty RECEIVE responses have no payload.

For example, these are complete SEND request and response **control objects**, not complete binary frames. Append the five bytes `hello` to the request control block after encoding its two length fields. The reply has no payload:

```json
{"v":1,"id":1,"op":"send","queue":"jobs","delay":0,"body_length":5,"headers_length":0}
```

```json
{"v":1,"id":1,"ok":true,"result":42,"body_length":0,"headers_length":0}
```

ID 1 assumes a successful HELLO exchange already occurred. The message ID 42 is illustrative. Mutating success is reported after commit; [delivery contracts](contracts.md#storage-and-durability) define the durability qualifications.

### KEEPALIVE

`keepalive_ms: 0` uses the configured reservation duration. A positive value requests a minimum duration measured from the worker's transaction-time sample, not a total handler lifetime. Renewal persists:

```text
max(existing expiry, now + max(configured reservation duration, keepalive_ms))
```

The receipt must still match its message, connection owner, Queue epoch, and delivery token, and must be unexpired. Renewal neither changes the token nor shortens the deadline. Expired, settled, or reclaimed deliveries cannot be revived. Missing, non-integer, negative, or overflowing durations and unexpected payloads are `invalid_request`.

### WAIT

`wait_ms: 0` probes immediately. `true` is a hint to attempt RECEIVE, not a reservation; another consumer can claim first. `false` means the wait completed without a readiness hint. An ordinary timeout leaves the session usable. RECEIVE does not accept `wait_ms`.

WAIT_ANY registers every selected queue before checking readiness. A ready queue completes the request and removes all its registrations. A zero-duration request returns false only after all selected queues have been checked without a readiness hint. It does not alter the consumer's receive order. Duplicate names, non-list input, empty lists, or more than 16 names are invalid; the broker never truncates a selection.

While either wait is outstanding, the broker monitors the socket for disconnect or unexpected bytes. Pipelined traffic cancels the wait and produces `invalid_request` when a reply can be sent. The monitor is cancelled and awaited before the wait reply. Disconnect, cancellation, and shutdown remove registrations and timers without creating a reservation.

Watched queues use persisted availability and reservation deadlines. Committed inserts and claims trigger readiness rechecks, and watches rebuild scheduling from storage as needed after restart. Wall-clock changes can affect hint timing; see [eligibility and ordering](contracts.md#eligibility-and-ordering).

A v1 broker that does not implement `wait_any` rejects it as `invalid_request`. The PHP client does not downgrade automatically to polling.

## Errors

| Wire code | Meaning | Established session |
| --- | --- | --- |
| `invalid_request` | Invalid framing content, sequence, operation, fields, types, bounds, or unexpected payload. | Ends. |
| `unsupported_protocol_version` | `v` is not the supported integer version. | Ends. |
| `frame_too_large` | Frame or payload exceeds a bound, or the outer length is invalid. | Ends. |
| `invalid_queue_name` | Missing, non-string, or invalid queue name, including an invalid entry in WAIT_ANY. | Remains usable. |
| `broker_shutting_down` | A request reaches the broker after shutdown starts. | Ends. A response is not guaranteed. |
| Receipt errors below | The receipt does not authorize the requested mutation. | Remains usable. |

Malformed traffic receives a bounded error response only when framing and the peer permit one. Shutdown and storage failure may close the connection without an error frame. A storage error has no dedicated v1 wire code; it stops the broker. An invalid queue-name or receipt response still consumes the expected request ID.

### Receipt errors

All six concrete exceptions are under `Ineersa\SqliteQueue\Exception` and extend `InvalidReceiptException`.

| Wire code | Exception | Failure |
| --- | --- | --- |
| `malformed_receipt` | `MalformedReceiptException` | Invalid syntax or message ID outside the positive PHP integer range. |
| `no_active_reservation` | `NoActiveReservationException` | The message is absent or unreserved. |
| `receipt_owner_mismatch` | `ReceiptOwnerMismatchException` | Another connection owns the reservation. |
| `receipt_epoch_mismatch` | `ReceiptEpochMismatchException` | Another Queue epoch issued the reservation. |
| `receipt_token_mismatch` | `ReceiptTokenMismatchException` | The delivery token does not match. |
| `expired_receipt` | `ExpiredReceiptException` | Expiry is at or before the transaction's sampled time. |

Syntax is checked before storage. After a zero-row conditional DELETE or renewal UPDATE, the worker diagnoses reservation failures in table order, using the same transaction and clock sample. The first failure wins. Missing rows carry no history distinguishing never-created from already-settled messages. `ClientContextClosedException` represents local engine-context cancellation, not a receipt wire code.

## Ownership and readiness

The broker locks the canonical database and endpoint paths for its lifetime with exclusive, nonblocking Symfony FlockStore locks. Parent directories must exist, be user-owned, and deny group and other access. Paths must be absolute; the canonical socket path is at most 100 bytes. The database must be a private regular file, not a symlink or multiply linked file. Newly created database and socket files use mode 0600.

Startup refuses every existing endpoint, including leftover sockets. Shutdown removes only the socket inode this broker created, then releases the locks after storage cleanup completes. Lock sidecars remain to preserve their namespace. After an abrupt exit, verify ownership before removing a leftover socket. See [broker operation](broker.md) for the operational procedure.

Readiness is one JSON `ready` event on stdout after lock acquisition, storage initialization, and socket binding. It contains `event`, `pid`, `storage_execution: "fabpot"`, `synchronous_effective`, `database`, `endpoint`, `max_payload`, and `max_connections`. It does not expose a driver-child PID. The command remains in the foreground; SIGINT and SIGTERM request shutdown.

Normal stop exits zero. Startup failure, observed storage failure, or shutdown-budget exhaustion exits nonzero. Exit zero does not certify that the driver never forced child termination after a close-protocol failure. The broker's drain, escalation, and lock-release rules are defined under [cancellation and budgets](sqlite-worker.md#cancellation-and-budgets).

## Client recovery

The PHP client maintains one connection and permits one outstanding call. Concurrent calls raise `LogicException` rather than queueing work. Local validation failures before a write, including oversized payloads, unencodable control, and invalid WAIT bounds, preserve the client and request sequence.

Known receipt rejections raise their concrete exceptions; `invalid_queue_name` raises `InvalidArgumentException`. These responses keep the client usable. Malformed remote responses raise `ProtocolException` identifying the violated constraint and close the client. EOF, timeout, cancellation, and socket I/O failure raise `TransportException` and close it. There is no automatic reconnect.

For committed-but-unconfirmed work, cancellation, and explicit recovery, follow [uncertain operations and cancellation](contracts.md#uncertain-operations-and-cancellation). The client never opens the queue database and cannot transfer receipts to a replacement connection.
