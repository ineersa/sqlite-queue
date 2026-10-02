# Package contracts

This reference records the contracts settled in Task 01. Later tasks must implement them
without re-deciding them. It does not publish PHP or wire signatures. Those land with the
client in Task 04 and the adapter in Task 06.

Evidence for the driver behavior behind these contracts is in
[driver-verification.md](driver-verification.md).

## Support matrix

| Component | Supported | Verified |
| --- | --- | --- |
| PHP | `^8.5` | 8.5.10 |
| `ext-sqlite3` | required | SQLite library 3.45.1 |
| `fabpot/amphp-sqlite3` | `^1.0` | v1.0.0, revision `1ee168273e29af037a5c4576349ff89e3a3b5fd` |
| `revolt/event-loop` | `^1.0` | v1.0.9 |
| `symfony/messenger` | `^8.0`, optional | Resolution only: v8.1.7 resolves, the adapter does not exist yet |

PHP 8.4.25 passed the driver suite while the constraint allowed it but is outside the supported
range; the committed lock requires PHP >= 8.5, so the suite does not run there. PHP 8.6 and
later are unverified. Symfony 7.4 resolves with the driver, and the package does not support
it. The core engine and client run without Symfony installed; the broker requires
`symfony/lock` and `symfony/filesystem` at runtime for exclusive ownership; `symfony/messenger`
stays a development dependency until the adapter in Task 06 moves it to an optional runtime
requirement.

Doctrine DBAL and the Symfony Messenger Doctrine SQLite transport are benchmark-only. The
package supports one async SQLite client, with no multi-driver abstraction.

## Package boundary

The package owns the queue engine and schema, a foreground broker executable, a persistent
socket protocol with a PHP client, Revolt integration, the Messenger transport adapter, a
standalone `bench/` runner, and its own tests and documentation.

The reserved command spelling is `sqlite-queue broker`. Client operations are send, receive,
acknowledge, reject, bounded wait, and close. Nothing else is part of the public boundary.
Message bodies and headers are opaque bytes to the broker; the package never deserializes
application objects, requires JSON, or logs payloads by default.

## Storage contract

SQLite holds the only authoritative state. There is no side journal and no in-memory mirror.

- One table holds every named queue. Sending to a new queue name requires no migration.
- A row represents message identity, queue name, body bytes, headers, an availability
  deadline, and the reservation state below. Exact column names and indexes are Task 03 work.
- A message is eligible only for its own queue and only when its availability deadline has
  passed and no reservation fence is active.
- Competing receives cannot acquire the same delivery. A delivery is returned only after its
  reservation commits, and the same rule holds for send, acknowledge, and reject.
- A rollback or storage error never produces a success reply.
- Application handlers never run inside a queue transaction. No transaction stays open while
  the broker waits for client input, writes a slow socket response, or waits for future
  availability.

### Durability

The queue database uses journal mode WAL and synchronous mode FULL on the writing connection.
The driver default for a file database is WAL and NORMAL, so the engine passes FULL
explicitly. A send, acknowledge, or reject is confirmed only after its commit returns.
Durability is bounded by SQLite and filesystem guarantees on the host. A broker crash does not
lose a confirmed mutation, and survival of a power loss holds only as far as FULL and the
filesystem provide.

Neither reference project sets `synchronous` explicitly, so their published throughput
numbers say nothing about the cost of FULL. Task 02 measures the baseline and Task 08 reports
the A/B comparison at equivalent durability. If FULL costs more than the comparison can
justify, that is a benchmark finding, not permission to weaken durability silently.

### Eligible-message ordering

Receive selects eligible messages in ascending insertion sequence, where the sequence is
assigned when the send commits. Priority scheduling is deferred, so no priority column or
ordering exists. Concurrent consumers have no promised completion order.

### Atomic claim

The selected driver rejects data-changing statements with `RETURNING`, so the engine cannot
claim with a single `UPDATE ... RETURNING` statement. The claim is one serialized storage
operation:

1. Begin a write transaction with the driver's transaction API.
2. Select an eligible message in insertion-sequence order.
3. Update its reservation fence with a conditional predicate on the current fence.
4. Treat a row count of zero as a lost claim and roll back.
5. Read the delivery data inside the same transaction.
6. Commit, then return the delivery.

The driver holds the connection mutex for the whole transaction, so a second storage operation
cannot interleave statements across a suspension. Reads inside an open transaction must use
the driver's transaction object; a connection-level read on the same fiber blocks that fiber.
The engine keeps transactions short and never mixes a non-storage wait into one. Nothing here
makes SQLite lock-free, and the extra driver round trips count in the benchmark.

## Delivery, visibility, and receipts

- Visibility timeout is configurable. The default is 5000 ms, matching both reference
  projects.
- The claim persists the reservation expiry. Disconnect does not release the reservation
  early, and a broker restart does not reset or extend it. The message becomes eligible again
  when the persisted expiry passes.
- There is no heartbeat or lease renewal in the MVP, and no maximum receive count. A message
  is never hidden or deleted because it was received too often.
- A receipt identifies a delivery, the client connection that owns it, and the broker epoch
  that issued it. All three are checked at acknowledge and reject time.
- A receipt from a disconnected client, from an earlier broker run, or from an earlier
  delivery of the same message fails explicitly. It must not mutate a newer reservation.
  Message identity reuse cannot revive an old receipt.
- Acknowledge ends the delivery successfully and removes the reservation. Reject deletes the
  current delivery terminally. Neither schedules a retry; Messenger's retry listener and
  backoff own that decision.
- Redelivery after expiry can repeat an external effect a slow consumer already performed.
  The package provides at-least-once delivery and no exactly-once execution guarantee.

Receipt fencing protects queue state. It cannot undo an action a stale worker already took.

## Ambiguity and reconnect

A broker can commit a mutation and lose the connection before the reply arrives. Absence of a
reply never proves absence of the mutation.

- On an uncertain send, receive, acknowledge, or reject, the client raises a transport
  exception. It does not replay, retry, or guess.
- The client is unusable after such a failure. The caller creates a new client explicitly and
  decides what to do about the unknown outcome.
- Broker death, an unreachable endpoint, or a dead persistence child produce the same visible
  transport failure.
- The package stores no request identity and no deduplication state, so replay is impossible
  even indirectly. A deliberate Messenger retry is a new publication, not a retransmission.
- Whether an application stops or resumes its own workers after a transport failure is outside
  the package.

## Delayed delivery

- Delay is milliseconds, preserved at the adapter boundary. A positive subsecond delay is
  never truncated to immediate delivery.
- The send persists an availability deadline as a wall-clock instant. Deadline evaluation
  compares against wall clock. A monotonic clock measures elapsed time only and is never
  persisted as a deadline.
- A message is not eligible before its deadline, under any code path.
- A broker restart rebuilds scheduling from persisted state. It never restarts the delay
  interval. Overdue messages are eligible immediately; future messages keep their original
  deadline.
- A wakeup is a hint to try receiving, not a reservation. Waiter registration has no race with
  an empty receive, and notification, not polling, is the normal path. Task 05 implements those
  wakeup lanes through `Client::wait()` and the protocol WAIT operation.
- Delayed work never blocks a ready message or another queue.

## Protocol

- Frames are length-prefixed and versioned. The connection handshake exchanges protocol
  version and required initialization information, and the broker rejects an incompatible
  client explicitly.
- Control fields and application payload are framed separately. Bodies and headers are
  binary-safe, including empty bodies and arbitrary bytes.
- One outstanding request per connection. The request carries a request identifier, and the
  reply carries the same identifier with either a result or an explicit error.
- The operations are send with queue, body, headers, and delay; immediate receive with queue;
  acknowledge with a receipt; reject with a receipt; and bounded wait with queue and
  `wait_ms`. Administrative endpoints are deferred and out of scope.
- Frame size, connection count, pending requests, and output buffers are bounded. Concrete
  limits are Task 04 work, chosen from payload requirements and tests. Limits fail explicitly
  and never truncate a message silently.
- Malformed, truncated, oversized, and unsupported frames fail with bounded resource use.
- WAIT is bounded and cancellable. The broker never suspends indefinitely on a client while
  holding storage-operation ownership.
- The broker owns its database and endpoint exclusively, uses private socket permissions, and
  checks the Unix socket path length. A stale socket file never justifies removing a live
  broker's endpoint.
- Readiness follows successful ownership, schema initialization, database connection, and
  socket readiness, not process creation.

The [v1 protocol reference](broker-protocol.md) defines framing, limits, WAIT, and error codes.
Receipt errors distinguish malformed input, no active reservation, owner mismatch, epoch mismatch,
token mismatch, and expiry. They leave the client session usable. The protocol reference defines
their wire codes and deterministic precedence when several reservation predicates fail.

## Reference sources

Both references are MIT licensed. No code was copied from either. Attribution stays with any
adaptation.

| Source | Pinned revision | What it informs |
| --- | --- | --- |
| [maragudk/goqite](https://github.com/maragudk/goqite) | `71a935991bf7971440a7e09f50d2fca5fbb73eb8` | Storage shape, receive/acknowledge/delete behavior, WAL connection setup |
| [minnzen/sqliteq](https://github.com/minnzen/sqliteq) | `202d9c7e864950bdb0966a5b3d25f22f0c7e229b` | Claim query shape, delivery generation fencing, benchmark layout |

Both default to a 5000 ms visibility timeout and a maximum receive count of 3, and both order
claims by priority descending then creation time. This package takes the timeout default and
deliberately drops the receive cap and priorities, per PLAN section 4.2. Neither reference
sets `synchronous`, so neither establishes the cost of the FULL setting used here.

## Decisions left open

- Messenger mapping details, including decode failure and worker idle integration: Task 06.
- Measured cost of synchronous FULL against the baseline durability: Task 02 and Task 08.
