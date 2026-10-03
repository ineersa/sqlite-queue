# Implementation plan: reusable PHP SQLite queue broker

## 1. Purpose and status

Build a standalone, reusable PHP queue broker backed by SQLite, with a PHP client, a Symfony Messenger transport adapter, and a package-owned benchmark.

This document contains the agreed scope and the context needed to implement the package without any preceding conversation. Application integration, application-specific supervision, data migration, and downstream deployment plans are outside this project.

This is a plan, not evidence of implemented or validated behavior. At preparation, this repository has an MIT license and a Composer scaffold with an empty dependency list. No queue engine, client, adapter, or benchmark was found during the initial inspection. Recheck the workspace before implementation because scaffolding changed while this plan was being written. Do not infer working APIs from the conceptual names in this document.

The agreed product boundaries are firm. The failure-policy details listed in section 6 were
settled in Task 01 and are recorded in `docs/contracts.md`; implementors must not re-decide
them. Dependency compatibility and executable bootstrap were routed in the same task.

Task 01 is complete. The supported dependency matrix, the delivery and receipt contracts, the
storage and claim contract, and the protocol shape are recorded in
[`docs/contracts.md`](../../docs/contracts.md), with the measured driver behavior in
[`docs/driver-verification.md`](../../docs/driver-verification.md). Section 6 keeps the settled
decisions and names the evidence still owed by later tasks.

Task 02 is complete. The standalone Doctrine SQLite runner captured all 24 scheduled baseline repetitions, including 14 incomplete repetitions caused by lock failures. The [method](../../docs/benchmark-method.md) and [capture](../../docs/benchmark-baseline.md) retain failures, raw evidence, and comparison limits. No candidate has been measured. Task 08 still owes matched A/B runs and sufficient tail samples.

Task 03 is complete. The [queue engine](../../docs/queue-engine.md) implements durable send, atomic receive, fenced settlement, visibility expiry, and persisted millisecond availability. File-backed tests cover concurrency, commit barriers, rollback, database-full failure, and persistence-worker death.

Task 04 is complete for the approved SIGTERM and SIGKILL lifecycle scope. PR #4 merged at `70bc6cf`. The user excluded shutdown with a deliberately SIGSTOPped worker from release requirements; removing that scenario does not fix its historical intermittent hang. The [responsibility refactor](TASK-04-RESPONSIBILITY-REFACTOR.md) separates queue policy, SQLite storage, broker session lifetime, and lifetime locks. The [protocol reference](../../docs/broker-protocol.md) defines limits and recovery.

Task 05 is implemented on `task-05-delayed-wakeups` as PR #6: bounded WAIT, `QueueNotifier` deadline scheduling, and restart/cancellation proof. Numeric queue keys and captured-timer races are fixed. Local Castor QA passes with 282 tests and 1,763 assertions. PR review remains open. Broker performance acceptance remains Task 08 work. The SIGSTOP-worker shutdown caveat from Task 04 still stands.

### Implementation task index

Read this plan before the assigned task. The task files divide the work; they do not replace these contracts. All tasks start as TODO. Keep focused correctness proof with each implementation task rather than postponing it to Task 07.

| Task | Scope | Dependencies |
| --- | --- | --- |
| [01: setup and contracts](TASK-01-SETUP-AND-CONTRACTS.md) | Done. Dependency matrix, QA foundation, async-driver verification, required policy decisions | None |
| [02: benchmark baseline](TASK-02-BENCHMARK-BASELINE.md) | Done. Package-local runner and recorded standard Messenger SQLite baseline, including failures | 01 |
| [03: async SQLite queue](TASK-03-ASYNC-SQLITE-QUEUE.md) | Done. Durable engine, atomic claims, receipts, persisted delayed availability | 01 |
| [04: broker and client](TASK-04-BROKER-AND-CLIENT.md) | Done for the approved SIGTERM/SIGKILL scope. Bounded sockets and foreground service | 03 |
| [05: delayed wakeups](TASK-05-DELAYED-WAKEUPS.md) | Done. Bounded WAIT, deadline scheduling, restart and cancellation. PR #6 merged | 04 |
| [06: Messenger adapter](TASK-06-MESSENGER-ADAPTER.md) | Implemented, awaiting PR/user review; not merged. Native consume, serializers, delay/retry mapping. QA 335/2028; isolated 8.0 adapter/native 53/257 | 05 |
| [07: failure and lifecycle proof](TASK-07-FAILURE-AND-LIFECYCLE-PROOF.md) | Remaining cross-component fault cases and independent safety review | 06 |
| [08: benchmark and MVP acceptance](TASK-08-BENCHMARK-AND-MVP-ACCEPTANCE.md) | Actual A/B comparison, documentation, package acceptance | 02 and 07 |

Use numeric order by default. Tasks 02 and 03 can proceed independently after Task 01, but freeze the baseline method before candidate tuning. Parallel writers require isolated checkouts and explicit integration ownership; do not overlap performance measurements with unrelated load.

Task completion handoffs must name the revision, implemented scope, actual validation commands/results, and remaining blockers. Do not treat unresolved policy gates, a baseline-only run, or an immediate-message prototype as completed MVP acceptance. These are project-local planning files.

## 2. Why build this

SQLite already permits one writer at a time. When many queue consumers and publishers open the database directly, they compete for that writer slot. Empty polling, reservation, acknowledgment, and publication can interfere with one another.

The proposed broker changes ownership:

```text
Publisher processes ─┐
Consumer processes  ─┼─ persistent local sockets ─ broker ─ async SQLite connection
Other clients       ─┘                                      └ persistence worker
```

Only the broker's persistence path writes the queue database. The broker serializes complete storage operations and notifies waiting consumers. Tools and other application handlers still execute in consumer processes, outside the broker.

Expected benefits are less writer contention, less polling delay, and a responsive socket loop while SQLite performs I/O. Costs include process startup, request serialization, socket traffic, persistence-worker IPC, and the remaining serialized database work. Moving contention into a broker does not eliminate waiting or make commits faster.

The benchmark must answer: what does the broker improve over consumers accessing SQLite directly, and what does it cost at equivalent durability?

## 3. Agreed architecture and ownership

### 3.1 Independent package

The new project at `/home/ineersa/projects/sqlite-queue` owns:

- The SQLite queue engine and schema.
- A foreground broker executable with explicit lifecycle.
- A persistent socket protocol and PHP client.
- Revolt event-loop integration.
- One supported async SQLite client initially.
- A Symfony Messenger transport adapter.
- Package-local correctness tests and a standalone `bench/` runner.
- Documentation sufficient to run and use the package on its own.

No consuming application, its configuration, runtime, or test fixtures may be a dependency of these components. Do not build a general plugin framework or alternate-backend abstraction for hypothetical future users.

The queue engine receives an explicit database path and operates on named queues. The broker receives its endpoint and operational configuration. It does not decide how a consuming application organizes its storage or processes.

### 3.2 Async implementation from MVP

Use Revolt for socket readiness, consumer notifications, delayed-message timers, and cancellation. Use an async SQLite client so database calls do not block the broker's event loop. This is part of MVP, not a later optimization.

`fabpot/amphp-sqlite3` is the evaluated candidate. Its inspected implementation starts an Amp process context for each SQLite connection. With one connection, the expected topology is a broker process plus one persistence child. Do not describe this as SQLite executing asynchronously in the broker process itself.

Keep one writable connection, not a pool of independent writers. Serializing individual SQL calls is insufficient: another request must not interleave statements inside an active queue transaction when a fiber suspends. Use existing Amp synchronization and driver transaction facilities to serialize complete operations.

Prefer existing Symfony, Amp, and Revolt facilities for process execution, streams, cancellation, locks, and Messenger integration. Do not create a custom persistence subprocess when the selected async client supplies it. Do not invent a Doctrine DBAL driver for the queue engine. Doctrine transport is the benchmark comparison backend.

### 3.3 Portable process ownership

Run a foreground broker configured with explicit database and socket paths. One broker owns one writable database connection and its persistence child. Exclude a second broker writer for the same database. The database survives normal shutdown and restart.

A caller or external supervisor may start and stop the foreground executable. The package owns its own readiness, shutdown, and persistence-child cleanup. It does not choose the caller's application topology or implement a shared-service supervisor.

## 4. MVP feature boundary

### 4.1 Required

1. One message table holds all named queues. A new queue name needs no migration.
2. Durable send and atomic receive with exclusive reservation.
3. ACK and terminal reject using delivery-specific receipts.
4. Lossless opaque message body bytes and headers.
5. Delayed delivery, including positive subsecond delays and Messenger `DelayStamp` compatibility.
6. Persisted availability deadlines and delayed wakeups across broker restart.
7. An explicit visibility/redelivery policy with documented behavior for uncertain deliveries and consumer failure.
8. Persistent socket server/client with bounded frames and buffering.
9. Bounded, cancellable consumer waiting and notifications instead of continuous polling as the normal broker path.
10. Commit-before-confirmation, defined ambiguous-outcome behavior, and deterministic process/resource ownership.
11. A thin Symfony Messenger adapter.
12. A standalone benchmark in this package, usable without any consuming application.

### 4.2 Deferred

- Priority scheduling.
- Batch send and batch receive APIs.
- Dead-letter administration, requeue, and purge commands.
- Detailed queue statistics APIs.
- Multiple SQLite drivers.
- A runtime typed-message validation framework. PHPStan/Psalm annotations may describe existing APIs without creating another subsystem.
- A job runner that executes handlers inside the broker.
- General remote-network deployment, multi-host coordination, or shared-project service management.
- An in-memory queue mirror, a custom write-ahead journal, or background persistence batching.

Delays are not deferred. A broker with only immediate delivery cannot replace the current transport.

Deferring administration does not permit silent data loss. Errors and uncertain deliveries still need documented outcomes and retained-state behavior. Do not introduce a default maximum-receive count that silently hides messages merely because a reference library has one.

## 5. Queue and delivery contracts

### 5.1 Storage responsibilities

SQLite is authoritative. Use SQLite transactions and recovery, not a separate custom journal. The schema must represent message identity, queue identity, payload/headers, durable availability, and the reservation state needed by the agreed receipt and redelivery policy.

Use reference libraries for mechanisms, not as a mandate to copy their entire schema. Exact column names, indexes, and public API names are not finalized here. Define them minimally after settling the delivery policy. Inspect the resulting query plans for the supported ready/delayed/reserved workloads.

Required invariants:

- A message is eligible only for its own queue and when its availability and reservation policy allow it.
- Competing receives cannot acquire the same current delivery.
- Return a delivery only after its reservation commits.
- Complete send, ACK, and reject successfully only after the corresponding storage mutation commits.
- A rollback or storage error must not produce a success reply.
- Application handlers never run inside a queue transaction.
- No transaction remains open while waiting for client input, writing a slow socket response, or waiting for future message availability.
- Eligible-message ordering is ascending insertion sequence with no priorities, settled in
  `docs/contracts.md`. Preserve that ordering consistently; do not promise completion order
  across concurrent consumers.
- Journal mode is WAL with synchronous FULL, and a send, acknowledge, or reject is confirmed
  only after its commit returns. Async-client defaults are not a durability specification; the
  file-database default is NORMAL. A process-crash recovery test is not proof that
  acknowledged writes survive power loss, so the promised durability is bounded by SQLite and
  filesystem guarantees. Task 08 measures what FULL costs against the baseline.

### 5.2 Atomic receive with the selected driver

The sqliteq reference uses `UPDATE ... WHERE id = (SELECT ...) RETURNING ...`. The inspected `fabpot/amphp-sqlite3` version does not support data-changing statements with `RETURNING`.

Do not copy that SQL and assume it works. Verify the selected driver version before implementation. If the restriction remains, a transactionally protected selection and conditional claim is a valid design to evaluate:

```text
Serialize one complete storage operation.
Begin a write transaction using supported driver facilities.
Select an eligible message under the agreed ordering.
Update its reservation and delivery generation consistently.
Read any required delivery data within that transaction.
Commit before returning the delivery.
Release operation ownership on every success/failure path.
```

This is a design outline, not a finalized schema or instruction to hand-roll transaction management. Keep it minimal, reuse the driver's transaction API, and prove exclusive claims with real SQLite. Include the additional driver IPC round trips in performance measurements.

An atomic statement avoids an application-level select/update race. It does not eliminate SQLite's internal writer lock. Do not advertise the broker as lock-free.

### 5.3 Message representation

Applications choose serialization. The broker does not deserialize PHP application objects or require JSON message bodies. Binary bodies, empty bodies, and headers must round-trip without corruption.

A Messenger envelope's serialized body and headers must survive transport unchanged in meaning. The client/adapter may encode transport metadata, but must not discard stamps, coerce payload types, or log content by default. Keep control-frame encoding separate from application payload encoding.

Validate queue identifiers and metadata. Clients must not submit filesystem paths or SQL through normal queue operations. The broker's database path is host configuration, not a per-message field.

### 5.4 Receipts, ACK, and reject

Use a receipt bound to a specific reservation and its owning context, not just a message row ID. A stale or foreign receipt must not acknowledge or reject a newer delivery. Message-ID reuse or a broker restart must not accidentally make an old receipt valid for another reservation.

Specify whether receipts survive a broker restart and how uncertainty is represented before implementing that path. Preserving a pending message does not imply that an old consumer may continue using its receipt.

ACK ends the delivery successfully. Terminal reject removes or terminally disposes of that delivery according to the adapter contract. It must not independently schedule a retry when Messenger has already done so.

Receipt fencing protects queue state. It cannot undo an external effect already performed by a stale worker and is not an exactly-once execution guarantee.

### 5.5 Mandatory delayed delivery

A delay determines when a message becomes eligible, not how long the publisher blocks.

- Preserve Messenger's millisecond delay value at the adapter boundary.
- Do not truncate a positive subsecond delay to immediate delivery.
- Persist the resulting availability deadline with the message.
- Never restart the delay interval after a broker restart.
- Exclude a future message from receive until its deadline.
- Wake waiting consumers when it becomes due, even if nothing else is published.
- If a new message has an earlier deadline, update the wake-up schedule.
- After restart, rebuild deadline scheduling from persisted state. Already-overdue messages are eligible immediately; future messages retain their original deadline.
- Delayed work must not block another ready message or another queue.

Persist deadlines in a form meaningful across process restart. Use monotonic clocks for elapsed-time measurement, not as durable timestamps that are assumed to survive reboot. Document the wall-clock assumptions of deadline evaluation.

A wakeup is a hint to try receiving, not a reservation. Register waiting consumers without a race between the empty receive and waiter registration. Recheck relevant readiness as part of that operation. Avoid constantly polling the database to implement what is advertised as notification-driven delivery.

Maintain only the derived waiter/deadline bookkeeping needed for these contracts. Do not load all queue bodies into memory or recreate the whole database as an in-memory queue.

## 6. Resolved delivery policy

Task 01 settled these decisions with the user. `docs/contracts.md` is the normative record.
Each subsection keeps the decision and names the proof the affected paths still owe.

### 6.1 Visibility and automatic redelivery

Goqite and sqliteq use visibility timeouts. A message becomes available again while a slow consumer may still be executing, which can repeat an external action. That is accepted.

The visibility timeout is configurable with a 5000 ms default, matching both references. A
claim persists its expiry. Disconnect does not release the reservation early, and a broker
restart neither resets nor extends it; the message becomes eligible again when the persisted
expiry passes. The MVP has no heartbeat or lease renewal, and no maximum receive count, so a
message is never hidden or deleted because it was received too often.

Automatic expiry is the redelivery mechanism, and explicit handler-failure retry stays with
Messenger. The package promises at-least-once delivery and no exactly-once execution
guarantee. Proof owed: an expired reservation becomes eligible again, disconnect and restart
preserve the original expiry, and a stale worker cannot fence a newer delivery (Tasks 03, 05,
07).

### 6.2 Ambiguous operations and client reconnect

A broker can commit a send or claim and lose the connection before the reply reaches the client. Absence of a reply does not prove absence of the mutation.

An uncertain send, receive, acknowledge, or reject raises a transport exception and is never
replayed. The client becomes unusable and the caller creates a new client explicitly, which
decides what to do about the unknown outcome. The package stores no durable request identity
and no deduplication state, so replay cannot happen indirectly. A deliberate Messenger retry
is a new publication, not a retransmission of the original one. Proof owed: disconnect after a
committed mutation produces the visible failure without a duplicate delivery, and Messenger's
retry listener interaction is exercised in Task 06.

### 6.3 Broker death while a handler runs

The broker may die while consumer processes continue executing. The client sees the same
transport failure as any other ambiguous operation, receipts issued by the dead broker epoch
fail against the restarted broker, and the reservation stays until its persisted expiry.
Deciding whether an application stops or resumes its own workers is outside scope.

A generic broker cannot infer whether a consumer performed an external effect, so connection
recovery is not an implicit promise of safe execution replay. Proof owed: owned-tree teardown,
no surviving persistence child, and defined receipt behavior after a broker restart
(Tasks 04 and 07).

### 6.4 Public package boundary

The package stays `ineersa/sqlite-queue` with `Ineersa\SqliteQueue\` mapped to `src/` by PSR-4, and stays MIT licensed.

The supported matrix is PHP `^8.5` and Symfony Messenger `^8.0`, verified on PHP 8.5.10 and
v8.1.7. PHP 8.4.25 passes the driver suite but is not supported; PHP 8.6 and later, and
Symfony 9 and later, are unverified. Console, Messenger, and Clock are runtime dependencies.
FrameworkBundle is optional for standalone APIs and required for application integration.
Native integration also passed isolated Symfony 8.0 tests; see Task 06 evidence.

The foreground command is `sqlite-queue broker`. Client send, immediate receive, acknowledge,
reject, close, and bounded wait are documented in [broker usage](../../docs/broker.md). No
multi-driver abstraction, version fallback, or speculative adapter layer is added.

## 7. Broker lifecycle and protocol requirements

Start with a foreground local Unix-socket service. Persistent connections avoid repeated connection setup. Multiplexed requests are not an MVP requirement; one outstanding request per connection is a reasonable initial simplification, subject to cancellation and wakeup needs.

The protocol covers send, immediate receive, ACK, terminal reject, bounded waiting, and the initialization information needed to reject incompatible clients. The [v1 wire format](../../docs/broker-protocol.md) defines those operations. Do not add queue-admin endpoints from the deferred feature list.

Before implementation, define a bounded frame format, request/reply correlation, and error representation. Existing Amp byte-stream and socket facilities should handle partial I/O and backpressure. Malformed, truncated, oversized, or unsupported frames must fail with bounded resource use.

Set finite bounds for frames, connections, pending requests, and output buffers. Choose defaults from supported payload requirements and tests, not the old Beanstalk experiment's arbitrary 16MiB limit. Limits should fail explicitly rather than silently truncate messages.

Never suspend indefinitely on a client while holding storage-operation ownership. WAIT has a bound and a cancellation path. A slow reader must not prevent unrelated clients from progressing except for the unavoidable serialized persistence work.

Use private socket/directory permissions and exclusive broker ownership for the configured database/socket namespace. Check Unix socket path-length limits. A stale socket file is not proof that its owner is dead; do not unlink another live broker's endpoint to force startup.

Lifecycle contracts include:

- Readiness only after ownership, schema initialization, database connection, and socket readiness succeed.
- Partial-startup cleanup, including a failed async persistence child.
- An explicit stopping state that rejects new work consistently and resolves or fails current waits/requests.
- Database preservation across normal stop/start.
- No surviving persistence child after completed broker shutdown.
- Clear failure when the persistence child exits or storage becomes unavailable.
- No content-bearing default logs. Operational logs may identify queue/message/request IDs and error phases without payloads or credentials.

Do not add a detached shared-service supervisor. The portable executable can be supervised by its caller.

## 8. Symfony Messenger adapter

The adapter makes the broker a transport, not a replacement for Messenger's bus or worker.

Use Symfony's real transport/factory, serializer, stamp, and exception facilities. Inspect the supported Symfony version's contracts before choosing implemented interfaces. Do not subclass a Doctrine connection with an unused local database merely to reuse its sender/receiver classes.

Required mappings:

| Messenger behavior | Broker behavior |
| --- | --- |
| Send serialized envelope | Durably persist body/headers and return transport identity as required. |
| Receive | Return an envelope only after a committed claim, with adapter-owned delivery receipt metadata. |
| ACK | Acknowledge that reservation, not an arbitrary ID. |
| Reject | Terminally dispose of that reservation, without another independent retry loop. |
| `DelayStamp` | Persist the intended availability and wake consumers when due. |
| Retry stamps/headers | Preserve the envelope information needed by Messenger retry listeners. |
| Serialization/decode failure | Follow supported Messenger transport behavior without leaving an invisible permanently stuck delivery. |
| Worker idle | Wait through the broker's bounded notification mechanism, with supported cancellation/shutdown. |

Messenger retries and backoff remain Messenger's responsibility. No broker job runner or independent handler registry is needed.

Coordinate notification waits with worker sleep through the actual Task 05 API:
`Client::wait(string $queue, int $timeoutMilliseconds, ?Cancellation $cancellation = null): bool`.
Bounds are `0..30_000`. `true` is only a receive hint; `false` is a normal timeout or empty
probe. Local out-of-range bounds preserve the request sequence. Cancellation invalidates the
connection with no replay. In the inspected Symfony worker, the idle event is followed by the
remaining configured sleep interval. Waking promptly and then sleeping for the rest of that
interval defeats part of the design. Recheck the selected version and integrate through
supported facilities without a busy loop, skipped worker limits, or hidden changes to unrelated
transports. Task 06 implements native consume idle WAIT with zero native sleep for one literal
receiver, while explicit positive sleep, multiple receivers, and regex-like names retain polling.
Non-signal idle stops can incur one 1,000ms WAIT budget. See the [Messenger reference](../../docs/messenger.md).

Batch receive is deferred. Do not promise a batch API to satisfy a hypothetical future Symfony version. Preserve the actual supported receiver contract and validate compatibility with a real worker.

The core queue engine should remain usable without booting a Symfony application. Messenger-specific types belong at the adapter boundary.

## 9. Standalone benchmark acceptance

### 9.1 Location and dependencies

Place the runner under this package's `bench/` directory, in the spirit of sqliteq's `bench/run.ts`. Provide one documented standalone invocation. The exact command does not exist yet.

The benchmark must run with this package and its declared dependencies alone. Standard Symfony Messenger and its Doctrine SQLite transport may be benchmark-only dependencies. Do not import a consuming application, custom application transport, runtime fixtures, or application test kernel.

The primary comparison uses the same Messenger workload against:

1. Standard Symfony Messenger Doctrine SQLite transport.
2. The real new broker transport adapter.

A direct PHP broker-client microbenchmark is useful separate evidence. Do not compare its raw timings with the full Messenger path and call that an equivalent A/B result.

A missing candidate cannot produce a successful comparison. A baseline-only run must say that the candidate has not been measured.

### 9.2 Initial workloads

Keep the suite small and generic:

- Send → receive → ACK with one publisher and one consumer.
- Multiple independent publishers and consumers, including several publishers feeding one consumer.
- A bounded backlog spread across named queues.
- Ready-but-idle consumers followed by publications.
- Delayed messages becoming eligible on an otherwise idle queue.
- Fixed small and larger payloads representative of a local application.

Choose sizes, counts, concurrency, warmup, and repetition before candidate tuning. Record them in the output/configuration. Use actual separate processes where the experiment is meant to test interprocess writer competition. A loop using one in-process connection is not evidence about that contention.

Do not benchmark deferred batching or priorities. Workloads must exercise generic queues and messages, not downstream application journeys.

### 9.3 Timings and resource accounting

Record correlated samples for:

- Publish invocation to durable confirmation.
- Publish invocation to consumer handler entry for immediate messages.
- ACK invocation to confirmed completion.
- Full send/receive/ACK cycle where a correlated round trip is measured.
- Delayed wake-up lateness relative to the eligibility deadline, not the original send time.

Report p50, p95, p99, maximum, sample count, and throughput per workload and repetition. Include failures, timeouts, unfinished operations, and scheduling lag. Preserve raw samples and provide a machine-readable summary plus a readable comparison.

Use monotonic clocks for durations. Verify same-host cross-process comparability before subtracting timestamps from different processes. Otherwise measure durations within a process or use request round trips. A consumer can receive a committed message before the publisher observes confirmation; do not clip that fact into a fabricated nonnegative confirmation-to-delivery latency.

Include CPU time, idle CPU rate, peak memory, and process count across the owned tree. Count the broker, consumers, publishers, and async SQLite persistence worker consistently. Do not describe transport timing as physical terminal rendering or LLM latency.

### 9.4 Fair comparison

- File-backed databases on the same filesystem/device, not `:memory:` or a hidden RAM-only comparison.
- Verify effective `journal_mode` and `synchronous` settings, commit-before-confirmation, and checkpoint behavior. Do not assume constructor defaults provide equivalent durability.
- Match payloads, serialization, middleware, handler work, and concurrency.
- Let each transport use its intended pickup mechanism. Record baseline polling and candidate notification settings instead of disabling the candidate's advantage.
- No weaker durability, asymmetric transaction batching, omitted fsync, or heavy diagnostics on only one backend.
- Separate startup/cold measurements from warmed results.
- Use repeated paired runs with alternating or seeded-random backend order. Preserve every scheduled run, including failures and outliers.
- Include controlled offered-load measurements where appropriate. Report scheduler lag/unfinished work so a slower backend cannot hide queueing by sending fewer requests. Rate generation is workload scheduling, not a test synchronization sleep.
- Check message IDs, payload integrity, expected deliveries, and ACK outcomes alongside speed.
- Record PHP, SQLite, dependency versions, source revisions, storage/machine information, concurrency, durability, warmup, and workload settings.

Fix the comparison method using baseline variability before tuning the candidate. Use enough observations to support tail estimates or label them inconclusive. Report individual-run variation, not only pooled percentiles.

Acceptance requires a working benchmark and honest reproducible results. A performance-win claim requires repeated concurrent-delivery tail improvement beyond baseline variation at equivalent durability. Do not invent an absolute latency guarantee or change the criterion after seeing candidate results. Neutral, regressing, or inconclusive evidence pauses default adoption for review rather than triggering speculative caches or weaker durability.

Long performance runs belong in an explicit bounded benchmark invocation. Ordinary unit tests should verify calculations, accounting, and report behavior deterministically, not assert that this machine is always faster than another transport.

## 10. Correctness and failure verification

Benchmarks do not establish delivery safety. Prove the following separately at the lowest suitable layer:

| Contract | Required evidence |
| --- | --- |
| Binary body/headers, empty payload, metadata validation | Client/codec and actual adapter round trips. |
| Atomic reservation | Concurrent real SQLite receivers synchronized by deterministic barriers; one owner per delivery. |
| Complete-operation serialization across awaits | Another request cannot enter an active transaction or observe an uncommitted claim. |
| Commit-before-confirmation and rollback | Real storage integration with controlled failure boundaries. No success after rollback. |
| Stale or foreign receipt | ACK/reject cannot mutate another/current reservation. |
| Delay mapping and subsecond values | Actual Messenger adapter mapping and no early eligibility. |
| Timer scheduling | Idle due wakeup, earlier inserted deadline, no missed waiter registration, and cancellation. |
| Delayed restart | Before-deadline and overdue restart preserve original availability. |
| Lost send/claim confirmation | Disconnect after storage success exercises the approved ambiguity policy without hidden duplicate replay. |
| Consumer exit after a possible external effect | Reservation/redelivery follows the approved package policy without implying exactly-once external effects. |
| Broker/persistence-child failure | Visible client failure, owned-tree teardown, no overlapping database owner, and defined receipt behavior. |
| Malformed/truncated/oversized frames and slow clients | Bounded buffers, explicit errors, no transaction retained while awaiting I/O. |
| Disk full/I/O errors | Propagated failures and retained consistency, not successful replies or empty catches. |
| Shutdown with pending WAIT and delayed work | Prompt bounded termination, no leaked resources, database survives. |
| Messenger integration | Real worker, serializer, delayed retry stamps, ACK/reject, and idle/shutdown behavior. |
| Benchmark integrity | Deterministic tests for timing math, percentile reporting, incomplete/error counts, and artifact generation. |

Do not add production APIs solely for tests. Use injected existing clock/storage/stream facilities or test-only doubles where appropriate. Use real file-backed SQLite for durability and transaction claims. A mocked repository cannot prove them.

Avoid arbitrary sleeps and timing lotteries. Use clocks, pipes, barriers, readiness events, and bounded cancellation. Timeouts are failure bounds, not synchronization. Every process has explicit ownership and teardown on success, failure, and cancellation.

The package owns its tests and isolation helpers. No external application test kernel or fixtures are required.

## 11. Repository and runtime safety

This checkout may contain live development-tool databases, logs, configuration, and unrelated uncommitted changes. They are not queue fixtures. Never benchmark against, migrate, truncate, replace, or inspect contents from those live databases.

Use unique disposable test/benchmark directories and explicit database/socket paths. Use isolated subprocess environments so inherited connection settings cannot redirect traffic to an unrelated service. Preserve existing configuration, IDE files, and unrelated changes.

Every spawned process must have explicit ownership. Never signal root-owned processes or processes marked as belonging to another live session. Do not repair a test leak by killing unrelated workers. The package's tests must verify its persistence-child cleanup rather than relying on a harness to hide leaks.

Use the project's declared QA runner. The portable benchmark must remain independently runnable; a development wrapper must not introduce a dependency on another application.

## 12. Implementation sequence and completion criteria

### Phase 1: settle the minimum contract

Inspect the reference code and selected dependency versions. Verify PHP/Symfony requirements, process bootstrap, async transaction ownership, and the `RETURNING` limitation. Section 6 decisions are resolved and recorded in `docs/contracts.md`; implementors follow them rather than re-deciding. Preserve upstream license obligations when adapting code.

Deliver a short recorded API/storage/failure contract and a minimal package skeleton. Do not ship an unsupported public API while its behavior is still undecided.

### Phase 2: establish the portable baseline

Create the small package-owned benchmark runner and standard Messenger SQLite baseline. Establish workload configuration, reporting, isolation, and repeatability. Label baseline-only results honestly. Do not fabricate an adapter for a nonexistent broker protocol.

### Phase 3: implement the complete MVP

Implement the queue engine, broker/client, delayed scheduling, lifecycle, and Messenger adapter using the agreed contracts. Add focused deterministic proof alongside each contract. No priorities, batches, stats product, or additional drivers.

An immediate-message prototype is useful during development but is not an MVP completion milestone. Mandatory delayed delivery and its restart/wakeup proof must pass before calling the package a replacement candidate.

### Phase 4: measure and review the package

Run the real A/B comparison with the actual broker adapter and equivalent durability. Review failures, tails, resource costs, and the async persistence child's overhead. Independently review correctness, protocol bounds, failure semantics, and specification fidelity.

Publish repeatable results, including unfavorable or inconclusive results. Do not claim speed based on a single sample or the presence of Amp/Revolt.

### Definition of done for the package MVP

- All section 4 required features work through the real client and Messenger adapter.
- All delivery-policy decisions needed by those features are documented and implemented consistently.
- Delayed messages preserve deadlines, cannot arrive early, survive restart, and wake idle consumers when due.
- Atomic claims and stale-receipt protection pass real persistence/concurrency proof.
- Commit confirmation, disconnects, storage errors, and broker/persistence-worker lifecycle follow the documented contract.
- The broker and its tests/benchmark have no consuming-application dependency.
- The standalone benchmark compares real backends fairly and reports sufficient evidence to assess the performance claim.
- No deferred feature or speculative compatibility layer was added to evade a failed proof or benchmark.
- Documentation describes installation, foreground broker use, PHP client use, Messenger mapping, durability/failure limits, and benchmark reproduction.

Completion ends at the independently usable package, client, Messenger adapter, documentation, and benchmark. Downstream application changes are not part of this plan.

## 13. Reference evidence

The following sources were inspected during planning. Recheck versions before adaptation. Keep necessary copyright/license notices; do not equate source availability with permission to omit attribution.

- **goqite**, primary queue-design reference, pinned at revision
  `71a935991bf7971440a7e09f50d2fca5fbb73eb8`: https://github.com/maragudk/goqite . Its
  documented SQS-style receive/timeout/extend/delete behavior is inspiration, not approval to
  inherit its redelivery defaults. At that revision it defaults to a 5 second timeout, a
  maximum receive count of 3, and priority-descending then creation-time claim order, and it
  leaves `synchronous` unset. MIT licensed; no code was copied.
- **sqliteq**, inspected revision `202d9c7e864950bdb0966a5b3d25f22f0c7e229b`:
  - Queue: https://github.com/minnzen/sqliteq/blob/202d9c7e864950bdb0966a5b3d25f22f0c7e229b/src/queue.ts
  - Benchmark: https://github.com/minnzen/sqliteq/blob/202d9c7e864950bdb0966a5b3d25f22f0c7e229b/bench/run.ts
  - Article: https://dev.to/minnzen/building-a-durable-message-queue-on-sqlite-for-ai-agent-orchestration-335m
  - Its receive generation protects conditional delete/extend. Its published microsecond benchmark uses direct synchronous calls in one process and is not a target for broker IPC latency.
  - At that revision it configures WAL and a 5 second busy timeout, leaves `synchronous`
    unset, and defaults to a 5 second timeout with a maximum receive count of 3. MIT licensed;
    no code was copied. Both revisions are relevant only as mechanism references, and neither
    sets the durability policy this package chose.
- **fabpot/amp-sqlite3**, Composer package `fabpot/amphp-sqlite3`, inspected revision `1ee168273e29af037a5c4576349ff89e3a53b5fd`:
  - README: https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/README.md
  - Requirements: https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/composer.json
  - Process startup: https://github.com/fabpot/amp-sqlite3/blob/1ee168273e29af037a5c4576349ff89e3a53b5fd/src/SqliteConnector.php
  - At that revision: PHP >=8.4, `ext-sqlite3`, Amp/parallel dependencies, process isolation per connection, writable-file WAL/NORMAL defaults, and no data-changing `RETURNING` statements. Do not confuse its minimum PHP version with the eventual package support policy.
- **TuxWeb/amp-sql-queue**, inspected revision `c0c64c4ef363c9e594de6944327d9495d6676eaf`: https://github.com/TuxWeb/amp-sql-queue/tree/c0c64c4ef363c9e594de6944327d9495d6676eaf . PostgreSQL/MySQL async worker library, useful for API/waiting reference but not a selected dependency or a substitute for this broker.
