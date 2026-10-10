# SQLite queue: bounded internal pipelining

**Status:** Proposed experiment; not an implemented or approved performance improvement.  
**Baseline:** `ineersa/sqlite-queue`, PR #9, `23c41b18f6259f226778a9ffffaff2fd36f1e194`.  
**Suggested repository path:** `.hatfield/plans/internal-pipelining-spec.md`.

This specification changes the parent-to-worker coordination, not SQLite storage. It follows the operation-level PDO rewrite and retains the claim simplification in `23c41b1`. Requirements below are proposed design decisions unless identified as baseline behavior. Recheck the integration branch before implementation. [R1]

## 0. Architecture decision: a worker is optional

**Internal IPC is not a SQLite requirement.** A simpler alternative is one broker process, one Revolt loop, and one local `Queue` / `SqliteQueueStorage` / PDO connection. Public client-to-broker sockets remain; only broker-to-worker IPC disappears.

Normal synchronous PDO calls can run inside an event-loop callback. The trade-off is that the loop cannot service other callbacks while those calls execute. Fibers and `Amp\async()` do not turn blocking database calls into nonblocking ones. A separate worker isolates this blocking work; it is not needed merely to serialize transactions. [E1]

The supplied diagnostic measured mean worker dispatch durations of approximately 100 microseconds for send, 141 for claim, and 86 for settlement. These include more than native SQL and are means, not worst-case loop-pause bounds. SQLite can spend longer in automatic checkpoints, FULL commits, or lock waits. The current storage uses a 5,000 ms busy timeout; lowering it does not bound every filesystem stall. [D1, R2, E2, E3]

**Recommended decision order:** first consider a small, separate one-process experiment. Proceed with the pipelined worker below when keeping the broker responsive during SQLite stalls is a requirement, or the simpler experiment proves unsuitable. Do not implement both as permanent selectable backends merely to run this comparison.

### Optional one-process experiment

In a separate worktree based on the same baseline, construct the existing local `Queue` and PDO storage in broker startup and call complete queue operations directly. Retain the public protocol, WAIT notifier, receipt fencing, SQL, transaction boundaries, payload limits, and durability settings. Keep SQL out of `Broker`; only its dependency wiring and lifecycle change. No suspension, network callback, or event-loop re-entry is allowed inside a local database transaction. Retain cooperative socket handling between complete operations; do not add a loop that drains arbitrary work without returning control.

Remove or bypass worker creation and supervision in that experiment; do not fabricate a persistence PID. Benchmarks must record that there is no worker process rather than count the broker twice. Do not install an artificial mutex, queue, or coroutine around each blocking call and label it asynchronous.

Measure the existing concurrent workload and unloaded roundtrip, plus diagnostic event-loop delay during ordinary traffic and checkpoint-producing/FULL traffic. Exercise external lock contention with explicit synchronization. Report actual timer and socket-service delays. Do not turn performance thresholds into deterministic correctness assertions or claim that a parent-loop timeout can interrupt a stuck PDO call. A bounded shutdown guarantee against a stuck native call would require an external supervisor or another execution context.

The decision is **less machinery with global database pauses** versus **more machinery with database-stall isolation**. Neither choice changes the requirement to commit before confirming success. This document does not authorize silently weakening durability to make either variant faster.

## 1. Goal and evidence

When retaining the worker, allow it to receive the next already-requested operation without waiting for the preceding reply to make its full return journey through the parent.

```text
Broker / parent                         Persistent PDO worker
  admit A, B, C
  writer sends A ---------------------> execute A; commit A
  writer sends B ---------------------> reply A
  writer sends C ---------------------> execute B; commit B
  reader receives reply A <------------ reply B
  reader receives reply B <------------ execute C; commit C
  reader receives reply C <------------ reply C
```

This diagram shows logical overlap, not guaranteed timing. The child still executes one operation and transaction at a time. There is one request writer and one independent response reader in the parent.

Baseline attribution reported 0.998 seconds inside child dispatch and 1.894 seconds in parent exchanges, a difference of 0.896 seconds. These were diagnostic lifetime aggregates, including warmup and notifier work, not a measured decomposition of cohort wall time. Pipelining may overlap some of this coordination with useful work; it cannot be assumed to remove it all. No throughput multiplier is promised. [D1]

## 2. Scope and unchanged contracts

The primary change belongs in `SqliteQueueWorker`. The factory may adapt initialization and internal test construction. `SqliteWorkerHandle` keeps ownership of process joining and termination. The existing sequential child runtime, `Queue`, prepared SQL, schema, and PDO configuration should remain unchanged. [R2–R5]

Preserve these behaviors:

- One worker, one connection, one transaction per mutation; replies report committed results, not mere acceptance into memory.
- Existing public Unix-socket protocol, session ownership, receipt token/epoch/expiry validation, queue ordering, binary payload bounds, NORMAL/FULL choice, and worker-local transaction-time clocks.
- Cancellation prevents dispatch; cancellation after dispatch does not revoke an operation. Lost confirmations are not replayed.
- Normal shutdown, fail-closed storage behavior, notification after committed mutations, and identity-safe resource cleanup.

Do not add group commit, shared transactions, message prefetch, worker pools, extra SQLite connections, speculative operations, external-protocol pipelining, automatic retries, auto-restart, priorities, a generic RPC framework, a second journal, or a batch-fill timer. Do not change the notifier algorithm to reduce probe counts in this experiment.

## 3. Parent organization and internal protocol

Use the existing AMPHP process context and channel. The locked `StreamChannel` supports full-duplex read/write. `ProcessContext` uses a separate exit-result channel for joining; its normal operation receiver must still have exactly one owner. Do not copy these dependencies' framing or serialization internals. [R6]

The steady-state parent needs:

| State / responsibility | Requirement |
| --- | --- |
| Waiting requests | One physically bounded FIFO in admission order. |
| Dispatched requests | A bounded ID-indexed collection with expected operation, validation context, deadline, and completion state. |
| Writer | At most one coroutine calls `context->send()` at a time. It does not await responses between sends. |
| Reader | Exactly one coroutine calls `context->receive()`. It validates and resolves responses. |
| Lifecycle | Explicit open, closing, failed, and closed behavior; one shared close outcome. |

Keep typed public operation methods unchanged. One small internal request-entry object and a state enum are reasonable when they simplify shared mutable state. Do not create a separate request/response class hierarchy, scheduler service, registry, or codec for every operation. Use repository naming conventions and existing exception types.

Keep the current wire arrays: requests contain `id`, `op`, and `data`; replies contain `id`, `op`, `status`, and `result` or `error`. Initialization is still request 1. The factory finishes and validates initialization before handing receive ownership to the steady-state reader. [R4, R7]

## 4. Ordering and dispatch identity

Admit valid requests to a FIFO. Concurrent callers have no ordering guarantee before admission; once admitted, preserve FIFO dispatch order among non-cancelled requests. Preserve each session's existing order. Do not skip a large head request to issue a smaller one, or move probes ahead of mutations.

Assign the next wire ID only when the writer is about to attempt the channel send. IDs start at 2 and are contiguous: removing a queued request must not create a gap the child rejects. Publish its pending entry, deadline, and dispatch state before calling `send()`, without a suspension between the final cancellation check and that state transition. The first attempted `send()` is the conservative non-revocable boundary; it does not prove any bytes reached the child.

The existing child executes and replies in request order. The reader must require the next expected response ID and the matching operation, in addition to validating the result. Unknown, duplicate, skipped, and out-of-order IDs are fatal protocol failures. An ID map is for correlation, not permission to accept arbitrary out-of-order replies. No reorder buffer is needed. [R4]

## 5. Limits and backpressure

### 5.1 Experiment limits

Let `P = Limits::MAX_PAYLOAD`, currently 1,040,000 bytes. Retain the baseline total admission count of 128 and its payload-scale cap of `64 × P`. [R3]

| Limit | Proposed value / meaning |
| --- | --- |
| In-flight operation window | **4** normal operations for the first experiment. |
| In-flight payload credit | **2 × P** bytes, reserving possible claim replies as below. |
| Total admitted operation count | **128**, including queued, dispatched, and retained completions. |
| Total admitted payload credit | **64 × P** bytes across those same states. |
| Close control | One shared lifecycle request, sent only after normal in-flight work drains. |

The depth and payload budget are independent limits: four small requests may overlap, but only two maximum-payload reservations fit. These are conservative experiment choices, not discovered optimal settings. Depth 1 must be testable; internal test injection or a committed experimental constant change is sufficient. Do not add a supported CLI, environment, bundle, or benchmark configuration matrix for this.

Charge each admitted operation as follows:

| Operation | Payload credit |
| --- | --- |
| Send | Actual body plus headers length. |
| Claim | **P**, even before its response size is known. |
| Settle / eligibility | Zero payload credit, but still one operation slot and bounded control data. |

Reserve the claim's worst-case reply before dispatch. Otherwise a stream of small claim requests could cause many large buffered replies despite a request-byte cap. Existing public/control-field validation and diagnostic limits remain necessary; zero payload credit never means unbounded metadata.

Both limits must hold before admission and again before moving the FIFO head into the in-flight window. A full pipeline window is normal waiting, not a storage error. Exhausting total admission preserves `StorageCapacityException`; it must not silently replay or endlessly queue ordinary operations. Preserve the notifier's capacity-notification path. [R8]

### 5.2 Ownership of credits and memory

Charge an entry once to total admission; moving it between collections must not double-charge or prematurely release it. In-flight credit is a subset of total credit, not a second retained payload.

Release in-flight credit only when a terminal response is validated **and** the associated send invocation has finished. Release total admission when the public operation consumes/discards its result and the sender no longer retains its request. Cancellation before dispatch removes the entry and its payload immediately. Channel failure stops producers, fails entries, and releases their resources exactly once as the owning coroutines unwind.

Do not leave unlimited cancellation tombstones in a FIFO, retain completed replies in a history map, or keep payload references in detached callbacks after releasing their credits. At the maximum queue size, a simple bounded scan to remove a cancelled entry is preferable to an elaborate linked-list framework.

These are bounds on logical retained payload, not an exact RSS promise: serialization, parser buffers, PHP objects, and socket buffers add overhead. Count active sender data, parsed responses, and completed-but-not-yet-consumed results in the ownership analysis. Once a result is handed back to `Broker`, normal client/session payload bounds apply outside this proxy.

## 6. Writer and reader behavior

### 6.1 Writer

On admission or released credit, wake one coalesced writer. It takes the FIFO head when both in-flight limits allow it, performs the final state/cancellation check, installs its dispatched entry, and sends exactly that request.

After `send()` returns, clear request-data references no longer needed and immediately consider the next already-queued operation. Never await the preceding operation's response merely to send the next request. Never wait for a minimum count or elapsed interval to fill the window.

When the FIFO is empty or credit is unavailable, suspend on state changes. No busy loop, periodic poll, or permanently rescheduled empty callback. Capacity notifications are hints: register/recheck without yielding between checking the predicate and installing the waiter so a release cannot be lost.

### 6.2 Reader

Independently receive one reply, check ID/operation/status and operation-specific result constraints, then complete the matching future. Known domain failures complete only that request exceptionally and continue reading; a normal invalid receipt must not become a channel failure because validation throws inside the shared reader.

Retain the existing precise response validation, including expected queue, receipt identity, payload limits, and positive message IDs. Invalid or unsupported internal responses fail the whole channel. Update state and counters before completing futures or waking the writer, so resumed code observes a consistent state.

Do not execute handlers, public socket writes, SQL, or arbitrary user callbacks in the reader. Completing a future must not wait for the requesting client to consume its network response. Slow clients must not block replies for unrelated requests.

### 6.3 Reply-before-send-return race

The reader may validate a response while the corresponding `send()` call has not yet returned. The pending entry must already exist. A validated terminal response establishes the operation's known outcome and can resolve its caller, but sender resources and credits remain owned until the write completes.

A later write/channel failure must not retract that known result; it fails the still-unresolved requests and retires the channel. Conversely, completing a write is never sufficient to report operation success. Track these two facts separately rather than treating send and receive as one atomic event.

### 6.4 Full-duplex progress

The reader must continue draining responses while the writer is backpressured. The child may be trying to send a large claim response while the parent is sending a large publication. Holding one lock across both directions, or sending the whole window before starting the reader, can deadlock that traffic. Prove progress with maximum-size payloads and constrained buffers; do not rely on kernel buffers always being large enough.

The child remains sequential, including sending each response before executing its next request. Pipelining permits requests to wait in the channel; it does not guarantee continuous child execution under response backpressure.

## 7. Cancellation

| Stage when cancellation is observed | Required behavior |
| --- | --- |
| Before admission | Fail locally; no request, ID, or credits. |
| Queued, before send attempt | Remove from FIFO; release credits and subscription; return the existing client-context cancellation error. |
| During or after send attempt | Do not abort or forget the operation because its caller cancelled. Consume its terminal response or fail the channel. |
| Terminal response validated | Preserve that actual result, even if the session disconnected meanwhile. |

Unsubscribe the caller's cancellation callback at dispatch, or make it an explicit no-op afterward. Public methods await the entry's completion without passing a revocable client token after dispatch. A cancelled queued request never consumes a wire ID; a dispatched request is never removed merely to free space.

For a committed send whose publisher is gone, `Broker` still notifies the queue. For a committed claim whose owner is gone, keep its reservation until expiry; notify readiness changes but do not deliver to the cancelled session. A valid dispatched ACK may commit after disconnect. These preserve the PDO rewrite contract. [R9]

Do not add cancel messages, pre-commit approval, compensating deletes, immediate reservation release, or a second connection to interrupt the worker.

## 8. Deadlines and failure handling

Preserve the current **10-second operation exchange deadline**, measured with monotonic/event-loop time from dispatch attempt. It covers send completion and the validated terminal response, including time queued behind earlier work in the child. Do not restart it when another reply arrives, when the send finishes, or when an entry reaches the head of the child queue. [R3]

A pipeline can therefore time out a request whose own SQL would be fast but which waited behind slow earlier operations. This is intentional for this experiment; do not quietly multiply the deadline by window depth. Waiting before dispatch is not charged to this exchange deadline, matching the existing admission distinction. Explicit client cancellation and closing can still remove queued work.

Use bounded per-dispatched-entry timers, cancelled when both write completion and terminal response are known. If the response is already known but its writer remains stalled, timeout retires the channel without retracting that response. There is no operation deadline when completely idle. Deterministic tests explicitly fire deadline behavior; they must not sleep ten seconds as proof.

On write failure, receive failure, invalid response, worker fatal error, worker death, or any dispatched exchange deadline:

1. Atomically mark the channel failed/closing and prevent all further sends.
2. Fail every unresolved entry and wake all capacity/close waiters; an exception during termination must not leave unrelated callers suspended forever.
3. Force-stop the worker through the existing handle, cancel entry timers/subscriptions, stop writer/reader activity, and complete process cleanup under the existing lifecycle budget.
4. Retain the first useful failure cause, without exposing payloads or creating an unbounded diagnostic history.

Distinguish outcomes: never-dispatched queued work is known not to have executed; dispatched work without a validated terminal response is uncertain; validated terminal outcomes remain known. Later pipelined requests may have executed or committed before an earlier failure was observed in the parent. Do not call all remaining requests rolled back, or replay them to discover their result.

A domain-error response is nonfatal and consumes one response ID normally. A fatal worker response stops the channel. Partial writes and parseable-but-invalid results are not safe-retry cases. No automatic restart, reconnection, or replay.

## 9. Startup, shutdown, and idle lifecycle

Initialization stays a serialized factory exchange, with the current readback and startup cleanup. Only after it succeeds may the steady-state reader start. The reader owns every subsequent operation response, including `Close`; neither callers nor shutdown may start a second receiver. [R5, R7]

`beginClose()` synchronously stops new admission and normal dispatch. Remove queued, never-dispatched requests with a clear closing error. Already-dispatched requests remain tracked and their responses continue to be consumed. Repeated close calls share one close future.

After all normal dispatched requests have terminal responses and the active sender has finished, send one `Close` through the same writer and resolve it through the same reader. It bypasses ordinary admission only as a single lifecycle request after normal work drains. No normal request may follow it. On a valid close response, end the receive loop rather than interpreting the expected subsequent EOF as a new failure.

Use the broker's existing shared shutdown deadline, not a new full budget per pending request, pump, or join. On failure or expiry, force-stop and fail unresolved work. `SqliteWorkerHandle` remains the sole join owner. Stop all scheduled writer callbacks, response reads, timers, and pipe drains; no abandoned referenced watcher may keep the broker alive. Cleanup must not release protected resources as though a child termination failure had succeeded. [R5]

Normal SIGTERM and applicable SIGKILL recovery remain required. Do not reintroduce the previously excluded deliberately SIGSTOPped-worker scenario as a release gate.

## 10. Notifications and eligibility probes

Preserve registration-before-recheck, watch identity, dirty-generation handling, delayed/visibility timers, and existing probe coalescing. Pipelining does not turn WAIT into a claim or make readiness results authoritative. [R8]

Keep commit-triggered notification in the broker path that consumes successful mutation results. A disconnected caller must not suppress it. A stale eligibility reply must not overwrite a newer dirty watch or resurrect a removed watch.

Unsent cancelled work must be removable without consuming wire IDs; already-sent stale probes still have replies that must be consumed. Stale probes, capacity waiters, and abandoned watches must remain bounded during repeated WAIT/disconnect/reconnect cycles. Do not change notifier scheduling or add a polling timer to solve capacity races in this patch. If watch teardown currently leaves capacity waiters alive, a narrowly scoped per-watch cancellation fix is permitted: the admission bound must not merely relocate an unbounded queue into notifier callbacks.

## 11. Deterministic acceptance tests

Reuse current fixtures where possible. Use scripted peers, barriers, explicit cancellation, and controlled deadline firing. Harness timeouts may abort a hang but are not correctness assertions. Do not instrument the production worker with a public test-control protocol.

| Area | Required proof |
| --- | --- |
| Real pipelining | With reply A withheld, a scripted peer receives B without first sending reply A. With depth 1, B cannot be dispatched yet. |
| Window count | Four zero/small-payload operations can be pending; a fifth remains queued until credit is released. |
| Window bytes | Large-send or claim reservations hit the byte limit before the count limit. No head bypass. Releasing credit allows progress. |
| Total admission | Operation and payload caps include queued, dispatched, and retained completions. Saturation follows existing capacity semantics. |
| FIFO and IDs | Cancel a middle queued request; surviving wire IDs remain contiguous and operations preserve admission order. |
| One reader/writer | Detect overlapping same-direction calls. Concurrent send and receive remain permitted. |
| Early response | Deliver a valid reply before its send returns; resolve once, retain write ownership, and clean counters once. Inject a later write failure too. |
| Reply validation | Wrong ID/op, duplicate, out-of-order, invalid result, or fatal response fails all unresolved requests. No future remains stuck. |
| Domain errors | An invalid receipt between two valid operations fails only itself; later valid response processing continues. |
| Cancellation | Test before admission, queued, boundary race, and after dispatch. Post-dispatch cancellation never loses a response or replays a mutation. |
| Individual transactions | Failure/rollback of one operation does not roll back an earlier committed operation. No confirmation before commit. |
| Ambiguous commit | Kill after commit but before confirmation; reopen and inspect state without retry. Distinguish confirmed, uncertain, and never-dispatched requests. |
| Full-duplex backpressure | Large claim reply plus large publication with constrained buffering completes without reader/writer deadlock. |
| Deadlines | Expire one dispatched request while others exist; all unresolved requests fail. Unrelated replies cannot extend its deadline. An idle connection does not time out. |
| Close races | Close during a pending write, several pending replies, a full admission queue, and immediately after a reply. One close message, no later normal sends, one join. |
| Notifications | Committed send after publisher disconnect wakes existing waiters; stale eligibility replies cannot overwrite newer readiness. |
| Resource retention | Repeated full/empty cycles, cancellations, and failures leave no entries, payload references, timers, subscriptions, or active pump work after cleanup. |

Keep existing storage, receipt, binary payload, rollback, clock, protocol, and Messenger tests. Pipelining tests do not replace them. A real-process smoke must exercise the unchanged PDO child, not just a fake channel.

## 12. Implementation and evaluation sequence

**Step A — contracts and tests.** Record the internal limits and implement the scripted-peer proofs for more than one request before the first reply, FIFO cancellation, and independent byte limits. Keep the SQL diff empty.

**Step B — parent pipeline.** Replace the whole-exchange mutex with the bounded FIFO, single writer, single reader, and per-request completion state. Reuse validation. Integrate failure fan-out, deadlines, cancellation, and lifecycle before benchmarking. Prefer extending the existing proxy over extracting a general scheduler.

**Step C — correctness and cleanup.** Run `vendor/bin/castor cs:fix`, then `vendor/bin/castor qa`. Inspect failed reports; do not suppress static-analysis rules or weaken tests to pass. Review the diff for unnecessary abstractions and orphaned stop-and-wait code.

**Step D — matched performance experiment.** Compare committed baseline `23c41b1` and the candidate using the existing 3,000-message concurrent test: three publishers, two stock consumers, 50 ms Doctrine polling, WAL/NORMAL, same payload mix, host, PHP, dependencies, and recording policy; `XDEBUG_MODE=off`. Run no competing QA or benchmark jobs. [D1]

Use three captures per revision in interleaved order, for example baseline/candidate, candidate/baseline, baseline/candidate. Retain every failure and report all runs. Also run the existing roundtrip and idle checks on each revision, and exercise FULL correctness separately. Do not pool modes or substitute one-second polling to shrink the apparent throughput gap.

Report cohort elapsed/rate, operation distributions, integrity, process cleanup, available CPU and memory, and important limitations. Preserve one-message latency: do not trade it away by waiting to fill the window. Optional depth-1 checks on the new pipeline implementation can diagnose dispatcher overhead; they are not a new product mode or an automatic sweep.

If temporary attribution is needed, retain bounded aggregate counters outside the normal benchmark path. **Parent exchange durations now overlap:** their sum, and their sum minus worker time, is no longer the previous serialized-exchange wall-time approximation. Do not manufacture an IPC percentage or speedup by summing overlapping spans.

## 13. Merge decision and deliverables

Deliver one focused patch, deterministic tests, a short contract note, and Git-ignored measurement reports. Update documentation only where responsibility, limits, timeout interpretation, and multiple uncertain in-flight outcomes need explanation. No benchmark rewrite or new dependency is required.

Correctness acceptance requires bounded state, real multi-request overlap, ordered responses, separate commits, no replay, and complete lifecycle cleanup. Performance acceptance is separate: the candidate must show a credible benefit across the matched captures without unacceptable unloaded latency, CPU, or memory regressions. Do not invent a required throughput multiplier after seeing results.

If the advantage is absent or inconsistent, keep the experiment/report and retain the simpler stop-and-wait implementation. If the single-process PDO alternative meets the actual responsiveness requirements with less machinery, prefer that alternative rather than shipping an unused pipeline alongside it.

## Sources and baseline map

**[D1] User-supplied diagnostic:** `Claim SQL simplification and worker attribution`, attachment `Pasted markdown(2).md`; diagnostic `9e7ef97` based on `0f8d2f7`, capture `native-20261008-020500-5233ce7a`. Measurement boundaries and totals appear in its “Temporary diagnostic pass”; before/after limitations in “Uninstrumented before and after”. Raw captures are local/Git-ignored and were not independently rerun for this specification.

**[R1] PR / pinned baseline:**
`https://github.com/ineersa/sqlite-queue/pull/9`
`https://github.com/ineersa/sqlite-queue/tree/23c41b18f6259f226778a9ffffaff2fd36f1e194`

**[R2] PDO storage and unchanged SQL:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteQueueStorage.php`

**[R3] Parent proxy, admission, validation, and exchange deadline:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteQueueWorker.php`

**[R4] Sequential child and ordered IDs:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteWorkerRuntime.php`

**[R5] Lifecycle owner and broker:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteWorkerHandle.php`
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Broker/Broker.php`

**[R6] Locked channel and context implementations:**
`https://github.com/amphp/byte-stream/blob/55a6bd071aec26fa2a3e002618c20c35e3df1b46/src/StreamChannel.php`
`https://github.com/amphp/parallel/blob/37f5b2754fadc229c00f9416bd68fb8d04529a81/src/Context/ProcessContext.php`
`https://github.com/amphp/parallel/blob/37f5b2754fadc229c00f9416bd68fb8d04529a81/src/Context/Internal/AbstractContext.php`

**[R7] Initialization and receive-ownership handover:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Sqlite/SqliteWorkerContextFactory.php`

**[R8] Notifier capacity and stale-result behavior:**
`https://github.com/ineersa/sqlite-queue/blob/23c41b18f6259f226778a9ffffaff2fd36f1e194/src/Broker/QueueNotifier.php`

**[R9] Post-commit broker notifications and cancelled claim delivery:** see [R5], `store()` and `claim()`.

**[E1] Cooperative execution and blocking calls:**
`https://amphp.org/amp`
`https://revolt.run/fibers`

**[E2] SQLite WAL, commit synchronization, and checkpoints:**
`https://www.sqlite.org/wal.html`

**[E3] SQLite busy timeout:**
`https://www.sqlite.org/c3ref/busy_timeout.html`
