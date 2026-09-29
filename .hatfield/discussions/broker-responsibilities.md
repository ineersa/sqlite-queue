# Broker responsibilities whiteboard

Date: 2026-09-29
Code discussed: PR #4, commit `302a70d`.
Mode: discussion only. This log does not authorize implementation changes or approve the PR.

## Questions

1. User: "I checked code a bit and have not full understanding of responsibilities, what components are responsible for what."
2. User: "Okay, so basically Broker is what? A supervisor?"
3. User asks why `BrokerFactory` calls its creation method `listen()` and why it is a factory rather than a builder.
   - PR comment on `src/Broker/BrokerFactory.php:45`: "logically that should be create() not listen()".
4. User asks why `BrokerPaths` is not a static helper in a utilities namespace.
5. User considers `SocketIdentity` a helper/utility rather than a broker component.
6. User finds the roles of `PersistenceFactory` and `Persistence` unclear and their names poor.

## Explanation so far

The message path is `Client → Frame over Unix socket → Broker → Queue → SQLite driver → worker process → database`.

- `Client` handles application requests, response validation, and its socket. It does not access SQLite.
- `Frame` handles wire encoding, decoding, and framing limits, not queue semantics.
- `Broker` is the queue server and runtime coordinator. It accepts clients, dispatches operations, associates sockets with queue sessions, and coordinates worker monitoring and shutdown. Supervision is one responsibility, not its whole role. It does not automatically restart the worker.
- `Queue` implements transactions, delayed availability, delivery claims, visibility expiry, and receipt fencing. It owns the SQLite connection.
- `Command` handles CLI options, OS signal registration, readiness output, and optional diagnostics.
- `BrokerFactory` acquires resources and constructs an initialized broker. It cleans up acquired resources when startup fails.
- `Ownership` holds database/endpoint locks and manages owned socket cleanup.
- `BrokerPaths` is a readonly pair of canonical database/endpoint paths plus broker-specific validation. Its policy includes private directories, effective-user ownership, endpoint length, and path conflicts. It is not a general path utility.
- `SocketIdentity` stores the device/inode identity captured for a socket and compares it during cleanup. It is a stateful value object, not just a bag of static functions. It currently serves broker socket ownership.
- `PersistenceFactory` implements Amp's `ContextFactory`. The SQLite connector invokes it to start the driver's existing worker through Amp. It retains a lifecycle handle while returning the process context to the driver.
- `Persistence` is that lifecycle handle: process ID, output-pipe draining, exit observation, and force-stop capability. It neither stores messages nor executes SQL. The driver remains responsible for joining its context.

## Naming and design positions offered for discussion

These are assistant recommendations, not agreed implementation decisions.

- `BrokerFactory::create()` describes the public operation more clearly than `listen()`. Socket listening is one startup effect; the method also acquires locks and initializes storage. Serving clients begins in `Broker::run()`.
- A factory fits the current operation: configuration is already supplied, and one call acquires resources and returns a ready object. A builder would be justified by a concrete need for incremental configuration or construction variants. It would still need the same acquisition and failure cleanup.
- Keep broker-specific path policy near the broker rather than hiding it under a generic utilities namespace.
- `SocketIdentity` could move to a filesystem namespace if other components need that abstraction. Being small or exposing static construction does not by itself make a class a generic utility.
- Suggested clearer names: `PersistenceFactory` → `SqliteWorkerContextFactory`; `Persistence` → `SqliteWorkerHandle`. The first describes the driver integration point, the second the worker lifecycle handle.

## Follow-up: agreed names and worker responsibility

- User accepts `BrokerFactory` and requests renaming `listen()` to the proper creation name, discussed as `create()`.
- User accepts keeping `SocketIdentity` and `BrokerPaths` under `Broker`.
- User requests `Broker\Command` become `Command\BrokerCommand`.
- User asks whether the two proposed worker classes are a SQLite connection and its connection factory, and why they belong under `Broker`.
- Clarification: the vendor's `SqliteConnector` creates the SQL connection. Our `ContextFactory` implementation instead participates in creating its child process, retaining a separate lifecycle handle. The handle exposes process observation and force-stop, not SQL operations. No second database connection is created by this pair.
- The current `Broker` placement groups the worker integration with its consumer, but is not a technical requirement. The later decision below moves this integration to `Sqlite`; implementation is deferred.

## Remaining questions and limits

### Does the SQLite driver already handle worker shutdown?

- User asks whether the native async SQLite dependency already solves termination of a stuck worker.
- Source inspected: `vendor/fabpot/amphp-sqlite3/src/Internal/Connection.php`, `close()` at lines 180–215 and private `forceClose()` at lines 899–924.
- The driver does handle normal shutdown and force-closes when operations, leases, or transaction locks are active. It is incorrect to describe the driver as lacking worker cleanup altogether.
- The specific gap is idle graceful shutdown: `close()` marks the connection closed, sends the worker a close request, then receives and joins without cancellation or a timeout. An unresponsive worker can leave that path waiting. A second `close()` returns immediately because the closed flag is already set. The private force-close method is not a public escape hatch.
- The broker's retained process handle is a workaround for that gap in the locked dependency. A driver-level bounded close or abort API would be the more natural place for this responsibility; no upstream change is assumed or authorized here.

- The worker-class names and namespace are recorded in the decisions below; implementation is deferred.
- No production code has been changed during this discussion.
- The intermittent SIGTERM hang remains unresolved. The five-second deadline depends on event-loop progress and starts at `stop()`, not kernel signal delivery.
- Deferred diagnostic writes protect resource cleanup but do not reveal live milestones from a hung process. Passing cleanup tests does not prove reliable signal delivery to `stop()`.

## Can the worker handle be removed?

- User asks: "Can we remove that handle? what will change?"
- Removing the wrapper class alone is different from removing the independent worker-supervision capability. Moving its process context, pipe drains, and cancellation into Broker would only relocate the same responsibilities.
- Removing the capability and using the driver alone should preserve normal queue operations and responsive graceful shutdown, but this has not been tested as a modified implementation.
- The current handle supplies independent force-stop, pipe-based exit observation, pipe-drain cleanup, and the worker PID. Those uses need removal or replacement, not just a deleted constructor argument.
- In the stalled graceful-close case, a timeout on the caller's await does not terminate the child. Without an independent termination mechanism, releasing broker ownership could leave the old worker alive with database access; merely waiting instead can leave shutdown unbounded.
- Alternatives discussed were driver-provided bounded abort/close, an external supervisor responsible for terminating the whole process tree, or an explicitly weaker broker shutdown contract. The user subsequently chose to retain the current workaround.
- Removing the handle does not address the unresolved SIGTERM path before stop(). No code changes are authorized by this question.

## Decisions recorded, not implemented

- Keep the independent worker handle as a workaround until the driver offers suitable bounded close/abort support.
- Move `Broker\PersistenceFactory` to `Sqlite\SqliteWorkerContextFactory`.
- Move `Broker\Persistence` to `Sqlite\SqliteWorkerHandle`.
- Document why this integration exists: the locked driver's idle graceful close can wait indefinitely after setting its closed flag, so another close cannot interrupt that wait.
- Document that the handle observes and terminates the same worker used by the driver. It does not create a second database connection or perform SQL operations. Queue owns the connection; the driver remains responsible for joining the context.
- Rename `BrokerFactory::listen()` to `create()`.
- Move `Broker\Command` to `Command\BrokerCommand`.
- Keep `BrokerPaths` and `SocketIdentity` in the `Broker` namespace.

The user explicitly clarified that this remains whiteboard defense mode: record decisions, do not implement them. An implementation delegation was cancelled before code changes. Working-tree inspection confirmed no production or test modifications. The unresolved SIGTERM issue is separate from these naming and organization decisions.

## What does Ownership mean?

- User: "Ownership I don't understand that concept fully".
- Here ownership means a broker's exclusive right to use a database path and a socket endpoint for its lifetime. It is distinct from PHP object ownership, message reservation ownership, and Unix file ownership, though the class also validates Unix permissions.
- The class holds two non-blocking Symfony FlockStore locks, keyed by canonical database and endpoint paths. Same database with another endpoint conflicts; another database with the same endpoint also conflicts. These are application-level lifetime locks, not SQLite transaction locks.
- This coordination is advisory: cooperating brokers respect it, but it does not stop an unrelated SQLite program from opening the database.
- After binding, `recordSocket()` captures the socket's device/inode and restricts its permissions. Cleanup removes the endpoint only if its identity still matches, releases both locks, and keeps the database file.
- Kernel file locks are released when the owning process exits, but a crash can leave the socket path behind. The current implementation refuses existing endpoints rather than automatically deleting them.
- Relevant source: `src/Broker/Ownership.php`, especially its constructor, `recordSocket()`, `close()`, and `acquire()`.

## Ownership naming follow-up

- User: "So it's pretty much application level locks for lifetime, ownership is really bad name".
- Agreed conceptual description: application-level database and endpoint locks held for the broker lifetime.
- User rejects `Ownership` as the class name. Proposed replacement: `BrokerLifetimeLocks`; the exact replacement is not yet approved.
- The current class also validates file permissions and manages identity-checked socket cleanup. Those responsibilities must remain explicit in its documentation if retained together; renaming does not remove them.
- Discussion only. No implementation changes.

## Separate locks from filesystem lifecycle

- User agrees that the class should not combine unrelated responsibilities merely to preserve the existing implementation: "if class breaks single responsibility principle it shoudln't do that".
- Decision: narrow the lifetime-lock component to acquiring, retaining, and releasing the database and endpoint locks. It should not create database files, change permissions, or delete socket paths.
- Proposed allocation: retain broker path validation in `BrokerPaths`; keep socket identity comparison in `SocketIdentity`; put file preparation and identity-checked socket cleanup with resource startup/lifecycle handling. `BrokerFactory` coordinates acquisition and rollback rather than the lock class taking responsibility for unrelated resources.
- The exact socket-lifecycle API and final lock-class name remain to be settled. Splitting responsibilities does not require creating a class for every method.
- Preserve the cleanup ordering and failure behavior: resource cleanup must not lose lock protection prematurely, and a socket-removal failure must not skip the remaining cleanup attempts.
- This is a recorded design decision, not implementation authorization. No production or test code changed.

## Resource lifecycle boundary accepted

- User explicitly accepts file preparation and socket removal as resource startup/cleanup responsibilities coordinated by `BrokerFactory` and the running `Broker`.
- Agreed boundary: the lifetime-lock component manages locks only; `BrokerFactory` coordinates resource preparation and startup rollback; the running `Broker` coordinates shutdown, including identity-checked socket removal and lock release.
- `BrokerPaths` retains path validation, and `SocketIdentity` retains socket identity capture/comparison. This agreement does not introduce a new resource-manager abstraction.
- Discussion only. Implementation remains deferred.

## Exception naming and namespaces

- User preference: every exception class must have the `Exception` suffix. Exceptions should live in a dedicated exception namespace or their relevant component namespace, not the package root.
- `InvalidReceipt` must become `InvalidReceiptException`.
- Place `ProtocolException` in `Protocol`, alongside the wire protocol and `ErrorCode` it uses.
- Proposed placement for the other two classes: `Exception\InvalidReceiptException` and `Exception\TransportException`. This avoids creating additional component namespaces solely for these exceptions. These exact placements remain proposals.
- Record the suffix and placement preference as a project convention for later implementation. Exception behavior and inheritance are not being changed by this discussion.
- No source or AGENTS.md changes now; the whiteboard log is the only edited file.

## Delivery DTO naming and placement

- User identifies `Delivery` as a DTO and requests the `DTO` namespace and `DTO` suffix.
- Decision: rename `Ineersa\SqliteQueue\Delivery` to `Ineersa\SqliteQueue\DTO\DeliveryDTO`, located at `src/DTO/DeliveryDTO.php`.
- This reorganizes the existing delivery object; it does not introduce a new DTO hierarchy or change its behavior.
- Recorded only. Implementation remains deferred during whiteboard defense.

## Other PR comments reviewed

- User asks whether the other PR comments have been read and whether there are objections.
- Retrieved all inline comments, issue comments, and reviews for PR #4. The latest additional inline comments request named diagnostic events, restricted construction of `SocketIdentity`, and operation-specific allowed fields expressed through constants/enums and enum methods.
- Diagnostic event names should use shared named constants or a backed enum rather than repeated string literals. This preference includes tracing, not only public protocol operations.
- A private `SocketIdentity` constructor fits construction through `fromEndpoint()`, which captures actual socket metadata. No production need for arbitrary device/inode construction has been established in this discussion.
- Proposed organization: `Operation::allowedFields()` owns the operation-specific field rules now matched inside `Broker::validateRequest()`. Shared field identifiers can be centralized without restoring the rejected request/response DTO hierarchy.
- Earlier `string|array` comment has an external API constraint: Amp's `ContextFactory::start()` requires that signature; narrowing it to string would violate the interface. It represents a script path or script path plus arguments.
- Earlier Symfony Finder suggestion is not a replacement for native device/inode/owner metadata checks. Symfony filesystem/path/lock facilities can handle their supported operations while the necessary stat checks stay explicit.
- No implementation changes or PR replies made during this review of the comments.

## Frame construction

- User asks whether `Frame::__construct()` should be private with construction through `read()`.
- Current behavior has two paths: `read()` parses incoming bytes, while Client and Broker construct outgoing frames from local control/body/header data and then call `encode()`.
- Unlike `SocketIdentity::fromEndpoint()`, socket reading is not required to obtain valid source data for every frame. A read-only construction API would leave outgoing frames without a construction path.
- A private constructor with a separate `Frame::create(...)` named constructor is possible if explicit named construction is preferred. That alone would not change validation: framing checks currently occur in read/encode, and operation-specific checks live in Client/Broker.
- User acknowledged the incoming/outgoing construction explanation: "Okay, got it". No constructor visibility change was requested after that explanation; this is not approval of a new `create()` API. No code changes.

## External API and filesystem checks accepted

- User accepts retaining `ContextFactory::start(string|array ...)` because Amp's interface requires it.
- User accepts native filesystem metadata checks for device/inode/ownership because Symfony Finder does not replace them.
- These qualifications are settled. No implementation changes.

## Tracing, unused error code, and deterministic tests

- User asks whether the buffered production trace should be removed in favor of external tracing when needed. It cannot diagnose the unresolved hang because it writes only after cleanup. It can describe completed shutdowns, but no separate requirement for that feature has been established.
- Assistant recommendation: remove `--trace-file`, its milestone buffers/observer plumbing, and trace-only tests/documentation. Preserve normal CLI readiness/stopped/failed output. Use external tracing of the real command for signal delivery and actual select timeout values. This recommendation does not imply the SIGTERM issue is fixed.
- External tracing can change scheduling. A passing traced run does not establish absence of the intermittent failure. Add a test-only live diagnostic channel only if a specific unanswered question justifies it.
- User agrees to remove `internal_storage_failure` from v1, including the enum case, client mapping, and documentation, rather than reserve an unused error code.
- User requires deterministic tests: "tests MUST be deterministic, zero tolerance for any timing related checks".
- Replace elapsed-time correctness assertions, including the storage-failure test's less-than-three-seconds check, with explicit observable behavior, controlled state transitions, and positive synchronization. Assert that fatal storage failure does not attempt an error-frame write and that resources are released.
- Distinguish harness safety timeouts from correctness assertions: a timeout may abort a hung test, but an elapsed-time threshold must not serve as the proof of correct behavior. Deadline behavior should use controlled scheduling/time or explicit cancellation rather than machine-speed assumptions.
- Whiteboard discussion only. No source or test changes.

## PHP extension requirements

- User asks whether Amp-specific PHP extensions are needed.
- No Amp-specific extension is required. Amp uses PHP's built-in fibers, and Revolt falls back to its stream-select driver when optional event-loop extensions are absent.
- For this foreground broker: PHP 8.5+, `ext-sqlite3`, `ext-pcntl`, and `ext-posix`. `ext-pdo_sqlite` is needed for the Doctrine benchmark, not for the broker's SQLite driver.
- Optional Revolt backends include `ext-uv`, `ext-ev`, and `ext-event`. Installing one changes the event-loop implementation, but is not evidence of a fix for the unresolved SIGTERM issue.
- Sources checked: root `composer.json` and `vendor/revolt/event-loop/src/EventLoop/DriverFactory.php`.

## Queue-name validity and the Queue abstraction

- User questions `Queue::receive(string $queue, string $session)` calling `validateQueue()`: invalid queues should not be constructible, rather than being checked on each operation.
- Current source: `Queue` is a multi-queue storage engine. Its constructor takes a connection, not a queue name; `send()` and `receive()` accept arbitrary strings and validate them. It is not an object representing one named queue.
- Under the current string API, removing validation would make direct engine calls unsafe. The user's requested invariant requires changing the model, not merely deleting the guard.
- Proposed smallest change for the current multi-queue engine: an immutable `QueueName` value object validates its name on construction; operations accept `QueueName`, and the broker converts untrusted wire strings at the boundary. Direct engine callers must construct the same validated value.
- Alternative conceptual model, not selected: make a `Queue` instance represent one named queue and separate the multi-queue storage engine. The user subsequently clarified that the accepted solution is `QueueName` owning validation; no per-name Queue instance redesign was approved.
- No new type, API change, or validation removal has been implemented. Discussion only.

## Queue session bookkeeping

- User clarified that "Yes that's proper solution" means `QueueName` owns name validation. Decision: use a validated immutable queue-name value object rather than repeating name validation in queue operations. The objection to `Queue::$sessions` is a separate responsibility concern.
- Decision: client-session creation, active membership, and disconnect lifecycle do not belong in queue storage. The broker already manages connections and should own their associated lifetime state.
- Distinguish that lifecycle state from persisted delivery ownership: storage still needs an owner identifier to fence receipt settlement atomically with the reservation token, broker epoch, and visibility deadline. Removing the session registry must not remove the SQL owner predicate.
- Current `Queue::receive()` checks session activity before claiming and after the transaction; `settle()` checks before work and after DELETE inside the transaction. These checks protect behavior when disconnect occurs during an asynchronous wait. A single check in Broker before calling storage would not preserve that behavior.
- Proposed boundary: Broker owns connection/session lifetime and cancellation; storage receives the owner identity and an operation cancellation mechanism, not a global registry of live clients. Cancellation must be observed at the relevant transaction boundaries, with rollback before commit where possible. A committed claim whose client disconnects remains reserved until expiry.
- No new session manager, public API, or implementation change has been selected or applied. Discussion only.

## Separate queue semantics from SQLite persistence

- User explicitly requires two concepts instead of the current mixed `Queue` class: one for queue/message logic and one for SQLite queries, transactions, and connection handling.
- Decision: `Queue` handles the application operations and their semantics, without SQL statements, schema setup, SQLite transaction calls, or connection ownership.
- Proposed storage class name: `Sqlite\SqliteQueueStorage`. It owns the SQLite connection, schema/durability setup, SQL execution, transaction/rollback handling, and persistence-operation serialization. The exact class name and API remain proposals.
- This decision supersedes the earlier target design in which Queue itself owns the SQL connection. That remains a description of current code, not the newly agreed destination. Resource shutdown stays coordinated by the broker lifecycle; storage becomes the sole connection owner.
- Queue logic supplies policy inputs such as queue identity, current time, delivery deadlines, and receipt ownership/token data. Storage implements atomic persistence operations using those inputs and returns results for queue logic to interpret.
- Preserve atomicity: selecting and reserving a message must remain one storage operation/transaction. Receipt-owner/token/epoch/expiry checks must remain part of the conditional settlement write, not merely an earlier check in Queue. Relevant domain constraints must still be expressed in SQL predicates for concurrency safety.
- The split is not a second generic SQL driver or a generic execute(sql) wrapper; the vendor driver already provides that. A queue-specific persistence API is the proposed boundary.
- No implementation, new interface hierarchy, or public method signatures have been approved or added during this discussion.

## Queue/storage separation accepted

- User confirms the proposed separation: "yes that looks better".
- Accepted direction: `Broker → Queue → SqliteQueueStorage → vendor SQLite driver`, with queue semantics separate from SQLite-specific persistence and connection ownership.
- Storage operations must preserve atomic claims and conditional receipt settlement. The split must not move those checks outside their database transaction/write.
- Detailed method signatures remain to be designed. Implementation remains deferred during whiteboard defense.

## Client invalidReply failure path

- User asks why `Client::invalidReply(): never` closes the client and throws, and whether it is a hack.
- Source inspected: `src/Client.php`, especially `exchange()`, operation-specific response validation, and the scalar-reading helpers.
- This is the shared fatal-response path, not a predicate. Operation-specific validation after `exchange()` detects invalid result types, unexpected payloads, invalid IDs, or a mismatched queue. The helper closes the connection and throws so none of those paths accidentally continue using a peer that violated the response contract.
- `never` is PHP's native return type for a function that does not return normally. Here it always throws; it is not an infinite loop. It also lets static analysis know that a failed type guard cannot fall through.
- The outcome may be unknown because the request has already been sent: the broker could commit a send or acknowledgement before returning an invalid confirmation. Closing the client does not roll back that operation, and automatic replay would be unsafe.
- Contrast with local encoding failure: before a write there is no remote ambiguity, and the connection is preserved under the recently corrected behavior.
- The fail-closed policy is reasonable, but `invalidReply` hides its throwing/closing effect and its generic exception loses the specific validation reason. Proposed clearer naming is `failInvalidResponse(...)`, with a reason if useful; no rename or API change has been approved.
- Discussion only. No code changes.

## How could the broker return an invalid confirmation?

- User asks how an invalid confirmation could occur.
- This is a defensive hypothetical, not an observed defect in the current broker. A correct, compatible broker should produce valid responses.
- Possible causes include a broker/serialization regression, incompatible protocol implementations, or connecting to a different/broken peer. A reply can be valid JSON and correctly framed yet have an invalid operation result, such as a string where an integer message ID is required.
- Reliable Unix sockets do not normally corrupt an integer into a string; partial reads are handled by framing. EOF, timeout, and truncation are separate transport/framing failure paths.
- Client tests deliberately use fake peers that return malformed replies to verify rejection and connection closure. Those tests do not establish that the real broker currently emits those replies.
- If an operation was sent and the response is malformed, the client cannot infer whether it committed. The earlier committed-send example illustrates that uncertainty, not a normal expected broker behavior.

## Classify malformed responses precisely

- User objects that a malformed response is a protocol/validation failure, not an unexplained "invalid reply", and rejects vague diagnostics that hide what failed.
- Agreed direction: identify the violated protocol constraint and report a specific reason, such as an integer message ID being required but a string received. Do not discard that information behind a generic `invalidReply()` message.
- Assistant recommendation: classify malformed remote responses as protocol failures while preserving connection invalidation and no automatic replay. The exact exception hierarchy and relationship to the existing transport-exception contract remain to be designed.
- Distinguish known response-validation failure from uncertainty about an earlier remote commit. We can know exactly which field violates the contract without knowing whether the broker committed before returning that response. Even a correct protocol cannot eliminate commit/confirmation ambiguity when the peer dies or a connection is lost.
- Local validation before sending remains different: the operation is known not to have reached the broker.
- Discussion only. No exception behavior or implementation changed.

## Remove the generic invalidReply helper

- User asks whether the generic `Client::invalidReply()` helper can be removed.
- Agreed direction: remove that helper and replace its call sites with specific protocol-validation failures that identify the violated constraint. Do not merely rename the same generic failure path.
- Retain response validation, connection invalidation on malformed remote responses, and no automatic replay. Removing the helper does not remove the uncertainty of a previously sent operation's commit outcome.
- Connection cleanup must still cover validation failures that currently occur after `exchange()` returns; exception propagation and cleanup boundaries need to be designed together.
- This remains a recorded whiteboard decision, not authorization to edit production code now.

## Use plain language for the shutdown hang

- User asks what "SIGTERM wedge" means.
- It means the broker sometimes hangs instead of shutting down after SIGTERM. It is not a named Amp feature, a diagnosed cause, or proof of a deadlock.
- In the observed test failures, the persistence worker was deliberately paused with SIGSTOP, SIGTERM was sent to the broker, and the broker remained alive when the test's 15-second safety timeout expired. The worker was still paused and the socket path remained.
- Use "intermittent shutdown hang" rather than "wedge" in explanations. The root cause remains unresolved; this terminology clarification does not change the implementation or evidence.

## Does the worker handle work?

- User asks whether the observed shutdown hang means `Persistence` does not work.
- Distinguish the worker handle from a signal handler. `Persistence::forceStop()` terminates the process when invoked; it does not receive SIGTERM or independently schedule its own invocation. Broker's event-loop deadline calls it.
- Focused tests provide evidence that explicit force-stop terminates a worker and that drain cancellation releases pipe-read watchers. They do not prove that signal-driven shutdown always reaches escalation.
- The integrated shutdown path is not reliable enough to claim complete protection. A stopped worker still present after the test timeout means termination did not take effect, but the available failing captures do not prove whether escalation was never invoked or failed to take effect.
- The handle is an independent way to terminate the child relative to the SQL connection, not an independently running watchdog. Its invocation still depends on Broker and event-loop progress.
- No implementation change or root-cause conclusion follows from this distinction. Discussion only.

## Would periodic worker checks replace the handle?

- User asks whether to remove/rework the current mechanism and periodically ping the worker instead.
- This is a proposal under discussion, not authorization to remove the previously retained workaround or implement a heartbeat.
- A process-existence check does not prove responsiveness: a SIGSTOPped worker still exists. A request/response ping could detect lack of response, but requires a deadline and still needs a termination capability if it fails.
- A ping timer and timeout running on the same broker event loop do not provide independent protection against that event loop failing to dispatch signals or timers. The existing one-second repeat timer has not explained the observed shutdown failure; adding traffic could mask it rather than identify it.
- A worker executing serialized SQLite work may not answer a ping promptly even when functioning correctly, so a heartbeat policy would need to distinguish legitimate work from an unresponsive worker.
- The known failure establishes that end-to-end signal-driven shutdown is unreliable, not that the worker handle's force-stop implementation is the failing component. Rework should follow evidence of the failing boundary.
- If protection against a stalled broker itself is required, an external watchdog/supervisor must manage the whole broker/worker process tree. That is a distinct operational requirement, not a consequence of adding a local timer.
- Discussion only. No code changes or new shutdown policy chosen.

## Why is the worker paused?

- User asks how the worker could be paused.
- In the reported failure scenario, the test deliberately sends `SIGSTOP` to the SQLite worker process. The operating system suspends its execution; it cannot process SQL or a graceful-close request. `SIGCONT` resumes it, and `SIGKILL` can terminate it while stopped.
- This is explicit fault injection to test shutdown with an unresponsive worker. It is not normal broker behavior, an Amp feature, or evidence that production workers spontaneously pause.
- The unexplained behavior is the broker failing to complete shutdown after SIGTERM under that injected condition, not why the worker entered the stopped state.

## Real-world relevance of a stopped worker

- User asks how this could occur outside tests.
- A literal stopped process normally requires an external action such as an operator sending SIGSTOP, a debugger stopping execution, or applicable job-control signals. No normal broker/SQLite code path has been identified that spontaneously puts its worker in that state.
- A worker can also become unresponsive through slow/stalled I/O, long-running work, or a defect. These are different failure modes, not evidence that workers naturally enter SIGSTOP, and the test does not establish their production occurrence or model all of them faithfully.
- The known reproduction is fault injection, not an observed production incident. A broader signal-dispatch defect is possible but remains unproven.
- Distinguish the chosen fault-tolerance requirement from the implementation evidence. The discussion must not present an artificially paused worker as a common or demonstrated production event.

## Does SIGTERM work?

- User asks: "sigterm works?"
- Normal SIGTERM shutdown has passed real-process tests, including broker exit, worker cleanup, and socket removal.
- The outstanding intermittent failure has been reproduced with the worker deliberately stopped by SIGSTOP. It has not been established as a failure during ordinary queue usage.
- Passing normal tests does not guarantee every signal delivery or shutdown under all conditions. No new test run or implementation change was performed for this clarification.

## Exclude the stopped-worker shutdown scenario

- User explicitly chooses to remove `testStoppedPersistenceWorkerCannotWedgeShutdown` and not require shutdown with a deliberately SIGSTOPped worker as a release condition. Their required operational cases are SIGTERM and SIGKILL.
- Decision for implementation: remove that test and stop treating its intermittent failure as the Task 04 merge blocker. Update current status/requirements documents accordingly, retaining an accurate historical record rather than claiming the failure was fixed.
- Preserve normal SIGTERM shutdown coverage and applicable SIGKILL failure/recovery coverage. Removing this fault-injection scenario does not establish stronger reliability than those tests demonstrate.
- This decision does not by itself remove the worker handle, change signal handling, or authorize deletion of unrelated tests. Earlier naming, responsibility, and deterministic-test decisions still apply.
- Whiteboard mode remains active: this is recorded for later implementation. No source, tests, or plan status files changed now.
