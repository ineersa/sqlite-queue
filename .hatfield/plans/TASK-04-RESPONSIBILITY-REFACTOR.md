# Broker responsibility refactor

Status: IMPLEMENTED. Ready for PR review.
Base: merged PR #4, `70bc6cf`.
Branch: `refactor/broker-responsibilities`.
Authority: user-approved decisions in `.hatfield/discussions/broker-responsibilities.md`, with implementation authorized after PR #4 merged.

## Scope

- [x] Rename `BrokerFactory::listen()` to `create()`; move the CLI to `Command\BrokerCommand`.
- [x] Move the retained driver workaround to `Sqlite\SqliteWorkerContextFactory` and `Sqlite\SqliteWorkerHandle`; document its purpose and the driver's sole join responsibility.
- [x] Replace `Ownership` with `BrokerLifetimeLocks`, which only acquires and releases locks. Keep file preparation and socket cleanup in factory/runtime lifecycle code. Make `SocketIdentity` construction private.
- [x] Move exceptions out of the root, use the Exception suffix, and move Delivery to `DTO\DeliveryDTO`.
- [x] Introduce an immutable `ValueObject\QueueName` responsible for queue-name validation.
- [x] Separate queue semantics from `Sqlite\SqliteQueueStorage`, the sole connection owner. Preserve atomic claims, conditional settlement, commit-before-confirmation, and rollback behavior.
- [x] Move session lifetime to the broker. Preserve disconnect fencing across asynchronous storage waits without a session registry in Queue/storage.
- [x] Put operation-specific allowed fields on the Operation enum and centralize protocol field identifiers.
- [x] Replace generic invalidReply failures with specific protocol errors while preserving connection cleanup and no replay. Preserve client reuse for local pre-write validation failures.
- [x] Remove production trace machinery and its dedicated tests/docs. Keep ordinary ready/stopped/failed CLI events.
- [x] Remove the unused internal_storage_failure protocol code.
- [x] Remove testStoppedPersistenceWorkerCannotWedgeShutdown as the user's excluded fault scenario. Retain SIGTERM and SIGKILL coverage and unrelated regression coverage.
- [x] Replace wall-clock correctness assertions with deterministic state/barrier checks. Harness safety timeouts may abort stuck tests, but are not proof of correct behavior.
- [x] Update current documentation, AGENTS conventions, and task status. Record the SIGSTOP scope change honestly, not as a diagnosed/fixed signal bug.
- [x] Run Castor formatting/full QA and independent review.

## Validation

`vendor/bin/castor cs:fix` and `vendor/bin/castor qa` pass on PHP 8.5.10. PHPUnit reports 190 tests and 1,047 assertions. Independent review found no blockers. The engine regression tests preserve transaction and disconnect guarantees, and new checks reject storage ownership borrowed by another fiber or acquired recursively. Storage-failure coverage asserts EOF without an error frame. Deadline and pipe-drain tests trigger cancellation explicitly rather than measure elapsed time.

The changes target a new PR against merged `main`. Public PHP names and engine construction change without compatibility aliases. Protocol v1 framing is unchanged; the unused storage-failure error code is removed.

## Non-goals

No Task 05 implementation, new protocol DTO hierarchy, signal mechanism replacement, heartbeat, OS matrix, or renewed shutdown stress investigation. Do not edit unrelated Hatfield configuration or the historical discussion to suggest decisions were already implemented.
