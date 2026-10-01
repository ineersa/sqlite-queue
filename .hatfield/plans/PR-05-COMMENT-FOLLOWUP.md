# PR #5 comment follow-up

Status: DONE. Implemented on `main` after PR #5 merged, as requested.

## Completed changes

- Moved `Command\BrokerEvent` to `Broker\BrokerEventEnum`. The command and broker readiness event share it. JSON field keys remain literal.
- Added distinct concrete exceptions and wire codes for malformed receipts, no active reservation, owner mismatch, epoch mismatch, token mismatch, and expiry. `InvalidReceiptException` remains their abstract catchable base. The generic `stale_receipt` code is removed from the unreleased API.
- Diagnosed zero-row settlement DELETEs inside the same immediate transaction with the same clock sample. Precedence is no active reservation, owner, epoch, token, then expiry. A zero-row DELETE with all predicates matching is a storage invariant failure.
- Separated client-lifetime cancellation into `ClientContextClosedException`. Broker cancellation ends the affected session without marking normal shutdown as a storage failure.
- Explained commit-before-confirmation loss and the no-replay rule in `TransportException` and the protocol reference.
- Documented foreground operation under an external supervisor, process-tree cleanup, and explicit client reconnect. Fatal storage failure still stops the broker. No transparent storage reconnect, supervisor configuration, or relaxed socket-ownership checks were added.
- Removed `SqliteQueueStorage::fromConnection()` and updated callers and documentation to use the constructor.
- Explained that private `__clone()` prevents sharing the owned connection and mutex with duplicated lifecycle state, not multiple independent instances.

## Validation

`composer install`, `vendor/bin/castor cs:fix`, and `vendor/bin/castor qa` pass on PHP 8.5.10. PHPUnit reports 240 tests and 1,325 assertions.

Coverage includes receipt syntax and integer overflow, each reservation mismatch for acknowledgement and rejection, deterministic mismatch precedence, absent and unreserved rows, storage invariant failures, all six broker/client wire mappings, continued client usability, and cancellation during a real broker claim. The transaction-acquisition, recursive-close, and gated-writer regressions from PR #5 remain intact.
