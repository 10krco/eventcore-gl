# Define the PostgreSQL projection API boundary

## Context

The transactional projection runner needs to let callbacks query the same
PostgreSQL session that advances their cursor. Its public transaction alias
and cursor errors exposed internal modules, which left consumers depending on
implementation types to describe callbacks or handle failures.

## Decision

Expose an opaque `eventcore/postgres_projection.Transaction`. The runner
provides it to apply and reset callbacks and resolves it in `execute`. Expired
scopes still return `TransactionClosed`. Keep session and transaction control
inside the runner.

Expose `ProgressError` in the same module for failures reading, advancing, or
deleting a cursor. Preserve all three failure cases and the underlying Pog
query error. Map the internal failures at the runner boundary.

## Consequences

Consumers can use and match the public types without importing internal
modules. Locking, rollback, commit, and cursor storage use the existing
implementation, and existing databases require no migration for this change.
