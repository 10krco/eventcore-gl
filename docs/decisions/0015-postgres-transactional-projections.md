# Run PostgreSQL projections in a fenced destination transaction

## Status

Accepted.

## Context

The transactional runner must persist each event's destination effects and
progress together. The delivery source can be a separate database. A
destination connection may disappear while a projector is active, and another
worker can then take its advisory lock. Application errors can request a fresh
attempt, a skip, a stop, or a fatal exit.

## Decision

Use a separate destination progress table and migration ledger entry. Hold a
named PostgreSQL advisory lock on one checked-out session for the run or reset.
On each attempt, begin a transaction on that session, verify the stored source
and selection identity, check progress, and commit destination changes with the
new position. Roll back before retrying or before starting a separate skip
transaction. Execute after-commit callbacks after PostgreSQL acknowledges the
commit. Treat loss of the commit response as indeterminate.

Pass projectors a transaction handle scoped to the owning BEAM process and the
current attempt. The handle expires at commit and rollback. Reset checks the
same identity and commits the reset callback and progress deletion together.
Reset-and-replay retains the leadership lock across both operations.

## Consequences

A retry can observe progress that advanced after an earlier attempt rolled back.
A later run uses database progress to recover after an indeterminate commit or
an after-commit failure. A same-pool source can use the held session between
destination transactions, so a one-connection pool can serve both roles.
