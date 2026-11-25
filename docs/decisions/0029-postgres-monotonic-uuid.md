# Allocate monotonic PostgreSQL delivery UUIDs

## Context

EventCore 2.1.0 compares UUIDs when resuming a legacy PostgreSQL reader. Its
UUIDv7 allocator increments a counter within a millisecond. Independently
random UUID tails let later inserts in the same millisecond sort before a
saved cursor.

## Decision

Allocate UUIDv7 values through a database-local clock row in the append
transaction. Keep the timestamp at or above the last allocated millisecond and
advance the 42-bit sequence when multiple events share it. If the sequence
wraps, advance the logical timestamp. Retain the RFC variant and random low
bits. Initialize the clock
once from the greatest indexed UUID, including legacy v4 identities. Backfill
missing values in committed position order.

## Consequences

Independent pools append in a shared increasing UUID order. Existing UUIDs
and checkpoints keep their meaning. A failed append rolls back the counter
with the event. Committed-position readers keep using their existing order.
