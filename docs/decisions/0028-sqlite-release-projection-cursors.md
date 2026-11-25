# ADR 0028. Replay SQLite release projections by event UUID

## Context

EventCore 2.1.0 SQLite legacy replay orders text UUIDs and uses SQLite's
LIKE and GLOB operators. An application upgrading an existing EventCore GL
database can already have committed-position checkpoints.

## Decision

Provide an explicit release migration that assigns UUIDs to events in a
side table. Backfill older events in insertion order. Assign new IDs
through an insert trigger in the same transaction as the event. Reserve
42 bits of the UUID for ordering events written in the same millisecond.
Clamp the clock to the last persisted UUID timestamp and advance it if the
42-bit position segment wraps.
Use SQLite's native predicates, then limit UUID rows before decoding.
Store release checkpoint UUIDs in a separate table on the chosen
checkpoint connection.

## Consequences

Existing event records and committed checkpoints retain their positions.
Release readers resume across reopen and reset only their own progress.
The release migration adds one UUID index entry per stored event.
A page containing only incompatible payloads returns no deliveries and
retains its cursor, matching the published reader.
