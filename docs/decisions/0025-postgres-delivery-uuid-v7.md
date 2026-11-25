# ADR 0025. Mint UUIDv7 identities for PostgreSQL delivery

## Context

EventCore 2.1.0 creates UUIDv7 event identities on PostgreSQL append.
Strict delivery exposes the persisted identity to consumers. Earlier Gleam
delivery migrations used UUIDv4, and a consumer may have stored those IDs.

## Decision

Set the delivery column default to RFC UUIDv7. Compose the millisecond
timestamp and version nibble with the variant and random suffix of
PostgreSQL's core random UUID generator. The expression also works on
PostgreSQL versions that precede the native UUIDv7 generator.

Reapply the delivery migration to update an existing column default.
Preserve every existing identity. Rows without a prior identity receive one
when the delivery column is created.

## Consequences

New IDs encode creation time to millisecond precision. The committed
delivery position remains the cursor, including for events generated in the
same millisecond and events with preexisting UUIDv4 identities.
