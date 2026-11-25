# ADR 0027. Replay PostgreSQL release projections by event UUID

## Context

EventCore 2.1.0 legacy PostgreSQL readers page through persisted event UUIDs
and delegate prefix and pattern matching to PostgreSQL. A UUID checkpoint
resumes that sequence. EventCore GL's committed positions represent a
different order and support existing readers and strict delivery.

## Decision

Expose a release backend that orders by UUID and filters with PostgreSQL
LIKE or an anchored regex before applying the row limit. Use a separate
UUID checkpoint table and the existing name-based advisory coordinator.
Use the existing delivery migration to supply identities to older rows.

## Consequences

Applications choose and migrate the release backend before starting a new
UUID-based projection. Existing progress continues to refer to committed
positions, and resetting release progress targets the UUID table. Native
delivery identities and historical backfill follow ADR 0013.
