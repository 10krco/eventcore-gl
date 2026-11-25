# ADR 0012. Read SQLite projections in committed insertion order

## Context

SQLite inserts an event's global position within the transaction that writes
the event. Reopening a database must preserve the position associated with
each legacy projector checkpoint.

## Decision

Read ascending position ranges through a maximum position captured at the
start of each page. Apply filters before the page limit and decode the payload
of each selected event. Reuse the shared bounded scanner so non-matching
events advance the scanned cursor. Store named checkpoints in either the event
database or a separately opened checkpoint database. The checkpoint handle's
BEAM process owns its named leases and monitors each holder.

## Consequences

A subsequent page observes new commits after the captured position. Named
progress survives closing and reopening either database. Views sharing a
checkpoint handle compete for the same lease.
