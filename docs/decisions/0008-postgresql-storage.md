# PostgreSQL serializes writes across pools

## Context

A PostgreSQL store can share a database with other EventCore pools. A
multi-stream append must validate every expected version, including streams
that receive no events. Readers need a stable global append order for
projection delivery.

## Decision

Use a Pog pool. Callers can supply an existing pool or start one with a
connection URL or driver configuration. Acquire transaction-scoped locks for
the registered streams before checking versions. Allocate batch positions
through one transactional counter row and commit it with the events. Use read
committed for the write transaction so a writer that waited on a stream lock
can see its predecessor's committed version.

Migrate the event, checkpoint and snapshot tables transactionally. Store
command snapshots as bounded binary records and replace them under a
key-specific transaction lock. Emit structured OTP logger reports for read,
tail read and append operations.

## Consequences

Independent pools see the same versions and positions. A failed batch rolls
back its position allocation. EventCore batches share one position allocator
per database. The local integration test starts a private PostgreSQL instance
and exercises the same database through two pools.
