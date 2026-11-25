# ADR 0024. Compose independent projection checkpoints and coordinators

## Context

The EventCore 2.1.0 in-memory, SQLite and PostgreSQL integrations expose
coordination independently from event storage. In-memory progress can also
live in a standalone checkpoint store. A caller may combine these components
with a different event source.

## Decision

Expose a standalone memory checkpoint store and a standalone BEAM-local
coordinator that both memory and SQLite projections can use. Each start
creates an isolated instance, owned by the process that starts it. A local
coordinator serializes acquisitions by name, monitors lease holders and uses
the existing lease token to fence stale releases.

Expose the existing PostgreSQL session advisory-lock coordinator directly
from a caller-supplied pool. Callers can compose a checkpoint adapter from
the same pool to route writes through the session holding the lease.
Existing bundled backends keep their current coordination scope.

## Consequences

The reader, checkpoint store and coordinator can be constructed separately
and passed to the legacy runner. Sharing the same local coordinator shares
its names. PostgreSQL coordinators from different pools in one database
contend on the same name.
