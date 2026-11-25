# 0014. Coordinate legacy PostgreSQL projections with session locks

## Context

Legacy projectors resume from named checkpoints and require exclusive
leadership across pools and BEAM nodes connected to the same database.
PostgreSQL already provides session advisory locks and releases them when
the connection closes.

## Decision

Reserve a PostgreSQL session from the checkpoint pool while the projector
runs. Use the stable signed 64-bit FNV-1a hash of the subscription name as
the advisory key. The BEAM holder owns the checkout and releases it with an
idempotent guard. Return only successfully unlocked sessions to the pool.
Pog disconnects the session when the holder dies.

Use the session holding the subscription lock for its checkpoint operations.
Read-only pages reuse a held session when they use the same pool. This supports a projector using a pool with
one connection. Replay scans bounded position ranges through the committed
frontier using the legacy event codec.

## Consequences

Each active projector reserves a checkout for its lifetime. A pool with
spare connections can serve multiple names. An exhausted pool returns a
coordination error after a bounded acquisition wait. Independent pools on the same database coordinate through PostgreSQL. Legacy reads and
checkpoint updates remain separate operations.
