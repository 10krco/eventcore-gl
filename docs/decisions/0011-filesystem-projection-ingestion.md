# ADR 0011. Persist filesystem projection positions in ingestion order

## Context

A replica can import a transaction that belongs earlier in canonical graph
order than a projector's checkpoint. A cursor in canonical order can skip
that transaction when the projector resumes.

## Decision

Assign projection positions when the journal first sees each transaction.
Keep the assignments under the Git-ignored local metadata directory as
checksummed, contiguous segments. A segment records transaction IDs and
event counts. On reopen, keep existing positions and assign new positions
to imported transactions in canonical order. Preserve the position range
of a transaction missing from the event index so subsequent positions stay
valid.

Persist an assignment after the transaction file and its directory are
synced under the configured policy. Publish the assignment by atomic rename.
If publication fails after a transaction commits, keep its pending assignment
and retry before exposing a projection frontier. Keep named projection
checkpoints in the same local metadata directory. Use the store process to
coordinate named leases on its BEAM node.

## Consequences

A resumed projector sees imports after its checkpoint even if replaying
a command sees those events earlier. Each replica has its own positions
and checkpoints. A projection can rebuild by starting from the beginning
of that replica's ingestion sequence, while the topology and prefix APIs
support consumers that need canonical order.
