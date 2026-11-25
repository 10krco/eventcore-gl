# ADR 0022. Open filesystem projection state independently

## Status

Accepted.

## Context

A projector can attach filesystem progress and coordination to any legacy
projection reader. EventCore 2.1.0 opens both facilities without the journal
writer lock, and competing processes contend for the same subscription.

## Decision

Expose standalone checkpoint and coordinator constructors. Checkpoints reuse
the journal's replica-local checkpoint format, atomic replacement, and
configured sync policy. Opening a Full checkpoint store syncs the ancestor
entries for new directories. The journal-backed coordinator and standalone
coordinator use the same named OS advisory lock files. A worker holds each
lock through the existing flock port, monitors the holder, and also monitors
the journal for journal-backed leases. Journal-backed coordination requires a
local journal handle. The runner checks the lock before processing or saving
progress and returns a leadership error when the port or journal dies.
Filesystem checkpoints offer a fenced save. The process holding a separate
per-name OS lock performs the atomic rename after the runner rechecks its
lease. A replacement waits for the rename or for the process to exit. If
that process dies before the rename, the former holder cannot publish its
prepared file. Use one coordinator root for all runners sharing a checkpoint.
A checked lease requires a fenced checkpoint adapter. A journal-backed fenced
save requires the journal to run on the writer's node. Release waits for the
worker to exit. Lock files stay in place so all contenders use the same inode.

## Consequences

A projector can persist and reopen progress without creating or opening an
event journal. Competing OS processes contend for the same name. Independent
names can advance in parallel, and holder exit releases its lease.

Existing coordinators can still construct `Lease(release)`. Lease consumers
can call `lease.release()` with either variant. Exhaustive matches on
`Lease` must also cover `CheckedLease`. Consumers can read `checkpoints.load`
and `checkpoints.save` across both checkpoint variants. Exhaustive matches on
`Checkpoints` must also cover `FencedCheckpoints`.
