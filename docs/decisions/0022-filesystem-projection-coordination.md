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
configured sync policy. The journal-backed coordinator and standalone
coordinator use the same named OS advisory lock files. A worker holds each
lock through the existing flock port, monitors the holder, and also monitors
the journal for journal-backed leases. Release waits for the worker to exit.
Lock files stay in place so all contenders use the same inode.

## Consequences

A projector can persist and reopen progress without creating or opening an
event journal. Competing OS processes contend for the same name. Independent
names can advance in parallel, and holder exit releases its lease.
