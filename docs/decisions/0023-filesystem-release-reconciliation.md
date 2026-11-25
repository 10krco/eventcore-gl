# Provide the published filesystem reconciliation rules

## Context

EventCore 2.1.0 treats every registered stream version as a claim on a
nonempty append. A transaction that descends from all claims closes the
fork. A resolver receives only the events from those claiming transactions.
Applications porting from that release can depend on those behaviors.

## Decision

Select the reconciliation policy when opening a journal. The explicit
`RustRelease` policy groups all claims by stream and base version, passes
the claiming transactions to the resolver, and joins their ancestry using
ordinary transaction records. An ordinary nonempty append joins the journal
tips. Empty appends succeed without consulting stream versions. A resolution
uses the forks identified at the start of the pass and checks the journal
revision before each publication.

`FullBranches` is the default policy from ADR 0005. Both policies use the
same transaction files, integrity checks, and per-replica projection
positions. The selected policy is scoped to the open handle.

## Consequences

A normal append under `RustRelease` can close a fork without invoking
domain reconciliation. A resolution made on a replica that already wrote
a later transaction on one branch can leave concurrent transactions with
the same replica identity. Applications sharing a journal should agree
on the policy they will use to interpret its forks.
