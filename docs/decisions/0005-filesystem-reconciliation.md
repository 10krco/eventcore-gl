# Filesystem replicas reconcile immutable transaction graphs

## Context

Working copies can publish concurrently and exchange transaction files
through Git. The union can contain competing extensions of a stream.
Applications need to inspect those branches and record a domain decision
that is reproducible across working copies.

## Decision

Record the local replica identity in each new transaction. Store the default
identity under the Git-ignored local metadata directory and bind it to the
machine and working copy. Allow explicit provisioning and report concurrent
transactions that use the same identity. Preserve read support for version
one transactions, which carry no replica identity.

Derive a canonical event order from ancestor depth, timestamp, replica
identity, and transaction nonce. Expose forks and a topology generation that
counts transactions with multiple parents. A domain resolver receives each
fork's typed branch events and can decline resolution or publish an immutable
merge referencing the fork heads, including a merge with zero events.

Run the resolver in the caller. Recheck the graph revision in the journal
server when committing a resolution. Report a revision change so the caller
can inspect current branches again.

## Consequences

Working copies that import the same valid files read the same event order.
Independent merge transactions can create another fork that can be resolved
with the same mechanism. Imported merges can reorder history behind a
projection cursor. A reconcile call can commit earlier resolutions before a
later resolution fails. Incomplete graphs remain readable and reject writes
while a parent is missing.
