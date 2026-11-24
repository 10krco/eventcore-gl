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
counts transactions with multiple parents. Expose a fingerprint of the
consumed event prefix so a projection can detect reordered history after
any import.

A domain resolver receives all typed stream events on each branch from
divergence to its head. A resolution records the stream, base version and
heads it joins alongside its events, which may be empty. Ordinary appends
to an unresolved stream fail until a resolver makes a decision. Later imports
can reopen a fork when a resolution did not cover the imported branch head.
Previously accepted events remain available to a subsequent resolver. Other
streams can advance while a fork remains open.

Run the resolver in the caller. Recheck the graph revision in the journal
server when committing a resolution. Include the latest local publication
as a causal parent. When a resolution writes another stream, its parents
include the writes that contributed that stream's current version and its
resolution records, including empty decisions. Compute ownership in canonical
order and walk each owner's ancestry once when finding its frontier. Refresh the remaining forks after each resolution.
Report a concurrent revision change so the caller can inspect current
branches again.

## Consequences

Working copies that import the same valid files read the same event order.
Independent merge transactions can create another fork that can be resolved
with the same mechanism. Imported merges can reorder history behind a
projection cursor. A reconcile call can commit earlier resolutions before a
later resolution fails. Incomplete graphs remain readable and reject writes
while a parent is missing. Replica collision detection requires complete
ancestry for both transactions so recovery status can report missing parents.
