# Architecture

EventCore is a Gleam library compiled to Erlang modules and executed on OTP.
The root package owns the public module namespace. Gleeunit exercises the
compiled package on the BEAM. The bootstrap provides the package boundary and
build environment. Commands, stores, projections and modeling arrive through
the planned work in the [parity inventory](docs/parity.md).

The compatibility boundary is the observable contract of the Rust 2.1.0
release. Types, process structure and durable encodings can follow OTP
conventions. Each capability needs executable evidence against the pinned
contract. See [ADR 0001](docs/decisions/0001-beam-parity.md).

## Event store and commands

Adapters expose pull-based event reads from an exclusive stream version and
atomic optimistic appends. An append compares every registered stream version,
including read-only participants, before recording any events. The in-memory
adapter owns its state in one OTP server per store. It returns only the requested
part of the immutable history and stops when its creating process exits.
See [ADR 0002](docs/decisions/0002-optimistic-append.md).

Command execution folds each participating stream into state as events arrive.
It reads initial declarations in order and enqueues discovered participants
after each stream. An append conflict restarts the fold and discovery from the
original declarations. See [ADR 0003](docs/decisions/0003-command-replay.md).

## Filesystem journal

The filesystem adapter publishes each append as one immutable transaction
under `events/`. Its filename is the SHA-256 checksum of the versioned
transaction bytes. The transaction records its parent IDs, expected stream
versions and encoded events. A single OTP process holds a host advisory lock,
checks versions and publishes files by atomic rename. Full sync flushes the
transaction file, its directory, and the journal directory’s ancestor entries on every open. On reopen the server rebuilds stream histories from the transaction
graph and reports damaged files and missing parents. Appends require all parents to be present. Complete graphs expose divergent stream histories for reconciliation. See [ADR 0004](docs/decisions/0004-filesystem-journal.md).

## Filesystem replica reconciliation

New transactions carry a replica identity that typically comes from
fingerprint-bound metadata under the journal's Git-ignored local directory.
On reopen, ancestor depth and stable transaction fields determine canonical
order. The journal compares branch ancestry to detect forks and replica
identity collisions. Callers can inspect complete branch histories and submit
a resolution against the graph revision they read. Each resolution records
the stream, base and heads it joins. Its ancestry includes those heads, the
latest local publication, and the history of any other stream it writes.
Later imports can reopen a fork, and resolvers can inspect the evidence
accepted by earlier resolutions. Projections can compare an event-prefix
fingerprint after importing history and rebuild when the consumed prefix
changes. See [ADR 0005](docs/decisions/0005-filesystem-reconciliation.md).
