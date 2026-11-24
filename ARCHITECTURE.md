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
transaction file, its directory, and parent entries for directories the store
creates. On reopen the server rebuilds stream histories from the transaction
graph and reports damaged files and missing parents. Complete graphs with
incompatible stream versions require reconciliation. See [ADR 0004](docs/decisions/0004-filesystem-journal.md).
