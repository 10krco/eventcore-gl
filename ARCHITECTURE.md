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
