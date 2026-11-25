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
Commands can build initial declarations from a typed primary stream and its
participants, retaining first-seen order across repeated identifiers. Hand-written
declarations validate uniqueness. Requirement guards compose with the handler
result and defer constructing a typed rejection until the rule fails.
See [ADR 0016](docs/decisions/0016-typed-command-helpers.md).

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
latest local publication, and the writes and decisions on any other stream it
writes.
Later imports can reopen a fork, and resolvers can inspect the evidence
accepted by earlier resolutions. Projections can compare an event-prefix
fingerprint after importing history and rebuild when the consumed prefix
changes. See [ADR 0005](docs/decisions/0005-filesystem-reconciliation.md).

## SQLite storage

One OTP server owns each SQLite connection and serializes work on it. Each
append uses an immediate SQLite transaction to check every expected stream
version and write the complete batch. SQLite arbitrates between independent
connections. Event rows have per-stream versions and a global sequence.
Explicit migrations create the event and checkpoint tables. A checkpoint
store can also use its own database. File opens apply any encryption key
and validate it before requiring WAL. In-memory opens require memory journal
mode. Supplied connections retain their
pragmas. See [ADR 0006](docs/decisions/0006-sqlite-storage.md).

## Command snapshots

Commands can save replay state after each participating stream in the order
the runtime discovers it. A saved version vector lets the runtime read only
the suffix of each unchanged stream. If an earlier stream advances, the
runtime rebuilds subsequent streams from the new accumulated state. A refresh
can remove participants when the stored snapshot still matches the one observed
during replay. Filesystem snapshots are local to the journal and tied to a fingerprint of the canonical
event prefix. Use the journal's command view to keep replay and writes on the
same history across reconciliation. See [ADR 0007](docs/decisions/0007-command-snapshots.md).

## PostgreSQL adapter

PostgreSQL stores event payloads as binary records and assigns their global
positions inside the append transaction. Transaction-scoped stream locks
protect version checks across pools, including streams registered for reads
only. The same database stores subscriber checkpoints and command snapshots.
Snapshot replacement uses a lock keyed by snapshot name. See
[ADR 0008](docs/decisions/0008-postgresql-storage.md).

## Projection reads

A projection reads an exclusive page after a backend-local cursor. The
reader applies stream and event-type filters before filling the page.
Checkpoint persistence sits behind a separate interface, so the caller can
resume by subscription name. The memory adapter records its global append
order in the same OTP transition as stream versions. It scans bounded chunks
outside the store process up to a fixed frontier and returns the scanned
position even when the filter finds no matches. See [ADR 0009](docs/decisions/0009-projection-cursors.md).

The legacy projection runner acquires the named backend lease, loads its
checkpoint, and drives batch or continuous polling in the caller's BEAM
process. It advances after successful or skipped deliveries and can save the
scanned cursor once a page finishes. Stop signals interrupt idle and retry
waits. Memory leases live in the store process, require a holder on its BEAM node, and are reclaimed when their
holder exits. [ADR 0010](docs/decisions/0010-legacy-projection-runner.md)
records the failure and checkpoint policies.

Filesystem projections use a replica-local ingestion sequence. The server
recovers persisted assignments before assigning positions to imported
transactions. It syncs the journal directory before publishing newly recovered
assignments. A bounded page fails if it encounters a reserved position with
an unavailable event.
Named checkpoints use atomic local files, and the store process owns the
named leases on its BEAM node. See
[ADR 0011](docs/decisions/0011-filesystem-projection-ingestion.md).

SQLite projection reads scan committed insertion positions in bounded ranges.
Each page captures an upper position before scanning. A projector can use
the event database for its progress or a separate checkpoint database.
Views that share a checkpoint handle also share its named leases.
See [ADR 0012](docs/decisions/0012-sqlite-projection-replay.md).

PostgreSQL strict delivery uses the committed event position counter as its
watermark. Separate migrations add persistent UUIDs and JSON metadata for
existing and future rows. The delivery source scans bounded position ranges,
applies its validated selection before counting deliveries, and returns the
original payload bytes and JSON metadata for application decoding.
See [ADR 0013](docs/decisions/0013-postgres-projection-delivery.md).

Legacy PostgreSQL projectors hold a session advisory lock in the checkpoint
database. Named checkpoint operations use the lock-owning session. Reads can
reuse a held session from the same pool. The projection reader scans committed positions in bounded
ranges and returns decoded events through the legacy event codec.
See [ADR 0014](docs/decisions/0014-postgres-legacy-projections.md).

Transactional PostgreSQL projections use a separate destination progress table.
A named advisory lock binds each runner to an exact PostgreSQL session. Every
attempt reloads the source and selection identity and the committed cursor.
Projector writes and cursor advancement commit together on that session.
Retry and skip follow rollback, and after-commit work runs after acknowledgement.
Reset uses the same lock and commits destination cleanup with cursor deletion.
Reset-and-replay holds leadership while it moves from cleanup to replay.
See [ADR 0015](docs/decisions/0015-postgres-transactional-projections.md).

## Typed model components

The experimental model module wraps domain values in role-specific opaque types.
Each component has a distinct witness type. A field value carries that witness
and its field occurrence, and checker identifiers preserve the role, component
name, and field name as separate values.
Domain modules combine required field values in private constructors. Input
origins and state recipes provide roots. Mappings read one or more typed fields
and produce the exact target field value, with fallible transformations
preserving their domain errors. See [ADR 0017](docs/decisions/0017-typed-model-components.md).

Modeled commands adapt their typed declarations, replay logic, and emitted
events to the ordinary command runtime. Modeled projections use the ordinary
projection runner with a checked effect sink. The adapter reports projection
errors separately from sink errors and passes the delivery position to the
sink. Tests and examples can use delivery positions to handle redelivery in
the serialized in-memory sink, then render its read model through a typed
view. See
[ADR 0018](docs/decisions/0018-modeled-runtime.md).
