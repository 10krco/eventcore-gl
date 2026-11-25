# Choose and configure a backend

EventCore commands use the same [store interface](eventcore/store.html) on every backend. Choose where to keep the event log and how to operate it before choosing a [projection runner](projections.html). Store adapters share an API. Their files, databases, event positions, and checkpoint formats are backend-specific.

| Backend | Use it for | Runtime and lifecycle |
| --- | --- | --- |
| [Memory](eventcore/memory.html) | Unit tests, tutorials, and disposable runs | No storage service. Events, snapshots, and checkpoints disappear when the store closes or its owning process exits. |
| [Filesystem](eventcore/filesystem.html) | A local journal, or applications that deliberately exchange and reconcile journal transactions | A writable journal directory and host advisory locking. One open writer per journal root. |
| [SQLite](eventcore/sqlite.html) | Embedded durable storage for an application on one host | A writable database path on a filesystem that supports SQLite WAL. Event and checkpoint connections can be separate. |
| [PostgreSQL](eventcore/postgres.html) | Shared durable storage for application instances and transactional projections | A PostgreSQL database and a supervised Pog pool. |

EventCore targets Erlang on the BEAM. The package includes a native SQLite dependency, so a clean build needs a C compiler, make, Rebar3, and Erlang/OTP development headers even when the application will use another backend. The filesystem backend uses the host's `flock` on `PATH`. Fenced filesystem checkpoints also use `sh` and `mv -fT`. Linux with util-linux and GNU coreutils is the configuration exercised by the project.

## Memory

Start the handle in the process that owns its lifetime. Pass `memory.as_store(handle)` to command and query code and call `memory.close(handle)` when finished. The [projection backend](eventcore/memory.html#projection_backend) adds a reader, checkpoints, and same-node named leadership. A test can create a fresh handle for each case without configuring a database.

## Encode persistent events

The filesystem, SQLite, and PostgreSQL adapters each take their own `Codec` with the same two functions. `encode` receives the stored event kind and domain payload and returns bytes. `decode` receives the stored kind and bytes and returns the domain payload. Both return a `Result` so malformed or unknown data can be reported.

Keep event kind names and encodings readable for as long as the log retains them. For example, keep a decoder for `account-opened.v1` when introducing `account-opened.v2`. Decode old payloads into a domain type the current code understands. Add a test that decodes a saved example of each historical encoding. Returning the bytes unchanged, as in the examples below, is appropriate when `BitArray` is already the application's payload type.

## Filesystem

```gleam
import eventcore/filesystem

pub fn open_journal(path: String) -> filesystem.Filesystem(BitArray) {
  let codec =
    filesystem.Codec(fn(_, bytes) { Ok(bytes) }, fn(_, bytes) { Ok(bytes) })
  let assert Ok(journal) = filesystem.open(path, codec)
  journal
}
```

Use `filesystem.as_store(journal)` for event reads and appends, and close the handle before reopening the same root in another process. The default `Full` sync policy flushes the append and relevant directories. `filesystem.Config(root, filesystem.Unsynced)` is available through `open_with_config` for workloads that can tolerate losing recent work in a crash.

The root contains an `events/` directory of immutable, checksummed transactions. Close the journal before importing or restoring transaction files. Reopen it, then use `filesystem.status` to inspect integrity failures, missing parents, and forks. For installations that exchange files, give each writer a distinct stable replica identity with [`open_with_replica`](eventcore/filesystem.html#open_with_replica). Restore missing ancestors before writing under the default reconciliation policy. Preserve immutable transaction files and resolve forks through the [reconciliation API](eventcore/filesystem.html#reconcile). Applications using command snapshots can use [`filesystem.command_view`](eventcore/filesystem.html#command_view) to bind command replay, snapshots, and appends to a consistent history view. When reconciliation changes the canonical history, use the [projection rebuild guidance](projections.html).

## SQLite

```gleam
import eventcore/sqlite
import gleam/option.{None}

pub fn open_databases(
  events_path: String,
  checkpoints_path: String,
) -> #(sqlite.EventStore(BitArray), sqlite.CheckpointStore) {
  let codec =
    sqlite.Codec(fn(_, bytes) { Ok(bytes) }, fn(_, bytes) { Ok(bytes) })
  let assert Ok(events) = sqlite.open(events_path, codec)
  let assert Ok(Nil) = sqlite.migrate(events)
  let assert Ok(checkpoints) = sqlite.open_checkpoints(checkpoints_path, None)
  let assert Ok(Nil) = sqlite.migrate_checkpoints(checkpoints)
  #(events, checkpoints)
}
```

Open these handles from a long-lived application process. The SQLite servers close their connections when that process exits. Use `sqlite.as_store(events)` for commands and `sqlite.projection_backend_with_checkpoints(events, checkpoints)` for a projection with its own checkpoint database. To keep both in one database, use `sqlite.projection_backend(events)` after the event migration. Call `close_checkpoints` and `close` for the handles you opened. For disposable databases, `sqlite.open_memory` and `sqlite.open_memory_checkpoints` use the same migration functions.

Open databases from trusted application paths. File opens require WAL mode. When copying an active database, use a SQLite-aware backup mechanism so you capture committed WAL data. If an application adopts an existing driver connection with `from_connection`, EventCore keeps its existing pragmas and takes ownership. Stop using the original connection handle after adoption.

For encryption, build the SQLite driver against SQLCipher and call `sqlite.open_file(path, Some(key), codec)`. Use `Some(key)` for each encrypted checkpoint file as well. The bundled SQLite build cannot satisfy a keyed open, which returns `EncryptionUnavailable` when SQLCipher support is absent. Configure linkage at native build time and verify both a successful keyed reopen and a rejected wrong-key reopen in the deployment build. The repository's Nix shell is a working example of `USE_SYSTEM_SQLITE=1` with SQLCipher linked in place of the system SQLite library. Supply keys from application secret configuration.

## PostgreSQL

```gleam
import eventcore/postgres
import pog

pub fn migrate_event_store(
  pool: pog.Connection,
) -> postgres.EventStore(BitArray) {
  let codec =
    postgres.Codec(fn(_, bytes) { Ok(bytes) }, fn(_, bytes) { Ok(bytes) })
  let events = postgres.from_pool(pool, codec)
  let assert Ok(Nil) = postgres.migrate(events)
  events
}
```

Pass a supervised Pog pool to `postgres.from_pool`. If EventCore starts the pool through `postgres.start` or `postgres.start_url`, the caller receives its PID and owns supervision and shutdown. Provide the connection URL or driver settings from your deployment configuration. Applications that import `pog` should declare it directly in their Gleam dependencies.

Run `postgres.migrate` before using the store and use `postgres.as_store(events)` for commands. The migration creates event, stream position, command snapshot, and basic checkpoint storage. For separate legacy checkpoints, wrap their database pool with `postgres.checkpoints_from_pool` and run `postgres.migrate_checkpoints` on it before composing the backend.

For transactional projections, run `postgres.migrate_delivery(events)` after the store migration, then run `postgres_projection.migrate` on the projection destination. The source migration backfills identities for existing events, so plan the migration window for an existing large log. See [projection setup and recovery](projections.html) for destination setup, identity, and failure handling.

The examples use `let assert` to keep setup short. In a service, report typed startup or migration failures and keep the dependents stopped until setup succeeds.
