//// SQLite event and projection checkpoint stores.

import eventcore/internal/projection_scan
import eventcore/projection
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/store.{type Event, type Store, type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

pub type Codec(payload) {
  Codec(
    encode: fn(String, payload) -> Result(BitArray, String),
    decode: fn(String, BitArray) -> Result(payload, String),
  )
}

pub opaque type EventStore(payload) {
  EventStore(server: Server, codec: Codec(payload))
}

pub opaque type CheckpointStore {
  CheckpointStore(server: Server)
}

/// An already open sqlite driver connection. The store owns the connection
/// after adoption. The caller must stop using the original handle.
pub type Connection

type Server

pub type OpenError {
  CannotOpen
  InvalidEncryptionKey
  EncryptionUnavailable
}

pub type MigrationError {
  MigrationFailed
}

pub type CheckpointError {
  InvalidPosition
  CheckpointFailed
}

type Schema {
  EventSchema
  CheckpointSchema
}

pub fn open(
  path: String,
  codec: Codec(payload),
) -> Result(EventStore(payload), OpenError) {
  open_file(path, None, codec)
}

pub fn open_file(
  path: String,
  encryption_key: Option(String),
  codec: Codec(payload),
) -> Result(EventStore(payload), OpenError) {
  start_file(path, encryption_key)
  |> result.map(EventStore(_, codec))
}

pub fn open_memory(
  codec: Codec(payload),
) -> Result(EventStore(payload), OpenError) {
  start_memory()
  |> result.map(EventStore(_, codec))
}

pub fn from_connection(
  connection: Connection,
  codec: Codec(payload),
) -> Result(EventStore(payload), OpenError) {
  start_supplied(connection)
  |> result.map(EventStore(_, codec))
}

pub fn open_checkpoints(
  path: String,
  encryption_key: Option(String),
) -> Result(CheckpointStore, OpenError) {
  start_file(path, encryption_key)
  |> result.map(CheckpointStore)
}

pub fn open_memory_checkpoints() -> Result(CheckpointStore, OpenError) {
  start_memory()
  |> result.map(CheckpointStore)
}

pub fn checkpoints_from_connection(
  connection: Connection,
) -> Result(CheckpointStore, OpenError) {
  start_supplied(connection)
  |> result.map(CheckpointStore)
}

/// Create or upgrade the event store schema before use.
pub fn migrate(events: EventStore(payload)) -> Result(Nil, MigrationError) {
  migrate_schema(events.server, EventSchema)
}

/// Create or upgrade the checkpoint schema before use.
pub fn migrate_checkpoints(
  checkpoints: CheckpointStore,
) -> Result(Nil, MigrationError) {
  migrate_schema(checkpoints.server, CheckpointSchema)
}

pub fn close(events: EventStore(payload)) -> Result(Nil, StoreError) {
  stop(events.server)
  |> result.replace_error(store.StoreFailure(store.CloseStore))
}

pub fn close_checkpoints(
  checkpoints: CheckpointStore,
) -> Result(Nil, CheckpointError) {
  stop(checkpoints.server)
  |> result.replace_error(CheckpointFailed)
}

pub fn load_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Option(Int), CheckpointError) {
  load(checkpoints.server, name)
}

pub fn save_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
  position: Int,
) -> Result(Nil, CheckpointError) {
  case position < 0 || position > 9_223_372_036_854_775_807 {
    True -> Error(InvalidPosition)
    False -> save(checkpoints.server, name, position)
  }
}

pub fn reset_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Nil, CheckpointError) {
  reset(checkpoints.server, name)
}

pub fn snapshots(events: EventStore(payload)) -> snapshot.Repository {
  snapshot.Repository(
    load: fn(key) {
      load_snapshot(events.server, snapshot.name(key))
      |> result.replace_error(store.StoreFailure(store.LoadSnapshot))
    },
    save: fn(key, observed, value) {
      save_snapshot(events.server, snapshot.name(key), observed, value)
      |> result.replace_error(store.StoreFailure(store.SaveSnapshot))
    },
  )
}

pub fn as_store(events: EventStore(payload)) -> Store(payload) {
  store.Store(
    fn(id, version) {
      read(events.server, id, stream.event_count(version))
      |> result.replace_error(store.StoreFailure(store.ReadStream))
      |> result.map(decode(_, events.codec))
    },
    fn(writes) {
      use entries <- result.try(
        list.try_map(store.entries(writes), fn(event) {
          events.codec.encode(event.kind, event.payload)
          |> result.map(store.Event(event.stream, event.kind, _))
          |> result.map_error(store.SerializationFailed(event.stream, _))
        }),
      )
      append(
        events.server,
        dict.to_list(store.expected_versions(writes)),
        entries,
      )
    },
  )
}

/// Reads bounded pages in committed insertion order.
pub fn projection_reader(
  events: EventStore(payload),
) -> projection.Reader(payload) {
  fn(filter, page) {
    use high <- result.try(projection_frontier(events.server))
    projection_scan.read(
      filter,
      page,
      high,
      fn(first, last) { projection_fetch(events.server, first, last) },
      fn(wire) {
        use event <- result.try(unpack_projection(wire))
        decode_event(event, events.codec)
      },
    )
  }
}

pub fn projection_checkpoints(
  checkpoints: CheckpointStore,
) -> projection.Checkpoints {
  projection.Checkpoints(
    fn(name) {
      load_checkpoint(checkpoints, name)
      |> result.replace_error(store.StoreFailure(store.LoadCheckpoint))
      |> result.map(fn(cursor) {
        case cursor {
          Some(sequence) -> option.from_result(projection.position(sequence))
          None -> None
        }
      })
    },
    fn(name, position) {
      save_checkpoint(checkpoints, name, projection.sequence(position))
      |> result.replace_error(store.StoreFailure(store.SaveCheckpoint))
    },
  )
}

/// Keeps progress and leadership on the event store handle.
pub fn projection_backend(
  events: EventStore(payload),
) -> projection_runner.Backend(payload) {
  projection_backend_with_checkpoints(events, CheckpointStore(events.server))
}

/// Shares progress and leadership through the given checkpoint handle.
pub fn projection_backend_with_checkpoints(
  events: EventStore(payload),
  checkpoints: CheckpointStore,
) -> projection_runner.Backend(payload) {
  projection_runner.Backend(
    projection_reader(events),
    projection_checkpoints(checkpoints),
    fn(name) {
      use release <- result.map(acquire_projection(checkpoints.server, name))
      projection_runner.Lease(release)
    },
  )
}

fn decode_event(
  event: Event(BitArray),
  codec: Codec(payload),
) -> Result(Event(payload), StoreError) {
  codec.decode(event.kind, event.payload)
  |> result.map(store.Event(event.stream, event.kind, _))
  |> result.map_error(store.DeserializationFailed(event.stream, _))
}

fn decode(
  items: List(Result(Event(BitArray), StoreError)),
  codec: Codec(payload),
) -> store.EventStream(payload) {
  store.EventStream(fn() {
    case items {
      [] -> Ok(store.End)
      [item, ..rest] -> {
        use event <- result.try(item)
        use event <- result.try(decode_event(event, codec))
        Ok(store.Next(event, decode(rest, codec)))
      }
    }
  })
}

@external(erlang, "eventcore_sqlite_ffi", "start_file")
fn start_file(
  path: String,
  encryption: Option(String),
) -> Result(Server, OpenError)

@external(erlang, "eventcore_sqlite_ffi", "start_memory")
fn start_memory() -> Result(Server, OpenError)

@external(erlang, "eventcore_sqlite_ffi", "start_supplied")
fn start_supplied(connection: Connection) -> Result(Server, OpenError)

@external(erlang, "eventcore_sqlite_ffi", "migrate")
fn migrate_schema(server: Server, schema: Schema) -> Result(Nil, MigrationError)

@external(erlang, "eventcore_sqlite_ffi", "stop")
fn stop(server: Server) -> Result(Nil, Nil)

@external(erlang, "eventcore_sqlite_ffi", "read")
fn read(
  server: Server,
  id: Id,
  after: Int,
) -> Result(List(Result(Event(BitArray), StoreError)), Nil)

@external(erlang, "eventcore_sqlite_ffi", "append")
fn append(
  server: Server,
  expected: List(#(Id, Version)),
  events: List(Event(BitArray)),
) -> Result(Nil, StoreError)

@external(erlang, "eventcore_sqlite_ffi", "checkpoint")
fn load(server: Server, name: String) -> Result(Option(Int), CheckpointError)

@external(erlang, "eventcore_sqlite_ffi", "save_checkpoint")
fn save(
  server: Server,
  name: String,
  position: Int,
) -> Result(Nil, CheckpointError)

@external(erlang, "eventcore_sqlite_ffi", "reset_checkpoint")
fn reset(server: Server, name: String) -> Result(Nil, CheckpointError)

@external(erlang, "eventcore_sqlite_ffi", "load_snapshot")
fn load_snapshot(
  server: Server,
  name: String,
) -> Result(Option(snapshot.Snapshot), Nil)

@external(erlang, "eventcore_sqlite_ffi", "save_snapshot")
fn save_snapshot(
  server: Server,
  name: String,
  observed: Option(snapshot.Snapshot),
  value: snapshot.Snapshot,
) -> Result(Nil, Nil)

@external(erlang, "eventcore_sqlite_ffi", "projection_frontier")
fn projection_frontier(server: Server) -> Result(Int, StoreError)

@external(erlang, "eventcore_sqlite_ffi", "projection_fetch")
fn projection_fetch(
  server: Server,
  first: Int,
  last: Int,
) -> Result(List(#(Int, Event(BitArray))), StoreError)

@external(erlang, "eventcore_sqlite_ffi", "unpack_projection")
fn unpack_projection(
  event: Event(BitArray),
) -> Result(Event(BitArray), StoreError)

@external(erlang, "eventcore_projection_leases", "request")
fn acquire_projection(
  server: Server,
  name: String,
) -> Result(fn() -> Nil, projection_runner.LeadershipError)
