//// SQLite event and projection checkpoint stores.

import eventcore/store.{type Event, type Store, type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option, None}
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

fn decode(
  items: List(Result(Event(BitArray), StoreError)),
  codec: Codec(payload),
) -> store.EventStream(payload) {
  store.EventStream(fn() {
    case items {
      [] -> Ok(store.End)
      [item, ..rest] -> {
        use event <- result.try(item)
        use payload <- result.try(
          codec.decode(event.kind, event.payload)
          |> result.map_error(store.DeserializationFailed(event.stream, _)),
        )
        Ok(store.Next(
          store.Event(event.stream, event.kind, payload),
          decode(rest, codec),
        ))
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
