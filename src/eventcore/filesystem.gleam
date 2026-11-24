//// Filesystem store with immutable checksummed transactions.
//// The host must provide the util-linux flock executable on PATH.

import eventcore/store.{
  type Event, type EventStream, type Store, type StoreError,
}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/result

pub type Sync {
  Full
  Unsynced
}

pub type Config {
  Config(root: String, sync: Sync)
}

/// Supply a stable binary representation for each event kind.
pub type Codec(payload) {
  Codec(
    encode: fn(String, payload) -> Result(BitArray, String),
    decode: fn(String, BitArray) -> Result(payload, String),
  )
}

pub type OpenError {
  StoreLocked
  LockExecutableMissing
  CannotOpen
}

pub type Status {
  Status(integrity_failures: List(String), dangling: List(Dangling))
}

pub type Dangling {
  Dangling(transaction: String, missing_parents: List(String))
}

pub opaque type Filesystem(payload) {
  Filesystem(server: Server, codec: Codec(payload))
}

type Server

pub fn open(
  root: String,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  open_with_config(Config(root, Full), codec)
}

pub fn open_with_config(
  config: Config,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  start(config.root, config.sync)
  |> result.map(Filesystem(_, codec))
}

pub fn close(filesystem: Filesystem(payload)) -> Result(Nil, StoreError) {
  stop(filesystem.server)
  |> result.replace_error(store.StoreFailure(store.CloseStore))
}

pub fn status(filesystem: Filesystem(payload)) -> Result(Status, StoreError) {
  journal_status(filesystem.server)
  |> result.replace_error(store.StoreFailure(store.ReadStream))
}

pub fn as_store(filesystem: Filesystem(payload)) -> Store(payload) {
  store.Store(
    fn(id, version) { read_after(filesystem, id, version) },
    append_events(filesystem, _),
  )
}

fn read_after(
  filesystem: Filesystem(payload),
  id: Id,
  after: Version,
) -> Result(EventStream(payload), StoreError) {
  read(filesystem.server, id, stream.event_count(after))
  |> result.replace_error(store.StoreFailure(store.ReadStream))
  |> result.map(fn(events) { decode_events(events, filesystem.codec) })
}

fn decode_events(
  events: List(Event(BitArray)),
  codec: Codec(payload),
) -> EventStream(payload) {
  store.EventStream(fn() {
    case events {
      [] -> Ok(store.End)
      [event, ..rest] -> {
        use payload <- result.try(
          codec.decode(event.kind, event.payload)
          |> result.map_error(store.DeserializationFailed(event.stream, _)),
        )
        Ok(store.Next(
          store.Event(event.stream, event.kind, payload),
          decode_events(rest, codec),
        ))
      }
    }
  })
}

fn append_events(
  filesystem: Filesystem(payload),
  writes: store.Writes(payload),
) -> Result(Nil, StoreError) {
  use events <- result.try(
    store.entries(writes)
    |> list.try_map(fn(event) {
      filesystem.codec.encode(event.kind, event.payload)
      |> result.map(store.Event(event.stream, event.kind, _))
      |> result.map_error(store.SerializationFailed(event.stream, _))
    }),
  )
  let expected = dict.to_list(store.expected_versions(writes))
  append(filesystem.server, expected, events)
}

@external(erlang, "eventcore_filesystem_ffi", "start")
fn start(root: String, sync: Sync) -> Result(Server, OpenError)

@external(erlang, "eventcore_filesystem_ffi", "stop")
fn stop(server: Server) -> Result(Nil, Nil)

@external(erlang, "eventcore_filesystem_ffi", "status")
fn journal_status(server: Server) -> Result(Status, Nil)

@external(erlang, "eventcore_filesystem_ffi", "read")
fn read(
  server: Server,
  id: Id,
  after: Int,
) -> Result(List(Event(BitArray)), Nil)

@external(erlang, "eventcore_filesystem_ffi", "append")
fn append(
  server: Server,
  expected: List(#(Id, Version)),
  events: List(Event(BitArray)),
) -> Result(Nil, StoreError)
