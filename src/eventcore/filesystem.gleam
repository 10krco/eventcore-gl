//// Filesystem store with immutable checksummed transactions.
//// The host must provide the util-linux flock executable on PATH.

import eventcore/store.{
  type Event, type EventStream, type Store, type StoreError,
}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

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
  InvalidReplicaId
  CannotOpen
}

pub type Status {
  Status(
    integrity_failures: List(String),
    dangling: List(Dangling),
    forks: List(Fork),
  )
}

pub type Dangling {
  Dangling(transaction: String, missing_parents: List(String))
}

pub type Fork {
  Fork(stream: Id, base_version: Version, transactions: List(String))
}

pub type Branch(payload) {
  Branch(transaction: String, events: List(Event(payload)))
}

pub type ForkContext(payload) {
  ForkContext(fork: Fork, branches: List(Branch(payload)))
}

pub type Resolution(payload) {
  Resolve(events: List(Event(payload)))
  Unresolvable(reason: String)
}

pub type ReconcileReport {
  ReconcileReport(resolved_count: Int, unresolved_streams: List(Id))
}

pub type JournalError {
  JournalUnavailable
  ReplicaIdentityConflict(replica: String, first: String, second: String)
  ReconciliationChanged
  InvalidEventPrefix
  JournalStoreError(StoreError)
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
  open_journal(config, None, codec)
}

pub fn open_with_replica(
  config: Config,
  replica: String,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  case string.trim(replica) {
    "" -> Error(InvalidReplicaId)
    id -> open_journal(config, Some(id), codec)
  }
}

fn open_journal(
  config: Config,
  replica: Option(String),
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  start(config.root, config.sync, replica)
  |> result.map(Filesystem(_, codec))
}

pub fn close(filesystem: Filesystem(payload)) -> Result(Nil, StoreError) {
  stop(filesystem.server)
  |> result.replace_error(store.StoreFailure(store.CloseStore))
}

pub fn status(filesystem: Filesystem(payload)) -> Result(Status, JournalError) {
  journal_status(filesystem.server)
}

pub fn replica_id(
  filesystem: Filesystem(payload),
) -> Result(String, JournalError) {
  journal_replica_id(filesystem.server)
}

pub fn detect_forks(
  filesystem: Filesystem(payload),
) -> Result(List(Fork), JournalError) {
  journal_forks(filesystem.server)
}

pub fn topology_generation(
  filesystem: Filesystem(payload),
) -> Result(Int, JournalError) {
  journal_topology_generation(filesystem.server)
}

/// Fingerprint the first count events in canonical journal order. Store the
/// token with a projection checkpoint and compare it after importing history.
pub fn event_prefix_token(
  filesystem: Filesystem(payload),
  count: Int,
) -> Result(String, JournalError) {
  case count < 0 {
    True -> Error(InvalidEventPrefix)
    False -> journal_event_prefix_token(filesystem.server, count)
  }
}

/// The resolver runs in the caller. If the graph changes before a resolution
/// can be recorded, reconcile returns ReconciliationChanged. Earlier
/// resolutions in this call remain committed.
pub fn reconcile(
  filesystem: Filesystem(payload),
  resolver: fn(ForkContext(payload)) -> Resolution(payload),
) -> Result(ReconcileReport, JournalError) {
  use #(revision, contexts) <- result.try(fork_snapshot(filesystem.server))
  reconcile_forks(filesystem, resolver, revision, contexts, 0, [])
}

type EncodedForkContext =
  #(Fork, List(#(String, List(Event(BitArray)))))

fn reconcile_forks(
  filesystem: Filesystem(payload),
  resolver: fn(ForkContext(payload)) -> Resolution(payload),
  revision: Int,
  contexts: List(EncodedForkContext),
  resolved: Int,
  declined: List(#(Id, Version)),
) -> Result(ReconcileReport, JournalError) {
  case contexts {
    [] ->
      Ok(ReconcileReport(
        resolved,
        list.reverse(declined) |> list.map(fn(fork) { fork.0 }),
      ))
    [#(fork, encoded), ..rest] -> {
      let key = #(fork.stream, fork.base_version)
      case list.contains(declined, key) {
        True ->
          reconcile_forks(
            filesystem,
            resolver,
            revision,
            rest,
            resolved,
            declined,
          )
        False -> {
          use branches <- result.try(
            list.try_map(encoded, fn(branch) {
              store.collect(decode_events(branch.1, filesystem.codec))
              |> result.map(Branch(branch.0, _))
              |> result.map_error(JournalStoreError)
            }),
          )
          case resolver(ForkContext(fork, branches)) {
            Unresolvable(_) ->
              reconcile_forks(filesystem, resolver, revision, rest, resolved, [
                key,
                ..declined
              ])
            Resolve(events) -> {
              use events <- result.try(
                encode_events(events, filesystem.codec)
                |> result.map_error(JournalStoreError),
              )
              use next <- result.try(resolve(
                filesystem.server,
                revision,
                fork,
                events,
              ))
              use #(current, refreshed) <- result.try(fork_snapshot(
                filesystem.server,
              ))
              case current == next {
                False -> Error(ReconciliationChanged)
                True ->
                  reconcile_forks(
                    filesystem,
                    resolver,
                    next,
                    refreshed,
                    resolved + 1,
                    declined,
                  )
              }
            }
          }
        }
      }
    }
  }
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
  use events <- result.try(encode_events(
    store.entries(writes),
    filesystem.codec,
  ))
  let expected = dict.to_list(store.expected_versions(writes))
  append(filesystem.server, expected, events)
}

fn encode_events(
  events: List(Event(payload)),
  codec: Codec(payload),
) -> Result(List(Event(BitArray)), StoreError) {
  list.try_map(events, fn(event) {
    codec.encode(event.kind, event.payload)
    |> result.map(store.Event(event.stream, event.kind, _))
    |> result.map_error(store.SerializationFailed(event.stream, _))
  })
}

@external(erlang, "eventcore_filesystem_ffi", "start_with_replica")
fn start(
  root: String,
  sync: Sync,
  replica: Option(String),
) -> Result(Server, OpenError)

@external(erlang, "eventcore_filesystem_ffi", "stop")
fn stop(server: Server) -> Result(Nil, Nil)

@external(erlang, "eventcore_filesystem_ffi", "status")
fn journal_status(server: Server) -> Result(Status, JournalError)

@external(erlang, "eventcore_filesystem_ffi", "replica_id")
fn journal_replica_id(server: Server) -> Result(String, JournalError)

@external(erlang, "eventcore_filesystem_ffi", "forks")
fn journal_forks(server: Server) -> Result(List(Fork), JournalError)

@external(erlang, "eventcore_filesystem_ffi", "topology_generation")
fn journal_topology_generation(server: Server) -> Result(Int, JournalError)

@external(erlang, "eventcore_filesystem_ffi", "event_prefix_token")
fn journal_event_prefix_token(
  server: Server,
  count: Int,
) -> Result(String, JournalError)

@external(erlang, "eventcore_filesystem_ffi", "fork_snapshot")
fn fork_snapshot(
  server: Server,
) -> Result(#(Int, List(EncodedForkContext)), JournalError)

@external(erlang, "eventcore_filesystem_ffi", "resolve")
fn resolve(
  server: Server,
  revision: Int,
  fork: Fork,
  events: List(Event(BitArray)),
) -> Result(Int, JournalError)

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
