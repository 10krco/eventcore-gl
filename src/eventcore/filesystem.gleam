//// Filesystem store with immutable checksummed transactions.
//// The host must provide the util-linux flock executable on PATH.

import eventcore/internal/projection_scan
import eventcore/projection
import eventcore/projection_runner
import eventcore/snapshot
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

/// Acquire a store and repository for one command execution. Ordinary appends
/// retain the view and still use optimistic stream versions. Reconciliation
/// invalidates it, in which case acquire a fresh view and reconstruct again.
pub fn command_view(
  filesystem: Filesystem(payload),
) -> Result(#(Store(payload), snapshot.Repository), StoreError) {
  use revision <- result.try(history_revision(filesystem.server))
  Ok(#(
    event_store(filesystem, Some(revision)),
    snapshot_repository(filesystem, revision),
  ))
}

fn snapshot_repository(
  filesystem: Filesystem(payload),
  revision: Int,
) -> snapshot.Repository {
  snapshot.Repository(
    load: fn(key) {
      load_snapshot(filesystem.server, revision, snapshot.name(key))
    },
    save: fn(key, observed, value) {
      save_snapshot(
        filesystem.server,
        revision,
        snapshot.name(key),
        observed,
        value,
      )
    },
  )
}

pub fn as_store(filesystem: Filesystem(payload)) -> Store(payload) {
  event_store(filesystem, None)
}

fn event_store(
  filesystem: Filesystem(payload),
  revision: Option(Int),
) -> Store(payload) {
  store.Store(
    fn(id, version) {
      read(filesystem.server, revision, id, stream.event_count(version))
      |> result.map(fn(events) { decode_events(events, filesystem.codec) })
    },
    append_events(filesystem, revision, _),
  )
}

/// Enumerates events in this replica's persistent ingestion order.
pub fn projection_reader(
  filesystem: Filesystem(payload),
) -> projection.Reader(payload) {
  fn(filter, page) {
    use high <- result.try(projection_frontier(filesystem.server))
    projection_scan.read(
      filter,
      page,
      high,
      fn(first, last) { projection_fetch(filesystem.server, first, last) },
      decode_event(_, filesystem.codec),
    )
  }
}

/// Stores progress under the journal's local metadata directory.
pub fn projection_checkpoints(
  filesystem: Filesystem(payload),
) -> projection.Checkpoints {
  projection.FencedCheckpoints(
    projection_load(filesystem.server, _),
    fn(name, position) { projection_save(filesystem.server, name, position) },
    fn(name, position, fence) {
      projection_fenced_save(filesystem.server, name, position, fence)
    },
  )
}

/// Open checkpoints independently of the event journal's writer lock.
pub fn open_projection_checkpoints(
  config: Config,
) -> Result(projection.Checkpoints, OpenError) {
  use directory <- result.map(open_checkpoint_directory(
    config.root,
    config.sync,
  ))
  projection.FencedCheckpoints(
    standalone_projection_load(directory, _),
    fn(name, position) {
      standalone_projection_save(directory, config.sync, name, position)
    },
    fn(name, position, fence) {
      standalone_projection_fenced_save(
        directory,
        config.sync,
        name,
        position,
        fence,
      )
    },
  )
}

/// Open named OS advisory locks without opening the event journal.
pub fn open_projection_coordinator(
  root: String,
) -> Result(projection_runner.Coordinator, OpenError) {
  use directory <- result.map(open_lock_directory(root))
  standalone_projection_acquire(directory, _)
}

/// Uses named OS advisory locks for the legacy runner.
pub fn projection_backend(
  filesystem: Filesystem(payload),
) -> projection_runner.Backend(payload) {
  projection_runner.Backend(
    projection_reader(filesystem),
    projection_checkpoints(filesystem),
    acquire_projection(filesystem.server, _),
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

fn decode_events(
  events: List(Event(BitArray)),
  codec: Codec(payload),
) -> EventStream(payload) {
  store.EventStream(fn() {
    case events {
      [] -> Ok(store.End)
      [event, ..rest] -> {
        use event <- result.try(decode_event(event, codec))
        Ok(store.Next(event, decode_events(rest, codec)))
      }
    }
  })
}

fn append_events(
  filesystem: Filesystem(payload),
  revision: Option(Int),
  writes: store.Writes(payload),
) -> Result(Nil, StoreError) {
  use events <- result.try(encode_events(
    store.entries(writes),
    filesystem.codec,
  ))
  let expected = dict.to_list(store.expected_versions(writes))
  append(filesystem.server, revision, expected, events)
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
  revision: Option(Int),
  id: Id,
  after: Int,
) -> Result(List(Event(BitArray)), StoreError)

@external(erlang, "eventcore_filesystem_ffi", "append")
fn append(
  server: Server,
  revision: Option(Int),
  expected: List(#(Id, Version)),
  events: List(Event(BitArray)),
) -> Result(Nil, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "load_snapshot")
fn load_snapshot(
  server: Server,
  revision: Int,
  name: String,
) -> Result(Option(snapshot.Snapshot), StoreError)

@external(erlang, "eventcore_filesystem_ffi", "save_snapshot")
fn save_snapshot(
  server: Server,
  revision: Int,
  name: String,
  observed: Option(snapshot.Snapshot),
  value: snapshot.Snapshot,
) -> Result(Nil, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "history_revision")
fn history_revision(server: Server) -> Result(Int, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "projection_frontier")
fn projection_frontier(server: Server) -> Result(Int, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "projection_fetch")
fn projection_fetch(
  server: Server,
  first: Int,
  last: Int,
) -> Result(List(#(Int, Event(BitArray))), StoreError)

@external(erlang, "eventcore_filesystem_ffi", "projection_load")
fn projection_load(
  server: Server,
  name: String,
) -> Result(Option(projection.Position), StoreError)

@external(erlang, "eventcore_filesystem_ffi", "projection_save")
fn projection_save(
  server: Server,
  name: String,
  position: projection.Position,
) -> Result(Nil, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "projection_acquire")
fn acquire_projection(
  server: Server,
  name: String,
) -> Result(projection_runner.Lease, projection_runner.LeadershipError)

@external(erlang, "eventcore_filesystem_ffi", "open_projection_checkpoints")
fn open_checkpoint_directory(
  root: String,
  sync: Sync,
) -> Result(String, OpenError)

@external(erlang, "eventcore_filesystem_ffi", "standalone_projection_load")
fn standalone_projection_load(
  directory: String,
  name: String,
) -> Result(Option(projection.Position), StoreError)

@external(erlang, "eventcore_filesystem_ffi", "standalone_projection_save")
fn standalone_projection_save(
  directory: String,
  sync: Sync,
  name: String,
  position: projection.Position,
) -> Result(Nil, StoreError)

@external(erlang, "eventcore_filesystem_ffi", "open_projection_coordinator")
fn open_lock_directory(root: String) -> Result(String, OpenError)

@external(erlang, "eventcore_filesystem_ffi", "standalone_projection_acquire")
fn standalone_projection_acquire(
  directory: String,
  name: String,
) -> Result(projection_runner.Lease, projection_runner.LeadershipError)

@external(erlang, "eventcore_filesystem_ffi", "projection_fenced_save")
fn projection_fenced_save(
  server: Server,
  name: String,
  position: projection.Position,
  fence: projection.CheckpointFence,
) -> Result(Nil, projection.CheckpointWriteError)

@external(erlang, "eventcore_filesystem_ffi", "standalone_projection_fenced_save")
fn standalone_projection_fenced_save(
  directory: String,
  sync: Sync,
  name: String,
  position: projection.Position,
  fence: projection.CheckpointFence,
) -> Result(Nil, projection.CheckpointWriteError)
