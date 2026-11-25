//// Filesystem store with immutable checksummed transactions.
//// The host must provide the util-linux flock executable on PATH.
////
//// See the [backend guide](../backends.html) for durability and recovery.

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

/// Durability policy for writes to a journal or checkpoint directory.
pub type Sync {
  /// Flush journal writes and directory updates for crash durability.
  Full
  /// Skip explicit filesystem sync. A crash can lose recent writes.
  Unsynced
}

/// Journal root and sync policy. Use the same root for standalone checkpoint
/// and coordinator components that share a lease.
pub type Config {
  Config(root: String, sync: Sync)
}

/// Policy for interpreting concurrent claims on a stream and recording
/// resolutions. Choose it when opening a journal.
pub type Reconciliation {
  /// Use complete branch histories for explicit conflict resolution.
  FullBranches
  /// Use the Rust 2.1.0 compatibility rules for fork detection and resolution.
  RustRelease
}

/// Supply a stable binary representation for each event kind.
pub type Codec(payload) {
  Codec(
    encode: fn(String, payload) -> Result(BitArray, String),
    decode: fn(String, BitArray) -> Result(payload, String),
  )
}

/// The journal or a standalone filesystem component could not be opened.
pub type OpenError {
  /// Another handle or process holds the journal writer lock.
  StoreLocked
  /// The required flock executable was unavailable.
  LockExecutableMissing
  /// The supplied replica identity was blank after trimming.
  InvalidReplicaId
  /// The directory, lock, or journal could not be prepared.
  CannotOpen
}

/// The journal inspection results. Integrity failures identify damaged or
/// unreadable transaction files. Dangling entries and forks describe the
/// transaction graph recovered from the remaining files.
pub type Status {
  Status(
    integrity_failures: List(String),
    dangling: List(Dangling),
    forks: List(Fork),
  )
}

/// A transaction that refers to one or more unavailable parent transactions.
/// Recover or import the missing parents to restore a complete history.
pub type Dangling {
  Dangling(transaction: String, missing_parents: List(String))
}

/// Concurrent transaction claims on one stream from a shared base version.
/// The transaction IDs identify the claims being considered for resolution.
pub type Fork {
  Fork(stream: Id, base_version: Version, transactions: List(String))
}

/// Decoded event evidence for one transaction in a fork. The reconciliation
/// policy determines the extent of its event history.
pub type Branch(payload) {
  Branch(transaction: String, events: List(Event(payload)))
}

/// The fork and its decoded branches as seen by a reconciliation callback.
pub type ForkContext(payload) {
  ForkContext(fork: Fork, branches: List(Branch(payload)))
}

/// The application decision returned for a fork. A resolution is recorded as
/// a journal transaction, preserving the existing branch evidence.
pub type Resolution(payload) {
  /// Supply events to attach to the joined branch evidence.
  Resolve(events: List(Event(payload)))
  /// Leave this fork open for later inspection or resolution.
  Unresolvable(reason: String)
}

/// The count of resolutions recorded by this call and streams for which the
/// resolver declined a fork.
pub type ReconcileReport {
  ReconcileReport(resolved_count: Int, unresolved_streams: List(Id))
}

/// A failure while inspecting or reconciling the journal.
pub type JournalError {
  /// The journal server is unavailable.
  JournalUnavailable
  /// Two independent transaction histories claim the same replica ID.
  ReplicaIdentityConflict(replica: String, first: String, second: String)
  /// The graph changed while a resolution was in progress. Inspect a fresh view.
  ReconciliationChanged
  /// The prefix length is negative or exceeds the available canonical history.
  InvalidEventPrefix
  /// Event encoding, decoding, or a store operation failed.
  JournalStoreError(StoreError)
}

/// An open journal and its event codec. Keep the opening process alive and
/// close the handle when the application no longer needs the journal lock.
pub opaque type Filesystem(payload) {
  Filesystem(
    server: Server,
    codec: Codec(payload),
    reconciliation: Reconciliation,
  )
}

type Server

/// Open a journal with full sync and the full-branch reconciliation policy.
/// The root is created if necessary and held under a writer lock.
pub fn open(
  root: String,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  open_with_config(Config(root, Full), codec)
}

/// Open a journal with the supplied root and sync policy, using the default
/// replica identity and full-branch reconciliation.
pub fn open_with_config(
  config: Config,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  open_with_reconciliation(config, None, FullBranches, codec)
}

/// Open with a caller-provided replica identity and full-branch
/// reconciliation. Supply an identity unique to this replica when exchanging
/// transactions with other journals.
pub fn open_with_replica(
  config: Config,
  replica: String,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  open_with_reconciliation(config, Some(replica), FullBranches, codec)
}

/// Select the fork and append semantics for this journal handle. RustRelease
/// replays the published release rules, including an ordinary append closing
/// a fork when it joins all the claiming transactions.
pub fn open_with_reconciliation(
  config: Config,
  replica: Option(String),
  reconciliation: Reconciliation,
  codec: Codec(payload),
) -> Result(Filesystem(payload), OpenError) {
  let replica = option.map(replica, string.trim)
  case replica {
    Some("") -> Error(InvalidReplicaId)
    _ ->
      start(config.root, config.sync, replica, reconciliation)
      |> result.map(Filesystem(_, codec, reconciliation))
  }
}

/// Stop the journal server and release the writer lock. Stop dependent
/// operations before closing their handle.
pub fn close(filesystem: Filesystem(payload)) -> Result(Nil, StoreError) {
  stop(filesystem.server)
  |> result.replace_error(store.StoreFailure(store.CloseStore))
}

/// Inspect damaged transactions, missing parents, and detectable forks in
/// the open journal. A replica identity conflict can prevent fork detection.
pub fn status(filesystem: Filesystem(payload)) -> Result(Status, JournalError) {
  journal_status(filesystem.server)
}

/// Read the replica identity used for transactions written by this journal.
pub fn replica_id(
  filesystem: Filesystem(payload),
) -> Result(String, JournalError) {
  journal_replica_id(filesystem.server)
}

/// Find the forks recognized by this journal’s reconciliation policy. An
/// identity shared by independent histories can produce a replica conflict.
pub fn detect_forks(
  filesystem: Filesystem(payload),
) -> Result(List(Fork), JournalError) {
  journal_forks(filesystem.server)
}

/// Count transactions with multiple parents in the current journal graph.
/// Use an event prefix token to compare the canonical order of a consumed
/// event prefix.
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
              case filesystem.reconciliation {
                RustRelease ->
                  reconcile_forks(
                    filesystem,
                    resolver,
                    next,
                    rest,
                    resolved + 1,
                    declined,
                  )
                FullBranches -> {
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

/// Expose the journal as an event store. For snapshot-backed commands that
/// must keep replay and append on one reconciled view, use `command_view`.
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

@external(erlang, "eventcore_filesystem_ffi", "start_with_reconciliation")
fn start(
  root: String,
  sync: Sync,
  replica: Option(String),
  reconciliation: Reconciliation,
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
