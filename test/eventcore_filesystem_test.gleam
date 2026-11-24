import eventcore/filesystem
import eventcore/store
import eventcore/stream
import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result
import store_contract

@external(erlang, "eventcore_filesystem_test_ffi", "temp_root")
fn temp_root() -> String

@external(erlang, "eventcore_filesystem_test_ffi", "remove_root")
fn remove_root(root: String) -> Nil

@external(erlang, "eventcore_filesystem_test_ffi", "transaction_paths")
fn transaction_paths(root: String) -> List(#(String, String))

@external(erlang, "eventcore_filesystem_test_ffi", "tamper")
fn tamper(path: String) -> Nil

@external(erlang, "eventcore_filesystem_test_ffi", "remove")
fn remove(path: String) -> Nil

fn codec() -> filesystem.Codec(Int) {
  filesystem.Codec(
    fn(_, value) { Ok(bit_array.from_string(int.to_string(value))) },
    fn(_, bytes) {
      use text <- result.try(
        bit_array.to_string(bytes) |> result.replace_error("expected text"),
      )
      int.parse(text) |> result.replace_error("expected integer")
    },
  )
}

pub fn full_sync_reopens_atomic_writes_and_locks_exclusively_test() {
  let root = temp_root()
  let assert Ok(journal) = filesystem.open(root, codec())
  let assert Error(filesystem.StoreLocked) = filesystem.open(root, codec())
  let storage = filesystem.as_store(journal)
  store_contract.check_atomic_streams(storage)
  let assert Ok(left) = stream.id("left")
  let assert Ok(right) = stream.id("right")
  let a = store.Event(left, "debit", -35)
  let b = store.Event(right, "credit", 35)
  let assert Ok(batch) =
    store.register(store.writes(), left, stream.initial_version)
  let assert Ok(batch) = store.register(batch, right, stream.initial_version)
  let assert Ok(batch) = store.append(batch, a)
  let assert Ok(batch) = store.append(batch, b)
  assert storage.append_events(batch) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(root, codec())
  let storage = filesystem.as_store(journal)
  let assert Ok(events) = store.read_stream(storage, left)
  assert store.collect(events) == Ok([a])
  let assert Ok(events) = store.read_stream(storage, right)
  assert store.collect(events) == Ok([b])
  assert filesystem.status(journal) == Ok(filesystem.Status([], [], []))
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(root)
}

pub fn corrupt_and_dangling_transactions_have_distinct_status_test() {
  let root = temp_root()
  let config = filesystem.Config(root, filesystem.Unsynced)
  let assert Ok(journal) = filesystem.open_with_config(config, codec())
  let storage = filesystem.as_store(journal)
  let assert Ok(id) = stream.id("chain")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(first) = store.register(store.writes(), id, zero)
  let assert Ok(first) = store.append(first, store.Event(id, "added", 1))
  assert storage.append_events(first) == Ok(Nil)
  let assert [#(parent, parent_path)] = transaction_paths(root)
  let assert Ok(second) = store.register(store.writes(), id, one)
  let second_event = store.Event(id, "added", 2)
  let assert Ok(second) = store.append(second, second_event)
  assert storage.append_events(second) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)
  let assert Ok(#(child, _)) =
    list.find(transaction_paths(root), fn(file) { file.0 != parent })

  tamper(parent_path)
  let assert Ok(journal) = filesystem.open_with_config(config, codec())
  assert filesystem.status(journal)
    == Ok(
      filesystem.Status([parent], [filesystem.Dangling(child, [parent])], []),
    )
  assert filesystem.close(journal) == Ok(Nil)

  remove(parent_path)
  let assert Ok(journal) = filesystem.open_with_config(config, codec())
  assert filesystem.status(journal)
    == Ok(filesystem.Status([], [filesystem.Dangling(child, [parent])], []))
  let assert Ok(events) = store.read_stream(filesystem.as_store(journal), id)
  assert store.collect(events) == Ok([second_event])
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(root)
}

@external(erlang, "eventcore_filesystem_test_ffi", "copy_transactions")
fn copy_transactions(source: String, destination: String) -> Nil

@external(erlang, "eventcore_filesystem_test_ffi", "add_malformed_transaction")
fn add_malformed_transaction(root: String, path: String) -> String

pub fn recovery_reports_malformed_records_and_preserves_healthy_events_test() {
  let parent = temp_root()
  let root = parent <> "/new/journal"
  let assert Ok(journal) = filesystem.open(root, codec())
  let assert Ok(id) = stream.id("healthy")
  let event = store.Event(id, "added", 42)
  let assert Ok(writes) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(writes) = store.append(writes, event)
  assert filesystem.as_store(journal).append_events(writes) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)
  let assert [#(_, path)] = transaction_paths(root)
  let malformed = add_malformed_transaction(root, path)

  let assert Ok(journal) = filesystem.open(root, codec())
  assert filesystem.status(journal)
    == Ok(filesystem.Status([malformed], [], []))
  let assert Ok(events) = store.read_stream(filesystem.as_store(journal), id)
  assert store.collect(events) == Ok([event])
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(parent)
}

pub fn incompatible_transaction_histories_require_reconciliation_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(id) = stream.id("contested")
  let assert Ok(base) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(journal) = filesystem.open(left, codec())
  let assert Ok(writes) = store.append(base, store.Event(id, "added", 1))
  assert filesystem.as_store(journal).append_events(writes) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(right, codec())
  let assert Ok(writes) = store.append(base, store.Event(id, "added", 2))
  assert filesystem.as_store(journal).append_events(writes) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)

  copy_transactions(right, left)
  let assert Ok(journal) = filesystem.open(left, codec())
  let assert Ok([filesystem.Fork(forked, _, [_, _])]) =
    filesystem.detect_forks(journal)
  assert forked == id
  assert filesystem.close(journal) == Ok(Nil)
  let unrelated = temp_root()
  let assert Ok(other) = stream.id("unrelated")
  let assert Ok(journal) = filesystem.open(unrelated, codec())
  let storage = filesystem.as_store(journal)
  let assert Ok(first) =
    store.register(store.writes(), other, stream.initial_version)
  let assert Ok(first) = store.append(first, store.Event(other, "added", 3))
  assert storage.append_events(first) == Ok(Nil)
  let assert [#(_, parent_path)] = transaction_paths(unrelated)
  let one = stream.next_version(stream.initial_version)
  let assert Ok(second) = store.register(store.writes(), other, one)
  let assert Ok(second) = store.append(second, store.Event(other, "added", 4))
  assert storage.append_events(second) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)
  remove(parent_path)
  copy_transactions(unrelated, left)

  let assert Ok(journal) = filesystem.open(left, codec())
  let storage = filesystem.as_store(journal)
  let assert Ok(events) = store.read_stream(storage, id)
  let assert Ok([_, _]) = store.collect(events)
  let two = stream.next_version(one)
  let assert Ok(writes) = store.register(store.writes(), id, two)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", 5))
  let assert Error(store.StoreFailure(_)) = storage.append_events(writes)
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(left)
  remove_root(right)
  remove_root(unrelated)
}

@external(erlang, "eventcore_filesystem_test_ffi", "open_with_failed_ancestor_sync")
fn open_with_failed_ancestor_sync(
  root: String,
  ancestor: String,
) -> Result(Nil, filesystem.OpenError)

@external(erlang, "eventcore_filesystem_test_ffi", "is_directory")
fn is_directory(path: String) -> Bool

pub fn full_open_retries_sync_for_existing_ancestor_entries_test() {
  let parent = temp_root()
  let root = parent <> "/nested/journal"
  assert open_with_failed_ancestor_sync(root, parent)
    == Error(filesystem.CannotOpen)
  assert is_directory(root <> "/events")
  assert open_with_failed_ancestor_sync(root, parent)
    == Error(filesystem.CannotOpen)
  let assert Ok(journal) = filesystem.open(root, codec())
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(parent)
}

fn append_value(
  journal: filesystem.Filesystem(Int),
  id: stream.Id,
  version: stream.Version,
  value: Int,
) {
  let assert Ok(writes) = store.register(store.writes(), id, version)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", value))
  let assert Ok(Nil) = filesystem.as_store(journal).append_events(writes)
  Nil
}

fn read_values(
  journal: filesystem.Filesystem(Int),
  id: stream.Id,
) -> List(Int) {
  let assert Ok(events) = store.read_stream(filesystem.as_store(journal), id)
  let assert Ok(events) = store.collect(events)
  list.map(events, fn(event) { event.payload })
}

pub fn replicas_reconcile_independent_merges_and_converge_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(id) = stream.id("replicated")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  let assert Ok(a_id) = filesystem.replica_id(a)
  let assert Ok(b_id) = filesystem.replica_id(b)
  assert a_id != b_id
  append_value(a, id, zero, 1)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(left, right)

  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  assert filesystem.replica_id(a) == Ok(a_id)
  append_value(a, id, one, 2)
  append_value(b, id, one, 3)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(left, right)
  copy_transactions(right, left)

  let verify_branches = fn(context: filesystem.ForkContext(Int)) {
    assert context.fork.stream == id
    assert context.fork.base_version == one
    let assert [branch1, branch2] = context.branches
    let assert [event1] = branch1.events
    let assert [event2] = branch2.events
    assert event1.payload + event2.payload == 5
    filesystem.Resolve([store.Event(id, "merged", 5)])
  }

  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  assert filesystem.reconcile(a, fn(_) {
      filesystem.Unresolvable("manual review")
    })
    == Ok(filesystem.ReconcileReport(0, [id]))
  assert filesystem.topology_generation(a) == Ok(0)
  assert filesystem.reconcile(a, verify_branches)
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.reconcile(b, verify_branches)
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.topology_generation(a) == Ok(1)
  assert filesystem.detect_forks(a) == Ok([])
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(left, right)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.topology_generation(a) == Ok(2)
  let assert Ok([filesystem.Fork(_, _, [_, _])]) = filesystem.detect_forks(a)
  assert filesystem.reconcile(a, fn(context) {
      assert read_values(a, id) == [1, 2, 3, 5, 5]
        || read_values(a, id) == [1, 3, 2, 5, 5]
      let assert [_, _] = context.branches
      filesystem.Resolve([])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.detect_forks(a) == Ok([])
  assert filesystem.topology_generation(a) == Ok(3)
  let canonical = read_values(a, id)
  assert filesystem.close(a) == Ok(Nil)
  copy_transactions(left, right)

  let assert Ok(b) = filesystem.open(right, codec())
  assert filesystem.status(b) == Ok(filesystem.Status([], [], []))
  assert filesystem.topology_generation(b) == Ok(3)
  assert read_values(b, id) == canonical
  assert filesystem.close(b) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

@external(erlang, "eventcore_filesystem_test_ffi", "copy_replica")
fn copy_replica(source: String, destination: String) -> Nil

pub fn copied_local_replica_identity_rotates_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(a_id) = filesystem.replica_id(a)
  assert filesystem.close(a) == Ok(Nil)
  copy_replica(left, right)
  let assert Ok(b) = filesystem.open(right, codec())
  let assert Ok(b_id) = filesystem.replica_id(b)
  assert a_id != b_id
  assert filesystem.close(b) == Ok(Nil)
  let assert Ok(b) = filesystem.open(right, codec())
  assert filesystem.replica_id(b) == Ok(b_id)
  assert filesystem.close(b) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

pub fn concurrent_use_of_an_explicit_replica_identity_is_reported_test() {
  let left = temp_root()
  let right = temp_root()
  let replica = "manually-configured-replica"
  let assert Ok(id) = stream.id("collision")
  let zero = stream.initial_version
  let config = fn(root) { filesystem.Config(root, filesystem.Full) }
  let assert Ok(a) =
    filesystem.open_with_replica(config(left), replica, codec())
  let assert Ok(b) =
    filesystem.open_with_replica(config(right), replica, codec())
  append_value(a, id, zero, 1)
  append_value(b, id, zero, 2)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)
  let assert Ok(a) =
    filesystem.open_with_replica(config(left), replica, codec())
  let assert Error(filesystem.ReplicaIdentityConflict(colliding, _, _)) =
    filesystem.detect_forks(a)
  assert colliding == replica
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

@external(erlang, "eventcore_filesystem_test_ffi", "downgrade_transaction")
fn downgrade_transaction(path: String) -> Nil

pub fn version_one_transactions_remain_readable_and_extendable_test() {
  let root = temp_root()
  let assert Ok(id) = stream.id("legacy")
  let assert Ok(journal) = filesystem.open(root, codec())
  append_value(journal, id, stream.initial_version, 7)
  assert filesystem.close(journal) == Ok(Nil)
  let assert [#(_, path)] = transaction_paths(root)
  downgrade_transaction(path)

  let assert Ok(journal) = filesystem.open(root, codec())
  assert read_values(journal, id) == [7]
  append_value(journal, id, stream.next_version(stream.initial_version), 8)
  assert filesystem.close(journal) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(root, codec())
  assert read_values(journal, id) == [7, 8]
  assert filesystem.status(journal) == Ok(filesystem.Status([], [], []))
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(root)
}

pub fn resolver_rejects_a_stale_graph_revision_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(id) = stream.id("concurrent-resolution")
  let zero = stream.initial_version
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  append_value(a, id, zero, 1)
  append_value(b, id, zero, 2)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(_) {
      let assert Ok(other) = stream.id("another-stream")
      append_value(a, other, zero, 3)
      filesystem.Resolve([store.Event(id, "stale-resolution", 4)])
    })
    == Error(filesystem.ReconciliationChanged)
  let assert [_, _] = read_values(a, id)
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}
