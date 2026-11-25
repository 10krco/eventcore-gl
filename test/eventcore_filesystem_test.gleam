import eventcore/command
import eventcore/filesystem
import eventcore/projection
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/store
import eventcore/stream
import eventcore/testing/adapter_contract as store_contract
import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result

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
  let backend = filesystem.projection_backend(journal)
  let assert Ok(#(_, snapshots)) = filesystem.command_view(journal)
  store_contract.check_read_only_participant(storage)
  store_contract.check_snapshots(snapshots)
  store_contract.check_replay(storage, backend.reader)
  store_contract.check_checkpoints(backend.checkpoints)
  store_contract.check_coordination(backend.acquire, backend.acquire)
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

pub fn append_to_another_stream_preserves_complete_fork_branches_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(id) = stream.id("fork")
  let assert Ok(other) = stream.id("unrelated")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  append_value(a, id, zero, 1)
  append_value(a, id, one, 2)
  append_value(b, id, zero, 3)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(before) = filesystem.detect_forks(a)
  let assert [_] = before
  append_value(a, other, zero, 4)
  assert filesystem.detect_forks(a) == Ok(before)
  let three = stream.next_version(stream.next_version(one))
  let assert Ok(writes) = store.register(store.writes(), id, three)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", 5))
  assert filesystem.as_store(a).append_events(writes)
    == Error(store.UnresolvedStreamFork(id))
  assert filesystem.reconcile(a, fn(context) {
      let assert [first, second] = context.branches
      let first = list.map(first.events, fn(event) { event.payload })
      let second = list.map(second.events, fn(event) { event.payload })
      assert #(first, second) == #([1, 2], [3])
        || #(first, second) == #([3], [1, 2])
      filesystem.Resolve([])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.close(a) == Ok(Nil)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.detect_forks(a) == Ok([])
  append_value(a, id, three, 5)
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

pub fn local_resolutions_for_two_streams_are_causally_ordered_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(first) = stream.id("one")
  let assert Ok(second) = stream.id("two")
  let zero = stream.initial_version
  let both = fn(journal, value) {
    let assert Ok(writes) = store.register(store.writes(), first, zero)
    let assert Ok(writes) = store.register(writes, second, zero)
    let assert Ok(writes) =
      store.append(writes, store.Event(first, "added", value))
    let assert Ok(writes) =
      store.append(writes, store.Event(second, "added", value))
    assert filesystem.as_store(journal).append_events(writes) == Ok(Nil)
  }
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  both(a, 1)
  both(b, 2)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(_) { filesystem.Resolve([]) })
    == Ok(filesystem.ReconcileReport(2, []))
  assert filesystem.close(a) == Ok(Nil)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.status(a) == Ok(filesystem.Status([], [], []))
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

pub fn prefix_token_changes_when_import_inserts_an_event_before_a_checkpoint_test() {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(id) = stream.id("local")
  let assert Ok(other) = stream.id("import")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  append_value(a, id, zero, 1)
  let assert Ok(first_token) = filesystem.event_prefix_token(a, 1)
  append_value(a, id, one, 2)
  assert filesystem.event_prefix_token(a, 1) == Ok(first_token)
  let assert Ok(checkpoint) = filesystem.event_prefix_token(a, 2)
  assert filesystem.event_prefix_token(a, 3)
    == Error(filesystem.InvalidEventPrefix)
  append_value(b, other, zero, 3)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.topology_generation(a) == Ok(0)
  let assert Ok(imported_token) = filesystem.event_prefix_token(a, 2)
  assert imported_token != checkpoint
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

pub fn missing_intermediate_parent_does_not_create_a_replica_collision_test() {
  let root = temp_root()
  let assert Ok(id) = stream.id("missing-intermediate")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(a) = filesystem.open(root, codec())
  append_value(a, id, zero, 1)
  let assert [#(first, _)] = transaction_paths(root)
  append_value(a, id, one, 2)
  let assert Ok(#(missing, path)) =
    list.find(transaction_paths(root), fn(file) { file.0 != first })
  append_value(a, id, stream.next_version(one), 3)
  let assert Ok(#(third, _)) =
    list.find(transaction_paths(root), fn(file) {
      file.0 != first && file.0 != missing
    })
  assert filesystem.close(a) == Ok(Nil)
  remove(path)
  let assert Ok(a) = filesystem.open(root, codec())
  assert filesystem.status(a)
    == Ok(filesystem.Status([], [filesystem.Dangling(third, [missing])], []))
  assert filesystem.detect_forks(a) == Ok([])
  assert filesystem.close(a) == Ok(Nil)
  remove_root(root)
}

pub fn late_imports_reopen_forks_with_complete_resolution_evidence_test() {
  let left = temp_root()
  let right = temp_root()
  let third = temp_root()
  let assert Ok(id) = stream.id("late-fork")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  let assert Ok(c) = filesystem.open(third, codec())
  append_value(a, id, zero, 1)
  append_value(b, id, zero, 2)
  append_value(c, id, zero, 4)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  assert filesystem.close(c) == Ok(Nil)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(_) { filesystem.Resolve([]) })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.detect_forks(a) == Ok([])
  assert filesystem.close(a) == Ok(Nil)

  let assert Ok(b) = filesystem.open(right, codec())
  append_value(b, id, one, 3)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok([filesystem.Fork(_, _, [_, _])]) = filesystem.detect_forks(a)
  assert filesystem.reconcile(a, fn(context) {
      let values =
        list.map(context.branches, fn(branch) {
          list.sort(
            list.map(branch.events, fn(event) { event.payload }),
            int.compare,
          )
        })
      assert values == [[1, 2], [2, 3]] || values == [[2, 3], [1, 2]]
      filesystem.Resolve([])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.close(a) == Ok(Nil)

  copy_transactions(third, left)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(context) {
      let values =
        list.map(context.branches, fn(branch) {
          list.sort(
            list.map(branch.events, fn(event) { event.payload }),
            int.compare,
          )
        })
      assert values == [[1, 2, 3], [4]] || values == [[4], [1, 2, 3]]
      filesystem.Resolve([])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.detect_forks(a) == Ok([])
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
  remove_root(third)
}

pub fn cross_stream_resolution_follows_the_imported_output_history_test() {
  let left = temp_root()
  let right = temp_root()
  let third = temp_root()
  let assert Ok(id) = stream.id("resolved")
  let assert Ok(output) = stream.id("output")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let two = stream.next_version(one)
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  let assert Ok(c) = filesystem.open(third, codec())
  append_value(a, id, zero, 10)
  append_value(b, id, zero, 20)
  append_value(c, output, zero, 1)
  append_value(c, output, one, 2)
  append_value(c, output, two, 3)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  assert filesystem.close(c) == Ok(Nil)
  copy_transactions(right, left)
  copy_transactions(third, left)

  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(_) {
      filesystem.Resolve([store.Event(output, "resolution", 4)])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert read_values(a, output) == [1, 2, 3, 4]
  assert filesystem.close(a) == Ok(Nil)
  let assert Ok(a) = filesystem.open(left, codec())
  assert read_values(a, output) == [1, 2, 3, 4]
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
  remove_root(third)
}

pub fn cross_stream_resolution_observes_imported_empty_decisions_test() {
  let left = temp_root()
  let right = temp_root()
  let first_output = temp_root()
  let second_output = temp_root()
  let late_output = temp_root()
  let assert Ok(id) = stream.id("resolved")
  let assert Ok(output) = stream.id("output")
  let zero = stream.initial_version
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  let assert Ok(c) = filesystem.open(first_output, codec())
  let assert Ok(d) = filesystem.open(second_output, codec())
  let assert Ok(e) = filesystem.open(late_output, codec())
  append_value(a, id, zero, 10)
  append_value(b, id, zero, 20)
  append_value(c, output, zero, 1)
  append_value(d, output, zero, 2)
  append_value(e, output, zero, 4)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  assert filesystem.close(c) == Ok(Nil)
  assert filesystem.close(d) == Ok(Nil)
  assert filesystem.close(e) == Ok(Nil)

  copy_transactions(second_output, first_output)
  let assert Ok(c) = filesystem.open(first_output, codec())
  assert filesystem.reconcile(c, fn(_) { filesystem.Resolve([]) })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.close(c) == Ok(Nil)

  copy_transactions(right, left)
  copy_transactions(first_output, left)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(_) {
      filesystem.Resolve([store.Event(output, "resolution", 3)])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.close(a) == Ok(Nil)

  copy_transactions(late_output, left)
  let assert Ok(a) = filesystem.open(left, codec())
  assert filesystem.reconcile(a, fn(context) {
      let filesystem.Fork(stream_id, _, _) = context.fork
      assert stream_id == output
      let values =
        list.map(context.branches, fn(branch) {
          list.sort(
            list.map(branch.events, fn(event) { event.payload }),
            int.compare,
          )
        })
      assert values == [[1, 2, 3], [4]] || values == [[4], [1, 2, 3]]
      filesystem.Resolve([])
    })
    == Ok(filesystem.ReconcileReport(1, []))
  assert filesystem.close(a) == Ok(Nil)
  list.each(
    [left, right, first_output, second_output, late_output],
    remove_root,
  )
}

pub fn command_snapshot_survives_filesystem_reopen_test() {
  let root = temp_root()
  let assert Ok(key) = snapshot.key("account/v1")
  let assert Ok(id) = stream.id("account")
  let zero = stream.initial_version
  let checkpoint =
    snapshot.Snapshot([
      snapshot.Checkpoint(id, stream.next_version(zero), <<5:3>>),
    ])
  let old = snapshot.Snapshot([snapshot.Checkpoint(id, zero, <<0:3>>)])

  let assert Ok(journal) = filesystem.open(root, codec())
  let assert Ok(#(_, snapshots)) = filesystem.command_view(journal)
  assert snapshots.load(key) == Ok(None)
  assert snapshots.save(key, None, checkpoint) == Ok(Nil)
  let assert Ok(Nil) = filesystem.close(journal)

  let assert Ok(journal) = filesystem.open(root, codec())
  let assert Ok(#(_, snapshots)) = filesystem.command_view(journal)
  assert snapshots.load(key) == Ok(Some(checkpoint))
  assert snapshots.save(key, Some(checkpoint), old) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(checkpoint))
  let assert Ok(Nil) = filesystem.close(journal)
  remove_root(root)
}

pub fn command_snapshot_invalidates_when_an_import_reorders_its_prefix_test() {
  let local = temp_root()
  let upstream = temp_root()
  let assert Ok(id) = stream.id("account")
  let assert Ok(other) = stream.id("older")
  let assert Ok(key) = snapshot.key("account/v1")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(journal) = filesystem.open(upstream, codec())
  let assert Ok(writes) = store.register(store.writes(), other, zero)
  let assert Ok(writes) = store.append(writes, store.Event(other, "added", 0))
  assert filesystem.as_store(journal).append_events(writes) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(local, codec())
  let storage = filesystem.as_store(journal)
  let assert Ok(writes) = store.register(store.writes(), id, zero)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", 1))
  assert storage.append_events(writes) == Ok(Nil)
  let checkpoint = snapshot.Snapshot([snapshot.Checkpoint(id, one, <<1>>)])
  let assert Ok(#(_, snapshots)) = filesystem.command_view(journal)
  assert snapshots.save(key, None, checkpoint) == Ok(Nil)
  let assert Ok(writes) = store.register(store.writes(), id, one)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", 2))
  assert storage.append_events(writes) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(checkpoint))
  assert filesystem.close(journal) == Ok(Nil)

  copy_transactions(upstream, local)
  let assert Ok(journal) = filesystem.open(local, codec())
  let assert Ok(#(_, snapshots)) = filesystem.command_view(journal)
  assert snapshots.load(key) == Ok(None)
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(local)
  remove_root(upstream)
}

pub fn command_view_rejects_a_commit_after_reconciliation_test() {
  check_command_view_reconciliation(False)
}

pub fn command_view_rejects_a_snapshot_save_after_reconciliation_test() {
  check_command_view_reconciliation(True)
}

fn check_command_view_reconciliation(refresh_snapshot: Bool) {
  let left = temp_root()
  let right = temp_root()
  let assert Ok(forked) = stream.id("forked")
  let assert Ok(output) = stream.id("output")
  let assert Ok(key) = snapshot.key("reconciled")
  let zero = stream.initial_version
  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(b) = filesystem.open(right, codec())
  append_value(a, forked, zero, 1)
  append_value(b, forked, zero, 2)
  assert filesystem.close(a) == Ok(Nil)
  assert filesystem.close(b) == Ok(Nil)
  copy_transactions(right, left)

  let assert Ok(a) = filesystem.open(left, codec())
  let assert Ok(#(storage, snapshots)) = filesystem.command_view(a)
  let event_count = case refresh_snapshot {
    True -> 100
    False -> 1
  }
  let expected = list.repeat(store.Event(output, "added", 3), event_count)
  let assert Ok(seed) = store.register(store.writes(), output, zero)
  let assert Ok(seed) = list.try_fold(expected, seed, store.append)
  assert storage.append_events(seed) == Ok(Nil)
  let reconcile = fn() {
    assert filesystem.reconcile(a, fn(_) { filesystem.Resolve([]) })
      == Ok(filesystem.ReconcileReport(1, []))
  }
  let snapshot_codec =
    snapshot.Codec(
      fn(total) {
        assert refresh_snapshot
        reconcile()
        Ok(<<total:32>>)
      },
      fn(_) { Error("there is no saved snapshot") },
    )
  let logic =
    command.Command(
      streams: command.single(output),
      initial_state: fn() { 0 },
      apply: fn(total, event) { total + event.payload },
      discover: fn(_) { [] },
      handle: fn(total) {
        assert !refresh_snapshot
        reconcile()
        Ok([store.Event(output, "added", total)])
      },
    )
  assert command.execute_with_snapshot(
      storage,
      logic,
      command.default_policy(),
      snapshots,
      key,
      snapshot_codec,
    )
    == Error(command.StorageError(store.JournalHistoryChanged))
  assert snapshots.load(key) == Error(store.JournalHistoryChanged)
  assert snapshots.save(key, None, snapshot.Snapshot([]))
    == Error(store.JournalHistoryChanged)
  let assert Error(store.JournalHistoryChanged) =
    storage.read_after(output, zero)

  let assert Ok(#(fresh, fresh_snapshots)) = filesystem.command_view(a)
  let assert Ok(tail) = fresh.read_after(output, zero)
  assert store.collect(tail) == Ok(expected)
  assert fresh_snapshots.load(key) == Ok(None)
  assert filesystem.close(a) == Ok(Nil)
  remove_root(left)
  remove_root(right)
}

pub fn projection_resumes_after_import_with_stable_local_positions_test() {
  let local = temp_root()
  let other = temp_root()
  let assert Ok(earlier) = stream.id("other/earlier")
  let assert Ok(local_id) = stream.id("local/only")
  let remote_event = store.Event(earlier, "note", 3)
  let local_first = store.Event(local_id, "note", 10)
  let local_last = store.Event(local_id, "note", 11)

  let assert Ok(remote) = filesystem.open(other, codec())
  write_projection_event(remote, remote_event, stream.initial_version)
  assert filesystem.close(remote) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(local, codec())
  write_projection_event(journal, local_first, stream.initial_version)
  write_projection_event(
    journal,
    local_last,
    stream.next_version(stream.initial_version),
  )
  let backend = filesystem.projection_backend(journal)
  let assert Ok(page) = projection.page(None, 10)
  let assert Ok(one) = projection.position(1)
  let assert Ok(two) = projection.position(2)
  let assert Ok(three) = projection.position(3)
  assert backend.reader(projection.all(), page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(local_first, one),
        projection.Delivery(local_last, two),
      ],
      Some(two),
    ))
  let assert Ok(lease) = backend.acquire("notes")
  assert backend.acquire("notes") == Error(projection_runner.AlreadyLeading)
  lease.release()
  let assert Ok(lease) = backend.acquire("notes")
  lease.release()
  assert backend.checkpoints.save("notes", two) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)

  copy_transactions(other, local)
  let assert Ok(journal) = filesystem.open(local, codec())
  let backend = filesystem.projection_backend(journal)
  assert backend.checkpoints.load("notes") == Ok(Some(two))
  let assert Ok(after) = projection.page(Some(two), 10)
  assert backend.reader(projection.all(), after)
    == Ok(projection.PageResult(
      [
        projection.Delivery(remote_event, three),
      ],
      Some(three),
    ))
  let assert Ok(prefix) = projection.prefix("local/")
  assert backend.reader(prefix, after)
    == Ok(projection.PageResult([], Some(three)))
  assert filesystem.close(journal) == Ok(Nil)

  let assert Ok(journal) = filesystem.open(local, codec())
  let backend = filesystem.projection_backend(journal)
  assert backend.reader(projection.all(), page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(local_first, one),
        projection.Delivery(local_last, two),
        projection.Delivery(remote_event, three),
      ],
      Some(three),
    ))
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(other)
  remove_root(local)
}

fn write_projection_event(
  journal: filesystem.Filesystem(Int),
  event: store.Event(Int),
  expected: stream.Version,
) -> Nil {
  let assert Ok(writes) = store.register(store.writes(), event.stream, expected)
  let assert Ok(writes) = store.append(writes, event)
  let storage = filesystem.as_store(journal)
  let assert Ok(Nil) = storage.append_events(writes)
  Nil
}

pub fn missing_transactions_leave_projection_position_ranges_reserved_test() {
  let local = temp_root()
  let backup = temp_root()
  let assert Ok(backup_journal) = filesystem.open(backup, codec())
  assert filesystem.close(backup_journal) == Ok(Nil)
  let assert Ok(journal) = filesystem.open(local, codec())
  let assert Ok(id) = stream.id("notes")
  let initial = store.Event(id, "note", 10)
  let missing = store.Event(id, "note", 20)
  let last = store.Event(id, "note", 30)
  let one_version = stream.next_version(stream.initial_version)
  write_projection_event(journal, initial, stream.initial_version)
  let assert [#(initial_id, _)] = transaction_paths(local)
  write_projection_event(journal, missing, one_version)
  let assert Ok(#(_, missing_path)) =
    list.find(transaction_paths(local), fn(file) { file.0 != initial_id })
  write_projection_event(journal, last, stream.next_version(one_version))
  let assert Ok(one) = projection.position(1)
  let backend = filesystem.projection_backend(journal)
  assert backend.checkpoints.save("notes", one) == Ok(Nil)
  assert filesystem.close(journal) == Ok(Nil)
  copy_transactions(local, backup)
  remove(missing_path)

  let assert Ok(journal) = filesystem.open(local, codec())
  let backend = filesystem.projection_backend(journal)
  let assert Ok(Some(saved)) = backend.checkpoints.load("notes")
  let assert Ok(page) = projection.page(Some(saved), 10)
  assert backend.reader(projection.all(), page)
    == Error(store.StoreFailure(store.ReadProjection))
  assert backend.checkpoints.load("notes") == Ok(Some(one))
  assert filesystem.close(journal) == Ok(Nil)

  copy_transactions(backup, local)
  let assert Ok(journal) = filesystem.open(local, codec())
  let backend = filesystem.projection_backend(journal)
  let assert Ok(Some(saved)) = backend.checkpoints.load("notes")
  let assert Ok(page) = projection.page(Some(saved), 10)
  let assert Ok(two) = projection.position(2)
  let assert Ok(three) = projection.position(3)
  assert backend.reader(projection.all(), page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(missing, two),
        projection.Delivery(last, three),
      ],
      Some(three),
    ))
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(local)
  remove_root(backup)
}

pub fn recovered_ingestion_requires_the_journal_directory_sync_test() {
  let local = temp_root()
  let other = temp_root()
  let assert Ok(journal) = filesystem.open(other, codec())
  let assert Ok(id) = stream.id("notes")
  let event = store.Event(id, "note", 1)
  write_projection_event(journal, event, stream.initial_version)
  assert filesystem.close(journal) == Ok(Nil)
  let assert Ok(journal) = filesystem.open(local, codec())
  assert filesystem.close(journal) == Ok(Nil)
  copy_transactions(other, local)

  assert open_with_failed_ancestor_sync(local, local <> "/events")
    == Error(filesystem.CannotOpen)
  assert !is_directory(local <> "/.eventcore/projection/ingestion")

  let assert Ok(journal) = filesystem.open(local, codec())
  let assert Ok(page) = projection.page(None, 10)
  let assert Ok(one) = projection.position(1)
  let reader = filesystem.projection_reader(journal)
  assert reader(projection.all(), page)
    == Ok(projection.PageResult([projection.Delivery(event, one)], Some(one)))
  assert filesystem.close(journal) == Ok(Nil)
  remove_root(other)
  remove_root(local)
}

@external(erlang, "eventcore_filesystem_test_ffi", "check_os_lock")
fn check_os_lock(root: String, name: String) -> Nil

@external(erlang, "eventcore_filesystem_test_ffi", "check_holder_exit")
fn check_holder_exit(
  acquire: projection_runner.Coordinator,
  name: String,
) -> Nil

pub fn standalone_projection_state_shares_journal_checkpoint_and_os_lock_test() {
  let root = temp_root()
  let config = filesystem.Config(root, filesystem.Full)
  let assert Ok(checkpoints) = filesystem.open_projection_checkpoints(config)
  let assert Ok(coordinate) = filesystem.open_projection_coordinator(root)
  let name = "projection/日本語"
  let assert Ok(first) = projection.position(1)
  let assert Ok(second) = projection.position(2)
  assert checkpoints.load(name) == Ok(None)
  assert checkpoints.save(name, first) == Ok(Nil)
  let assert Ok(journal) = filesystem.open(root, codec())
  let backend = filesystem.projection_backend(journal)
  assert backend.checkpoints.load(name) == Ok(Some(first))
  assert backend.checkpoints.save(name, second) == Ok(Nil)
  let assert Ok(reopened) = filesystem.open_projection_checkpoints(config)
  assert reopened.load(name) == Ok(Some(second))
  assert reopened.load("other") == Ok(None)

  let assert Ok(lease) = backend.acquire(name)
  assert coordinate(name) == Error(projection_runner.AlreadyLeading)
  check_os_lock(root, name)
  let assert Ok(other) = coordinate("other")
  other.release()
  lease.release()
  let assert Ok(lease) = coordinate(name)
  assert backend.acquire(name) == Error(projection_runner.AlreadyLeading)
  lease.release()
  check_holder_exit(coordinate, name)
  let assert Ok(active) = backend.acquire(name)
  assert filesystem.close(journal) == Ok(Nil)
  let after_close = await_lease(coordinate, name)
  assert projection_runner.check_lease(active)
    == Error(projection_runner.CoordinatorUnavailable)
  active.release()
  assert coordinate(name) == Error(projection_runner.AlreadyLeading)
  after_close.release()
  remove_root(root)
}

@external(erlang, "eventcore_filesystem_test_ffi", "await_lease")
fn await_lease(
  acquire: projection_runner.Coordinator,
  name: String,
) -> projection_runner.Lease

@external(erlang, "eventcore_filesystem_test_ffi", "kill_projection_lock")
fn kill_projection_lock() -> Nil

pub fn projector_stops_without_checkpointing_if_flock_process_exits_test() {
  let root = temp_root()
  let assert Ok(coordinate) = filesystem.open_projection_coordinator(root)
  let assert Ok(id) = stream.id("projected")
  let assert Ok(first) = projection.position(1)
  let assert Ok(second) = projection.position(2)
  let event = store.Event(id, "created", 1)
  let reader = fn(_, _) {
    Ok(projection.PageResult(
      [projection.Delivery(event, first), projection.Delivery(event, second)],
      Some(second),
    ))
  }
  let checkpoints =
    projection.Checkpoints(fn(_) { Ok(None) }, fn(_, _) {
      panic as "a projector without leadership cannot save progress"
    })
  let projector =
    projection_runner.Projector(
      name: "lock-loss",
      filter: projection.all(),
      initial: Nil,
      apply: fn(_, delivery) {
        assert delivery.position == first
        kill_projection_lock()
        #(Nil, Ok(Nil))
      },
      on_error: fn(_) { projection_runner.Fatal },
    )
  assert projection_runner.run(
      projection_runner.Backend(reader, checkpoints, coordinate),
      projector,
      projection_runner.default_config,
      projection_runner.uninterrupted(),
    )
    == Error(projection_runner.Leadership(
      projection_runner.CoordinatorUnavailable,
    ))
  let next = await_lease(coordinate, "lock-loss")
  next.release()
  remove_root(root)
}
