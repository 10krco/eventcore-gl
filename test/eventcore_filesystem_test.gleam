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
  assert filesystem.status(journal) == Ok(filesystem.Status([], []))
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
    == Ok(filesystem.Status([parent], [filesystem.Dangling(child, [parent])]))
  assert filesystem.close(journal) == Ok(Nil)

  remove(parent_path)
  let assert Ok(journal) = filesystem.open_with_config(config, codec())
  assert filesystem.status(journal)
    == Ok(filesystem.Status([], [filesystem.Dangling(child, [parent])]))
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
  assert filesystem.status(journal) == Ok(filesystem.Status([malformed], []))
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
  let assert Error(filesystem.ConflictingHistory) =
    filesystem.open(left, codec())
  remove_root(left)
  remove_root(right)
}
