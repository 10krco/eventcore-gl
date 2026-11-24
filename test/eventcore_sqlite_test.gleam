import eventcore/sqlite
import eventcore/store
import eventcore/stream
import gleam/option.{None, Some}
import store_contract

fn binary_codec() -> sqlite.Codec(BitArray) {
  sqlite.Codec(fn(_, value) { Ok(value) }, fn(_, bytes) { Ok(bytes) })
}

pub fn memory_contract_test() {
  let assert Ok(db) =
    sqlite.open_memory(
      sqlite.Codec(fn(_, value) { Ok(<<value:32>>) }, fn(_, bytes) {
        case bytes {
          <<value:32>> -> Ok(value)
          _ -> Error("expected a 32-bit integer")
        }
      }),
    )
  assert sqlite.migrate(db) == Ok(Nil)
  assert sqlite.migrate(db) == Ok(Nil)
  store_contract.check_atomic_streams(sqlite.as_store(db))
  assert sqlite.close(db) == Ok(Nil)
}

pub fn encrypted_database_and_checkpoints_reopen_test() {
  let path = temp_database()
  let key = Some("a'quoted λ passphrase")
  let codec = binary_codec()
  let assert Ok(id) = stream.id("encrypted")
  let event = store.Event(id, "created", <<5:3>>)
  let assert Ok(db) = sqlite.open_file(path, key, codec)
  assert sqlite.migrate(db) == Ok(Nil)
  let assert Ok(writes) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(writes) = store.append(writes, event)
  assert sqlite.as_store(db).append_events(writes) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)
  let assert Error(sqlite.InvalidEncryptionKey) =
    sqlite.open_file(path, Some("wrong"), codec)
  let assert Error(sqlite.CannotOpen) = sqlite.open(path, codec)
  let assert Ok(db) = sqlite.open_file(path, key, codec)
  let assert Ok(events) = store.read_stream(sqlite.as_store(db), id)
  assert store.collect(events) == Ok([event])
  assert sqlite.close(db) == Ok(Nil)

  let checkpoint_path = temp_database()
  let assert Ok(checkpoints) = sqlite.open_checkpoints(checkpoint_path, key)
  assert sqlite.migrate_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.load_checkpoint(checkpoints, "projection") == Ok(None)
  assert sqlite.save_checkpoint(checkpoints, "projection", 1) == Ok(Nil)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.open_checkpoints(checkpoint_path, Some("wrong"))
    == Error(sqlite.InvalidEncryptionKey)
  let assert Ok(checkpoints) = sqlite.open_checkpoints(checkpoint_path, key)
  assert sqlite.load_checkpoint(checkpoints, "projection") == Ok(Some(1))
  assert sqlite.reset_checkpoint(checkpoints, "projection") == Ok(Nil)
  assert sqlite.load_checkpoint(checkpoints, "projection") == Ok(None)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert delete_database(path) == Nil
  assert delete_database(checkpoint_path) == Nil
}

@external(erlang, "eventcore_sqlite_test_ffi", "temp_database")
fn temp_database() -> String

@external(erlang, "eventcore_sqlite_test_ffi", "delete_database")
fn delete_database(path: String) -> Nil

pub fn supplied_connection_keeps_its_pragmas_test() {
  let connection = open_supplied()
  let assert Ok(db) = sqlite.from_connection(connection, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert pragmas(connection) == #(1, "memory")
  assert sqlite.close(db) == Ok(Nil)
}

pub fn independent_connections_contend_on_the_same_expected_version_test() {
  let path = temp_database()
  let assert Ok(first) = sqlite.open(path, binary_codec())
  let assert Ok(second) = sqlite.open(path, binary_codec())
  assert sqlite.migrate(first) == Ok(Nil)
  let assert Ok(id) = stream.id("contended")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(writes) = store.register(store.writes(), id, zero)
  let assert Ok(writes) =
    store.append(writes, store.Event(id, "created", <<42>>))
  let #(a, b) =
    two_writers(fn() { sqlite.as_store(first).append_events(writes) }, fn() {
      sqlite.as_store(second).append_events(writes)
    })
  let conflict = Error(store.VersionConflict(id, zero, one))
  assert #(a, b) == #(Ok(Nil), conflict) || #(a, b) == #(conflict, Ok(Nil))
  let assert Ok(events) = store.read_stream(sqlite.as_store(first), id)
  assert store.collect(events) == Ok([store.Event(id, "created", <<42>>)])
  assert sqlite.close(first) == Ok(Nil)
  assert sqlite.close(second) == Ok(Nil)
  assert delete_database(path) == Nil
}

@external(erlang, "eventcore_sqlite_test_ffi", "open_supplied")
fn open_supplied() -> sqlite.Connection

@external(erlang, "eventcore_sqlite_test_ffi", "pragmas")
fn pragmas(connection: sqlite.Connection) -> #(Int, String)

@external(erlang, "eventcore_sqlite_test_ffi", "two_writers")
fn two_writers(
  first: fn() -> Result(Nil, store.StoreError),
  second: fn() -> Result(Nil, store.StoreError),
) -> #(Result(Nil, store.StoreError), Result(Nil, store.StoreError))

pub fn a_database_error_rolls_back_the_batch_and_keeps_the_connection_usable_test() {
  let connection = open_supplied()
  let assert Ok(db) = sqlite.from_connection(connection, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert reject_kind(connection) == Nil
  let events = sqlite.as_store(db)
  let assert Ok(left) = stream.id("rollback-left")
  let assert Ok(right) = stream.id("rollback-right")
  let assert Ok(writes) =
    store.register(store.writes(), left, stream.initial_version)
  let assert Ok(writes) = store.register(writes, right, stream.initial_version)
  let first = store.Event(left, "created", <<42>>)
  let assert Ok(first_write) = store.append(writes, first)
  let assert Ok(batch) =
    store.append(first_write, store.Event(right, "refused", <<7>>))
  assert events.append_events(batch)
    == Error(store.StoreFailure(store.AppendEvents))
  let assert Ok(left_history) = store.read_stream(events, left)
  let assert Ok(right_history) = store.read_stream(events, right)
  assert store.collect(left_history) == Ok([])
  assert store.collect(right_history) == Ok([])
  assert events.append_events(first_write) == Ok(Nil)
  let assert Ok(left_history) = store.read_stream(events, left)
  assert store.collect(left_history) == Ok([first])
  assert sqlite.close(db) == Ok(Nil)
}

@external(erlang, "eventcore_sqlite_test_ffi", "reject_kind")
fn reject_kind(connection: sqlite.Connection) -> Nil
