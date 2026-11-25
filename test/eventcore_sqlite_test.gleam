import eventcore/projection
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/sqlite
import eventcore/store
import eventcore/stream
import eventcore/testing/adapter_contract as store_contract
import gleam/list
import gleam/option.{None, Some}
import gleam/result

fn binary_codec() -> sqlite.Codec(BitArray) {
  sqlite.Codec(fn(_, value) { Ok(value) }, fn(_, bytes) { Ok(bytes) })
}

fn integer_codec() -> sqlite.Codec(Int) {
  sqlite.Codec(fn(_, value) { Ok(<<value:32>>) }, fn(_, bytes) {
    case bytes {
      <<value:32>> -> Ok(value)
      _ -> Error("expected a 32-bit integer")
    }
  })
}

pub fn memory_contract_test() {
  let assert Ok(db) = sqlite.open_memory(integer_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert sqlite.migrate(db) == Ok(Nil)
  store_contract.check_atomic_streams(sqlite.as_store(db))
  let assert Ok(checkpoints) = sqlite.open_memory_checkpoints()
  assert sqlite.migrate_checkpoints(checkpoints) == Ok(Nil)
  let backend = sqlite.projection_backend_with_checkpoints(db, checkpoints)
  store_contract.check_read_only_participant(sqlite.as_store(db))
  store_contract.check_snapshots(sqlite.snapshots(db))
  store_contract.check_replay(sqlite.as_store(db), backend.reader)
  store_contract.check_checkpoints(backend.checkpoints)
  store_contract.check_coordination(backend.acquire, backend.acquire)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)
}

pub fn encryption_matches_the_selected_backend_test() {
  case sqlite_test_backend() {
    "sqlcipher" -> encrypted_database_and_checkpoints_reopen()
    "plain" -> {
      let path = temp_database()
      let key = Some("passphrase")
      let assert Error(sqlite.EncryptionUnavailable) =
        sqlite.open_file(path, key, binary_codec())
      assert sqlite.open_checkpoints(path, key)
        == Error(sqlite.EncryptionUnavailable)
      assert delete_database(path) == Nil
    }
    _ ->
      panic as "Expected EVENTCORE_SQLITE_TEST_BACKEND to be plain or sqlcipher"
  }
}

fn encrypted_database_and_checkpoints_reopen() {
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

@external(erlang, "eventcore_sqlite_test_ffi", "sqlite_test_backend")
fn sqlite_test_backend() -> String

@external(erlang, "eventcore_sqlite_test_ffi", "delete_database")
fn delete_database(path: String) -> Nil

pub fn supplied_connection_keeps_its_pragmas_test() {
  let connection = open_supplied()
  let assert Ok(db) = sqlite.from_connection(connection, integer_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert pragmas(connection) == #(1, "memory", 1)
  store_contract.check_atomic_streams(sqlite.as_store(db))
  assert pragmas(connection) == #(1, "memory", 1)
  assert sqlite.close(db) == Ok(Nil)
}

pub fn supplied_checkpoint_writes_accept_count_rows_test() {
  let connection = open_supplied()
  let assert Ok(checkpoints) = sqlite.checkpoints_from_connection(connection)
  assert sqlite.migrate_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.save_checkpoint(checkpoints, "projection", 1) == Ok(Nil)
  assert sqlite.save_checkpoint(checkpoints, "projection", 2) == Ok(Nil)
  assert sqlite.load_checkpoint(checkpoints, "projection") == Ok(Some(2))
  assert sqlite.reset_checkpoint(checkpoints, "projection") == Ok(Nil)
  assert sqlite.reset_checkpoint(checkpoints, "projection") == Ok(Nil)
  assert sqlite.load_checkpoint(checkpoints, "projection") == Ok(None)
  assert pragmas(connection) == #(1, "memory", 1)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
}

pub fn invalid_payload_length_and_padding_return_typed_errors_test() {
  let connection = open_supplied()
  let assert Ok(db) = sqlite.from_connection(connection, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  let assert Ok(id) = stream.id("malformed")
  assert insert_invalid_payloads(connection) == Nil
  let storage = sqlite.as_store(db)
  let zero = stream.initial_version
  let failure = Error(store.DeserializationFailed(id, "Invalid SQLite payload"))
  let assert Ok(events) = storage.read_after(id, zero)
  assert store.collect(events) == failure
  let assert Ok(events) = storage.read_after(id, stream.next_version(zero))
  assert store.collect(events) == failure
  assert sqlite.close(db) == Ok(Nil)
}

@external(erlang, "eventcore_sqlite_test_ffi", "insert_invalid_payloads")
fn insert_invalid_payloads(connection: sqlite.Connection) -> Nil

pub fn file_open_requires_wal_test() {
  let assert Error(sqlite.CannotOpen) = sqlite.open(":memory:", binary_codec())
  let path = temp_database()
  let assert Ok(db) = sqlite.open(path, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert file_journal_mode(path) == "wal"
  assert sqlite.close(db) == Ok(Nil)
  assert delete_database(path) == Nil
}

@external(erlang, "eventcore_sqlite_test_ffi", "file_journal_mode")
fn file_journal_mode(path: String) -> String

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
fn pragmas(connection: sqlite.Connection) -> #(Int, String, Int)

@external(erlang, "eventcore_sqlite_test_ffi", "two_writers")
fn two_writers(
  first: fn() -> Result(Nil, store.StoreError),
  second: fn() -> Result(Nil, store.StoreError),
) -> #(Result(Nil, store.StoreError), Result(Nil, store.StoreError))

pub fn a_database_error_rolls_back_the_batch_and_keeps_the_connection_usable_test() {
  assert_failed_batch_rolls_back(open_supplied(), reject_kind)
}

pub fn an_ignored_write_rolls_back_with_and_without_count_rows_test() {
  let counted = open_supplied()
  let uncounted = open_supplied()
  assert disable_count_changes(uncounted) == Nil
  assert_failed_batch_rolls_back(counted, ignore_kind)
  assert_failed_batch_rolls_back(uncounted, ignore_kind)
}

fn assert_failed_batch_rolls_back(
  connection: sqlite.Connection,
  install_trigger: fn(sqlite.Connection) -> Nil,
) {
  let assert Ok(db) = sqlite.from_connection(connection, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  assert install_trigger(connection) == Nil
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

@external(erlang, "eventcore_sqlite_test_ffi", "ignore_kind")
fn ignore_kind(connection: sqlite.Connection) -> Nil

@external(erlang, "eventcore_sqlite_test_ffi", "disable_count_changes")
fn disable_count_changes(connection: sqlite.Connection) -> Nil

pub fn command_snapshot_reopens_and_prevents_regression_test() {
  let path = temp_database()
  let key = case sqlite_test_backend() {
    "sqlcipher" -> Some("snapshot password")
    _ -> None
  }
  let assert Ok(name) = snapshot.key("command-state")
  let assert Ok(id) = stream.id("account")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let newest = snapshot.Snapshot([snapshot.Checkpoint(id, one, <<5:3>>)])
  let stale = snapshot.Snapshot([snapshot.Checkpoint(id, zero, <<0>>)])
  let assert Ok(db) = sqlite.open_file(path, key, binary_codec())
  assert sqlite.migrate(db) == Ok(Nil)
  let snapshots = sqlite.snapshots(db)
  assert snapshots.load(name) == Ok(None)
  assert snapshots.save(name, None, newest) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)

  let assert Ok(db) = sqlite.open_file(path, key, binary_codec())
  let assert Ok(other) = sqlite.open_file(path, key, binary_codec())
  let snapshots = sqlite.snapshots(db)
  assert sqlite.snapshots(other).save(name, Some(newest), stale) == Ok(Nil)
  assert snapshots.load(name) == Ok(Some(newest))
  assert sqlite.close(other) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)
  assert delete_database(path) == Nil
}

pub fn projection_pages_resume_from_a_separate_sqlite_checkpoint_test() {
  let path = temp_database()
  let checkpoint_path = temp_database()
  let codec = integer_codec()
  let assert Ok(db) = sqlite.open(path, codec)
  let assert Ok(checkpoints) = sqlite.open_checkpoints(checkpoint_path, None)
  assert sqlite.migrate(db) == Ok(Nil)
  assert sqlite.migrate_checkpoints(checkpoints) == Ok(Nil)
  let assert Ok(id) = stream.id("notes/a")
  let first = store.Event(id, "note", 1)
  let last = store.Event(id, "note", 132)
  let entries = [
    first,
    ..list.append(list.repeat(store.Event(id, "ignored", 0), 130), [last])
  ]
  let assert Ok(writes) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(writes) = list.try_fold(entries, writes, store.append)
  assert sqlite.as_store(db).append_events(writes) == Ok(Nil)
  let backend = sqlite.projection_backend_with_checkpoints(db, checkpoints)
  let assert Ok(lease) = backend.acquire("notes")
  let other_view = sqlite.projection_backend_with_checkpoints(db, checkpoints)
  assert other_view.acquire("notes") == Error(projection_runner.AlreadyLeading)
  lease.release()
  let assert Ok(prefix) = projection.prefix("notes/")
  let selection = projection.with_event_type(prefix, "note")
  let assert Ok(page) = projection.page(None, 1)
  let assert Ok(one) = projection.position(1)
  let assert Ok(final) = projection.position(132)
  assert backend.reader(selection, page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(first, one),
      ],
      Some(one),
    ))
  assert backend.checkpoints.save("notes", one) == Ok(Nil)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)

  let assert Ok(db) = sqlite.open(path, codec)
  let assert Ok(checkpoints) = sqlite.open_checkpoints(checkpoint_path, None)
  let backend = sqlite.projection_backend_with_checkpoints(db, checkpoints)
  assert backend.checkpoints.load("notes") == Ok(Some(one))
  let assert Ok(page) = projection.page(Some(one), 2)
  assert backend.reader(selection, page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(last, final),
      ],
      Some(final),
    ))
  assert sqlite.reset_checkpoint(checkpoints, "notes") == Ok(Nil)
  assert backend.checkpoints.load("notes") == Ok(None)
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert sqlite.close(db) == Ok(Nil)
  assert delete_database(path) == Nil
  assert delete_database(checkpoint_path) == Nil
}

pub fn invalid_sqlite_projection_checkpoint_fails_instead_of_replaying_test() {
  let path = temp_database()
  let assert Ok(checkpoints) = sqlite.open_checkpoints(path, None)
  assert sqlite.migrate_checkpoints(checkpoints) == Ok(Nil)
  let progress = sqlite.projection_checkpoints(checkpoints)
  assert progress.load("notes") == Ok(None)
  assert sqlite.save_checkpoint(checkpoints, "notes", 0) == Ok(Nil)
  assert progress.load("notes")
    == Error(store.StoreFailure(store.LoadCheckpoint))
  assert sqlite.close_checkpoints(checkpoints) == Ok(Nil)
  assert delete_database(path) == Nil
}

pub fn sqlite_projection_frontier_excludes_concurrent_commits_until_next_page_test() {
  let path = temp_database()
  let codec = integer_codec()
  let assert Ok(writer) = sqlite.open(path, codec)
  assert sqlite.migrate(writer) == Ok(Nil)
  let assert Ok(id) = stream.id("notes")
  let first = store.Event(id, "note", 1)
  let last_initial = store.Event(id, "note", 2)
  let concurrent = store.Event(id, "note", 3)
  let filler = list.repeat(store.Event(id, "ignored", 0), 128)
  let assert Ok(initial) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(initial) = store.append(initial, first)
  let assert Ok(initial) = list.try_fold(filler, initial, store.append)
  let assert Ok(initial) = store.append(initial, last_initial)
  assert sqlite.as_store(writer).append_events(initial) == Ok(Nil)
  let assert Ok(version) = stream.version(130)
  let assert Ok(next) = store.register(store.writes(), id, version)
  let assert Ok(next) = store.append(next, concurrent)
  let reader_codec =
    sqlite.Codec(codec.encode, fn(kind, bytes) {
      use value <- result.try(codec.decode(kind, bytes))
      case value {
        1 -> {
          let assert Ok(Nil) = sqlite.as_store(writer).append_events(next)
          Ok(value)
        }
        _ -> Ok(value)
      }
    })
  let assert Ok(db) = sqlite.open(path, reader_codec)
  let reader = sqlite.projection_reader(db)
  let assert Ok(page) = projection.page(None, 3)
  let assert Ok(one) = projection.position(1)
  let assert Ok(last_position) = projection.position(130)
  let selection = projection.with_event_type(projection.all(), "note")
  assert reader(selection, page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(first, one),
        projection.Delivery(last_initial, last_position),
      ],
      Some(last_position),
    ))
  let assert Ok(page) = projection.page(Some(last_position), 3)
  let assert Ok(next_position) = projection.position(131)
  assert reader(selection, page)
    == Ok(projection.PageResult(
      [
        projection.Delivery(concurrent, next_position),
      ],
      Some(next_position),
    ))
  assert sqlite.close(db) == Ok(Nil)
  assert sqlite.close(writer) == Ok(Nil)
  assert delete_database(path) == Nil
}
