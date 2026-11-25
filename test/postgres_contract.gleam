//// Run against two independently opened pools on the same fresh database.

import gleam/string

import eventcore/postgres
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner as runner
import eventcore/snapshot
import eventcore/store
import eventcore/stream
import eventcore/testing/adapter_contract as store_contract
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/option.{None, Some}
import gleam/result
import pog

pub fn check(first: pog.Connection, second: pog.Connection) -> Nil {
  let codec =
    postgres.Codec(fn(_, value: Int) { Ok(<<value:64>>) }, fn(_, bits) {
      case bits {
        <<value:64-signed>> -> Ok(value)
        _ -> Error("invalid test payload")
      }
    })
  let events = postgres.from_pool(first, codec)
  let reopened = postgres.from_pool(second, codec)
  assert postgres.ping(events) == Ok(Nil)
  let storage = postgres.as_store(events)
  let assert Ok(id) = stream.id("contract/empty-append")
  let assert Ok(stale) =
    store.register(
      store.writes(),
      id,
      stream.next_version(stream.initial_version),
    )
  assert storage.append_events(stale) == Ok(Nil)
  assert postgres.migrate(events) == Ok(Nil)
  assert storage.append_events(stale) == Ok(Nil)
  let assert Ok(empty) = store.read_stream(storage, id)
  assert store.collect(empty) == Ok([])
  assert postgres.migrate(reopened) == Ok(Nil)
  store_contract.check_atomic_streams(postgres.as_store(events))

  let assert Ok(debit) = stream.id("contract/debit")
  let assert Ok(debit_events) =
    store.read_stream(postgres.as_store(reopened), debit)
  assert store.collect(debit_events)
    == Ok([
      store.Event(debit, "opened", 100),
      store.Event(debit, "withdrawn", 5),
    ])

  let checkpoints = postgres.checkpoints_from_pool(first)
  let second_checkpoints = postgres.checkpoints_from_pool(second)
  assert postgres.migrate_checkpoints(second_checkpoints) == Ok(Nil)
  assert postgres.load_checkpoint(second_checkpoints, "ledger") == Ok(None)
  assert postgres.save_checkpoint(checkpoints, "ledger", 2) == Ok(Nil)
  assert postgres.load_checkpoint(second_checkpoints, "ledger") == Ok(Some(2))

  check_suppressed_reset(first, second_checkpoints)
  assert postgres.reset_checkpoint(second_checkpoints, "ledger") == Ok(Nil)
  assert postgres.reset_checkpoint(second_checkpoints, "ledger") == Ok(Nil)
  assert postgres.load_checkpoint(checkpoints, "ledger") == Ok(None)

  check_competing_writers(
    postgres.as_store(events),
    postgres.as_store(reopened),
  )
  check_trigger_rollback(first, postgres.as_store(reopened))
  let acquire = postgres.projection_coordinator(first)
  let competing_acquire = postgres.projection_coordinator(second)
  let assert Ok(leader) = acquire("standalone")
  assert competing_acquire("standalone") == Error(runner.AlreadyLeading)
  leader.release()
  let assert Ok(one) = projection.position(1)
  let projection_checkpoints =
    postgres.projection_checkpoints(second_checkpoints)
  let saved = process.new_subject()
  let released = process.new_subject()
  // The integration fixture gives the second pool exactly one connection.
  // The checkpoint save must finish while that connection holds the lock.
  let _ =
    process.spawn(fn() {
      let assert Ok(leader) = competing_acquire("standalone")
      let release = process.new_subject()
      assert projection_checkpoints.save("standalone", one) == Ok(Nil)
      process.send(saved, release)
      assert process.receive(release, 3000) == Ok(Nil)
      leader.release()
      process.send(released, Nil)
    })
  let assert Ok(release) = process.receive(saved, 3000)
  assert acquire("standalone") == Error(runner.AlreadyLeading)
  process.send(release, Nil)
  assert process.receive(released, 3000) == Ok(Nil)
  assert postgres.load_checkpoint(checkpoints, "standalone") == Ok(Some(1))
  check_legacy_projection(events, reopened, second_checkpoints)
  check_committed_delivery(first, events, reopened)

  let assert Ok(key) = snapshot.key("ledger/v1")
  let assert Ok(two) = stream.version(2)
  let assert Ok(one) = stream.version(1)
  let newer = snapshot.Snapshot([snapshot.Checkpoint(debit, two, <<2:2>>)])
  let older = snapshot.Snapshot([snapshot.Checkpoint(debit, one, <<1:2>>)])
  let snapshots = postgres.snapshots(postgres.from_pool(first, codec))
  let second_snapshots = postgres.snapshots(reopened)
  assert snapshots.load(key) == Ok(None)
  assert snapshots.save(key, None, newer) == Ok(Nil)
  assert second_snapshots.save(key, Some(newer), older) == Ok(Nil)
  assert second_snapshots.load(key) == Ok(Some(newer))
  let backend = postgres.projection_backend(events)
  let second_backend = postgres.projection_backend(reopened)
  store_contract.check_read_only_participant(postgres.as_store(events))
  store_contract.check_snapshots(snapshots)
  store_contract.check_replay(postgres.as_store(events), second_backend.reader)
  store_contract.check_checkpoints(second_backend.checkpoints)
  store_contract.check_coordination(backend.acquire, second_backend.acquire)
}

fn check_competing_writers(first: store.Store(Int), second: store.Store(Int)) {
  let assert Ok(id) = stream.id("concurrent")
  let initial = stream.initial_version
  let after = stream.next_version(initial)
  let replies = process.new_subject()
  process.spawn_unlinked(fn() {
    let batch = single_event(id, initial, 7)
    process.send(replies, #(first, 7, first.append_events(batch)))
  })
  process.spawn_unlinked(fn() {
    let batch = single_event(id, initial, 8)
    process.send(replies, #(second, 8, second.append_events(batch)))
  })
  let assert Ok(a) = process.receive(replies, 4000)
  let assert Ok(b) = process.receive(replies, 4000)
  let #(#(_, winner_value, won), #(loser, loser_value, lost)) = case a.2 {
    Ok(Nil) -> #(a, b)
    Error(_) -> #(b, a)
  }
  assert won == Ok(Nil)
  let assert Error(store.VersionConflict(lost_id, expected, actual)) = lost
  assert #(lost_id, expected, actual) == #(id, initial, after)
  assert loser.append_events(single_event(id, actual, loser_value)) == Ok(Nil)
  let assert Ok(events) = store.read_stream(first, id)
  assert store.collect(events)
    == Ok([
      store.Event(id, "created", winner_value),
      store.Event(id, "created", loser_value),
    ])
}

fn single_event(id: stream.Id, version: stream.Version, value: Int) {
  let assert Ok(batch) =
    store.writes()
    |> store.register(id, version)
    |> result.try(store.append(_, store.Event(id, "created", value)))
  batch
}

fn check_suppressed_reset(
  pool: pog.Connection,
  checkpoints: postgres.CheckpointStore,
) -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION eventcore_test_preserve() RETURNS trigger
       LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$",
    )
    |> pog.execute(pool)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER eventcore_test_preserve BEFORE DELETE ON eventcore_checkpoints
       FOR EACH ROW EXECUTE FUNCTION eventcore_test_preserve()",
    )
    |> pog.execute(pool)
  assert postgres.reset_checkpoint(checkpoints, "ledger")
    == Error(postgres.CheckpointFailed)
  assert postgres.load_checkpoint(checkpoints, "ledger") == Ok(Some(2))
  let assert Ok(_) =
    pog.query("DROP TRIGGER eventcore_test_preserve ON eventcore_checkpoints")
    |> pog.execute(pool)
  Nil
}

fn check_trigger_rollback(pool: pog.Connection, events: store.Store(Int)) {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION eventcore_test_ignore() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.kind = 'ignored' THEN RETURN NULL; END IF;
        RETURN NEW;
      END $$",
    )
    |> pog.execute(pool)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER eventcore_test_ignore BEFORE INSERT ON eventcore_events
      FOR EACH ROW EXECUTE FUNCTION eventcore_test_ignore()",
    )
    |> pog.execute(pool)
  let assert Ok(pog.Returned(rows: [last_position], ..)) =
    pog.query("SELECT last_position FROM eventcore_positions WHERE singleton")
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(pool)
  let assert Ok(id) = stream.id("rollback")
  let assert Ok(batch) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "created", 1)))
    |> result.try(store.append(_, store.Event(id, "ignored", 2)))
  assert events.append_events(batch)
    == Error(store.StoreFailure(store.AppendEvents))
  let assert Ok(empty) = store.read_stream(events, id)
  assert store.collect(empty) == Ok([])
  let assert Ok(batch) =
    single_event(id, stream.initial_version, 1)
    |> store.append(store.Event(id, "second", 2))
  assert events.append_events(batch) == Ok(Nil)
  let row = {
    use position <- decode.field(0, decode.int)
    use kind <- decode.field(1, decode.string)
    decode.success(#(position, kind))
  }
  let assert Ok(pog.Returned(rows: positions, ..)) =
    pog.query(
      "SELECT position, kind FROM eventcore_events WHERE stream = $1 ORDER BY position",
    )
    |> pog.parameter(pog.text(stream.name(id)))
    |> pog.returning(row)
    |> pog.execute(pool)
  assert positions
    == [#(last_position + 1, "created"), #(last_position + 2, "second")]
}

fn check_committed_delivery(
  pool: pog.Connection,
  events: postgres.EventStore(Int),
  reopened: postgres.EventStore(Int),
) -> Nil {
  assert postgres.migrate_delivery(events) == Ok(Nil)
  let assert Ok(id) = delivery.source_id("orders-database")
  let source = postgres.projection_source(reopened, id)
  let assert Ok(selection_id) = delivery.selection_id("debit-v1")
  let assert Ok(prefix) = projection.prefix("contract/debit")
  let assert Ok(opened) = delivery.event_type("opened")
  let assert Ok(withdrawn) = delivery.event_type("withdrawn")
  let assert Ok(selection) =
    delivery.selection(selection_id, prefix, [opened, withdrawn])
  let assert Ok(page) = projection.page(None, 1)
  let assert Ok([first]) = source.read(selection, page, delivery.Unbounded)
  assert first.source == id
  assert first.event_type == opened
  assert first.payload == <<100:64>>
  assert first.metadata == "{}"
  assert first.version == stream.next_version(stream.initial_version)
  assert string.slice(first.event_id, 14, 1) == "7"

  // Simulate a database created before the UUIDv7 default was installed.
  let prior_id = "41db1265-8bc1-4ab3-992f-e85799a4af1d"
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE eventcore_events ALTER COLUMN event_id SET DEFAULT gen_random_uuid()",
    )
    |> pog.execute(pool)
  let assert Ok(_) =
    pog.query(
      "UPDATE eventcore_events SET event_id = $1::text::uuid WHERE event_id = $2::text::uuid",
    )
    |> pog.parameter(pog.text(prior_id))
    |> pog.parameter(pog.text(first.event_id))
    |> pog.execute(pool)
  let assert Ok([previous]) = source.read(selection, page, delivery.Unbounded)
  assert previous.event_id == prior_id
  assert postgres.migrate_delivery(reopened) == Ok(Nil)
  assert source.read(selection, page, delivery.Unbounded) == Ok([previous])

  let assert Ok(_) =
    pog.query(
      "UPDATE eventcore_events SET metadata = $1::text::json
      WHERE event_id = $2::text::uuid",
    )
    |> pog.parameter(pog.text("{\"precision\":0.12345678901234567890123456789}"))
    |> pog.parameter(pog.text(prior_id))
    |> pog.execute(pool)
  let assert Ok([raw]) = source.read(selection, page, delivery.Unbounded)
  assert raw.metadata == "{\"precision\":0.12345678901234567890123456789}"

  let before_ms = database_time_ms(pool)
  let assert Ok(Some(before)) = source.high_watermark()
  let assert Ok(new_stream) = stream.id("contract/debit/after")
  let assert Ok(batch) =
    store.register(store.writes(), new_stream, stream.initial_version)
  let assert Ok(batch) =
    store.append(batch, store.Event(new_stream, " opened ", 200))
  assert postgres.as_store(events).append_events(batch) == Ok(Nil)
  let assert Ok(Some(boundary)) = source.high_watermark()

  let one = stream.next_version(stream.initial_version)
  let assert Ok(next_batch) =
    store.writes()
    |> store.register(new_stream, one)
    |> result.try(store.append(_, store.Event(new_stream, "unselected", 250)))
    |> result.try(store.append(_, store.Event(new_stream, "created", 300)))
  assert postgres.as_store(reopened).append_events(next_batch) == Ok(Nil)
  let assert Ok(Some(following)) = source.high_watermark()
  assert projection.sequence(following) == projection.sequence(boundary) + 2
  let assert Ok(persisted) = delivery.persisted_event_type(" opened ")
  let assert Ok(later) = delivery.event_type("created")
  let assert Ok(exact_selection) =
    delivery.selection(selection_id, prefix, [persisted, later])
  let assert Ok(all) = projection.page(Some(before), 2)
  let assert Ok([new_first, new_second]) =
    source.read(exact_selection, all, delivery.Unbounded)
  assert string.slice(new_first.event_id, 14, 1) == "7"
  assert string.slice(new_second.event_id, 14, 1) == "7"
  assert new_first.event_id != new_second.event_id
  let after_ms = database_time_ms(pool)
  let variant = string.slice(new_first.event_id, 19, 1)
  assert variant == "8" || variant == "9" || variant == "a" || variant == "b"
  let timestamp = string.slice(new_first.event_id, 0, 13)
  let assert Ok(timestamp_ms) =
    timestamp |> string.replace("-", "") |> int.base_parse(16)
  assert timestamp_ms >= before_ms && timestamp_ms <= after_ms
  store_contract.check_delivery_boundary(
    source,
    exact_selection,
    before,
    boundary,
    following,
    prior_id,
    persisted,
    <<200:64>>,
    <<300:64>>,
  )

  Nil
}

fn database_time_ms(pool: pog.Connection) -> Int {
  let decoder = {
    use time <- decode.field(0, decode.int)
    decode.success(time)
  }
  let assert Ok(pog.Returned(count: _, rows: [time])) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    |> pog.returning(decoder)
    |> pog.execute(pool)
  time
}

fn check_legacy_projection(events, reopened, second_checkpoints) {
  let primary = postgres.projection_backend(events)
  let secondary = postgres.projection_backend(reopened)
  let assert Ok(lease) = primary.acquire("debit")
  assert secondary.acquire("debit") == Error(runner.AlreadyLeading)
  let blocked = process.new_subject()
  let _ =
    process.spawn(fn() { process.send(blocked, primary.acquire("debit")) })
  assert process.receive(blocked, 1000)
    == Ok(Error(runner.CoordinatorUnavailable))
  let assert Ok(independent) = secondary.acquire("other")
  independent.release()
  lease.release()
  let assert Ok(replacement) = secondary.acquire("debit")
  lease.release()
  assert primary.acquire("debit") == Error(runner.AlreadyLeading)
  replacement.release()

  let ready = process.new_subject()
  let holder =
    process.spawn_unlinked(fn() {
      let assert Ok(_) = primary.acquire("crash")
      process.send(ready, Nil)
      process.sleep_forever()
    })
  let monitor = process.monitor(holder)
  assert process.receive(ready, 1000) == Ok(Nil)
  assert secondary.acquire("crash") == Error(runner.AlreadyLeading)
  process.kill(holder)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  assert process.selector_receive(selector, 1000) == Ok(Nil)
  let assert Ok(after_crash) = acquire_after_crash(secondary.acquire, 100)
  after_crash.release()
  let assert Ok(reconnected) = acquire_after_crash(primary.acquire, 100)
  reconnected.release()

  let assert Ok(filter) = projection.prefix("contract/debit")
  let projector =
    runner.Projector(
      name: "debit",
      filter: filter,
      initial: 0,
      apply: fn(total, delivery) { #(total + delivery.event.payload, Ok(Nil)) },
      on_error: fn(_) { runner.Fatal },
    )
  let control = runner.Control(fn(_) { True })
  assert runner.run(primary, projector, runner.default_config, control)
    == Ok(105)
  assert runner.run(secondary, projector, runner.default_config, control)
    == Ok(0)
  assert postgres.reset_checkpoint(second_checkpoints, "debit") == Ok(Nil)
  assert runner.run(secondary, projector, runner.default_config, control)
    == Ok(105)
}

fn acquire_after_crash(acquire, attempts) {
  case acquire("crash"), attempts {
    Error(runner.AlreadyLeading), remaining
    | Error(runner.CoordinatorUnavailable), remaining
      if remaining > 0
    -> {
      process.sleep(10)
      acquire_after_crash(acquire, remaining - 1)
    }
    result, _ -> result
  }
}

pub fn check_distinct_names(first: pog.Connection, second: pog.Connection) {
  let codec =
    postgres.Codec(fn(_, bytes) { Ok(bytes) }, fn(_, bytes) { Ok(bytes) })
  let primary = postgres.projection_backend(postgres.from_pool(first, codec))
  let secondary = postgres.projection_backend(postgres.from_pool(second, codec))
  let assert Ok(one) = primary.acquire("independent-one")
  let assert Ok(two) = primary.acquire("independent-two")
  // The two OIDs are the high and low halves of FNV-1a("independent-one").
  let assert Ok(owned) =
    pog.query(
      "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
       AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
       AND objsubid = 1 AND classid = 4027170674::oid AND objid = 2137831580::oid",
    )
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(second)
  let assert [owner_pid] = owned.rows
  assert secondary.acquire("independent-one") == Error(runner.AlreadyLeading)
  assert secondary.acquire("independent-two") == Error(runner.AlreadyLeading)
  let assert Ok(terminated) =
    pog.query("SELECT pg_terminate_backend($1, 1000)")
    |> pog.parameter(pog.int(owner_pid))
    |> pog.returning(decode.at([0], decode.bool))
    |> pog.execute(second)
  assert terminated.rows == [True]
  let assert Ok(after_release) = secondary.acquire("independent-one")
  let assert Ok(position) = projection.position(1)
  assert primary.checkpoints.save("independent-one", position)
    == Error(store.StoreFailure(store.SaveCheckpoint))
  assert secondary.checkpoints.load("independent-one") == Ok(None)
  assert primary.checkpoints.save("independent-two", position) == Ok(Nil)
  one.release()
  assert primary.checkpoints.load("independent-two") == Ok(Some(position))
  after_release.release()
  assert secondary.acquire("independent-two") == Error(runner.AlreadyLeading)
  two.release()
}
