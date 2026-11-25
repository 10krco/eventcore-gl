//// Run against two independently opened pools on the same fresh database.

import eventcore/postgres
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/snapshot
import eventcore/store
import eventcore/stream
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/result
import pog
import store_contract

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
  assert postgres.migrate(events) == Ok(Nil)
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
}

fn check_competing_writers(first: store.Store(Int), second: store.Store(Int)) {
  let assert Ok(id) = stream.id("concurrent")
  let initial = stream.initial_version
  let after = stream.next_version(initial)
  let replies = process.new_subject()
  process.spawn(fn() {
    let batch = single_event(id, initial, 7)
    process.send(replies, #(first, 7, first.append_events(batch)))
  })
  process.spawn(fn() {
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
  assert postgres.migrate_delivery(reopened) == Ok(Nil)
  assert source.read(selection, page, delivery.Unbounded) == Ok([first])

  let assert Ok(_) =
    pog.query(
      "UPDATE eventcore_events SET metadata = $1::text::json
      WHERE event_id = $2::text::uuid",
    )
    |> pog.parameter(pog.text("{\"precision\":0.12345678901234567890123456789}"))
    |> pog.parameter(pog.text(first.event_id))
    |> pog.execute(pool)
  let assert Ok([raw]) = source.read(selection, page, delivery.Unbounded)
  assert raw.metadata == "{\"precision\":0.12345678901234567890123456789}"

  let assert Ok(Some(before)) = source.high_watermark()
  let assert Ok(new_stream) = stream.id("contract/debit/after")
  let assert Ok(batch) =
    store.register(store.writes(), new_stream, stream.initial_version)
  let assert Ok(batch) =
    store.append(batch, store.Event(new_stream, " opened ", 200))
  assert postgres.as_store(events).append_events(batch) == Ok(Nil)
  let assert Ok(Some(boundary)) = source.high_watermark()

  let next_batch =
    single_event(new_stream, stream.next_version(stream.initial_version), 300)
  assert postgres.as_store(reopened).append_events(next_batch) == Ok(Nil)
  let assert Ok(persisted) = delivery.persisted_event_type(" opened ")
  let assert Ok(later) = delivery.event_type("created")
  let assert Ok(exact_selection) =
    delivery.selection(selection_id, prefix, [persisted, later])
  let assert Ok(page) = projection.page(Some(before), 1)
  let assert Ok([at_boundary]) =
    source.read(exact_selection, page, delivery.Inclusive(boundary))
  assert at_boundary.position == boundary
  assert at_boundary.event_type == persisted
  assert at_boundary.event_id != first.event_id
  assert at_boundary.payload == <<200:64>>

  let assert Ok(page) = projection.page(Some(boundary), 1)
  assert source.read(exact_selection, page, delivery.Inclusive(boundary))
    == Ok([])
  let assert Ok([after]) =
    source.read(exact_selection, page, delivery.Unbounded)
  assert after.event_id != at_boundary.event_id
  assert after.payload == <<300:64>>
  assert projection.sequence(after.position)
    == projection.sequence(boundary) + 1
  Nil
}
