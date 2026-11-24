//// Run against two independently opened pools on the same fresh database.

import eventcore/postgres
import eventcore/snapshot
import eventcore/store
import eventcore/stream
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

  assert postgres.reset_checkpoint(second_checkpoints, "ledger") == Ok(Nil)
  assert postgres.load_checkpoint(checkpoints, "ledger") == Ok(None)

  check_competing_writers(
    postgres.as_store(events),
    postgres.as_store(reopened),
  )
  check_trigger_rollback(first, postgres.as_store(reopened))

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
  let assert Ok(batch) =
    store.writes()
    |> store.register(id, initial)
    |> result.try(store.append(_, store.Event(id, "created", 7)))
  let replies = process.new_subject()
  process.spawn(fn() { process.send(replies, first.append_events(batch)) })
  process.spawn(fn() { process.send(replies, second.append_events(batch)) })
  let assert Ok(a) = process.receive(replies, 4000)
  let assert Ok(b) = process.receive(replies, 4000)
  let conflict = Error(store.VersionConflict(id, initial, after))
  assert a == Ok(Nil) && b == conflict || b == Ok(Nil) && a == conflict
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
  let assert Ok(id) = stream.id("rollback")
  let assert Ok(batch) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "created", 1)))
    |> result.try(store.append(_, store.Event(id, "ignored", 2)))
  assert events.append_events(batch)
    == Error(store.StoreFailure(store.AppendEvents))
  let assert Ok(events) = store.read_stream(events, id)
  assert store.collect(events) == Ok([])
}
