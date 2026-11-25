import eventcore/internal/postgres_projection_progress as progress
import eventcore/postgres
import eventcore/postgres_projection as transactional
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner
import eventcore/store
import eventcore/stream
import eventcore/testing/transactional_contract
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/result
import pog

pub fn check(first: pog.Connection, second: pog.Connection) -> Nil {
  let codec =
    postgres.Codec(fn(_, value) { Ok(value) }, fn(_, value) { Ok(value) })
  let events = postgres.from_pool(first, codec)
  assert postgres.migrate(events) == Ok(Nil)
  assert postgres.migrate_delivery(events) == Ok(Nil)
  let destination = transactional.destination(first)
  let alternative = transactional.destination(second)
  assert transactional.migrate(destination) == Ok(Nil)
  assert transactional.migrate(alternative) == Ok(Nil)
  let assert Ok(_) =
    pog.query("CREATE TABLE projection_effects (amount bigint NOT NULL)")
    |> pog.execute(first)
  let assert Ok(id) = stream.id("transactional/amounts")
  let assert Ok(batch) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "amount", <<1>>)))
    |> result.try(store.append(_, store.Event(id, "amount", <<2>>)))
    |> result.try(store.append(_, store.Event(id, "amount", <<3>>)))
  assert postgres.as_store(events).append_events(batch) == Ok(Nil)
  let assert Ok(source_id) = delivery.source_id("transactional-store")
  let assert Ok(selection_id) = delivery.selection_id("amount-v1")
  let assert Ok(kind) = delivery.event_type("amount")
  let assert Ok(selection) =
    delivery.selection(selection_id, projection.all(), [kind])
  let source = postgres.projection_source(events, source_id)
  let assert Ok(position1) = projection.position(1)
  let assert Ok(position2) = projection.position(2)
  let assert Ok(position3) = projection.position(3)
  let writer = postgres.from_pool(second, codec) |> postgres.as_store
  transactional_contract.check(
    transactional_contract.Fixture(
      destination:,
      alternative:,
      source:,
      source_id:,
      selection:,
      positions: #(position1, position2, position3),
      insert: fn(tx, amount) { insert(tx, amount) |> result.replace(Nil) },
      clear:,
      effect_count: fn() { count(second) },
      load_progress: fn(name) {
        use observed <- result.map(progress.load(second, name))
        case observed {
          None -> None
          Some(progress.Progress(source, selection, position)) ->
            Some(transactional_contract.Progress(source, selection, position))
        }
      },
      append_live_event: fn() {
        let assert Ok(new_stream) = stream.id("transactional/live")
        let assert Ok(batch) =
          store.writes()
          |> store.register(new_stream, stream.initial_version)
          |> result.try(store.append(
            _,
            store.Event(new_stream, "amount", <<4>>),
          ))
        writer.append_events(batch)
      },
    ),
  )

  let assert Ok(policy) = transactional.retry_policy(1, 100, 2.0, 5000)
  let config =
    transactional.Config(
      ..transactional.config(selection),
      batch_size: 1,
      retry_policy: policy,
    )
  check_idle_fencing(destination, source, config, second)
  check_error_policy_after_rollback(destination, source, config, second)
  check_leadership_loss(destination, source, config, second)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE projection_effects ADD CONSTRAINT projection_amount_unique
      UNIQUE (amount) DEFERRABLE INITIALLY DEFERRED",
    )
    |> pog.execute(second)
  check_commit_rejection(destination, source, config, second, position1, 1)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION reject_projection() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'projection commit rejected by deferred trigger';
      END $$",
    )
    |> pog.execute(second)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER reject_projection
      AFTER INSERT ON projection_effects DEFERRABLE INITIALLY DEFERRED
      FOR EACH ROW WHEN (NEW.amount = 600) EXECUTE FUNCTION reject_projection()",
    )
    |> pog.execute(second)
  check_commit_rejection(destination, source, config, second, position1, 600)
  let assert Ok(_) =
    pog.query(
      "CREATE OR REPLACE FUNCTION reject_projection() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'projection commit rejected with a custom SQLSTATE'
          USING ERRCODE = '22000';
      END $$",
    )
    |> pog.execute(second)
  check_commit_rejection(destination, source, config, second, position1, 600)
  check_commit_disconnect(destination, source, config, second, position1)
}

fn check_idle_fencing(
  destination,
  source: delivery.Source(error),
  config,
  observer,
) {
  let assert Ok(name) = delivery.projector_name("transactional-idle")
  let pages = process.new_subject()
  let empty =
    delivery.Source(
      id: source.id,
      high_watermark: source.high_watermark,
      read: fn(_, page, _) {
        assert projection.page_after(page) == None
        process.send(pages, Nil)
        Ok([])
      },
    )
  let projector =
    transactional.Projector(
      name,
      fn(_) { panic as "empty source decoded an envelope" },
      fn(_, _, _) { panic as "empty source applied an envelope" },
      fn(_) { transactional.Fatal },
    )
  let polls = process.new_subject()
  process.send(polls, Nil)
  let continuous =
    transactional.Config(
      ..config,
      mode: transactional.Continuous(
        projection_runner.Control(fn(_) {
          case process.receive(polls, 0) {
            Ok(Nil) -> {
              assert process.receive(pages, 0) == Ok(Nil)
              True
            }
            Error(Nil) -> {
              assert process.receive(pages, 0) == Error(Nil)
              let assert Ok(pog.Returned(rows: [True], ..)) =
                pog.query(
                  "SELECT pg_terminate_backend(pid, 1000) FROM pg_locks
              WHERE locktype = 'advisory' AND granted AND mode = 'ExclusiveLock'
                AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                AND objsubid = 1",
                )
                |> pog.returning(decode.at([0], decode.bool))
                |> pog.execute(observer)
              True
            }
          }
        }),
      ),
    )
  assert transactional.run(destination, projector, empty, continuous)
    == Error(transactional.LeadershipLost)
  assert progress.load(observer, name) == Ok(None)
}

fn check_error_policy_after_rollback(destination, source, config, observer) {
  let assert Ok(name) = delivery.projector_name("transactional-policy")
  let txs = process.new_subject()
  let lock =
    pog.query(
      "SELECT amount FROM projection_effects
    WHERE amount = 1 FOR UPDATE NOWAIT",
    )
    |> pog.returning(decode.at([0], decode.int))
  let projector =
    transactional.Projector(
      name,
      fn(_) { Ok(Nil) },
      fn(_, _, tx) {
        let assert Ok(pog.Returned(rows: [1], ..)) =
          transactional.execute(tx, lock)
        process.send(txs, tx)
        Error("policy")
      },
      fn(_) {
        let assert Ok(tx) = process.receive(txs, 0)
        assert transactional.execute(tx, lock)
          == Error(transactional.TransactionClosed)
        let assert Ok(pog.Returned(rows: [1], ..)) = pog.execute(lock, observer)
        transactional.Stop
      },
    )
  let assert Ok(position) = projection.position(1)
  assert transactional.run(destination, projector, source, config)
    == Ok(transactional.Stopped(position, 0, 0))
}

fn check_commit_rejection(
  destination,
  source,
  config,
  observer,
  position,
  amount,
) {
  let assert Ok(name) = delivery.projector_name("transactional-rejected")
  let projector =
    transactional.Projector(
      name,
      fn(_) { Ok(Nil) },
      fn(_, _, tx) {
        let assert Ok(_) = insert(tx, amount)
        Ok(fn() { panic as "rejected commit ran the after-commit hook" })
      },
      fn(_) { transactional.Fatal },
    )
  assert transactional.run(destination, projector, source, config)
    == Error(transactional.CommitRolledBack(position))
  assert transactional.reset(
      destination,
      name,
      source.id,
      delivery.selection_identity(config.selection),
      fn(tx) {
        let assert Ok(_) = insert(tx, amount)
        Ok(Nil)
      },
    )
    == Error(transactional.ResetCommitRolledBack)
  assert count(observer) == 4
  assert progress.load(observer, name) == Ok(None)
}

fn check_leadership_loss(destination, source, config, observer) {
  let assert Ok(name) = delivery.projector_name("transactional-fenced-retry")
  let sender = process.new_subject()
  let projector =
    transactional.Projector(
      name,
      fn(_) { Ok(Nil) },
      fn(_, _, tx) {
        let assert Ok(pog.Returned(rows: [pid], ..)) =
          pog.query("SELECT pg_backend_pid()")
          |> pog.returning(decode.at([0], decode.int))
          |> transactional.execute(tx, _)
        let assert Ok(_) = insert(tx, 99)
        process.send(sender, pid)
        Error("retry after losing the backend")
      },
      fn(_) { transactional.Retry },
    )
  let fenced =
    transactional.Config(..config, retry_wait: fn(_) {
      let assert Ok(pid) = process.receive(sender, 0)
      let assert Ok(pog.Returned(rows: [True], ..)) =
        pog.query("SELECT pg_terminate_backend($1, 1000)")
        |> pog.parameter(pog.int(pid))
        |> pog.returning(decode.at([0], decode.bool))
        |> pog.execute(observer)
      Nil
    })
  assert transactional.run(destination, projector, source, fenced)
    == Error(transactional.LeadershipLost)
  assert count(observer) == 4
  assert progress.load(observer, name) == Ok(None)
}

fn check_commit_disconnect(destination, source, config, observer, position1) {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION disconnect_projector() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      PERFORM pg_terminate_backend(pg_backend_pid());
      RETURN NEW;
    END $$",
    )
    |> pog.execute(observer)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER disconnect_projector
    AFTER INSERT ON projection_effects DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.amount = 500) EXECUTE FUNCTION disconnect_projector()",
    )
    |> pog.execute(observer)
  let assert Ok(name) = delivery.projector_name("transactional-disconnect")
  let projector =
    transactional.Projector(
      name,
      fn(_) { Ok(Nil) },
      fn(_, _, tx) {
        let assert Ok(_) = insert(tx, 500)
        Ok(fn() { panic as "commit was not acknowledged" })
      },
      fn(_) { transactional.Fatal },
    )
  assert transactional.run(destination, projector, source, config)
    == Error(transactional.CommitIndeterminate(position1))
  assert count(observer) == 4
  assert progress.load(observer, name) == Ok(None)
  Nil
}

fn insert(transaction: transactional.Transaction, amount: Int) {
  pog.query("INSERT INTO projection_effects(amount) VALUES ($1)")
  |> pog.parameter(pog.int(amount))
  |> transactional.execute(transaction, _)
}

fn clear(transaction: transactional.Transaction) -> Nil {
  let assert Ok(_) =
    pog.query("DELETE FROM projection_effects")
    |> transactional.execute(transaction, _)
  Nil
}

fn count(connection: pog.Connection) -> Int {
  let assert Ok(pog.Returned(rows: [count], ..)) =
    pog.query("SELECT count(*) FROM projection_effects")
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(connection)
  count
}
