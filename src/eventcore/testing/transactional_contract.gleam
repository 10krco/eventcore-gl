//// Reusable PostgreSQL transactional projector behavior checks.

import eventcore/postgres_projection as transactional
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner
import eventcore/store
import gleam/erlang/process
import gleam/option.{type Option, None, Some}

/// The source, selection, and last position recorded for a projector.
pub type Progress {
  Progress(
    source: delivery.SourceId,
    selection: delivery.SelectionId,
    position: projection.Position,
  )
}

/// Supply independent destination handles for the same database. Prepare an
/// empty effect relation and a source with exactly three selected events at
/// the stated increasing positions. Observe effects and progress from the
/// second handle so uncommitted state cannot satisfy the assertions.
pub type Fixture(source_error, progress_error) {
  Fixture(
    destination: transactional.Destination,
    alternative: transactional.Destination,
    source: delivery.Source(source_error),
    source_id: delivery.SourceId,
    selection: delivery.Selection,
    positions: #(projection.Position, projection.Position, projection.Position),
    insert: fn(transactional.Transaction, Int) ->
      Result(Nil, transactional.TransactionQueryError),
    clear: fn(transactional.Transaction) -> Nil,
    effect_count: fn() -> Int,
    load_progress: fn(delivery.ProjectorName) ->
      Result(Option(Progress), progress_error),
    append_live_event: fn() -> Result(Nil, store.StoreError),
  )
}

/// Checks effect and progress atomicity, retries, skip, after-commit,
/// leadership, reset and replay, continuous polling, and terminal decisions.
pub fn check(fixture: Fixture(source_error, progress_error)) -> Nil {
  let Fixture(
    destination:,
    alternative:,
    source:,
    source_id:,
    selection:,
    positions:,
    ..,
  ) = fixture
  let #(position1, position2, position3) = positions
  let selection_id = delivery.selection_identity(selection)
  let assert Ok(name) = delivery.projector_name("transactional-amounts")
  let assert Ok(policy) = transactional.retry_policy(1, 100, 2.0, 5000)
  let signal = process.new_subject()
  let config =
    transactional.Config(
      ..transactional.config(selection),
      batch_size: 1,
      retry_policy: policy,
      retry_wait: fn(delay) {
        assert delay == 100
        assert fixture.effect_count() == 0
        assert fixture.load_progress(name) == Ok(None)
        process.send(signal, Nil)
      },
    )
  let projector =
    transactional.Projector(
      name,
      fn(event) { Ok(event.position) },
      fn(position, _, tx) {
        let assert Ok(Nil) = fixture.insert(tx, projection.sequence(position))
        case position {
          _ if position == position1 ->
            case process.receive(signal, 0) {
              Error(Nil) -> Error("retry")
              Ok(Nil) ->
                Ok(fn() {
                  assert fixture.insert(tx, 999)
                    == Error(transactional.TransactionClosed)
                  assert fixture.effect_count() == 1
                  assert fixture.load_progress(name)
                    == Ok(Some(Progress(source_id, selection_id, position1)))
                  Ok(Nil)
                })
            }
          _ if position == position2 -> Error("skip")
          _ -> Ok(fn() { Error("hook") })
        }
      },
      fn(failure) {
        case failure.error {
          "retry" -> transactional.Retry
          _ -> transactional.Skip
        }
      },
    )
  assert transactional.run(destination, projector, source, config)
    == Error(transactional.AfterCommitFailed(position3, "hook"))
  assert fixture.effect_count() == 2
  assert fixture.load_progress(name)
    == Ok(Some(Progress(source_id, selection_id, position3)))
  assert transactional.run(destination, projector, source, config)
    == Ok(transactional.CaughtUp(0, 0, Some(position3)))

  let assert Ok(candidate) = delivery.selection_id("amount-v2")
  let assert Ok(fallback) = delivery.selection_id("amount-v3")
  let alternate_id = case selection_id == candidate {
    True -> fallback
    False -> candidate
  }
  assert transactional.reset(destination, name, source_id, alternate_id, fn(_) {
      panic as "mismatched reset callback"
    })
    == Error(transactional.ResetSelectionMismatch(selection_id, alternate_id))
  assert transactional.reset(destination, name, source_id, selection_id, fn(tx) {
      fixture.clear(tx)
      Error("rollback")
    })
    == Error(transactional.ResetCallbackFailed("rollback"))
  assert fixture.effect_count() == 2
  assert fixture.load_progress(name)
    == Ok(Some(Progress(source_id, selection_id, position3)))

  let replay =
    transactional.Projector(
      name,
      fn(event) { Ok(event.position) },
      fn(position, _, tx) {
        let assert Ok(Nil) = fixture.insert(tx, projection.sequence(position))
        Ok(transactional.no_after_commit)
      },
      fn(_) { transactional.Fatal },
    )
  assert transactional.reset_and_replay(
      destination,
      replay,
      source,
      config,
      fn(tx) {
        assert transactional.reset(
            alternative,
            name,
            source_id,
            selection_id,
            fn(_) { panic as "concurrent reset callback" },
          )
          == Error(transactional.ResetLeadership(
            projection_runner.AlreadyLeading,
          ))
        fixture.clear(tx)
        Ok(Nil)
      },
    )
    == Ok(transactional.CaughtUp(3, 0, Some(position3)))
  assert fixture.effect_count() == 3
  let polls = process.new_subject()
  process.send(polls, Nil)
  let continuous =
    transactional.Config(
      ..config,
      mode: transactional.Continuous(
        projection_runner.Control(fn(interval) {
          assert interval == 1000
          case process.receive(polls, 0) {
            Error(Nil) -> False
            Ok(Nil) -> {
              assert fixture.append_live_event() == Ok(Nil)
              True
            }
          }
        }),
      ),
    )
  assert transactional.run(destination, replay, source, continuous)
    == Ok(transactional.Cancelled(1, 0))
  assert fixture.effect_count() == 4

  let assert Ok(failure_name) = delivery.projector_name("transactional-failure")
  let fails =
    transactional.Projector(
      failure_name,
      fn(event) { Ok(event.position) },
      fn(_, _, tx) {
        let assert Ok(Nil) = fixture.insert(tx, 99)
        Error("apply")
      },
      fn(_) { transactional.Stop },
    )
  assert transactional.run(destination, fails, source, config)
    == Ok(transactional.Stopped(position1, 0, 0))
  assert transactional.run(
      destination,
      transactional.Projector(..fails, on_error: fn(_) { transactional.Fatal }),
      source,
      config,
    )
    == Error(transactional.ApplicationFailed(position1, "apply"))
  assert transactional.run(
      destination,
      transactional.Projector(..fails, on_error: fn(_) { transactional.Retry }),
      source,
      transactional.config(selection),
    )
    == Error(transactional.RetryExhausted(position1, 1, "apply"))
  assert fixture.effect_count() == 4
  assert fixture.load_progress(failure_name) == Ok(None)
}
