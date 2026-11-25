//// PostgreSQL transactional projection runner.

import eventcore/internal/postgres_projection_progress as progress
import eventcore/internal/postgres_projection_session as session
import eventcore/projection.{type Position}
import eventcore/projection_delivery as delivery
import eventcore/projection_runner as legacy
import gleam/float
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/result
import pog

pub opaque type Destination {
  Destination(pool: pog.Connection)
}

pub fn destination(pool: pog.Connection) -> Destination {
  Destination(pool)
}

pub fn migrate(
  destination: Destination,
) -> Result(Nil, pog.TransactionError(pog.QueryError)) {
  progress.migrate(destination.pool)
}

pub type Mode {
  Batch
  Continuous(legacy.Control)
}

pub type Config {
  Config(
    selection: delivery.Selection,
    mode: Mode,
    batch_size: Int,
    poll_interval: Int,
    retry_policy: RetryPolicy,
    retry_wait: fn(Int) -> Nil,
  )
}

pub opaque type RetryPolicy {
  RetryPolicy(retries: Int, initial: Int, multiplier: Float, maximum: Int)
}

pub fn retry_policy(
  retries: Int,
  initial_ms: Int,
  multiplier: Float,
  maximum_ms: Int,
) -> Result(RetryPolicy, Nil) {
  case
    retries >= 0
    && retries < 4_294_967_295
    && initial_ms >= 0
    && maximum_ms >= 0
    && multiplier >=. 1.0
    && multiplier <=. 1.7976931348623157e308
  {
    True -> Ok(RetryPolicy(retries, initial_ms, multiplier, maximum_ms))
    False -> Error(Nil)
  }
}

pub fn config(selection: delivery.Selection) -> Config {
  let legacy.Control(wait) = legacy.uninterrupted()
  Config(selection, Batch, 100, 1000, RetryPolicy(0, 100, 2.0, 30_000), fn(ms) {
    let _ = wait(ms)
    Nil
  })
}

pub type Decision {
  Retry
  Skip
  Stop
  Fatal
}

pub type Failure(error) {
  Failure(position: Position, error: error, attempt: Int)
}

pub type Transaction =
  session.Transaction

pub type TransactionQueryError {
  TransactionClosed
  QueryFailed(pog.QueryError)
}

/// Runs a query on the transaction active in the current callback.
/// Keep transaction and advisory lock control with the runner.
pub fn execute(
  transaction: Transaction,
  query: pog.Query(row),
) -> Result(pog.Returned(row), TransactionQueryError) {
  use connection <- result.try(
    session.connection(transaction) |> result.replace_error(TransactionClosed),
  )
  pog.execute(query, connection) |> result.map_error(QueryFailed)
}

pub type Projector(event, error) {
  Projector(
    name: delivery.ProjectorName,
    decode: fn(delivery.Envelope) -> Result(event, error),
    apply: fn(event, Position, Transaction) ->
      Result(fn() -> Result(Nil, error), error),
    on_error: fn(Failure(error)) -> Decision,
  )
}

pub fn no_after_commit() -> Result(Nil, error) {
  Ok(Nil)
}

pub type RunError(source_error, error) {
  InvalidConfiguration
  Leadership(legacy.LeadershipError)
  LeadershipLost
  SourceRead(source_error)
  InvalidDelivery(Position)
  SourceMismatch(persisted: delivery.SourceId, configured: delivery.SourceId)
  SelectionMismatch(
    persisted: delivery.SelectionId,
    configured: delivery.SelectionId,
  )
  ProgressFailed(position: Option(Position), error: progress.Error)
  DecodeFailed(position: Position, error: error)
  ApplicationFailed(position: Position, error: error)
  RetryExhausted(position: Position, attempts: Int, error: error)
  CommitRolledBack(Position)
  CommitIndeterminate(Position)
  AfterCommitFailed(position: Position, error: error)
}

pub type Outcome {
  CaughtUp(processed: Int, skipped: Int, through: Option(Position))
  Stopped(position: Position, processed: Int, skipped: Int)
  Cancelled(processed: Int, skipped: Int)
}

type Runner(event, source_error, error) {
  Runner(
    leader: session.Leader,
    projector: Projector(event, error),
    source: delivery.Source(source_error),
    config: Config,
  )
}

type Cursor {
  Cursor(after: Option(Position), processed: Int, skipped: Int)
}

type Drain {
  Drained(Cursor)
  PendingStop(Position, Cursor)
}

type DeliveryOutcome {
  Applied
  Skipped
  Suppressed
  Pending
}

/// Holds destination leadership for the whole run. Projector writes must use
/// the supplied transaction, and external effects belong in the returned hook.
pub fn run(
  destination: Destination,
  projector: Projector(event, error),
  source: delivery.Source(source_error),
  config: Config,
) -> Result(Outcome, RunError(source_error, error)) {
  case valid_config(config) {
    False -> Error(InvalidConfiguration)
    True ->
      session.with_leader(destination.pool, projector.name, fn(leader) {
        run_with_leader(Runner(leader, projector, source, config))
      })
      |> result.map_error(fn(error) {
        case error {
          session.AcquisitionFailed(error) -> Leadership(error)
          session.ReleaseFailed -> LeadershipLost
        }
      })
      |> result.flatten
  }
}

fn valid_config(config: Config) -> Bool {
  config.batch_size > 0
  && config.batch_size <= 4_294_967_295
  && config.poll_interval > 0
}

pub type ResetError(error) {
  ResetLeadership(legacy.LeadershipError)
  ResetLeadershipLost
  ResetSourceMismatch(
    persisted: delivery.SourceId,
    configured: delivery.SourceId,
  )
  ResetSelectionMismatch(
    persisted: delivery.SelectionId,
    configured: delivery.SelectionId,
  )
  ResetProgressFailed(progress.Error)
  ResetCallbackFailed(error)
  ResetCommitRolledBack
  ResetCommitIndeterminate
}

pub type ResetReplayError(source_error, error, reset_error) {
  ResetFailed(ResetError(reset_error))
  ReplayFailed(RunError(source_error, error))
}

pub fn reset(
  destination: Destination,
  name: delivery.ProjectorName,
  source: delivery.SourceId,
  selection: delivery.SelectionId,
  callback: fn(Transaction) -> Result(Nil, error),
) -> Result(Nil, ResetError(error)) {
  session.with_leader(destination.pool, name, fn(leader) {
    reset_with_leader(leader, name, source, selection, callback)
  })
  |> result.map_error(reset_leadership)
  |> result.flatten
}

pub fn reset_and_replay(
  destination: Destination,
  projector: Projector(event, error),
  source: delivery.Source(source_error),
  config: Config,
  callback: fn(Transaction) -> Result(Nil, reset_error),
) -> Result(Outcome, ResetReplayError(source_error, error, reset_error)) {
  case valid_config(config) {
    False -> Error(ReplayFailed(InvalidConfiguration))
    True ->
      session.with_leader(destination.pool, projector.name, fn(leader) {
        use _ <- result.try(
          reset_with_leader(
            leader,
            projector.name,
            source.id,
            delivery.selection_identity(config.selection),
            callback,
          )
          |> result.map_error(ResetFailed),
        )
        run_with_leader(Runner(leader, projector, source, config))
        |> result.map_error(ReplayFailed)
      })
      |> result.map_error(fn(error) { ResetFailed(reset_leadership(error)) })
      |> result.flatten
  }
}

fn reset_leadership(error: session.LeaderError) -> ResetError(error) {
  case error {
    session.AcquisitionFailed(error) -> ResetLeadership(error)
    session.ReleaseFailed -> ResetLeadershipLost
  }
}

fn reset_with_leader(leader, name, source, selection, callback) {
  use tx <- result.try(
    session.begin(leader) |> result.replace_error(ResetLeadershipLost),
  )
  let changes = {
    use observed <- result.try(
      progress.lock(tx, name) |> result.map_error(ResetProgressFailed),
    )
    use _ <- result.try(validate_identity(
      observed,
      source,
      selection,
      ResetSourceMismatch,
      ResetSelectionMismatch,
    ))
    use _ <- result.try(
      callback(session.scope(leader)) |> result.map_error(ResetCallbackFailed),
    )
    progress.delete(tx, name, observed) |> result.map_error(ResetProgressFailed)
  }
  case changes {
    Ok(Nil) ->
      session.commit(leader)
      |> result.map_error(fn(error) {
        case error {
          session.RolledBack -> ResetCommitRolledBack
          session.SessionFailed -> ResetCommitIndeterminate
        }
      })
    Error(_) -> {
      use _ <- result.try(
        session.rollback(leader) |> result.replace_error(ResetLeadershipLost),
      )
      changes
    }
  }
}

fn run_with_leader(runner: Runner(event, source_error, error)) {
  use tx <- result.try(begin(runner.leader))
  let observed = read_progress(runner, tx, None)
  use _ <- result.try(rollback(runner.leader))
  use after <- result.try(observed)
  cycle(runner, Cursor(after, 0, 0))
}

fn cycle(runner: Runner(event, source_error, error), cursor: Cursor) {
  use through <- result.try(
    runner.source.high_watermark() |> result.map_error(SourceRead),
  )
  let drained = case through {
    None -> Ok(Drained(cursor))
    Some(bound) -> drain(runner, cursor, bound)
  }
  use outcome <- result.try(drained)
  case outcome {
    PendingStop(position, cursor) ->
      Ok(Stopped(position, cursor.processed, cursor.skipped))
    Drained(cursor) ->
      case runner.config.mode {
        Batch -> Ok(CaughtUp(cursor.processed, cursor.skipped, through))
        Continuous(legacy.Control(wait)) ->
          case wait(runner.config.poll_interval) {
            True -> cycle(runner, cursor)
            False -> Ok(Cancelled(cursor.processed, cursor.skipped))
          }
      }
  }
}

fn drain(runner: Runner(event, source_error, error), cursor: Cursor, bound) {
  let assert Ok(page) = projection.page(cursor.after, runner.config.batch_size)
  use envelopes <- result.try(
    runner.source.read(runner.config.selection, page, delivery.Inclusive(bound))
    |> result.map_error(SourceRead),
  )
  case envelopes {
    [] -> Ok(Drained(cursor))
    _ -> {
      use next <- result.try(deliver(runner, cursor, bound, envelopes))
      case next {
        Drained(cursor) -> drain(runner, cursor, bound)
        PendingStop(_, _) -> Ok(next)
      }
    }
  }
}

fn deliver(
  runner: Runner(event, source_error, error),
  cursor: Cursor,
  bound,
  envelopes: List(delivery.Envelope),
) {
  case envelopes {
    [] -> Ok(Drained(cursor))
    [envelope, ..rest] -> {
      let position = envelope.position
      case
        envelope.source == runner.source.id
        && later_than(position, cursor.after)
        && projection.sequence(position) <= projection.sequence(bound)
        && delivery.selects(
          runner.config.selection,
          envelope.stream,
          delivery.event_type_name(envelope.event_type),
        )
      {
        False -> Error(InvalidDelivery(position))
        True -> {
          use outcome <- result.try(attempt(
            runner,
            envelope,
            None,
            1,
            int.to_float(runner.config.retry_policy.initial),
          ))
          let next = Cursor(Some(position), cursor.processed, cursor.skipped)
          case outcome {
            Applied ->
              deliver(
                runner,
                Cursor(..next, processed: next.processed + 1),
                bound,
                rest,
              )
            Skipped ->
              deliver(
                runner,
                Cursor(..next, skipped: next.skipped + 1),
                bound,
                rest,
              )
            Suppressed -> deliver(runner, next, bound, rest)
            Pending -> Ok(PendingStop(position, cursor))
          }
        }
      }
    }
  }
}

fn attempt(
  runner: Runner(event, source_error, error),
  envelope: delivery.Envelope,
  decoded,
  number,
  delay,
) {
  let position = envelope.position
  use transaction <- result.try(begin(runner.leader))
  use pending <- result.try(check_pending(runner, transaction, position))
  case pending {
    False -> rollback(runner.leader) |> result.replace(Suppressed)
    True -> {
      let event = case decoded {
        Some(event) -> Ok(event)
        None ->
          runner.projector.decode(envelope)
          |> result.map_error(fn(error) { DecodeFailed(position, error) })
      }
      use event <- result.try(rollback_on_error(runner.leader, event))
      case
        runner.projector.apply(event, position, session.scope(runner.leader))
      {
        Ok(after_commit) -> {
          use _ <- result.try(advance(runner, transaction, position))
          use _ <- result.try(
            after_commit()
            |> result.map_error(fn(error) { AfterCommitFailed(position, error) }),
          )
          Ok(Applied)
        }
        Error(error) -> {
          let decision =
            runner.projector.on_error(Failure(position, error, number))
          use _ <- result.try(rollback(runner.leader))
          case decision {
            Stop -> Ok(Pending)
            Fatal -> Error(ApplicationFailed(position, error))
            Skip -> skip(runner, position)
            Retry -> {
              let policy = runner.config.retry_policy
              case number > policy.retries {
                True -> Error(RetryExhausted(position, number, error))
                False -> {
                  let cap = int.to_float(policy.maximum)
                  let wait = float.min(delay, cap)
                  runner.config.retry_wait(int.min(
                    float.round(wait),
                    policy.maximum,
                  ))
                  let next = case
                    wait == 0.0 || wait >=. cap /. policy.multiplier
                  {
                    True ->
                      case wait == 0.0 {
                        True -> 0.0
                        False -> cap
                      }
                    False -> wait *. policy.multiplier
                  }
                  attempt(runner, envelope, Some(event), number + 1, next)
                }
              }
            }
          }
        }
      }
    }
  }
}

fn skip(runner: Runner(event, source_error, error), position) {
  use transaction <- result.try(begin(runner.leader))
  use pending <- result.try(check_pending(runner, transaction, position))
  case pending {
    True -> advance(runner, transaction, position) |> result.replace(Skipped)
    False -> rollback(runner.leader) |> result.replace(Suppressed)
  }
}

fn check_pending(
  runner: Runner(event, source_error, error),
  transaction,
  position,
) {
  rollback_on_error(
    runner.leader,
    read_progress(runner, transaction, Some(position)),
  )
  |> result.map(fn(after) { later_than(position, after) })
}

fn read_progress(
  runner: Runner(event, source_error, error),
  transaction,
  position,
) {
  use row <- result.try(
    progress.lock(transaction, runner.projector.name)
    |> result.map_error(fn(error) { ProgressFailed(position, error) }),
  )
  use _ <- result.try(validate_identity(
    row,
    runner.source.id,
    delivery.selection_identity(runner.config.selection),
    SourceMismatch,
    SelectionMismatch,
  ))
  Ok(option.map(row, fn(row) { row.position }))
}

fn validate_identity(row, source, selection, wrong_source, wrong_selection) {
  case row {
    Some(progress.Progress(persisted, _, _)) if persisted != source ->
      Error(wrong_source(persisted, source))
    Some(progress.Progress(_, persisted, _)) if persisted != selection ->
      Error(wrong_selection(persisted, selection))
    _ -> Ok(Nil)
  }
}

fn later_than(position, after) {
  case after {
    None -> True
    Some(after) -> projection.sequence(position) > projection.sequence(after)
  }
}

fn advance(runner: Runner(event, source_error, error), transaction, position) {
  let candidate =
    progress.Progress(
      runner.source.id,
      delivery.selection_identity(runner.config.selection),
      position,
    )
  let saved =
    progress.advance(transaction, runner.projector.name, candidate)
    |> result.map_error(fn(error) { ProgressFailed(Some(position), error) })
  use _ <- result.try(rollback_on_error(runner.leader, saved))
  session.commit(runner.leader)
  |> result.map_error(fn(error) {
    case error {
      session.RolledBack -> CommitRolledBack(position)
      session.SessionFailed -> CommitIndeterminate(position)
    }
  })
}

fn begin(leader) {
  session.begin(leader) |> result.replace_error(LeadershipLost)
}

fn rollback(leader) {
  session.rollback(leader) |> result.replace_error(LeadershipLost)
}

fn rollback_on_error(leader, value) {
  case value {
    Ok(_) -> value
    Error(_) -> {
      use _ <- result.try(rollback(leader))
      value
    }
  }
}
