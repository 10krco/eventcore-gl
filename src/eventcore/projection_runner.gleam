//// Batch and continuous projections with backend-owned leadership.

import eventcore/projection.{
  type Checkpoints, type Delivery, type Filter, type Position, type Reader,
}
import eventcore/store.{type StoreError}
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/option.{type Option, Some}
import gleam/result

pub type LeadershipError {
  AlreadyLeading
  CoordinatorUnavailable
  RemoteHolder
}

pub type Lease {
  Lease(release: fn() -> Nil)
  CheckedLease(
    release: fn() -> Nil,
    check: fn() -> Result(Nil, LeadershipError),
  )
}

/// Check that a lease can still be used before processing or saving progress.
pub fn check_lease(lease: Lease) -> Result(Nil, LeadershipError) {
  case lease {
    Lease(_) -> Ok(Nil)
    CheckedLease(_, check) -> check()
  }
}

pub type Coordinator =
  fn(String) -> Result(Lease, LeadershipError)

pub type Backend(payload) {
  Backend(
    reader: Reader(payload),
    checkpoints: Checkpoints,
    acquire: Coordinator,
  )
}

pub type FailureStrategy {
  Fatal
  Skip
  Retry
}

pub type Failure(error) {
  Failure(error: error, position: Position, retries: Int)
}

pub type Projector(payload, state, error) {
  Projector(
    name: String,
    filter: Filter,
    initial: state,
    apply: fn(state, Delivery(payload)) -> #(state, Result(Nil, error)),
    on_error: fn(Failure(error)) -> FailureStrategy,
  )
}

pub type Mode {
  Batch
  Continuous
}

pub type Config {
  Config(
    mode: Mode,
    poll_interval: Int,
    empty_backoff: Int,
    poll_failure_backoff: Int,
    poll_retries: Int,
    event_retries: Int,
    event_retry_delay: Int,
    event_retry_multiplier: Float,
    event_retry_max_delay: Int,
  )
}

pub const default_config =
  Config(
    mode: Batch,
    poll_interval: 100,
    empty_backoff: 50,
    poll_failure_backoff: 100,
    poll_retries: 5,
    event_retries: 3,
    event_retry_delay: 100,
    event_retry_multiplier: 2.0,
    event_retry_max_delay: 5000,
  )

pub type Control {
  Control(wait: fn(Int) -> Bool)
}

pub fn uninterrupted() -> Control {
  Control(wait: fn(delay) { sleep(delay) })
}

/// Wait for time to elapse or a stop message. The subject belongs to the
/// process running the projector. Send Nil from another process to stop it.
pub fn cancellable(stop: Subject(Nil)) -> Control {
  Control(wait: fn(delay) { wait_for_stop(stop, delay) })
}

fn wait_for_stop(stop: Subject(Nil), delay: Int) -> Bool {
  let chunk = int.min(delay, 4_294_967_295)
  case process.receive(stop, chunk) {
    Ok(Nil) -> False
    Error(Nil) ->
      case delay > chunk {
        True -> wait_for_stop(stop, delay - chunk)
        False -> True
      }
  }
}

pub type RunError(error) {
  InvalidConfiguration
  Leadership(LeadershipError)
  CheckpointLoad(StoreError)
  PollFailed(StoreError)
  ApplyFailed(Failure(error))
  RetryExhausted(Failure(error))
}

type Run(payload, state, error) {
  Run(
    backend: Backend(payload),
    projector: Projector(payload, state, error),
    config: Config,
    control: Control,
    lease: Lease,
  )
}

pub fn run(
  backend: Backend(payload),
  projector: Projector(payload, state, error),
  config: Config,
  control: Control,
) -> Result(state, RunError(error)) {
  use Nil <- result.try(validate(config))
  use lease <- result.try(
    backend.acquire(projector.name) |> result.map_error(Leadership),
  )
  with_release(lease.release, fn() {
    use Nil <- result.try(check_lease(lease) |> result.map_error(Leadership))
    use position <- result.try(
      backend.checkpoints.load(projector.name)
      |> result.map_error(CheckpointLoad),
    )
    poll(
      Run(backend, projector, config, control, lease),
      projector.initial,
      position,
      0,
    )
  })
}

fn validate(config: Config) -> Result(Nil, RunError(error)) {
  case
    config.poll_interval >= 0
    && config.empty_backoff >= 0
    && config.poll_failure_backoff >= 0
    && config.poll_retries >= 0
    && config.event_retries >= 0
    && config.event_retry_delay >= 0
    && config.event_retry_delay <= 18_446_744_073_709_551_615
    && config.event_retry_multiplier >=. 1.0
    && config.event_retry_max_delay >= 0
    && config.event_retry_max_delay <= 18_446_744_073_709_551_615
  {
    True -> Ok(Nil)
    False -> Error(InvalidConfiguration)
  }
}

fn poll(
  run: Run(payload, state, error),
  state: state,
  after: Option(Position),
  failures: Int,
) -> Result(state, RunError(error)) {
  use Nil <- result.try(check_lease(run.lease) |> result.map_error(Leadership))
  case run.control.wait(0) {
    False -> Ok(state)
    True -> {
      let assert Ok(page) = projection.page(after, 1000)
      case run.backend.reader(run.projector.filter, page) {
        Error(error) if failures >= run.config.poll_retries ->
          Error(PollFailed(error))
        Error(_) -> {
          log(run.projector.name, "poll_failed", failures + 1)
          case run.control.wait(run.config.poll_failure_backoff) {
            True -> poll(run, state, after, failures + 1)
            False -> Ok(state)
          }
        }
        Ok(projection.PageResult(events, scanned_through)) -> {
          let delay = case events {
            [] -> run.config.empty_backoff
            _ -> run.config.poll_interval
          }
          consume(run, state, after, events, 0, delay, scanned_through)
        }
      }
    }
  }
}

fn consume(
  run: Run(payload, state, error),
  state: state,
  after: Option(Position),
  events: List(Delivery(payload)),
  retries: Int,
  next_poll_delay: Int,
  scanned_through: Option(Position),
) -> Result(state, RunError(error)) {
  use Nil <- result.try(check_lease(run.lease) |> result.map_error(Leadership))
  case events {
    [] -> {
      use next <- result.try(case scanned_through {
        Some(position) if scanned_through != after ->
          save(run, position) |> result.replace(scanned_through)
        _ -> Ok(after)
      })
      case run.config.mode {
        Batch -> Ok(state)
        Continuous ->
          case run.control.wait(next_poll_delay) {
            True -> poll(run, state, next, 0)
            False -> Ok(state)
          }
      }
    }
    [event, ..rest] ->
      case run.control.wait(0) {
        False -> Ok(state)
        True -> {
          let #(state, applied) = run.projector.apply(state, event)
          use Nil <- result.try(
            check_lease(run.lease) |> result.map_error(Leadership),
          )
          case applied {
            Ok(Nil) ->
              advance(
                run,
                state,
                event.position,
                rest,
                next_poll_delay,
                scanned_through,
              )
            Error(error) -> {
              let failure = Failure(error, event.position, retries)
              case run.projector.on_error(failure) {
                Fatal -> Error(ApplyFailed(failure))
                Skip -> {
                  log(
                    run.projector.name,
                    "event_skipped",
                    projection.sequence(event.position),
                  )
                  advance(
                    run,
                    state,
                    event.position,
                    rest,
                    next_poll_delay,
                    scanned_through,
                  )
                }
                Retry if retries >= run.config.event_retries ->
                  Error(RetryExhausted(failure))
                Retry ->
                  case run.control.wait(retry_delay(run.config, retries)) {
                    True ->
                      consume(
                        run,
                        state,
                        after,
                        events,
                        retries + 1,
                        next_poll_delay,
                        scanned_through,
                      )
                    False -> Ok(state)
                  }
              }
            }
          }
        }
      }
  }
}

fn advance(
  run: Run(payload, state, error),
  state: state,
  position: Position,
  rest: List(Delivery(payload)),
  next_poll_delay: Int,
  scanned_through: Option(Position),
) -> Result(state, RunError(error)) {
  use Nil <- result.try(save(run, position))
  consume(run, state, Some(position), rest, 0, next_poll_delay, scanned_through)
}

fn save(
  run: Run(payload, state, error),
  position: Position,
) -> Result(Nil, RunError(error)) {
  use Nil <- result.try(check_lease(run.lease) |> result.map_error(Leadership))
  case run.backend.checkpoints.save(run.projector.name, position) {
    Ok(Nil) -> Ok(Nil)
    Error(_) -> {
      log(
        run.projector.name,
        "checkpoint_save_failed",
        projection.sequence(position),
      )
      Ok(Nil)
    }
  }
}

fn retry_delay(config: Config, retries: Int) -> Int {
  case config.event_retry_delay {
    0 -> 0
    initial -> {
      let cap = int.to_float(config.event_retry_max_delay)
      let base = int.to_float(initial)
      case float.power(config.event_retry_multiplier, int.to_float(retries)) {
        Ok(multiplier) if multiplier <. cap /. base ->
          float.truncate(base *. multiplier)
        _ -> config.event_retry_max_delay
      }
    }
  }
}

@external(erlang, "eventcore_projection_runner_ffi", "sleep")
fn sleep(delay: Int) -> Bool

@external(erlang, "eventcore_projection_runner_ffi", "with_release")
fn with_release(release: fn() -> Nil, work: fn() -> result) -> result

@external(erlang, "eventcore_projection_runner_ffi", "log")
fn log(projector: String, event: String, detail: Int) -> Nil
