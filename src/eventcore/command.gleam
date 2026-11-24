//// Reconstruct state from participating streams and commit a command atomically.

import eventcore/store.{type Event, type Store, type StoreError, type Writes}
import eventcore/stream.{type Id}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set.{type Set}

pub opaque type Declarations {
  Declarations(List(Id))
}

pub type DeclarationError {
  EmptyDeclarations
  DuplicateStream(Id)
}

pub fn declare(streams: List(Id)) -> Result(Declarations, DeclarationError) {
  case streams {
    [] -> Error(EmptyDeclarations)
    _ -> {
      use _ <- result.try(
        list.try_fold(streams, set.new(), fn(seen, id) {
          case set.contains(seen, id) {
            True -> Error(DuplicateStream(id))
            False -> Ok(set.insert(seen, id))
          }
        }),
      )
      Ok(Declarations(streams))
    }
  }
}

pub fn single(id: Id) -> Declarations {
  Declarations([id])
}

pub fn with_participant(
  declarations: Declarations,
  id: Id,
) -> Result(Declarations, DeclarationError) {
  let Declarations(streams) = declarations
  declare(list.append(streams, [id]))
}

pub type Rejection(domain_error) {
  InvalidState(String)
  BusinessRule(domain_error)
}

pub type CommandError(domain_error) {
  Rejected(Rejection(domain_error))
  StorageError(StoreError)
  ConcurrencyError(max_retries: Int)
}

pub type Command(state, payload, domain_error) {
  Command(
    streams: Declarations,
    initial_state: fn() -> state,
    apply: fn(state, Event(payload)) -> state,
    discover: fn(state) -> List(Id),
    handle: fn(state) -> Result(List(Event(payload)), Rejection(domain_error)),
  )
}

pub type Execution {
  Execution(attempts: Int)
}

pub type Backoff {
  Fixed(delay_ms: Int)
  Exponential(base_ms: Int)
}

pub type RetryContext {
  RetryContext(streams: List(Id), attempt: Int, delay_ms: Int)
}

pub type Trace {
  ExecutionStarted
  CommandSucceeded(attempts: Int)
  Retrying(RetryContext)
  RetriesExhausted(streams: List(Id), max_retries: Int)
  CommandFailed
}

pub opaque type RetryPolicy {
  RetryPolicy(
    max_retries: Int,
    backoff: Backoff,
    metrics: Option(fn(RetryContext) -> Nil),
    trace: fn(Trace) -> Nil,
  )
}

pub fn default_policy() -> RetryPolicy {
  RetryPolicy(4, Exponential(10), None, log_trace)
}

pub fn retry_policy(
  max_retries: Int,
  backoff: Backoff,
) -> Result(RetryPolicy, Nil) {
  let delay = case backoff {
    Fixed(delay) | Exponential(delay) -> delay
  }
  case
    max_retries >= 0
    && max_retries <= 4_294_967_295
    && delay >= 0
    && delay <= 18_446_744_073_709_551_615
  {
    True -> Ok(RetryPolicy(max_retries, backoff, None, log_trace))
    False -> Error(Nil)
  }
}

pub fn with_metrics(
  policy: RetryPolicy,
  hook: fn(RetryContext) -> Nil,
) -> RetryPolicy {
  RetryPolicy(..policy, metrics: Some(hook))
}

pub fn with_trace(policy: RetryPolicy, trace: fn(Trace) -> Nil) -> RetryPolicy {
  RetryPolicy(..policy, trace: trace)
}

@external(erlang, "eventcore_command_ffi", "sleep")
fn sleep(delay_ms: Int) -> Nil

@external(erlang, "eventcore_command_ffi", "trace")
fn log_trace(trace: Trace) -> Nil

pub fn execute(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  policy: RetryPolicy,
) -> Result(Execution, CommandError(domain_error)) {
  policy.trace(ExecutionStarted)
  execute_attempt(storage, command, policy, 1)
}

fn execute_attempt(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  policy: RetryPolicy,
  attempt: Int,
) -> Result(Execution, CommandError(domain_error)) {
  let Declarations(streams) = command.streams
  let replayed =
    replay(
      storage,
      command,
      streams,
      [],
      set.from_list(streams),
      command.initial_state(),
      store.writes(),
      [],
    )
  case replayed {
    Error(error) -> fail(policy, StorageError(error))
    Ok(#(state, writes, participants)) -> {
      let prepared =
        command.handle(state)
        |> result.map_error(Rejected)
        |> result.try(fn(events) {
          list.try_fold(events, writes, store.append)
          |> result.map_error(StorageError)
        })
      case prepared {
        Error(error) -> fail(policy, error)
        Ok(writes) ->
          case storage.append_events(writes) {
            Ok(Nil) -> {
              policy.trace(CommandSucceeded(attempt))
              Ok(Execution(attempt))
            }
            Error(store.VersionConflict(_, _, _))
              if attempt <= policy.max_retries
            -> {
              let delay = delay_ms(policy.backoff, attempt - 1)
              let context = RetryContext(participants, attempt, delay)
              policy.trace(Retrying(context))
              case policy.metrics {
                Some(hook) -> hook(context)
                None -> Nil
              }
              sleep(delay)
              execute_attempt(storage, command, policy, attempt + 1)
            }
            Error(store.VersionConflict(_, _, _)) -> {
              policy.trace(RetriesExhausted(participants, policy.max_retries))
              Error(ConcurrencyError(policy.max_retries))
            }
            Error(error) -> fail(policy, StorageError(error))
          }
      }
    }
  }
}

fn fail(
  policy: RetryPolicy,
  error: CommandError(domain_error),
) -> Result(a, CommandError(domain_error)) {
  policy.trace(CommandFailed)
  Error(error)
}

fn replay(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  remaining: List(Id),
  pending: List(Id),
  scheduled: Set(Id),
  state: state,
  writes: Writes(payload),
  visited: List(Id),
) -> Result(#(state, Writes(payload), List(Id)), StoreError) {
  case remaining, pending {
    [], [] -> Ok(#(state, writes, list.reverse(visited)))
    [], _ ->
      replay(
        storage,
        command,
        list.reverse(pending),
        [],
        scheduled,
        state,
        writes,
        visited,
      )
    [id, ..remaining], _ -> {
      use events <- result.try(store.read_stream(storage, id))
      use #(state, version) <- result.try(
        store.fold(events, #(state, stream.initial_version), fn(acc, event) {
          #(command.apply(acc.0, event), stream.next_version(acc.1))
        }),
      )
      use writes <- result.try(store.register(writes, id, version))
      let #(scheduled, pending) =
        list.fold(command.discover(state), #(scheduled, pending), fn(acc, id) {
          case set.contains(acc.0, id) {
            True -> acc
            False -> #(set.insert(acc.0, id), [id, ..acc.1])
          }
        })
      replay(storage, command, remaining, pending, scheduled, state, writes, [
        id,
        ..visited
      ])
    }
  }
}

const max_delay = 18_446_744_073_709_551_615

fn delay_ms(backoff: Backoff, exponent: Int) -> Int {
  case backoff {
    Fixed(delay) -> delay
    Exponential(0) -> 0
    Exponential(base) -> {
      let jitter = 1.0 +. { float.random() -. 0.5 } *. 0.4
      let delay = exponential_delay(base, exponent)
      int.min(max_delay, float.truncate(int.to_float(delay) *. jitter))
    }
  }
}

fn exponential_delay(delay: Int, exponent: Int) -> Int {
  case exponent {
    0 -> delay
    _ if exponent >= 64 || delay > max_delay / 2 -> max_delay
    _ -> exponential_delay(delay * 2, exponent - 1)
  }
}
