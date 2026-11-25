//// Reconstruct state from participating streams and commit a command atomically.

import eventcore/snapshot
import eventcore/store.{type Event, type Store, type StoreError, type Writes}
import eventcore/stream.{type Id, type Version}
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

/// Declares a primary stream and additional participants in first-seen order.
/// Repeated stream identifiers refer to the same participant.
pub fn across(primary: Id, participants: List(Id)) -> Declarations {
  Declarations(list.unique([primary, ..participants]))
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

/// Use with `result.try` to stop a handler when a business rule fails.
/// The error is evaluated only when the condition is false.
pub fn require(
  condition: Bool,
  error: fn() -> domain_error,
) -> Result(Nil, Rejection(domain_error)) {
  case condition {
    True -> Ok(Nil)
    False -> Error(BusinessRule(error()))
  }
}

pub type CommandError(domain_error) {
  Rejected(Rejection(domain_error))
  StorageError(StoreError)
  ConcurrencyError(max_retries: Int)
  InvalidSnapshot(detail: String)
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
  let Declarations(streams) = command.streams
  run(storage, command, policy, fn() {
    replay(
      storage,
      command,
      ReplayQueue(streams, [], set.from_list(streams)),
      command.initial_state(),
      store.writes(),
      [],
    )
    |> result.map_error(StorageError)
  })
}

/// Reuse a durable projection of this command's replay state. Include every
/// command and state-schema dimension that affects reconstruction in the key.
pub fn execute_with_snapshot(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  policy: RetryPolicy,
  repository: snapshot.Repository,
  key: snapshot.Key,
  codec: snapshot.Codec(state),
) -> Result(Execution, CommandError(domain_error)) {
  run(storage, command, policy, fn() {
    use stored <- result.try(
      repository.load(key) |> result.map_error(StorageError),
    )
    let Declarations(streams) = command.streams
    let queue = ReplayQueue(streams, [], set.from_list(streams))
    use checkpoints <- result.try(case stored {
      None -> Ok(None)
      Some(snapshot.Snapshot(checkpoints)) ->
        validate_checkpoints(queue, checkpoints, command.discover, codec)
    })
    use restored <- result.try(replay_snapshot(
      storage,
      command,
      queue,
      SnapshotReplay(command.initial_state(), store.writes(), [], 0, False),
      option.unwrap(checkpoints, []),
    ))
    let refresh = case checkpoints {
      Some(_) -> restored.advanced
      None -> restored.count >= 100
    }
    let ordered = list.reverse(restored.checkpoints)
    use _ <- result.try(case refresh {
      True -> {
        use encoded <- result.try(
          list.try_map(ordered, fn(checkpoint) {
            use bytes <- result.try(case checkpoint.encoded {
              Some(bytes) -> Ok(bytes)
              None ->
                codec.encode(checkpoint.state)
                |> result.map_error(InvalidSnapshot)
            })
            Ok(snapshot.Checkpoint(checkpoint.stream, checkpoint.version, bytes))
          }),
        )
        repository.save(key, stored, snapshot.Snapshot(encoded))
        |> result.map_error(StorageError)
      }
      False -> Ok(Nil)
    })
    let participants = list.map(ordered, fn(c) { c.stream })
    Ok(#(restored.state, restored.writes, participants))
  })
}

fn run(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  policy: RetryPolicy,
  reconstruct: fn() ->
    Result(#(state, Writes(payload), List(Id)), CommandError(domain_error)),
) -> Result(Execution, CommandError(domain_error)) {
  policy.trace(ExecutionStarted)
  execute_attempt(storage, command, policy, reconstruct, 1)
}

fn execute_attempt(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  policy: RetryPolicy,
  reconstruct: fn() ->
    Result(#(state, Writes(payload), List(Id)), CommandError(domain_error)),
  attempt: Int,
) -> Result(Execution, CommandError(domain_error)) {
  let replayed = reconstruct()
  case replayed {
    Error(error) -> fail(policy, error)
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
              execute_attempt(
                storage,
                command,
                policy,
                reconstruct,
                attempt + 1,
              )
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
  queue: ReplayQueue,
  state: state,
  writes: Writes(payload),
  visited: List(Id),
) -> Result(#(state, Writes(payload), List(Id)), StoreError) {
  case pop(queue) {
    None -> Ok(#(state, writes, list.reverse(visited)))
    Some(#(id, queue)) -> {
      use events <- result.try(store.read_stream(storage, id))
      use #(state, version) <- result.try(
        store.fold(events, #(state, stream.initial_version), fn(acc, event) {
          #(command.apply(acc.0, event), stream.next_version(acc.1))
        }),
      )
      use writes <- result.try(store.register(writes, id, version))
      replay(
        storage,
        command,
        enqueue(queue, command.discover(state)),
        state,
        writes,
        [id, ..visited],
      )
    }
  }
}

type ReplayQueue {
  ReplayQueue(remaining: List(Id), pending: List(Id), scheduled: Set(Id))
}

fn pop(queue: ReplayQueue) -> Option(#(Id, ReplayQueue)) {
  case queue.remaining, queue.pending {
    [id, ..rest], _ -> Some(#(id, ReplayQueue(..queue, remaining: rest)))
    [], [] -> None
    [], pending ->
      pop(ReplayQueue(..queue, remaining: list.reverse(pending), pending: []))
  }
}

fn enqueue(queue: ReplayQueue, related: List(Id)) -> ReplayQueue {
  list.fold(related, queue, fn(queue, id) {
    case set.contains(queue.scheduled, id) {
      True -> queue
      False ->
        ReplayQueue(
          ..queue,
          pending: [id, ..queue.pending],
          scheduled: set.insert(queue.scheduled, id),
        )
    }
  })
}

fn validate_checkpoints(
  queue: ReplayQueue,
  checkpoints: List(snapshot.Checkpoint),
  discover: fn(state) -> List(Id),
  codec: snapshot.Codec(state),
) -> Result(
  Option(List(#(snapshot.Checkpoint, state))),
  CommandError(domain_error),
) {
  case pop(queue), checkpoints {
    None, [] -> Ok(Some([]))
    Some(#(id, queue)), [checkpoint, ..tail] if id == checkpoint.stream -> {
      use state <- result.try(
        codec.decode(checkpoint.state) |> result.map_error(InvalidSnapshot),
      )
      validate_checkpoints(
        enqueue(queue, discover(state)),
        tail,
        discover,
        codec,
      )
      |> result.map(option.map(_, fn(tail) { [#(checkpoint, state), ..tail] }))
    }
    _, _ -> Ok(None)
  }
}

type ReplayCheckpoint(state) {
  ReplayCheckpoint(
    stream: Id,
    version: Version,
    state: state,
    encoded: Option(BitArray),
  )
}

type SnapshotReplay(state, payload) {
  SnapshotReplay(
    state: state,
    writes: Writes(payload),
    checkpoints: List(ReplayCheckpoint(state)),
    count: Int,
    advanced: Bool,
  )
}

fn replay_snapshot(
  storage: Store(payload),
  command: Command(state, payload, domain_error),
  queue: ReplayQueue,
  progress: SnapshotReplay(state, payload),
  cached: List(#(snapshot.Checkpoint, state)),
) -> Result(SnapshotReplay(state, payload), CommandError(domain_error)) {
  case pop(queue) {
    None -> Ok(progress)
    Some(#(id, queue)) -> {
      let #(base, version, encoded, cached) = case cached {
        [#(checkpoint, state), ..rest] -> #(
          state,
          checkpoint.version,
          Some(checkpoint.state),
          rest,
        )
        [] -> #(progress.state, stream.initial_version, None, [])
      }
      use events <- result.try(
        storage.read_after(id, version) |> result.map_error(StorageError),
      )
      use #(state, current) <- result.try(
        store.fold(events, #(base, version), fn(acc, event) {
          #(command.apply(acc.0, event), stream.next_version(acc.1))
        })
        |> result.map_error(StorageError),
      )
      let changed = current != version
      let encoded = case changed {
        True -> None
        False -> encoded
      }
      use writes <- result.try(
        store.register(progress.writes, id, current)
        |> result.map_error(StorageError),
      )
      let progress =
        SnapshotReplay(
          state,
          writes,
          [
            ReplayCheckpoint(id, current, state, encoded),
            ..progress.checkpoints
          ],
          progress.count + stream.event_count(current),
          progress.advanced || changed,
        )
      let cached = case changed {
        True -> []
        False -> cached
      }
      replay_snapshot(
        storage,
        command,
        enqueue(queue, command.discover(state)),
        progress,
        cached,
      )
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
