//// Deterministic conflict and configurable store failure injection.

import eventcore/store.{type Store}
import eventcore/stream
import gleam/float
import gleam/option.{type Option, None, Some}

/// Probabilities for injected faults and an optional reproducible seed.
pub opaque type Chaos {
  Chaos(seed: Option(Int), failure: Float, conflict: Float)
}

/// Start with a random seed and zero probability of injected faults.
pub fn chaos() -> Chaos {
  Chaos(None, 0.0, 0.0)
}

/// Start with seed zero and zero probability of injected faults.
pub fn deterministic() -> Chaos {
  Chaos(Some(0), 0.0, 0.0)
}

/// Use the given seed to reproduce outcomes for the same sequence of calls.
pub fn with_seed(config: Chaos, seed: Int) -> Chaos {
  Chaos(..config, seed: Some(seed))
}

/// Set the probability that a read or append fails with `StoreFailure`.
/// The probability is clamped to the range from zero to one.
pub fn with_failure_probability(config: Chaos, probability: Float) -> Chaos {
  Chaos(..config, failure: float.clamp(probability, 0.0, 1.0))
}

/// Set the probability that an append fails with `VersionConflict`.
/// The probability is clamped to the range from zero to one.
pub fn with_version_conflict_probability(
  config: Chaos,
  probability: Float,
) -> Chaos {
  Chaos(..config, conflict: float.clamp(probability, 0.0, 1.0))
}

/// Inject before delegating. All callers share the same atomic append budget.
pub fn conflicts(storage: Store(payload), count: Int) -> Store(payload) {
  let next = counter()
  let failure = conflict("deterministic-conflict")
  store.Store(..storage, append_events: fn(writes) {
    case next() <= count {
      True -> Error(failure)
      False -> storage.append_events(writes)
    }
  })
}

/// Wrap reads and appends. Seeded runs are reproducible for the same sequence
/// of calls. On append, a selected conflict takes precedence over failure.
pub fn with_chaos(storage: Store(payload), config: Chaos) -> Store(payload) {
  let seed = case config.seed {
    Some(seed) -> seed
    None -> random_seed()
  }
  let next = counter()
  let conflict_error = conflict("chaos-conflict")
  store.Store(
    read_after: fn(id, version) {
      let sequence = next()
      case sample(seed, sequence, 0) <. config.failure {
        True -> Error(store.StoreFailure(store.ReadStream))
        False -> storage.read_after(id, version)
      }
    },
    append_events: fn(writes) {
      let sequence = next()
      case
        sample(seed, sequence, 1) <. config.conflict,
        sample(seed, sequence, 2) <. config.failure
      {
        True, _ -> Error(conflict_error)
        False, True -> Error(store.StoreFailure(store.AppendEvents))
        False, False -> storage.append_events(writes)
      }
    },
  )
}

fn conflict(name: String) -> store.StoreError {
  let assert Ok(id) = stream.id(name)
  let initial = stream.initial_version
  store.VersionConflict(id, initial, stream.next_version(initial))
}

@external(erlang, "eventcore_testing_faults_ffi", "counter")
fn counter() -> fn() -> Int

@external(erlang, "eventcore_testing_faults_ffi", "random_seed")
fn random_seed() -> Int

@external(erlang, "eventcore_testing_faults_ffi", "sample")
fn sample(seed: Int, sequence: Int, operation: Int) -> Float
