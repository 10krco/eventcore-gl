//// Given, when, and then helpers using a private in-memory store.

import eventcore/command
import eventcore/memory
import eventcore/projection
import eventcore/projection_runner as runner
import eventcore/store.{type Event, type StoreError}
import eventcore/stream.{type Id}
import eventcore/testing/event_collector
import gleam/list
import gleam/result

pub opaque type Scenario(payload) {
  Scenario(memory.Memory(payload))
}

pub type Observation(payload, error) {
  Observation(
    execution: Result(command.Execution, command.CommandError(error)),
    events: List(Event(payload)),
  )
}

pub fn start() -> Result(Scenario(payload), StoreError) {
  memory.start() |> result.map(Scenario)
}

pub fn close(scenario: Scenario(payload)) -> Result(Nil, StoreError) {
  let Scenario(memory) = scenario
  memory.close(memory)
}

/// Append setup events to the stream at its current version.
pub fn given_events(
  scenario: Scenario(payload),
  id: Id,
  events: List(Event(payload)),
) -> Result(Scenario(payload), StoreError) {
  case events {
    [] -> Ok(scenario)
    [_, ..] -> {
      let Scenario(memory) = scenario
      let storage = memory.as_store(memory)
      use latest <- result.try(store.read_stream(storage, id))
      use version <- result.try(
        store.fold(latest, stream.initial_version, fn(version, _) {
          stream.next_version(version)
        }),
      )
      use initial <- result.try(store.register(store.writes(), id, version))
      use writes <- result.try(list.try_fold(events, initial, store.append))
      use Nil <- result.try(storage.append_events(writes))
      Ok(scenario)
    }
  }
}

/// Execute the command and collect one replay batch from a fresh checkpoint.
/// The scenario is single-use. Close it after collecting the observation.
pub fn when(
  scenario: Scenario(payload),
  command: command.Command(state, payload, error),
) -> Result(Observation(payload, error), runner.RunError(Nil)) {
  let Scenario(memory) = scenario
  let execution =
    command.execute(memory.as_store(memory), command, command.default_policy())
  let backend = memory.projection_backend(memory)
  use events <- result.map(event_collector.collect(
    backend,
    "scenario",
    projection.all(),
  ))
  Observation(execution, events)
}

pub fn succeeded(
  observation: Observation(payload, error),
) -> Observation(payload, error) {
  let assert Ok(_) = observation.execution
  observation
}

pub fn failed_with(
  observation: Observation(payload, error),
  expected: command.CommandError(error),
) -> Observation(payload, error) {
  assert observation.execution == Error(expected)
  observation
}

pub fn then_events(
  observation: Observation(payload, error),
  expected: List(Event(payload)),
) -> Observation(payload, error) {
  assert observation.events == expected
  observation
}

pub fn then_event_count(
  observation: Observation(payload, error),
  expected: Int,
) -> Observation(payload, error) {
  assert list.length(observation.events) == expected
  observation
}
