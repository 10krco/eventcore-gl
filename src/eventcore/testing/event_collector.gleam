//// Collect events through the legacy projection runner.

import eventcore/projection.{type Filter}
import eventcore/projection_runner as runner
import eventcore/store.{type Event}
import gleam/list
import gleam/result

pub fn projector(
  name: String,
  filter: Filter,
) -> runner.Projector(payload, List(Event(payload)), Nil) {
  runner.Projector(
    name: name,
    filter: filter,
    initial: [],
    apply: fn(events, delivery) { #([delivery.event, ..events], Ok(Nil)) },
    on_error: fn(_) { runner.Fatal },
  )
}

/// Collect one batch in delivery order and persist the runner checkpoint.
pub fn collect(
  backend: runner.Backend(payload),
  name: String,
  filter: Filter,
) -> Result(List(Event(payload)), runner.RunError(Nil)) {
  runner.run(
    backend,
    projector(name, filter),
    runner.default_config,
    runner.uninterrupted(),
  )
  |> result.map(list.reverse)
}
