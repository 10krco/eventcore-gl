//// Run modeled commands, projections, and views using the standard runtime.

import eventcore/command
import eventcore/model.{type Component, type Modeled}
import eventcore/projection
import eventcore/projection_runner
import eventcore/store
import eventcore/stream
import gleam/list
import gleam/result

pub opaque type Events(event_witness, payload) {
  NoEvents(reason: String)
  Events(
    newest_first: List(
      store.Event(Modeled(model.Event(event_witness), payload)),
    ),
  )
}

pub fn one(
  event: store.Event(Modeled(model.Event(event_witness), payload)),
) -> Events(event_witness, payload) {
  Events([event])
}

pub fn none(reason: String) -> Events(event_witness, payload) {
  NoEvents(reason)
}

pub fn push(
  events: Events(event_witness, payload),
  event: store.Event(Modeled(model.Event(event_witness), payload)),
) -> Events(event_witness, payload) {
  case events {
    NoEvents(_) -> one(event)
    Events(newest_first) -> Events([event, ..newest_first])
  }
}

pub type CommandLogic(
  command_witness,
  state_witness,
  event_witness,
  input,
  state,
  event,
  error,
) {
  CommandLogic(
    streams: fn(Modeled(model.Command(command_witness), input)) ->
      command.Declarations,
    initial: fn() -> Modeled(model.State(state_witness), state),
    evolve: fn(
      Modeled(model.State(state_witness), state),
      store.Event(Modeled(model.Event(event_witness), event)),
    ) -> Modeled(model.State(state_witness), state),
    discover: fn(Modeled(model.State(state_witness), state)) -> List(stream.Id),
    decide: fn(
      Modeled(model.Command(command_witness), input),
      Modeled(model.State(state_witness), state),
    ) -> Result(Events(event_witness, event), command.Rejection(error)),
  )
}

pub fn as_command(
  input: Modeled(model.Command(command_witness), input),
  event: Component(model.Event(event_witness), event),
  logic: CommandLogic(
    command_witness,
    state_witness,
    event_witness,
    input,
    state,
    event,
    error,
  ),
) -> command.Command(Modeled(model.State(state_witness), state), event, error) {
  command.Command(
    streams: logic.streams(input),
    initial_state: logic.initial,
    apply: fn(state, stored) {
      logic.evolve(state, modeled_event(event, stored))
    },
    discover: logic.discover,
    handle: fn(state) {
      case logic.decide(input, state) {
        Ok(NoEvents(_)) -> Ok([])
        Ok(Events(newest_first)) ->
          Ok(
            list.fold(newest_first, [], fn(events, modeled) {
              [
                store.Event(
                  modeled.stream,
                  modeled.kind,
                  model.value(modeled.payload),
                ),
                ..events
              ]
            }),
          )
        Error(error) -> Error(error)
      }
    },
  )
}

fn modeled_event(
  component: Component(model.Event(event_witness), event),
  event: store.Event(event),
) -> store.Event(Modeled(model.Event(event_witness), event)) {
  store.Event(event.stream, event.kind, model.build(component, event.payload))
}

pub type Action(effect_witness, effect) {
  Apply(Modeled(model.Effect(effect_witness), effect))
  Ignore(reason: String)
}

pub type Projection(event_witness, effect_witness, event, effect, error) {
  Projection(
    name: String,
    filter: projection.Filter,
    project: fn(projection.Delivery(Modeled(model.Event(event_witness), event))) ->
      Result(Action(effect_witness, effect), error),
  )
}

pub type Sink(effect_witness, effect, error) =
  fn(Modeled(model.Effect(effect_witness), effect), projection.Position) ->
    Result(Nil, error)

pub type CheckedError(projection_error, sink_error) {
  ProjectionFailed(projection_error)
  SinkFailed(sink_error)
}

pub fn checked_projection(
  event: Component(model.Event(event_witness), event),
  projection: Projection(
    event_witness,
    effect_witness,
    event,
    effect,
    projection_error,
  ),
  sink: Sink(effect_witness, effect, sink_error),
  on_error: fn(
    projection_runner.Failure(CheckedError(projection_error, sink_error)),
  ) -> projection_runner.FailureStrategy,
) -> projection_runner.Projector(
  event,
  Nil,
  CheckedError(projection_error, sink_error),
) {
  projection_runner.Projector(
    projection.name,
    projection.filter,
    Nil,
    fn(_, delivery) {
      let received =
        projection.Delivery(
          modeled_event(event, delivery.event),
          delivery.position,
        )
      case projection.project(received) {
        Error(error) -> #(Nil, Error(ProjectionFailed(error)))
        Ok(Ignore(_)) -> #(Nil, Ok(Nil))
        Ok(Apply(effect)) -> #(
          Nil,
          sink(effect, delivery.position) |> result.map_error(SinkFailed),
        )
      }
    },
    on_error,
  )
}

type Server(state)

pub opaque type MemorySink(read_model_witness, read_model) {
  MemorySink(
    server: Server(Modeled(model.ReadModel(read_model_witness), read_model)),
  )
}

@external(erlang, "eventcore_memory_ffi", "start")
fn start_server(initial: state) -> Result(Server(state), Nil)

@external(erlang, "eventcore_memory_ffi", "call")
fn call(
  server: Server(state),
  operation: fn(state) -> #(state, response),
) -> Result(response, Nil)

@external(erlang, "eventcore_memory_ffi", "stop")
fn stop(server: Server(state)) -> Result(Nil, Nil)

pub fn memory_sink(
  initial: Modeled(model.ReadModel(read_model_witness), read_model),
) -> Result(MemorySink(read_model_witness, read_model), Nil) {
  start_server(initial) |> result.map(MemorySink)
}

pub fn as_sink(
  sink: MemorySink(read_model_witness, read_model),
  apply: fn(
    Modeled(model.ReadModel(read_model_witness), read_model),
    Modeled(model.Effect(effect_witness), effect),
  ) -> Modeled(model.ReadModel(read_model_witness), read_model),
) -> Sink(effect_witness, effect, Nil) {
  fn(effect, _) {
    call(sink.server, fn(current) { #(apply(current, effect), Nil) })
  }
}

pub fn read(
  sink: MemorySink(read_model_witness, read_model),
) -> Result(Modeled(model.ReadModel(read_model_witness), read_model), Nil) {
  call(sink.server, fn(current) { #(current, current) })
}

pub fn close(
  sink: MemorySink(read_model_witness, read_model),
) -> Result(Nil, Nil) {
  stop(sink.server)
}

pub fn render(
  model: Modeled(model.ReadModel(read_model_witness), read_model),
  view: fn(Modeled(model.ReadModel(read_model_witness), read_model)) ->
    Modeled(model.Output(output_witness), output),
) -> Modeled(model.Output(output_witness), output) {
  view(model)
}
