import eventcore/memory
import eventcore/projection
import eventcore/projection_runner as runner
import eventcore/store
import eventcore/stream
import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/result

pub fn batch_retries_skips_and_saves_filtered_frontier_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let assert Ok(id) = stream.id("orders")
  let assert Ok(writes) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "selected", 1)))
    |> result.try(store.append(_, store.Event(id, "selected", 2)))
    |> result.try(store.append(_, store.Event(id, "ignored", 3)))
  assert memory.as_store(memory).append_events(writes) == Ok(Nil)

  let waits = process.new_subject()
  let control =
    runner.Control(fn(delay) {
      process.send(waits, delay)
      True
    })
  let projector =
    runner.Projector(
      name: "orders",
      filter: projection.with_event_type(projection.all(), "selected"),
      initial: 0,
      apply: fn(attempts, delivery) {
        case delivery.event.payload, attempts {
          1, 0 -> #(1, Error("retry"))
          1, 1 -> #(2, Ok(Nil))
          _, _ -> #(attempts + 1, Error("skip"))
        }
      },
      on_error: fn(failure) {
        case failure.error {
          "retry" -> runner.Retry
          _ -> runner.Skip
        }
      },
    )
  assert runner.run(backend, projector, runner.default_config, control) == Ok(3)
  assert process.receive(waits, 0) == Ok(0)
  assert process.receive(waits, 0) == Ok(0)
  assert process.receive(waits, 0) == Ok(100)
  let assert Ok(position) = projection.position(3)
  assert backend.checkpoints.load("orders") == Ok(Some(position))
  assert runner.run(backend, projector, runner.default_config, control) == Ok(0)
  assert memory.close(memory) == Ok(Nil)
}

pub fn fatal_event_leaves_checkpoint_and_releases_lease_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let assert Ok(id) = stream.id("orders")
  let assert Ok(writes) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "selected", 1)))
  assert memory.as_store(memory).append_events(writes) == Ok(Nil)
  let projector =
    runner.Projector(
      "orders",
      projection.all(),
      Nil,
      fn(_, _) { #(Nil, Error("fatal")) },
      fn(_) { runner.Fatal },
    )
  let assert Ok(position) = projection.position(1)
  assert runner.run(
      backend,
      projector,
      runner.default_config,
      runner.uninterrupted(),
    )
    == Error(runner.ApplyFailed(runner.Failure("fatal", position, 0)))
  assert backend.checkpoints.load("orders") == Ok(None)
  let assert Ok(lease) = backend.acquire("orders")
  assert backend.acquire("orders") == Error(runner.AlreadyLeading)
  let assert Ok(other) = backend.acquire("another")
  other.release()
  lease.release()
  assert memory.close(memory) == Ok(Nil)
}

pub fn continuous_empty_poll_can_be_cancelled_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let stop = process.new_subject()
  let reader = fn(filter, page) {
    process.send(stop, Nil)
    backend.reader(filter, page)
  }
  let projector =
    runner.Projector(
      "empty",
      projection.all(),
      Nil,
      fn(_, _) { #(Nil, Ok(Nil)) },
      fn(_) { runner.Fatal },
    )
  let config = runner.Config(..runner.default_config, mode: runner.Continuous)
  assert runner.run(
      runner.Backend(..backend, reader: reader),
      projector,
      config,
      runner.cancellable(stop),
    )
    == Ok(Nil)
  let assert Ok(lease) = backend.acquire("empty")
  lease.release()
  assert memory.close(memory) == Ok(Nil)
}

pub fn dead_holder_releases_memory_leadership_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let ready = process.new_subject()
  let holder =
    process.spawn_unlinked(fn() {
      let assert Ok(lease) = backend.acquire("orders")
      process.send(ready, lease)
      process.receive_forever(process.new_subject())
    })
  let monitor = process.monitor(holder)
  let assert Ok(stale) = process.receive(ready, 1000)
  assert backend.acquire("orders") == Error(runner.AlreadyLeading)
  process.kill(holder)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  assert process.selector_receive(selector, 1000) == Ok(Nil)
  let assert Ok(current) = backend.acquire("orders")
  stale.release()
  assert backend.acquire("orders") == Error(runner.AlreadyLeading)
  current.release()
  assert memory.close(memory) == Ok(Nil)
}

pub fn poll_failure_retries_are_bounded_without_checkpoint_advance_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let attempts = process.new_subject()
  let failure = store.StoreFailure(store.ReadProjection)
  let reader = fn(_, page) {
    process.send(attempts, projection.page_after(page))
    Error(failure)
  }
  let projector =
    runner.Projector(
      "orders",
      projection.all(),
      Nil,
      fn(_, _) { #(Nil, Ok(Nil)) },
      fn(_) { runner.Fatal },
    )
  assert runner.run(
      runner.Backend(..backend, reader: reader),
      projector,
      runner.default_config,
      runner.Control(fn(_) { True }),
    )
    == Error(runner.PollFailed(failure))
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Ok(None)
  assert process.receive(attempts, 0) == Error(Nil)
  assert backend.checkpoints.load("orders") == Ok(None)
  assert memory.close(memory) == Ok(Nil)
}

pub fn exhausted_event_retry_leaves_event_available_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let assert Ok(id) = stream.id("orders")
  let assert Ok(writes) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "submitted", 1)))
  assert memory.as_store(memory).append_events(writes) == Ok(Nil)

  let delays = process.new_subject()
  let control =
    runner.Control(fn(delay) {
      case delay {
        0 -> Nil
        _ -> process.send(delays, delay)
      }
      True
    })
  let projector =
    runner.Projector(
      "orders",
      projection.all(),
      0,
      fn(attempts, _) { #(attempts + 1, Error(attempts + 1)) },
      fn(_) { runner.Retry },
    )
  let assert Ok(position) = projection.position(1)
  assert runner.run(backend, projector, runner.default_config, control)
    == Error(runner.RetryExhausted(runner.Failure(4, position, 3)))
  assert process.receive(delays, 0) == Ok(100)
  assert process.receive(delays, 0) == Ok(200)
  assert process.receive(delays, 0) == Ok(400)
  assert process.receive(delays, 0) == Error(Nil)
  assert backend.checkpoints.load("orders") == Ok(None)

  let recovered =
    runner.Projector(
      ..projector,
      apply: fn(total, delivery: projection.Delivery(Int)) {
        #(total + delivery.event.payload, Ok(Nil))
      },
    )
  assert runner.run(backend, recovered, runner.default_config, control) == Ok(1)
  assert backend.checkpoints.load("orders") == Ok(Some(position))
  assert memory.close(memory) == Ok(Nil)
}

pub fn failed_checkpoint_save_replays_after_restart_test() {
  let assert Ok(memory) = memory.start()
  let backend = memory.projection_backend(memory)
  let assert Ok(id) = stream.id("orders")
  let assert Ok(writes) =
    store.writes()
    |> store.register(id, stream.initial_version)
    |> result.try(store.append(_, store.Event(id, "submitted", 1)))
  assert memory.as_store(memory).append_events(writes) == Ok(Nil)
  let failing =
    projection.Checkpoints(..backend.checkpoints, save: fn(_, _) {
      Error(store.StoreFailure(store.SaveCheckpoint))
    })
  let projector =
    runner.Projector(
      "orders",
      projection.all(),
      0,
      fn(total, delivery) { #(total + delivery.event.payload, Ok(Nil)) },
      fn(_) { runner.Fatal },
    )
  let control = runner.uninterrupted()
  assert runner.run(
      runner.Backend(..backend, checkpoints: failing),
      projector,
      runner.default_config,
      control,
    )
    == Ok(1)
  assert backend.checkpoints.load("orders") == Ok(None)
  assert runner.run(backend, projector, runner.default_config, control) == Ok(1)
  let assert Ok(position) = projection.position(1)
  assert backend.checkpoints.load("orders") == Ok(Some(position))
  assert memory.close(memory) == Ok(Nil)
}
