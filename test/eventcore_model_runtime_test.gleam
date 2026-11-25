import eventcore/command
import eventcore/memory
import eventcore/model
import eventcore/model_runtime as runtime
import eventcore/projection
import eventcore/projection_runner as runner
import eventcore/store
import eventcore/stream

type Deposit {
  Deposit
}

type Account {
  Account
}

type Transaction {
  Transaction
}

type Balance {
  Balance
}

type Change {
  Change
}

type Statement {
  Statement
}

type TransactionPayload {
  Opened
  Deposited(Int)
}

pub fn modeled_command_projection_and_view_test() {
  let assert Ok(account) = stream.id("bank/1")
  let assert Ok(memory) = memory.start()
  let event = model.event("Transaction", Transaction)
  let deposit = model.command("Deposit", Deposit)
  let account_state = model.state("Account", Account)

  let operation =
    runtime.as_command(
      model.build(deposit, 40),
      event,
      runtime.CommandLogic(
        streams: fn(_) { command.single(account) },
        initial: fn() { model.build(account_state, 0) },
        evolve: fn(current, historical) {
          case model.value(historical.payload) {
            Opened -> current
            Deposited(amount) ->
              model.build(account_state, model.value(current) + amount)
          }
        },
        discover: fn(_) { [] },
        decide: fn(input, current) {
          case model.value(current) {
            0 ->
              Ok(
                runtime.one(store.Event(
                  account,
                  "opened",
                  model.build(event, Opened),
                ))
                |> runtime.push(store.Event(
                  account,
                  "deposited",
                  model.build(event, Deposited(model.value(input))),
                )),
              )
            _ -> Ok(runtime.none("deposit already recorded"))
          }
        },
      ),
    )
  let storage = memory.as_store(memory)
  assert command.execute(storage, operation, command.default_policy())
    == Ok(command.Execution(1))
  assert command.execute(storage, operation, command.default_policy())
    == Ok(command.Execution(1))

  let assert Ok(stored) = store.read_stream(storage, account)
  assert store.collect(stored)
    == Ok([
      store.Event(account, "opened", Opened),
      store.Event(account, "deposited", Deposited(40)),
    ])

  let balance = model.read_model("Balance", Balance)
  let change = model.effect("Change", Change)
  let assert Ok(sink) = runtime.memory_sink(model.build(balance, #(0, 0, 0)))
  let projector =
    runtime.Projection("balance", projection.all(), fn(delivery) {
      case model.value(delivery.event.payload) {
        Opened -> Ok(runtime.Ignore("zero initial balance"))
        Deposited(amount) -> Ok(runtime.Apply(model.build(change, amount)))
      }
    })
  let checked =
    runtime.checked_projection(
      event,
      projector,
      runtime.as_sink(sink, fn(previous, effect, position) {
        let #(total, seen, deliveries) = model.value(previous)
        let sequence = projection.sequence(position)
        case sequence > seen {
          True ->
            model.build(balance, #(
              total + model.value(effect),
              sequence,
              deliveries + 1,
            ))
          False -> model.build(balance, #(total, seen, deliveries + 1))
        }
      }),
      fn(_) { runner.Fatal },
    )
  let backend = memory.projection_backend(memory)
  let fail_checkpoint =
    runner.Backend(
      ..backend,
      checkpoints: projection.Checkpoints(backend.checkpoints.load, fn(_, _) {
        Error(store.StoreFailure(store.SaveCheckpoint))
      }),
    )
  assert runner.run(
      fail_checkpoint,
      checked,
      runner.default_config,
      runner.uninterrupted(),
    )
    == Ok(Nil)
  assert runner.run(
      backend,
      checked,
      runner.default_config,
      runner.uninterrupted(),
    )
    == Ok(Nil)
  let assert Ok(read_model) = runtime.read(sink)
  assert model.value(read_model) == #(40, 2, 2)
  let statement = model.output("Statement", Statement)
  let output =
    runtime.render(read_model, fn(balance) {
      let #(total, _, _) = model.value(balance)
      model.build(statement, #("available", total))
    })
  assert model.value(output) == #("available", 40)
  assert runtime.close(sink) == Ok(Nil)
  assert memory.close(memory) == Ok(Nil)
}

pub fn checked_projection_distinguishes_projection_and_sink_failures_test() {
  let assert Ok(account) = stream.id("bank/failed")
  let assert Ok(position) = projection.position(17)
  let delivery =
    projection.Delivery(store.Event(account, "deposit", 10), position)
  let event = model.event("Transaction", Transaction)
  let effect = model.effect("Change", Change)

  let invalid =
    runtime.Projection("invalid", projection.all(), fn(_) {
      Error("invalid event")
    })
  let checked =
    runtime.checked_projection(
      event,
      invalid,
      fn(_, _) { panic as "sink ran after failed projection" },
      fn(_) { runner.Fatal },
    )
  assert checked.apply(Nil, delivery)
    == #(Nil, Error(runtime.ProjectionFailed("invalid event")))

  let valid =
    runtime.Projection("valid", projection.all(), fn(event) {
      assert event.position == position
      Ok(runtime.Apply(model.build(effect, model.value(event.event.payload))))
    })
  let checked =
    runtime.checked_projection(
      event,
      valid,
      fn(value, seen_position) {
        assert seen_position == position
        assert model.value(value) == 10
        Error("unavailable")
      },
      fn(_) { runner.Fatal },
    )
  assert checked.apply(Nil, delivery)
    == #(Nil, Error(runtime.SinkFailed("unavailable")))
}
