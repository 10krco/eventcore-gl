import eventcore/command
import eventcore/store
import eventcore/stream
import eventcore/testing/faults
import eventcore/testing/scenario
import gleam/list
import gleam/result

pub fn seeded_failures_repeat_and_probability_endpoints_are_exact_test() {
  let storage =
    store.Store(fn(_, _) { Ok(store.from_items([])) }, fn(_) { Ok(Nil) })
  let let_pass = faults.with_chaos(storage, faults.chaos())
  assert let_pass.append_events(store.writes()) == Ok(Nil)
  let always =
    faults.deterministic()
    |> faults.with_failure_probability(2.0)
    |> faults.with_version_conflict_probability(2.0)
  let failing = faults.with_chaos(storage, always)
  let assert Ok(id) = stream.id("account")
  let assert Error(store.StoreFailure(store.ReadStream)) =
    failing.read_after(id, stream.initial_version)
  let assert Error(store.VersionConflict(_, _, _)) =
    failing.append_events(store.writes())
  let mixed =
    faults.deterministic()
    |> faults.with_seed(2026)
    |> faults.with_failure_probability(0.5)
  let run = fn() {
    let wrapped = faults.with_chaos(storage, mixed)
    list.repeat(Nil, 32)
    |> list.map(fn(_) { wrapped.append_events(store.writes()) })
  }
  let first = run()
  assert first == run()
  assert list.contains(first, Ok(Nil))
  assert list.contains(first, Error(store.StoreFailure(store.AppendEvents)))
}

pub fn deterministic_conflicts_replay_until_the_budget_is_spent_test() {
  let assert Ok(id) = stream.id("account")
  let event = store.Event(id, "deposited", 20)
  let storage =
    store.Store(fn(_, _) { Ok(store.from_items([])) }, fn(writes) {
      assert store.entries(writes) == [event]
      Ok(Nil)
    })
  let wrapped = faults.conflicts(storage, 2)
  let operation = deposit(id, 20)
  assert command.execute(wrapped, operation, command.default_policy())
    == Ok(command.Execution(3))
  assert command.execute(wrapped, operation, command.default_policy())
    == Ok(command.Execution(1))
}

pub fn scenario_observes_given_and_new_events_in_delivery_order_test() {
  let assert Ok(id) = stream.id("account")
  let opened = store.Event(id, "opened", 10)
  let assert Ok(initial) = scenario.start()
  let assert Ok(given) = scenario.given_events(initial, id, [opened])
  let assert Ok(given) = scenario.given_events(given, id, [])
  let earlier = store.Event(id, "deposited", 5)
  let assert Ok(given) = scenario.given_events(given, id, [earlier])
  let assert Ok(observation) = scenario.when(given, deposit(id, 20))
  observation
  |> scenario.succeeded
  |> scenario.then_events([opened, earlier, store.Event(id, "deposited", 20)])
  |> scenario.then_event_count(3)
  assert scenario.close(given) == Ok(Nil)
}

pub fn scenario_observes_rejection_without_new_events_test() {
  let assert Ok(id) = stream.id("account")
  let opened = store.Event(id, "opened", 10)
  let assert Ok(initial) = scenario.start()
  let assert Ok(given) = scenario.given_events(initial, id, [opened])
  let assert Ok(observation) = scenario.when(given, deposit(id, 0))
  observation
  |> scenario.failed_with(command.Rejected(command.BusinessRule(10)))
  |> scenario.then_events([opened])
  assert scenario.close(given) == Ok(Nil)
}

fn deposit(id: stream.Id, amount: Int) -> command.Command(Int, Int, Int) {
  command.Command(
    command.single(id),
    fn() { 0 },
    fn(balance, event) { balance + event.payload },
    fn(_) { [] },
    fn(balance) {
      use _ <- result.try(command.require(amount > 0, fn() { balance }))
      Ok([store.Event(id, "deposited", amount)])
    },
  )
}
