# Testing

Use an isolated in-memory store for command tests. Given–when–then scenarios
check domain rules against recorded history. Fault injection lets you check
what an application does when an append conflicts or a store operation fails.

## Given a history, run a command

Place this module in `test/eventcore_behavior_test.gleam` in the quickstart
project, then run `gleam test`. The two scenarios execute the same command
against the same opening balance. One asserts a withdrawal event. The other
asserts the exact rejection and the original history.

```gleam
import eventcore/command
import eventcore/store
import eventcore/stream
import eventcore/testing/scenario

pub fn withdrawal_records_an_event_test() {
  let assert Ok(account) = stream.id("account/42")
  let opened = store.Event(account, "opened", 1000)
  let debit = store.Event(account, "withdrawn", -400)
  let assert Ok(fixture) = scenario.start()
  let assert Ok(fixture) = scenario.given_events(fixture, account, [opened])
  let assert Ok(observation) = scenario.when(fixture, withdraw(account, 400))

  observation
  |> scenario.succeeded
  |> scenario.then_events([opened, debit])
  |> scenario.then_event_count(2)

  let assert Ok(Nil) = scenario.close(fixture)
}

pub fn overdraw_preserves_history_test() {
  let assert Ok(account) = stream.id("account/42")
  let opened = store.Event(account, "opened", 1000)
  let assert Ok(fixture) = scenario.start()
  let assert Ok(fixture) = scenario.given_events(fixture, account, [opened])
  let assert Ok(observation) = scenario.when(fixture, withdraw(account, 1100))

  observation
  |> scenario.failed_with(
    command.Rejected(command.BusinessRule("invalid withdrawal")),
  )
  |> scenario.then_events([opened])

  let assert Ok(Nil) = scenario.close(fixture)
}

fn withdraw(
  account: stream.Id,
  cents: Int,
) -> command.Command(Int, Int, String) {
  command.Command(
    streams: command.single(account),
    initial_state: fn() { 0 },
    apply: fn(balance, event) { balance + event.payload },
    discover: fn(_) { [] },
    handle: fn(balance) {
      case cents > 0 && balance >= cents {
        True -> Ok([store.Event(account, "withdrawn", -cents)])
        False -> Error(command.BusinessRule("invalid withdrawal"))
      }
    },
  )
}
```

Start a fresh scenario for each observation. `scenario.when` collects one batch
of up to 1,000 deliveries, including setup events, from its initial checkpoint.
For larger histories, use a memory store directly and check the relevant
stream with `store.read_stream` and `store.fold` or `store.collect`.

## Force a conflict

The following standalone module uses a memory store and injects conflicts on
its first two wrapped append calls. The command succeeds on its third attempt,
and replay finds one committed event. Put it in `test/eventcore_retry_test.gleam`
and run `gleam test`.

```gleam
import eventcore/command
import eventcore/memory
import eventcore/store
import eventcore/stream
import eventcore/testing/faults

pub fn retries_commit_one_batch_test() {
  let assert Ok(events) = memory.start()
  let storage = memory.as_store(events)
  let assert Ok(account) = stream.id("account/42")
  let deposit = store.Event(account, "deposited", 500)
  let operation =
    command.Command(
      streams: command.single(account),
      initial_state: fn() { Nil },
      apply: fn(_, _) { Nil },
      discover: fn(_) { [] },
      handle: fn(_) { Ok([deposit]) },
    )

  let assert Ok(policy) = command.retry_policy(2, command.Fixed(0))
  assert command.execute(faults.conflicts(storage, 2), operation, policy)
    == Ok(command.Execution(3))

  let assert Ok(history) = store.read_stream(storage, account)
  assert store.collect(history) == Ok([deposit])
  let assert Ok(Nil) = memory.close(events)
}
```

Each `faults.conflicts` wrapper has one append budget shared by all its callers.
Use a fresh wrapper for each independent retry test. For broader failure
coverage, wrap the store with `faults.with_chaos` and configure read and append
failures or append conflicts. A seed reproduces outcomes for the same sequence
of calls. When both choices are selected for an append, the conflict takes
precedence.

Run integration tests against the [storage backend](backends.html) your
application deploys. Cover payload encoding and decoding, storage lifecycle,
and restart or checkpoint behavior. The repository's
[PostgreSQL integration tests](https://github.com/10krco/eventcore-gl/blob/v1.0.0/test/eventcore_postgres_integration_test.gleam)
reuse the account commands exercised against memory and verify PostgreSQL-specific
behavior.

See the [scenario API](eventcore/testing/scenario.html) and the
[fault injection API](eventcore/testing/faults.html).
