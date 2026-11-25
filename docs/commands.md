# Commands and atomic transfers

A command reads the event streams it needs, reconstructs the state used by its
business rules, and returns proposed events. EventCore checks the versions it
read and submits the whole batch as one atomic append. A transfer can debit
one account and credit another in a single command.

## Define the decision

This module uses integer cents as signed event deltas. An account is open
once its stream contains an opening event. A transfer reads both account
streams, checks both accounts exist, and writes one event to each. For an
existing project with EventCore installed, put this code in `src/accounts.gleam`.

```gleam
import eventcore/command
import eventcore/store.{type Event, Event}
import eventcore/stream.{type Id}
import gleam/dict.{type Dict}
import gleam/result

pub type AccountError {
  AlreadyOpen
  AccountClosed(Id)
  InvalidAmount
  SameAccount
  InsufficientFunds
}

pub fn open(
  account: Id,
  cents: Int,
) -> command.Command(Dict(Id, Int), Int, AccountError) {
  command.Command(
    streams: command.single(account),
    initial_state: dict.new,
    apply: apply,
    discover: fn(_) { [] },
    handle: fn(balances) {
      use _ <- result.try(command.require(cents >= 0, fn() { InvalidAmount }))
      use _ <- result.try(
        command.require(!dict.has_key(balances, account), fn() { AlreadyOpen }),
      )
      Ok([Event(account, "opened", cents)])
    },
  )
}

pub fn transfer(
  source: Id,
  destination: Id,
  cents: Int,
) -> command.Command(Dict(Id, Int), Int, AccountError) {
  command.Command(
    streams: command.across(source, [destination]),
    initial_state: dict.new,
    apply: apply,
    discover: fn(_) { [] },
    handle: fn(balances) {
      use _ <- result.try(
        command.require(source != destination, fn() { SameAccount }),
      )
      use _ <- result.try(command.require(cents > 0, fn() { InvalidAmount }))
      use available <- result.try(balance(balances, source))
      use _ <- result.try(balance(balances, destination))
      use _ <- result.try(
        command.require(available >= cents, fn() { InsufficientFunds }),
      )
      Ok([
        Event(source, "transferred-out", -cents),
        Event(destination, "transferred-in", cents),
      ])
    },
  )
}

fn balance(
  balances: Dict(Id, Int),
  account: Id,
) -> Result(Int, command.Rejection(AccountError)) {
  dict.get(balances, account)
  |> result.replace_error(command.BusinessRule(AccountClosed(account)))
}

fn apply(balances: Dict(Id, Int), event: Event(Int)) -> Dict(Id, Int) {
  case event.kind, dict.get(balances, event.stream) {
    "opened", Error(_) -> dict.insert(balances, event.stream, event.payload)
    "transferred-out", Ok(previous) | "transferred-in", Ok(previous) ->
      dict.insert(balances, event.stream, previous + event.payload)
    _, _ -> balances
  }
}
```

## Execute and inspect the result

Replace the quickstart's `src/eventcore_demo.gleam` with the module below and
run `gleam run`. The assertions check that the accepted transfer leaves
7,000 cents in the source account and 5,000 cents in the destination, and
that the rejected transfer leaves both histories unchanged.

```gleam
import accounts
import eventcore/command
import eventcore/memory
import eventcore/store.{Event}
import eventcore/stream

pub fn main() {
  let assert Ok(events) = memory.start()
  let storage = memory.as_store(events)
  let assert Ok(source) = stream.id("account/source")
  let assert Ok(destination) = stream.id("account/destination")
  let policy = command.default_policy()

  let assert Ok(_) =
    command.execute(storage, accounts.open(source, 10_000), policy)
  let assert Ok(_) =
    command.execute(storage, accounts.open(destination, 2000), policy)

  assert command.execute(
      storage,
      accounts.transfer(source, destination, 3000),
      policy,
    )
    == Ok(command.Execution(1))
  assert command.execute(
      storage,
      accounts.transfer(source, destination, 8000),
      policy,
    )
    == Error(command.Rejected(command.BusinessRule(accounts.InsufficientFunds)))

  let assert Ok(source_history) = store.read_stream(storage, source)
  assert store.collect(source_history)
    == Ok([
      Event(source, "opened", 10_000),
      Event(source, "transferred-out", -3000),
    ])

  let assert Ok(destination_history) = store.read_stream(storage, destination)
  assert store.collect(destination_history)
    == Ok([
      Event(destination, "opened", 2000),
      Event(destination, "transferred-in", 3000),
    ])

  let assert Ok(Nil) = memory.close(events)
}
```

## Participation and replay

- Use `command.single` when a command needs one stream and `command.across`
  when it already knows its participants. Use `command.declare` to validate a
  list of identifiers and report empty lists or duplicates explicitly.
- Use `discover` for identifiers learned during replay, for example a policy
  stream referenced by an account event. EventCore discovers streams after
  replaying each participant and visits each stream once per attempt.
- Include every stream whose state can change the decision. When appending a
  nonempty batch, EventCore checks the versions of read-only participants as
  well. A produced event must belong to a declared or discovered participant.
- Keep `initial_state`, `apply`, `discover`, and `handle` deterministic and free
  of side effects. Replay processes one stream at a time, with events in each
  stream's version order. An append conflict can rerun the entire decision
  against newer history. Process external effects from committed events using
  a [projection](projections.html).

The default policy allows four retries after the initial attempt, with
jittered exponential backoff with a 10-millisecond base. The returned
`Execution.attempts` includes the initial attempt. A business-rule rejection
returns `Rejected`. An append conflict that exhausts the policy returns
`ConcurrencyError`. Other store failures return `StorageError`.

For commands with long histories, `execute_with_snapshot` can reuse replay
state through a snapshot repository and a state codec. Choose a key that
covers the command scope and state schema. Use a new key when changing the
encoding or the way callbacks reconstruct state.

See the [command API](eventcore/command.html), the
[testing guide](testing.html), and the full
[bank account example](https://github.com/10krco/eventcore-gl/blob/v1.0.0/test/bank_account_example.gleam)
for deposits, withdrawals, and projected balances.
