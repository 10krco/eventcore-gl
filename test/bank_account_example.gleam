//// Account commands and a balance projection shared by memory and PostgreSQL.

import eventcore/command
import eventcore/projection
import eventcore/projection_runner as runner
import eventcore/store.{type Event, Event}
import eventcore/stream.{type Id}
import gleam/dict.{type Dict}
import gleam/list
import gleam/result

pub type AccountError {
  AlreadyOpen
  AccountClosed(Id)
  InvalidAmount
  InsufficientFunds
  SameAccount
}

type AccountCommand =
  command.Command(Dict(Id, Int), Int, AccountError)

pub fn open(account: Id) -> AccountCommand {
  account_command(command.single(account), fn(balances) {
    use _ <- result.try(
      command.require(!dict.has_key(balances, account), fn() { AlreadyOpen }),
    )
    Ok([Event(account, "opened", 0)])
  })
}

pub fn deposit(account: Id, cents: Int) -> AccountCommand {
  account_command(command.single(account), fn(balances) {
    use _ <- result.try(balance(balances, account))
    use _ <- result.try(command.require(cents > 0, fn() { InvalidAmount }))
    Ok([Event(account, "deposited", cents)])
  })
}

pub fn withdraw(account: Id, cents: Int) -> AccountCommand {
  account_command(command.single(account), fn(balances) {
    use _ <- result.try(can_withdraw(balances, account, cents))
    Ok([Event(account, "withdrawn", -cents)])
  })
}

pub fn transfer(from: Id, to: Id, cents: Int) -> AccountCommand {
  account_command(command.across(from, [to]), fn(balances) {
    use _ <- result.try(command.require(from != to, fn() { SameAccount }))
    use _ <- result.try(can_withdraw(balances, from, cents))
    use _ <- result.try(balance(balances, to))
    Ok([
      Event(from, "transferred-out", -cents),
      Event(to, "transferred-in", cents),
    ])
  })
}

fn account_command(
  declarations: command.Declarations,
  decide: fn(Dict(Id, Int)) ->
    Result(List(Event(Int)), command.Rejection(AccountError)),
) -> AccountCommand {
  command.Command(declarations, dict.new, apply, fn(_) { [] }, decide)
}

fn can_withdraw(
  balances: Dict(Id, Int),
  account: Id,
  cents: Int,
) -> Result(Nil, command.Rejection(AccountError)) {
  use available <- result.try(balance(balances, account))
  use _ <- result.try(command.require(cents > 0, fn() { InvalidAmount }))
  command.require(available >= cents, fn() { InsufficientFunds })
}

fn balance(
  balances: Dict(Id, Int),
  account: Id,
) -> Result(Int, command.Rejection(AccountError)) {
  dict.get(balances, account)
  |> result.replace_error(command.BusinessRule(AccountClosed(account)))
}

fn apply(balances: Dict(Id, Int), event: Event(Int)) -> Dict(Id, Int) {
  let previous = dict.get(balances, event.stream) |> result.unwrap(0)
  dict.insert(balances, event.stream, previous + event.payload)
}

pub fn project(
  backend: runner.Backend(Int),
  name: String,
) -> Result(Dict(Id, Int), runner.RunError(Nil)) {
  let assert Ok(filter) = projection.prefix("bank/")
  let projector =
    runner.Projector(
      name: name,
      filter: filter,
      initial: dict.new(),
      apply: fn(balances, delivery) {
        #(apply(balances, delivery.event), Ok(Nil))
      },
      on_error: fn(_) { runner.Fatal },
    )
  runner.run(backend, projector, runner.default_config, runner.uninterrupted())
}

pub fn check(storage: store.Store(Int), backend: runner.Backend(Int)) -> Nil {
  let assert Ok(source) = stream.id("bank/source")
  let assert Ok(target) = stream.id("bank/target")
  let execute = fn(operation) {
    command.execute(storage, operation, command.default_policy())
  }
  let assert Error(command.Rejected(command.BusinessRule(AccountClosed(id)))) =
    execute(deposit(source, 1))
  assert id == source
  assert execute(open(source)) == Ok(command.Execution(1))
  assert execute(deposit(source, 150)) == Ok(command.Execution(1))
  assert execute(withdraw(source, 50)) == Ok(command.Execution(1))
  assert execute(open(target)) == Ok(command.Execution(1))
  assert execute(deposit(target, 20)) == Ok(command.Execution(1))
  assert execute(transfer(source, target, 30)) == Ok(command.Execution(1))
  assert execute(transfer(source, target, 80))
    == Error(command.Rejected(command.BusinessRule(InsufficientFunds)))
  let assert Ok(balances) = project(backend, "bank-account-example")
  assert dict.get(balances, source) == Ok(70)
  assert dict.get(balances, target) == Ok(50)
  assert list.fold(dict.values(balances), 0, fn(sum, value) { sum + value })
    == 120
  let assert Ok(history) = store.read_stream(storage, source)
  let assert Ok(events) = store.collect(history)
  assert list.map(events, fn(event) { event.kind })
    == ["opened", "deposited", "withdrawn", "transferred-out"]
  let assert Ok(history) = store.read_stream(storage, target)
  let assert Ok(events) = store.collect(history)
  assert list.map(events, fn(event) { event.kind })
    == ["opened", "deposited", "transferred-in"]
  Nil
}
