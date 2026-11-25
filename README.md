# EventCore for Gleam

EventCore is an event-sourcing library for Gleam on Erlang/OTP. Write business
rules against state reconstructed from events, commit changes across multiple
streams in one operation, and build read models from committed history.

## Install

Install EventCore from your Gleam project directory.

```sh
gleam add eventcore
```

Use the Erlang target. Building EventCore requires Gleam, Erlang/OTP, Rebar3,
`make`, a C compiler, and Erlang/OTP development headers. The build compiles the bundled SQLite dependency
even if your application uses only the in-memory adapter. The release toolchain
was verified on x86-64 Linux with Gleam 1.19.2 and Erlang/OTP 28.5.0.8.

## Run your first command

The following example records additions and withdrawals on a single account
stream. It reads the resulting history and prints the balance, in cents.

```sh
gleam new eventcore_demo
cd eventcore_demo
gleam add eventcore
```

Save this module as `src/eventcore_demo.gleam`.

```gleam
import eventcore/command
import eventcore/memory
import eventcore/store
import eventcore/stream
import gleam/int
import gleam/io

pub fn main() {
  let assert Ok(events) = memory.start()
  let storage = memory.as_store(events)
  let assert Ok(account) = stream.id("account/42")
  let policy = command.default_policy()

  let assert Ok(_) = command.execute(storage, change(account, 1500), policy)
  let assert Ok(_) = command.execute(storage, change(account, -400), policy)
  let assert Ok(history) = store.read_stream(storage, account)
  let assert Ok(balance) = store.fold(history, 0, add_delta)

  io.println("Balance in cents: " <> int.to_string(balance))
  let assert Ok(Nil) = memory.close(events)
}

fn change(account: stream.Id, delta: Int) -> command.Command(Int, Int, String) {
  command.Command(
    streams: command.single(account),
    initial_state: fn() { 0 },
    apply: add_delta,
    discover: fn(_) { [] },
    handle: fn(balance) {
      case balance + delta >= 0 {
        True -> Ok([store.Event(account, "balance-changed", delta)])
        False -> Error(command.BusinessRule("insufficient funds"))
      }
    },
  )
}

fn add_delta(balance: Int, event: store.Event(Int)) -> Int {
  balance + event.payload
}
```

Run `gleam run`. The example prints `Balance in cents: 1100`. EventCore replays
the first event before deciding whether to accept the withdrawal. The memory
adapter keeps history for the lifetime of its store process.

## Learn EventCore

- [Commands and atomic transfers](https://hexdocs.pm/eventcore/1.0.0/commands.html)
  explains stream participation, business rules, and concurrency retries.
- [Projections](https://hexdocs.pm/eventcore/1.0.0/projections.html) explains
  delivery, checkpoints, and transactional PostgreSQL read models.
- [Storage backends](https://hexdocs.pm/eventcore/1.0.0/backends.html) compares
  in-memory, filesystem, SQLite, and PostgreSQL storage and their requirements.
- [Testing](https://hexdocs.pm/eventcore/1.0.0/testing.html) shows how to assert
  event histories, rejections, and retry behavior.
- [API reference](https://hexdocs.pm/eventcore/1.0.0/eventcore.html) starts at
  the package's root module. HexDocs lists each public module in its navigation.

## API stability

EventCore's documented public API follows semantic versioning. The
`eventcore/model`, `eventcore/model_runtime`, and `eventcore/model_checker`
modules are experimental and may change in a 1.x minor release.

## Project

[Source](https://github.com/10krco/eventcore-gl/tree/v1.0.0) ·
[Architecture](https://github.com/10krco/eventcore-gl/blob/v1.0.0/ARCHITECTURE.md) ·
[License (MIT)](https://github.com/10krco/eventcore-gl/blob/v1.0.0/LICENSE) ·
[Contributing](https://github.com/10krco/eventcore-gl/blob/v1.0.0/docs/contributing.md)
