# EventCore for Gleam

A Gleam library for multi-stream event sourcing on Erlang/OTP. Porting progress
is tracked in the [feature inventory](docs/parity.md) and on the Tiber board.

## Develop

Use Nix with flakes enabled on x86-64 Linux. From a fresh checkout:

```sh
git clone https://github.com/10krco/eventcore-gl.git
cd eventcore-gl
nix develop
lefthook install
gleam format --check
gleam build --warnings-as-errors
gleam test
tiber list
```

With direnv and its Nix flake integration enabled, run `direnv allow` in the
checkout to enter the same locked shell. Git reads the board from
`origin/tiber`. Publishing board changes requires push access.

## Runnable examples

Run these examples with `gleam test` in the development shell.

- [Bank account commands](test/bank_account_example.gleam) open accounts,
  make deposits and withdrawals, transfer between streams, and project balances
  from committed events. The same flow runs on
  [memory](test/eventcore_examples_test.gleam) and
  [PostgreSQL](test/eventcore_postgres_integration_test.gleam).
- [Retry example](test/eventcore_examples_test.gleam) forces two conflicts and
  verifies that the deposit appears once in the event history.
- [Given, when, and then](test/eventcore_testing_tools_test.gleam) sets up
  event history and checks successful and rejected commands.
- [Modeled transfer](test/model_transfer_example.gleam) assembles required
  typed fields. [Modeled runtime tests](test/eventcore_model_runtime_test.gleam)
  execute a modeled command and read its projected state through a typed view.

## Parity baseline

The comparison release is [EventCore 2.1.0](https://crates.io/crates/eventcore/2.1.0)
at [fea3e386](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692).
The port targets equivalent behavior on the BEAM with Gleam-native APIs and
independent storage formats. [Architecture](ARCHITECTURE.md) records this decision.
