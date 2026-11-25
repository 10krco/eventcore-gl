# Contribute to EventCore for Gleam

Use Nix with flakes enabled on x86-64 Linux. From a fresh checkout:

```sh
git clone https://github.com/10krco/eventcore-gl.git
cd eventcore-gl
nix develop
lefthook install
gleam format --check
gleam build --warnings-as-errors
gleam test
gleam docs build
tiber list
```

With direnv and its Nix flake integration enabled, run `direnv allow` in the
checkout to enter the same locked shell. Git reads the board from
`origin/tiber`. Publishing board changes requires push access.

The Nix shell uses SQLCipher and provides PostgreSQL binaries for the
integration tests, which start disposable databases over local Unix sockets.
CI also runs the suite with bundled SQLite. To reproduce that mode from the
Nix shell, rebuild the native dependency first.

```sh
gleam clean
env -u USE_SYSTEM_SQLITE -u CFLAGS -u LDFLAGS EVENTCORE_SQLITE_TEST_BACKEND=plain gleam test
```

The [feature inventory](parity.md) tracks behavioral parity with
[EventCore 2.1.0](https://crates.io/crates/eventcore/2.1.0).
[Architecture](../ARCHITECTURE.md) records the port's design decisions.
The [bank account example](../test/bank_account_example.gleam) and
[modeled transfer example](../test/model_transfer_example.gleam) are exercised
by `gleam test`. Use the [repository instructions](../AGENTS.md) for branch,
commit, and change review requirements.
