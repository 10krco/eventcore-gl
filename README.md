# EventCore for Gleam

A Gleam library for multi-stream event sourcing on Erlang/OTP. The bootstrap
contains a loadable package and its development environment. Porting progress
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

## Parity baseline

The comparison release is [EventCore 2.1.0](https://crates.io/crates/eventcore/2.1.0)
at [fea3e386](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692).
The port targets equivalent behavior on the BEAM with Gleam-native APIs and
independent storage formats. [Architecture](ARCHITECTURE.md) records this decision.
