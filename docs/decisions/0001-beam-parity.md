# 0001. Port the EventCore behavioral contract to the BEAM

Accepted at bootstrap.

Rust EventCore 2.1.0 provides command, storage, projection, modeling and
testing contracts. Rust language features and storage representations would
constrain the design of a Gleam library if treated as compatibility promises.

We will implement equivalent observable behavior and guarantees on Erlang/OTP.
Gleam types and module APIs may differ. Adapters may use independent durable
formats. Interoperability with existing Rust data files is outside this
decision. Porting tasks must demonstrate success, rejection, recovery and
concurrency behavior wherever the corresponding upstream contract covers them.

The release commit and evidence map live in the [inventory](../parity.md).
