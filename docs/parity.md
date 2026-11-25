# EventCore 2.1.0 feature inventory

The inventory records Gleam acceptance tests by capability. Each row needs a passing Gleam test and CI revision before it can count toward parity. The owning Tiber task records CI evidence. Use the task ID with `tiber show` to read its checklist.

The comparison uses the [published crate](https://crates.io/crates/eventcore/2.1.0), the [release manifest](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/Cargo.toml), and the release's supporting crates and tests. It covers the default macros, optional PostgreSQL and SQLite integrations, experimental modeling and model checking, and the memory, filesystem and testing crates. Source links are pinned to the release commit.

## Command runtime and in-memory store

Owning Tiber task `20261124-vkzx`.

| Capability and upstream reference | Observable acceptance scenario | BEAM evidence |
| --- | --- | --- |
| [Declare streams and execute commands](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/single_stream_command_test.rs) | Reject empty and duplicate stream declarations, validate stream identities, reconstruct state, enforce business rules and return typed validation, domain, storage or exhausted-conflict errors. | [Command tests](../test/eventcore_command_test.gleam) |
| [Discover participants dynamically](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/dynamic_stream_discovery_test.rs) | Read newly discovered streams after the initial declarations, rediscover participants on conflict replay, and commit against all participating versions. | [Conflict replay test](../test/eventcore_command_test.gleam) |
| [Prove atomic writes in memory](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/multi_stream_atomic_test.rs) | Execute a transfer, reject a stale read-only or writable participant, and show that every stream retains its pre-attempt contents on failure. | [Command transfer](../test/eventcore_command_test.gleam) and [memory tests](../test/eventcore_memory_test.gleam) |
| [Retry conflicts and instrument attempts](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/retry_policy_test.rs) | Exercise default, custom and exhausted retries, fixed and exponential backoff with jitter, the retry metrics hook, trace fields and the successful attempt count. | [Retry tests](../test/eventcore_command_test.gleam) |
| [Exercise store vocabulary and isolation](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-testing/src/contract.rs) | Read missing and existing streams, preserve per-stream order, allow repeated registrations with identical expected versions, reject contradictory versions and unregistered appends, propagate the first stream error and preserve batch atomicity. Pass the shared adapter contracts in memory. | [Store tests](../test/eventcore_store_test.gleam) and [shared adapter contract](../test/store_contract.gleam) |

Gleam keeps zero exponential backoff at zero. The Rust 2.1.0 implementation overflows its intermediate power calculation starting at retry 65, yielding a saturated delay.

## Durable storage and command state

Owning Tiber task `20261124-m7wf`.

| Capability and upstream reference | Observable acceptance scenario | BEAM evidence |
| --- | --- | --- |
| [Persist and verify filesystem transactions](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-fs/tests/git_robustness_test.rs) | Reopen a store after atomic multi-stream append, exercise configured sync policies, and detect a tampered transaction and a dangling parent. | [Filesystem journal tests](../test/eventcore_filesystem_test.gleam) |
| [Merge independently written filesystem replicas](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-fs/tests/merge_mode_test.rs) | Distinguish copied replica identities, detect shared-replica conflicts and forks, decline or reconcile a fork, and obtain identical canonical history in either union direction, including merges of merges. | [Replica reconciliation tests](../test/eventcore_filesystem_test.gleam) |
| [Run SQLite storage contracts](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-sqlite/tests/contract_suite_test.rs) | Pass the shared adapter contracts, persist batch appends across reopen, accept a supplied connection, and migrate fresh encrypted event and checkpoint databases. | [SQLite event and checkpoint tests](../test/eventcore_sqlite_test.gleam). Snapshot and projection scenarios are tracked in their own rows. |
| [Run PostgreSQL storage contracts](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-postgres/tests/contract_suite_test.rs) | Pass shared contracts against a real database, migrate and reconnect, commit batches atomically, retry competing writers, and assert the published observability behavior. | [Live PostgreSQL contract](../test/eventcore_postgres_integration_test.gleam) and [two-pool scenarios](../test/postgres_contract.gleam). |
| [Restore and catch up command state](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/command_state_snapshot_test.rs) | Handle a missing snapshot, persist monotonically advancing snapshots, exercise the default 100-event refresh threshold and replay only required events in stream order. Exercise restart on each supporting adapter. | [Replay and refresh tests](../test/eventcore_command_test.gleam), [journal restart and invalidation tests](../test/eventcore_filesystem_test.gleam), and [SQLite restart tests](../test/eventcore_sqlite_test.gleam). The [PostgreSQL contract](../test/postgres_contract.gleam) reopens the snapshot through a second pool and rejects a regressing version. |

Rust 2.1.0 closes a fork when any transaction joins its ancestry and passes
only the initial competing transactions to the resolver. Gleam keeps the
fork open until an explicit resolution names it and supplies each branch's
events through its current head. Unrelated streams can advance while writes
to the forked stream wait for resolution. These differences prevent unrelated
writes from concealing a conflict and give the resolver the full evidence.
[Rust fork detection](https://github.com/jwilger/eventcore/blob/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-fs/src/merge.rs#L231)
[Rust resolver context](https://github.com/jwilger/eventcore/blob/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-fs/src/lib.rs#L622)

## Projections

Owning Tiber task `20261124-7mt3`.

| Capability and upstream reference | Observable acceptance scenario | BEAM evidence |
| --- | --- | --- |
| [Replay filtered events and resume checkpoints](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-types/src/projection.rs) | Page by opaque position with all, prefix, pattern and event-type selections. Match wildcards across separators and reject invalid patterns. Project mixed types, establish subscription leadership, release it on exit and resume from the persisted checkpoint. | [Memory paging and checkpoint resume](../test/eventcore_projection_test.gleam). The [memory runner](../test/eventcore_projection_runner_test.gleam) verifies per-name leadership and release. The filesystem adapter persists named progress and coordinates leases on its BEAM node. [SQLite replay and checkpoints](../test/eventcore_sqlite_test.gleam) cover separate files, lease contention, filtered paging, and concurrent commits. PostgreSQL is tracked separately. |
| [Exercise legacy failure and polling policies](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/src/projection.rs) | Run batch and continuous projections, including empty polls and failures. Exercise fatal, skip and bounded retry decisions and show which checkpoints advance. | [Legacy runner tests](../test/eventcore_projection_runner_test.gleam) cover retries, skip, fatal, empty polling cancellation, and checkpoint advancement. |
| [Recover filesystem projections after a merge](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-fs/tests/projection_after_merge_test.rs) | Deliver an earlier event introduced by a merge to a live projection and compare rebuilt state against live state. | [Filesystem projection tests](../test/eventcore_filesystem_test.gleam) cover stable positions, checkpoint reopen, late imports, and replay from the beginning. |
| [Distinguish PostgreSQL legacy and transactional progress](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-testing/src/projection_contract.rs) | Reset and replay a legacy checkpoint. For the transactional runner, commit mutation and progress together, roll both back on failure, reload progress on retry, and report lost acknowledgement as indeterminate. | Planned |
| [Deliver PostgreSQL pages at a committed frontier](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-postgres/tests/projection_delivery_contract_test.rs) | Preserve envelope fields and raw JSON precision. Backfill pre-migration events, observe legacy writes, bound batches by a fixed watermark, and drain filtered pages during concurrent commits. | [Live two-pool delivery contract](../test/postgres_contract.gleam) and [selection validation](../test/eventcore_projection_delivery_test.gleam). |
| [Fence, cancel, reset and replay transactional projectors](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-postgres/tests/transactional_projection_contract_test.rs) | Validate source and selection identity even for empty pages, fence lost leadership, classify stop/skip/fatal and after-commit outcomes, deliver post-catch-up events, cancel idle polling and perform coordinated atomic reset and replay. | Planned |

## Typed modeling and model checking

Owning Tiber task `20261124-f83b`.

| Capability and upstream reference | Observable acceptance scenario | BEAM evidence |
| --- | --- | --- |
| [Derive command stream declarations and enforce requirements](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-macros/tests/command_derive_macro_test.rs) | Exercise single- and multi-stream command conveniences and requirement guards. Reject malformed declarations and wrong stream types using Gleam-native checks. | Planned |
| [Build typed components and mappings](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-macros/tests/ui) | Build valid stream identities, inputs, commands, events, state, read models, effects, outputs, recipes and fallible mappings. Reject missing fields, role swaps, invalid origins, payloads and transforms. | Planned |
| [Run modeled commands and projections](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/modeled_command_test.rs) | Execute modeled commands through the runtime, produce typed events, apply or explicitly ignore projected effects through a checked sink, and expose a typed view. | Planned |
| [Check descriptors, assumptions and failures](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/tests/model_checker_failures_test.rs) | Run default and configured checks against complete and incomplete graphs. Verify diagnostics and distinguish proven coverage, explicit assumptions and rejected models. | Planned |

## Testing tools, examples, and parity verification

Owning Tiber task `20261124-9whz`.

| Capability and upstream reference | Observable acceptance scenario | BEAM evidence |
| --- | --- | --- |
| [Implement reusable adapter and projector contract suites](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-testing/src/projection_contract.rs) | Run reusable store, checkpoint, coordination, delivery, transaction, continuous-processing and reset contracts on their supported adapters. | Planned |
| [Provide deterministic testing and fault injection](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-testing/src/lib.rs) | Run given/when/then scenarios, collect emitted events, inject deterministic conflicts and configurable chaos, and verify operation names on failures. | Planned |
| [Port the published examples](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore-examples/tests) | Run the single-stream, transfer, retry, scenario and experimental-modeling examples as tests. Exercise the representative bank account with memory and PostgreSQL. | Planned |
| [Close the evidence inventory](https://github.com/jwilger/eventcore/tree/fea3e386cce904e9ef9b76cd303926870d04a692/eventcore/Cargo.toml) | Audit release features, public exports, examples and tests against this inventory, add missing rows, and attach passing Gleam test and CI references to every row before claiming parity. | Planned |
