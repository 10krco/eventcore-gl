//// EventCore records domain events and builds read models on Erlang/OTP.
////
//// - [Commands](eventcore/command.html) replay stream history, make a
////   decision, and commit events across streams atomically.
//// - [Streams](eventcore/stream.html), the [store](eventcore/store.html),
////   and [snapshots](eventcore/snapshot.html) define the storage contracts.
//// - [Memory](eventcore/memory.html), [filesystem](eventcore/filesystem.html),
////   [SQLite](eventcore/sqlite.html), and [PostgreSQL](eventcore/postgres.html)
////   implement the store interface.
//// - [Projection reads](eventcore/projection.html),
////   [delivery](eventcore/projection_delivery.html), and the
////   [portable runner](eventcore/projection_runner.html) connect committed
////   history to read models. The
////   [PostgreSQL runner](eventcore/postgres_projection.html) commits read-model
////   writes and progress together.
//// - [Testing tools](testing.html) exercise commands, concurrency retries,
////   projections, and backend contracts.
////
//// The documented public APIs are covered by EventCore's 1.x compatibility
//// guarantee, except for the experimental [model](eventcore/model.html),
//// [model runtime](eventcore/model_runtime.html), and
//// [model checker](eventcore/model_checker.html) modules.

/// The upstream Rust release used for behavioral comparisons.
pub const upstream_release = "2.1.0"
