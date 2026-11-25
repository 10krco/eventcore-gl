import bank_account_example
import eventcore/internal/postgres_projection_progress as progress
import eventcore/internal/postgres_projection_session as session
import eventcore/postgres
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner
import eventcore/store
import gleam/erlang/process.{type Pid}
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import pog
import postgres_contract
import postgres_transactional_contract

pub fn postgres_two_pool_contract_test() {
  use socket <- with_database()
  use <- check_started_store(socket)
  use first <- with_pool(socket, "eventcore_first", 1)
  use second <- with_pool(socket, "eventcore_second", 1)
  use larger <- with_pool(socket, "eventcore_larger", 2)
  check_operations(fn() { postgres_contract.check(first, second) })
  postgres_contract.check_distinct_names(larger, second)
}

fn check_started_store(socket: String, work: fn() -> Nil) {
  let codec =
    postgres.Codec(fn(_, bytes) { Ok(bytes) }, fn(_, bytes) { Ok(bytes) })
  let assert Error(store.StoreFailure(store.StartStore)) =
    postgres.start_url(
      process.new_name("invalid_eventcore"),
      "not a database URL",
      codec,
    )
  let assert Ok(#(pid, events)) =
    postgres.start(pool_config(socket, "eventcore_started"), codec)
  using_pool(pid, fn() {
    assert postgres.ping(events) == Ok(Nil)
    Nil
  })
  work()
}

fn with_pool(
  socket: String,
  name: String,
  size: Int,
  work: fn(pog.Connection) -> Nil,
) {
  let assert Ok(actor.Started(pid, pool)) =
    pog.start(pool_config(socket, name) |> pog.pool_size(size))
  using_pool(pid, fn() { work(pool) })
}

fn pool_config(socket: String, name: String) -> pog.Config {
  pog.default_config(process.new_name(name))
  |> pog.host(socket)
  |> pog.database("postgres")
  |> pog.user("eventcore")
  |> pog.pool_size(1)
}

@external(erlang, "eventcore_postgres_test_ffi", "with_database")
fn with_database(work: fn(String) -> Nil) -> Nil

@external(erlang, "eventcore_postgres_test_ffi", "using_pool")
fn using_pool(pid: Pid, work: fn() -> Nil) -> Nil

@external(erlang, "eventcore_postgres_test_ffi", "check_operations")
fn check_operations(work: fn() -> Nil) -> Nil

pub fn postgres_transactional_progress_contract_test() {
  use socket <- with_database()
  use first <- with_pool(socket, "eventcore_progress_first", 1)
  use second <- with_pool(socket, "eventcore_progress_second", 1)
  assert progress.migrate(first) == Ok(Nil)
  assert progress.migrate(second) == Ok(Nil)
  let assert Ok(name) = delivery.projector_name("transactional-progress")
  let assert Ok(source) = delivery.source_id("events")
  let assert Ok(selection) = delivery.selection_id("selected")
  let assert Ok(position) = projection.position(1)
  let candidate = progress.Progress(source, selection, position)
  assert progress.load(second, name) == Ok(None)
  assert session.with_leader(first, name, fn(leader) {
      assert session.with_leader(second, name, fn(_) { Ok(Nil) })
        == Error(session.AcquisitionFailed(projection_runner.AlreadyLeading))
      let assert Ok(transaction) = session.begin(leader)
      assert progress.advance(transaction, name, candidate) == Ok(Nil)
      assert session.rollback(leader) == Ok(Nil)
      assert progress.load(second, name) == Ok(None)
      let assert Ok(transaction) = session.begin(leader)
      assert progress.advance(transaction, name, candidate) == Ok(Nil)
      assert session.commit(leader) == Ok(Nil)
      Ok(Nil)
    })
    == Ok(Ok(Nil))
  assert progress.load(second, name) == Ok(Some(candidate))
  assert pog.transaction(second, fn(tx) {
      use observed <- result.try(progress.lock(tx, name))
      progress.delete(tx, name, observed)
    })
    == Ok(Nil)
  assert progress.load(first, name) == Ok(None)
  Nil
}

pub fn postgres_transactional_projection_contract_test() {
  use socket <- with_database()
  use first <- with_pool(socket, "eventcore_transactional_first", 1)
  use second <- with_pool(socket, "eventcore_transactional_second", 1)
  postgres_transactional_contract.check(first, second)
}

pub fn bank_account_postgres_example_test() {
  use socket <- with_database()
  use writes <- with_pool(socket, "bank_writes", 1)
  use replay <- with_pool(socket, "bank_replay", 1)
  let codec =
    postgres.Codec(fn(_, cents) { Ok(<<cents:64>>) }, fn(_, bytes) {
      case bytes {
        <<cents:64-signed>> -> Ok(cents)
        _ -> Error("invalid account event")
      }
    })
  let events = postgres.from_pool(writes, codec)
  let reopened = postgres.from_pool(replay, codec)
  assert postgres.migrate(events) == Ok(Nil)
  assert postgres.migrate_checkpoints(postgres.checkpoints_from_pool(replay))
    == Ok(Nil)
  bank_account_example.check(
    postgres.as_store(events),
    postgres.projection_backend(reopened),
  )
}
