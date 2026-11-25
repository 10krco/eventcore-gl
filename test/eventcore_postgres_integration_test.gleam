import eventcore/postgres
import eventcore/store
import gleam/erlang/process.{type Pid}
import gleam/otp/actor
import pog
import postgres_contract

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
