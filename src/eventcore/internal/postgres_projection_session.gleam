import eventcore/projection_delivery as delivery
import eventcore/projection_runner.{type LeadershipError}
import gleam/result
import pog

pub type Leader

pub type LeaderError {
  AcquisitionFailed(LeadershipError)
  ReleaseFailed
}

type Command {
  Begin
  Commit
  Rollback
  Check
}

pub type CommandError {
  SessionFailed
  RolledBack
}

pub fn with_leader(
  pool: pog.Connection,
  name: delivery.ProjectorName,
  work: fn(Leader) -> Result(value, error),
) -> Result(Result(value, error), LeaderError) {
  with_transactional_leader(pool, delivery.projector_name_value(name), work)
}

pub fn check(leader: Leader) -> Result(Nil, CommandError) {
  transactional_command(leader, Check) |> result.map(fn(_) { Nil })
}

pub fn begin(leader: Leader) -> Result(pog.Connection, CommandError) {
  transactional_command(leader, Begin)
}

pub fn commit(leader: Leader) -> Result(Nil, CommandError) {
  transactional_command(leader, Commit) |> result.map(fn(_) { Nil })
}

pub fn rollback(leader: Leader) -> Result(Nil, CommandError) {
  transactional_command(leader, Rollback) |> result.map(fn(_) { Nil })
}

@external(erlang, "eventcore_postgres_ffi", "with_transactional_leader")
fn with_transactional_leader(
  pool: pog.Connection,
  name: String,
  work: fn(Leader) -> Result(value, error),
) -> Result(Result(value, error), LeaderError)

@external(erlang, "eventcore_postgres_ffi", "transactional_command")
fn transactional_command(
  leader: Leader,
  command: Command,
) -> Result(pog.Connection, CommandError)

pub type Transaction

@external(erlang, "eventcore_postgres_ffi", "transactional_scope")
pub fn scope(leader: Leader) -> Transaction

@external(erlang, "eventcore_postgres_ffi", "transactional_connection")
pub fn connection(transaction: Transaction) -> Result(pog.Connection, Nil)
