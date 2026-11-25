import bank_account_example
import eventcore/command
import eventcore/memory
import eventcore/store
import eventcore/stream
import eventcore/testing/faults

pub fn bank_account_memory_example_test() {
  let assert Ok(events) = memory.start()
  bank_account_example.check(
    memory.as_store(events),
    memory.projection_backend(events),
  )
  assert memory.close(events) == Ok(Nil)
}

pub fn account_deposit_retries_without_duplicate_events_test() {
  let assert Ok(events) = memory.start()
  let storage = memory.as_store(events)
  let assert Ok(id) = stream.id("bank/retry")
  assert command.execute(
      storage,
      bank_account_example.open(id),
      command.default_policy(),
    )
    == Ok(command.Execution(1))
  let delayed = faults.conflicts(storage, 2)
  assert command.execute(
      delayed,
      bank_account_example.deposit(id, 50),
      command.default_policy(),
    )
    == Ok(command.Execution(3))
  let assert Ok(history) = store.read_stream(storage, id)
  assert store.collect(history)
    == Ok([
      store.Event(id, "opened", 0),
      store.Event(id, "deposited", 50),
    ])
  assert memory.close(events) == Ok(Nil)
}
