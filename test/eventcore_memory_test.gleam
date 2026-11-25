import eventcore/memory
import eventcore/store
import eventcore/stream
import eventcore/testing/adapter_contract as store_contract

pub fn append_is_atomic_with_stale_read_only_participant_test() {
  let assert Ok(memory) = memory.start()
  let storage = memory.as_store(memory)
  let assert Ok(debit) = stream.id("account/debit")
  let assert Ok(credit) = stream.id("account/credit")
  let zero = stream.initial_version
  let one = stream.next_version(zero)

  let opening = store.Event(credit, "opened", 100)
  let assert Ok(batch) = store.register(store.writes(), credit, zero)
  let assert Ok(batch) = store.append(batch, opening)
  assert storage.append_events(batch) == Ok(Nil)

  let withdrawal = store.Event(debit, "debited", 20)
  let assert Ok(batch) = store.register(store.writes(), debit, zero)
  let assert Ok(batch) = store.register(batch, credit, zero)
  let assert Ok(batch) = store.append(batch, withdrawal)
  assert storage.append_events(batch)
    == Error(store.VersionConflict(credit, zero, one))
  let assert Ok(events) = store.read_stream(storage, debit)
  assert store.collect(events) == Ok([])
  let assert Ok(events) = store.read_stream(storage, credit)
  assert store.collect(events) == Ok([opening])

  let deposit = store.Event(credit, "credited", 20)
  let assert Ok(batch) = store.register(store.writes(), debit, zero)
  let assert Ok(batch) = store.register(batch, credit, one)
  let assert Ok(batch) = store.append(batch, withdrawal)
  let assert Ok(batch) = store.append(batch, deposit)
  assert storage.append_events(batch) == Ok(Nil)
  let assert Ok(events) = store.read_stream(storage, debit)
  assert store.collect(events) == Ok([withdrawal])
  let assert Ok(events) = store.read_stream(storage, credit)
  assert store.collect(events) == Ok([opening, deposit])
  let assert Ok(events) = storage.read_after(credit, one)
  assert store.collect(events) == Ok([deposit])
  assert memory.close(memory) == Ok(Nil)
  assert store.read_stream(storage, credit)
    == Error(store.StoreFailure(store.ReadStream))
}

@external(erlang, "eventcore_memory_test_ffi", "stops_with_owner")
fn stops_with_owner(
  start: fn() -> Result(memory.Memory(Nil), store.StoreError),
) -> Bool

pub fn store_stops_with_owner_test() {
  assert stops_with_owner(memory.start)
}

pub fn passes_the_shared_stream_and_append_contract_test() {
  let assert Ok(pid) = memory.start()
  store_contract.check_atomic_streams(memory.as_store(pid))
  let backend = memory.projection_backend(pid)
  store_contract.check_read_only_participant(memory.as_store(pid))
  store_contract.check_snapshots(memory.snapshots(pid))
  store_contract.check_replay(memory.as_store(pid), backend.reader)
  store_contract.check_checkpoints(backend.checkpoints)
  store_contract.check_coordination(backend.acquire, backend.acquire)
  assert memory.close(pid) == Ok(Nil)
}
