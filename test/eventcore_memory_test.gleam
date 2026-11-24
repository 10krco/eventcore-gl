import eventcore/memory
import eventcore/store
import eventcore/stream

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
