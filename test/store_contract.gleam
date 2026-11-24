//// Portable stream and append checks for adapters under test.

import eventcore/store.{type Store}
import eventcore/stream

pub fn check_atomic_streams(storage: Store(Int)) -> Nil {
  let assert Ok(debit) = stream.id("contract/debit")
  let assert Ok(credit) = stream.id("contract/credit")
  let assert Ok(missing) = stream.id("contract/missing")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let two = stream.next_version(one)

  let assert Ok(events) = store.read_stream(storage, missing)
  assert store.collect(events) == Ok([])

  let opened = store.Event(debit, "opened", 100)
  let assert Ok(opening) = store.register(store.writes(), debit, zero)
  let assert Ok(opening) = store.append(opening, opened)
  assert storage.append_events(opening) == Ok(Nil)
  let assert Ok(events) = store.read_stream(storage, credit)
  assert store.collect(events) == Ok([])

  let withdrawn = store.Event(debit, "withdrawn", 5)
  let deposited = store.Event(credit, "deposited", 5)
  let assert Ok(writes) = store.register(store.writes(), debit, zero)
  let assert Ok(writes) = store.register(writes, credit, zero)
  let assert Ok(writes) = store.append(writes, deposited)
  let assert Ok(writes) = store.append(writes, withdrawn)
  assert storage.append_events(writes)
    == Error(store.VersionConflict(debit, zero, one))
  let assert Ok(events) = store.read_stream(storage, debit)
  assert store.collect(events) == Ok([opened])
  let assert Ok(events) = store.read_stream(storage, credit)
  assert store.collect(events) == Ok([])

  let assert Ok(writes) = store.register(store.writes(), debit, one)
  let assert Ok(writes) = store.register(writes, credit, zero)
  let assert Ok(writes) = store.append(writes, withdrawn)
  assert storage.append_events(writes) == Ok(Nil)
  assert storage.append_events(writes)
    == Error(store.VersionConflict(debit, one, two))
  let assert Ok(events) = store.read_stream(storage, debit)
  assert store.collect(events) == Ok([opened, withdrawn])
  let assert Ok(events) = storage.read_after(debit, one)
  assert store.collect(events) == Ok([withdrawn])
  let assert Ok(events) = storage.read_after(debit, stream.next_version(two))
  assert store.collect(events) == Ok([])
}
