//// Reusable assertions for fresh EventCore adapter fixtures.
//// Each assertion group uses reserved `contract/` streams or names.

import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/store.{type Store}
import eventcore/stream
import gleam/list
import gleam/option.{None, Some}

/// Checks stream isolation, optimistic conflicts, atomic writes, and versioned reads.
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

/// Checks atomic rejection when a registered read-only participant changed.
pub fn check_read_only_participant(storage: Store(Int)) -> Nil {
  let assert Ok(reader) = stream.id("contract/read-only")
  let assert Ok(writer) = stream.id("contract/writer")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let observed = store.Event(reader, "observed", 1)
  let written = store.Event(writer, "written", 2)
  let assert Ok(batch) = store.register(store.writes(), reader, zero)
  let assert Ok(batch) = store.append(batch, observed)
  assert storage.append_events(batch) == Ok(Nil)
  let assert Ok(batch) = store.register(store.writes(), reader, zero)
  let assert Ok(batch) = store.register(batch, writer, zero)
  let assert Ok(batch) = store.append(batch, written)
  assert storage.append_events(batch)
    == Error(store.VersionConflict(reader, zero, one))
  let assert Ok(events) = store.read_stream(storage, writer)
  assert store.collect(events) == Ok([])
  let assert Ok(batch) = store.register(store.writes(), reader, one)
  let assert Ok(batch) = store.register(batch, writer, zero)
  let assert Ok(batch) = store.append(batch, written)
  assert storage.append_events(batch) == Ok(Nil)
  let assert Ok(events) = store.read_stream(storage, reader)
  assert store.collect(events) == Ok([observed])
  let assert Ok(events) = store.read_stream(storage, writer)
  assert store.collect(events) == Ok([written])
}

/// Checks missing, exact round trip, and monotonic snapshot replacement.
pub fn check_snapshots(repository: snapshot.Repository) -> Nil {
  let assert Ok(key) = snapshot.key("contract/snapshot")
  let assert Ok(id) = stream.id("contract/snapshot-stream")
  let one = stream.next_version(stream.initial_version)
  let two = stream.next_version(one)
  let first = snapshot.Snapshot([snapshot.Checkpoint(id, one, <<1:2>>)])
  let second = snapshot.Snapshot([snapshot.Checkpoint(id, two, <<2:2>>)])
  assert repository.load(key) == Ok(None)
  assert repository.save(key, None, first) == Ok(Nil)
  assert repository.load(key) == Ok(Some(first))
  assert repository.save(key, Some(first), second) == Ok(Nil)
  assert repository.save(key, Some(first), first) == Ok(Nil)
  assert repository.load(key) == Ok(Some(second))
}

/// Checks cross-stream append order, exclusive resumption, and filtering.
/// The reader and store must refer to the same event history.
pub fn check_replay(
  storage: Store(Int),
  reader: projection.Reader(Int),
) -> Nil {
  let assert Ok(one) = stream.id("contract/replay/item-a")
  let assert Ok(two) = stream.id("contract/replay/item-b")
  let assert Ok(other) = stream.id("contract/replay-sibling")
  let first = store.Event(one, "selected", 10)
  let second = store.Event(two, "selected", 20)
  let third = store.Event(one, "unselected-kind", 30)
  let fourth = store.Event(other, "selected", 40)
  let zero = stream.initial_version
  let assert Ok(batch) = store.register(store.writes(), one, zero)
  let assert Ok(batch) = store.register(batch, two, zero)
  let assert Ok(batch) = store.register(batch, other, zero)
  let assert Ok(batch) = store.append(batch, second)
  let assert Ok(batch) = store.append(batch, first)
  let assert Ok(batch) = store.append(batch, third)
  let assert Ok(batch) = store.append(batch, fourth)
  assert storage.append_events(batch) == Ok(Nil)
  let assert Ok(prefix) = projection.prefix("contract/replay/")
  let assert Ok(page) = projection.page(None, 1)
  let assert Ok(projection.PageResult(
    [projection.Delivery(event, cursor)],
    through,
  )) = reader(prefix, page)
  assert event == second
  assert through == Some(cursor)
  let assert Ok(page) = projection.page(Some(cursor), 10)
  let assert Ok(result) = reader(prefix, page)
  assert list.map(result.deliveries, fn(delivery) { delivery.event })
    == [first, third]

  let assert Ok(page) = projection.page(None, 10)
  let assert Ok(pattern) = projection.pattern("contract/replay/item-?")
  let assert Ok(result) = reader(pattern, page)
  assert list.map(result.deliveries, fn(delivery) { delivery.event })
    == [second, first, third]
  let assert Ok(pattern) = projection.pattern("contract/replay/item-[a]")
  let assert Ok(result) =
    reader(projection.with_event_type(pattern, "selected"), page)
  assert list.map(result.deliveries, fn(delivery) { delivery.event }) == [first]
}

/// Checks absent, overwrite, and independence across named checkpoints.
pub fn check_checkpoints(checkpoints: projection.Checkpoints) -> Nil {
  let first = "contract/checkpoint-first"
  let second = "contract/checkpoint-second"
  let assert Ok(one) = projection.position(1)
  let assert Ok(two) = projection.position(2)
  assert checkpoints.load(first) == Ok(None)
  assert checkpoints.save(first, one) == Ok(Nil)
  assert checkpoints.load(first) == Ok(Some(one))
  assert checkpoints.load(second) == Ok(None)
  assert checkpoints.save(second, one) == Ok(Nil)
  assert checkpoints.save(first, two) == Ok(Nil)
  assert checkpoints.load(first) == Ok(Some(two))
  assert checkpoints.load(second) == Ok(Some(one))
}

/// Checks same-name contention, independent names, and explicit release.
/// Supply independent coordinators targeting the same backend where supported.
pub fn check_coordination(
  first: projection_runner.Coordinator,
  second: projection_runner.Coordinator,
) -> Nil {
  let name = "contract/leader"
  let assert Ok(lease) = first(name)
  assert second(name) == Error(projection_runner.AlreadyLeading)
  let assert Ok(other) = second("contract/other-leader")
  other.release()
  lease.release()
  let assert Ok(next) = second(name)
  next.release()
}

/// Checks inclusive bounds and exclusive resume across two consecutive selected
/// envelopes. The fixture must append both envelopes before calling this check.
pub fn check_delivery_boundary(
  source: delivery.Source(error),
  selection: delivery.Selection,
  before: projection.Position,
  boundary: projection.Position,
  earlier_event_id: String,
  boundary_kind: delivery.EventType,
  boundary_payload: BitArray,
  following_payload: BitArray,
) -> Nil {
  let assert Ok(page) = projection.page(Some(before), 1)
  let assert Ok([at_boundary]) =
    source.read(selection, page, delivery.Inclusive(boundary))
  assert at_boundary.source == source.id
  assert at_boundary.position == boundary
  assert at_boundary.event_type == boundary_kind
  assert at_boundary.event_id != earlier_event_id
  assert at_boundary.payload == boundary_payload

  let assert Ok(page) = projection.page(Some(boundary), 1)
  assert source.read(selection, page, delivery.Inclusive(boundary)) == Ok([])
  let assert Ok([after]) = source.read(selection, page, delivery.Unbounded)
  assert after.source == source.id
  assert after.event_id != at_boundary.event_id
  assert after.payload == following_payload
  assert projection.sequence(after.position)
    == projection.sequence(boundary) + 1
}
