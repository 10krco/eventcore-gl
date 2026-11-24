import eventcore/memory
import eventcore/projection
import eventcore/store
import eventcore/stream
import gleam/option.{None, Some}
import gleam/result

pub fn glob_and_literal_prefix_test() {
  let assert Ok(id) = stream.id("ledger/éu/account-7")
  let assert Ok(pattern) = projection.pattern(" ledger/*/account-[0-9] ")
  assert projection.matches(pattern, id, "opened")
  let assert Ok(recursive) = projection.pattern("ledger/**/account-?")
  assert projection.matches(recursive, id, "opened")
  let assert Ok(flat) = stream.id("ledger/account-7")
  assert projection.matches(recursive, flat, "opened")
  let assert Ok(negated) = projection.pattern("ledger/**/account-[!7]")
  assert !projection.matches(negated, id, "opened")
  let assert Ok(prefix) = projection.prefix(" ledger/é ")
  assert projection.matches(prefix, id, "opened")
  let assert Error(stream.ReservedGlobCharacter) = projection.prefix("ledger/*")
  let assert Error(Nil) = projection.pattern("ledger/[0-9")
  let assert Error(Nil) = projection.pattern("ledger/**suffix")
  let assert Error(Nil) = projection.pattern("ledger/***")
}

pub fn memory_filtered_read_resumes_in_append_order_test() {
  let assert Ok(memory) = memory.start()
  let events = memory.as_store(memory)
  let assert Ok(debit) = stream.id("ledger/eu/debit")
  let assert Ok(credit) = stream.id("ledger/credit")
  let assert Ok(unrelated) = stream.id("other")
  let debit_event = store.Event(debit, "applied", 10)
  let credit_event = store.Event(credit, "applied", 20)
  let assert Ok(writes) =
    store.writes()
    |> store.register(debit, stream.initial_version)
    |> result.try(store.register(_, credit, stream.initial_version))
    |> result.try(store.register(_, unrelated, stream.initial_version))
    |> result.try(store.append(_, store.Event(unrelated, "applied", 0)))
    |> result.try(store.append(_, debit_event))
    |> result.try(store.append(_, store.Event(debit, "skipped", 11)))
    |> result.try(store.append(_, credit_event))
    |> result.try(store.append(_, store.Event(credit, "skipped", 21)))
  assert events.append_events(writes) == Ok(Nil)

  let read = memory.projection_reader(memory)
  let checkpoints = memory.projection_checkpoints(memory)
  let assert Ok(pattern) = projection.pattern("ledger/*")
  let filter = projection.with_event_type(pattern, "applied")
  let assert Ok(first_page) = projection.page(None, 1)
  let assert Ok(projection.PageResult(
    [projection.Delivery(first, cursor)],
    Some(scanned),
  )) = read(filter, first_page)
  assert scanned == cursor
  assert first == debit_event
  assert projection.sequence(cursor) == 2
  assert checkpoints.load("balances") == Ok(None)
  assert checkpoints.save("balances", cursor) == Ok(Nil)
  let assert Ok(saved) = checkpoints.load("balances")
  assert saved == Some(cursor)
  let assert Ok(resume) = projection.page(saved, 1)
  let assert Ok(projection.PageResult(
    [projection.Delivery(second, cursor)],
    Some(scanned),
  )) = read(filter, resume)
  assert scanned == cursor
  assert second == credit_event
  assert projection.sequence(cursor) == 4
  let assert Ok(drained) = projection.page(Some(cursor), 1)
  let assert Ok(frontier) = projection.position(5)
  assert read(filter, drained) == Ok(projection.PageResult([], Some(frontier)))
  let assert Ok(first_with_room) = projection.page(None, 3)
  let assert Ok(projection.PageResult([debit, credit], Some(scanned))) =
    read(filter, first_with_room)
  assert debit.event == debit_event
  assert credit.event == credit_event
  assert scanned == frontier
  assert memory.close(memory) == Ok(Nil)
}

pub fn empty_page_advances_to_scanned_frontier_test() {
  let assert Ok(memory) = memory.start()
  let events = memory.as_store(memory)
  let assert Ok(id) = stream.id("notifications")
  let assert Ok(writes) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(writes) = emit_unrelated(writes, id, 260)
  assert events.append_events(writes) == Ok(Nil)

  let read = memory.projection_reader(memory)
  let filter = projection.with_event_type(projection.all(), "subscribed")
  let assert Ok(first) = projection.page(None, 1)
  let assert Ok(projection.PageResult([], Some(scanned))) = read(filter, first)
  assert projection.sequence(scanned) == 260

  let assert Ok(version) = stream.version(260)
  let matching = store.Event(id, "subscribed", 261)
  let assert Ok(writes) =
    store.writes()
    |> store.register(id, version)
    |> result.try(store.append(_, matching))
  assert events.append_events(writes) == Ok(Nil)
  let assert Ok(next) = projection.page(Some(scanned), 1)
  let assert Ok(projection.PageResult(
    [projection.Delivery(event, position)],
    Some(scanned),
  )) = read(filter, next)
  assert event == matching
  assert position == scanned
  assert projection.sequence(position) == 261
  assert memory.close(memory) == Ok(Nil)
}

fn emit_unrelated(writes: store.Writes(Int), id: stream.Id, remaining: Int) {
  case remaining {
    0 -> Ok(writes)
    _ ->
      store.append(writes, store.Event(id, "unrelated", remaining))
      |> result.try(fn(writes) { emit_unrelated(writes, id, remaining - 1) })
  }
}
