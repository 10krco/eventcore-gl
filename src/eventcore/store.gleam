//// Optimistic event-store boundary shared by command execution and adapters.

import eventcore/stream.{type Id, type Version}
import gleam/dict.{type Dict}
import gleam/list
import gleam/result

pub type Event(payload) {
  Event(stream: Id, kind: String, payload: payload)
}

pub type Operation {
  StartStore
  ReadStream
  AppendEvents
  CloseStore
  LoadSnapshot
  SaveSnapshot
}

pub type StoreError {
  ConflictingExpectedVersions(stream: Id, first: Version, second: Version)
  UndeclaredStream(stream: Id)
  SerializationFailed(stream: Id, detail: String)
  DeserializationFailed(stream: Id, detail: String)
  StoreFailure(operation: Operation)
  VersionConflict(stream: Id, expected: Version, actual: Version)
  UnresolvedStreamFork(stream: Id)
  JournalHistoryChanged
}

pub opaque type Writes(payload) {
  Writes(expected: Dict(Id, Version), reversed_events: List(Event(payload)))
}

pub fn writes() -> Writes(payload) {
  Writes(dict.new(), [])
}

pub fn register(
  writes: Writes(payload),
  stream: Id,
  expected: Version,
) -> Result(Writes(payload), StoreError) {
  case dict.get(writes.expected, stream) {
    Error(Nil) ->
      Ok(
        Writes(
          ..writes,
          expected: dict.insert(writes.expected, stream, expected),
        ),
      )
    Ok(first) if first == expected -> Ok(writes)
    Ok(first) -> Error(ConflictingExpectedVersions(stream, first, expected))
  }
}

pub fn append(
  writes: Writes(payload),
  event: Event(payload),
) -> Result(Writes(payload), StoreError) {
  case dict.has_key(writes.expected, event.stream) {
    True ->
      Ok(Writes(..writes, reversed_events: [event, ..writes.reversed_events]))
    False -> Error(UndeclaredStream(event.stream))
  }
}

pub fn expected_versions(writes: Writes(payload)) -> Dict(Id, Version) {
  writes.expected
}

pub fn entries(writes: Writes(payload)) -> List(Event(payload)) {
  list.reverse(writes.reversed_events)
}

/// An incremental, pull-based read. A fault can occur between events.
pub type EventStream(payload) {
  EventStream(fn() -> Result(Step(payload), StoreError))
}

pub type Step(payload) {
  End
  Next(Event(payload), EventStream(payload))
}

pub fn from_items(
  items: List(Result(Event(payload), StoreError)),
) -> EventStream(payload) {
  EventStream(fn() {
    case items {
      [] -> Ok(End)
      [item, ..rest] ->
        result.map(item, fn(event) { Next(event, from_items(rest)) })
    }
  })
}

pub fn fold(
  events: EventStream(payload),
  initial: state,
  apply: fn(state, Event(payload)) -> state,
) -> Result(state, StoreError) {
  let EventStream(pull) = events
  case pull() {
    Ok(End) -> Ok(initial)
    Ok(Next(event, rest)) -> fold(rest, apply(initial, event), apply)
    Error(error) -> Error(error)
  }
}

pub fn collect(
  events: EventStream(payload),
) -> Result(List(Event(payload)), StoreError) {
  fold(events, [], fn(events, event) { [event, ..events] })
  |> result.map(list.reverse)
}

/// Adapters read from an exclusive version and compare all registered versions
/// in one atomic append, including streams with no events in the batch.
pub type Store(payload) {
  Store(
    read_after: fn(Id, Version) -> Result(EventStream(payload), StoreError),
    append_events: fn(Writes(payload)) -> Result(Nil, StoreError),
  )
}

pub fn read_stream(
  store: Store(payload),
  id: Id,
) -> Result(EventStream(payload), StoreError) {
  store.read_after(id, stream.initial_version)
}
