//// Optimistic event-store boundary shared by command execution and adapters.

import eventcore/stream.{type Id, type Version}
import gleam/dict.{type Dict}
import gleam/list
import gleam/result

/// A domain event associated with one stream. The application chooses the
/// event kind and payload. Durable adapters can use the kind to select a codec.
pub type Event(payload) {
  Event(stream: Id, kind: String, payload: payload)
}

/// The operation identified by a generic store failure.
pub type Operation {
  StartStore
  ReadStream
  ReadProjection
  LoadCheckpoint
  SaveCheckpoint
  AppendEvents
  CloseStore
  LoadSnapshot
  SaveSnapshot
}

/// Why an event or snapshot operation failed.
pub type StoreError {
  /// One write batch registered different versions for the same stream.
  ConflictingExpectedVersions(stream: Id, first: Version, second: Version)
  /// The event belongs to a stream that was never registered in the batch.
  UndeclaredStream(stream: Id)
  /// The adapter could not encode an event payload.
  SerializationFailed(stream: Id, detail: String)
  /// The adapter could not decode a stored event payload.
  DeserializationFailed(stream: Id, detail: String)
  /// The adapter reported a failure for the given operation.
  StoreFailure(operation: Operation)
  /// The optimistic append found a stream version other than the expected one.
  VersionConflict(stream: Id, expected: Version, actual: Version)
  /// A filesystem journal stream has branches that require reconciliation.
  UnresolvedStreamFork(stream: Id)
  /// The filesystem command view became stale after history reconciliation.
  JournalHistoryChanged
}

/// A batch of events and the expected stream versions for an atomic append.
pub opaque type Writes(payload) {
  Writes(expected: Dict(Id, Version), reversed_events: List(Event(payload)))
}

/// Create an empty batch with no stream expectations.
pub fn writes() -> Writes(payload) {
  Writes(dict.new(), [])
}

/// Add a stream version to compare at append time. Register the version even
/// when a command reads the stream without producing any events for it.
/// Registering the same stream and version again leaves the batch unchanged.
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

/// Add an event to the batch. Its stream must already have a registered version.
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

/// Return all registered versions, including read-only participants.
pub fn expected_versions(writes: Writes(payload)) -> Dict(Id, Version) {
  writes.expected
}

/// Return proposed events in the order they were added to the batch.
pub fn entries(writes: Writes(payload)) -> List(Event(payload)) {
  list.reverse(writes.reversed_events)
}

/// An incremental, pull-based read. A fault can occur between events.
pub type EventStream(payload) {
  EventStream(fn() -> Result(Step(payload), StoreError))
}

/// The next item from an incremental stream.
pub type Step(payload) {
  /// The stream has no further events.
  End
  /// The current event and the continuation for the remaining events.
  Next(Event(payload), EventStream(payload))
}

/// Build an incremental stream from a list of event results in order.
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

/// Consume events in order, stopping at the first read error.
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

/// Consume the stream into a list in delivery order, or return a read error.
pub fn collect(
  events: EventStream(payload),
) -> Result(List(Event(payload)), StoreError) {
  fold(events, [], fn(events, event) { [event, ..events] })
  |> result.map(list.reverse)
}

/// Adapters read from an exclusive version. A nonempty append compares all
/// registered versions atomically, including streams that emit no events.
pub type Store(payload) {
  Store(
    read_after: fn(Id, Version) -> Result(EventStream(payload), StoreError),
    append_events: fn(Writes(payload)) -> Result(Nil, StoreError),
  )
}

/// Read from the initial version. Consume the returned stream to obtain events
/// and detect any failures that occur after the read begins.
pub fn read_stream(
  store: Store(payload),
  id: Id,
) -> Result(EventStream(payload), StoreError) {
  store.read_after(id, stream.initial_version)
}
