//// Strict delivery of persisted events at committed, source-local positions.

import eventcore/projection
import eventcore/stream
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/set
import gleam/string

/// Persistent application-chosen identity of an event source. Give distinct
/// feeds distinct IDs.
pub opaque type SourceId {
  SourceId(String)
}

/// Persistent application-chosen identity of a projection's selection. Change
/// it when the selection's meaning changes.
pub opaque type SelectionId {
  SelectionId(String)
}

/// An event kind as used to select or identify a persisted event.
pub opaque type EventType {
  EventType(String)
}

/// Name used to coordinate a projector and look up its progress.
pub opaque type ProjectorName {
  ProjectorName(String)
}

/// Creates a source ID by trimming surrounding whitespace and rejecting a
/// blank value.
pub fn source_id(value: String) -> Result(SourceId, Nil) {
  nonblank(value) |> result.map(SourceId)
}

/// Returns the validated source ID as text.
pub fn source_name(id: SourceId) -> String {
  let SourceId(value) = id
  value
}

/// Creates a selection ID by trimming surrounding whitespace and rejecting a
/// blank value.
pub fn selection_id(value: String) -> Result(SelectionId, Nil) {
  nonblank(value) |> result.map(SelectionId)
}

/// Returns the validated selection ID as text.
pub fn selection_name(id: SelectionId) -> String {
  let SelectionId(value) = id
  value
}

/// Creates a selection event kind by trimming surrounding whitespace and
/// rejecting a blank value.
pub fn event_type(value: String) -> Result(EventType, Nil) {
  nonblank(value) |> result.map(EventType)
}

/// Select an event kind exactly as it was persisted, including whitespace.
pub fn persisted_event_type(value: String) -> Result(EventType, Nil) {
  nonblank(value) |> result.map(fn(_) { EventType(value) })
}

/// Returns the persisted event-kind spellings in the order supplied to the
/// selection.
pub fn selection_event_types(selection: Selection) -> List(String) {
  list.map(selection.event_types, event_type_name)
}

/// Returns the event kind as text, preserving the spelling of a persisted
/// kind.
pub fn event_type_name(kind: EventType) -> String {
  let EventType(value) = kind
  value
}

/// Creates a projector name by trimming surrounding whitespace and rejecting a
/// blank value.
pub fn projector_name(value: String) -> Result(ProjectorName, Nil) {
  nonblank(value) |> result.map(ProjectorName)
}

/// Returns the validated projector name as text.
pub fn projector_name_value(name: ProjectorName) -> String {
  let ProjectorName(value) = name
  value
}

fn nonblank(value: String) -> Result(String, Nil) {
  case string.trim(value) {
    "" -> Error(Nil)
    trimmed -> Ok(trimmed)
  }
}

/// The supplied event-kind list was empty or contained a duplicate.
pub type SelectionError {
  EmptyEventTypes
  DuplicateEventType(EventType)
}

/// A stable identity, stream filter, and explicit set of allowed event kinds.
pub opaque type Selection {
  Selection(
    id: SelectionId,
    streams: projection.Filter,
    event_types: List(EventType),
  )
}

/// Builds a selection. At least one event kind is required and every kind must
/// be unique.
pub fn selection(
  id: SelectionId,
  streams: projection.Filter,
  event_types: List(EventType),
) -> Result(Selection, SelectionError) {
  case event_types {
    [] -> Error(EmptyEventTypes)
    [_, ..] -> {
      use _ <- result.try(
        list.try_fold(event_types, set.new(), fn(seen, kind) {
          case set.contains(seen, kind) {
            True -> Error(DuplicateEventType(kind))
            False -> Ok(set.insert(seen, kind))
          }
        }),
      )
      Ok(Selection(id, streams, event_types))
    }
  }
}

/// Returns the identity checked against persisted projection progress.
pub fn selection_identity(selection: Selection) -> SelectionId {
  selection.id
}

/// Checks the stream filter and exact event-kind allowlist against a persisted
/// event.
pub fn selects(
  selection: Selection,
  stream: stream.Id,
  persisted_kind: String,
) -> Bool {
  list.any(selection.event_types, fn(kind) {
    event_type_name(kind) == persisted_kind
  })
  && projection.matches(selection.streams, stream, persisted_kind)
}

/// Optional inclusive ending position for a read from a source.
pub type UpperBound {
  Unbounded
  Inclusive(projection.Position)
}

/// Holds the original encoded payload and metadata. Neither field is
/// reserialized during delivery.
pub type Envelope {
  Envelope(
    source: SourceId,
    position: projection.Position,
    event_id: String,
    stream: stream.Id,
    version: stream.Version,
    event_type: EventType,
    payload: BitArray,
    metadata: String,
  )
}

/// A committed event feed with a stable identity. Return matching deliveries
/// in strictly increasing order after the page cursor, within the page size
/// and upper bound.
pub type Source(error) {
  Source(
    id: SourceId,
    high_watermark: fn() -> Result(Option(projection.Position), error),
    read: fn(Selection, projection.Page, UpperBound) ->
      Result(List(Envelope), error),
  )
}
