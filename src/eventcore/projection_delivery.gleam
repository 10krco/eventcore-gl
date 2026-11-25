//// Strict delivery of persisted events at committed, source-local positions.

import eventcore/projection
import eventcore/stream
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/set
import gleam/string

pub opaque type SourceId {
  SourceId(String)
}

pub opaque type SelectionId {
  SelectionId(String)
}

pub opaque type EventType {
  EventType(String)
}

pub opaque type ProjectorName {
  ProjectorName(String)
}

pub fn source_id(value: String) -> Result(SourceId, Nil) {
  nonblank(value) |> result.map(SourceId)
}

pub fn source_name(id: SourceId) -> String {
  let SourceId(value) = id
  value
}

pub fn selection_id(value: String) -> Result(SelectionId, Nil) {
  nonblank(value) |> result.map(SelectionId)
}

pub fn selection_name(id: SelectionId) -> String {
  let SelectionId(value) = id
  value
}

pub fn event_type(value: String) -> Result(EventType, Nil) {
  nonblank(value) |> result.map(EventType)
}

pub fn event_type_name(kind: EventType) -> String {
  let EventType(value) = kind
  value
}

pub fn projector_name(value: String) -> Result(ProjectorName, Nil) {
  nonblank(value) |> result.map(ProjectorName)
}

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

pub type SelectionError {
  EmptyEventTypes
  DuplicateEventType(EventType)
}

pub opaque type Selection {
  Selection(
    id: SelectionId,
    streams: projection.Filter,
    event_types: List(EventType),
  )
}

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

pub fn selection_identity(selection: Selection) -> SelectionId {
  selection.id
}

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

pub type Source(error) {
  Source(
    id: SourceId,
    high_watermark: fn() -> Result(Option(projection.Position), error),
    read: fn(Selection, projection.Page, UpperBound) ->
      Result(List(Envelope), error),
  )
}
