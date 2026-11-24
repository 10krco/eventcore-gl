//// Store-independent projection reads and checkpoint positions.

import eventcore/store.{type Event, type StoreError}
import eventcore/stream.{type Id}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

pub opaque type Position {
  Position(Int)
}

pub fn position(sequence: Int) -> Result(Position, Nil) {
  case sequence > 0 {
    True -> Ok(Position(sequence))
    False -> Error(Nil)
  }
}

/// For adapters that persist the position. A reader interprets its own
/// positions within a single store's delivery sequence.
pub fn sequence(position: Position) -> Int {
  let Position(value) = position
  value
}

pub opaque type Page {
  Page(after: Option(Position), limit: Int)
}

pub fn page(after: Option(Position), limit: Int) -> Result(Page, Nil) {
  case limit > 0 {
    True -> Ok(Page(after, limit))
    False -> Error(Nil)
  }
}

pub fn page_after(page: Page) -> Option(Position) {
  page.after
}

pub fn page_limit(page: Page) -> Int {
  page.limit
}

pub type Delivery(payload) {
  Delivery(event: Event(payload), position: Position)
}

pub type Reader(payload) =
  fn(Filter, Page) -> Result(List(Delivery(payload)), StoreError)

pub type Checkpoints {
  Checkpoints(
    load: fn(String) -> Result(Option(Position), StoreError),
    save: fn(String, Position) -> Result(Nil, StoreError),
  )
}

pub opaque type Filter {
  Filter(stream: StreamFilter, kind: Option(String))
}

type StreamFilter {
  All
  Prefix(String)
  Pattern(Glob)
}

type Glob

pub fn all() -> Filter {
  Filter(All, None)
}

pub fn prefix(value: String) -> Result(Filter, stream.IdError) {
  stream.id(value)
  |> result.map(fn(id) { Filter(Prefix(stream.name(id)), None) })
}

pub fn pattern(value: String) -> Result(Filter, Nil) {
  let trimmed = string.trim(value)
  let length = list.length(string.to_utf_codepoints(trimmed))
  case length > 0 && length <= 255 {
    True ->
      compile_glob(trimmed)
      |> result.map(fn(glob) { Filter(Pattern(glob), None) })
    False -> Error(Nil)
  }
}

pub fn with_event_type(filter: Filter, kind: String) -> Filter {
  Filter(..filter, kind: Some(kind))
}

pub fn matches(filter: Filter, stream: Id, kind: String) -> Bool {
  case filter.kind {
    Some(expected) if expected != kind -> False
    _ ->
      case filter.stream {
        All -> True
        Prefix(prefix) -> string.starts_with(stream.name(stream), prefix)
        Pattern(pattern) -> matches_glob(pattern, stream.name(stream))
      }
  }
}

@external(erlang, "eventcore_projection_ffi", "compile_glob")
fn compile_glob(pattern: String) -> Result(Glob, Nil)

@external(erlang, "eventcore_projection_ffi", "matches_glob")
fn matches_glob(pattern: Glob, stream: String) -> Bool
