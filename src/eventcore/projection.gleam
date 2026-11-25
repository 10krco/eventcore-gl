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

/// The cursor covers deliveries and non-matching events examined by this
/// page. Persist it only after processing every delivery in the page.
pub type PageResult(payload) {
  PageResult(
    deliveries: List(Delivery(payload)),
    scanned_through: Option(Position),
  )
}

/// Returns deliveries in increasing order, strictly after the input cursor.
/// The scanned cursor includes the final delivery and may include a tail of
/// filtered events. An empty scan preserves the input cursor.
pub type Reader(payload) =
  fn(Filter, Page) -> Result(PageResult(payload), StoreError)

/// The filesystem adapter owns this capability and validates it at commit.
pub type CheckpointFence

pub type CheckpointWriteError {
  LeaseLost
  WriteFailed(StoreError)
}

pub type Checkpoints {
  Checkpoints(
    load: fn(String) -> Result(Option(Position), StoreError),
    save: fn(String, Position) -> Result(Nil, StoreError),
  )
  FencedCheckpoints(
    load: fn(String) -> Result(Option(Position), StoreError),
    save: fn(String, Position) -> Result(Nil, StoreError),
    save_fenced: fn(String, Position, CheckpointFence) ->
      Result(Nil, CheckpointWriteError),
  )
}

pub opaque type Filter {
  Filter(stream: StreamFilter, kind: Option(String), release: Bool)
}

type StreamFilter {
  All
  Prefix(String)
  Pattern(source: String, compiled: Glob)
}

type Glob

pub fn all() -> Filter {
  Filter(All, None, False)
}

pub fn prefix(value: String) -> Result(Filter, stream.IdError) {
  stream.id(value)
  |> result.map(fn(id) { Filter(Prefix(stream.name(id)), None, False) })
}

pub fn pattern(value: String) -> Result(Filter, Nil) {
  let trimmed = string.trim(value)
  let length = list.length(string.to_utf_codepoints(trimmed))
  case length > 0 && length <= 255 {
    True ->
      compile_glob(trimmed)
      |> result.map(fn(glob) { Filter(Pattern(trimmed, glob), None, False) })
    False -> Error(Nil)
  }
}

pub fn with_event_type(filter: Filter, kind: String) -> Filter {
  Filter(..filter, kind: Some(kind))
}

/// Select EventCore 2.1.0 legacy pagination. Use the consumer's event kind
/// when the filter does not already name one.
pub fn rust_release_pagination(
  filter: Filter,
  consumer_kind: String,
) -> Filter {
  let kind = case filter.kind {
    None -> Some(consumer_kind)
    Some(_) -> filter.kind
  }
  Filter(..filter, kind: kind, release: True)
}

@internal
pub fn uses_release_pagination(filter: Filter) -> Bool {
  filter.release
}

@internal
pub fn stream_prefix(filter: Filter) -> Option(String) {
  case filter.stream {
    Prefix(prefix) -> Some(prefix)
    _ -> None
  }
}

@internal
pub fn stream_pattern(filter: Filter) -> Option(String) {
  case filter.stream {
    Pattern(source, _) -> Some(source)
    _ -> None
  }
}

@internal
pub fn event_kind(filter: Filter) -> Option(String) {
  filter.kind
}

pub fn matches(filter: Filter, stream: Id, kind: String) -> Bool {
  case filter.kind {
    Some(expected) if expected != kind -> False
    _ ->
      case filter.stream {
        All -> True
        Prefix(prefix) -> string.starts_with(stream.name(stream), prefix)
        Pattern(_, pattern) -> matches_glob(pattern, stream.name(stream))
      }
  }
}

@external(erlang, "eventcore_projection_ffi", "compile_glob")
fn compile_glob(pattern: String) -> Result(Glob, Nil)

@external(erlang, "eventcore_projection_ffi", "matches_glob")
fn matches_glob(pattern: Glob, stream: String) -> Bool
