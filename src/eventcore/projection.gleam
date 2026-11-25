//// Store-independent projection reads and checkpoint positions.

import eventcore/store.{type Event, type StoreError}
import eventcore/stream.{type Id}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

/// A cursor in one store's delivery sequence. Keep it with the reader that
/// produced it.
pub opaque type Position {
  Position(Int)
}

/// Constructs a cursor from a positive stored sequence number.
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

/// A bounded read request starting strictly after an optional cursor.
pub opaque type Page {
  Page(after: Option(Position), limit: Int)
}

/// A zero limit reads no events and preserves the input cursor.
pub fn page(after: Option(Position), limit: Int) -> Result(Page, Nil) {
  case limit >= 0 {
    True -> Ok(Page(after, limit))
    False -> Error(Nil)
  }
}

/// Returns the exclusive starting cursor, if one was supplied.
pub fn page_after(page: Page) -> Option(Position) {
  page.after
}

/// Returns the requested maximum number of deliveries.
pub fn page_limit(page: Page) -> Int {
  page.limit
}

/// A stored event paired with its position in the reader's delivery order.
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

/// A fenced checkpoint failed because leadership was lost or storage rejected
/// the write.
pub type CheckpointWriteError {
  LeaseLost
  WriteFailed(StoreError)
}

/// Loads and saves cursors by projector name. Fenced stores also validate
/// leadership at write time.
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

/// Stream and event-kind criteria used to select projection deliveries.
pub opaque type Filter {
  Filter(stream: StreamFilter, kind: Option(String), release: Bool)
}

type StreamFilter {
  All
  Prefix(String)
  Pattern(source: String, compiled: Glob)
}

type Glob

/// Matches every stream and event kind.
pub fn all() -> Filter {
  Filter(All, None, False)
}

/// Matches stream names that start with a validated stream ID prefix.
pub fn prefix(value: String) -> Result(Filter, stream.IdError) {
  stream.id(value)
  |> result.map(fn(id) { Filter(Prefix(stream.name(id)), None, False) })
}

/// Matches stream names against a compiled glob. Rejects empty, oversized, or
/// invalid patterns.
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

/// Adds or replaces the exact persisted event-kind constraint.
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

/// Reports whether a stream and persisted event kind satisfy the filter.
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
