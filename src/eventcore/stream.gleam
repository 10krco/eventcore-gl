//// Validated identifiers and optimistic versions for event streams.

import gleam/list
import gleam/string

/// A validated identifier for one ordered stream of events.
pub opaque type Id {
  Id(String)
}

/// Why a stream identifier failed validation.
pub type IdError {
  /// The identifier is empty after trimming whitespace.
  EmptyId
  /// The trimmed identifier has more than 255 Unicode code points.
  IdTooLong
  /// The identifier contains `*`, `?`, `[`, or `]`.
  ReservedGlobCharacter
}

/// Trim surrounding whitespace and validate the remaining identifier.
pub fn id(value: String) -> Result(Id, IdError) {
  let trimmed = string.trim(value)
  case trimmed {
    "" -> Error(EmptyId)
    _ -> {
      case list.length(string.to_utf_codepoints(trimmed)) > 255 {
        True -> Error(IdTooLong)
        False ->
          case list.any(["*", "?", "[", "]"], string.contains(trimmed, _)) {
            True -> Error(ReservedGlobCharacter)
            False -> Ok(Id(trimmed))
          }
      }
    }
  }
}

/// Return the validated, trimmed name.
pub fn name(id: Id) -> String {
  let Id(value) = id
  value
}

/// The number of events in a stream at a given point.
pub opaque type Version {
  Version(Int)
}

/// Construct a version from a nonnegative event count.
pub fn version(count: Int) -> Result(Version, Nil) {
  case count >= 0 {
    True -> Ok(Version(count))
    False -> Error(Nil)
  }
}

/// The version of a stream before its first event.
pub const initial_version = Version(0)

/// Return the number of events represented by the version.
pub fn event_count(version: Version) -> Int {
  let Version(count) = version
  count
}

/// Advance the version by one event.
pub fn next_version(version: Version) -> Version {
  Version(event_count(version) + 1)
}
