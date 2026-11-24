//// Validated identifiers and optimistic versions for event streams.

import gleam/list
import gleam/string

pub opaque type Id {
  Id(String)
}

pub type IdError {
  EmptyId
  IdTooLong
  ReservedGlobCharacter
}

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

pub fn name(id: Id) -> String {
  let Id(value) = id
  value
}

pub opaque type Version {
  Version(Int)
}

pub fn version(count: Int) -> Result(Version, Nil) {
  case count >= 0 {
    True -> Ok(Version(count))
    False -> Error(Nil)
  }
}

pub const initial_version = Version(0)

pub fn event_count(version: Version) -> Int {
  let Version(count) = version
  count
}

pub fn next_version(version: Version) -> Version {
  Version(event_count(version) + 1)
}
