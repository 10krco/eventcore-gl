//// Durable command-state checkpoints and their storage boundary.

import eventcore/store.{type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

pub opaque type Key {
  Key(String)
}

pub fn key(value: String) -> Result(Key, Nil) {
  let value = string.trim(value)
  case value != "" && list.length(string.to_utf_codepoints(value)) <= 255 {
    True -> Ok(Key(value))
    False -> Error(Nil)
  }
}

pub fn name(key: Key) -> String {
  let Key(value) = key
  value
}

pub type Checkpoint {
  Checkpoint(stream: Id, version: Version, state: BitArray)
}

pub type Snapshot {
  Snapshot(checkpoints: List(Checkpoint))
}

/// Save atomically. Removing participants requires the exact snapshot that
/// was loaded for replay. Shared stream versions must always advance or match.
pub type Repository {
  Repository(
    load: fn(Key) -> Result(Option(Snapshot), StoreError),
    save: fn(Key, Option(Snapshot), Snapshot) -> Result(Nil, StoreError),
  )
}

pub type Codec(state) {
  Codec(
    encode: fn(state) -> Result(BitArray, String),
    decode: fn(BitArray) -> Result(state, String),
  )
}

/// Decide under the repository's write lock whether to replace its value.
pub fn can_replace(
  current: Option(Snapshot),
  observed: Option(Snapshot),
  candidate: Snapshot,
) -> Bool {
  case current {
    None -> True
    Some(earlier) -> {
      let versions =
        list.fold(candidate.checkpoints, dict.new(), fn(versions, checkpoint) {
          dict.insert(versions, checkpoint.stream, checkpoint.version)
        })
      let may_drop = observed == current
      list.all(earlier.checkpoints, fn(checkpoint) {
        case dict.get(versions, checkpoint.stream) {
          Ok(version) ->
            stream.event_count(version)
            >= stream.event_count(checkpoint.version)
          Error(Nil) -> may_drop
        }
      })
    }
  }
}
