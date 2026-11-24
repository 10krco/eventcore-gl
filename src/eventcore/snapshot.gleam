//// Durable command-state checkpoints and their storage boundary.

import eventcore/store.{type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option}
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

/// Save atomically if the incoming snapshot covers every stored stream version.
pub type Repository {
  Repository(
    load: fn(Key) -> Result(Option(Snapshot), StoreError),
    save: fn(Key, Snapshot) -> Result(Nil, StoreError),
  )
}

pub type Codec(state) {
  Codec(
    encode: fn(state) -> Result(BitArray, String),
    decode: fn(BitArray) -> Result(state, String),
  )
}

/// True when every stream in the earlier snapshot is present at an equal or
/// higher version in the candidate.
pub fn covers(candidate: Snapshot, earlier: Snapshot) -> Bool {
  let versions =
    list.fold(candidate.checkpoints, dict.new(), fn(versions, checkpoint) {
      dict.insert(
        versions,
        checkpoint.stream,
        stream.event_count(checkpoint.version),
      )
    })
  list.all(earlier.checkpoints, fn(checkpoint) {
    case dict.get(versions, checkpoint.stream) {
      Ok(count) -> count >= stream.event_count(checkpoint.version)
      Error(Nil) -> False
    }
  })
}
