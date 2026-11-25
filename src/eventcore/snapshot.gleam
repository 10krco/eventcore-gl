//// Durable command-state checkpoints and their storage boundary.
////
//// A command snapshot stores replay state for participating streams.

import eventcore/store.{type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

/// A validated repository key for one logical command-state snapshot.
pub opaque type Key {
  Key(String)
}

/// Trim and validate a snapshot name. The result must contain between one
/// and 255 Unicode code points. Choose a stable name for the state it tracks.
pub fn key(value: String) -> Result(Key, Nil) {
  let value = string.trim(value)
  case value != "" && list.length(string.to_utf_codepoints(value)) <= 255 {
    True -> Ok(Key(value))
    False -> Error(Nil)
  }
}

/// Return the normalized snapshot name used by the repository.
pub fn name(key: Key) -> String {
  let Key(value) = key
  value
}

/// Encoded accumulated command state after replaying the named stream to
/// the given version. This is command replay state, distinct from a
/// projection cursor.
pub type Checkpoint {
  Checkpoint(stream: Id, version: Version, state: BitArray)
}

/// The accumulated-state checkpoints in the order the command runtime
/// visited the participating streams. Retain that order when storing or
/// restoring a snapshot.
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

/// Serialize and restore command state at stream boundaries. If an
/// application changes its state encoding, keep a decoder for saved
/// snapshots or allow the command runtime to rebuild from events.
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
