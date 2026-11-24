//// PostgreSQL event and checkpoint storage for an existing Pog pool.

import eventcore/snapshot
import eventcore/store.{type Event, type Store, type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/erlang/process.{type Name, type Pid}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import pog

pub type Codec(payload) {
  Codec(
    encode: fn(String, payload) -> Result(BitArray, String),
    decode: fn(String, BitArray) -> Result(payload, String),
  )
}

pub opaque type EventStore(payload) {
  EventStore(pool: pog.Connection, codec: Codec(payload))
}

pub opaque type CheckpointStore {
  CheckpointStore(pool: pog.Connection)
}

pub type MigrationError {
  MigrationFailed
}

pub type CheckpointError {
  InvalidPosition
  CheckpointFailed
}

const max_bigint = 9_223_372_036_854_775_807

const checkpoint_schema =
  "CREATE TABLE IF NOT EXISTS eventcore_checkpoints (
  name text PRIMARY KEY, position bigint NOT NULL CHECK (position >= 0))"

/// Start a caller-supervised pool using a PostgreSQL connection URL.
pub fn start_url(
  name: Name(pog.Message),
  url: String,
  codec: Codec(payload),
) -> Result(#(Pid, EventStore(payload)), StoreError) {
  use config <- result.try(
    pog.url_config(name, url)
    |> result.replace_error(store.StoreFailure(store.StartStore)),
  )
  start(config, codec)
}

/// Start a caller-supervised pool with explicit driver settings.
pub fn start(
  config: pog.Config,
  codec: Codec(payload),
) -> Result(#(Pid, EventStore(payload)), StoreError) {
  use started <- result.try(
    pog.start(config)
    |> result.replace_error(store.StoreFailure(store.StartStore)),
  )
  let actor.Started(pid, pool) = started
  let events = from_pool(pool, codec)
  case ping(events) {
    Ok(Nil) -> Ok(#(pid, events))
    Error(error) -> {
      stop_pool(pid)
      Error(error)
    }
  }
}

@external(erlang, "eventcore_postgres_ffi", "stop_pool")
fn stop_pool(pid: Pid) -> Nil

/// Use an existing pool. Its lifecycle remains with the caller.
pub fn from_pool(
  pool: pog.Connection,
  codec: Codec(payload),
) -> EventStore(payload) {
  EventStore(pool, codec)
}

pub fn ping(events: EventStore(payload)) -> Result(Nil, StoreError) {
  execute(events.pool, "SELECT 1", [])
  |> result.replace_error(store.StoreFailure(store.StartStore))
}

pub fn migrate(events: EventStore(payload)) -> Result(Nil, MigrationError) {
  migrate_tables(events.pool, [
    "CREATE TABLE IF NOT EXISTS eventcore_positions (
      singleton boolean PRIMARY KEY CHECK (singleton),
      last_position bigint NOT NULL CHECK (last_position >= 0))",
    "INSERT INTO eventcore_positions(singleton, last_position) VALUES (true, 0)
     ON CONFLICT (singleton) DO NOTHING",
    "CREATE TABLE IF NOT EXISTS eventcore_events (
      position bigint PRIMARY KEY CHECK (position > 0),
      stream text NOT NULL, version bigint NOT NULL CHECK (version > 0),
      kind text NOT NULL, payload bytea NOT NULL, UNIQUE (stream, version))",
    checkpoint_schema,
    "CREATE TABLE IF NOT EXISTS eventcore_command_snapshots (
      name text PRIMARY KEY, body bytea NOT NULL)",
  ])
}

/// Use the supplied pool for checkpoint-only operations.
pub fn checkpoints_from_pool(pool: pog.Connection) -> CheckpointStore {
  CheckpointStore(pool)
}

pub fn migrate_checkpoints(
  checkpoints: CheckpointStore,
) -> Result(Nil, MigrationError) {
  migrate_tables(checkpoints.pool, [checkpoint_schema])
}

fn migrate_tables(
  pool: pog.Connection,
  statements: List(String),
) -> Result(Nil, MigrationError) {
  pog.transaction(pool, fn(tx) {
    use Nil <- result.try(
      execute(tx, "SELECT 1 FROM pg_advisory_xact_lock(2072806917, 1)", []),
    )
    list.try_each(statements, fn(sql) { execute(tx, sql, []) })
  })
  |> result.replace_error(MigrationFailed)
}

/// Store command-state snapshots in the same database as the event log.
pub fn snapshots(events: EventStore(payload)) -> snapshot.Repository {
  snapshot.Repository(
    load: fn(key) {
      read_snapshot(events.pool, snapshot.name(key))
      |> result.replace_error(store.StoreFailure(store.LoadSnapshot))
    },
    save: fn(key, observed, candidate) {
      let name = snapshot.name(key)
      pog.transaction(events.pool, fn(tx) {
        use Nil <- result.try(read_committed(tx) |> result.replace_error(Nil))
        use Nil <- result.try(
          execute(
            tx,
            "SELECT 1 FROM pg_advisory_xact_lock(2072806917, hashtext($1))",
            [
              pog.text(name),
            ],
          )
          |> result.replace_error(Nil),
        )
        use previous <- result.try(read_snapshot(tx, name))
        let replace = snapshot.can_replace(previous, observed, candidate)
        case replace {
          False -> Ok(Nil)
          True ->
            execute_one(
              tx,
              "INSERT INTO eventcore_command_snapshots(name, body) VALUES ($1, $2)
               ON CONFLICT (name) DO UPDATE SET body = excluded.body",
              [pog.text(name), pog.bytea(encode_snapshot(candidate))],
            )
        }
      })
      |> result.replace_error(store.StoreFailure(store.SaveSnapshot))
    },
  )
}

fn read_snapshot(
  pool: pog.Connection,
  name: String,
) -> Result(Option(snapshot.Snapshot), Nil) {
  use returned <- result.try(
    pog.query("SELECT body FROM eventcore_command_snapshots WHERE name = $1")
    |> pog.parameter(pog.text(name))
    |> pog.returning(decode.at([0], decode.bit_array))
    |> pog.execute(pool)
    |> result.replace_error(Nil),
  )
  case returned.rows {
    [] -> Ok(None)
    [body] -> decode_snapshot(body) |> result.map(Some)
    _ -> Error(Nil)
  }
}

@external(erlang, "eventcore_snapshot_wire", "encode")
fn encode_snapshot(value: snapshot.Snapshot) -> BitArray

@external(erlang, "eventcore_snapshot_wire", "decode")
fn decode_snapshot(value: BitArray) -> Result(snapshot.Snapshot, Nil)

pub fn as_store(events: EventStore(payload)) -> Store(payload) {
  store.Store(fn(id, version) { read(events, id, version) }, fn(writes) {
    append(events, writes)
  })
}

pub fn reset_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Nil, CheckpointError) {
  pog.transaction(checkpoints.pool, fn(tx) {
    use Nil <- result.try(read_committed(tx) |> result.replace_error(Nil))
    let parameters = [pog.text(name)]
    use existing <- result.try(
      query(
        tx,
        "SELECT 1 FROM eventcore_checkpoints WHERE name = $1 FOR UPDATE",
        parameters,
      )
      |> result.replace_error(Nil),
    )
    case existing.count {
      0 -> Ok(Nil)
      1 ->
        execute_one(
          tx,
          "DELETE FROM eventcore_checkpoints WHERE name = $1",
          parameters,
        )
      _ -> Error(Nil)
    }
  })
  |> result.replace_error(CheckpointFailed)
}

pub fn load_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Option(Int), CheckpointError) {
  use returned <- result.try(
    pog.query("SELECT position FROM eventcore_checkpoints WHERE name = $1")
    |> pog.parameter(pog.text(name))
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(checkpoints.pool)
    |> result.replace_error(CheckpointFailed),
  )
  case returned.rows {
    [] -> Ok(None)
    [position] -> Ok(Some(position))
    _ -> Error(CheckpointFailed)
  }
}

pub fn save_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
  position: Int,
) -> Result(Nil, CheckpointError) {
  case position < 0 || position > max_bigint {
    True -> Error(InvalidPosition)
    False ->
      execute_one(
        checkpoints.pool,
        "INSERT INTO eventcore_checkpoints(name, position) VALUES ($1, $2)
         ON CONFLICT (name) DO UPDATE SET position = excluded.position",
        [pog.text(name), pog.int(position)],
      )
      |> result.replace_error(CheckpointFailed)
  }
}

fn read(
  events: EventStore(payload),
  id: Id,
  version: Version,
) -> Result(store.EventStream(payload), StoreError) {
  let after = stream.event_count(version)
  trace_read(stream.name(id), after)
  case after > max_bigint {
    True -> Ok(store.from_items([]))
    False -> {
      let row = {
        use kind <- decode.field(0, decode.string)
        use bytes <- decode.field(1, decode.bit_array)
        decode.success(#(kind, bytes))
      }
      pog.query(
        "SELECT kind, payload FROM eventcore_events
                 WHERE stream = $1 AND version > $2 ORDER BY version",
      )
      |> pog.parameter(pog.text(stream.name(id)))
      |> pog.parameter(pog.int(after))
      |> pog.returning(row)
      |> pog.execute(events.pool)
      |> result.replace_error(store.StoreFailure(store.ReadStream))
      |> result.map(fn(returned) {
        decode_events(events.codec, id, returned.rows)
      })
    }
  }
}

fn decode_events(
  codec: Codec(payload),
  id: Id,
  rows: List(#(String, BitArray)),
) -> store.EventStream(payload) {
  store.EventStream(fn() {
    case rows {
      [] -> Ok(store.End)
      [#(kind, bytes), ..rest] -> {
        use payload <- result.try(
          unpack_payload(bytes)
          |> result.try(codec.decode(kind, _))
          |> result.map_error(store.DeserializationFailed(id, _)),
        )
        Ok(store.Next(
          store.Event(id, kind, payload),
          decode_events(codec, id, rest),
        ))
      }
    }
  })
}

fn append(
  events: EventStore(payload),
  writes: store.Writes(payload),
) -> Result(Nil, StoreError) {
  let expected = store.expected_versions(writes)
  let entries = store.entries(writes)
  trace_append(dict.size(expected), list.length(entries))
  let ordered =
    dict.to_list(expected)
    |> list.sort(fn(first, second) {
      string.compare(stream.name(first.0), stream.name(second.0))
    })
  use entries <- result.try(
    list.try_map(entries, fn(event) {
      events.codec.encode(event.kind, event.payload)
      |> result.map(fn(payload) {
        store.Event(event.stream, event.kind, pack_payload(payload))
      })
      |> result.map_error(store.SerializationFailed(event.stream, _))
    }),
  )
  case
    pog.transaction(events.pool, fn(tx) {
      let failure = store.StoreFailure(store.AppendEvents)
      use Nil <- result.try(read_committed(tx) |> result.replace_error(failure))
      use Nil <- result.try(
        list.try_each(ordered, fn(entry) {
          execute(
            tx,
            "SELECT 1 FROM pg_advisory_xact_lock(hashtextextended($1, 0))",
            [
              pog.text(stream.name(entry.0)),
            ],
          )
          |> result.replace_error(store.StoreFailure(store.AppendEvents))
        }),
      )
      use Nil <- result.try(
        list.try_each(ordered, fn(entry) { check_version(tx, entry.0, entry.1) }),
      )
      append_rows(tx, expected, entries)
    })
  {
    Ok(Nil) -> Ok(Nil)
    Error(pog.TransactionRolledBack(error)) -> Error(error)
    Error(pog.TransactionQueryError(_)) ->
      Error(store.StoreFailure(store.AppendEvents))
  }
}

fn check_version(
  tx: pog.Connection,
  id: Id,
  expected: Version,
) -> Result(Nil, StoreError) {
  let failure = store.StoreFailure(store.AppendEvents)
  use returned <- result.try(
    pog.query(
      "SELECT COALESCE(MAX(version), 0) FROM eventcore_events WHERE stream = $1",
    )
    |> pog.parameter(pog.text(stream.name(id)))
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(tx)
    |> result.replace_error(failure),
  )
  let expected_count = stream.event_count(expected)
  case returned.rows {
    [count] if count == expected_count -> Ok(Nil)
    [count] ->
      stream.version(count)
      |> result.replace_error(failure)
      |> result.try(fn(actual) {
        Error(store.VersionConflict(id, expected, actual))
      })
    _ -> Error(failure)
  }
}

fn append_rows(
  tx: pog.Connection,
  expected: Dict(Id, Version),
  entries: List(Event(BitArray)),
) -> Result(Nil, StoreError) {
  case entries {
    [] -> Ok(Nil)
    [_, ..] -> {
      let count = list.length(entries)
      let failure = store.StoreFailure(store.AppendEvents)
      use returned <- result.try(
        pog.query(
          "UPDATE eventcore_positions SET last_position = last_position + $1
                   WHERE singleton RETURNING last_position",
        )
        |> pog.parameter(pog.int(count))
        |> pog.returning(decode.at([0], decode.int))
        |> pog.execute(tx)
        |> result.replace_error(failure),
      )
      case returned.rows {
        [last] -> {
          use _ <- result.try(
            list.try_fold(entries, #(last - count, expected), fn(acc, event) {
              let #(position, versions) = acc
              use current <- result.try(
                dict.get(versions, event.stream)
                |> result.replace_error(store.UndeclaredStream(event.stream)),
              )
              let next = stream.next_version(current)
              execute_one(
                tx,
                "INSERT INTO eventcore_events(position, stream, version, kind, payload)
                         VALUES ($1, $2, $3, $4, $5)",
                [
                  pog.int(position + 1),
                  pog.text(stream.name(event.stream)),
                  pog.int(stream.event_count(next)),
                  pog.text(event.kind),
                  pog.bytea(event.payload),
                ],
              )
              |> result.replace_error(failure)
              |> result.map(fn(_) {
                #(position + 1, dict.insert(versions, event.stream, next))
              })
            }),
          )
          Ok(Nil)
        }
        _ -> Error(failure)
      }
    }
  }
}

fn read_committed(tx: pog.Connection) -> Result(Nil, pog.QueryError) {
  execute(tx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", [])
}

fn execute(
  pool: pog.Connection,
  sql: String,
  values: List(pog.Value),
) -> Result(Nil, pog.QueryError) {
  query(pool, sql, values) |> result.map(fn(_) { Nil })
}

fn execute_one(pool: pog.Connection, sql: String, values: List(pog.Value)) {
  use returned <- result.try(
    query(pool, sql, values) |> result.replace_error(Nil),
  )
  case returned.count {
    1 -> Ok(Nil)
    _ -> Error(Nil)
  }
}

fn query(pool: pog.Connection, sql: String, values: List(pog.Value)) {
  list.fold(values, pog.query(sql), fn(query, value) {
    pog.parameter(query, value)
  })
  |> pog.execute(pool)
}

fn pack_payload(payload: BitArray) -> BitArray {
  let size = bit_array.bit_size(payload)
  let padding = { 8 - size % 8 } % 8
  <<size:64, payload:bits, 0:size(padding)>>
}

fn unpack_payload(bytes: BitArray) -> Result(BitArray, String) {
  case bytes {
    <<size:64, payload:bits-size(size), pad:bits>> -> {
      let padding = { 8 - size % 8 } % 8
      case pad == <<0:size(padding)>> {
        True -> Ok(payload)
        False -> Error("Invalid PostgreSQL payload padding")
      }
    }
    _ -> Error("Invalid PostgreSQL payload envelope")
  }
}

@external(erlang, "eventcore_postgres_ffi", "trace_read")
fn trace_read(stream: String, after: Int) -> Nil

@external(erlang, "eventcore_postgres_ffi", "trace_append")
fn trace_append(stream_count: Int, event_count: Int) -> Nil
