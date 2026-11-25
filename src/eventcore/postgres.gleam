//// PostgreSQL event and checkpoint storage for an existing Pog pool.

import eventcore/internal/uuid_position

import eventcore/internal/projection_scan
import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/store.{type Event, type Store, type StoreError}
import eventcore/stream.{type Id, type Version}
import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/erlang/process.{type Name, type Pid}
import gleam/int
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

pub type DeliveryError {
  DeliveryReadFailed
  InvalidDeliveryStream(String)
  InvalidDeliveryVersion(Int)
  InvalidDeliveryPayload(projection.Position, String)
}

// A single database-local counter serializes writes through the default.
const next_event_id_sql =
  "CREATE OR REPLACE FUNCTION eventcore_next_event_id()
RETURNS uuid LANGUAGE sql VOLATILE AS $body$
  UPDATE eventcore_uuid_clock AS clock SET
    milliseconds = GREATEST(current.milliseconds, clock.milliseconds +
      CASE WHEN clock.sequence = 4398046511103 THEN 1 ELSE 0 END),
    sequence = CASE WHEN current.milliseconds > clock.milliseconds
      THEN floor(random() * 2199023255552)::bigint
      ELSE (clock.sequence + 1) & 4398046511103 END
  FROM (SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
          AS milliseconds) AS current
  WHERE clock.singleton
  RETURNING (
    lpad(to_hex(clock.milliseconds), 12, '0') ||
    '7' || lpad(to_hex(clock.sequence >> 30), 3, '0') ||
    to_hex(8 | ((clock.sequence >> 28) & 3)) ||
    lpad(to_hex(clock.sequence & 268435455), 7, '0') ||
    right(gen_random_uuid()::text, 8)
  )::uuid
$body$"

// UUID ordering compares the timestamp, version, then sequence. A v4 ID
// therefore precedes every v7 ID with the same 48-bit prefix.
const seed_event_id_sql =
  "DO $body$ DECLARE latest text; BEGIN
  IF NOT EXISTS (SELECT 1 FROM eventcore_uuid_clock WHERE singleton) THEN
    SELECT replace(event_id::text, '-', '') INTO latest
    FROM eventcore_events WHERE event_id IS NOT NULL
    ORDER BY event_id DESC LIMIT 1;
    INSERT INTO eventcore_uuid_clock(singleton, milliseconds, sequence)
    VALUES (true,
      COALESCE(('x' || left(latest, 12))::bit(48)::bigint, 0)
        + CASE WHEN substr(latest, 13, 1) > '7' THEN 1 ELSE 0 END,
      CASE WHEN substr(latest, 13, 1) = '7' THEN
        ((('x' || substr(latest, 14, 3))::bit(12)::bigint) << 30)
          | ((('x' || substr(latest, 17, 8))::bit(32)::bigint) & 1073741823)
        ELSE -1 END);
  END IF;
END $body$"

/// Adds persisted event identities to the existing committed event sequence.
/// Reapplying the migration retains existing IDs and the UUID clock.
pub fn migrate_delivery(
  events: EventStore(payload),
) -> Result(Nil, MigrationError) {
  migrate_tables(events.pool, [
    "ALTER TABLE eventcore_events
      ADD COLUMN IF NOT EXISTS event_id uuid,
      ADD COLUMN IF NOT EXISTS metadata json NOT NULL DEFAULT '{}'::json",
    "CREATE TABLE IF NOT EXISTS eventcore_uuid_clock (
      singleton boolean PRIMARY KEY CHECK (singleton),
      milliseconds bigint NOT NULL CHECK (milliseconds BETWEEN 0 AND 281474976710655),
      sequence bigint NOT NULL)",
    "CREATE UNIQUE INDEX IF NOT EXISTS eventcore_events_event_id ON eventcore_events(event_id)",
    seed_event_id_sql,
    next_event_id_sql,
    "DO $body$ DECLARE event_position bigint; BEGIN
      FOR event_position IN SELECT position FROM eventcore_events
        WHERE event_id IS NULL ORDER BY position
      LOOP
        UPDATE eventcore_events SET event_id = eventcore_next_event_id()
          WHERE position = event_position;
      END LOOP;
    END $body$",
    "ALTER TABLE eventcore_events
      ALTER COLUMN event_id SET NOT NULL,
      ALTER COLUMN event_id SET DEFAULT eventcore_next_event_id()",
  ])
}

pub fn projection_source(
  events: EventStore(payload),
  id: delivery.SourceId,
) -> delivery.Source(DeliveryError) {
  delivery.Source(
    id,
    fn() {
      delivery_frontier(projection_connection(events.pool))
      |> result.map(delivery_position)
    },
    fn(selection, page, through) {
      read_delivery(
        projection_connection(events.pool),
        id,
        selection,
        page,
        through,
      )
    },
  )
}

fn delivery_position(value: Int) -> Option(projection.Position) {
  option.from_result(projection.position(value))
}

fn delivery_frontier(pool: pog.Connection) -> Result(Int, DeliveryError) {
  use returned <- result.try(
    pog.query("SELECT last_position FROM eventcore_positions WHERE singleton")
    |> pog.returning(decode.at([0], decode.int))
    |> pog.execute(pool)
    |> result.replace_error(DeliveryReadFailed),
  )
  case returned.rows {
    [position] -> Ok(position)
    _ -> Error(DeliveryReadFailed)
  }
}

fn read_delivery(
  pool: pog.Connection,
  id: delivery.SourceId,
  selection: delivery.Selection,
  page: projection.Page,
  through: delivery.UpperBound,
) -> Result(List(delivery.Envelope), DeliveryError) {
  use frontier <- result.try(delivery_frontier(pool))
  let high = case through {
    delivery.Unbounded -> frontier
    delivery.Inclusive(position) ->
      int.min(frontier, projection.sequence(position))
  }
  let after =
    option.map(projection.page_after(page), projection.sequence)
    |> option.unwrap(0)
  scan_delivery(
    pool,
    id,
    selection,
    after,
    high,
    projection.page_limit(page),
    [],
  )
}

type DeliveryRow {
  DeliveryRow(
    position: Int,
    id: String,
    stream: String,
    version: Int,
    kind: String,
    payload: BitArray,
    metadata: String,
  )
}

fn delivery_row() -> decode.Decoder(DeliveryRow) {
  use position <- decode.field(0, decode.int)
  use id <- decode.field(1, decode.string)
  use stream <- decode.field(2, decode.string)
  use version <- decode.field(3, decode.int)
  use kind <- decode.field(4, decode.string)
  use payload <- decode.field(5, decode.bit_array)
  use metadata <- decode.field(6, decode.string)
  decode.success(DeliveryRow(
    position,
    id,
    stream,
    version,
    kind,
    payload,
    metadata,
  ))
}

fn scan_delivery(pool, id, selection, after, high, remaining, reversed) {
  case after >= high || remaining == 0 {
    True -> Ok(list.reverse(reversed))
    False -> {
      let through = int.min(after + 128, high)
      use candidates <- result.try(
        pog.query(
          "SELECT position, stream, kind FROM eventcore_events
          WHERE position BETWEEN $1 AND $2 AND kind = ANY($3::text[])
          ORDER BY position",
        )
        |> pog.parameter(pog.int(after + 1))
        |> pog.parameter(pog.int(through))
        |> pog.parameter(pog.array(
          pog.text,
          delivery.selection_event_types(selection),
        ))
        |> pog.returning({
          use position <- decode.field(0, decode.int)
          use stream <- decode.field(1, decode.string)
          use kind <- decode.field(2, decode.string)
          decode.success(#(position, stream, kind))
        })
        |> pog.execute(pool)
        |> result.replace_error(DeliveryReadFailed),
      )
      use positions <- result.try(
        selected_delivery_positions(candidates.rows, selection, remaining, []),
      )
      use rows <- result.try(fetch_delivery_rows(pool, positions))
      use Nil <- result.try(case list.length(positions) == list.length(rows) {
        True -> Ok(Nil)
        False -> Error(DeliveryReadFailed)
      })
      use envelopes <- result.try(list.try_map(rows, decode_delivery(_, id)))
      scan_delivery(
        pool,
        id,
        selection,
        through,
        high,
        remaining - list.length(envelopes),
        list.fold(envelopes, reversed, fn(acc, envelope) { [envelope, ..acc] }),
      )
    }
  }
}

fn selected_delivery_positions(rows, selection, remaining, reversed) {
  case rows, remaining {
    [], _ | _, 0 -> Ok(reversed)
    [#(position, name, kind), ..rest], _ -> {
      use id <- result.try(
        stream.id(name) |> result.replace_error(InvalidDeliveryStream(name)),
      )
      case delivery.selects(selection, id, kind) {
        True ->
          selected_delivery_positions(rest, selection, remaining - 1, [
            position,
            ..reversed
          ])
        False ->
          selected_delivery_positions(rest, selection, remaining, reversed)
      }
    }
  }
}

fn fetch_delivery_rows(pool, positions) {
  case positions {
    [] -> Ok([])
    [_, ..] ->
      pog.query(
        "SELECT position, event_id::text, stream, version, kind, payload, metadata::text
        FROM eventcore_events WHERE position = ANY($1::bigint[]) ORDER BY position",
      )
      |> pog.parameter(pog.array(pog.int, positions))
      |> pog.returning(delivery_row())
      |> pog.execute(pool)
      |> result.map(fn(result) { result.rows })
      |> result.replace_error(DeliveryReadFailed)
  }
}

fn decode_delivery(row: DeliveryRow, id: delivery.SourceId) {
  use stream <- result.try(
    stream.id(row.stream)
    |> result.replace_error(InvalidDeliveryStream(row.stream)),
  )
  use position <- result.try(
    projection.position(row.position)
    |> result.replace_error(DeliveryReadFailed),
  )
  use version <- result.try(
    stream.version(row.version)
    |> result.replace_error(InvalidDeliveryVersion(row.version)),
  )
  use kind <- result.try(
    delivery.persisted_event_type(row.kind)
    |> result.replace_error(DeliveryReadFailed),
  )
  use payload <- result.try(
    unpack_payload(row.payload)
    |> result.map_error(InvalidDeliveryPayload(position, _)),
  )
  Ok(delivery.Envelope(
    id,
    position,
    row.id,
    stream,
    version,
    kind,
    payload,
    row.metadata,
  ))
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

/// Read committed insertion positions using the ordinary event codec.
pub fn projection_reader(
  events: EventStore(payload),
) -> projection.Reader(payload) {
  fn(filter, page) {
    let pool = projection_connection(events.pool)
    use frontier <- result.try(
      pog.query("SELECT last_position FROM eventcore_positions WHERE singleton")
      |> pog.returning(decode.at([0], decode.int))
      |> pog.execute(pool)
      |> result.replace_error(store.StoreFailure(store.ReadProjection)),
    )
    case frontier.rows {
      [high] ->
        projection_scan.read(
          filter,
          page,
          high,
          fn(first, last) { projection_range(pool, first, last) },
          fn(event) {
            use payload <- result.map(
              unpack_payload(event.payload)
              |> result.try(events.codec.decode(event.kind, _))
              |> result.map_error(store.DeserializationFailed(event.stream, _)),
            )
            store.Event(event.stream, event.kind, payload)
          },
        )
      _ -> Error(store.StoreFailure(store.ReadProjection))
    }
  }
}

/// Read legacy release pages in persisted UUID order. Run `migrate_delivery`
/// on the event database and pair this reader with release checkpoints.
pub fn projection_release_reader(
  events: EventStore(payload),
  consumer_kind: String,
) -> projection.Reader(payload) {
  fn(filter, page) {
    let failure = store.StoreFailure(store.ReadProjection)
    let after = projection.page_after(page)
    use cursor <- result.try(case after {
      None -> Ok(None)
      Some(position) ->
        uuid_position.to_uuid(position)
        |> result.map(Some)
        |> result.replace_error(failure)
    })
    let pattern =
      projection.stream_pattern(filter) |> option.map(release_glob_regex)
    let kind = projection.event_kind(filter) |> option.unwrap(consumer_kind)
    let row = {
      use uuid <- decode.field(0, decode.string)
      use stream <- decode.field(1, decode.string)
      use kind <- decode.field(2, decode.string)
      use payload <- decode.field(3, decode.bit_array)
      decode.success(#(uuid, stream, kind, payload))
    }
    use returned <- result.try(
      pog.query(
        "SELECT events.event_id::text AS id_text, stream, kind, payload
         FROM eventcore_events AS events
         WHERE kind = $1
           AND ($2::text IS NULL OR events.event_id > $2::text::uuid)
           AND ($3::text IS NULL OR stream LIKE $3::text || '%')
           AND ($4::text IS NULL OR stream ~ $4::text)
         ORDER BY events.event_id LIMIT $5",
      )
      |> pog.parameter(pog.text(kind))
      |> pog.parameter(pog.nullable(pog.text, cursor))
      |> pog.parameter(pog.nullable(pog.text, projection.stream_prefix(filter)))
      |> pog.parameter(pog.nullable(pog.text, pattern))
      |> pog.parameter(
        pog.int(int.min(projection.page_limit(page), max_bigint)),
      )
      |> pog.returning(row)
      |> pog.execute(projection_connection(events.pool))
      |> result.replace_error(failure),
    )
    release_page(events.codec, returned.rows, after, [])
  }
}

fn release_page(
  codec: Codec(payload),
  rows: List(#(String, String, String, BitArray)),
  after: Option(projection.Position),
  reversed: List(projection.Delivery(payload)),
) -> Result(projection.PageResult(payload), StoreError) {
  case rows {
    [] -> Ok(projection.PageResult(list.reverse(reversed), after))
    [#(uuid, name, kind, packed), ..rest] -> {
      let failure = store.StoreFailure(store.ReadProjection)
      use id <- result.try(stream.id(name) |> result.replace_error(failure))
      use position <- result.try(
        uuid_position.from_uuid(uuid) |> result.replace_error(failure),
      )
      use data <- result.try(
        unpack_payload(packed) |> result.replace_error(failure),
      )
      case codec.decode(kind, data) {
        Error(_) -> release_page(codec, rest, after, reversed)
        Ok(payload) ->
          release_page(codec, rest, Some(position), [
            projection.Delivery(store.Event(id, kind, payload), position),
            ..reversed
          ])
      }
    }
  }
}

@external(erlang, "eventcore_postgres_ffi", "release_glob_regex")
fn release_glob_regex(pattern: String) -> String

fn projection_range(pool, first, last) {
  use returned <- result.try(
    pog.query(
      "SELECT position, stream, kind, payload FROM eventcore_events
      WHERE position BETWEEN $1 AND $2 ORDER BY position",
    )
    |> pog.parameter(pog.int(first))
    |> pog.parameter(pog.int(last))
    |> pog.returning({
      use position <- decode.field(0, decode.int)
      use stream <- decode.field(1, decode.string)
      use kind <- decode.field(2, decode.string)
      use payload <- decode.field(3, decode.bit_array)
      decode.success(#(position, stream, kind, payload))
    })
    |> pog.execute(pool)
    |> result.replace_error(store.StoreFailure(store.ReadProjection)),
  )
  list.try_map(returned.rows, fn(row) {
    let #(position, name, kind, payload) = row
    use id <- result.map(
      stream.id(name)
      |> result.replace_error(store.StoreFailure(store.ReadProjection)),
    )
    #(position, store.Event(id, kind, payload))
  })
}

/// Set up the UUID cursor namespace for release-compatible legacy readers.
pub fn migrate_release_checkpoints(
  checkpoints: CheckpointStore,
) -> Result(Nil, MigrationError) {
  migrate_tables(checkpoints.pool, [
    "CREATE TABLE IF NOT EXISTS eventcore_release_checkpoints (
      name text PRIMARY KEY, position uuid NOT NULL)",
  ])
}

pub fn projection_release_checkpoints(
  checkpoints: CheckpointStore,
) -> projection.Checkpoints {
  projection.Checkpoints(
    fn(name) {
      let failure = store.StoreFailure(store.LoadCheckpoint)
      let session = projection_checkpoint_session(checkpoints, name)
      use returned <- result.try(
        pog.query(
          "SELECT position::text FROM eventcore_release_checkpoints WHERE name = $1",
        )
        |> pog.parameter(pog.text(name))
        |> pog.returning(decode.at([0], decode.string))
        |> pog.execute(session.pool)
        |> result.replace_error(failure),
      )
      case returned.rows {
        [] -> Ok(None)
        [uuid] ->
          uuid_position.from_uuid(uuid)
          |> result.map(Some)
          |> result.replace_error(failure)
        _ -> Error(failure)
      }
    },
    fn(name, position) {
      let failure = store.StoreFailure(store.SaveCheckpoint)
      let session = projection_checkpoint_session(checkpoints, name)
      use uuid <- result.try(
        uuid_position.to_uuid(position) |> result.replace_error(failure),
      )
      execute_one(
        session.pool,
        "INSERT INTO eventcore_release_checkpoints(name, position)
         VALUES ($1, $2::text::uuid)
         ON CONFLICT (name) DO UPDATE SET position = excluded.position",
        [pog.text(name), pog.text(uuid)],
      )
      |> result.replace_error(failure)
    },
  )
}

pub fn projection_checkpoints(
  checkpoints: CheckpointStore,
) -> projection.Checkpoints {
  projection.Checkpoints(
    fn(name) {
      use cursor <- result.try(
        load_checkpoint(projection_checkpoint_session(checkpoints, name), name)
        |> result.replace_error(store.StoreFailure(store.LoadCheckpoint)),
      )
      case cursor {
        None -> Ok(None)
        Some(sequence) ->
          projection.position(sequence)
          |> result.map(Some)
          |> result.replace_error(store.StoreFailure(store.LoadCheckpoint))
      }
    },
    fn(name, position) {
      save_checkpoint(
        projection_checkpoint_session(checkpoints, name),
        name,
        projection.sequence(position),
      )
      |> result.replace_error(store.StoreFailure(store.SaveCheckpoint))
    },
  )
}

pub fn projection_backend(
  events: EventStore(payload),
) -> projection_runner.Backend(payload) {
  projection_backend_with_checkpoints(
    events,
    checkpoints_from_pool(events.pool),
  )
}

pub fn projection_backend_with_checkpoints(
  events: EventStore(payload),
  checkpoints: CheckpointStore,
) -> projection_runner.Backend(payload) {
  projection_runner.Backend(
    projection_reader(events),
    projection_checkpoints(checkpoints),
    projection_coordinator(checkpoints.pool),
  )
}

/// Combine UUID-ordered replay, UUID checkpoints and session leadership.
pub fn projection_release_backend(
  events: EventStore(payload),
  checkpoints: CheckpointStore,
  consumer_kind: String,
) -> projection_runner.Backend(payload) {
  projection_runner.Backend(
    projection_release_reader(events, consumer_kind),
    projection_release_checkpoints(checkpoints),
    projection_coordinator(checkpoints.pool),
  )
}

/// Acquires a session advisory lock from an existing pool. Use the same pool
/// as the checkpoint adapter to route its writes through the locked session.
pub fn projection_coordinator(
  pool: pog.Connection,
) -> projection_runner.Coordinator {
  fn(name) {
    use release <- result.map(acquire_projection(pool, name))
    projection_runner.Lease(release)
  }
}

@external(erlang, "eventcore_postgres_ffi", "acquire_projection")
fn acquire_projection(
  pool: pog.Connection,
  name: String,
) -> Result(fn() -> Nil, projection_runner.LeadershipError)

fn projection_checkpoint_session(
  checkpoints: CheckpointStore,
  name: String,
) -> CheckpointStore {
  CheckpointStore(checkpoint_connection(checkpoints.pool, name))
}

@external(erlang, "eventcore_postgres_ffi", "checkpoint_connection")
fn checkpoint_connection(pool: pog.Connection, name: String) -> pog.Connection

@external(erlang, "eventcore_postgres_ffi", "projection_connection")
fn projection_connection(pool: pog.Connection) -> pog.Connection

pub fn reset_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Nil, CheckpointError) {
  reset_checkpoint_in(checkpoints, name, "eventcore_checkpoints")
}

/// Remove progress only from the release-compatible UUID namespace.
pub fn reset_release_checkpoint(
  checkpoints: CheckpointStore,
  name: String,
) -> Result(Nil, CheckpointError) {
  reset_checkpoint_in(checkpoints, name, "eventcore_release_checkpoints")
}

fn reset_checkpoint_in(
  checkpoints: CheckpointStore,
  name: String,
  table: String,
) -> Result(Nil, CheckpointError) {
  pog.transaction(checkpoints.pool, fn(tx) {
    use Nil <- result.try(read_committed(tx) |> result.replace_error(Nil))
    let parameters = [pog.text(name)]
    use existing <- result.try(
      query(
        tx,
        "SELECT 1 FROM " <> table <> " WHERE name = $1 FOR UPDATE",
        parameters,
      )
      |> result.replace_error(Nil),
    )
    case existing.count {
      0 -> Ok(Nil)
      1 ->
        execute_one(
          tx,
          "DELETE FROM " <> table <> " WHERE name = $1",
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
  case store.entries(writes) {
    [] -> Ok(Nil)
    [_, ..] as entries ->
      append_nonempty(events, store.expected_versions(writes), entries)
  }
}

fn append_nonempty(
  events: EventStore(payload),
  expected: Dict(Id, Version),
  entries: List(Event(payload)),
) -> Result(Nil, StoreError) {
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
