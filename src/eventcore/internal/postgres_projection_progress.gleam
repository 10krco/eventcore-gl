import eventcore/projection.{type Position}
import eventcore/projection_delivery as delivery
import gleam/dynamic/decode
import gleam/option.{type Option, None, Some}
import gleam/result
import pog

pub type Progress {
  Progress(
    source: delivery.SourceId,
    selection: delivery.SelectionId,
    position: Position,
  )
}

pub type Error {
  Database(pog.QueryError)
  InvalidProgress
  UnexpectedRows
}

pub fn migrate(
  pool: pog.Connection,
) -> Result(Nil, pog.TransactionError(pog.QueryError)) {
  pog.transaction(pool, fn(tx) {
    use _ <- result.try(
      pog.query("SELECT 1 FROM pg_advisory_xact_lock(3720066808599939109)")
      |> pog.execute(tx),
    )
    use _ <- result.try(
      pog.query(
        "CREATE TABLE IF NOT EXISTS eventcore_projection_schema_versions (
          component text NOT NULL, version bigint NOT NULL,
          applied_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (component, version))",
      )
      |> pog.execute(tx),
    )
    use current <- result.try(
      pog.query(
        "SELECT EXISTS (SELECT 1 FROM eventcore_projection_schema_versions
          WHERE component = 'projection-destination' AND version = 2)",
      )
      |> pog.returning(decode.at([0], decode.bool))
      |> pog.execute(tx),
    )
    case current.rows {
      [True] -> Ok(Nil)
      _ -> {
        use _ <- result.try(
          pog.query(
            "CREATE TABLE IF NOT EXISTS eventcore_projection_progress (
              projector_name text PRIMARY KEY, source_id text NOT NULL,
              selection_id text NOT NULL,
              last_position bigint NOT NULL CHECK (last_position > 0),
              updated_at timestamptz NOT NULL DEFAULT now())",
          )
          |> pog.execute(tx),
        )
        pog.query(
          "INSERT INTO eventcore_projection_schema_versions(component, version)
            VALUES ('projection-destination', 2)",
        )
        |> pog.execute(tx)
        |> result.map(fn(_) { Nil })
      }
    }
  })
}

pub fn load(
  connection: pog.Connection,
  projector: delivery.ProjectorName,
) -> Result(Option(Progress), Error) {
  use rows <- result.try(query_progress(connection, projector, ""))
  decode_progress(rows)
}

pub fn lock(
  transaction: pog.Connection,
  projector: delivery.ProjectorName,
) -> Result(Option(Progress), Error) {
  use rows <- result.try(query_progress(transaction, projector, " FOR UPDATE"))
  decode_progress(rows)
}

fn query_progress(connection, projector, suffix) {
  pog.query("SELECT source_id, selection_id, last_position
      FROM eventcore_projection_progress WHERE projector_name = $1" <> suffix)
  |> pog.parameter(pog.text(delivery.projector_name_value(projector)))
  |> pog.returning({
    use source <- decode.field(0, decode.string)
    use selection <- decode.field(1, decode.string)
    use position <- decode.field(2, decode.int)
    decode.success(#(source, selection, position))
  })
  |> pog.execute(connection)
  |> result.map(fn(returned) { returned.rows })
  |> result.map_error(Database)
}

fn decode_progress(rows) {
  case rows {
    [] -> Ok(None)
    [#(source, selection, position)] -> {
      let parsed = #(
        delivery.source_id(source),
        delivery.selection_id(selection),
        projection.position(position),
      )
      case parsed {
        #(Ok(source), Ok(selection), Ok(position)) ->
          Ok(Some(Progress(source, selection, position)))
        _ -> Error(InvalidProgress)
      }
    }
    _ -> Error(UnexpectedRows)
  }
}

pub fn advance(
  transaction: pog.Connection,
  projector: delivery.ProjectorName,
  progress: Progress,
) -> Result(Nil, Error) {
  use returned <- result.try(
    pog.query(
      "INSERT INTO eventcore_projection_progress
        (projector_name, source_id, selection_id, last_position)
        VALUES ($1, $2, $3, $4)
       ON CONFLICT (projector_name) DO UPDATE SET
        source_id = excluded.source_id, selection_id = excluded.selection_id,
        last_position = excluded.last_position, updated_at = now()",
    )
    |> pog.parameter(pog.text(delivery.projector_name_value(projector)))
    |> pog.parameter(pog.text(delivery.source_name(progress.source)))
    |> pog.parameter(pog.text(delivery.selection_name(progress.selection)))
    |> pog.parameter(pog.int(projection.sequence(progress.position)))
    |> pog.execute(transaction)
    |> result.map_error(Database),
  )
  case returned.count {
    1 -> Ok(Nil)
    _ -> Error(UnexpectedRows)
  }
}

pub fn delete(
  transaction: pog.Connection,
  projector: delivery.ProjectorName,
  observed: Option(Progress),
) -> Result(Nil, Error) {
  case observed {
    None -> Ok(Nil)
    Some(_) -> {
      use returned <- result.try(
        pog.query(
          "DELETE FROM eventcore_projection_progress WHERE projector_name = $1",
        )
        |> pog.parameter(pog.text(delivery.projector_name_value(projector)))
        |> pog.execute(transaction)
        |> result.map_error(Database),
      )
      case returned.count {
        1 -> Ok(Nil)
        _ -> Error(UnexpectedRows)
      }
    }
  }
}
