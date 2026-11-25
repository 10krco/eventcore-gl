import eventcore/projection
import eventcore/store
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

const chunk_size = 128

pub fn read(
  filter: projection.Filter,
  page: projection.Page,
  high: Int,
  fetch: fn(Int, Int) ->
    Result(List(#(Int, store.Event(wire))), store.StoreError),
  decode: fn(store.Event(wire)) ->
    Result(store.Event(payload), store.StoreError),
) -> Result(projection.PageResult(payload), store.StoreError) {
  let after = case projection.page_after(page) {
    Some(position) -> projection.sequence(position)
    None -> 0
  }
  scan(
    Reader(filter, high, fetch, decode, projection.page_after(page)),
    after,
    projection.page_limit(page),
    [],
  )
}

type Reader(wire, payload) {
  Reader(
    filter: projection.Filter,
    high: Int,
    fetch: fn(Int, Int) ->
      Result(List(#(Int, store.Event(wire))), store.StoreError),
    decode: fn(store.Event(wire)) ->
      Result(store.Event(payload), store.StoreError),
    input_cursor: Option(projection.Position),
  )
}

fn scan(
  reader: Reader(wire, payload),
  after: Int,
  remaining: Int,
  reversed: List(projection.Delivery(payload)),
) -> Result(projection.PageResult(payload), store.StoreError) {
  case remaining == 0 || after >= reader.high {
    True -> finish(reader, after, reversed)
    False -> {
      let through = int.min(reader.high, after + chunk_size)
      use events <- result.try(reader.fetch(after + 1, through))
      consume(reader, events, through, remaining, reversed)
    }
  }
}

fn consume(
  reader: Reader(wire, payload),
  events: List(#(Int, store.Event(wire))),
  through: Int,
  remaining: Int,
  reversed: List(projection.Delivery(payload)),
) -> Result(projection.PageResult(payload), store.StoreError) {
  case events {
    [] -> scan(reader, through, remaining, reversed)
    [#(sequence, event), ..rest] ->
      case projection.matches(reader.filter, event.stream, event.kind) {
        False -> consume(reader, rest, through, remaining, reversed)
        True ->
          case reader.decode(event) {
            Error(store.DeserializationFailed(_, _) as error) ->
              case
                projection.uses_release_pagination(reader.filter),
                remaining
              {
                False, _ -> Error(error)
                True, 1 -> finish(reader, sequence, reversed)
                True, _ ->
                  consume(reader, rest, through, remaining - 1, reversed)
              }
            Error(error) -> Error(error)
            Ok(event) -> {
              use position <- result.try(
                projection.position(sequence)
                |> result.replace_error(store.StoreFailure(store.ReadProjection)),
              )
              let reversed = [projection.Delivery(event, position), ..reversed]
              case remaining {
                1 -> finish(reader, sequence, reversed)
                _ -> consume(reader, rest, through, remaining - 1, reversed)
              }
            }
          }
      }
  }
}

fn finish(
  reader: Reader(wire, payload),
  scanned: Int,
  reversed: List(projection.Delivery(payload)),
) -> Result(projection.PageResult(payload), store.StoreError) {
  let cursor = case
    projection.uses_release_pagination(reader.filter),
    reversed
  {
    False, _ -> option_position(scanned)
    True, [delivery, ..] -> Some(delivery.position)
    True, [] -> reader.input_cursor
  }
  Ok(projection.PageResult(list.reverse(reversed), cursor))
}

fn option_position(sequence: Int) {
  case projection.position(sequence) {
    Ok(position) -> Some(position)
    Error(Nil) -> None
  }
}
