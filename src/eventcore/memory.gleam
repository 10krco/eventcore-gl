//// In-memory event store backed by an OTP process. Call close when finished.

import eventcore/store.{
  type Event, type EventStream, type Store, type StoreError, type Writes,
}
import eventcore/stream.{type Id, type Version}
import gleam/dict.{type Dict}
import gleam/list
import gleam/result

pub opaque type Memory(payload) {
  Memory(Server(Dict(Id, History(payload))))
}

type Server(state)

type History(payload) {
  History(version: Version, newest_first: List(Event(payload)))
}

@external(erlang, "eventcore_memory_ffi", "start")
fn start_server(initial: state) -> Result(Server(state), Nil)

@external(erlang, "eventcore_memory_ffi", "call")
fn call(
  server: Server(state),
  operation: fn(state) -> #(state, response),
) -> Result(response, Nil)

@external(erlang, "eventcore_memory_ffi", "stop")
fn stop(server: Server(state)) -> Result(Nil, Nil)

pub fn start() -> Result(Memory(payload), StoreError) {
  start_server(dict.new())
  |> result.map(Memory)
  |> result.replace_error(store.StoreFailure(store.StartStore))
}

pub fn close(memory: Memory(payload)) -> Result(Nil, StoreError) {
  let Memory(server) = memory
  stop(server)
  |> result.replace_error(store.StoreFailure(store.CloseStore))
}

pub fn as_store(memory: Memory(payload)) -> Store(payload) {
  store.Store(
    fn(id, version) { read_after(memory, id, version) },
    append_events(memory, _),
  )
}

fn history(streams: Dict(Id, History(payload)), id: Id) -> History(payload) {
  dict.get(streams, id)
  |> result.unwrap(History(stream.initial_version, []))
}

fn read_after(
  memory: Memory(payload),
  id: Id,
  after: Version,
) -> Result(EventStream(payload), StoreError) {
  let Memory(server) = memory
  call(server, fn(streams) { #(streams, history(streams, id)) })
  |> result.replace_error(store.StoreFailure(store.ReadStream))
  |> result.map(fn(history) {
    let count = stream.event_count(history.version) - stream.event_count(after)
    history.newest_first
    |> list.take(count)
    |> list.fold([], fn(items, event) { [Ok(event), ..items] })
    |> store.from_items
  })
}

fn append_events(
  memory: Memory(payload),
  writes: Writes(payload),
) -> Result(Nil, StoreError) {
  let Memory(server) = memory
  use response <- result.try(
    call(server, fn(streams) {
      let checked =
        store.expected_versions(writes)
        |> dict.to_list
        |> list.try_fold(Nil, fn(_, entry) {
          let #(id, expected) = entry
          let actual = history(streams, id).version
          case actual == expected {
            True -> Ok(Nil)
            False -> Error(store.VersionConflict(id, expected, actual))
          }
        })
      case checked {
        Error(_) as conflict -> #(streams, conflict)
        Ok(Nil) -> {
          let updated =
            writes
            |> store.entries
            |> list.fold(streams, fn(streams, event) {
              let previous = history(streams, event.stream)
              let updated =
                History(stream.next_version(previous.version), [
                  event,
                  ..previous.newest_first
                ])
              dict.insert(streams, event.stream, updated)
            })
          #(updated, Ok(Nil))
        }
      }
    })
    |> result.replace_error(store.StoreFailure(store.AppendEvents)),
  )
  response
}
