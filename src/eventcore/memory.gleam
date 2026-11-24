//// In-memory event store backed by an OTP process. The store stops when its
//// creating process exits. Call close to release it earlier.

import eventcore/projection
import eventcore/projection_runner
import eventcore/snapshot
import eventcore/store.{
  type Event, type EventStream, type Store, type StoreError, type Writes,
}
import eventcore/stream.{type Id, type Version}
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result

pub opaque type Memory(payload) {
  Memory(Server(State(payload)))
}

type State(payload) {
  State(
    streams: Dict(Id, History(payload)),
    snapshots: Dict(snapshot.Key, snapshot.Snapshot),
    projection_events: Dict(Int, Event(payload)),
    last_position: Int,
    checkpoints: Dict(String, projection.Position),
  )
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
  start_server(State(dict.new(), dict.new(), dict.new(), 0, dict.new()))
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
  call(server, fn(state) {
    let history = history(state.streams, id)
    let count = stream.event_count(history.version) - stream.event_count(after)
    #(state, list.take(history.newest_first, count))
  })
  |> result.replace_error(store.StoreFailure(store.ReadStream))
  |> result.map(fn(newest_first) {
    newest_first
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
    call(server, fn(state) {
      let streams = state.streams
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
        Error(_) as conflict -> #(state, conflict)
        Ok(Nil) -> {
          let updated = list.fold(store.entries(writes), state, record_event)
          #(updated, Ok(Nil))
        }
      }
    })
    |> result.replace_error(store.StoreFailure(store.AppendEvents)),
  )
  response
}

pub fn snapshots(memory: Memory(payload)) -> snapshot.Repository {
  let Memory(server) = memory
  snapshot.Repository(
    load: fn(key) {
      call(server, fn(state) {
        let saved = case dict.get(state.snapshots, key) {
          Ok(saved) -> Some(saved)
          Error(Nil) -> None
        }
        #(state, saved)
      })
      |> result.replace_error(store.StoreFailure(store.LoadSnapshot))
    },
    save: fn(key, observed, candidate) {
      call(server, fn(state) {
        let current = case dict.get(state.snapshots, key) {
          Ok(previous) -> Some(previous)
          Error(Nil) -> None
        }
        let replace = snapshot.can_replace(current, observed, candidate)
        let next = case replace {
          True ->
            State(
              ..state,
              snapshots: dict.insert(state.snapshots, key, candidate),
            )
          False -> state
        }
        #(next, Nil)
      })
      |> result.replace_error(store.StoreFailure(store.SaveSnapshot))
    },
  )
}

fn record_event(
  state: State(payload),
  event: Event(payload),
) -> State(payload) {
  let previous = history(state.streams, event.stream)
  let updated =
    History(stream.next_version(previous.version), [
      event,
      ..previous.newest_first
    ])
  let position = state.last_position + 1
  State(
    ..state,
    streams: dict.insert(state.streams, event.stream, updated),
    projection_events: dict.insert(state.projection_events, position, event),
    last_position: position,
  )
}

const projection_chunk_size = 128

const read_projection_failure = store.StoreFailure(store.ReadProjection)

/// Enumerates events in append order across streams.
pub fn projection_reader(
  memory: Memory(payload),
) -> projection.Reader(payload) {
  let Memory(server) = memory
  fn(filter, page) {
    let after = case projection.page_after(page) {
      Some(position) -> projection.sequence(position)
      None -> 0
    }
    use high <- result.try(
      call(server, fn(state) { #(state, state.last_position) })
      |> result.replace_error(read_projection_failure),
    )
    scan_page(
      server,
      filter,
      high,
      after + 1,
      projection.page_limit(page),
      [],
      [],
    )
  }
}

fn scan_page(
  server: Server(State(payload)),
  filter: projection.Filter,
  high: Int,
  next: Int,
  remaining: Int,
  chunk: List(Event(payload)),
  reversed: List(projection.Delivery(payload)),
) -> Result(projection.PageResult(payload), StoreError) {
  case remaining == 0 || next > high, chunk {
    True, _ ->
      Ok(projection.PageResult(
        list.reverse(reversed),
        option.from_result(projection.position(next - 1)),
      ))
    False, [] -> {
      let through = int.min(high, next + projection_chunk_size - 1)
      use chunk <- result.try(
        call(server, fn(state) {
          #(state, fetch_chunk(state.projection_events, next, through, []))
        })
        |> result.replace_error(read_projection_failure)
        |> result.flatten,
      )
      scan_page(server, filter, high, next, remaining, chunk, reversed)
    }
    False, [event, ..rest] ->
      case projection.matches(filter, event.stream, event.kind) {
        False ->
          scan_page(server, filter, high, next + 1, remaining, rest, reversed)
        True -> {
          use position <- result.try(
            projection.position(next)
            |> result.replace_error(read_projection_failure),
          )
          scan_page(server, filter, high, next + 1, remaining - 1, rest, [
            projection.Delivery(event, position),
            ..reversed
          ])
        }
      }
  }
}

fn fetch_chunk(
  by_position: Dict(Int, Event(payload)),
  next: Int,
  through: Int,
  reversed: List(Event(payload)),
) -> Result(List(Event(payload)), StoreError) {
  case next > through {
    True -> Ok(list.reverse(reversed))
    False ->
      case dict.get(by_position, next) {
        Error(Nil) -> Error(read_projection_failure)
        Ok(event) ->
          fetch_chunk(by_position, next + 1, through, [event, ..reversed])
      }
  }
}

pub fn projection_checkpoints(
  memory: Memory(payload),
) -> projection.Checkpoints {
  let Memory(server) = memory
  projection.Checkpoints(
    load: fn(name) {
      call(server, fn(state) {
        #(state, option.from_result(dict.get(state.checkpoints, name)))
      })
      |> result.replace_error(store.StoreFailure(store.LoadCheckpoint))
    },
    save: fn(name, position) {
      call(server, fn(state) {
        #(
          State(
            ..state,
            checkpoints: dict.insert(state.checkpoints, name, position),
          ),
          Nil,
        )
      })
      |> result.replace_error(store.StoreFailure(store.SaveCheckpoint))
    },
  )
}

/// Uses a lease on the store’s BEAM node for each named projector.
pub fn projection_backend(
  memory: Memory(payload),
) -> projection_runner.Backend(payload) {
  let Memory(server) = memory
  projection_runner.Backend(
    projection_reader(memory),
    projection_checkpoints(memory),
    fn(name) {
      use release <- result.map(acquire(server, name))
      projection_runner.Lease(release)
    },
  )
}

@external(erlang, "eventcore_memory_ffi", "acquire")
fn acquire(
  server: Server(state),
  name: String,
) -> Result(fn() -> Nil, projection_runner.LeadershipError)
