import eventcore/command
import eventcore/memory
import eventcore/snapshot
import eventcore/store
import eventcore/stream
import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/result

type Notifications(value)

@external(erlang, "eventcore_command_test_ffi", "notifications")
fn notifications() -> Notifications(value)

@external(erlang, "eventcore_command_test_ffi", "record")
fn record(notifications: Notifications(value), value: value) -> Nil

@external(erlang, "eventcore_command_test_ffi", "drain")
fn drain(notifications: Notifications(value)) -> List(value)

pub fn declarations_validate_and_preserve_uniqueness_test() {
  let assert Ok(id) = stream.id("account")
  let assert Error(command.EmptyDeclarations) = command.declare([])
  let assert Error(command.DuplicateStream(duplicate)) =
    command.with_participant(command.single(id), id)
  assert duplicate == id
}

pub fn typed_participants_replay_once_in_first_seen_order_test() {
  let assert Ok(source) = stream.id("source")
  let assert Ok(target) = stream.id("target")
  let storage =
    store.Store(
      fn(id, _) { Ok(store.from_items([Ok(store.Event(id, "opened", Nil))])) },
      fn(writes) {
        assert dict.size(store.expected_versions(writes)) == 2
        Ok(Nil)
      },
    )
  let operation =
    command.Command(
      command.across(source, [source, target, target]),
      fn() { [] },
      fn(state, event) { [event.stream, ..state] },
      fn(_) { [] },
      fn(state) {
        assert list.reverse(state) == [source, target]
        Ok([])
      },
    )
  assert command.execute(storage, operation, command.default_policy())
    == Ok(command.Execution(1))
}

pub fn requirement_is_lazy_and_short_circuits_a_command_handler_test() {
  let assert Ok(id) = stream.id("account")
  let storage =
    store.Store(fn(_, _) { Ok(store.from_items([])) }, fn(_) {
      panic as "a rejected command must not append"
    })
  let operation =
    command.Command(
      command.single(id),
      fn() { 20 },
      fn(state, _) { state },
      fn(_) { [] },
      fn(balance) {
        use _ <- result.try(
          command.require(balance > 0, fn() {
            panic as "a satisfied requirement must not evaluate its error"
          }),
        )
        use _ <- result.try(command.require(balance >= 30, fn() { balance }))
        panic as "a failed requirement must stop the handler"
      },
    )
  assert command.execute(storage, operation, command.default_policy())
    == Error(command.Rejected(command.BusinessRule(20)))
}

pub fn discovers_participants_and_replays_after_conflicts_test() {
  let assert Ok(pid) = memory.start()
  let storage = memory.as_store(pid)
  let assert Ok(primary) = stream.id("primary")
  let assert Ok(initial_peer) = stream.id("initial-peer")
  let assert Ok(secondary) = stream.id("secondary")
  let assert Ok(tertiary) = stream.id("tertiary")
  let assert Ok(replacement) = stream.id("replacement")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let assert Ok(seed) = store.register(store.writes(), primary, zero)
  let assert Ok(seed) = store.append(seed, store.Event(primary, "opened", 100))
  let assert Ok(seed) = store.register(seed, secondary, zero)
  let assert Ok(seed) = store.append(seed, store.Event(secondary, "opened", 20))
  let assert Ok(seed) = store.register(seed, replacement, zero)
  let assert Ok(seed) =
    store.append(seed, store.Event(replacement, "opened", 25))
  let assert Ok(Nil) = storage.append_events(seed)

  let instrumented =
    store.Store(storage.read_after, fn(writes) {
      case dict.get(store.expected_versions(writes), primary) {
        Ok(version) if version == one -> {
          assert dict.has_key(store.expected_versions(writes), secondary)
          assert !dict.has_key(store.expected_versions(writes), replacement)
          let assert Ok(concurrent) =
            store.register(store.writes(), primary, one)
          let assert Ok(concurrent) =
            store.append(concurrent, store.Event(primary, "adjusted", 5))
          let assert Ok(Nil) = storage.append_events(concurrent)
          storage.append_events(writes)
        }
        _ -> {
          assert !dict.has_key(store.expected_versions(writes), secondary)
          assert dict.get(store.expected_versions(writes), replacement)
            == Ok(one)
          storage.append_events(writes)
        }
      }
    })

  let assert Ok(streams) = command.declare([primary, initial_peer])
  let transfer =
    command.Command(
      streams:,
      initial_state: fn() { [] },
      apply: fn(applied, event) { [event.payload, ..applied] },
      discover: fn(applied) {
        case applied {
          [20, ..] | [25, ..] -> [tertiary, primary]
          [100, ..] -> [secondary, initial_peer, secondary]
          [5, 100] -> [replacement]
          _ -> []
        }
      },
      handle: fn(applied) {
        case applied {
          [25, 5, 100] ->
            Ok([
              store.Event(primary, "debited", -30),
              store.Event(replacement, "credited", 30),
            ])
          [20, 100] -> Ok([store.Event(primary, "debited", -40)])
          _ -> Error(command.BusinessRule("unexpected reconstruction"))
        }
      },
    )

  let context =
    command.RetryContext([primary, initial_peer, secondary, tertiary], 1, 0)
  let assert Ok(policy) = command.retry_policy(1, command.Fixed(0))
  let retries = notifications()
  let traces = notifications()
  let policy =
    policy
    |> command.with_metrics(record(retries, _))
    |> command.with_trace(record(traces, _))

  assert command.execute(instrumented, transfer, policy)
    == Ok(command.Execution(2))
  assert drain(retries) == [context]
  assert drain(traces)
    == [
      command.ExecutionStarted,
      command.Retrying(context),
      command.CommandSucceeded(2),
    ]
  let assert Ok(events) = store.read_stream(storage, primary)
  assert store.collect(events)
    == Ok([
      store.Event(primary, "opened", 100),
      store.Event(primary, "adjusted", 5),
      store.Event(primary, "debited", -30),
    ])
  let assert Ok(events) = store.read_stream(storage, secondary)
  assert store.collect(events) == Ok([store.Event(secondary, "opened", 20)])
  let assert Ok(events) = store.read_stream(storage, replacement)
  assert store.collect(events)
    == Ok([
      store.Event(replacement, "opened", 25),
      store.Event(replacement, "credited", 30),
    ])
  assert memory.close(pid) == Ok(Nil)
}

pub fn retry_default_and_exhaustion_test() {
  let assert Ok(id) = stream.id("conflict")
  let zero = stream.initial_version
  let conflict = store.VersionConflict(id, zero, stream.next_version(zero))
  let storage =
    store.Store(fn(_, _) { Ok(store.from_items([])) }, fn(_) { Error(conflict) })
  let operation =
    command.Command(
      command.single(id),
      fn() { Nil },
      fn(_, _) { Nil },
      fn(_) { [] },
      fn(_) { Ok([store.Event(id, "added", Nil)]) },
    )
  let policy =
    command.default_policy()
    |> command.with_trace(fn(trace) {
      case trace {
        command.Retrying(command.RetryContext([owner], attempt, delay)) -> {
          assert owner == id
          let base = case attempt {
            1 -> 10
            2 -> 20
            3 -> 40
            4 -> 80
            _ -> panic as "unexpected retry"
          }
          assert delay >= base * 8 / 10 && delay <= base * 12 / 10
        }
        command.RetriesExhausted([owner], max_retries) -> {
          assert owner == id
          assert max_retries == 4
        }
        _ -> Nil
      }
    })
  assert command.execute(storage, operation, policy)
    == Error(command.ConcurrencyError(4))

  let assert Ok(zero_delay) = command.retry_policy(65, command.Exponential(0))
  let zero_delay = command.with_trace(zero_delay, fn(_) { Nil })
  assert command.execute(storage, operation, zero_delay)
    == Error(command.ConcurrencyError(65))
}

pub fn business_rejections_and_storage_errors_are_terminal_test() {
  let assert Ok(id) = stream.id("account")
  let assert Ok(other) = stream.id("other")
  let failure = store.StoreFailure(store.ReadStream)
  let storage =
    store.Store(fn(_, _) { Ok(store.from_items([])) }, fn(_) {
      panic as "the command should never reach append"
    })
  let operation =
    command.Command(
      command.single(id),
      fn() { Nil },
      fn(_, _) { Nil },
      fn(_) { [] },
      fn(_) { Error(command.BusinessRule("insufficient funds")) },
    )
  let policy = command.default_policy()
  assert command.execute(storage, operation, policy)
    == Error(command.Rejected(command.BusinessRule("insufficient funds")))

  let unregistered =
    command.Command(..operation, handle: fn(_) {
      Ok([store.Event(other, "debited", 1)])
    })
  assert command.execute(storage, unregistered, policy)
    == Error(command.StorageError(store.UndeclaredStream(other)))

  let unreadable =
    store.Store(..storage, read_after: fn(_, _) {
      Ok(store.from_items([Error(failure)]))
    })
  assert command.execute(unreadable, operation, policy)
    == Error(command.StorageError(failure))
}

pub fn returns_first_attempt_and_validates_retry_options_test() {
  let assert Ok(id) = stream.id("account")
  let assert Ok(pid) = memory.start()
  let storage = memory.as_store(pid)
  let opening = store.Event(id, "opened", 100)
  let open_account =
    command.Command(
      command.single(id),
      fn() { 0 },
      fn(total, event) { total + event.payload },
      fn(_) { [] },
      fn(total) {
        case total {
          0 -> Ok([opening])
          _ -> Error(command.InvalidState("account already opened"))
        }
      },
    )
  let policy = command.default_policy()
  assert command.execute(storage, open_account, policy)
    == Ok(command.Execution(1))
  assert command.execute(storage, open_account, policy)
    == Error(command.Rejected(command.InvalidState("account already opened")))
  let assert Ok(events) = store.read_stream(storage, id)
  assert store.collect(events) == Ok([opening])
  let assert Error(Nil) = command.retry_policy(-1, command.Fixed(0))
  let assert Error(Nil) = command.retry_policy(0, command.Exponential(-1))
  assert memory.close(pid) == Ok(Nil)
}

pub fn saved_command_state_replays_later_streams_after_an_earlier_tail_test() {
  let assert Ok(pid) = memory.start()
  let storage = memory.as_store(pid)
  let assert Ok(first) = stream.id("first")
  let assert Ok(later) = stream.id("later")
  let assert Ok(replacement) = stream.id("replacement")
  let zero = stream.initial_version
  let assert Ok(version_100) = stream.version(100)
  let assert Ok(seed) = store.register(store.writes(), first, zero)
  let assert Ok(seed) = store.register(seed, later, zero)
  let assert Ok(seed) = store.register(seed, replacement, zero)
  let assert Ok(seed) =
    list.try_fold(list.repeat(1, 100), seed, fn(writes, value) {
      store.append(writes, store.Event(first, "first", value))
    })
  let assert Ok(seed) = store.append(seed, store.Event(later, "later", 2))
  let assert Ok(seed) =
    store.append(seed, store.Event(replacement, "replacement", 4))
  let assert Ok(Nil) = storage.append_events(seed)

  let reads = notifications()
  let saves = notifications()
  let handled = notifications()
  let traced =
    store.Store(
      fn(id, version) {
        record(reads, #(id, version))
        storage.read_after(id, version)
      },
      storage.append_events,
    )
  let assert Ok(key) = snapshot.key("snapshot-fixture")
  let codec =
    snapshot.Codec(fn(state) { Ok(<<state:32>>) }, fn(bytes) {
      case bytes {
        <<state:32>> -> Ok(state)
        _ -> Error("invalid fixture state")
      }
    })
  let command =
    command.Command(
      streams: command.single(first),
      initial_state: fn() { 0 },
      apply: fn(state, event) {
        case event.kind {
          "first" -> state + event.payload
          "tail" -> state + event.payload
          _ -> state * 10 + event.payload
        }
      },
      discover: fn(state) {
        case state {
          100 -> [later]
          103 -> [replacement]
          _ -> []
        }
      },
      handle: fn(state) {
        record(handled, state)
        let events: Result(List(store.Event(Int)), command.Rejection(Nil)) =
          Ok([])
        events
      },
    )
  let repository = memory.snapshots(pid)
  let cached =
    snapshot.Repository(repository.load, fn(key, observed, saved) {
      record(saves, saved)
      repository.save(key, observed, saved)
    })
  let assert Ok(_) =
    command.execute_with_snapshot(
      traced,
      command,
      command.default_policy(),
      cached,
      key,
      codec,
    )
  assert drain(handled) == [1002]
  assert drain(reads) == [#(first, zero), #(later, zero)]
  let assert [_] = drain(saves)
  let assert Ok(_) =
    command.execute_with_snapshot(
      traced,
      command,
      command.default_policy(),
      cached,
      key,
      codec,
    )
  assert drain(handled) == [1002]
  assert drain(reads)
    == [#(first, version_100), #(later, stream.next_version(zero))]
  assert drain(saves) == []

  let assert Ok(tail) = store.register(store.writes(), first, version_100)
  let assert Ok(tail) = store.append(tail, store.Event(first, "tail", 3))
  let assert Ok(Nil) = storage.append_events(tail)
  let assert Ok(_) =
    command.execute_with_snapshot(
      traced,
      command,
      command.default_policy(),
      cached,
      key,
      codec,
    )
  assert drain(handled) == [1034]
  assert drain(reads) == [#(first, version_100), #(replacement, zero)]
  let assert [snapshot.Snapshot([_, checkpoint])] = drain(saves)
  assert checkpoint.stream == replacement
  let assert Ok(_) =
    command.execute_with_snapshot(
      traced,
      command,
      command.default_policy(),
      cached,
      key,
      codec,
    )
  assert drain(handled) == [1034]
  assert drain(reads)
    == [
      #(first, stream.next_version(version_100)),
      #(replacement, stream.next_version(zero)),
    ]
  assert drain(saves) == []
  let assert Ok(Nil) = memory.close(pid)
}

pub fn memory_snapshot_rejects_a_regressing_version_vector_test() {
  let assert Ok(pid) = memory.start()
  let snapshots = memory.snapshots(pid)
  let assert Ok(key) = snapshot.key("  stable-key ")
  assert snapshot.name(key) == "stable-key"
  let assert Ok(first) = stream.id("first")
  let assert Ok(second) = stream.id("second")
  let zero = stream.initial_version
  let one = stream.next_version(zero)
  let two = stream.next_version(one)
  let original =
    snapshot.Snapshot([
      snapshot.Checkpoint(first, one, <<1>>),
      snapshot.Checkpoint(second, one, <<2>>),
    ])
  let regressing =
    snapshot.Snapshot([
      snapshot.Checkpoint(first, two, <<3>>),
      snapshot.Checkpoint(second, zero, <<4>>),
    ])
  let covering =
    snapshot.Snapshot([
      snapshot.Checkpoint(first, two, <<5>>),
      snapshot.Checkpoint(second, one, <<6>>),
    ])
  assert snapshots.load(key) == Ok(None)
  assert snapshots.save(key, None, original) == Ok(Nil)
  assert snapshots.save(key, Some(original), regressing) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(original))
  assert snapshots.save(key, Some(original), covering) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(covering))

  let without_second =
    snapshot.Snapshot([snapshot.Checkpoint(first, two, <<7>>)])
  assert snapshots.save(key, Some(original), without_second) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(covering))
  assert snapshots.save(key, Some(covering), without_second) == Ok(Nil)
  assert snapshots.load(key) == Ok(Some(without_second))
  let assert Ok(Nil) = memory.close(pid)
}

pub fn snapshot_refresh_threshold_applies_before_a_command_rejection_test() {
  let assert Ok(pid) = memory.start()
  let storage = memory.as_store(pid)
  let snapshots = memory.snapshots(pid)
  let assert Ok(key) = snapshot.key("threshold")
  let assert Ok(id) = stream.id("threshold")
  let assert Ok(version_99) = stream.version(99)
  let assert Ok(version_100) = stream.version(100)
  let assert Ok(seed) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(seed) =
    list.try_fold(list.repeat(Nil, 99), seed, fn(writes, _) {
      store.append(writes, store.Event(id, "added", Nil))
    })
  assert storage.append_events(seed) == Ok(Nil)
  let codec =
    snapshot.Codec(fn(state) { Ok(<<state:32>>) }, fn(bytes) {
      case bytes {
        <<state:32>> -> Ok(state)
        _ -> Error("invalid count")
      }
    })
  let reject =
    command.Command(
      streams: command.single(id),
      initial_state: fn() { 0 },
      apply: fn(count, _) { count + 1 },
      discover: fn(_) { [] },
      handle: fn(_) { Error(command.BusinessRule("declined")) },
    )
  let run = fn() {
    command.execute_with_snapshot(
      storage,
      reject,
      command.default_policy(),
      snapshots,
      key,
      codec,
    )
  }
  let fail_encode =
    snapshot.Codec(
      fn(_) { Error("encoder called below threshold") },
      codec.decode,
    )
  assert command.execute_with_snapshot(
      storage,
      reject,
      command.default_policy(),
      snapshots,
      key,
      fail_encode,
    )
    == Error(command.Rejected(command.BusinessRule("declined")))
  assert snapshots.load(key) == Ok(None)

  let assert Ok(tail) = store.register(store.writes(), id, version_99)
  let assert Ok(tail) = store.append(tail, store.Event(id, "added", Nil))
  assert storage.append_events(tail) == Ok(Nil)
  assert run() == Error(command.Rejected(command.BusinessRule("declined")))
  assert snapshots.load(key)
    == Ok(
      Some(
        snapshot.Snapshot([
          snapshot.Checkpoint(id, version_100, <<100:32>>),
        ]),
      ),
    )
  assert memory.close(pid) == Ok(Nil)
}
