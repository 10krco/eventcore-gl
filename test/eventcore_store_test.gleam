import eventcore/store
import eventcore/stream
import gleam/string

pub fn validate_stream_ids_test() {
  let assert Ok(id) = stream.id("  account/one \n")
  assert stream.name(id) == "account/one"
  assert stream.id(" \n") == Error(stream.EmptyId)
  assert stream.id("account/*") == Error(stream.ReservedGlobCharacter)
  let composed = "e\u{0301}"
  let assert Ok(_) = stream.id(string.repeat(composed, 127))
  assert stream.id(string.repeat(composed, 128)) == Error(stream.IdTooLong)
  assert stream.version(-1) == Error(Nil)
}

pub fn require_consistent_expected_versions_test() {
  let assert Ok(id) = stream.id("account/one")
  let version = stream.initial_version
  let event = store.Event(id, "opened", 10)
  assert store.append(store.writes(), event)
    == Error(store.UndeclaredStream(id))
  let assert Ok(writes) = store.register(store.writes(), id, version)
  assert store.register(writes, id, version) == Ok(writes)
  let newer = stream.next_version(version)
  assert store.register(writes, id, newer)
    == Error(store.ConflictingExpectedVersions(id, version, newer))
  let assert Ok(writes) = store.append(writes, event)
  let changed = store.Event(id, "changed", 20)
  let assert Ok(writes) = store.append(writes, changed)
  assert store.entries(writes) == [event, changed]
}

pub fn collect_stops_at_first_error_test() {
  let assert Ok(id) = stream.id("account/one")
  let event = store.Event(id, "opened", 10)
  let failure = store.DeserializationFailed(id, "invalid payload")
  assert store.collect(
      store.from_items([
        Ok(event),
        Error(failure),
        Error(store.StoreFailure(store.ReadStream)),
      ]),
    )
    == Error(failure)
}
