# Run and recover a projection

A projection derives a read model from committed events. Give each independent read model its own stable projector name and keep its checkpoint with the read model for recovery. Positions are ordered within one source and cursor format. Preserve the source, filter, and progress store together when restarting.

| Runner | Choose it when | Recovery contract |
| --- | --- | --- |
| [Portable runner](eventcore/projection_runner.html) | You use memory, filesystem, SQLite, or PostgreSQL and can make the sink idempotent | The callback and checkpoint run separately. The same event can be delivered again after interruption or a failed checkpoint write. |
| [Transactional PostgreSQL runner](eventcore/postgres_projection.html) | The read model lives in PostgreSQL and its changes must commit with progress | Writes through the supplied transaction and the progress update commit together. External effects have a separate recovery contract. |

## Portable runner

Every event-store adapter exposes a `projection_backend`. It bundles a reader, a checkpoint store, and a named lease. You can also compose those components explicitly. Keep the coordination scope at least as broad as the processes that can update the same checkpoint. In-memory and SQLite leases are local to the coordinated BEAM handle. For PostgreSQL, use the same checkpoint pool for the coordinator and checkpoint adapter so checkpoint writes use the lock-owning session. Keep the matching coordinator and fenced checkpoint components together on a filesystem deployment.

This batch example appends one event and derives its total. It is a complete `main` for a project that depends on EventCore.

```gleam
import eventcore/memory
import eventcore/projection
import eventcore/projection_runner as runner
import eventcore/store
import eventcore/stream
import gleam/int
import gleam/io

pub fn main() {
  let assert Ok(events) = memory.start()
  let assert Ok(id) = stream.id("counter/main")
  let assert Ok(writes) =
    store.register(store.writes(), id, stream.initial_version)
  let assert Ok(writes) = store.append(writes, store.Event(id, "added", 3))
  let assert Ok(Nil) = memory.as_store(events).append_events(writes)

  let projector =
    runner.Projector(
      name: "counter-v1",
      filter: projection.with_event_type(projection.all(), "added"),
      initial: 0,
      apply: fn(total, delivery) { #(total + delivery.event.payload, Ok(Nil)) },
      on_error: fn(_) { runner.Fatal },
    )
  let assert Ok(total) =
    runner.run(
      memory.projection_backend(events),
      projector,
      runner.default_config,
      runner.uninterrupted(),
    )
  io.println(int.to_string(total))
  let assert Ok(Nil) = memory.close(events)
}
```

The example prints `3`. The portable runner’s default batch mode processes one page per run. For a durable read model, load its current state before a resumed run or keep the state in a durable sink. The runner persists the cursor but starts each run with the `initial` you supply. Handle duplicate deliveries by persisting the source and position alongside the sink update, or by using an equivalent application idempotency key.

Use `runner.Config(..runner.default_config, mode: runner.Continuous)` to poll continuously. For a supervised worker that can stop cleanly, create a stop subject in the worker, pass `runner.cancellable(stop)` to `run`, and send `Nil` on that subject from another process. Cancellation is checked between deliveries and during waits. An application callback runs to completion.

On a callback failure, `Retry` reapplies the buffered delivery with capped backoff and a retry limit. Return the old state on failure if that state must be used on retry. `Skip` advances progress, so choose it only after deciding the read model can omit that event. `Fatal` ends the run. Poll retries are separate from event retries. Ordinary checkpoint write failures are logged and the run can continue. Monitor `checkpoint_save_failed` and make updates safe to redeliver. Losing a checked lease stops the runner.

The default portable cursor is a backend's numeric scan position. A filtered page may advance its cursor beyond its last matching event after all deliveries are handled. SQLite and PostgreSQL also have a UUID-ordered compatibility profile using `projection_release_backend`. For SQLite, migrate the events with `migrate_release_delivery`. For PostgreSQL, use `migrate_delivery`. In either backend, run `migrate_release_checkpoints` on the checkpoint store. The compatibility readers apply the page limit before decoding. An entirely undecodable page preserves the input cursor, so incompatible persisted events can stall the runner. Treat switching profiles as a rebuild, with a fresh checkpoint name or a coordinated reset.

## Transactional PostgreSQL projections

Prepare the source with `postgres.migrate` and then `postgres.migrate_delivery`. Prepare the destination with `postgres_projection.migrate` and apply your application's read-model schema there. The source and destination may use separate databases. Allow the destination pool to hold a session for each running projector and reserve additional connections for concurrent work.

Choose a source ID that identifies the event log and a selection ID that identifies its stream and event-kind filter. `projection_delivery.selection` requires at least one event kind and rejects duplicates. Keep the ID unchanged when restarting with the same selection. Use a new name, a new selection ID, and separate destination data when changing the selection and building the new read model alongside the old one. The runner verifies stored source and selection IDs and cannot infer that the filter changed when its ID stays the same.

Create this example table in the destination database. The table belongs to the application.

```sql
CREATE TABLE counter_total (
  singleton boolean PRIMARY KEY CHECK (singleton),
  total bigint NOT NULL
);

INSERT INTO counter_total (singleton, total) VALUES (true, 0);
```

This batch runner expects an `added` event whose stored payload is a single unsigned byte. The application decodes the original stored bytes, then performs its write on the transaction supplied by EventCore.

```gleam
import eventcore/postgres
import eventcore/postgres_projection as transactional
import eventcore/projection
import eventcore/projection_delivery as delivery
import gleam/result
import pog

pub type CounterError {
  InvalidAmount
  WriteFailed(transactional.TransactionQueryError)
  MissingCounter
}

pub fn catch_up(
  events: postgres.EventStore(BitArray),
  destination_pool: pog.Connection,
) -> Result(
  transactional.Outcome,
  transactional.RunError(postgres.DeliveryError, CounterError),
) {
  let assert Ok(source_id) = delivery.source_id("counter-event-log")
  let assert Ok(selection_id) = delivery.selection_id("counter-added-v1")
  let assert Ok(name) = delivery.projector_name("counter-total-v1")
  let assert Ok(kind) = delivery.event_type("added")
  let assert Ok(filter) = projection.prefix("counter/")
  let assert Ok(selection) = delivery.selection(selection_id, filter, [kind])
  let projector =
    transactional.Projector(
      name:,
      decode: fn(envelope) {
        case envelope.payload {
          <<amount>> -> Ok(amount)
          _ -> Error(InvalidAmount)
        }
      },
      apply: fn(amount, _, tx) {
        use returned <- result.try(
          pog.query(
            "UPDATE counter_total SET total = total + $1 WHERE singleton",
          )
          |> pog.parameter(pog.int(amount))
          |> transactional.execute(tx, _)
          |> result.map_error(WriteFailed),
        )
        case returned.count {
          1 -> Ok(transactional.no_after_commit)
          _ -> Error(MissingCounter)
        }
      },
      on_error: fn(_) { transactional.Fatal },
    )
  transactional.run(
    transactional.destination(destination_pool),
    projector,
    postgres.projection_source(events, source_id),
    transactional.config(selection),
  )
}
```

Batch mode captures a committed upper position and processes the selection through that position. Run again to consume later appends. `transactional.Continuous(control)` captures another upper position after each polling wait. Build a `transactional.RetryPolicy` with `retry_policy` to allow retries, and return `Retry` from the application failure handler when the error is retryable. The default has zero application retries. `Stop` leaves the event pending. `Skip` commits progress without the failed write. `Fatal` returns an error. A decode failure stops the run before the application callback.

Use `transactional.execute` only while the callback is running. Keep transaction and advisory lock control with the runner. For effects such as publishing a message, write an outbox record in the same transaction and deliver from the outbox separately when the effect must survive a crash. A returned after-commit hook runs after PostgreSQL acknowledges the commit. A crash can interrupt the hook, and a hook error does not roll back the progress or read model.

## Rebuild a projection

1. Identify the projector name, source, selection, and destination before touching progress. Stop or coordinate competing workers. If readers must keep serving during a rebuild, use a new projector name and destination read model and switch readers after catch-up.
2. For a portable runner, clear or replace the derived data and reset the matching checkpoint while the worker is stopped. Backends with separate checkpoint APIs expose a reset operation. A fresh unique projector name starts with no checkpoint. For a filesystem journal, imported history can change canonical ordering. The portable filesystem projection reader uses replica-local ingestion order even when replaying from the start. If the read model needs canonical order, rebuild from the canonical history and use [`filesystem.event_prefix_token`](eventcore/filesystem.html#event_prefix_token) to detect changes to the consumed prefix after import.
3. For PostgreSQL transactional projections, use [`reset_and_replay`](eventcore/postgres_projection.html#reset_and_replay) in batch mode, or [`reset`](eventcore/postgres_projection.html#reset) if replay will run separately. Use the supplied transaction in the reset callback to clear only this projector's derived data. Cleanup and progress deletion commit together. Reset-and-replay keeps the same leadership across both phases. If replay fails after a successful reset, the new read model may be partial. Correct the failure, then resume with the same identities.
4. Verify the rebuilt model against the source and check observed progress before sending readers to it. Keep the original event log available for another rebuild.

### Handle an interrupted transactional run

| Result | What to do |
| --- | --- |
| `Leadership` or `LeadershipLost` | Check for a live runner and for destination pool or database failures. Resume under the same name after the old session is gone. |
| `SourceMismatch` or `SelectionMismatch` | Correct the configuration for the existing read model, or build a new one under a new name and identities. |
| `DecodeFailed` or `RetryExhausted` | Diagnose the event at the reported position. Fix decoding or the application failure, then resume from persisted progress. |
| `CommitRolledBack` | Diagnose the database error. The failed transaction did not persist its changes or progress. |
| `CommitIndeterminate` or `ResetCommitIndeterminate` | Reconnect and check both stored progress and derived data before taking manual recovery action. A lost commit acknowledgement can mean PostgreSQL committed. Resume with the same identities after checking; the runner reloads committed progress. |
| `AfterCommitFailed` | Recover the external effect separately with its idempotency key or outbox. The database write and progress already committed, so a resumed run moves past this event. |
