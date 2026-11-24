# Command snapshots follow replay order

## Context

A command can discover more streams while replaying events. State that was
derived after processing a later stream can change when an earlier stream
advances. Filesystem reconciliation can also change the canonical event
prefix underlying a snapshot.

## Decision

Store an ordered checkpoint for each participating stream. Each checkpoint
contains its version and the accumulated state encoded by the command. The
command supplies a key tied to its replay logic and state schema. Validate
the recorded participant order before resuming replay. Start with a full replay
and save after 100 reconstructed events. Refresh a saved snapshot when any
participant advances, rebuilding subsequent checkpoints from the changed state.

Save a candidate atomically if it covers the previous version vector. Memory
keeps the snapshots with the event store. SQLite keeps them in the event
database. The filesystem journal atomically publishes them as local files
tied to a canonical event prefix. A filesystem command view pins the replay
revision across reads, snapshot operations and appends.

## Consequences

Commands can restore accumulated state after reopening a durable store. A
snapshot contains one encoded state per participant. A filesystem snapshot
whose prefix changed is skipped, so the command can rebuild it from the
new journal history.
