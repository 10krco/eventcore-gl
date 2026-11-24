# ADR 0010. Legacy projection runner

## Context

A live read model needs one owner per subscription, a place to resume after a
restart, and a policy for polling and handler failures. Applying an event and
saving legacy progress are distinct operations. A restart can replay an
event when its effect succeeds and its checkpoint write fails.

## Decision

Run a projector in the calling BEAM process with a backend reader,
checkpoint repository, and per-name lease. Acquire leadership before
loading progress. Release it whenever the run returns or raises. The memory
backend monitors the lease holder and drops its lease on process exit.
Lease releases carry an identity so a stale release cannot remove a newer
holder.

Batch mode consumes one page of up to 1,000 deliveries. Continuous mode
polls again with separate delays for delivered, empty, and failed polls.
Cancellation interrupts waits and is checked between deliveries. The handler
chooses whether to fail the run, skip the event, or retry it with bounded
backoff. Completed and skipped deliveries advance progress. After all
deliveries finish, a scanned-through cursor can also advance over unrelated
events. A checkpoint load error stops the runner. Save errors are logged and
processing continues.

## Consequences

Each backend controls its leadership scope and progress persistence. The
runner supports the retry and checkpoint semantics of the legacy Rust runner.
Applications can receive an event again after a checkpoint save failure.
