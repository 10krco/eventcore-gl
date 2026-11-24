# Filesystem transactions use an immutable journal

## Context

The filesystem adapter needs atomic writes spanning several streams, durable
reopen, and an audit of changed files and missing parents. Independently
written journals need a file layout that can be combined by copying immutable
transactions into a shared directory.

## Decision

Write each batch as a versioned Erlang term in a temporary file next to its
final SHA-256-named path. Publish it with one rename. Full sync flushes
the file before rename and the directory afterward. When opening a new
journal, it also flushes the parent entries for newly created directories. Callers provide codecs
for event payloads. The journal rebuilds its index on open and verifies every
transaction checksum before reading its contents.

One OTP server holds the util-linux `flock` advisory lock for the life of
an open store. The OS releases the lock when its holder exits. Repository
metadata keeps local locks and staging files out of Git.

## Consequences

The file format belongs to this adapter. Reopening requires a graph scan.
Damaged transactions are excluded from the reconstructed history and reported
by status. Transactions with missing parents remain readable and status names
the missing parents. A complete graph with incompatible stream versions
requires reconciliation before it can open. A directory sync error after rename returns an append
error while retaining the newly visible transaction in memory.
