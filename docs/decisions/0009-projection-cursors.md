# ADR 0009. Backend-local projection cursors

## Context

A projector reads across streams and resumes independently of command replay.
Filtering must precede the page limit so unrelated events never fill a page.
The cursor records the order in which the backend delivers events to a
projection. A stream version only records the order within one stream.

## Decision

Define a shared projection reader over a filter and page with an opaque,
exclusive cursor. Keep checkpoint persistence behind a separate interface.
Each backend assigns cursors according to its own delivery sequence. Treat
saved cursors as meaningful within that sequence.

The in-memory store records global append order in the same OTP state
transition as its stream histories. Reads walk that sequence after the
cursor, apply the stream and event-type filter, and stop at the requested
number of matches. Checkpoints share the lifetime of the in-memory store.

Filter patterns operate on Unicode code points. Literal prefixes reject glob
syntax, and validated globs match the entire stream name. A suffix matcher
bounds glob evaluation by the product of the pattern and stream-name lengths.

## Consequences

A consumer can resume from a stored position without re-reading the
previously applied event. Filtering can leave gaps in the returned positions.
Consumers use the returned cursor rather than deriving it from a page length.

The legacy cursor model can serve backends with different local delivery
orders. PostgreSQL's lossless delivery envelope and transactional projection
progress use a separate boundary.
