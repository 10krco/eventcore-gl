# SQLite owns atomic appends and encrypted database files

## Context

The SQLite adapter needs to enforce all registered versions across independent
connections. It also needs open and migration operations for database files,
in-memory databases, and connections supplied by callers. The EventCore 2.1.0
SQLite adapter can encrypt event and checkpoint databases with SQLCipher.

## Decision

Own each connection in one OTP server. Acquire SQLite's write reservation with
`BEGIN IMMEDIATE` before reading any expected versions. Insert the entire batch
and commit in that transaction. Roll back on a conflict or a failed statement.
A failed rollback retires the connection.

Provide explicit migrations for event and checkpoint storage. Bind event data
as query parameters. Store caller-encoded payloads as a 64-bit bit count
followed by the padded bytes. Validate the count and padding when reading.
Use an integer sequence for database-wide event
order and a unique stream and version pair for stream order.

Use the Erlang SQLite driver in system-library mode in the Nix environment.
Its link resolves to the pinned SQLCipher library. Apply the escaped key on
file opens before reading the schema or enabling WAL. Require the effective
journal mode to be WAL for file opens and memory for in-memory opens. Verify
cipher support and the key before returning a configured encrypted connection. Take ownership
of supplied connections with their existing SQLite settings.

## Consequences

SQLite serializes writers, including independent connections to the same file.
A busy or failed statement can surface as a store failure. Callers migrate a
database before using it. The SQL schema and on-disk encoding belong to this
port. Builds with plain SQLite can use unencrypted databases and return an
error for encrypted opens.
