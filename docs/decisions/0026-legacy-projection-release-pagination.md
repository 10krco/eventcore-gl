# ADR 0026. Reproduce release pagination for legacy projections

## Context

Legacy EventCore 2.1.0 readers default to the consumer's event kind.
They apply the page limit to matching rows, then omit rows that fail event
deserialization. The runner advances its checkpoint only on delivery.

## Decision

Provide an explicit release pagination filter. The caller supplies the
consumer's kind. A kind already set on the filter takes precedence.
The legacy scan counts matching rows before decoding, omits deserialization
failures and reports progress through the last delivery. An empty delivery
page preserves the input cursor. Storage and framing errors still surface.

## Consequences

The normal scan reports deserialization failures and can advance through
filtered events. The release filter reproduces candidate counting and
checkpoint behavior on the adapter's delivery sequence. A page consisting
entirely of incompatible rows stays at the current release cursor.
