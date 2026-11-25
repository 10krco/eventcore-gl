# ADR 0016. Build command declarations and guards with typed functions

EventCore 2.1.0 derives ordered, deduplicated stream declarations from
annotated Rust fields. Its requirement guard stops a handler with a business
rule rejection and evaluates the error expression only when the rule fails.

Gleam callers use `command.single` for one stream and `command.across` with a
primary stream and a list of other participants. Both take validated stream
identifiers. The primary argument guarantees a nonempty declaration, and
repeated identifiers retain their first occurrence. The existing explicit
declaration API continues to report repeated or empty declarations as errors.

Handlers compose `command.require` with `result.try`. The caller supplies a
function for the domain error, so typed values and formatted strings can both
be built lazily. The executor reports the failure through its existing command
rejection and skips the append.
