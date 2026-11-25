# ADR 0019. Check explicit registries of modeled declarations

Gleam domain modules assemble their checker registry explicitly from typed
fields, input origins, state seeds, executable mappings, and named
assumptions. A mapping contributes its source and target identities directly.
A previous-value source reads the modeled value provided by the caller and
marks the dependency as temporal in the same mapping.

The checker indexes registrations and walks field provenance in stable order.
It requires a producer for each non-root field, validates every source and
alternative, and distinguishes ordinary cycles from temporal recurrences with
a seed. Assumptions require explicit opt-in and an accepted assumption changes
the report status. Reports retain diagnostic codes, subjects, guidance,
available traces, and optional source locations.
