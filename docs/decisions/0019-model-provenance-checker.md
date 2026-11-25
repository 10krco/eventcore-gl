# ADR 0019. Check explicit registries of modeled declarations

Gleam domain modules assemble their checker registry explicitly from typed
fields, input origins, state seeds, executable mappings, and named
assumptions. A mapping contributes its source and target identities directly.
A previous-value source reads the modeled value provided by the caller and
marks the dependency as temporal in the same mapping.

The checker indexes registrations and walks field provenance in stable order.
It requires provenance for each non-root field, checks every producer and
source traversed outside a terminal boundary, and distinguishes ordinary
cycles from temporal recurrences with a seed. Assumptions require explicit opt-in and an accepted assumption changes
the report status. Reports retain diagnostic codes, subjects, guidance,
available traces, and optional source locations.

The Rust 2.1.0 checker treats roots and accepted assumptions as terminal
boundaries. It classifies a recurrence using the dependency that closes the
cycle during ordered traversal and accepts an all-current producer as a
temporal seed. The Gleam checker retains these rules. The release compatibility
tests characterize terminal boundaries and both two-field recurrence outcomes.
