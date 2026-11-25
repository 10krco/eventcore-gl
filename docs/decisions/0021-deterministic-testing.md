# ADR 0021. Run test scenarios through commands and replay

Expose scenario, event collection, and fault injection helpers under
`eventcore/testing`. A scenario seeds a private memory store and runs a
command through the ordinary execution path. It collects the resulting
history through one legacy projection batch. Given steps can append at the
current stream version, and observations preserve typed command failures.

Fault wrappers decorate any store. An atomic append counter lets callers
share one deterministic conflict budget. Configurable chaos uses a seed
and atomic call numbers to reproduce outcomes for the same call order.
Reads and appends retain their operation names and a selected append
conflict takes precedence over a selected store failure.
