# ADR 0020. Publish reusable backend contract checks

Expose assertion groups under `eventcore/testing` so adapter authors can run
the same scenarios as the built-in stores. The checks use public storage,
snapshot, reader, checkpoint, and coordinator interfaces. Each group owns
reserved names in a fresh fixture, and checks for unsupported capabilities
can be omitted independently.

Strict delivery checks accept a delivery source with append boundaries
established by the fixture. Transactional projection checks use the public
PostgreSQL runner and fixture callbacks for application writes and independent
durable observations. PostgreSQL fixtures keep server fault injection and
database setup at the test boundary.
