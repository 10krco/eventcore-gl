# ADR 0017. Define typed model components with opaque field values

The Rust modeling derives generate builders with required fields and typed
mapping targets. Gleam domain modules declare their components and field
occurrences with ordinary functions. Give each component its own witness type,
including components that share a domain value type. Each field carries its
component witness and field occurrence in its type. The checker uses a structured
field identifier containing the role, component name, and field name.

Domain modules define typed builders over opaque field values and can keep
their value constructors private. The builder extracts field values and wraps
the completed domain value. An input origin accepts raw data. State fields
accept typed constant, default, or absence recipes. Other field values come
from executable mappings. Semantic stream roles wrap validated stream IDs
with a distinct domain witness and expose the underlying ID for declaration.

Mappings read typed component values and produce a value for one exact target
field. Multiple source fields compose into a tuple for a pure or fallible
transform. Field and mapping names are retained alongside the typed API so a
checker can consume the same declarations.
