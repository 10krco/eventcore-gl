//// Typed components and mappings for experimental domain modeling.

import eventcore/stream
import gleam/list
import gleam/option.{type Option, None}
import gleam/result

pub type Input

pub type Command

pub type Event

pub type State

pub type ReadModel

pub type Effect

pub type Output

pub opaque type Component(role, value) {
  Component(name: String)
}

pub fn input(name: String) -> Component(Input, value) {
  Component(name)
}

pub fn command(name: String) -> Component(Command, value) {
  Component(name)
}

pub fn event(name: String) -> Component(Event, value) {
  Component(name)
}

pub fn state(name: String) -> Component(State, value) {
  Component(name)
}

pub fn read_model(name: String) -> Component(ReadModel, value) {
  Component(name)
}

pub fn effect(name: String) -> Component(Effect, value) {
  Component(name)
}

pub fn output(name: String) -> Component(Output, value) {
  Component(name)
}

pub opaque type Modeled(role, value) {
  Modeled(inner: value)
}

/// Complete a component using its domain constructor and typed field values.
/// Domain modules can keep their underlying constructors private.
pub fn build(
  _component: Component(role, value),
  value: value,
) -> Modeled(role, value) {
  Modeled(value)
}

pub fn value(modeled: Modeled(role, value)) -> value {
  modeled.inner
}

pub opaque type Field(role, owner, occurrence, value) {
  Field(name: String, get: fn(owner) -> value)
}

pub opaque type FieldValue(role, owner, occurrence, value) {
  FieldValue(inner: value)
}

/// The occurrence witness distinguishes fields with the same underlying type.
pub fn field(
  component: Component(role, owner),
  name: String,
  _occurrence: occurrence,
  get: fn(owner) -> value,
) -> Field(role, owner, occurrence, value) {
  Field(component.name <> "." <> name, get)
}

pub fn field_name(field: Field(role, owner, occurrence, value)) -> String {
  field.name
}

pub fn field_value(
  field_value: FieldValue(role, owner, occurrence, value),
) -> value {
  field_value.inner
}

pub opaque type Origin(owner, occurrence, value) {
  Origin(field: Field(Input, owner, occurrence, value))
}

pub fn origin(
  component: Component(Input, owner),
  name: String,
  occurrence: occurrence,
  get: fn(owner) -> value,
) -> Origin(owner, occurrence, value) {
  Origin(field(component, name, occurrence, get))
}

pub fn origin_field(
  origin: Origin(owner, occurrence, value),
) -> Field(Input, owner, occurrence, value) {
  origin.field
}

pub fn supply(
  _origin: Origin(owner, occurrence, value),
  value: value,
) -> FieldValue(Input, owner, occurrence, value) {
  FieldValue(value)
}

pub fn constant(
  _field: Field(State, owner, occurrence, value),
  value: value,
) -> FieldValue(State, owner, occurrence, value) {
  FieldValue(value)
}

pub fn default(
  field: Field(State, owner, occurrence, value),
  make: fn() -> value,
) -> FieldValue(State, owner, occurrence, value) {
  constant(field, make())
}

pub fn absence(
  field: Field(State, owner, occurrence, Option(value)),
) -> FieldValue(State, owner, occurrence, Option(value)) {
  constant(field, None)
}

pub opaque type Source(input, value) {
  Source(read: fn(input) -> value, fields: List(String))
}

pub fn source(
  field: Field(role, owner, occurrence, value),
) -> Source(Modeled(role, owner), value) {
  Source(fn(input) { field.get(value(input)) }, [field.name])
}

pub fn sources(
  first: Source(input, left),
  second: Source(input, right),
) -> Source(input, #(left, right)) {
  Source(
    fn(input) { #(first.read(input), second.read(input)) },
    list.append(first.fields, second.fields),
  )
}

/// Combine fields from distinct components for a cross-component mapping.
pub fn paired_sources(
  first: Source(left_input, left),
  second: Source(right_input, right),
) -> Source(#(left_input, right_input), #(left, right)) {
  Source(
    fn(input: #(left_input, right_input)) {
      #(first.read(input.0), second.read(input.1))
    },
    list.append(first.fields, second.fields),
  )
}

pub opaque type Mapping(input, output) {
  Mapping(
    name: String,
    source_names: List(String),
    target_name: String,
    apply: fn(input) -> output,
  )
}

pub fn try_mapping(
  name: String,
  source: Source(input, from),
  target: Field(role, owner, occurrence, to),
  transform: fn(from) -> Result(to, error),
) -> Mapping(input, Result(FieldValue(role, owner, occurrence, to), error)) {
  Mapping(name, source.fields, target.name, fn(input) {
    transform(source.read(input)) |> result.map(FieldValue)
  })
}

pub fn mapping(
  name: String,
  source: Source(input, from),
  target: Field(role, owner, occurrence, to),
  transform: fn(from) -> to,
) -> Mapping(input, FieldValue(role, owner, occurrence, to)) {
  Mapping(name, source.fields, target.name, fn(input) {
    FieldValue(transform(source.read(input)))
  })
}

pub fn apply(mapping: Mapping(input, output), input: input) -> output {
  mapping.apply(input)
}

pub fn mapping_fields(
  mapping: Mapping(input, output),
) -> #(String, List(String), String) {
  #(mapping.name, mapping.source_names, mapping.target_name)
}

pub opaque type StreamIdentity(role) {
  StreamIdentity(id: stream.Id)
}

pub fn stream_identity(_role: role, id: stream.Id) -> StreamIdentity(role) {
  StreamIdentity(id)
}

pub fn stream_id(identity: StreamIdentity(role)) -> stream.Id {
  identity.id
}
