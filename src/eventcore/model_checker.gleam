//// Check information provenance across executable model declarations.
////
//// **Experimental.** This module is outside the EventCore 1.x compatibility
//// guarantee. Its types and functions may change in a minor release.

import eventcore/model.{type Dependency, type FieldId, Current, Previous}
import gleam/bool
import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/set.{type Set}
import gleam/string

pub opaque type Descriptor {
  Field(id: FieldId, root: Bool, location: Option(String))
  Mapping(
    name: String,
    sources: List(Dependency),
    target: FieldId,
    location: Option(String),
  )
  Assumption(name: String, target: FieldId, location: Option(String))
}

pub fn field(field: model.Field(role, owner, occurrence, value)) -> Descriptor {
  Field(model.field_id(field), False, None)
}

pub fn origin(
  origin: model.Origin(witness, owner, occurrence, value),
) -> Descriptor {
  Field(model.field_id(model.origin_field(origin)), True, None)
}

pub fn state_seed(
  field: model.Field(model.State(witness), owner, occurrence, value),
) -> Descriptor {
  Field(model.field_id(field), True, None)
}

pub fn mapping(mapping: model.Mapping(input, output)) -> Descriptor {
  let #(name, sources, target) = model.mapping_dependencies(mapping)
  Mapping(name, sources, target, None)
}

pub fn assumption(
  name: String,
  target: model.Field(role, owner, occurrence, value),
) -> Descriptor {
  Assumption(name, model.field_id(target), None)
}

pub fn at(descriptor: Descriptor, location: String) -> Descriptor {
  case descriptor {
    Field(id, root, _) -> Field(id, root, Some(location))
    Mapping(name, sources, target, _) ->
      Mapping(name, sources, target, Some(location))
    Assumption(name, target, _) -> Assumption(name, target, Some(location))
  }
}

pub opaque type Options {
  Options(all_assumptions: Bool, names: Set(String))
}

pub fn strict() -> Options {
  Options(False, set.new())
}

pub fn allow_assumptions(options: Options) -> Options {
  Options(..options, all_assumptions: True)
}

pub fn allow_assumption(options: Options, name: String) -> Options {
  Options(..options, names: set.insert(options.names, name))
}

pub type Status {
  Verified
  Assumed
}

pub type Report {
  Report(status: Status, warnings: List(Diagnostic))
}

pub type CheckError {
  Incomplete(diagnostics: List(Diagnostic))
}

pub type Diagnostic {
  Diagnostic(
    code: String,
    subject: String,
    message: String,
    help: String,
    trace: List(String),
    location: Option(String),
  )
}

pub fn render(diagnostic: Diagnostic) -> String {
  let trace = case diagnostic.trace {
    [] -> ""
    path -> "\ntrace: " <> string.join(path, " -> ")
  }
  let location = case diagnostic.location {
    None -> ""
    Some(location) -> "\nlocation: " <> location
  }
  diagnostic.code
  <> ": "
  <> diagnostic.subject
  <> "\n  "
  <> diagnostic.message
  <> "\nhelp: "
  <> diagnostic.help
  <> trace
  <> location
}

type Graph {
  Graph(
    fields: Dict(FieldId, Descriptor),
    recipes: Dict(FieldId, List(Descriptor)),
    assumptions: Dict(FieldId, List(Descriptor)),
    options: Options,
  )
}

type DescriptorId {
  FieldRegistration(FieldId)
  NamedRegistration(String)
}

type Evaluation {
  Evaluation(
    visited: Set(FieldId),
    active: Set(FieldId),
    assumed: Bool,
    errors: List(Diagnostic),
  )
}

pub fn check(descriptors: List(Descriptor)) -> Result(Report, CheckError) {
  check_with(descriptors, strict())
}

pub fn check_with(
  descriptors: List(Descriptor),
  options: Options,
) -> Result(Report, CheckError) {
  case descriptors {
    [] ->
      Error(
        Incomplete([
          Diagnostic(
            "ECM001",
            "registry",
            "no modeled descriptors were supplied for checking",
            "register modeled components and executable mappings",
            [],
            None,
          ),
        ]),
      )
    descriptors -> evaluate(descriptors, options)
  }
}

fn evaluate(
  descriptors: List(Descriptor),
  options: Options,
) -> Result(Report, CheckError) {
  let descriptors = list.sort(descriptors, compare_descriptors)
  let #(graph, _, errors) =
    list.fold(
      descriptors,
      #(Graph(dict.new(), dict.new(), dict.new(), options), set.new(), []),
      register,
    )
  let graph =
    Graph(
      ..graph,
      recipes: dict.map_values(graph.recipes, fn(_, values) {
        list.reverse(values)
      }),
      assumptions: dict.map_values(graph.assumptions, fn(_, values) {
        list.reverse(values)
      }),
    )
  let errors =
    list.fold(descriptors, errors, fn(errors, descriptor) {
      case descriptor {
        Field(..) -> errors
        Mapping(target: target, ..) | Assumption(target: target, ..) ->
          case dict.has_key(graph.fields, target) {
            True -> errors
            False -> [
              Diagnostic(
                "ECM005",
                subject(descriptor),
                "registration target `"
                  <> display_field(target)
                  <> "` is not a modeled field",
                "register the target field or correct the target path",
                [],
                location(descriptor),
              ),
              ..errors
            ]
          }
      }
    })
  let fields =
    graph.fields
    |> dict.to_list
    |> list.sort(fn(a, b) { compare_descriptors(a.1, b.1) })
  let evaluation =
    list.fold(
      fields,
      Evaluation(set.new(), set.new(), False, errors),
      fn(evaluation, entry) { visit(entry.0, graph, [], evaluation) },
    )
  case evaluation.errors {
    [] -> {
      let status = case evaluation.assumed {
        True -> Assumed
        False -> Verified
      }
      Ok(Report(status, warnings(graph, fields)))
    }
    errors -> Error(Incomplete(sort_diagnostics(errors)))
  }
}

fn register(
  accumulator: #(Graph, Set(DescriptorId), List(Diagnostic)),
  descriptor: Descriptor,
) -> #(Graph, Set(DescriptorId), List(Diagnostic)) {
  let #(graph, seen, errors) = accumulator
  let id = case descriptor {
    Field(id: id, ..) -> FieldRegistration(id)
    Mapping(name: name, ..) | Assumption(name: name, ..) ->
      NamedRegistration(name)
  }
  let errors = case set.contains(seen, id) {
    True -> [
      Diagnostic(
        "ECM002",
        subject(descriptor),
        "duplicate modeled descriptor registration",
        "give each field or mapping a unique identifier",
        [],
        location(descriptor),
      ),
      ..errors
    ]
    False -> errors
  }
  let graph = case descriptor {
    Field(id: id, ..) ->
      Graph(..graph, fields: dict.insert(graph.fields, id, descriptor))
    Mapping(target: target, ..) ->
      Graph(..graph, recipes: prepend(graph.recipes, target, descriptor))
    Assumption(target: target, ..) ->
      Graph(
        ..graph,
        assumptions: prepend(graph.assumptions, target, descriptor),
      )
  }
  #(graph, set.insert(seen, id), errors)
}

fn prepend(
  dict: Dict(key, List(value)),
  key: key,
  value: value,
) -> Dict(key, List(value)) {
  let existing = lookup(dict, key)
  dict.insert(dict, key, [value, ..existing])
}

fn lookup(dict: Dict(key, List(value)), key: key) -> List(value) {
  case dict.get(dict, key) {
    Ok(values) -> values
    Error(Nil) -> []
  }
}

fn subject(descriptor: Descriptor) -> String {
  case descriptor {
    Field(id: id, ..) -> display_field(id)
    Mapping(name: name, ..) | Assumption(name: name, ..) -> name
  }
}

fn stable_name(descriptor: Descriptor) -> String {
  case descriptor {
    Field(id: id, ..) -> id.component <> "." <> id.name
    _ -> subject(descriptor)
  }
}

fn compare_descriptors(a: Descriptor, b: Descriptor) -> order.Order {
  case string.compare(stable_name(a), stable_name(b)) {
    // Different structured identities can render as the same dotted path.
    order.Eq -> string.compare(string.inspect(a), string.inspect(b))
    different -> different
  }
}

fn location(descriptor: Descriptor) -> Option(String) {
  case descriptor {
    Field(location: location, ..)
    | Mapping(location: location, ..)
    | Assumption(location: location, ..) -> location
  }
}

fn display_field(id: FieldId) -> String {
  let role = case id.role {
    model.InputRole -> "input"
    model.CommandRole -> "command"
    model.EventRole -> "event"
    model.StateRole -> "state"
    model.ReadModelRole -> "read_model"
    model.EffectRole -> "effect"
    model.OutputRole -> "output"
  }
  role <> " " <> id.component <> "." <> id.name
}

fn sort_diagnostics(diagnostics: List(Diagnostic)) -> List(Diagnostic) {
  list.sort(diagnostics, fn(a, b) {
    case string.compare(a.code, b.code) {
      order.Eq -> string.compare(a.subject, b.subject)
      order.Lt -> order.Lt
      order.Gt -> order.Gt
    }
  })
}

fn trace(path: List(FieldId)) -> List(String) {
  list.reverse(path) |> list.map(display_field)
}

fn visit(
  id: FieldId,
  graph: Graph,
  path: List(FieldId),
  evaluation: Evaluation,
) -> Evaluation {
  use <- bool.guard(set.contains(evaluation.visited, id), evaluation)
  let path = [id, ..path]
  let evaluation =
    Evaluation(..evaluation, active: set.insert(evaluation.active, id))
  // The Rust 2.1.0 checker treats roots as terminal provenance boundaries.
  let next = case dict.get(graph.fields, id) {
    Ok(Field(root: True, ..)) -> evaluation
    Ok(descriptor) -> visit_required(id, descriptor, graph, path, evaluation)
    Error(Nil) -> evaluation
  }
  Evaluation(
    ..next,
    visited: set.insert(next.visited, id),
    active: set.delete(next.active, id),
  )
}

fn visit_required(
  id: FieldId,
  field: Descriptor,
  graph: Graph,
  path: List(FieldId),
  evaluation: Evaluation,
) -> Evaluation {
  // An explicit assumption takes precedence over producers in the baseline.
  case lookup(graph.assumptions, id), lookup(graph.recipes, id) {
    [_, ..] as assumptions, _ ->
      check_assumptions(graph, id, assumptions, evaluation)
    [], [] ->
      add_error(
        evaluation,
        Diagnostic(
          "ECM003",
          display_field(id),
          "non-root modeled field has no executable producer",
          "add a mapping, default, absence, or explicit origin",
          trace(path),
          location(field),
        ),
      )
    [], recipes ->
      list.fold(recipes, evaluation, fn(state, recipe) {
        case recipe {
          Mapping(sources: sources, ..) ->
            list.fold(sources, state, fn(state, source) {
              visit_dependency(source, recipe, graph, path, state)
            })
          _ -> state
        }
      })
  }
}

fn check_assumptions(
  graph: Graph,
  id: FieldId,
  assumptions: List(Descriptor),
  evaluation: Evaluation,
) -> Evaluation {
  let allowed =
    graph.options.all_assumptions
    || list.any(assumptions, fn(descriptor) {
      set.contains(graph.options.names, subject(descriptor))
    })
  case allowed {
    True -> Evaluation(..evaluation, assumed: True)
    False ->
      list.fold(assumptions, evaluation, fn(state, descriptor) {
        let name = subject(descriptor)
        add_error(
          state,
          Diagnostic(
            "ECM008",
            name,
            "assumption boundary for `"
              <> display_field(id)
              <> "` is not enabled",
            "allow assumption `"
              <> name
              <> "` or replace it with an executable mapping",
            [],
            location(descriptor),
          ),
        )
      })
  }
}

fn visit_dependency(
  dependency: Dependency,
  recipe: Descriptor,
  graph: Graph,
  path: List(FieldId),
  evaluation: Evaluation,
) -> Evaluation {
  let id = model.dependency_field(dependency)
  case
    dict.has_key(graph.fields, id),
    set.contains(evaluation.active, id),
    dependency
  {
    False, _, _ ->
      add_error(
        evaluation,
        Diagnostic(
          "ECM004",
          subject(recipe),
          "mapping source `" <> display_field(id) <> "` is not a modeled field",
          "register the source field or correct the source path",
          trace([id, ..path]),
          location(recipe),
        ),
      )
    True, True, Current(_) ->
      add_error(
        evaluation,
        Diagnostic(
          "ECM006",
          subject(recipe),
          "mapping closes an ordinary cycle through `"
            <> display_field(id)
            <> "`",
          "use a previous source and provide a seed",
          trace([id, ..path]),
          location(recipe),
        ),
      )
    True, True, Previous(_) ->
      case has_seed(id, graph) {
        True -> evaluation
        False ->
          add_error(
            evaluation,
            Diagnostic(
              "ECM007",
              subject(recipe),
              "temporal source `"
                <> display_field(id)
                <> "` has no non-temporal seed",
              "add a default, absence, origin, or non-temporal mapping",
              trace([id, ..path]),
              location(recipe),
            ),
          )
      }
    True, False, _ -> visit(id, graph, path, evaluation)
  }
}

// The release recognizes the presence of an all-current recipe as a seed.
fn has_seed(id: FieldId, graph: Graph) -> Bool {
  case dict.get(graph.fields, id) {
    Ok(Field(root: True, ..)) -> True
    _ ->
      list.any(lookup(graph.recipes, id), fn(recipe) {
        case recipe {
          Mapping(sources: sources, ..) ->
            list.all(sources, fn(source) {
              case source {
                Current(_) -> True
                Previous(_) -> False
              }
            })
          _ -> False
        }
      })
  }
}

fn add_error(evaluation: Evaluation, error: Diagnostic) -> Evaluation {
  Evaluation(..evaluation, errors: [error, ..evaluation.errors])
}

fn warnings(
  graph: Graph,
  fields: List(#(FieldId, Descriptor)),
) -> List(Diagnostic) {
  let consumed =
    dict.values(graph.recipes)
    |> list.flatten
    |> list.flat_map(fn(recipe) {
      case recipe {
        Mapping(sources: sources, ..) ->
          list.map(sources, model.dependency_field)
        _ -> []
      }
    })
    |> set.from_list
  let warnings =
    list.flat_map(fields, fn(entry) {
      let #(id, descriptor) = entry
      case descriptor, set.contains(consumed, id) {
        Field(root: root, ..), False -> unused(id, root)
        _, _ -> []
      }
    })
  let warnings = case
    list.any(fields, fn(entry) { entry.0.role == model.OutputRole })
  {
    True -> warnings
    False -> [
      Diagnostic(
        "ECM101",
        "model",
        "the linked model has no modeled output",
        "register an output when the model renders a view",
        [],
        None,
      ),
      ..warnings
    ]
  }
  sort_diagnostics(warnings)
}

fn unused(id: FieldId, root: Bool) -> List(Diagnostic) {
  let origin = case root {
    True -> [
      Diagnostic(
        "ECM102",
        display_field(id),
        "modeled origin is not consumed by any mapping",
        "remove the unused field or add a mapping that consumes it",
        [],
        None,
      ),
    ]
    False -> []
  }
  case id.role {
    model.EventRole -> [
      Diagnostic(
        "ECM103",
        display_field(id),
        "modeled event field is not consumed downstream",
        "add a projection or view mapping that consumes it",
        [],
        None,
      ),
      ..origin
    ]
    _ -> origin
  }
}
