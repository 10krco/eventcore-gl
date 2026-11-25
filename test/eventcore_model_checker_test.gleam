import eventcore/model
import eventcore/model_checker as checker
import gleam/list
import gleam/option.{None, Some}

type Input {
  Input
}

type Command {
  Command
}

type Output {
  Output
}

type ReadModel {
  ReadModel
}

type Amount {
  Amount
}

pub fn check_executable_mappings_test() {
  let raw = model.input("Request", Input)
  let command = model.command("Transfer", Command)
  let output = model.output("Receipt", Output)
  let input = model.origin(raw, "amount", Amount, fn(n: Int) { n })
  let transfer = model.field(command, "amount", Amount, fn(n: Int) { n })
  let receipt = model.field(output, "amount", Amount, fn(n: Int) { n })
  let parse =
    model.mapping(
      "parse",
      model.source(model.origin_field(input)),
      transfer,
      fn(n) { n },
    )
  let render =
    model.mapping("render", model.source(transfer), receipt, fn(n) { n })
  let descriptors = [
    checker.origin(input),
    checker.field(transfer),
    checker.field(receipt),
    checker.mapping(parse),
    checker.mapping(render),
  ]
  assert checker.check(descriptors) == Ok(checker.Report(checker.Verified, []))
  assert checker.check(list.reverse(descriptors)) == checker.check(descriptors)
}

pub fn check_reports_missing_duplicate_and_unknown_registrations_test() {
  let command = model.command("Transfer", Command)
  let output = model.output("Receipt", Output)
  let transfer = model.field(command, "amount", Amount, fn(n: Int) { n })
  let receipt = model.field(output, "amount", Amount, fn(n: Int) { n })
  let copy = model.mapping("copy", model.source(transfer), receipt, fn(n) { n })

  let assert Error(checker.Incomplete([empty])) = checker.check([])
  assert empty.code == "ECM001"

  let field = checker.field(transfer)
  let duplicate = checker.at(field, "duplicate.gleam")
  let mapping = checker.at(checker.mapping(copy), "example.gleam")
  let assert Error(checker.Incomplete(diagnostics)) =
    checker.check([field, duplicate, mapping])
  assert checker.check([mapping, duplicate, field])
    == Error(checker.Incomplete(diagnostics))
  assert list.map(diagnostics, fn(d) { d.code })
    == ["ECM002", "ECM003", "ECM005"]
  let assert Ok(unknown) = list.last(diagnostics)
  assert unknown.location == Some("example.gleam")

  let assert Error(checker.Incomplete([unknown])) =
    checker.check([checker.field(receipt), mapping])
  assert unknown.code == "ECM004"
  assert unknown.trace == ["output Receipt.amount", "command Transfer.amount"]
}

pub fn check_temporal_cycles_require_seeds_test() {
  let raw = model.input("Request", Input)
  let read_model = model.read_model("Balance", ReadModel)
  let amount = model.origin(raw, "amount", Amount, fn(n: Int) { n })
  let balance = model.field(read_model, "amount", Amount, fn(n: Int) { n })
  let ordinary =
    model.mapping("loop", model.source(balance), balance, fn(n) { n + 1 })
  let previous =
    model.mapping("advance", model.previous(balance), balance, fn(n) { n + 1 })
  let seed =
    model.mapping(
      "initialize",
      model.source(model.origin_field(amount)),
      balance,
      fn(n) { n },
    )
  let field = checker.field(balance)
  let assert Error(checker.Incomplete([cycle])) =
    checker.check([field, checker.mapping(ordinary)])
  assert cycle.code == "ECM006"
  let assert Error(checker.Incomplete([unseeded])) =
    checker.check([field, checker.mapping(previous)])
  assert unseeded.code == "ECM007"

  let assert Ok(checker.Report(checker.Verified, [warning])) =
    checker.check([
      field,
      checker.origin(amount),
      checker.mapping(seed),
      checker.mapping(previous),
    ])
  assert warning.code == "ECM101"
  let applied = model.apply(previous, model.build(read_model, 20))
  assert model.field_value(applied) == 21
}

pub fn check_requires_explicit_assumptions_and_reports_unused_fields_test() {
  let raw = model.input("Request", Input)
  let read_model = model.read_model("Balance", ReadModel)
  let balance = model.field(read_model, "amount", Amount, fn(n: Int) { n })
  let input = model.origin(raw, "amount", Amount, fn(n: Int) { n })
  let descriptors = [
    checker.field(balance),
    checker.assumption("imported", balance),
    checker.origin(input),
  ]
  let assert Error(checker.Incomplete([disabled])) = checker.check(descriptors)
  assert disabled.code == "ECM008"
  assert disabled.location == None
  let assert Ok(checker.Report(checker.Assumed, warnings)) =
    checker.check_with(
      descriptors,
      checker.strict() |> checker.allow_assumption("imported"),
    )
  assert list.map(warnings, fn(d) { d.code }) == ["ECM101", "ECM102"]
  assert checker.check_with(
      descriptors,
      checker.strict() |> checker.allow_assumptions(),
    )
    == Ok(checker.Report(checker.Assumed, warnings))
}

type Event {
  Event
}

type Currency {
  Currency
}

pub fn check_requires_every_source_and_alternative_test() {
  let request = model.input("Request", Input)
  let transaction = model.event("Transaction", Event)
  let origin =
    model.origin(request, "amount", Amount, fn(pair: #(Int, String)) { pair.0 })
  let currency =
    model.origin(request, "currency", Currency, fn(pair: #(Int, String)) {
      pair.1
    })
  let amount = model.field(transaction, "amount", Amount, fn(n: Int) { n })
  let simple =
    model.mapping(
      "amount",
      model.source(model.origin_field(origin)),
      amount,
      fn(n) { n },
    )
  let alternative =
    model.mapping(
      "amount_in_currency",
      model.sources(
        model.source(model.origin_field(origin)),
        model.source(model.origin_field(currency)),
      ),
      amount,
      fn(values) { values.0 },
    )
  let descriptors = [
    checker.field(amount),
    checker.origin(origin),
    checker.mapping(simple),
    checker.mapping(alternative),
  ]
  let assert Error(checker.Incomplete([diagnostic])) =
    checker.check(descriptors)
  assert diagnostic.code == "ECM004"
  assert diagnostic.subject == "amount_in_currency"

  let assert Ok(checker.Report(checker.Verified, warnings)) =
    checker.check([checker.origin(currency), ..descriptors])
  assert list.map(warnings, fn(warning) { warning.code })
    == ["ECM101", "ECM103"]
}

type State {
  State
}

pub fn check_preserves_release_terminal_boundaries_test() {
  let state = model.state("State", State)
  let output = model.output("Output", Output)
  let value = model.field(state, "value", Amount, fn(n: Int) { n })
  let missing = model.field(state, "missing", Currency, fn(n: Int) { n })
  let visible = model.field(output, "value", Amount, fn(n: Int) { n })
  let update =
    model.mapping("update", model.source(missing), value, fn(n) { n })
  let render =
    model.mapping("render", model.source(missing), visible, fn(n) { n })

  let assert Ok(checker.Report(checker.Verified, _)) =
    checker.check([checker.state_seed(value), checker.mapping(update)])
  let assert Ok(checker.Report(checker.Assumed, _)) =
    checker.check_with(
      [
        checker.field(visible),
        checker.assumption("external", visible),
        checker.mapping(render),
      ],
      checker.strict() |> checker.allow_assumption("external"),
    )
}

pub fn check_preserves_release_temporal_traversal_and_seed_rule_test() {
  let state = model.state("Balance", State)
  let request = model.input("Request", Input)
  let raw = model.origin(request, "value", Amount, fn(n: Int) { n })
  let a = model.field(state, "a", Amount, fn(n: Int) { n })
  let b = model.field(state, "b", Currency, fn(n: Int) { n })
  let seed =
    model.mapping("seed", model.source(model.origin_field(raw)), b, fn(n) { n })
  let carry_to_a = model.mapping("carry", model.previous(b), a, fn(n) { n })
  let copy_to_b = model.mapping("current", model.source(a), b, fn(n) { n })

  let assert Error(checker.Incomplete([cycle])) =
    checker.check([
      checker.field(a),
      checker.field(b),
      checker.origin(raw),
      checker.mapping(carry_to_a),
      checker.mapping(copy_to_b),
      checker.mapping(seed),
    ])
  assert cycle.code == "ECM006"

  let carry_to_b = model.mapping("carry", model.previous(a), b, fn(n) { n })
  let copy_to_a = model.mapping("current", model.source(b), a, fn(n) { n })
  let assert Ok(checker.Report(checker.Verified, _)) =
    checker.check([
      checker.field(a),
      checker.field(b),
      checker.mapping(copy_to_a),
      checker.mapping(carry_to_b),
    ])
}

pub fn check_traverses_producers_in_release_name_order_test() {
  let state = model.state("State", State)
  let target = model.field(state, "a_target", Amount, fn(n: Int) { n })
  let first = model.field(state, "b_first", Amount, fn(n: Int) { n })
  let second = model.field(state, "c_second", Amount, fn(n: Int) { n })
  let missing = model.field(state, "d_missing", Amount, fn(n: Int) { n })
  let producer_z =
    model.mapping("z_producer", model.source(first), target, fn(n) { n })
  let producer_a =
    model.mapping("a_producer", model.source(second), target, fn(n) { n })
  let from_missing_first =
    model.mapping("first", model.source(missing), first, fn(n) { n })
  let from_missing_second =
    model.mapping("second", model.source(missing), second, fn(n) { n })
  let assert Error(checker.Incomplete([error])) =
    checker.check([
      checker.field(target),
      checker.field(first),
      checker.field(second),
      checker.field(missing),
      checker.mapping(producer_z),
      checker.mapping(producer_a),
      checker.mapping(from_missing_first),
      checker.mapping(from_missing_second),
    ])
  assert error.code == "ECM003"
  assert error.trace
    == ["state State.a_target", "state State.c_second", "state State.d_missing"]
}
