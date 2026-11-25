import eventcore/model
import eventcore/stream
import gleam/option.{type Option, None}

type SourceField {
  SourceField
}

type DestinationField {
  DestinationField
}

type AmountField {
  AmountField
}

type MemoField {
  MemoField
}

type Request {
  Request(
    source: model.StreamIdentity(DebitWitness),
    target: model.StreamIdentity(CreditWitness),
    amount: Int,
  )
}

type Transfer {
  Transfer(
    source: model.StreamIdentity(DebitWitness),
    target: model.StreamIdentity(CreditWitness),
    amount: Int,
  )
}

type Ledger {
  Ledger(total: Int, memo: Option(String))
}

pub fn typed_transfer_construction_and_fallible_mapping_test() {
  let request = model.input("Request")
  let transfer = model.command("Transfer")
  let source =
    model.origin(request, "source", SourceField, fn(r: Request) { r.source })
  let target =
    model.origin(request, "target", DestinationField, fn(r: Request) {
      r.target
    })
  let amount =
    model.origin(request, "amount", AmountField, fn(r: Request) { r.amount })
  let to_source =
    model.field(transfer, "source", SourceField, fn(t: Transfer) { t.source })
  let to_target =
    model.field(transfer, "target", DestinationField, fn(t: Transfer) {
      t.target
    })
  let to_amount =
    model.field(transfer, "amount", AmountField, fn(t: Transfer) { t.amount })

  // Keep role witnesses with their domain constructor.
  let assert Ok(from_id) = stream.id("from")
  let assert Ok(to_id) = stream.id("to")
  let from = debit(from_id)
  let to = credit(to_id)
  let request_value =
    model.build(
      request,
      Request(
        model.supply(source, from) |> model.field_value,
        model.supply(target, to) |> model.field_value,
        model.supply(amount, 4) |> model.field_value,
      ),
    )

  let map_source =
    model.mapping(
      "source",
      model.source(model.origin_field(source)),
      to_source,
      fn(id) { id },
    )
  let map_target =
    model.mapping(
      "target",
      model.source(model.origin_field(target)),
      to_target,
      fn(id) { id },
    )
  let map_amount =
    model.try_mapping(
      "amount",
      model.source(model.origin_field(amount)),
      to_amount,
      fn(n) {
        case n > 0 {
          True -> Ok(n)
          False -> Error("invalid amount")
        }
      },
    )
  let moved_source = model.apply(map_source, request_value)
  let moved_target = model.apply(map_target, request_value)
  let assert Ok(moved_amount) = model.apply(map_amount, request_value)
  let transferred =
    build_transfer(transfer, moved_source, moved_target, moved_amount)
  assert model.value(transferred) == Transfer(from, to, 4)
  assert model.stream_id(model.value(transferred).source) == from_id
  assert model.apply(map_amount, model.build(request, Request(from, to, 0)))
    == Error("invalid amount")
}

fn build_transfer(
  component: model.Component(model.Command, Transfer),
  source: model.FieldValue(
    model.Command,
    Transfer,
    SourceField,
    model.StreamIdentity(DebitWitness),
  ),
  target: model.FieldValue(
    model.Command,
    Transfer,
    DestinationField,
    model.StreamIdentity(CreditWitness),
  ),
  amount: model.FieldValue(model.Command, Transfer, AmountField, Int),
) -> model.Modeled(model.Command, Transfer) {
  model.build(
    component,
    Transfer(
      model.field_value(source),
      model.field_value(target),
      model.field_value(amount),
    ),
  )
}

type DebitWitness {
  DebitWitness
}

type CreditWitness {
  CreditWitness
}

fn debit(id: stream.Id) -> model.StreamIdentity(DebitWitness) {
  model.stream_identity(DebitWitness, id)
}

fn credit(id: stream.Id) -> model.StreamIdentity(CreditWitness) {
  model.stream_identity(CreditWitness, id)
}

pub fn state_recipes_build_empty_state_test() {
  let ledger = model.state("Ledger")
  let total =
    model.field(ledger, "total", AmountField, fn(l: Ledger) { l.total })
  let memo = model.field(ledger, "memo", MemoField, fn(l: Ledger) { l.memo })
  let built =
    model.build(
      ledger,
      Ledger(
        model.default(total, fn() { 0 }) |> model.field_value,
        model.absence(memo) |> model.field_value,
      ),
    )
  assert model.value(built) == Ledger(0, None)
}

type Rate {
  Rate(percent: Int)
}

pub fn mapping_combines_fields_from_distinct_components_test() {
  let transfer = model.command("Transfer")
  let pricing = model.input("Rate")
  let amount =
    model.field(transfer, "amount", AmountField, fn(t: Transfer) { t.amount })
  let rate =
    model.origin(pricing, "percent", AmountField, fn(r: Rate) { r.percent })
  let effect = model.effect("Fee")
  let fee = model.field(effect, "amount", AmountField, fn(n: Int) { n })

  let calculator =
    model.mapping(
      "fee",
      model.paired_sources(
        model.source(amount),
        model.source(model.origin_field(rate)),
      ),
      fee,
      fn(values) { values.0 * values.1 / 100 },
    )
  let assert Ok(from) = stream.id("from")
  let assert Ok(to) = stream.id("to")
  let input = #(
    model.build(transfer, Transfer(debit(from), credit(to), 120)),
    model.build(pricing, Rate(5)),
  )

  assert model.apply(calculator, input) |> model.field_value == 6
  assert model.mapping_fields(calculator)
    == #("fee", ["Transfer.amount", "Rate.percent"], "Fee.amount")
}
