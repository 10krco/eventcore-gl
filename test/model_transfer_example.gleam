import eventcore/model
import eventcore/stream

pub type SourceField {
  SourceField
}

pub type DestinationField {
  DestinationField
}

pub type AmountField {
  AmountField
}

pub type Transfer {
  Transfer(
    source: model.StreamIdentity(DebitWitness),
    target: model.StreamIdentity(CreditWitness),
    amount: Int,
  )
}

pub fn build_transfer(
  component component: model.Component(
    model.Command(TransferComponent),
    Transfer,
  ),
  source source: model.FieldValue(
    model.Command(TransferComponent),
    Transfer,
    SourceField,
    model.StreamIdentity(DebitWitness),
  ),
  target target: model.FieldValue(
    model.Command(TransferComponent),
    Transfer,
    DestinationField,
    model.StreamIdentity(CreditWitness),
  ),
  amount amount: model.FieldValue(
    model.Command(TransferComponent),
    Transfer,
    AmountField,
    Int,
  ),
) -> model.Modeled(model.Command(TransferComponent), Transfer) {
  model.build(
    component,
    Transfer(
      model.field_value(source),
      model.field_value(target),
      model.field_value(amount),
    ),
  )
}

pub type DebitWitness {
  DebitWitness
}

pub type CreditWitness {
  CreditWitness
}

pub fn debit(id: stream.Id) -> model.StreamIdentity(DebitWitness) {
  model.stream_identity(DebitWitness, id)
}

pub fn credit(id: stream.Id) -> model.StreamIdentity(CreditWitness) {
  model.stream_identity(CreditWitness, id)
}

pub type TransferComponent {
  TransferComponent
}
