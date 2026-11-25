import eventcore/projection
import eventcore/projection_delivery as delivery
import eventcore/stream

pub fn identities_and_selection_are_validated_test() {
  assert delivery.source_id(" \t") == Error(Nil)
  assert delivery.selection_id("") == Error(Nil)
  assert delivery.projector_name("   ") == Error(Nil)
  assert delivery.event_type("") == Error(Nil)
  let assert Ok(source) = delivery.source_id(" events ")
  assert delivery.source_name(source) == "events"
  let assert Ok(id) = delivery.selection_id(" selected ")
  let assert Ok(created) = delivery.event_type(" created ")
  let assert Ok(updated) = delivery.event_type("updated")
  let assert Ok(prefix) = projection.prefix("order/")
  assert delivery.selection(id, prefix, []) == Error(delivery.EmptyEventTypes)
  assert delivery.selection(id, prefix, [created, created])
    == Error(delivery.DuplicateEventType(created))
  let assert Ok(selection) = delivery.selection(id, prefix, [created, updated])
  let assert Ok(order) = stream.id("order/1")
  let assert Ok(invoice) = stream.id("invoice/1")
  assert delivery.selection_name(delivery.selection_identity(selection))
    == "selected"
  assert delivery.selects(selection, order, "created")
  assert delivery.selects(selection, order, "updated")
  assert !delivery.selects(selection, order, "deleted")
  assert !delivery.selects(selection, invoice, "created")
}
