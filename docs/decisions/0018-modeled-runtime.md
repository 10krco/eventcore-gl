# ADR 0018. Run typed models through the EventCore runtime

A modeled command needs the same stream discovery, concurrency checks, and
retry behavior as an ordinary EventCore command. Adapt typed command logic to
the existing command interface. Wrap historical payloads before they enter
modeled logic and unwrap newly emitted payloads at the store boundary.
A command can emit an ordered collection or explicitly give a reason for
emitting no events.

Adapt a modeled projection to the existing named projection runner. The
projector explicitly chooses whether to apply an effect or ignore a delivery.
Pass the effect and its source position to a typed sink, and distinguish
projection errors from sink errors for retry policy. Provide a serialized
in-memory sink for testing effect application with the delivery position in
the same serialized update. A typed view function renders a modeled read
model.
