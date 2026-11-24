# ADR 0003. Reconstruct command state in participant order

Event application and stream discovery can depend on the order in which a
command sees its streams. An attempt also needs the versions of every stream
it consulted, even if it emits no events for some of them.

Read the declared streams in declaration order. After each stream is fully
folded into state, enqueue any newly discovered streams in discovery order.
Each stream is read once per attempt. Capture its version from the number of
events folded and register it for the atomic append.

Only an append version conflict triggers a retry. Retry with a new initial
state, starting again from the original declarations. The command callbacks
may run on several attempts and must keep side effects out of reconstruction
and decision making. Subscribers can observe retries through the policy hook.
