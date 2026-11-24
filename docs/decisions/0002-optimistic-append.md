# ADR 0002. Compare stream versions in the adapter

The executor reconstructs command state outside the store. Another command can
advance any participating stream before the first command commits.

The store contract accepts one batch with the expected versions of all
participating streams and the events to append. An adapter compares those
versions and appends the entire batch in one operation. Read-only participants
carry expected versions too. On a version conflict the executor reconstructs
from fresh reads before retrying.

The in-memory adapter serializes requests through one OTP server per store.
It validates every expected version before returning the updated server state.
Reads capture the requested events from immutable history, so a caller can
consume them while the server handles further requests. The creating process
owns the store's lifetime and can close it early.
