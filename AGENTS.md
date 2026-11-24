## Context helpers

- Always make the first step of any plan be to set a goal for the completion of said plan.
- Use RTK for supported noisy shell commands. The native hook normally rewrites
  them automatically; use `rtk proxy <command>` when full output is needed.
- Use billion-context to summarize consumed context when useful. Preserve open
  objectives, decisions, exact errors, and evidence needed for the next action.
- Hindsight provides repository memory. Check relevant decisions before substantial
  work and verify remembered claims against current source and runtime evidence.
  Use deeper reflection deliberately when retrieved pages do not explain why.
- Batch independent tool reads and keep complete logs on disk, returning concise
  evidence. Expand output when needed to establish correctness.

## Delivery

Work from a topic branch. Make small signed commits, each passing the full
Lefthook pre-commit gate. Track porting work on this repository's Tiber board.
Before merging, verify CI and the organization-required approval on the final
revision, including an actual approval from the auto_review GitHub App.

For each significant architectural decision, add an ADR under docs/decisions
and update ARCHITECTURE.md substantively in the same commit. Supersede a merged
ADR through a new record. Use one architectural decision per squash PR so its
pairing survives the organization's merge policy.
