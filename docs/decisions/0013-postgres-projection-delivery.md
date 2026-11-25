# ADR 0013. Deliver persisted PostgreSQL envelopes at a committed frontier

## Context

A projector needs to drain a stable range of selected events while writes
continue. It also needs the stored event identity and encoded data for its
own decoding and progress tracking. Existing event rows already have a
transactional global position.

## Decision

Add persisted UUID identities and JSON metadata to the events table through a
separate, repeatable migration. PostgreSQL fills both columns for existing
rows and supplies defaults to ordinary event-store writes. Expose a delivery
source with an explicit source identity, validated selections and inclusive
bounds. Read the committed position counter at the start of a page, then scan
bounded ranges in ascending order. Apply selection before the page limit.
Deliver the encoded payload and metadata without application decoding.

## Consequences

Projectors can capture a watermark and drain through it while later commits
remain available to a subsequent catch-up. Event IDs remain stable across
pools and migration reruns. Large JSON numbers retain their persisted
representation.
