# 0015: Documentation written per slice

## Status

Accepted

## Context

The full documentation set (architecture, event flows, patterns, operations, ADRs, per-service
READMEs) could in principle be written once, after all three slices and six services exist.
Documentation written only at the end of a six-service build describes the system as imagined
during planning rather than as actually built, and any drift between the two is caught, if at all,
by a single end-of-project review rather than by each slice's own verification step.

## Decision

The documentation set is authored incrementally: each slice ends with the documentation for what
that slice built. Slice 1 produces `architecture.md`, `event-flows.md`, `patterns.md`,
`local-development.md`, the four slice-1 service READMEs, the ADRs for the corrections taken in
slice 1, and the README quick start. `docs/operations.md` and the final documentation review are
slice 3's.

## Consequences

Documents written in slice 1 describe a three-service saga (plus Configuration Service) and
explicitly do not yet cover Telegram Service, the API Gateway, or recovery — every slice-1 document
that lists "what this system does not have" must be revisited and corrected when slice 2 adds
those pieces, rather than being complete on first write. There are therefore two review passes
required over the project's lifetime for anything like `README.md`'s known-limitations section:
once now, and again each time a slice removes one of the limitations it names.
