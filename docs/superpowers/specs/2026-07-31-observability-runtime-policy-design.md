# Observability Runtime Policy Design

## Goal

Move the remaining observability deployment policy into the existing
`ServiceConfig`, preserving behavior and exposing every value through a
`CRABKA_OBSERVABILITY_*` environment variable.

## Boundary

Configure distributor ingest age, future grace, quota burst, and WAL startup
retry; compactor WAL polling, accumulation, batching, idle, and object-store
retry; and querier frontier refresh, index caches, fetch concurrency, hot-tail
timing, and dependency reconnect timing.

Keep Loki request defaults and protocol compatibility ceilings fixed. They are
client-controlled API behavior, not deployment resource policy. No CRD owns
this standalone service.

## Shape

Add fields directly to `ServiceConfig`. Durations remain `Time` and use the
existing positive-time parser. Positive counts use `NonZeroUsize`. Preserve
all current literals as defaults. Validate related bounds after parsing:

- startup attempt timeout does not exceed startup deadline;
- initial retry backoff does not exceed its maximum;
- accumulation poll timeout does not exceed its window.

The equal hot-tail poll/delivery cadence is one option. The equal querier WAL
and authorizer reconnect cadence is one option. Other numerically equal values
remain separate because they govern unrelated behavior.

## Wiring and verification

Pass policy only into the owning role and the existing functions/builders that
consume it. Tests cover defaults, CLI/environment overrides, invalid zero and
cross-field combinations, and one behavioral override per policy group.
Workspace check, strict Clippy, nightly formatting, and diff hygiene close the
slice.
