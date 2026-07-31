# Observability Demo Consumer Behavior Configuration

## Scope

Expose the classic Consumer's three existing behavior choices on the
observability demo Consume role:

| Setting | Default | Accepted values |
|---|---|---|
| auto offset reset | `latest` | `latest`, `earliest`, `none` |
| isolation level | `read-uncommitted` | `read-uncommitted`, `read-committed` |
| partition assignor | `range` | `range`, `cooperative-sticky` |

Group instance ID and client rack remain outside this slice because they are
workload identity and topology rather than hardcoded operational defaults.

## Interface

The demo exposes these exact CLI and environment pairs:

| CLI | Environment |
|---|---|
| `--consumer-auto-offset-reset` | `CRABKA_DEMO_CONSUMER_AUTO_OFFSET_RESET` |
| `--consumer-isolation-level` | `CRABKA_DEMO_CONSUMER_ISOLATION_LEVEL` |
| `--consumer-assignor` | `CRABKA_DEMO_CONSUMER_ASSIGNOR` |

CLI values override environment values. Omitting both preserves the current
Consumer builder defaults. Explicit values on Produce or Stream roles fail
before telemetry initialization or external I/O.

## Implementation

Implement `FromStr` for the existing `AutoOffsetReset`, `IsolationLevel`, and
`Assignor` enums in `crabka-client-consumer`, using the accepted spellings
above. The demo CLI fields use those enums directly, so parsing remains owned
by the domain types without a Clap dependency or demo-only mirror types.

The resolved values flow directly through `run_consume` into the existing
`auto_offset_reset`, `isolation_level`, and `assignor` Consumer builder
setters. Compose passes the variables only to `demo-consume`, with defaults
matching the library.

No CRD owns this standalone demo. These enumerated choices need neither UOM
quantities nor `refined_type` validation.

## Validation

Tests cover:

- every accepted spelling and rejection of unknown values;
- unchanged defaults and independent overrides;
- environment parsing and CLI precedence;
- rejection on non-Consume roles before external I/O;
- exact help entries;
- Compose ownership and defaults;
- direct propagation to the existing Consumer builder.

Run focused client-consumer and demo tests, then workspace all-target check,
strict Clippy, nightly formatting, and diff hygiene.
