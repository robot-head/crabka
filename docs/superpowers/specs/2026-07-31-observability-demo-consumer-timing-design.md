# Observability Demo Consumer Timing Design

## Goal

Expose the four classic Consumer timing settings already accepted by its
builder through the observability demo Consume role. Preserve the existing
defaults:

- session timeout: `45s`;
- rebalance timeout: `1m`;
- heartbeat interval: `3s`; and
- request and connection timeout: `30s`.

## Approach

Add no policy wrapper or library API. The Consumer builder already accepts and
propagates all four UOM `Time` values. The demo resolves the optional
role-scoped inputs once and forwards them directly to:

```rust
Consumer::builder()
    .session_timeout(session_timeout)
    .rebalance_timeout(rebalance_timeout)
    .heartbeat_interval(heartbeat_interval)
    .request_timeout(request_timeout)
```

This preserves the existing request-version lowering, broker-side group
validation, timeout saturation, and startup behavior. Do not introduce new
cross-field rules: the library currently permits brokers to enforce their own
session and rebalance bounds, and this configuration exposure must not change
that behavior.

## CLI and Environment

The Consume role owns:

| CLI | Environment | Default |
|---|---|---:|
| `--consumer-session-timeout` | `CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT` | `45s` |
| `--consumer-rebalance-timeout` | `CRABKA_DEMO_CONSUMER_REBALANCE_TIMEOUT` | `1m` |
| `--consumer-heartbeat-interval` | `CRABKA_DEMO_CONSUMER_HEARTBEAT_INTERVAL` | `3s` |
| `--consumer-request-timeout` | `CRABKA_DEMO_CONSUMER_REQUEST_TIMEOUT` | `30s` |

Use optional `Time` fields with the existing positive UOM parser. Absence
selects the exact builder defaults. Optionality preserves detection of explicit
CLI or environment values so Produce and Stream can reject them before
telemetry initialization or external I/O. Clap retains CLI-over-environment
precedence.

Only `demo-consume` receives the four Compose variables with the unit-bearing
defaults above. No CRD is added because the operator does not own the
standalone demo Consumer.

## Validation and Errors

Reject zero, negative, malformed, and non-finite values through the existing
positive `Time` parser. Preserve fractional and large-time behavior already
defined by the Consumer builder and its protocol-lowering helpers. A
non-Consume role reports the first explicit option and its human-readable UOM
value.

## Verification

Tests cover:

- exact defaults and four independent overrides;
- CLI-over-environment precedence;
- zero rejection;
- Produce and Stream role rejection before external I/O;
- forwarding all four values to the Consumer builder; and
- Compose defaults and Consume-only ownership.

Run the complete demo all-target suite, strict package and workspace Clippy,
workspace check, nightly formatting, and diff hygiene. Append the completed
slice to `docs/configuration-audit.md`. Do not run `cargo clean`; it remains
reserved for completion of the repository-wide goal.
