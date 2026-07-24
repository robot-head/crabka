# Gres WAL Recovery Timeout Policy

## Goal

Replace the fixed 10-second connect timeout and 30-second request timeout on
raw committed-WAL recovery connections with validated settings exposed
through the Gres CLI, environment, and fleet CRD.

## Scope

This slice owns the `ConnectionOptions` used by `open_wal_connection`. That
shared boundary serves both normal committed-WAL replay and the immediate
committed-end sampler.

It does not configure DNS lookup, admin metadata, topic creation, producer,
registry, or generic client timeouts. Those paths use different clients and
require separate owner reviews.

## Configuration Surface

The standalone process accepts two optional positive millisecond values:

- `--wal-recovery-connect-timeout-ms`
  / `CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS`
- `--wal-recovery-request-timeout-ms`
  / `CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS`

Effective defaults are 10,000 ms and 30,000 ms. Explicit settings require
`--substrate-bootstrap`; inert local-engine configuration is rejected before
listener or network I/O.

The fleet CRD adds:

```yaml
spec:
  compute:
    walRecoveryConnectTimeoutMs: 10000
    walRecoveryRequestTimeoutMs: 30000
```

Both optional fields have schema minimum one. The operator renders both
effective CLI pairs for every substrate compute, independent of checkpoint
and multi-range mode.

## Ownership and Data Flow

`crabka-gres-substrate` owns
`DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS` and
`DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS`.

The existing `RecoveryReadPolicy` gains two private `Duration` fields. Its
four-argument constructor remains source-compatible and installs the compiled
timeout defaults. A validated `with_timeouts(connect_ms, request_ms)` builder
uses `refined_type` and replaces both values together. Duration accessors feed
`ConnectionOptions` directly.

`LiveRecoveryConfig` already carries `RecoveryReadPolicy`, and Gres already
routes every production recovery configuration through one helper. No new
policy type or propagation path is needed.

Gres adds two optional `PositiveMillis` parser fields and extends the existing
recovery-policy validation, environment isolation, default/environment/CLI
precedence, and zero-boundary tests from four settings to six.

`GresComputeSpec::effective_policy` resolves the two CRD fields through
`PositiveMillis` and the substrate-owned defaults. Deployment rendering adds
the exact pairs beside the existing WAL recovery arguments.

## Semantics and Errors

The two timeouts are independent positive durations; neither must be less than
the other because they guard different operations.

- Connect timeout bounds TCP connection establishment.
- Request timeout bounds each request on an established raw WAL connection.
- Zero fails CLI/environment parsing, programmatic policy construction, CRD
  schema validation, and operator effective-policy validation.
- Explicit local-mode configuration fails before I/O.

The committed-end sampler retains its fixed zero Kafka fetch wait. Connection
timeouts do not change fetch wait, byte limits, retry count, isolation,
partition identity, or offset behavior.

## Tests and Verification

- Substrate tests pin defaults, builder validation, distinctive accessors, and
  exact `ConnectionOptions` wiring for replay and committed-end dialing.
- Gres tests cover defaults, environment input, true CLI-over-environment
  precedence, zero, inert-use rejection, hostile environment isolation, and
  propagation through the existing shared recovery-config helper.
- Operator tests cover round trips, schema minima, defaults, exact validation
  paths, and exact single-/multi-range arguments.
- Generated CRDs must match all nine checked-in files exactly.
- Full affected tests, strict Clippy, help output, formatting, and
  `git diff --check` must pass.

## Deferred Values

DNS, admin-client, topic-operation, producer, registry, and generic
client-library timeouts remain separate owners. This slice adds no speculative
unified timeout abstraction.
