# Gres WAL Producer Flush Policy

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the producer's fixed 50-millisecond polling loop and 1,000-attempt
limit with one validated flush deadline exposed through the generic producer,
Gres CLI/environment, and fleet CRD.

## Scope

This slice owns the maximum time `Producer::flush` waits for accumulated and
in-flight records to drain. The existing effective bound becomes one exact
50-second default.

The 50-millisecond polling interval does not become configuration. It is an
implementation workaround rather than deployment policy and is removed.
Produce request, retry, batching, and transaction timeouts remain owned by
their existing policies.

## Configuration Surface

The generic producer builder adds an optional `flush_timeout` duration while
preserving source-compatible defaults.

Standalone Gres accepts one whole-millisecond value in
`1..=2,147,483,647`:

- `--wal-producer-flush-timeout-ms`
- `CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS`

The fleet CRD adds the corresponding optional field:

```yaml
spec:
  compute:
    walProducerFlushTimeoutMs: 50000
```

Its generated schema has `minimum: 1` and `maximum: 2147483647`.

The operator always renders the effective argument for substrate-backed
single-range and multi-range compute deployments. Other generic producer
callers retain the default until their owning runtime configuration is
audited; the library no longer hides the value.

## Ownership and Data Flow

`crabka-client-producer` owns the named 50-second default and validates a
whole-millisecond duration in `1..=i32::MAX` through `refined_type`. The
builder stores the validated duration on `Producer`, and `flush` uses it as
the sole deadline. No new policy abstraction is added for one scalar.

Gres reuses its existing refined positive-millisecond parser, resolves the
optional value once during pre-I/O runtime configuration, stores it in
`LiveRecoveryConfig`, and supplies it at the sole WAL `Producer::builder()`
site.

`GresComputeSpec::effective_policy` resolves and validates the optional CRD
field through the same generic producer boundary. The central deployment
renderer emits the effective CLI pair exactly once for every compute.

## Flush Semantics

`flush` force-wakes the sender, establishes one absolute deadline, and waits
until both conditions hold:

- every accumulator has no current or ready records;
- the producer's in-flight batch count is zero.

Each loop registers the `Notify` future before checking those conditions.
This closes the check-to-subscribe race without periodic polling. A sender
notification causes an immediate recheck; the deadline bounds a stopped or
stuck sender. Successful flush, explicit close, and transactional commit or
abort retain their current behavior.

## Errors

- Zero, fractional-millisecond, or out-of-range generic durations fail builder
  validation before broker I/O.
- Invalid CLI/environment values fail during startup parsing or pre-I/O
  configuration.
- Explicit WAL producer configuration outside substrate mode remains rejected.
- Invalid CRD values fail schema or effective-policy validation with the exact
  `spec.compute.walProducerFlushTimeoutMs` path.
- Deadline expiry continues to return `ProducerError::FlushTimeout`.

## Tests and Verification

- Generic producer tests pin the 50-second default, distinctive valid values,
  validation boundaries, and pre-I/O rejection.
- Paused-time behavior tests prove exact deadline expiry, immediate completion
  after notification, and no missed wake between subscription and state check.
- Gres tests cover defaults, environment input, CLI precedence, hostile
  environment isolation, inert-use rejection, help, and exact propagation to
  the WAL producer.
- Operator tests cover defaults, schema bounds, exact validation errors, and
  exact-once single-range and multi-range deployment arguments.
- All nine CRDs are regenerated twice and compared with the checked-in
  manifests.
- A fresh runtime-value scan and focused producer audit must find no remaining
  flush polling or timeout literal in production code.
- Full affected tests, strict Clippy, formatting, and `git diff --check` must
  pass.

## Deferred Values

This slice does not expose protocol invariants or unrelated generic producer
settings. DNS resolution, checkpoint deletion, registry clients, and producer
configuration in other deployable runtimes remain separate audited owners.
