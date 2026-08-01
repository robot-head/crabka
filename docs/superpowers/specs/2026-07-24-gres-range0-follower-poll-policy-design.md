# Gres Range-0 Follower Poll Policy

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the fixed 100 ms range-0 follower polling cadence with one validated
runtime setting exposed through the Gres CLI, environment, and fleet CRD.

## Scope

This slice owns only the periodic wait in
`attach_range0_read_barrier`. The existing notification still wakes catalog
barrier waiters immediately.

Kafka WAL fetch wait, empty-fetch retry count, connection timeouts, protocol
partition values, and offset arithmetic are not part of this setting. The
fetch and retry values affect every recovery path and require a separate
recovery-policy review.

## Configuration Surface

The standalone process accepts an optional positive millisecond value:

- CLI: `--range0-follower-poll-interval-ms`
- environment: `CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS`
- effective default: 100 ms

The option requires `--ranges`, which already requires substrate mode. This
rejects an inert setting on local and single-range runtimes. Zero is rejected
by the existing `PositiveMillis` type, which uses `refined_type`.

The fleet CRD adds:

```yaml
spec:
  compute:
    range0FollowerPollIntervalMs: 100
```

The schema minimum is one. The operator validates the optional value through
`PositiveMillis` and renders the CLI pair only when range control is enabled,
the same branch that renders `--ranges`.

## Ownership and Data Flow

`crabka-gres-control` owns
`DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS`, so the binary and operator share
one compiled default.

`ServeArgs` keeps the parser value optional. `SubstrateRuntimeConfig::from_args`
resolves it to a `Duration` using the shared default. The effective duration
flows through `attach_range0_read_barrier` into the existing `tokio::select!`
sleep branch. No one-field policy struct or new dependency is needed.

`GresComputeSpec::effective_policy` resolves the CRD value to
`PositiveMillis`. `render_deployment` converts it to the CLI argument only in
the existing range-control branch.

## Errors

- Zero fails during CLI/environment parsing and CRD effective-policy
  validation.
- Explicit CLI/environment configuration without `--ranges` is rejected.
- CRD values below one are rejected by both the generated schema and the
  operator's effective-policy validation.

## Tests and Verification

- Gres parser tests cover absence, environment input, CLI precedence, zero,
  and the `--ranges` requirement.
- A paused-time Gres test proves the configured duration controls the periodic
  wake while `Notify` still wakes immediately.
- Operator tests pin the shared default, schema minimum, validation error path,
  and exact multi-range Deployment argument.
- Generated CRDs must match all checked-in CRDs exactly.
- Full affected crate tests, strict Clippy, formatting, help output, and
  `git diff --check` must pass.

## Fixed and Deferred Values

The notification branch, loop shape, coordinator range identity, and
zero-offset/protocol arithmetic remain fixed behavior. General WAL recovery
fetch wait and retry limits are deferred to their broader recovery-policy
owner rather than being mislabeled as follower-only configuration.
