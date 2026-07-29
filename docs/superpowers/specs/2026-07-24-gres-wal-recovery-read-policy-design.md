# Gres WAL Recovery Read Policy

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the fixed Kafka WAL recovery fetch wait, byte limits, and empty-fetch
retry limit with one validated policy exposed through the Gres CLI,
environment, and fleet CRD.

## Scope

This slice owns the normal committed-WAL recovery read loop:

- fetch maximum wait: 100 ms
- per-partition fetch maximum: 1,048,576 bytes
- whole-response fetch maximum: 52,428,800 bytes
- consecutive empty-fetch retries: 100

The committed-end sampler's zero wait remains fixed because it must return
immediately at the stable end. Kafka partition zero, read-committed isolation,
one-byte fetch minimum, error codes, and offset arithmetic are protocol or
algorithm invariants and remain fixed. Connection and request timeouts are a
separate policy owner.

## Configuration Surface

The standalone process accepts four positive values:

- `--wal-recovery-fetch-max-wait-ms`,
  `CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS`
- `--wal-recovery-fetch-partition-max-bytes`,
  `CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES`
- `--wal-recovery-fetch-response-max-bytes`,
  `CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES`
- `--wal-recovery-empty-fetch-retries`,
  `CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES`

The parser fields are optional so compiled defaults remain distinguishable
from explicit process configuration. Explicit settings require
`--substrate-bootstrap`; inert local-engine configuration is rejected.

The fleet CRD adds the corresponding optional positive fields under
`spec.compute`:

```yaml
spec:
  compute:
    walRecoveryFetchMaxWaitMs: 100
    walRecoveryFetchPartitionMaxBytes: 1048576
    walRecoveryFetchResponseMaxBytes: 52428800
    walRecoveryEmptyFetchRetries: 100
```

The generated schema minimum is one for every field. The operator always
renders the four effective CLI pairs for a substrate-backed compute, in both
single-range and multi-range deployments.

## Ownership and Data Flow

`crabka-gres-substrate` owns the four defaults and a small
`RecoveryReadPolicy`. Its constructor validates the raw protocol and retry
values with `refined_type`; fields remain private and are exposed only through
typed accessors. `Default` uses the four compiled defaults.

`LiveRecoveryConfig` contains the policy and defaults it in `new`. A
`with_read_policy` builder permits configured callers without changing the
existing constructor signature or test callers.

`crabka-client-core::IsolatedFetch` gains `max_bytes`, replacing its hidden
50 MiB request constant. The older positional `fetch_partition` helper keeps
the same behavior by supplying a named client-core default. Existing
`IsolatedFetch` callers explicitly preserve their current 50 MiB response
limit unless they already own a more specific policy. Recovery supplies both
configured byte limits.

Gres parses values through its existing `PositiveI32` and `PositiveUsize`
wrappers, which are backed by `refined_type`, constructs one
`RecoveryReadPolicy` in `SubstrateRuntimeConfig::from_args`, and uses one
shared recovery-config helper for follower, split, single-range, and
multi-range construction.

`GresComputeSpec::effective_policy` validates the CRD fields through the same
existing positive wrappers and resolves omitted fields through the defaults
owned by `crabka-gres-substrate`.

## Retry Semantics

The retry value retains existing behavior: it counts retries after the first
empty fetch. A value of one permits the initial empty fetch plus one retry,
then fails on a second consecutive empty result without cursor progress.
Cursor progress and any returned records reset the consecutive-empty count.

## Errors

- Zero or out-of-range CLI/environment values fail during Clap parsing.
- Explicit recovery settings without substrate mode fail before listener or
  network I/O, including programmatic `ServeArgs` construction.
- Invalid programmatic `RecoveryReadPolicy` construction returns an error.
- Invalid CRD values fail both schema validation and effective-policy
  validation.

## Tests and Verification

- Client-core tests prove `IsolatedFetch.max_bytes` reaches the wire and the
  positional helper retains its 50 MiB default.
- Substrate tests pin defaults, constructor validation, request wiring, retry
  boundaries, progress resets, and the fixed zero-wait sampler.
- Gres child-process parser tests cover defaults, environment input, CLI
  precedence, zero, and inert local-mode rejection under hostile parent
  environments.
- Gres construction tests prove every runtime recovery path receives the
  effective policy through the shared helper.
- Operator tests pin defaults, schema minima, validation paths, and exact
  single-range and multi-range Deployment arguments.
- Generated CRDs must match all checked-in CRDs exactly.
- Full affected crate tests, strict Clippy, formatting, help output, and
  `git diff --check` must pass.

## Deferred Values

Kafka connection timeout, request timeout, DNS behavior, topic creation, and
writer-side policy remain separate owners. This slice does not add speculative
configuration for protocol identities or the immediate committed-end sample.
