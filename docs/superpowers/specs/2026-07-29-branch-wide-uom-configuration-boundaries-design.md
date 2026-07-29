# Branch-Wide UOM Configuration Boundaries Design

## Goal

Make every dimensioned configuration surface introduced on
`configuration_expose` accept and carry a unit-of-measure quantity instead of a
unit-suffixed primitive, while preserving behavior and keeping dimensionless
values primitive or refined.

## Audit Baseline

The audit compares branch head `dfb46b262173bb933f95130bc4ebd5b363f2b68b`
with merge-base `1d171e99ac73cebdb944479d0d249b816e55a454`.
That range contains 361 commits and changes 908 files.

The final branch diff adds 244 unique environment variables across 13 Rust
entry points. A focused name scan finds 192 dimensioned boundary appearances:

- 20 gateway and schema-registry appearances already use unitless names and
  unit-bearing UOM values;
- 172 appearances still expose a fixed unit in their name or primitive input
  type.

The remaining work is concentrated in admin UI, bench driver, broker, Gres
CLI/runtime/operator surfaces, observability demo, operator runtime settings,
traces, and profiles. Configuration-file and CRD fields are included even when
they do not have a corresponding environment variable.

## Type and Syntax Rules

Operator-facing values use:

| Dimension | Rust boundary type | Examples |
| --- | --- | --- |
| elapsed time | `crabka_units::Time` | `500ms`, `30s`, `8h` |
| byte size | `crabka_units::ByteSize` | `1MiB`, `1.5GB` |
| byte throughput | `crabka_units::ByteRate` | `10MiB/s` |
| frequency | `crabka_units::Frequency` | `5Hz` |
| ratio | `crabka_units::Ratio` | `25%`, `0.25` |

Nonzero values require an explicit unit. Bare nonzero numbers are rejected
rather than assigned an implicit scale. Zero remains accepted only for
settings whose existing semantics permit it.

Dimensionless counts, attempts, capacities measured in entries, partition and
replication counts, offsets, IDs, enum values, strings, paths, addresses, and
booleans remain primitive or `refined_type` newtypes.

Validated primitive newtypes remain at protocol and storage boundaries where
an exact integer domain matters. A UOM configuration value is lowered once at
that boundary and must be:

- finite;
- nonnegative or positive as the setting requires;
- an exact whole count in the protocol unit;
- within the destination integer range; and
- within any existing semantic ceiling.

Fractional values are accepted only when the runtime meaning supports them.
For example, a Tokio poll timeout may accept `1.5s`, while a Kafka `int32`
millisecond field must reject `1.5ms`.

## Naming Rules

Configuration names describe the concept, not one serialization unit:

```text
*_TIMEOUT_MS       -> *_TIMEOUT
*_INTERVAL_SECONDS -> *_INTERVAL
*_MAX_BYTES        -> *_MAX
*_BYTES_PER_SEC    -> *_RATE
```

The same rule applies to command-line flags, environment variables,
configuration-file keys, CRD fields, Rust boundary fields, Compose variables,
and documented examples.

Examples:

```text
BENCH_SAMPLE_INTERVAL=500ms
CRABKA_ADMIN_UI_SESSION_TTL=8h
CRABKA_TRACES_BLOCK_READ_MAX=1GiB
CRABKA_PROFILES_WAL_POLL_TIMEOUT=500ms
```

`WAL` retains its write-ahead-log spelling; `WALL` is not introduced.

No aliases for the old unit-suffixed names are retained. These surfaces were
introduced on this unmerged branch, and carrying both forms would create
permanent ambiguity and duplicate documentation.

## CLI and Environment Boundaries

Clap fields store UOM quantities directly. Shared parsing and positivity
checks should live in `crabka-units` when they are reused across crates;
crate-local logic remains only for domain-specific integer lowering.

Defaults use the same human form accepted from operators, such as `500ms` or
`1MiB`, and preserve the exact existing quantity.

Command-line values continue to override environment values. Invalid values
fail before network, filesystem, object-store, Kubernetes, or broker I/O.

## Configuration Files

Human-authored TOML, YAML, and JSON configuration fields use
`crabka_units::serde_units::human` adapters. Optional fields use the existing
`option_*` adapters.

Field names lose fixed-unit suffixes. Defaults and merge precedence remain
unchanged. Where a config is translated into an internal UOM-based runtime
struct, the translation becomes direct rather than round-tripping through a
primitive.

## CRDs

Dimensioned CRD fields use UOM types with human serde adapters and string
schemas:

```rust
#[serde(
    default,
    skip_serializing_if = "Option::is_none",
    with = "crabka_units::serde_units::human::option_time"
)]
#[schemars(with = "Option<String>")]
pub timeout: Option<Time>
```

The same pattern applies to `ByteSize`, `ByteRate`, `Frequency`, and `Ratio`.
Controllers render the human value into unitless environment-variable names
and lower it only where a downstream protocol requires an integer.

## Explicit Exceptions

The following stay primitive and may retain a unit suffix because their
external contract is not an operator-selected extent:

- absolute timestamps and epochs such as `*_unix_ms` and
  `*_timestamp_ms`;
- Kubernetes-native probe fields such as `initialDelaySeconds` and
  `periodSeconds`;
- Kafka wire/record fields exposed solely as compatibility data rather than
  runtime configuration;
- byte lengths that are structural protocol invariants rather than tunable
  limits; and
- test-fixture values that model those external contracts.

Every exception found by the final scan must be listed with its owner and
reason in the configuration audit. There is no blanket filename or crate
allowlist.

## Owner Migration

The implementation is divided by ownership so each commit remains reviewable:

1. shared UOM parsing and exact-lowering support;
2. admin UI and operator runtime;
3. bench driver and observability demo;
4. traces and profiles;
5. broker CLI and file configuration;
6. Gres CLI, runtime, CRDs, and controller wiring;
7. schema-registry and Kafka CRD gaps;
8. deployment manifests, examples, and documentation; and
9. branch-wide audit closure.

Gateway and the already-unitized schema-registry CLI are regression-checked
rather than rewritten unless the shared helper lets existing duplicate code be
deleted safely.

## Behavior Preservation

All defaults preserve the same physical quantity. Runtime scheduling,
timeouts, byte caps, protocol requests, object-store behavior, retry policies,
and precedence remain unchanged.

The migration changes only operator syntax and boundary types. It does not make
new values configurable, add compatibility aliases, or broaden any existing
safety ceiling.

## Tests

Each owner is migrated test-first. Coverage proves:

1. defaults preserve the old physical quantity;
2. explicit units parse for every supported dimension;
3. bare nonzero numbers, wrong dimensions, malformed input, nonfinite values,
   invalid signs, fractional protocol units, and overflow are rejected;
4. command-line values override environment values;
5. config-file and CRD values round-trip in human form;
6. controller-rendered environment names and values match the new contract;
7. Compose and checked-in manifests render defaults and overrides; and
8. protocol lowering preserves exact existing integer values.

## Completion Gates

The final branch-diff scan must enumerate every newly added CLI, environment,
configuration-file, CRD, Compose, and manifest field whose name or type
suggests time, size, rate, frequency, or ratio.

Every result must be classified as:

- UOM configuration flow;
- dimensionless value;
- external-contract exception;
- invariant; or
- unresolved.

Completion requires zero unresolved results. It also requires affected-package
tests, workspace all-target tests, workspace strict Clippy, nightly formatting,
Compose and manifest rendering, CRD generation/schema tests, diff hygiene,
lockfile inspection, and an updated `docs/configuration-audit.md`.
