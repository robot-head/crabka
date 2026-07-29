# Observability WAL Fetch Limits Design

## Goal

Replace the fixed traces and profiles WAL consumer fetch limits with validated
runtime settings while preserving the existing two-mebibyte total and
256-kibibyte per-partition defaults.

## Scope

This slice covers the explicit WAL consumer limits in:

- every traces role that uses the shared `wal_consumer` helper; and
- the profiles block-builder consumer.

It does not change Kafka protocol behavior, consumer-group behavior, unrelated
consumer defaults, or services without these explicit limits.

## Public Configuration

The traces binary accepts:

```text
--wal-fetch-max-bytes
CRABKA_TRACES_WAL_FETCH_MAX_BYTES

--wal-fetch-partition-max-bytes
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES
```

The profiles binary accepts the same command-line arguments with:

```text
CRABKA_PROFILES_WAL_FETCH_MAX_BYTES
CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX_BYTES
```

Command-line values win over environment values. Defaults remain 2,097,152
total bytes and 262,144 bytes per partition.

Both values must be positive. Malformed, negative, zero, and primitive-overflow
values are rejected by Clap before network I/O.

## Validated Types

Add `ConsumerFetchMaxBytes(i32)` and
`ConsumerFetchPartitionMaxBytes(i32)` to `crabka-client-consumer`. Both use
`refined_type::rule::GreaterI32<0>` and implement the parsing and display traits
needed by Clap. Each exposes its value as a `crabka_units::ByteSize` for the
runtime boundary while retaining the exact positive-`i32` Kafka protocol
domain.

The UOM-integrated classic consumer builder keeps its existing `ByteSize`
arguments and validation. Traces and profiles pass the validated settings as
`ByteSize` values and retain their current application-specific defaults rather
than changing the client library's independent default fetch policy.

No dependency is added: `crabka-client-consumer` already depends on
`refined_type` and `crabka-units`, and both services already depend on the
consumer and units crates.

## Runtime Flow

Traces passes both validated values to its common `wal_consumer` helper. That
single helper applies them to block-builder, live-store, embedded querier
live-store, and metrics-generator consumers.

Profiles stores both validated values in `BlockBuilderConfig`. Its existing
constructor supplies the old defaults, the binary replaces them from parsed
configuration, and `run_with_config` applies them to the consumer builder.

Existing constructors and callers preserve their behavior through the typed
defaults.

## Deployment Wiring

The observability Docker Compose deployment exposes the traces variables on
`traces-block-builder` and the profiles variables on
`profiles-block-builder`, preserving the current defaults. It does not add
unused values to demo services that do not run these consumers.

No CRD or operator field is added because these observability services are not
managed by an existing repository CRD.

## Tests

Test-first coverage will prove:

1. both shared types accept positive values and reject zero, negative,
   malformed, and overflow values;
2. the classic consumer builder preserves its validation behavior;
3. both binaries preserve the old defaults, read environment values, and
   prefer command-line values;
4. traces passes both values through its common helper;
5. `BlockBuilderConfig` preserves and applies both profiles values; and
6. Docker Compose preserves each default and accepts overrides.

Completion gates are focused tests, all-target tests for affected packages,
strict Clippy, nightly formatting, one help entry per argument in each binary,
Compose validation, diff hygiene, and lockfile stability.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts plus verification evidence.
