# Blockstore Parquet Read Cap Design

## Goal

Replace the fixed Parquet block-read limit with a validated traces runtime
setting while preserving the existing one-gibibyte default and rejection
behavior.

## Scope

This slice covers blockstore's explicit Parquet readers and their production
traces callers:

- compactor whole-block reads;
- query-frontend row-group metadata reads; and
- querier selected-row-group reads.

It does not change Parquet encoding, block writing, DataFusion's independent
whole-block scan path, or the index-snapshot policy.

Metrics receives no setting: its production code does not call the capped
blockstore reader APIs. Test-only metrics callers continue using the default
wrapper.

## Public Configuration

The traces binary accepts:

```text
--block-read-max-bytes
CRABKA_TRACES_BLOCK_READ_MAX_BYTES
```

Command-line values win over environment values. The default remains
1,073,741,824 bytes.

The value must be positive. Malformed, negative, zero, and primitive-overflow
values are rejected by Clap before object-store or network I/O.

## Validated Type

Add `BlockReadMaxBytes(u64)`, validated with
`refined_type::rule::GreaterU64<0>`, to `crabka-blockstore`. It implements
`FromStr`, `Display`, and `Default`, and exposes its validated primitive value.

Reuse blockstore's existing `refined_type` dependency. Do not add a policy
object, builder, or dependency.

## Blockstore API

Keep the existing public read functions and `BlockStore::new` as
default-preserving compatibility wrappers.

Add only the configurable entry points needed by production:

- whole-block, row-group metadata, and selected-row-group readers accept
  `BlockReadMaxBytes`;
- a configurable `BlockStore` constructor stores the cap; and
- `BlockStore` uses that cap for metadata and selected-row-group reads.

`empty_like` preserves the configured cap. The common size check remains in
blockstore and rejects an object after `head()` and before Parquet bytes are
streamed.

## Runtime Flow

The value flows through the traces binary:

```text
CLI / environment / typed default
  -> compactor
  -> configurable whole-block read
```

```text
CLI / environment / typed default
  -> query-frontend BlockStore
  -> configurable row-group metadata read
```

```text
CLI / environment / typed default
  -> querier BlockStore
  -> configurable selected-row-group read
```

Existing errors and caller fallback behavior remain unchanged.

## Deployment Wiring

The observability Docker Compose deployment adds an overrideable
`CRABKA_TRACES_BLOCK_READ_MAX_BYTES` value to the traces querier, preserving
the one-gibibyte default. The demo does not run the traces query-frontend or
compactor roles, so no unused deployment setting is added for them.

No CRD or operator field is added because the traces service is not managed by
an existing repository CRD.

## Tests

Test-first coverage will prove:

1. the typed default preserves one gibibyte;
2. positive values are accepted and zero, malformed, negative, and overflow
   values are rejected;
3. all three reader forms reject objects over the configured cap and accept an
   object exactly at the cap;
4. `BlockStore` carries the configured cap through metadata and selected
   row-group reads, including `empty_like`;
5. the traces binary accepts the environment value and prefers the command-line
   value;
6. compactor, query-frontend, and querier production paths receive the value;
   and
7. Docker Compose preserves the default and accepts an override.

Completion gates are focused tests, all-target tests for affected packages,
strict Clippy, nightly formatting, one help entry for the setting, Compose
validation, diff hygiene, and no lockfile changes.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts and verification evidence.
