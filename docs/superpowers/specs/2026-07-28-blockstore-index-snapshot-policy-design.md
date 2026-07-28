# Blockstore Index Snapshot Policy Design

## Goal

Replace the fixed index-snapshot read limit and retention count with validated
runtime settings while preserving the existing 256-mebibyte read limit and
eight-snapshot retention behavior.

## Scope

This slice covers the `TraceIndex` and `ProfileIndex` snapshot paths used by the
traces and profiles services.

It does not change the separate one-gibibyte Parquet block-read limit, snapshot
serialization, snapshot naming, latest-snapshot selection, object-store layout,
or index contents.

## Public Configuration

Both service binaries accept:

- `--index-snapshot-max-bytes`
- `--index-snapshot-retain`

The traces binary reads `CRABKA_TRACES_INDEX_SNAPSHOT_MAX_BYTES` and
`CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN`. The profiles binary reads
`CRABKA_PROFILES_INDEX_SNAPSHOT_MAX_BYTES` and
`CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN`.

Command-line values win over environment values. Defaults remain 268,435,456
bytes and eight snapshots.

Both values must be positive. Malformed, negative, zero, and
primitive-overflow values are rejected by Clap before object-store or network
I/O.

## Validated Types

Add `IndexSnapshotMaxBytes(u64)`, validated with
`refined_type::rule::GreaterU64<0>`, and
`IndexSnapshotRetain(usize)`, validated with
`refined_type::rule::GreaterUsize<0>`.

Each type implements `FromStr`, `Display`, and `Default`, and exposes its
validated primitive value. The existing defaults become named constants.

Add the workspace-pinned `refined_type` dependency only to
`crabka-blockstore`. The traces and profiles binaries reuse the exported
blockstore types. `Cargo.lock` must otherwise remain unchanged.

## Blockstore API

Keep the existing public `TraceIndex` and `ProfileIndex` load and save methods
as compatibility wrappers using the existing defaults.

Add the minimum configurable variants:

- latest-snapshot loads accept `IndexSnapshotMaxBytes`;
- snapshot saves accept `IndexSnapshotRetain`; and
- the shared snapshot writer prunes with the validated retention value.

Both trace and profile loads use `crabka_object_store::read_capped`. This adds
the existing 256-mebibyte safety boundary to the currently unbounded trace
snapshot read without changing the default profile boundary.

No combined policy object or builder is added: reads and writes each need only
one setting.

## Runtime Flow

The maximum-byte value reaches every traces and profiles startup, periodic
refresh, compactor, query-index, and block-builder load path.

The retention value reaches every traces and profiles block-builder and
compactor save path. Their existing configuration structs gain only the fields
needed to carry these values through library-owned loops.

The value flows are:

```text
CLI / environment / typed default
  -> service or block-builder configuration
  -> TraceIndex / ProfileIndex configurable load
  -> read_capped
```

```text
CLI / environment / typed default
  -> service or block-builder configuration
  -> TraceIndex / ProfileIndex configurable save
  -> shared snapshot writer
  -> prune retained snapshots
```

Missing or malformed snapshots retain the existing caller behavior, including
callers that fall back to an empty index.

## Deployment Wiring

The observability Docker Compose deployment is the checked-in deployment owner
for these services. Add overrideable environment values to the trace and
profile roles that load or save snapshots, using the existing defaults.

No CRD or operator field is added because traces and profiles are not managed
by an existing repository CRD.

## Tests

Test-first coverage will prove:

1. both typed defaults preserve the existing values;
2. positive values are accepted and zero, malformed, negative, and overflow
   values are rejected;
3. trace and profile snapshot loads reject objects over the configured cap;
4. configurable saves retain exactly the requested positive count;
5. both binaries accept environment values and prefer command-line values;
6. block-builder, compactor, refresh, and one-shot load paths receive the
   configured values; and
7. Docker Compose preserves the defaults and accepts overrides.

Completion gates are focused tests, all-target tests for the affected packages,
strict Clippy, nightly formatting, one help entry per setting in each binary,
Compose validation, diff hygiene, and no dependency-version or transitive lock
changes.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts and verification evidence. The separate Parquet
block-read cap remains explicitly pending for the next blockstore slice.
