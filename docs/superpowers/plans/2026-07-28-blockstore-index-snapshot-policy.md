# Blockstore Index Snapshot Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make trace/profile index-snapshot read limits and retention counts
configurable while preserving the 256-MiB and eight-snapshot defaults.

**Architecture:** Validate two scalar settings in `crabka-blockstore`, reuse
them directly at the traces/profiles Clap boundaries, and pass each only to the
existing load or save seam that consumes it. Keep current public methods as
default-preserving wrappers.

**Tech Stack:** Rust 2024, Clap, `refined_type`, object_store, Docker Compose.

## Constraints

- Preserve the 268,435,456-byte and eight-snapshot defaults.
- CLI overrides environment.
- Reject zero, malformed, negative, and primitive-overflow values.
- Preserve snapshot format, naming, latest selection, and caller fallback
  behavior.
- Do not change the separate one-gibibyte Parquet block-read limit.
- Add no policy wrapper, builder, CRD, or operator field.
- Add only the workspace-pinned `refined_type` dependency to blockstore; allow
  only the corresponding direct-dependency entry change in `Cargo.lock`.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `Cargo.lock`
- `crates/blockstore/Cargo.toml`
- `crates/blockstore/src/index_snapshot.rs`
- `crates/blockstore/src/lib.rs`
- `crates/blockstore/src/profile_index.rs`
- `crates/blockstore/src/trace_index.rs`
- `crates/traces/src/bin/crabka-traces.rs`
- `crates/traces/src/blockbuilder.rs`
- `crates/profiles/src/bin/crabka-profiles.rs`
- `crates/profiles/src/blockbuilder.rs`
- `demo/observability/docker-compose.yml`
- `crates/observability-demo-app/tests/observability_demo_config.rs`
- `docs/configuration-audit.md`

### Task 1: Add validated blockstore settings and configurable APIs

- [ ] Add failing type tests for:

```rust
IndexSnapshotMaxBytes::default().into_value() == 256 * 1024 * 1024
IndexSnapshotRetain::default().into_value() == 8
```

and acceptance of one plus rejection of `"0"`, `"not-a-number"`, `"-1"`,
and the primitive-overflow value for each type.

- [ ] Add failing behavioral tests proving:

  - `TraceIndex::load_latest_snapshot_with_max_bytes` rejects an object larger
    than the configured cap;
  - `ProfileIndex::load_latest_snapshot_with_max_bytes` does the same; and
  - repeated configurable saves retain exactly two snapshots.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore index_snapshot --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore snapshot_with --locked
```

- [ ] Add the workspace `refined_type` dependency to blockstore and implement:

```rust
pub const DEFAULT_INDEX_SNAPSHOT_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_INDEX_SNAPSHOT_RETAIN: usize = 8;

pub struct IndexSnapshotMaxBytes(u64);
pub struct IndexSnapshotRetain(usize);
```

Use `GreaterU64<0>` and `GreaterUsize<0>`. Implement `new`, `into_value`,
`Default`, `Display`, and `FromStr` with accurate `# Errors` documentation.
Re-export both types and defaults from `lib.rs`; retain existing public constant
aliases for compatibility.

- [ ] Change the crate-private shared snapshot writer to accept
`IndexSnapshotRetain` and remove the defensive `.max(1)`, because validation
now enforces that invariant.

- [ ] Add only these public method variants to both index types:

```rust
load_with_max_bytes(...)
load_latest_snapshot_with_max_bytes(...)
save_latest_snapshot_with_retain(...)
```

Keep existing public methods as wrappers using the typed defaults.

- [ ] Route trace loads through `crabka_object_store::read_capped`, matching the
existing profile implementation. Do not cap serialization or writes.

- [ ] Verify GREEN and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-blockstore --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Confirm the lock diff changes only `crabka-blockstore`'s direct dependency
list.

- [ ] Commit only the blockstore API and lock entry:

```bash
git add Cargo.lock crates/blockstore/Cargo.toml crates/blockstore/src/index_snapshot.rs \
  crates/blockstore/src/lib.rs crates/blockstore/src/profile_index.rs \
  crates/blockstore/src/trace_index.rs
git commit -m "feat(blockstore): expose snapshot policy"
```

### Task 2: Thread settings through traces

- [ ] Add failing CLI tests for both defaults, invalid values, environment
values, and command-line precedence:

```text
CRABKA_TRACES_INDEX_SNAPSHOT_MAX_BYTES=1024
CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN=3
--index-snapshot-max-bytes 2048
--index-snapshot-retain 4
```

Use the existing hermetic child-process pattern for environment mutation.

- [ ] Add a failing block-builder test showing the configured retention value
reaches its snapshot save.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces index_snapshot --locked
```

- [ ] Add typed fields to `Cli`:

```text
--index-snapshot-max-bytes / CRABKA_TRACES_INDEX_SNAPSHOT_MAX_BYTES
--index-snapshot-retain / CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN
```

- [ ] Use the configured maximum in every trace snapshot load:

  - block-builder startup;
  - querier startup and periodic refresh;
  - query-frontend catalog construction;
  - any other one-shot query path; and
  - compactor startup.

- [ ] Add only `index_snapshot_retain` to `BlockBuilderConfig`, because its loop
only saves. Use it in `flush_partition_windows`.

- [ ] Use configured retention in the one-shot compactor save.

- [ ] Verify GREEN, all trace call sites, and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-traces --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-traces --bin crabka-traces --locked -- --help
test "$(target/debug/crabka-traces --help | rg -c -- '--index-snapshot-max-bytes')" -eq 1
test "$(target/debug/crabka-traces --help | rg -c -- '--index-snapshot-retain')" -eq 1
rg -n 'load_latest_snapshot|save_latest_snapshot' \
  crates/traces/src crates/traces/tests
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
```

Classify every production call and confirm it uses a configurable variant.

- [ ] Commit only traces files:

```bash
git add crates/traces/src/bin/crabka-traces.rs crates/traces/src/blockbuilder.rs
git commit -m "feat(traces): configure snapshot policy"
```

### Task 3: Thread settings through profiles

- [ ] Add failing CLI tests equivalent to Task 2 using:

```text
CRABKA_PROFILES_INDEX_SNAPSHOT_MAX_BYTES
CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN
```

- [ ] Add a failing `BlockBuilderConfig` test showing both configured values
reach block-builder load/save behavior.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles index_snapshot --locked
```

- [ ] Add typed CLI fields and use the configured maximum in querier,
query-frontend, periodic refresh, compactor, and block-builder load paths.

- [ ] Add `index_snapshot_max_bytes` and `index_snapshot_retain` to
`BlockBuilderConfig`, preserve typed defaults in `new`, and use them in its
load/save loop.

- [ ] Use configured retention in the profiles compactor save.

- [ ] Verify GREEN, all profile call sites, and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-profiles --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-profiles --bin crabka-profiles --locked -- --help
test "$(target/debug/crabka-profiles --help | rg -c -- '--index-snapshot-max-bytes')" -eq 1
test "$(target/debug/crabka-profiles --help | rg -c -- '--index-snapshot-retain')" -eq 1
rg -n 'load_latest_snapshot|save_latest_snapshot' \
  crates/profiles/src crates/profiles/tests
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
```

Classify every production call and confirm it uses a configurable variant.

- [ ] Commit only profiles files:

```bash
git add crates/profiles/src/bin/crabka-profiles.rs crates/profiles/src/blockbuilder.rs
git commit -m "feat(profiles): configure snapshot policy"
```

### Task 4: Wire the observability deployment

- [ ] Add a failing demo configuration test that checks:

  - trace/profile block-builders receive both settings;
  - trace/profile queriers receive the maximum-byte setting;
  - defaults render as 268,435,456 and eight; and
  - explicit Compose environment overrides render unchanged.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
```

- [ ] Add only the relevant signal-specific environment entries to each
Compose role. Do not pass retention to read-only queriers.

- [ ] Verify GREEN and rendered Compose:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
docker compose -f demo/observability/docker-compose.yml config > /tmp/crabka-compose-default.yml
CRABKA_TRACES_INDEX_SNAPSHOT_MAX_BYTES=1024 \
CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN=3 \
CRABKA_PROFILES_INDEX_SNAPSHOT_MAX_BYTES=2048 \
CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN=4 \
  docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-compose-override.yml
rg -n 'INDEX_SNAPSHOT_(MAX_BYTES|RETAIN)' \
  /tmp/crabka-compose-default.yml /tmp/crabka-compose-override.yml
git diff --check
```

- [ ] Commit only deployment files:

```bash
git add demo/observability/docker-compose.yml \
  crates/observability-demo-app/tests/observability_demo_config.rs
git commit -m "feat(demo): wire snapshot policy"
```

### Task 5: Close the snapshot-policy audit slice

- [ ] Capture exact evidence:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/(blockstore|traces|profiles)/'
rg -n 'MAX_(INDEX|PROFILE_INDEX)_SNAPSHOT_BYTES|DEFAULT_INDEX_SNAPSHOT_RETAIN|index_snapshot_(max_bytes|retain)|index-snapshot-(max-bytes|retain)|INDEX_SNAPSHOT_(MAX_BYTES|RETAIN)' \
  crates demo docs/configuration-audit.md
```

Classify every focused-search line. Confirm the snapshot limit and retention
are configured defaults, compatibility aliases, propagation, deployment,
tests, or audit evidence rather than unresolved production owners.

- [ ] Append a snapshot-policy section to `docs/configuration-audit.md` with
the defaults, validation, precedence, complete runtime/deployment flow, trace
cap correction, exact counts, and verification evidence.

- [ ] Run final gates:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore -p crabka-traces -p crabka-profiles \
    -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-blockstore -p crabka-traces -p crabka-profiles \
    -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
docker compose -f demo/observability/docker-compose.yml config --quiet
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

Confirm the lock diff remains limited to blockstore's direct dependency entry.

- [ ] Commit only the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record snapshot policy"
```

### Task 6: Select the next blockstore owner

- [ ] Re-run the complete repository scanner.
- [ ] Confirm the separate one-gibibyte Parquet block-read cap remains pending.
- [ ] Re-enter the design approval workflow before changing it.

Keep the unrelated bench-driver implementation plans untracked.
