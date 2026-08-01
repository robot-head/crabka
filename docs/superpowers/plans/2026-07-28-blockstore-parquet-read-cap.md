# Blockstore Parquet Read Cap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the traces Parquet block-read cap configurable while preserving
the one-gibibyte default.

**Architecture:** Validate one scalar in `crabka-blockstore`, keep the size
check at the shared reader boundary, store the value in `BlockStore`, and pass
the same typed value from the traces CLI to compactor, query-frontend, and
querier paths. Existing APIs remain default wrappers.

**Tech Stack:** Rust 2024, Clap, `refined_type`, object_store, Docker Compose.

## Constraints

- Preserve the 1,073,741,824-byte default and reject-before-streaming behavior.
- CLI overrides environment.
- Reject zero, malformed, negative, and primitive-overflow values.
- Do not change DataFusion's independent whole-block scan path.
- Add no metrics setting, policy object, builder, dependency, CRD, or operator
  field.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `crates/blockstore/src/lib.rs`
- `crates/blockstore/src/reader.rs`
- `crates/blockstore/src/store.rs`
- `crates/traces/src/bin/crabka-traces.rs`
- `crates/traces/src/compactor.rs`
- `crates/traces/src/frontend/job.rs`
- `demo/observability/docker-compose.yml`
- `crates/observability-demo-app/tests/observability_demo_config.rs`
- `docs/configuration-audit.md`

### Task 1: Add the validated blockstore cap

- [ ] Add failing type tests proving:

```rust
BlockReadMaxBytes::default().into_value() == 1024 * 1024 * 1024
```

and acceptance of one plus rejection of `"0"`, `"not-a-number"`, `"-1"`, and
u64 overflow.

- [ ] Convert the existing private cap tests into failing public configurable
API tests for whole-block, metadata, and selected-row-group reads. Each must
reject a real object above a one-byte cap and accept it at its exact size.

- [ ] Add failing `BlockStore` tests proving its configured cap reaches metadata
and selected-row-group reads and survives `empty_like`.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore block_read --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore max_bytes --locked
```

- [ ] Implement in `reader.rs`:

```rust
pub const DEFAULT_BLOCK_READ_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub struct BlockReadMaxBytes(u64);
```

Use `GreaterU64<0>`. Implement `new`, `into_value`, `Default`, `Display`, and
`FromStr`. Keep `MAX_BLOCK_BYTES` as a compatibility alias.

- [ ] Make these configurable functions public and typed:

```rust
read_block_with_max_bytes(...)
read_row_group_metadata_with_max_bytes(...)
read_block_row_groups_with_max_bytes(...)
```

Keep existing functions as wrappers using `BlockReadMaxBytes::default()`.
Retain one shared reject-before-streaming size check.

- [ ] Add `block_read_max_bytes: BlockReadMaxBytes` to `BlockStore`.
`BlockStore::new` remains the default wrapper; add
`BlockStore::new_with_block_read_max_bytes`. Add a metadata-read method and use
the stored cap in it and `scan_block_row_groups`. Preserve the value in
`empty_like`.

- [ ] Re-export the type, default, alias, and configurable functions from
`lib.rs`.

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

Confirm `Cargo.lock` is unchanged.

- [ ] Commit only blockstore files:

```bash
git add crates/blockstore/src/lib.rs crates/blockstore/src/reader.rs \
  crates/blockstore/src/store.rs
git commit -m "feat(blockstore): expose parquet read cap"
```

### Task 2: Thread the cap through traces

- [ ] Add failing CLI tests for the default, invalid values, environment value,
and command-line precedence:

```text
CRABKA_TRACES_BLOCK_READ_MAX_BYTES=1024
--block-read-max-bytes 2048
```

Use the existing hermetic child-process pattern for environment mutation.

- [ ] Add failing behavior tests proving:

  - configurable compaction rejects an input block above the supplied cap;
  - `TraceIndexCatalog` metadata loading uses its `BlockStore` cap; and
  - querier selected-row-group scans use the same `BlockStore` cap.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces block_read_max_bytes --locked
```

- [ ] Add the typed CLI field:

```text
--block-read-max-bytes
CRABKA_TRACES_BLOCK_READ_MAX_BYTES
```

- [ ] Keep existing compactor helpers as default wrappers and add configurable
variants only where required. Pass the configured value from `run_compactor`
through index-window and whole-block compaction.

- [ ] Construct capped `BlockStore` values in the production querier and
query-frontend paths. Change `TraceIndexCatalog` to call the `BlockStore`
metadata method so both query paths use the stored value.

- [ ] Leave the in-memory live-store path on the default because it does not
read persisted Parquet blocks.

- [ ] Verify GREEN, production callers, help, and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-traces --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-traces --bin crabka-traces --locked -- --help
test "$(target/debug/crabka-traces --help | rg -c -- '--block-read-max-bytes')" -eq 1
rg -n 'read_block|read_row_group_metadata|scan_block_row_groups|BlockStore::new' \
  crates/traces/src crates/traces/tests
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Classify every production caller and confirm it uses the configured cap or is
an intentional non-persisted/default compatibility path. Confirm the lockfile
is unchanged.

- [ ] Commit only traces files:

```bash
git add crates/traces/src/bin/crabka-traces.rs crates/traces/src/compactor.rs \
  crates/traces/src/frontend/job.rs
git commit -m "feat(traces): configure parquet read cap"
```

### Task 3: Wire the observability deployment

- [ ] Add a failing demo configuration test proving the traces querier has:

```text
CRABKA_TRACES_BLOCK_READ_MAX_BYTES=${CRABKA_TRACES_BLOCK_READ_MAX_BYTES:-1073741824}
```

and that an explicit override renders unchanged.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
```

- [ ] Add only the traces-querier environment entry. Do not add unused entries
to roles absent from the demo.

- [ ] Verify GREEN and rendered Compose:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-parquet-cap-default.yml
CRABKA_TRACES_BLOCK_READ_MAX_BYTES=2048 \
  docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-parquet-cap-override.yml
rg -n 'CRABKA_TRACES_BLOCK_READ_MAX_BYTES' \
  /tmp/crabka-parquet-cap-default.yml /tmp/crabka-parquet-cap-override.yml
git diff --check
```

- [ ] Commit only deployment files:

```bash
git add demo/observability/docker-compose.yml \
  crates/observability-demo-app/tests/observability_demo_config.rs
git commit -m "feat(demo): wire parquet read cap"
```

### Task 4: Close the audit slice

- [ ] Capture exact evidence:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/(blockstore|traces)/'
rg -n 'MAX_BLOCK_BYTES|DEFAULT_BLOCK_READ_MAX_BYTES|BlockReadMaxBytes|block_read_max_bytes|block-read-max-bytes|BLOCK_READ_MAX_BYTES' \
  crates demo docs/configuration-audit.md
```

Classify every focused-search line. Confirm each production reference is a
configured default, compatibility API, propagation, deployment, or test rather
than an unresolved owner.

- [ ] Append a Parquet-read-cap section to `docs/configuration-audit.md` with
the default, validation, precedence, complete runtime/deployment flow, exact
counts, and verification evidence.

- [ ] Run final gates:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-blockstore -p crabka-traces \
    -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-blockstore -p crabka-traces \
    -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
docker compose -f demo/observability/docker-compose.yml config --quiet
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

- [ ] Add exact final evidence to the audit and commit only it:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record parquet read cap"
```

- [ ] Confirm only pre-existing untracked plan files remain.
