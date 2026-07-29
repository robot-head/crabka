# Traces Scan Concatenation Cap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the traces scan-concatenation memory cap configurable while
preserving the 1,500,000,000-byte default and fixed Arrow safety ceiling.

**Architecture:** Add one bounded traces-owned newtype, store it directly in
`CrabkaSpanStore`, and pass it into the existing concatenation guard. Keep the
current constructor as a default-preserving wrapper and add one configurable
constructor for production wiring.

**Tech Stack:** Rust 2024, Clap, `refined_type`, `crabka-units`, Docker Compose.

## Constraints

- Preserve the 1,500,000,000-byte default.
- Accept only `1..=1_500_000_000`; the Arrow safety ceiling is invariant.
- CLI overrides environment and invalid input fails before external I/O.
- Preserve exact-cap acceptance and over-cap rejection.
- Add no policy object, builder, CRD, operator field, or new external
  dependency.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `crates/traces/Cargo.toml`
- `Cargo.lock`
- `crates/traces/src/querier/store.rs`
- `crates/traces/src/bin/crabka-traces.rs`
- `demo/observability/docker-compose.yml`
- `crates/observability-demo-app/tests/observability_demo_config.rs`
- `docs/configuration-audit.md`

### Task 1: Validate and apply the traces scan cap

- [ ] Add failing store tests proving:

  - `ScanConcatMaxBytes` accepts `1` and `1_500_000_000`;
  - it rejects zero, malformed, negative, `u64` overflow, and
    `1_500_000_001`;
  - `Display`/`FromStr` round-trip and `size()` preserves the exact byte count;
  - `CrabkaSpanStore::new` preserves the existing default; and
  - a small valid batch is accepted when the configured cap equals its memory
    size and rejected when the cap is one byte smaller.

- [ ] Add failing CLI tests proving the default, invalid inputs, environment
  parsing, and command-line precedence:

```text
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES=1024
--scan-concat-max-bytes 2048
```

Use the existing child-process pattern for environment mutation.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces scan_concat --locked
```

- [ ] Add the workspace-pinned `refined_type` dependency to
  `crates/traces/Cargo.toml`.

- [ ] Add `DEFAULT_SCAN_CONCAT_MAX_BYTES` and
  `ScanConcatMaxBytes(u64)` beside `CrabkaSpanStore`. Validate with
  `MinMaxU64<1, 1_500_000_000>` and implement only `new`, `into_value`,
  `size`, `Default`, `Display`, and `FromStr`.

- [ ] Store the value in `CrabkaSpanStore`. Keep `new` as a default wrapper and
  add `new_with_scan_concat_max_bytes` for the two production construction
  paths.

- [ ] Pass the stored `ByteSize` to `recompute_scan_nested_sets`; remove the
  local fixed cap while retaining the fixed ceiling documentation and existing
  error.

- [ ] Add:

```text
--scan-concat-max-bytes
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES
```

Pass it to the querier and live-store `CrabkaSpanStore` constructors.

- [ ] Verify GREEN and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-traces --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-traces --bin crabka-traces --locked -- --help
test "$(target/debug/crabka-traces --help | \
  rg -c -- '--scan-concat-max-bytes')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Confirm the lockfile changes only the local `crabka-traces` dependency list.

- [ ] Commit only the traces files and lockfile:

```bash
git add Cargo.lock crates/traces/Cargo.toml \
  crates/traces/src/querier/store.rs \
  crates/traces/src/bin/crabka-traces.rs
git commit -m "feat(traces): configure scan concat cap"
```

### Task 2: Wire the observability deployment

- [ ] Add a failing demo configuration test proving `traces-querier` contains:

```text
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES: "${CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES:-1500000000}"
```

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app traces_querier_scan_concat --locked
```

- [ ] Add that single environment mapping to `traces-querier`; add no unused
  mapping to other services.

- [ ] Verify GREEN and deployment rendering:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
docker compose -f demo/observability/docker-compose.yml config --quiet
docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-scan-concat-default.yml
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES=4096 \
  docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-scan-concat-override.yml
rg -n 'CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES' \
  /tmp/crabka-scan-concat-default.yml \
  /tmp/crabka-scan-concat-override.yml
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
```

- [ ] Commit only the deployment files:

```bash
git add demo/observability/docker-compose.yml \
  crates/observability-demo-app/tests/observability_demo_config.rs
git commit -m "feat(demo): wire traces scan concat cap"
```

### Task 3: Audit, verify, commit, and push

- [ ] Run `tools/audit-runtime-values.sh` and record exact line, file, and
  affected-package counts.

- [ ] Classify every result from:

```bash
rg -n \
  'MAX_SCAN_CONCAT|DEFAULT_SCAN_CONCAT_MAX_BYTES|ScanConcatMaxBytes|scan_concat_max_bytes|scan-concat-max-bytes|SCAN_CONCAT_MAX_BYTES' \
  crates demo docs/configuration-audit.md
```

- [ ] Append the owner, default, validation, precedence, runtime flow,
  deployment scope, exact counts, and verification evidence to
  `docs/configuration-audit.md`.

- [ ] Run the final gate:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --locked \
  -p crabka-traces -p observability-demo-app --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --locked \
  -p crabka-traces -p observability-demo-app --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-traces --bin crabka-traces --locked -- --help
docker compose -f demo/observability/docker-compose.yml config --quiet
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
tools/audit-runtime-values.sh > /tmp/crabka-runtime-audit-final.txt
```

Confirm one help entry, default and override Compose values, scanner stability,
and only the expected `Cargo.lock` dependency-list change.

- [ ] Commit the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record traces scan concat cap"
```

- [ ] Fetch, rebase only if the remote advanced, push
  `configuration_expose`, and verify local and remote HEADs match. Preserve the
  unrelated untracked plans.
