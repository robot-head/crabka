# Observability WAL Fetch Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make traces and profiles WAL consumer fetch budgets configurable
while preserving the two-mebibyte total and 256-kibibyte per-partition
defaults.

**Architecture:** Add two positive-`i32` CLI newtypes to the consumer crate,
convert them to UOM `ByteSize` values at the runtime boundary, and pass them
through the existing traces helper and profiles block-builder config. Keep
application defaults in their owning services and the shared consumer
builder's independent 50/1 MiB defaults unchanged.

**Tech Stack:** Rust 2024, Clap, `refined_type`, `crabka-units`, Docker
Compose.

## Constraints

- Preserve 2,097,152 total bytes and 262,144 bytes per partition.
- CLI overrides environment.
- Reject zero, malformed, negative, and `i32`-overflow values before network
  I/O.
- Preserve UOM `ByteSize` throughout the consumer builder and wire conversion.
- Do not change unrelated consumer defaults or behavior.
- Add no policy object, builder, dependency, CRD, or operator field.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `crates/client-consumer/src/consumer.rs`
- `crates/client-consumer/src/lib.rs`
- `crates/traces/src/bin/crabka-traces.rs`
- `crates/profiles/src/blockbuilder.rs`
- `crates/profiles/src/bin/crabka-profiles.rs`
- `demo/observability/docker-compose.yml`
- `crates/observability-demo-app/tests/observability_demo_config.rs`
- `docs/configuration-audit.md`

### Task 1: Add shared validated fetch-size types

- [ ] Add failing unit tests proving both types:

  - accept `1` and `i32::MAX`;
  - reject `"0"`, `"-1"`, malformed input, and values above `i32::MAX`;
  - round-trip through `Display`/`FromStr`; and
  - convert exactly to `ByteSize`.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer consumer_fetch --locked
```

- [ ] Add `ConsumerFetchMaxBytes(i32)` and
  `ConsumerFetchPartitionMaxBytes(i32)` beside the classic consumer
  configuration types. Validate with `GreaterI32<0>`, implement `new`,
  `bytes`, `size`, `Display`, and `FromStr`, and re-export both from `lib.rs`.

- [ ] Keep `Consumer::builder()` accepting `ByteSize`. Do not change
  `DEFAULT_FETCH_MAX`, `DEFAULT_FETCH_PARTITION_MAX`, or duplicate its existing
  fail-fast checks.

- [ ] Verify GREEN and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Confirm `Cargo.lock` is unchanged.

- [ ] Commit only the consumer crate files:

```bash
git add crates/client-consumer/src/consumer.rs \
  crates/client-consumer/src/lib.rs
git commit -m "feat(consumer): validate fetch byte settings"
```

### Task 2: Configure every traces WAL consumer

- [ ] Add failing CLI tests for preserved defaults, invalid values, environment
  values, and command-line precedence:

```text
CRABKA_TRACES_WAL_FETCH_MAX_BYTES=1024
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES=256
--wal-fetch-max-bytes 2048
--wal-fetch-partition-max-bytes 512
```

Use the existing hermetic child-process pattern for environment mutation.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces wal_fetch --locked
```

- [ ] Replace `WAL_FETCH_MAX` and `WAL_FETCH_PARTITION_MAX` with raw named
  service defaults and two small typed-default functions used by Clap.

- [ ] Add:

```text
--wal-fetch-max-bytes
CRABKA_TRACES_WAL_FETCH_MAX_BYTES

--wal-fetch-partition-max-bytes
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES
```

- [ ] Pass both typed values into the shared `wal_consumer` helper and convert
  with `size()` at the consumer-builder call. Update all four call paths:
  block-builder, live-store, embedded querier live-store, and
  metrics-generator. The required helper arguments make omitted propagation a
  compile error; do not add a mock Kafka service solely to inspect builder
  state.

- [ ] Verify GREEN, caller coverage, help, and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-traces --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-traces --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-traces --bin crabka-traces --locked -- --help
test "$(target/debug/crabka-traces --help | rg -c -- '--wal-fetch-max-bytes')" -eq 1
test "$(target/debug/crabka-traces --help | rg -c -- '--wal-fetch-partition-max-bytes')" -eq 1
rg -n 'wal_consumer\(|fetch_max\(|fetch_partition_max\(' \
  crates/traces/src/bin/crabka-traces.rs
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Confirm every production call supplies both CLI values and `Cargo.lock` is
unchanged.

- [ ] Commit only the traces binary:

```bash
git add crates/traces/src/bin/crabka-traces.rs
git commit -m "feat(traces): configure WAL fetch limits"
```

### Task 3: Configure the profiles block-builder consumer

- [ ] Add failing `BlockBuilderConfig` tests proving its defaults preserve
  2,097,152 and 262,144 bytes and configured values survive in the config.

- [ ] Add failing profiles CLI tests for invalid values, environment values,
  and command-line precedence using the same values and child-process pattern
  as traces.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles wal_fetch --locked
```

- [ ] Add typed `wal_fetch_max_bytes` and
  `wal_fetch_partition_max_bytes` fields to `BlockBuilderConfig`. Its existing
  constructor supplies the preserved service defaults through two public
  typed-default functions that the profiles CLI also reuses.

- [ ] Change `run_with_config` to call `.size()` and pass the resulting
  `ByteSize` values to `Consumer::builder`.

- [ ] Add the profiles CLI/environment arguments and copy them into
  `BlockBuilderConfig` for `Target::BlockBuilder`.

- [ ] Verify GREEN, help, and package quality:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-profiles --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-profiles --bin crabka-profiles --locked -- --help
test "$(target/debug/crabka-profiles --help | rg -c -- '--wal-fetch-max-bytes')" -eq 1
test "$(target/debug/crabka-profiles --help | rg -c -- '--wal-fetch-partition-max-bytes')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Confirm `Cargo.lock` is unchanged.

- [ ] Commit only profiles files:

```bash
git add crates/profiles/src/blockbuilder.rs \
  crates/profiles/src/bin/crabka-profiles.rs
git commit -m "feat(profiles): configure WAL fetch limits"
```

### Task 4: Wire the observability deployment

- [ ] Add a failing demo configuration test proving:

```text
traces-block-builder:
  CRABKA_TRACES_WAL_FETCH_MAX_BYTES=${CRABKA_TRACES_WAL_FETCH_MAX_BYTES:-2097152}
  CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES=${CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES:-262144}

profiles-block-builder:
  CRABKA_PROFILES_WAL_FETCH_MAX_BYTES=${CRABKA_PROFILES_WAL_FETCH_MAX_BYTES:-2097152}
  CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX_BYTES=${CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX_BYTES:-262144}
```

and that explicit overrides render unchanged.

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
```

- [ ] Add only those four environment entries. Do not add unused values to
  demo roles that do not run the affected consumers.

- [ ] Verify GREEN and rendered Compose:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app observability_demo_config --locked
docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-wal-fetch-default.yml
CRABKA_TRACES_WAL_FETCH_MAX_BYTES=4096 \
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES=1024 \
CRABKA_PROFILES_WAL_FETCH_MAX_BYTES=8192 \
CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX_BYTES=2048 \
  docker compose -f demo/observability/docker-compose.yml config \
  > /tmp/crabka-wal-fetch-override.yml
rg -n 'WAL_FETCH_(MAX|PARTITION_MAX)_BYTES' \
  /tmp/crabka-wal-fetch-default.yml /tmp/crabka-wal-fetch-override.yml
git diff --check
```

- [ ] Commit only deployment files:

```bash
git add demo/observability/docker-compose.yml \
  crates/observability-demo-app/tests/observability_demo_config.rs
git commit -m "feat(demo): wire WAL fetch limits"
```

### Task 5: Close the audit slice

- [ ] Capture exact evidence:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/(client-consumer|traces|profiles)/'
rg -n 'WAL_FETCH_(MAX|PARTITION_MAX)|ConsumerFetch(Max|PartitionMax)Bytes|wal_fetch_(max|partition_max)_bytes|wal-fetch-(max|partition-max)-bytes' \
  crates demo docs/configuration-audit.md
```

Classify every focused-search line. Confirm each production reference is a
configured default, validation, propagation, deployment, or compatibility
reference rather than an unresolved owner.

- [ ] Append a WAL-fetch-limits section to `docs/configuration-audit.md` with
  defaults, validation, precedence, UOM flow, production callers, deployment
  wiring, exact counts, and verification evidence.

- [ ] Run final gates:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer -p crabka-traces -p crabka-profiles \
    -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer -p crabka-traces -p crabka-profiles \
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
git commit -m "docs(audit): record WAL fetch limits"
```

- [ ] Confirm only pre-existing untracked plan files plus this plan remain.
