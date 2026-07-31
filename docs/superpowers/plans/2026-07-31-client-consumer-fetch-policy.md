# Client Consumer Fetch Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace classic Consumer's hardcoded fetch minimum and expose its
complete UOM byte policy through the observability demo Consume role.

**Architecture:** Reuse `FetchMinBytes`, `ConsumerFetchMaxBytes`, and
`ConsumerFetchPartitionMaxBytes`; add no policy wrapper. Validate once before
consumer network I/O, carry the minimum beside the existing byte budgets, and
lower all three only when building Kafka requests. The demo resolves optional
role-scoped UOM inputs and supplies them to the existing builder.

**Tech Stack:** Rust, `bon`, Clap, `crabka-units`, `refined_type`, Docker
Compose, Cargo.

## Global Constraints

- Preserve defaults: minimum `1B`, total maximum `50MiB`, per-partition maximum
  `1MiB`.
- All three values are positive, finite, whole-byte UOM quantities fitting
  Kafka `i32`.
- Require minimum at most total maximum.
- Do not require per-partition maximum at most total maximum.
- Preserve fetch versions, polling timeout, isolation level, oversized-first-
  batch handling, and existing callers.
- Reuse existing validated types and dependencies; add no policy wrapper or
  dependency.
- Explicit demo inputs are valid only for Consume.
- Add no CRD because the operator does not own the demo Consumer.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not run `cargo clean`; reserve it for the completed repository-wide goal.

---

### Task 1: Add Classic Consumer Fetch Minimum

**Files:**
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/poll.rs`

**Interfaces:**
- Consumes: `crabka_client_core::FetchMinBytes`
- Produces: `Consumer::builder().fetch_min(ByteSize)`
- Produces internally:
  `build_fetch_request(timeout_ms: i32, isolation_level: IsolationLevel,
  min: ByteSize, max: ByteSize, topics: Vec<FetchTopic>) -> FetchRequest`

- [ ] **Step 1: Write failing propagation and validation tests**

Extend the existing consumer builder validation test with:

```rust
let error = Consumer::builder()
    .bootstrap("unused")
    .group_id("group")
    .subscribe(["topic"])
    .fetch_min(bytes(0))
    .build()
    .await
    .expect_err("zero fetch minimum");
assert!(error.to_string().contains("fetch min"));
```

Extend the `build_fetch_request` unit test to pass `bytes(7)` and assert:

```rust
assert_eq!(request.min_bytes, 7);
```

Add a builder test with `fetch_min = bytes(2)` and `fetch_max = bytes(1)` that
fails before connection creation with:

```text
consumer fetch min must not exceed consumer fetch max
```

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer fetch_min --lib --locked
```

Expected: the classic Consumer builder has no `fetch_min` input or the request
still contains `1`.

- [ ] **Step 3: Implement the minimum path**

Add to `Consumer::start`:

```rust
#[builder(default = bytes(1))]
fetch_min: ByteSize,
```

Validate with `FetchMinBytes::try_from(fetch_min)`, reject minimum above the
already validated total maximum, carry the resulting `ByteSize` through
`StartConfig` and `Consumer`, and change request construction to:

```rust
FetchRequest {
    max_wait_ms: timeout_ms,
    min_bytes: min.bytes_i32(),
    max_bytes: max.bytes_i32(),
    isolation_level: isolation_level.wire(),
    topics,
    ..Default::default()
}
```

Delete the production `min_bytes: 1` literal.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
git add crates/client-consumer/src/consumer.rs crates/client-consumer/src/poll.rs
git commit -m "feat(consumer): expose fetch minimum"
```

---

### Task 2: Expose the Demo Consume-Role Policy

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/consumer_fetch_policy_config.rs`
- Modify:
  `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `Consumer::builder().fetch_min`, `.fetch_max`, and
  `.fetch_partition_max`
- Produces:
  `effective_consumer_fetch_policy(&Cli) ->
  std::io::Result<(ByteSize, ByteSize, ByteSize)>`

- [ ] **Step 1: Write failing CLI, role, and Compose tests**

Add a unit test that resolves defaults and:

```rust
assert_eq!(min, bytes(1));
assert_eq!(max, mebibytes(50));
assert_eq!(partition_max, mebibytes(1));
```

Parse custom Consume inputs `3B`, `32MiB`, and `2MiB` and assert the same
values. In the subprocess test, prove environment parsing and CLI precedence
by rejecting `--consumer-fetch-min 5B` on Stream while the environment contains
`3B`; stderr must show `5B`. Test zero and minimum-above-maximum rejection.

Extend the Compose contract to require under `demo-consume` only:

```yaml
CRABKA_DEMO_CONSUMER_FETCH_MIN: "${CRABKA_DEMO_CONSUMER_FETCH_MIN:-1B}"
CRABKA_DEMO_CONSUMER_FETCH_MAX: "${CRABKA_DEMO_CONSUMER_FETCH_MAX:-50MiB}"
CRABKA_DEMO_CONSUMER_FETCH_PARTITION_MAX: "${CRABKA_DEMO_CONSUMER_FETCH_PARTITION_MAX:-1MiB}"
```

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app consumer_fetch_policy --all-targets --locked
```

Expected: the CLI fields, resolver, and Compose values are absent.

- [ ] **Step 3: Implement the demo policy**

Add three optional Clap `ByteSize` fields using `parse::positive_byte_size`,
the exact CLI/environment names in the approved design, and no Clap defaults.
The resolver must reject the first explicit value on non-Consume roles, select
typed library defaults when absent, validate via the three existing semantic
types, and enforce minimum at most total maximum.

Resolve before telemetry initialization. Pass the values to:

```rust
Consumer::builder()
    .fetch_min(fetch_min)
    .fetch_max(fetch_max)
    .fetch_partition_max(fetch_partition_max)
```

Add the three Compose variables only to `demo-consume`.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
git add crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/consumer_fetch_policy_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose consumer fetch policy"
```

---

### Task 3: Audit and Close the Slice

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify:
  `docs/superpowers/plans/2026-07-31-client-consumer-fetch-policy.md`

**Interfaces:**
- Proves the classic fetch minimum is no longer hardcoded and records that the
  repository-wide audit remains active

- [ ] **Step 1: Audit ownership**

```bash
rg -n \
  'min_bytes: 1|fetch_min|fetch_max|fetch_partition_max|ConsumerFetch(Max|PartitionMax)Bytes|FetchMinBytes|CRABKA_DEMO_CONSUMER_FETCH_' \
  crates/client-consumer crates/observability-demo-app demo/observability
```

Every production hit must be a default, validation, propagation, request
lowering, CLI/environment input, or deployment input. Remaining numeric hits
must be tests.

- [ ] **Step 2: Run affected and workspace gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer -p observability-demo-app \
  --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

- [ ] **Step 3: Re-run affected tests after formatting**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer -p observability-demo-app \
  --all-targets --locked
```

- [ ] **Step 4: Document and commit**

Append exact defaults, CLI/environment names, validation, request flow, test
counts, and audit classification to `docs/configuration-audit.md`. Check every
completed plan checkbox, then:

```bash
git add docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-client-consumer-fetch-policy.md
git commit -m "docs(config): close consumer fetch policy"
```
