# Share Consumer Acquire Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Kafka's two ShareFetch acquisition policies through `ShareConsumer::builder()` while preserving batch-optimized behavior by default.

**Architecture:** Add one public enum beside the existing share-consumer value types, store it on `ShareConsumer`, and convert it to Kafka's `i8` code only in `build_share_fetch_request`. Reuse the existing builder and request path; do not add parsing or a deployment surface without a production owner.

**Tech Stack:** Rust 2024, Bon builders, CrabKafka protocol/client crates, `assert2`, Cargo, Clippy, rustfmt, ripgrep.

## Global Constraints

- Expose exactly `ShareAcquireMode::BatchOptimized` and `ShareAcquireMode::RecordLimit`.
- Default to `BatchOptimized`, preserving the current wire value `0`.
- Map `BatchOptimized` to `0` and `RecordLimit` to `1`.
- Store the semantic enum on `ShareConsumer`; convert to `i8` only while building `ShareFetchRequest`.
- Preserve request-version 1 behavior; the generated encoder already omits `share_acquire_mode` before version 2.
- Do not add a numeric newtype, string parser, raw numeric public input, dependency, protocol-generator change, CLI/environment setting, CRD field, or operator wiring.
- Do not use `refined_type`: the closed enum already excludes invalid values.
- Preserve fetch limits, acknowledgement behavior, session epochs, decoding, and error propagation.
- Re-export the enum from the share module and crate root.
- Use `assert2`, never Rust's plain assertion macros.
- Follow TDD: observe the intended failure before production implementation.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Pass `--locked` to every lock-aware Cargo command.
- Do not change `Cargo.lock`.
- Preserve and never stage unrelated dirty or untracked workspace files.

## File Map

- `crates/client-consumer/src/share/types.rs`: public enum, default, private wire conversion, and mapping tests.
- `crates/client-consumer/src/share/consumer.rs`: builder input and semantic consumer state.
- `crates/client-consumer/src/share/poll.rs`: request propagation and exact request tests.
- `crates/client-consumer/src/share/mod.rs`: share-module re-export.
- `crates/client-consumer/src/lib.rs`: crate-root re-export.
- `docs/configuration-audit.md`: completed owner, exact scanner evidence, verification, and next unresolved candidate.

---

### Task 1: Expose and route the acquisition mode

**Files:**

- Modify: `crates/client-consumer/src/share/types.rs`
- Modify: `crates/client-consumer/src/share/consumer.rs`
- Modify: `crates/client-consumer/src/share/poll.rs`
- Modify: `crates/client-consumer/src/share/mod.rs`
- Modify: `crates/client-consumer/src/lib.rs`
- Test: `crates/client-consumer/src/share/types.rs`
- Test: `crates/client-consumer/src/share/poll.rs`

**Interfaces:**

- Produces: `pub enum ShareAcquireMode { BatchOptimized, RecordLimit }`.
- Produces internally: `ShareAcquireMode::wire(self) -> i8`.
- Extends: `ShareConsumer::builder().acquire_mode(ShareAcquireMode)`.
- Extends internally: `ShareConsumer::acquire_mode: ShareAcquireMode`.
- Extends internally: `build_share_fetch_request(..., acquire_mode: ShareAcquireMode, topics: Vec<FetchTopic>)`.

- [ ] **Step 1: Record the package baseline**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
```

Expected: exit 0. Record each suite summary as Cargo reports it.

- [ ] **Step 2: Add failing enum tests**

In the existing `tests` module in
`crates/client-consumer/src/share/types.rs`, add:

```rust
#[test]
fn acquire_mode_default_is_batch_optimized() {
    assert2::assert!(
        ShareAcquireMode::default() == ShareAcquireMode::BatchOptimized
    );
}

#[test]
fn acquire_mode_wire_codes_match_kafka() {
    for (mode, expected) in [
        (ShareAcquireMode::BatchOptimized, 0),
        (ShareAcquireMode::RecordLimit, 1),
    ] {
        assert2::assert!(mode.wire() == expected);
    }
}
```

- [ ] **Step 3: Add failing request-propagation coverage**

In `crates/client-consumer/src/share/poll.rs`, rename
`share_fetch_request_preserves_configured_limits_and_timeout_bounds` to
`share_fetch_request_preserves_acquire_mode_limits_and_timeout_bounds`.

Pass `ShareAcquireMode::RecordLimit` immediately before `topics` in the first
`build_share_fetch_request` call and change the whole-value expectation to:

```rust
share_acquire_mode: 1,
```

Pass `ShareAcquireMode::BatchOptimized` immediately before `Vec::new()` in the
second call and extend its final assertion to:

```rust
assert2::assert!(
    (saturated.max_wait_ms, saturated.share_acquire_mode) == (i32::MAX, 0)
);
```

Update the test module's private `test_consumer` constructor with:

```rust
acquire_mode: ShareAcquireMode::BatchOptimized,
```

Do the same in the private `test_consumer` constructor in
`crates/client-consumer/src/share/consumer.rs`. These fixture changes keep
direct struct construction exhaustive after the production field is added.

- [ ] **Step 4: Run focused tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer acquire_mode --lib --locked
```

Expected: compilation fails because `ShareAcquireMode` and the consumer field do
not exist and `build_share_fetch_request` does not accept the new argument.

- [ ] **Step 5: Implement the typed public mode**

In `crates/client-consumer/src/share/types.rs`, add beside `ShareAckMode`:

```rust
/// How a ShareFetch applies its maximum-record limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShareAcquireMode {
    /// Acquire complete record batches, which may exceed the requested record
    /// limit. This preserves the existing Kafka-compatible behavior.
    #[default]
    BatchOptimized,
    /// Stop acquiring when the requested record limit is reached.
    RecordLimit,
}

impl ShareAcquireMode {
    /// The `i8` wire value carried in ShareFetch version 2 and later.
    pub(crate) fn wire(self) -> i8 {
        match self {
            ShareAcquireMode::BatchOptimized => 0,
            ShareAcquireMode::RecordLimit => 1,
        }
    }
}
```

In `crates/client-consumer/src/share/mod.rs`, extend the type re-export:

```rust
pub use types::{ShareAckMode, ShareAckType, ShareAcquireMode, ShareConsumerRecord};
```

In `crates/client-consumer/src/lib.rs`, add `ShareAcquireMode` to the existing
`pub use share::{...}` list.

- [ ] **Step 6: Route the semantic value to ShareFetch**

In `crates/client-consumer/src/share/consumer.rs`, import
`ShareAcquireMode` beside `ShareAckMode`.

Add this field immediately after `fetch_max_records`:

```rust
pub(crate) acquire_mode: ShareAcquireMode,
```

Add this builder input immediately after `ack_mode`:

```rust
#[builder(default = ShareAcquireMode::BatchOptimized)]
acquire_mode: ShareAcquireMode,
```

Store `acquire_mode` in the final `ShareConsumer` literal.

In `crates/client-consumer/src/share/poll.rs`, import `ShareAcquireMode`, add it
to `build_share_fetch_request` immediately after `max_records`, and set the
request field explicitly:

```rust
share_acquire_mode: acquire_mode.wire(),
```

Pass `self.acquire_mode` from `ShareConsumer::poll` immediately after
`self.fetch_max_records`.

- [ ] **Step 7: Verify focused behavior and the package**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer acquire_mode --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
```

Expected: both commands exit 0. The focused run proves the default and exact
wire mapping; the package run proves all direct constructors and existing
share-consumer behavior still compile and pass.

- [ ] **Step 8: Commit the implementation**

Review the exact staged scope:

```bash
git diff --check
git add -- \
  crates/client-consumer/src/share/types.rs \
  crates/client-consumer/src/share/consumer.rs \
  crates/client-consumer/src/share/poll.rs \
  crates/client-consumer/src/share/mod.rs \
  crates/client-consumer/src/lib.rs
git diff --cached --check
git diff --cached --name-only
git commit -m "feat(consumer): expose share acquire mode"
```

Expected: only the five listed client-consumer files are committed.

---

### Task 2: Close the audit owner and verify the workspace

**Files:**

- Modify: `docs/configuration-audit.md`

**Interfaces:**

- Consumes: the implemented `ShareAcquireMode` builder-to-request path.
- Produces: a `Share Consumer Acquire Mode` audit section with exact scanner
  counts, exclusive classification, verification results, and the next real
  unresolved operational owner.

- [ ] **Step 1: Capture reproducible audit evidence**

Run:

```bash
tools/audit-runtime-values.sh > /tmp/share-acquire-mode-runtime-audit.txt
wc -l /tmp/share-acquire-mode-runtime-audit.txt
cut -d: -f1 /tmp/share-acquire-mode-runtime-audit.txt | sort -u | wc -l
rg -n \
  "share_acquire_mode|ShareAcquireMode|BatchOptimized|RecordLimit|acquire_mode" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_share_consumer.rs \
  docs/configuration-audit.md \
  > /tmp/share-acquire-mode-focused-audit.txt
wc -l /tmp/share-acquire-mode-focused-audit.txt
```

Inspect every focused line and classify it exactly once as production policy
flow, test/harness evidence, prior-audit text, or unresolved owner. Confirm the
category counts sum to the focused total.

Review the remaining production entries in the full scanner output and select
the next value that represents real operational policy. Do not select protocol
codes, sentinels, test fixtures, ignored arguments, or values already backed by
configuration.

- [ ] **Step 2: Add the completed audit section**

Append `## Share Consumer Acquire Mode` to
`docs/configuration-audit.md`. Record:

- `BatchOptimized` as the default and wire value `0`.
- `RecordLimit` as wire value `1`.
- The exact live flow:

  ```text
  ShareConsumer::builder().acquire_mode
    -> ShareConsumer::acquire_mode
    -> ShareConsumer::poll
    -> build_share_fetch_request
    -> ShareFetchRequest::share_acquire_mode
  ```

- Version 1 compatibility through the generated encoder's existing
  version-gated field omission.
- Why no `refined_type`, string parser, CLI/environment setting, CRD, or
  operator field was added.
- The full scanner line/file counts and the focused search command.
- The mutually exclusive focused classification and arithmetic proving it
  covers every focused line.
- A named `### Adjacent Pending Policy` selected from the reviewed production
  scanner entries, with a concrete reason it is operational and still
  unresolved.
- Confirmation that `Cargo.lock` is unchanged.

- [ ] **Step 3: Run final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --workspace --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
git diff --exit-code -- Cargo.lock
```

Expected: all commands exit 0. Record the actual test summaries and any
pre-existing Clippy warnings in the audit; do not describe a failed or skipped
command as passing.

- [ ] **Step 4: Reconcile formatting and audit evidence**

Run the focused audit command from Step 1 again after formatting and the audit
edit. Recalculate its total and exclusive categories, then correct the recorded
numbers if the documentation itself changed the result.

Inspect:

```bash
git status --short
git diff -- \
  crates/client-consumer/src/share/types.rs \
  crates/client-consumer/src/share/consumer.rs \
  crates/client-consumer/src/share/poll.rs \
  crates/client-consumer/src/share/mod.rs \
  crates/client-consumer/src/lib.rs \
  docs/configuration-audit.md
```

Expected: only the intended acquire-mode implementation and audit changes appear
in this slice; unrelated dirty and untracked paths remain unstaged.

- [ ] **Step 5: Commit the audit**

```bash
git add -- docs/configuration-audit.md
git diff --cached --check
git diff --cached --name-only
git commit -m "docs(audit): record share acquire mode"
```

Expected: only `docs/configuration-audit.md` is committed.
