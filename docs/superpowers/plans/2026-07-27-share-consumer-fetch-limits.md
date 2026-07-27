# ShareConsumer Fetch Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ShareConsumer's fixed ShareFetch byte and record limits with validated public builder settings while preserving the exact supported-wire defaults.

**Architecture:** Define three public semantic `i32` values beside `ShareConsumer`, validate raw builder inputs before client construction, and store the validated values on the consumer. Pass them into every ShareFetch, using the configured record limit for both `max_records` and `batch_size`. Delete the version-0-only per-partition byte constant and leave that generated field at its zero default.

**Tech Stack:** Rust, Bon builders, `refined_type::rule::GreaterI32`, CrabKafka protocol/client crates, Cargo, rustfmt, Clippy.

## Global Constraints

- This slice changes only `ShareConsumer`; classic `Consumer`, Client Streams, and broker policy remain unchanged.
- Preserve these exact defaults: minimum bytes `1`, maximum bytes `52_428_800`, and maximum records `500`.
- Accept each setting only in `1..=i32::MAX`; reject zero and negative values.
- Reject `fetch_min_bytes > fetch_max_bytes`.
- Use `refined_type::rule::GreaterI32<0>`; do not add a dependency or shared fetch-policy abstraction.
- Keep public builder setters as raw `i32`; store the validated raw `i32` values privately.
- Validate after the existing subscription and group-id checks and before either `Client` is constructed or any network I/O occurs.
- Invalid values return `ConsumerError::RebalanceFailed` with a ShareConsumer-specific setting name.
- Set ShareFetch `batch_size` equal to the configured maximum-record count; do not add a separate batch-size setting.
- Keep caller-supplied `poll(timeout)` as the sole owner of ShareFetch `max_wait_ms`.
- Delete `PARTITION_MAX_BYTES`; supported ShareFetch versions 1 and 2 do not encode the version-0-only field, so leave `FetchPartition::partition_max_bytes` at its generated zero default.
- Preserve assignment grouping, acknowledgement behavior, session epochs, decoding, and error propagation.
- Export all three constants and semantic types from `share` and re-export them from the crate root.
- This is library-only because the repository has no production ShareConsumer owner; add no CLI, environment variable, demo service, CRD, or operator field.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`, and use `--locked`.
- Do not change `Cargo.lock`.

---

### Task 1: Configure and enforce ShareConsumer fetch limits

**Files:**

- Modify: `crates/client-consumer/src/share/consumer.rs`
- Modify: `crates/client-consumer/src/share/poll.rs`
- Modify: `crates/client-consumer/src/share/mod.rs`
- Modify: `crates/client-consumer/src/lib.rs`
- Test: `crates/client-consumer/src/share/consumer.rs`
- Test: `crates/client-consumer/src/share/poll.rs`

**Interfaces:**

- Consumes: the existing workspace dependency `refined_type`, `ConsumerError::RebalanceFailed`, `ShareConsumer::builder()`, and `ShareFetchRequest`.
- Produces: three public default constants, `ShareConsumerFetchMinBytes`, `ShareConsumerFetchMaxBytes`, `ShareConsumerFetchMaxRecords`, raw builder setters with matching names, private consumer fields, and configured ShareFetch request values.

- [ ] **Step 1: Add failing semantic-value tests**

In `crates/client-consumer/src/share/consumer.rs`, add these tests to the existing `tests` module:

```rust
#[test]
fn share_fetch_limits_use_defaults_and_valid_overrides() {
    check!(
        ShareConsumerFetchMinBytes::default().bytes()
            == DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES
    );
    check!(
        ShareConsumerFetchMaxBytes::default().bytes()
            == DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES
    );
    check!(
        ShareConsumerFetchMaxRecords::default().records()
            == DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS
    );

    check!(ShareConsumerFetchMinBytes::new(7).unwrap().bytes() == 7);
    check!(
        ShareConsumerFetchMaxBytes::new(65_536).unwrap().bytes() == 65_536
    );
    check!(
        ShareConsumerFetchMaxRecords::new(37).unwrap().records() == 37
    );
}

#[test]
fn share_fetch_limits_validate_boundaries() {
    for invalid in [-1, 0] {
        check!(
            ShareConsumerFetchMinBytes::new(invalid)
                .unwrap_err()
                .contains("share consumer fetch min bytes")
        );
        check!(
            ShareConsumerFetchMaxBytes::new(invalid)
                .unwrap_err()
                .contains("share consumer fetch max bytes")
        );
        check!(
            ShareConsumerFetchMaxRecords::new(invalid)
                .unwrap_err()
                .contains("share consumer fetch max records")
        );
    }

    check!(
        ShareConsumerFetchMinBytes::new(i32::MAX)
            .unwrap()
            .bytes()
            == i32::MAX
    );
    check!(
        ShareConsumerFetchMaxBytes::new(i32::MAX)
            .unwrap()
            .bytes()
            == i32::MAX
    );
    check!(
        ShareConsumerFetchMaxRecords::new(i32::MAX)
            .unwrap()
            .records()
            == i32::MAX
    );
}
```

- [ ] **Step 2: Add the failing pre-I/O cross-setting test**

In the same test module, add:

```rust
#[tokio::test]
async fn invalid_share_fetch_limits_fail_before_broker_lookup() {
    let error = ShareConsumer::builder()
        .bootstrap("invalid.invalid:9092")
        .group_id("fetch-limit-validation")
        .subscribe(["topic".to_owned()])
        .fetch_min_bytes(2)
        .fetch_max_bytes(1)
        .build()
        .await
        .err()
        .expect("minimum above maximum must fail");

    check!(
        error
            .to_string()
            .contains("share consumer fetch min bytes must not exceed fetch max bytes")
    );
}
```

The deliberately unresolvable bootstrap proves that relation validation happens before broker lookup.

- [ ] **Step 3: Run focused tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_limits --locked
```

Expected: compilation fails because the semantic types, constants, and builder setters do not exist.

- [ ] **Step 4: Implement the public validated values**

In `crates/client-consumer/src/share/consumer.rs`, extend the rule import:

```rust
use refined_type::rule::{GreaterI32, MinMaxU128};
```

Add these constants and types beside the existing leave-heartbeat configuration:

```rust
/// Default minimum response bytes for a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES: i32 = 1;
/// Default maximum response bytes for a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES: i32 = 52_428_800;
/// Default maximum records acquired by a `ShareFetch`.
pub const DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS: i32 = 500;

/// Validated minimum response bytes for a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMinBytes(i32);

impl ShareConsumerFetchMinBytes {
    /// Validate a positive minimum byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch min bytes: {error}"))
    }

    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }
}

impl Default for ShareConsumerFetchMinBytes {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES)
            .expect("default share consumer fetch min bytes is valid")
    }
}

/// Validated maximum response bytes for a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMaxBytes(i32);

impl ShareConsumerFetchMaxBytes {
    /// Validate a positive maximum byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch max bytes: {error}"))
    }

    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }
}

impl Default for ShareConsumerFetchMaxBytes {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES)
            .expect("default share consumer fetch max bytes is valid")
    }
}

/// Validated maximum records acquired by a `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerFetchMaxRecords(i32);

impl ShareConsumerFetchMaxRecords {
    /// Validate a positive maximum record count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("share consumer fetch max records: {error}"))
    }

    /// Return the validated record count.
    #[must_use]
    pub const fn records(self) -> i32 {
        self.0
    }
}

impl Default for ShareConsumerFetchMaxRecords {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS)
            .expect("default share consumer fetch max records is valid")
    }
}
```

- [ ] **Step 5: Add builder validation and stored consumer fields**

Add these private fields to `ShareConsumer` immediately after `share_session_epoch`:

```rust
pub(crate) fetch_min_bytes: i32,
pub(crate) fetch_max_bytes: i32,
pub(crate) fetch_max_records: i32,
```

Add these raw inputs to `ShareConsumer::start` after `ack_mode`:

```rust
#[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES)]
fetch_min_bytes: i32,
#[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES)]
fetch_max_bytes: i32,
#[builder(default = DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS)]
fetch_max_records: i32,
```

After the existing empty-subscription and empty-group checks, but before leave-heartbeat validation and the first `Client::builder()`, add:

```rust
let fetch_min_bytes = ShareConsumerFetchMinBytes::new(fetch_min_bytes)
    .map_err(ConsumerError::RebalanceFailed)?
    .bytes();
let fetch_max_bytes = ShareConsumerFetchMaxBytes::new(fetch_max_bytes)
    .map_err(ConsumerError::RebalanceFailed)?
    .bytes();
let fetch_max_records = ShareConsumerFetchMaxRecords::new(fetch_max_records)
    .map_err(ConsumerError::RebalanceFailed)?
    .records();
if fetch_min_bytes > fetch_max_bytes {
    return Err(ConsumerError::RebalanceFailed(
        "share consumer fetch min bytes must not exceed fetch max bytes".to_owned(),
    ));
}
```

Forward the three values in the returned `ShareConsumer`. Add the three exact defaults to both direct test fixtures in `consumer.rs` and `poll.rs`.

- [ ] **Step 6: Export the public configuration API**

In `crates/client-consumer/src/share/mod.rs`, export:

```rust
pub use consumer::{
    DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES, DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
    DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES, DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT,
    ShareConsumer, ShareConsumerFetchMaxBytes, ShareConsumerFetchMaxRecords,
    ShareConsumerFetchMinBytes, ShareConsumerLeaveHeartbeatTimeout,
};
```

Mirror those six new names in the `pub use share::{...};` block in `crates/client-consumer/src/lib.rs`.

- [ ] **Step 7: Run focused semantic and builder validation tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_limits --locked
```

Expected: all three focused tests pass.

- [ ] **Step 8: Change request tests first**

In `crates/client-consumer/src/share/poll.rs`, rename `share_fetch_request_preserves_wire_fields_and_timeout_bounds` to `share_fetch_request_preserves_configured_limits_and_timeout_bounds`.

Pass distinctive values `7`, `65_536`, and `37` to both `build_share_fetch_request` calls, immediately after `timeout`. Update the first expected request to:

```rust
min_bytes: 7,
max_bytes: 65_536,
max_records: 37,
batch_size: 37,
```

In `share_fetch_topics_group_assignment_and_attach_partition_acks`, change the tuple expectation to:

```rust
(
    part.partition_max_bytes,
    part.acknowledgement_batches.as_slice()
) == (0, &[ack][..])
```

- [ ] **Step 9: Run the request tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_request_preserves_configured_limits_and_timeout_bounds --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_topics_group_assignment_and_attach_partition_acks --locked
```

Expected: the request test does not compile with the old helper signature, and the topic test fails because the old code still writes `1_048_576`.

- [ ] **Step 10: Wire stored values into every ShareFetch**

Delete `MAX_BYTES`, `PARTITION_MAX_BYTES`, and `MAX_RECORDS` from `poll.rs`.

Remove `partition_max_bytes: PARTITION_MAX_BYTES,` from `build_share_fetch_topics`; retain `..Default::default()` so the generated field remains zero.

Extend `build_share_fetch_request` after `timeout`:

```rust
min_bytes: i32,
max_bytes: i32,
max_records: i32,
```

Use them in the request:

```rust
min_bytes,
max_bytes,
max_records,
batch_size: max_records,
```

In `ShareConsumer::poll`, pass:

```rust
self.fetch_min_bytes,
self.fetch_max_bytes,
self.fetch_max_records,
```

between `timeout` and `topics`.

- [ ] **Step 11: Run focused and complete package verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_limits --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_request_preserves_configured_limits_and_timeout_bounds --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer share_fetch_topics_group_assignment_and_attach_partition_acks --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: every test and lint passes; formatting and diff checks are silent; the `Cargo.lock` diff is empty.

- [ ] **Step 12: Commit the library change**

Stage only the four library files and commit:

```bash
git add \
  crates/client-consumer/src/share/consumer.rs \
  crates/client-consumer/src/share/poll.rs \
  crates/client-consumer/src/share/mod.rs \
  crates/client-consumer/src/lib.rs
git diff --cached --check
git commit -m "feat(share): configure fetch limits"
```

Expected: the commit contains the public validated types, builder flow, request propagation, exports, and tests, with no unrelated workspace files.

---

### Task 2: Record ownership, verification, and the next pending value

**Files:**

- Modify: `docs/configuration-audit.md`

**Interfaces:**

- Consumes: the completed public ShareConsumer fetch-limit flow and `tools/audit-runtime-values.sh`.
- Produces: an auditable `## ShareConsumer Fetch Limits` section with exclusive focused-search classification, verification evidence, the library-only deployment decision, and the next pending operational owner.

- [ ] **Step 1: Run the repository scanner and exact focused search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "fetch_min_bytes|fetch_max_bytes|fetch_max_records|ShareConsumerFetch(MinBytes|MaxBytes|MaxRecords)|DEFAULT_SHARE_CONSUMER_FETCH_(MIN_BYTES|MAX_BYTES|MAX_RECORDS)|PARTITION_MAX_BYTES|batch_size" \
  crates/client-consumer \
  docs/configuration-audit.md
```

Record both commands' line/file totals. Classify every focused result exactly once as:

- ShareConsumer production;
- classic Consumer production;
- test or harness;
- prior audit;
- unresolved owner.

The category sum must equal the focused-search total. Do not count the new audit section until after capturing the pre-append search.

- [ ] **Step 2: Append the audit section with exact implemented behavior**

Append `## ShareConsumer Fetch Limits` to `docs/configuration-audit.md`. State all of the following with the actual scanner counts from Step 1:

```text
- the three public semantic types accept 1 through i32::MAX;
- defaults remain exactly 1 minimum byte, 52,428,800 maximum bytes, and 500 maximum records;
- zero and negative values are rejected;
- a minimum greater than the maximum is rejected;
- ShareConsumer::builder() accepts raw i32 values and validates before Client construction or network I/O;
- the validated values are stored on ShareConsumer and used by every poll;
- max_records and batch_size both receive the configured maximum-record value;
- poll(timeout) remains the sole max_wait_ms control;
- PARTITION_MAX_BYTES was deleted because supported ShareFetch versions 1 and 2 do not encode the version-0-only field;
- FetchPartition::partition_max_bytes remains at its generated zero default;
- there is no CLI, environment variable, demo service, CRD, or operator field because no production process owns ShareConsumer;
- Cargo.lock remained unchanged;
- focused tests, all-target tests, strict Clippy, nightly formatting, and diff hygiene passed.
```

Include this exact flow:

```text
ShareConsumer::start
  -> validated ShareConsumer fetch fields
  -> ShareConsumer::poll
  -> build_share_fetch_request
     -> min_bytes
     -> max_bytes
     -> max_records
     -> batch_size = max_records
```

Under `### Adjacent Pending Policy`, identify the next scanner-visible operational owner as `share_acquire_mode: 0` in `crates/client-consumer/src/share/poll.rs`. It is a supported ShareFetch policy field and needs its own design decision; do not fold it into this slice or claim the repository-wide goal is complete.

- [ ] **Step 3: Verify audit text and rerun final gates**

Run:

```bash
rg -n \
  "fetch_min_bytes|fetch_max_bytes|fetch_max_records|ShareConsumerFetch(MinBytes|MaxBytes|MaxRecords)|DEFAULT_SHARE_CONSUMER_FETCH_(MIN_BYTES|MAX_BYTES|MAX_RECORDS)|PARTITION_MAX_BYTES|batch_size|share_acquire_mode" \
  crates/client-consumer \
  docs/configuration-audit.md
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: the focused search includes the new audit references; all tests and lint pass; formatting and diff checks are silent; the `Cargo.lock` diff remains empty.

- [ ] **Step 4: Commit the audit record**

Stage only the audit file and commit:

```bash
git add docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(share): record fetch limits"
```

Expected: the commit contains only the completed-slice audit record.

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git log --oneline ef09f2a7..HEAD
git diff --stat ef09f2a7..HEAD
git diff --check ef09f2a7..HEAD
git diff -- Cargo.lock
```

Inspect the full diff and confirm it contains only the implementation plan, the four intended library files, their tests, and the configuration-audit update. Confirm once more that defaults, validation ordering, request propagation, protocol-version cleanup, public exports, library-only ownership, and the unchanged lockfile match the approved design.
