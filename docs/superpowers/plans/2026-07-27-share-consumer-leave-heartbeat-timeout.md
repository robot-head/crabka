# ShareConsumer Leave-Heartbeat Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ShareConsumer's fixed five-second final leave-heartbeat deadline with a validated public builder setting while preserving the exact existing shutdown behavior.

**Architecture:** Define the public semantic value beside `ShareConsumer`, validate the raw builder duration before any client construction, and carry the validated raw `Duration` in the private coordinator state. Extract the existing final send into a small `leave_group` helper so a stalled mock broker can prove that shutdown observes the configured deadline without exercising the long-running heartbeat loop.

**Tech Stack:** Rust, Tokio, Bon builders, `refined_type::rule::MinMaxU128`, CrabKafka protocol/client crates, `MockBroker`, Cargo, rustfmt, Clippy.

## Global Constraints

- This slice changes only `ShareConsumer`; classic `Consumer` and Client Streams remain unchanged.
- `DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT` is exactly five seconds.
- Accept only positive, whole-millisecond durations in `1..=u64::MAX` milliseconds; reject zero, fractional milliseconds, and larger durations.
- Zero does not disable the final leave heartbeat.
- Use `refined_type::rule::MinMaxU128`; do not add a dependency or shared cross-protocol abstraction.
- Keep the public builder setter as raw `Duration`; carry the validated value privately as raw `Duration`.
- Validate after the existing subscription and group-id checks and before either `Client` is constructed or any network I/O occurs.
- Invalid values return `ConsumerError::RebalanceFailed` with a ShareConsumer-specific message.
- Preserve final acknowledgement flushing before coordinator cancellation and join.
- Preserve exactly one best-effort `ShareGroupHeartbeat` with `member_epoch = -1`; timeout, transport, and broker errors remain ignored, with no retry or disable switch.
- Export the constant and semantic type from `share` and re-export them from the crate root.
- This is library-only because the repository has no production ShareConsumer owner; add no CLI, environment variable, demo service, CRD, or operator field.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`, and use `--locked`.
- Do not change `Cargo.lock`.

---

### Task 1: Configure and enforce the ShareConsumer leave-heartbeat timeout

**Files:**

- Modify: `crates/client-consumer/src/share/consumer.rs`
- Modify: `crates/client-consumer/src/share/coordinator.rs`
- Modify: `crates/client-consumer/src/share/mod.rs`
- Modify: `crates/client-consumer/src/lib.rs`
- Test: `crates/client-consumer/src/share/consumer.rs`
- Test: `crates/client-consumer/src/share/coordinator.rs`

**Interfaces:**

- Consumes: the existing workspace dependency `refined_type`, `ConsumerError::RebalanceFailed`, `ShareConsumer::builder()`, `ShareCoordinatorState`, and `MockBroker`.
- Produces: `pub const DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT: Duration`, `pub struct ShareConsumerLeaveHeartbeatTimeout(Duration)`, `ShareConsumerLeaveHeartbeatTimeout::new(Duration) -> Result<Self, String>`, `duration(self) -> Duration`, `milliseconds(self) -> u64`, raw builder setter `.leave_heartbeat_timeout(Duration)`, private `ShareCoordinatorState::leave_heartbeat_timeout: Duration`, and private `async fn leave_group(&ShareCoordinatorState)`.

- [ ] **Step 1: Add failing semantic-value and early-validation tests**

In `crates/client-consumer/src/share/consumer.rs`, add these tests to the existing `tests` module:

```rust
#[test]
fn leave_heartbeat_timeout_uses_default_and_valid_override() {
    let default = ShareConsumerLeaveHeartbeatTimeout::default();
    check!(
        default.duration() == DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT
    );
    check!(default.milliseconds() == 5_000);

    let configured =
        ShareConsumerLeaveHeartbeatTimeout::new(Duration::from_millis(37)).unwrap();
    check!(configured.duration() == Duration::from_millis(37));
    check!(configured.milliseconds() == 37);
}

#[test]
fn leave_heartbeat_timeout_validates_millisecond_boundaries() {
    assert2::assert!(
        ShareConsumerLeaveHeartbeatTimeout::new(Duration::ZERO)
            .unwrap_err()
            .contains("share consumer leave-heartbeat timeout")
    );
    assert2::assert!(
        ShareConsumerLeaveHeartbeatTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1))
            .unwrap_err()
            .contains("whole number of milliseconds")
    );
    check!(
        ShareConsumerLeaveHeartbeatTimeout::new(Duration::from_millis(u64::MAX))
            .unwrap()
            .milliseconds()
            == u64::MAX
    );
    assert2::assert!(
        ShareConsumerLeaveHeartbeatTimeout::new(Duration::from_secs(u64::MAX))
            .unwrap_err()
            .contains("share consumer leave-heartbeat timeout")
    );
}

#[tokio::test]
async fn invalid_leave_heartbeat_timeout_fails_before_broker_lookup() {
    let error = ShareConsumer::builder()
        .bootstrap("invalid.invalid:9092")
        .group_id("leave-validation")
        .subscribe(["topic".to_owned()])
        .leave_heartbeat_timeout(Duration::ZERO)
        .build()
        .await
        .err()
        .expect("zero leave-heartbeat timeout must fail");

    assert2::assert!(
        error
            .to_string()
            .contains("share consumer leave-heartbeat timeout")
    );
}
```

The deliberately unresolvable bootstrap proves ordering: the expected validation error must be returned without attempting broker lookup.

- [ ] **Step 2: Run the focused tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_heartbeat_timeout --locked
```

Expected: compilation fails because `ShareConsumerLeaveHeartbeatTimeout`, `DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT`, and the builder setter do not exist.

- [ ] **Step 3: Implement the validated value and builder validation**

In `crates/client-consumer/src/share/consumer.rs`, import `MinMaxU128` and add the public constant and type above `ShareConsumer`:

```rust
use refined_type::rule::MinMaxU128;

/// Default deadline for the final best-effort ShareGroup leave heartbeat.
pub const DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT: Duration =
    Duration::from_secs(5);

/// Validated deadline for the final best-effort ShareGroup leave heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareConsumerLeaveHeartbeatTimeout(Duration);

impl ShareConsumerLeaveHeartbeatTimeout {
    /// Validate a positive, whole-millisecond timeout representable as `u64`.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value above
    /// `u64::MAX` milliseconds.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("share consumer leave-heartbeat timeout: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("share consumer leave-heartbeat timeout: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "share consumer leave-heartbeat timeout must be a whole number of milliseconds"
                    .to_owned(),
            );
        }
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated timeout in whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics only if this type's constructor invariant is broken.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated share consumer leave-heartbeat timeout fits u64")
    }
}

impl Default for ShareConsumerLeaveHeartbeatTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT)
            .expect("default share consumer leave-heartbeat timeout is valid")
    }
}
```

Add the raw input to `ShareConsumer::start` immediately after `heartbeat_interval`:

```rust
#[builder(default = DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT)]
leave_heartbeat_timeout: Duration,
```

After the existing empty-subscription and empty-group checks, but before the first `Client::builder()`, validate it:

```rust
let leave_heartbeat_timeout =
    ShareConsumerLeaveHeartbeatTimeout::new(leave_heartbeat_timeout)
        .map_err(ConsumerError::RebalanceFailed)?
        .duration();
```

When constructing `ShareCoordinatorState`, forward the validated duration:

```rust
leave_heartbeat_timeout,
```

- [ ] **Step 4: Export the public configuration API**

In `crates/client-consumer/src/share/mod.rs`, replace the consumer export with:

```rust
pub use consumer::{
    DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT, ShareConsumer,
    ShareConsumerLeaveHeartbeatTimeout,
};
```

In `crates/client-consumer/src/lib.rs`, replace the share re-export with:

```rust
pub use share::{
    DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT, ShareAckMode, ShareAckType, ShareConsumer,
    ShareConsumerLeaveHeartbeatTimeout, ShareConsumerRecord,
};
```

- [ ] **Step 5: Run the focused semantic and builder tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_heartbeat_timeout --locked
```

Expected: all three focused tests pass.

- [ ] **Step 6: Add the failing configured-deadline coordinator test**

In `crates/client-consumer/src/share/coordinator.rs`, extend the test imports:

```rust
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use bytes::BytesMut;
use crabka_client_core::MockBroker;
use crabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{
            ApiVersion, ApiVersionsResponse,
        },
        common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
        share_group_heartbeat_request,
    },
    tagged_fields::UnknownTaggedFields,
};
```

Keep the module's access to the existing outer `Arc` through the explicit test import. Add this API-version response helper:

```rust
fn api_versions_for_share_leave() -> Vec<u8> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: share_group_heartbeat_request::API_KEY,
                min_version: share_group_heartbeat_request::MIN_VERSION,
                max_version: share_group_heartbeat_request::MAX_VERSION,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buffer = BytesMut::new();
    response
        .encode(&mut buffer, 0)
        .expect("encode API versions");
    buffer.to_vec()
}
```

Add the test:

```rust
#[tokio::test]
async fn leave_group_uses_configured_timeout_for_one_best_effort_request() {
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&requests);
    let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            Some(api_versions_for_share_leave())
        } else if api_key == share_group_heartbeat_request::API_KEY {
            observed.fetch_add(1, Ordering::SeqCst);
            None
        } else {
            panic!("unexpected API key {api_key}");
        }
    })
    .await;
    let client = Client::builder()
        .bootstrap(mock.addr.to_string())
        .client_id("share-leave-timeout-test")
        .request_timeout(Duration::from_secs(5))
        .build()
        .await
        .expect("client");
    let state = ShareCoordinatorState {
        client,
        group_id: "group-a".into(),
        member_id: "member-a".into(),
        member_epoch: Arc::new(Mutex::new(3)),
        assignment: Arc::new(Mutex::new(Vec::new())),
        topic_names: Arc::new(Mutex::new(HashMap::new())),
        subscribe: vec!["topic-a".into()],
        heartbeat_interval: Duration::from_secs(1),
        leave_heartbeat_timeout: Duration::from_millis(37),
    };

    tokio::time::timeout(Duration::from_secs(1), leave_group(&state))
        .await
        .expect("configured leave deadline bounds shutdown");

    mock.stop();
    assert2::assert!(requests.load(Ordering::SeqCst) == 1);
}
```

Also add `leave_heartbeat_timeout: Duration::from_secs(5)` to the existing `state()` fixture.

- [ ] **Step 7: Run the coordinator test and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_group_uses_configured_timeout_for_one_best_effort_request --locked
```

Expected: compilation fails because `leave_group` and `ShareCoordinatorState::leave_heartbeat_timeout` do not exist.

- [ ] **Step 8: Implement the private coordinator deadline flow**

In `ShareCoordinatorState`, add:

```rust
pub leave_heartbeat_timeout: Duration,
```

Extract the existing final send into this helper:

```rust
async fn leave_group(state: &ShareCoordinatorState) {
    let leave = state.client.send(build_leave_heartbeat_request(
        state.group_id.clone(),
        state.member_id.clone(),
    ));
    let _ = tokio::time::timeout(state.leave_heartbeat_timeout, leave).await;
}
```

At the end of `run`, preserve the current ordering and replace only the inline hardcoded block with:

```rust
// Graceful departure: a leave heartbeat (`member_epoch = -1`) tells the
// broker to evict us now rather than waiting out the session timeout.
// Best-effort and bounded so a hung broker can't block `close()`.
leave_group(&state).await;
```

Do not change `ShareConsumer::close`: it must still flush final acknowledgements, cancel the heartbeat task, and await it.

- [ ] **Step 9: Run focused and complete package verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_heartbeat_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_group_uses_configured_timeout_for_one_best_effort_request --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: every test and lint passes; formatting and diff checks are silent; the `Cargo.lock` diff is empty.

- [ ] **Step 10: Commit the library change**

Stage only the four library files and commit:

```bash
git add \
  crates/client-consumer/src/share/consumer.rs \
  crates/client-consumer/src/share/coordinator.rs \
  crates/client-consumer/src/share/mod.rs \
  crates/client-consumer/src/lib.rs
git diff --cached --check
git commit -m "feat(share): configure leave timeout"
```

Expected: the commit contains the public validated type, builder flow, coordinator enforcement, exports, and tests, with no unrelated workspace files.

---

### Task 2: Record ownership, verification, and the next pending value

**Files:**

- Modify: `docs/configuration-audit.md`

**Interfaces:**

- Consumes: the completed public `ShareConsumerLeaveHeartbeatTimeout` flow and `tools/audit-runtime-values.sh`.
- Produces: an auditable `## ShareConsumer Leave-Heartbeat Timeout` section with exclusive focused-search classification, verification evidence, the library-only deployment decision, and the next pending operational owner.

- [ ] **Step 1: Run the repository scanner and exact focused search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "leave_heartbeat_timeout|ShareConsumerLeaveHeartbeatTimeout|DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT|build_leave_heartbeat_request|member_epoch: -1" \
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

- [ ] **Step 2: Append the audit section with the exact implemented behavior**

Append `## ShareConsumer Leave-Heartbeat Timeout` to `docs/configuration-audit.md`. State all of the following with the actual scanner counts from Step 1:

```text
- the public semantic type accepts positive whole milliseconds through u64::MAX and defaults to exactly 5,000 ms;
- zero, fractional milliseconds, and larger durations are rejected;
- ShareConsumer::builder() accepts raw Duration and validates before Client construction or network I/O;
- the validated Duration flows through ShareCoordinatorState to tokio::time::timeout;
- shutdown preserves final acknowledgement flush, then coordinator cancellation and join;
- the coordinator sends exactly one best-effort member_epoch = -1 heartbeat;
- timeout, transport, and broker errors remain ignored, with no retry or disable switch;
- there is no CLI, environment variable, demo service, CRD, or operator field because no production process owns ShareConsumer;
- Cargo.lock remained unchanged;
- focused tests, all-target tests, strict Clippy, nightly formatting, and diff hygiene passed.
```

Include this exact flow:

```text
ShareConsumer::start
  -> ShareCoordinatorState
  -> coordinator observes shutdown
  -> leave_group
  -> tokio::time::timeout(configured timeout, final heartbeat)
```

Under `### Adjacent Pending Policy`, identify the next scanner-visible operational owner as the ShareConsumer poll-fetch limits in `crates/client-consumer/src/share/poll.rs`: `MAX_BYTES = 52_428_800`, `PARTITION_MAX_BYTES = 1_048_576`, `MAX_RECORDS = 500`, and request `min_bytes = 1`. Do not claim the repository-wide goal is complete.

- [ ] **Step 3: Verify the audit text and rerun final gates**

Run:

```bash
rg -n \
  "leave_heartbeat_timeout|ShareConsumerLeaveHeartbeatTimeout|DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT|build_leave_heartbeat_request|member_epoch: -1" \
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
git commit -m "docs(share): record leave timeout"
```

Expected: the commit contains only the completed-slice audit record.

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git log --oneline f5e02b4d..HEAD
git diff --stat f5e02b4d..HEAD
git diff --check f5e02b4d..HEAD
git diff -- Cargo.lock
```

Inspect the full diff and confirm it contains only the implementation plan, the four intended library files, their tests, and the configuration-audit update. Confirm once more that shutdown semantics, public exports, validation ordering, library-only ownership, and the unchanged lockfile match the approved design.
