# Client Consumer Subscription Metadata Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the classic Consumer's fixed five-second subscribed-topic metadata refresh interval with validated public builder configuration while preserving recovery behavior.

**Architecture:** Add one public refined duration type beside the existing classic Consumer leave timeout, validate the raw builder input before startup side effects, and carry the resulting `Duration` through `StartConfig` into `CoordinatorState`. Replace only the coordinator's fixed elapsed-time threshold, keeping heartbeat wakeups as the sole timer and using a private predicate for the inclusive boundary.

**Tech Stack:** Rust, `refined_type::rule::MinMaxU128`, Tokio paused time, Bon builders, CrabKafka client integration tests, Cargo, Clippy, rustfmt, ripgrep.

## Global Constraints

- Change only classic `Consumer`; `ShareConsumer` remains unchanged.
- Preserve the exact default of five seconds.
- Accept positive whole-millisecond durations from 1 millisecond through
  `u64::MAX` milliseconds.
- Reject zero, fractional milliseconds, and larger durations.
- Use the existing `refined_type::rule::MinMaxU128` dependency.
- Add a raw `Duration` builder setter named
  `subscription_metadata_refresh_interval`.
- Validate after the existing subscription, group-id, group-instance-id, and
  fetch-budget checks, but before `StartConfig` enters the retry loop, before
  `Client` construction, and before network I/O.
- Invalid values return `ConsumerError::RebalanceFailed` with the setting name
  `consumer subscription metadata refresh interval`.
- Carry the validated value through `StartConfig` and `Consumer::start_once`
  into `CoordinatorState`; do not retain it on the returned `Consumer`.
- Preserve heartbeat cadence, missed-tick behavior, no refresh while rejoining,
  best-effort metadata error handling, growth detection, monotonic baseline
  merging, assignment, commit, leave-group, and shutdown behavior.
- Keep heartbeat-loop wakeups as the only refresh opportunity; add no
  independent timer or sub-heartbeat precision promise.
- Add no disable switch, shared cadence abstraction, external dependency, CLI,
  environment variable, CRD, or operator field in this library slice.
- Keep `observability-demo-app` as the first production propagation owner after
  this slice; keep the separately approved `ShareAcquireMode::BatchOptimized`
  default queued.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate and apply the metadata refresh interval

**Files:**

- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/coordinator.rs`
- Modify: `crates/client-consumer/src/lib.rs`
- Modify: `crates/integration-tests/tests/consumer_integration.rs`

**Interfaces:**

- Consumes: existing `MinMaxU128`, `ConsumerError::RebalanceFailed`,
  `Consumer::builder()`, `StartConfig`, `CoordinatorState`, and the coordinator
  heartbeat loop.
- Produces:
  `pub const DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL: Duration`
  with the value `Duration::from_secs(5)`.
- Produces:
  `pub struct ConsumerSubscriptionMetadataRefreshInterval(Duration)`.
- Produces:
  `ConsumerSubscriptionMetadataRefreshInterval::new(Duration) -> Result<Self, String>`.
- Produces:
  `ConsumerSubscriptionMetadataRefreshInterval::duration(self) -> Duration`.
- Produces:
  `ConsumerSubscriptionMetadataRefreshInterval::milliseconds(self) -> u64`.
- Produces: raw
  `Consumer::builder().subscription_metadata_refresh_interval(Duration)`.
- Carries: `subscription_metadata_refresh_interval: Duration` through
  `StartConfig` and `CoordinatorState`.
- Produces:
  `subscription_metadata_refresh_due(Instant, Duration) -> bool`.

- [ ] **Step 1: Add failing semantic-type and pre-I/O tests**

In the existing `consumer.rs` test module, add:

```rust
#[test]
fn subscription_metadata_refresh_interval_uses_default_and_valid_override() {
    let default = ConsumerSubscriptionMetadataRefreshInterval::default();
    assert2::assert!(default.duration() == Duration::from_secs(5));
    assert2::assert!(default.milliseconds() == 5_000);

    let interval =
        ConsumerSubscriptionMetadataRefreshInterval::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
    assert2::assert!(interval.duration() == Duration::from_millis(37));
    assert2::assert!(interval.milliseconds() == 37);
}

#[test]
fn subscription_metadata_refresh_interval_validates_millisecond_boundaries() {
    assert2::assert!(
        ConsumerSubscriptionMetadataRefreshInterval::new(Duration::ZERO).is_err()
    );
    assert2::assert!(
        ConsumerSubscriptionMetadataRefreshInterval::new(
            Duration::from_millis(1) + Duration::from_nanos(1)
        )
        .is_err()
    );
    assert2::assert!(
        ConsumerSubscriptionMetadataRefreshInterval::new(
            Duration::from_millis(u64::MAX)
        )
        .is_ok()
    );
    assert2::assert!(
        ConsumerSubscriptionMetadataRefreshInterval::new(
            Duration::from_secs(u64::MAX)
        )
        .is_err()
    );
}

#[tokio::test]
async fn invalid_subscription_metadata_refresh_interval_fails_before_broker_lookup() {
    let error = Consumer::builder()
        .bootstrap("invalid.invalid:9092")
        .group_id("metadata-refresh-validation")
        .subscribe(["topic".to_owned()])
        .subscription_metadata_refresh_interval(Duration::ZERO)
        .build()
        .await
        .err()
        .expect("invalid configuration");

    assert2::assert!(
        error
            .to_string()
            .contains("consumer subscription metadata refresh interval")
    );
}
```

The deliberately unresolvable bootstrap proves invalid input fails before
broker lookup.

- [ ] **Step 2: Add the failing inclusive elapsed-time test**

In `coordinator.rs`'s existing `retry_tests` module, add:

```rust
#[tokio::test(start_paused = true)]
async fn subscription_metadata_refresh_due_uses_configured_inclusive_boundary() {
    let last_check = tokio::time::Instant::now();
    let interval = Duration::from_millis(37);

    tokio::time::advance(Duration::from_millis(36)).await;
    assert2::assert!(!subscription_metadata_refresh_due(last_check, interval));

    tokio::time::advance(Duration::from_millis(1)).await;
    assert2::assert!(subscription_metadata_refresh_due(last_check, interval));
}
```

Also add a distinctive field to the explicit `CoordinatorState` literal in
`coordinator_leave_group_uses_configured_timeout`:

```rust
subscription_metadata_refresh_interval: Duration::from_millis(37),
```

This keeps test construction exhaustive when the production state gains the
new field.

- [ ] **Step 3: Run focused tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer subscription_metadata_refresh --locked
```

Expected: compilation fails because the semantic type, constant, builder
setter, coordinator field, and elapsed-time predicate do not exist.

- [ ] **Step 4: Implement the public validated duration**

In `consumer.rs`, beside `ConsumerLeaveGroupTimeout`, add:

```rust
/// Default cadence for checking subscribed-topic metadata changes.
pub const DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL: Duration =
    Duration::from_secs(5);

/// Positive, whole-millisecond subscribed-topic metadata refresh cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerSubscriptionMetadataRefreshInterval(Duration);

impl ConsumerSubscriptionMetadataRefreshInterval {
    /// Validate a subscribed-topic metadata refresh interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| {
                format!("consumer subscription metadata refresh interval: {error}")
            })?
            .into_value();
        let milliseconds = u64::try_from(milliseconds).map_err(|error| {
            format!("consumer subscription metadata refresh interval: {error}")
        })?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "consumer subscription metadata refresh interval must be a whole number of milliseconds"
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

    /// Return the validated duration in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics only if the invariant established by [`Self::new`] is violated.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated consumer subscription metadata refresh interval fits u64")
    }
}

impl Default for ConsumerSubscriptionMetadataRefreshInterval {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL)
            .expect("default consumer subscription metadata refresh interval is valid")
    }
}
```

Reuse the existing `MinMaxU128` import. Do not add or change dependencies.

- [ ] **Step 5: Validate once and carry the duration into coordinator state**

Add this field to `StartConfig`:

```rust
subscription_metadata_refresh_interval: Duration,
```

Add this raw builder parameter immediately after `heartbeat_interval`:

```rust
#[builder(default = DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL)]
subscription_metadata_refresh_interval: std::time::Duration,
```

After the existing leave-group timeout validation and before constructing
`StartConfig`, add:

```rust
let subscription_metadata_refresh_interval =
    ConsumerSubscriptionMetadataRefreshInterval::new(
        subscription_metadata_refresh_interval,
    )
    .map_err(ConsumerError::RebalanceFailed)?;
```

Initialize `StartConfig` with:

```rust
subscription_metadata_refresh_interval:
    subscription_metadata_refresh_interval.duration(),
```

In `spawn_consumer`, add `subscription_metadata_refresh_interval` to the
explicit `StartConfig` destructure. Add the same `Duration` field to
`CoordinatorState` beside `heartbeat_interval`, then initialize it in the
state literal:

```rust
subscription_metadata_refresh_interval,
```

Do not add the field to the returned `Consumer`.

- [ ] **Step 6: Replace only the fixed coordinator threshold**

Delete `SUBSCRIPTION_METADATA_REFRESH` from `coordinator.rs` and add:

```rust
fn subscription_metadata_refresh_due(
    last_check: tokio::time::Instant,
    interval: Duration,
) -> bool {
    last_check.elapsed() >= interval
}
```

Replace:

```rust
if !needs_rejoin && last_meta_check.elapsed() >= SUBSCRIPTION_METADATA_REFRESH {
```

with:

```rust
if !needs_rejoin
    && subscription_metadata_refresh_due(
        last_meta_check,
        state.subscription_metadata_refresh_interval,
    )
{
```

Leave `last_meta_check` initialization and advancement unchanged. Do not add a
timer, alter the heartbeat ticker, refresh while `needs_rejoin` is true, or
change metadata error handling and count merging.

- [ ] **Step 7: Export the public configuration**

In `lib.rs`, extend the consumer re-export:

```rust
pub use consumer::{
    Consumer, ConsumerLeaveGroupTimeout, ConsumerRecord,
    ConsumerSubscriptionMetadataRefreshInterval,
    DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT,
    DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL, Header,
};
```

- [ ] **Step 8: Exercise the override on the existing recovery integration**

In
`crates/integration-tests/tests/consumer_integration.rs::cold_start_rejoins_when_subscribed_topic_appears`,
add the distinctive override after `heartbeat_interval`:

```rust
.subscription_metadata_refresh_interval(Duration::from_millis(750))
```

Update the nearby comment to say recovery is driven by the configured metadata
refresh interval and bounded by heartbeat wakeups. Keep the 30-second outer
deadline: it protects slow CI and tests recovery semantics, not exact wall-clock
precision.

- [ ] **Step 9: Run focused green tests and the recovery integration**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer subscription_metadata_refresh --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-integration-tests --test consumer_integration cold_start_rejoins_when_subscribed_topic_appears --locked
```

Expected: all focused unit tests pass, and the single-member consumer recovers
after its subscribed topic appears while using the distinctive override.

- [ ] **Step 10: Run Task 1 package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-integration-tests --test consumer_integration --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: tests, strict Clippy, formatting, and diff hygiene pass;
`Cargo.lock` has no diff.

- [ ] **Step 11: Commit Task 1**

Stage only the four Task 1 files:

```bash
git add -- \
  crates/client-consumer/src/consumer.rs \
  crates/client-consumer/src/coordinator.rs \
  crates/client-consumer/src/lib.rs \
  crates/integration-tests/tests/consumer_integration.rs
git diff --cached --check
git commit -m "feat(consumer): configure metadata refresh"
```

Expected: the commit contains only the validated library setting, its
coordinator propagation, public exports, tests, and the focused recovery
integration update.

---

### Task 2: Record the library slice and remaining owners

**Files:**

- Modify: `docs/configuration-audit.md`

**Interfaces:**

- Consumes: the completed classic Consumer metadata-refresh flow and
  `tools/audit-runtime-values.sh`.
- Produces: an exclusive focused-search classification, an explicit
  `observability-demo-app` follow-up owner, and the parked
  `ShareAcquireMode::BatchOptimized` decision.

- [ ] **Step 1: Run the repository scanner and focused search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "SUBSCRIPTION_METADATA_REFRESH|subscription_metadata_refresh|ConsumerSubscriptionMetadataRefreshInterval|DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL|subscribed_partition_counts|ShareAcquireMode|BatchOptimized" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_integration.rs \
  crates/observability-demo-app \
  docs/configuration-audit.md
```

Record the exact scanner line/file totals. Classify every focused-search line
exactly once as classic Consumer production, ShareConsumer production,
integration test, observability-demo owner, prior audit, parked acquisition
mode, or unresolved owner. Verify that category totals equal the focused-search
total.

- [ ] **Step 2: Append the completed audit section**

Append `## Client Consumer Subscription Metadata Refresh Interval` to
`docs/configuration-audit.md`. State:

- the positive whole-millisecond range and exact 5,000-millisecond default;
- the raw builder setter and pre-retry/pre-I/O validation boundary;
- the exact
  `Consumer::start -> StartConfig -> start_once -> CoordinatorState -> run`
  flow;
- the inclusive elapsed-time threshold and heartbeat-wakeup precision bound;
- unchanged no-refresh-during-rejoin, best-effort metadata-error, growth
  detection, and monotonic baseline semantics;
- the exact scanner and focused-search commands, totals, and exclusive
  classifications from Step 1;
- Task 1 verification results and unchanged `Cargo.lock`;
- that this library slice adds no CLI, environment variable, or CRD;
- that `observability-demo-app` is the first production configuration owner to
  propagate next, using only its Consume role and `demo-consume` service and no
  CRD; and
- that the separately queued ShareAcquireMode slice uses the approved
  `BatchOptimized` default.

Add `### Adjacent Pending Policy` and explicitly keep both the demo propagation
slice and the repository-wide hardcoded-operational-value objective open.

- [ ] **Step 3: Run fresh final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-integration-tests --test consumer_integration cold_start_rejoins_when_subscribed_topic_appears --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-integration-tests --test consumer_integration --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; `Cargo.lock` has no diff.

- [ ] **Step 4: Commit Task 2**

Stage only the audit file:

```bash
git add -- docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(consumer): record metadata refresh"
```

Expected: the commit contains only the completed library-slice audit record.

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git log --oneline 14c0865c..HEAD
git diff --stat 14c0865c..HEAD
git diff --check 14c0865c..HEAD
git diff 14c0865c..HEAD -- Cargo.lock
git status --short
```

Inspect the full range and confirm it contains only this implementation plan,
the three intended library files, the one focused integration test, and the
configuration-audit update. Confirm defaults, validation ordering, state
propagation, inclusive threshold behavior, heartbeat precision, public
exports, remaining owner classification, and the unchanged lockfile match the
approved design. Confirm all pre-existing unrelated dirty and untracked files
remain unstaged.
