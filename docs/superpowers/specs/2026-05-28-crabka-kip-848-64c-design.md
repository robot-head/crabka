# Slice 64c — KIP-848 custom server-side assignor plugin point

**Status:** design
**Date:** 2026-05-28
**Roadmap:** follow-up to slice 64a (KIP-848 foundations, PR #260) and slice 64b (rack-aware UniformAssignor, PR #266). Closes one of the explicit "out of scope" bullets from 64a's STATUS entry.

## Goal

Let operators register custom server-side assignors at broker startup. Each registered assignor is selectable by name via the heartbeat request's `server_assignor` field, identical to how `"uniform"` and `"range"` work today.

## Non-goals

- Dynamic registration / removal at runtime. The list is fixed once `Broker::start` runs.
- Java-style reflection or runtime class loading. Custom assignors are Rust types compiled in.
- Extract the `Assignor` trait into a separate crate. (Possible future option; not needed for 64c.)
- Sample external assignor crate or examples beyond the test fixtures.
- Operator config-file syntax for assignor names. Operators register programmatically in their broker bootstrap.
- JVM-client engagement (separate follow-up tracked under slice 64a-followup).

## Architecture

### Architectural choice

Approach 1 from the brainstorm: the `Vec<String>` field on `NextGenConfig` becomes `Vec<Arc<dyn Assignor>>`. The list **is** the registry. There is no name-vs-impl indirection layer.

### Trait + visibility

`Assignor` trait (`crates/broker/src/coordinator/next_gen/assignor/mod.rs`) stays unchanged in shape:

```rust
pub trait Assignor: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn assign(
        &self,
        members: &[MemberSubscription],
        topics: &TopicMetadata,
    ) -> Assignment;
}
```

Already `pub`. `MemberSubscription`, `TopicMetadata`, `Assignment` are also already `pub`. No change required.

Submodule visibility changes:

```rust
// crates/broker/src/coordinator/next_gen/assignor/mod.rs
pub mod uniform;
pub mod range;
pub use range::RangeAssignor;
pub use uniform::UniformAssignor;
```

The two built-in impls become reachable from external crates so consumers can instantiate them directly (e.g., for tests, fallback chains).

### `NextGenConfig` changes

Replace `pub assignors: Vec<String>` with `pub assignors: Vec<Arc<dyn Assignor>>`:

```rust
// crates/broker/src/coordinator/next_gen/config.rs

use std::sync::Arc;
use super::assignor::{Assignor, RangeAssignor, UniformAssignor};

pub struct NextGenConfig {
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub assignors: Vec<Arc<dyn Assignor>>,
    pub max_size: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum AssignorRegistrationError {
    #[error("an assignor named {0} is already registered")]
    DuplicateName(String),
}

impl NextGenConfig {
    /// Register an additional assignor. Returns an error if the name is
    /// already taken. Built-ins (`uniform`, `range`) are registered by
    /// `Default::default()`; calling `register_assignor` with either
    /// name surfaces as a duplicate-name error.
    pub fn register_assignor(
        &mut self,
        assignor: Arc<dyn Assignor>,
    ) -> Result<(), AssignorRegistrationError> {
        let name = assignor.name();
        if self.assignors.iter().any(|a| a.name() == name) {
            return Err(AssignorRegistrationError::DuplicateName(name.into()));
        }
        self.assignors.push(assignor);
        Ok(())
    }

    /// Resolve a registered assignor by name. Cloning an `Arc` is cheap.
    pub fn find_assignor(&self, name: &str) -> Option<Arc<dyn Assignor>> {
        self.assignors
            .iter()
            .find(|a| a.name() == name)
            .cloned()
    }

    /// `true` when a client may legally request this name via
    /// `ConsumerGroupHeartbeatRequest::server_assignor`.
    pub fn assignor_enabled(&self, name: &str) -> bool {
        self.find_assignor(name).is_some()
    }
}

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            // ...
            assignors: vec![
                Arc::new(UniformAssignor::default()),
                Arc::new(RangeAssignor),
            ],
            // ...
        }
    }
}
```

`thiserror` is already a workspace dependency (used by `BrokerError`).

### Free `assignor::select()` is deleted

Today `crates/broker/src/coordinator/next_gen/assignor/mod.rs` has:

```rust
pub fn select(name: &str) -> Option<Box<dyn Assignor>> {
    match name {
        "uniform" => Some(Box::new(uniform::UniformAssignor::default())),
        "range" => Some(Box::new(range::RangeAssignor)),
        _ => None,
    }
}
```

This function is removed. Its only callers are the reconciler (which is updated below) and `assignor_enabled`-adjacent paths (which now consult the config directly).

### Reconciler signature change

```rust
// crates/broker/src/coordinator/next_gen/reconciler.rs

pub fn reconcile_if_dirty(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor: &dyn Assignor,           // CHANGED from `assignor_name: &str`
) -> ReconcileOutcome {
    if !group.dirty {
        return ReconcileOutcome::NoChange;
    }
    // (No more `assignor::select(...)` lookup — the caller resolved it.)
    let subscriptions: Vec<MemberSubscription> = build_subscriptions(group, input);
    let topics = build_topic_metadata(input);
    let assignment = assignor.assign(&subscriptions, &topics);
    group.bump_epoch();
    group.install_target(assignment);
    group.dirty = false;
    ReconcileOutcome::Recomputed
}
```

The `Option`-returning branch ("unknown assignor → no-op") disappears — resolution now happens upstream where it can fail informatively.

### Actor changes (`group_actor.rs`)

`pick_assignor` returns the resolved `Arc<dyn Assignor>`:

```rust
fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> Arc<dyn Assignor> {
    for m in state.members.values() {
        if let Some(name) = m.server_assignor.as_deref() {
            if let Some(a) = config.find_assignor(name) {
                return a;
            }
        }
    }
    config
        .assignors
        .first()
        .cloned()
        .expect("NextGenConfig must have at least one registered assignor")
}
```

`run_reconcile` passes the resolved trait object straight through:

```rust
fn run_reconcile(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
) {
    let input = metadata.snapshot();
    let assignor = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &*assignor);
}
```

The heartbeat handler's `UNSUPPORTED_ASSIGNOR` (error code 111) guard is unchanged — still uses `config.assignor_enabled(name)`.

### Operator-facing usage

```rust
use std::sync::Arc;
use crabka_broker::coordinator::next_gen::assignor::{
    Assignment, Assignor, MemberSubscription, TopicMetadata,
};

#[derive(Debug)]
struct MyOpaqueAssignor;

impl Assignor for MyOpaqueAssignor {
    fn name(&self) -> &'static str { "opaque" }
    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        // ...
    }
}

// In broker bootstrap:
let mut config = BrokerConfig::default();
config
    .next_gen_consumer_group
    .register_assignor(Arc::new(MyOpaqueAssignor))?;
Broker::start(config).await?;
```

The consumer then sends `server_assignor: Some("opaque")` in its heartbeat — same wire path that "uniform"/"range" already use.

### Why the operator API is `register_assignor`, not direct vec mutation

`config.next_gen_consumer_group.assignors.push(Arc::new(MyOpaqueAssignor))` would compile and work, but silently accepts duplicate names — last one wins by `find_assignor`'s `find()` semantics. `register_assignor` rejects duplicates so misconfiguration surfaces at startup, not as silent assignor-shadowing at heartbeat time.

The raw `assignors` field stays `pub` (parity with other `NextGenConfig` fields) — operators who want unchecked control can still use it. The helper is the recommended path.

## Error handling

| Failure | Handling |
|---------|----------|
| `register_assignor` with duplicate name | Returns `AssignorRegistrationError::DuplicateName(name)`. |
| `pick_assignor` on empty `assignors` vec | Panics with `"NextGenConfig must have at least one registered assignor"`. Only reachable if operator deliberately cleared the vec — non-recoverable. |
| Client requests unregistered `server_assignor` | Heartbeat returns `UNSUPPORTED_ASSIGNOR` (111). No change from current. |
| Member previously chose an assignor name that's no longer registered (operator removed it across a restart) | `pick_assignor` skips the missing entry, tries the next member preference, falls back to `assignors.first()`. Documented in `pick_assignor`'s doc comment. |
| Custom `assign()` panics | Actor task crashes; existing `get_or_create` dead-actor detection respawns from `seeds_cache`. Same failure model as the persistence path. |

No new wire error codes.

## Testing

### Unit tests on `NextGenConfig` (new — file currently has no tests)

5 tests in a new `#[cfg(test)] mod tests` block in `config.rs`:

- `default_registers_uniform_and_range` — `NextGenConfig::default().assignors.len() == 2`; names include both.
- `register_assignor_succeeds_for_new_name` — register `TestAssignor("custom")` → `Ok(())`; `find_assignor("custom").is_some()`.
- `register_assignor_rejects_duplicate_name` — register `TestAssignor("uniform")` → `Err(DuplicateName("uniform"))`.
- `find_assignor_returns_registered_impl` — register, then `find_assignor` returns an `Arc<dyn Assignor>` whose `.name()` matches.
- `assignor_enabled_matches_find_assignor` — both return consistent boolean/Option for several names.

`TestAssignor(&'static str)` is a fixture defined inside the test module: holds a name, returns an empty `Assignment`.

### Reconciler tests adapted

`crates/broker/src/coordinator/next_gen/reconciler.rs`:

- `dirty_triggers_recompute` — pass `&UniformAssignor::default()` directly instead of `"uniform"`.
- `clean_is_no_op` — same.
- **`unknown_assignor_is_no_op` is deleted.** The reconciler no longer takes a name, so an unknown assignor can't reach it. The upstream guard (`pick_assignor` skipping unregistered names) is covered by a new test in `group_actor.rs`.
- `idempotent_under_repeated_calls`, `metadata_change_via_dirty_flag_recomputes` — pass `&UniformAssignor::default()`.
- `subscription_topic_ids_resolved` — no change (doesn't call reconcile).

### `group_actor.rs` new tests

Two new tokio async tests inside the existing test module:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pick_assignor_skips_unregistered_member_preference() {
    // Build a group where a member has server_assignor = Some("ghost") with
    // no such name registered. Assert that pick_assignor falls back to the
    // first registered assignor (uniform).
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_assignor_invoked_when_requested() {
    // Counted fixture:
    #[derive(Debug)]
    struct CountingAssignor {
        calls: Arc<AtomicUsize>,
    }
    impl Assignor for CountingAssignor {
        fn name(&self) -> &'static str { "counting" }
        fn assign(&self, _: &[MemberSubscription], _: &TopicMetadata) -> Assignment {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Assignment::default()
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = NextGenConfig::default();
    config.register_assignor(Arc::new(CountingAssignor { calls: calls.clone() })).unwrap();

    let coord = Arc::new(NextGenCoordinator::new(
        config,
        empty_metadata(),
        Arc::new(InMemoryOffsetsLog::default()),
    ));
    let handle = coord.get_or_create("g");

    let (tx, rx) = tokio::sync::oneshot::channel();
    handle.tx.send(GroupActorMessage::Heartbeat {
        request: ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            server_assignor: Some("counting".into()),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
        client_host: String::new(),
        reply: tx,
    }).await.unwrap();
    let resp = rx.await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(calls.load(Ordering::SeqCst) >= 1, "custom assignor must be invoked");
}
```

### Compatibility

The 6 raw-RPC integration tests in `crates/broker/tests/consumer_group_next_gen.rs` continue to pass unmodified. They don't touch the registry; they just exercise the wire path. Confirming they still pass demonstrates the migration didn't break the existing surface.

### JVM acceptance

No change. The 4 `jvm_kip848_*` tests stay `#[ignore]`d (separate gap). Custom assignors don't unblock JVM client engagement.

## Acceptance gates

1. `cargo test --workspace` green.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo fmt --check` clean.
4. STATUS.md updated. Specifically: remove the "Custom server-side assignor plugin point (64c)" bullet from slice 64a's "Out of scope (follow-up slices)" list.
