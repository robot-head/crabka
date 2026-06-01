# KIP-932 Share Groups — Slice A (Membership Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A client can join a share group via `ShareGroupHeartbeat(76)`, converge a group epoch, receive a partition assignment, and observe membership via `ShareGroupDescribe(77)`; members leave by epoch -1 or session timeout; state survives a broker restart. No record delivery.

**Architecture:** Share groups become a new group variant inside the existing unified `GroupCoordinator` (`crates/broker/src/coordinator/unified/`). Rather than overload the consumer-hardcoded per-group actor, we add a parallel, simpler `share/` submodule (state, actor, assignor, persistence) and a `share_groups` registry on `GroupCoordinator`, reusing the existing `OffsetsLog` (`__consumer_offsets`), `MetadataProvider`, reconcile pattern, and bootstrap-replay pattern. A prerequisite codegen fix makes message-local `commonStructs` message-scoped so `ShareGroupDescribe` (and the latent `ConsumerGroupDescribe` bug) get correct types.

**Tech Stack:** Rust 2024, tokio actors (mpsc + oneshot), `crabka-protocol` generated codecs (quote!+rustfmt codegen), `DashMap`, `assert2` in tests, the in-process `crabka_client_core::Client` test harness.

**Reference spec:** `docs/superpowers/specs/2026-05-30-crabka-kip-932-share-groups-design.md`

---

## Key facts established during research (do not re-derive)

- Coordinator struct is **`GroupCoordinator`** at `crates/broker/src/coordinator/unified/mod.rs`, reached from `Broker` via `broker.group_manager.next_gen() -> Option<&Arc<unified::GroupCoordinator>>`.
- Per-group consumer actor: `crates/broker/src/coordinator/unified/actor.rs` (`GroupActorMessage`, `GroupActorHandle`). It is **consumer-hardcoded** — we do NOT modify it; we add a parallel share actor.
- Kafka error codes are `pub const NAME: i16 = N;` in `crates/broker/src/codes.rs` (no enum). Handlers set `error_code: codes::NAME`.
- ApiVersions list is built in `crates/broker/src/api_catalog.rs` via the `v!(<generated_request_module>)` macro; KIP-848 entries are in `admin_apis()`.
- Cluster features live in `crates/metadata/src/metadata_version.rs::supported_features()` and are surfaced via `crates/broker/src/features.rs`.
- Generated share message field names (verbatim, owned flavor):
  - `ShareGroupHeartbeatRequest { group_id: String, member_id: String, member_epoch: i32, rack_id: Option<String>, subscribed_topic_names: Option<Vec<String>>, unknown_tagged_fields }` (API_KEY=76, MIN=MAX=1)
  - `ShareGroupHeartbeatResponse { throttle_time_ms: i32, error_code: i16, error_message: Option<String>, member_id: Option<String>, member_epoch: i32, heartbeat_interval_ms: i32, assignment: Option<Assignment>, unknown_tagged_fields }`; nested `Assignment { topic_partitions: Vec<common::topic_partitions::TopicPartitions> }`; `TopicPartitions { topic_id: Uuid, partitions: Vec<i32> }`.
  - `ShareGroupDescribeRequest { group_ids: Vec<String>, include_authorized_operations: bool, unknown_tagged_fields }` (API_KEY=77, MIN=MAX=1)
  - `ShareGroupDescribeResponse { throttle_time_ms: i32, groups: Vec<DescribedGroup>, unknown_tagged_fields }`; `DescribedGroup { error_code, error_message, group_id, group_state, group_epoch, assignment_epoch, assignor_name, members: Vec<Member>, authorized_operations, .. }`; `Member { member_id, rack_id, member_epoch, client_id, client_host, subscribed_topic_names: Vec<String>, assignment: <message-scoped Assignment after Task 1>, .. }`.
- Import path convention: `crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest`, etc.
- Handlers registered in `build_table` use the plain 4-arg `HandlerFn`; handlers needing the connection principal take `RequestContext` and are intercepted inline in `network::dispatch` (e.g. `describe_groups`).
- Tests drive the broker via `crabka_client_core::Client::send(req)` (typed); **api_catalog must advertise 76/77 or version negotiation fails**.

## `commonStruct` collisions found (Task 1 target)

Two names have ≥2 distinct shapes across schemas; codegen currently flattens them into one global `common::<name>` (last shape wins):

| Name | Shape A (messages) | Shape B (messages) |
|------|--------------------|--------------------|
| `Assignment` | `{TopicPartitions}` — ConsumerGroupDescribeResponse, ShareGroupDescribeResponse | `{ActiveTasks,StandbyTasks,WarmupTasks}` — StreamsGroupDescribeResponse |
| `TopicPartitions` | `{TopicId,TopicName,Partitions}` — Consumer/ShareGroupDescribeResponse | `{TopicId,Partitions}` — Consumer/ShareGroupHeartbeatResponse |

Non-generated consumers of `common::` types in the broker (must keep compiling after Task 1): `crates/broker/src/coordinator/unified/actor.rs:535` (`common::topic_partitions::TopicPartitions`), `crates/broker/src/txn/handlers/add_partitions_to_txn.rs:32-34`, `crates/broker/src/handlers/describe_quorum.rs:30`.

---

## Task dependency & batching

- **Task 1** (codegen) is a prerequisite for **Task 11** (ShareGroupDescribe) only. Land it first; it regenerates protocol files and touches broker import sites.
- **Batch α (parallel, after Task 1):** Task 2 (codes.rs), Task 3 (api_catalog + feature), Task 4 (share config). Disjoint files.
- **Task 5** (variant plumbing on `GroupCoordinator`/`GroupType`) gates the coordinator-core tasks 6–9.
- **Batch β (parallel, after Task 5):** Task 6 (share/state.rs), Task 7 (share/assignor.rs), Task 8 (share/persistence.rs). Disjoint new files.
- **Task 9** (share/actor.rs) depends on 6,7,8. **Task 10/11** (handlers) depend on 9 + 3. **Task 12** (tests) last.

---

## Task 1: Codegen — message-scoped `commonStructs`

**Why:** Kafka `commonStructs` are message-local. The codegen shares them globally by name, so `Assignment`/`TopicPartitions` collide and mis-type Consumer/Share Describe. Fix: emit each message's common structs under a per-message common module and resolve references to it.

**Files:**
- Modify: `crates/protocol-codegen/src/resolve.rs:140-152` (reference path)
- Modify: `crates/protocol-codegen/src/main.rs:100-230` (emission layout + `common/mod.rs`)
- Modify: `crates/protocol-codegen/src/emit/mod_rs.rs` (common submodule listing, if it enumerates common names)
- Modify: `crates/protocol-codegen/src/emit/owned.rs:62-99` and `emit/borrowed.rs` (commons keyed by message)
- Regenerate: `crates/protocol/generated/**` and `crates/protocol/src/{owned,borrowed}/common/**`
- Fix imports: `crates/broker/src/coordinator/unified/actor.rs:535`, and verify `txn/handlers/add_partitions_to_txn.rs`, `handlers/describe_quorum.rs`
- Test: `crates/protocol-codegen/tests/snapshot.rs` (or a new `commonstruct_scoping.rs` test)

**Design:** Change the common module path from `super::common::<struct_snake>::<Name>` to **`super::common::<message_snake>::<struct_snake>::<Name>`**, and emit bodies under `generated/common/<message_snake>/<struct_snake>.<flavor>.rs`. Each message owns its commonStructs; identical names in different messages no longer collide. (`module_name` already snake-cases; reuse it for the message segment from `spec.name`.)

- [ ] **Step 1: Write the failing test** proving the three `Assignment` shapes are distinct types.

Add to `crates/protocol-codegen/tests/snapshot.rs`:

```rust
#[test]
fn common_structs_are_message_scoped() {
    // Regression for KIP-932: message-local commonStructs named the same
    // across different messages must NOT collapse into one global type.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap().join("protocol").join("schemas");
    let specs = crabka_protocol_codegen::ir::load_dir(&dir).unwrap();

    let resolve = |msg: &str| {
        let spec = specs.iter().find(|s| s.name == msg).unwrap();
        crabka_protocol_codegen::resolve::resolve_message(spec).unwrap()
    };
    let share = resolve("ShareGroupDescribeResponse");
    let streams = resolve("StreamsGroupDescribeResponse");

    // Both define a commonStruct "Assignment" but with different shapes:
    // their resolved Rust paths MUST differ (message-scoped), not be a
    // single shared `super::common::assignment::Assignment`.
    let share_path = &share.get("Assignment").unwrap().rust_path;
    let streams_path = &streams.get("Assignment").unwrap().rust_path;
    assert_ne!(
        share_path, streams_path,
        "Assignment commonStruct must be message-scoped, got {share_path} == {streams_path}"
    );
    assert!(share_path.contains("share_group_describe_response"));
    assert!(streams_path.contains("streams_group_describe_response"));
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-protocol-codegen common_structs_are_message_scoped`
Expected: FAIL — both paths equal `super::common::assignment::Assignment`.

(If `ir`/`resolve` are not `pub` at crate root, add `pub mod ir; pub mod resolve;` exposure in `crates/protocol-codegen/src/lib.rs` as part of this step, mirroring how the existing snapshot test reaches `ir`.)

- [ ] **Step 3: Make the reference path message-scoped**

In `crates/protocol-codegen/src/resolve.rs`, change `resolve_message` to accept the owning message name and qualify the path. Replace the common-struct insertion loop (lines ~140-152):

```rust
let msg_mod = crate::name_conv::module_name(&spec.name);
for cs in &spec.common_structs {
    let snake = crate::name_conv::module_name(&cs.name);
    map.insert(
        cs.name.clone(),
        Resolution {
            kind: StructKind::Common,
            // Message-scoped: super::common::<message>::<struct>::<Name>
            rust_path: format!("super::common::{msg_mod}::{snake}::{}", cs.name),
            needs_lifetime: cs_needing_lt.contains(&cs.name),
        },
    );
}
```

- [ ] **Step 4: Emit common bodies under a per-message directory**

In `crates/protocol-codegen/src/emit/owned.rs` (and `borrowed.rs`), change the `commons` collection to carry the owning message segment, e.g. push `(format!("{msg_mod}/{cs_snake}"), body)` instead of bare `cs.name`. In `crates/protocol-codegen/src/main.rs` (lines ~148-230), write each body to `generated/common/<msg_mod>/<cs_snake>.<flavor>.rs`, build the `BTreeSet` of these qualified keys, and emit `src/{flavor}/common/mod.rs` with nested `pub mod <msg_mod> { pub mod <cs_snake>; }` (or a flat `pub mod <msg_mod>__<cs_snake>` if nested modules complicate the `include!` wrappers — prefer the nested form to match the `super::common::<msg>::<struct>` path from Step 3). Update `write_common_wrapper` and `emit/mod_rs.rs` accordingly so the wrapper `include!`s the new path.

- [ ] **Step 5: Regenerate and run the codegen test**

Run: `crates/protocol/regenerate.sh` (the repo's regenerate script — see CONTRIBUTING.md) then `cargo test -p crabka-protocol-codegen common_structs_are_message_scoped`
Expected: PASS.

- [ ] **Step 6: Fix the broker import site(s)**

`crates/broker/src/coordinator/unified/actor.rs:535` builds the heartbeat-response `TopicPartitions`. Update its path to the now message-scoped module for `ConsumerGroupHeartbeatResponse`:

```rust
|(tid, parts)| crabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions {
    topic_id: crabka_protocol::primitives::uuid::Uuid::from(*tid),
    partitions: parts.clone(),
    ..Default::default()
},
```

(Adjust the exact module path to match what Step 4 emits. Verify `txn/handlers/add_partitions_to_txn.rs:32-34` and `handlers/describe_quorum.rs:30` — those names do not collide, but their paths now include a message segment; update them too if Step 4 scopes *all* common structs. If you scoped only colliding names, these are unchanged — but the chosen design scopes all, so update them.)

- [ ] **Step 7: Build, test, and confirm no drift**

Run: `cargo build --workspace`
Run: `cargo test --workspace`
Run: `crates/protocol/regenerate.sh && git diff --exit-code crates/protocol/generated`
Expected: build + tests PASS; `git diff` empty (no drift).

- [ ] **Step 8: Commit**

```bash
git add crates/protocol-codegen crates/protocol crates/broker/src/coordinator/unified/actor.rs crates/broker/src/txn crates/broker/src/handlers/describe_quorum.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "fix(codegen): scope commonStructs per message (KIP-932 prereq)

Message-local commonStructs named the same across messages (Assignment,
TopicPartitions) collapsed into one global type, mis-typing Consumer/Share
group Describe responses. Emit them under per-message common modules.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Share-group error codes

**Files:**
- Modify: `crates/broker/src/codes.rs`
- Test: inline `#[cfg(test)]` in `codes.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/broker/src/codes.rs`:

```rust
#[test]
fn share_group_error_codes_match_kafka() {
    assert!(INVALID_RECORD_STATE == 121);
    assert!(SHARE_SESSION_NOT_FOUND == 122);
    assert!(INVALID_SHARE_SESSION_EPOCH == 123);
    assert!(FENCED_STATE_EPOCH == 124);
    assert!(SHARE_SESSION_LIMIT_REACHED == 133);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker codes::tests::share_group_error_codes_match_kafka`
Expected: FAIL — constants undefined.

- [ ] **Step 3: Add the constants**

Append to `crates/broker/src/codes.rs` near the KIP-848 consumer-group codes (around the `STALE_MEMBER_EPOCH`/`UNKNOWN_SUBSCRIPTION_ID` cluster):

```rust
/// KIP-932: an acknowledgement targeted a record that is no longer Acquired.
pub const INVALID_RECORD_STATE: i16 = 121;
/// KIP-932: the share session named by the request does not exist.
pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
/// KIP-932: the share session epoch did not match the broker's expectation.
pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
/// KIP-932: the share coordinator fenced a write on a stale state epoch.
pub const FENCED_STATE_EPOCH: i16 = 124;
/// KIP-932: the per-broker share session cache is full.
pub const SHARE_SESSION_LIMIT_REACHED: i16 = 133;
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker codes::tests::share_group_error_codes_match_kafka`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/codes.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): add share-group error codes (121-124,133)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Advertise ShareGroup RPCs + `share.version` feature

**Files:**
- Modify: `crates/protocol/src/api_catalog.rs` (function `admin_apis`)
- Modify: `crates/metadata/src/metadata_version.rs` (feature table)
- Modify: `crates/broker/src/features.rs` (surface the feature)
- Test: `crates/broker/tests/share_groups.rs` (a focused ApiVersions test) or inline in `api_catalog.rs`

Note: `api_catalog.rs` is in the **broker** crate (`crates/broker/src/api_catalog.rs`) per research — confirm path; the `v!` macro reads `API_KEY/MIN_VERSION/MAX_VERSION` from the generated owned module.

- [ ] **Step 1: Write the failing test**

Add to `crates/broker/src/api_catalog.rs` tests:

```rust
#[test]
fn share_group_apis_are_advertised() {
    let apis = supported_apis();
    let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
    assert!(keys.contains(&76), "ShareGroupHeartbeat (76) not advertised");
    assert!(keys.contains(&77), "ShareGroupDescribe (77) not advertised");
    let hb = apis.iter().find(|a| a.api_key == 76).unwrap();
    assert!(hb.min_version == 1 && hb.max_version == 1);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker api_catalog::tests::share_group_apis_are_advertised`
Expected: FAIL — 76/77 absent.

- [ ] **Step 3: Advertise the APIs**

In `admin_apis()` next to the KIP-848 entries, add:

```rust
// KIP-932 share-group membership protocol.
v!(share_group_heartbeat_request),
v!(share_group_describe_request),
```

- [ ] **Step 4: Add the `share.version` cluster feature**

In `crates/metadata/src/metadata_version.rs`, add a feature name const and a `SupportedFeature` row:

```rust
pub const SHARE_VERSION_FEATURE: &str = "share.version";
pub const SHARE_VERSION_MIN: i16 = 0;
pub const SHARE_VERSION_MAX: i16 = 1;
```

and push a row into the table returned by `supported_features()`:

```rust
SupportedFeature { name: SHARE_VERSION_FEATURE, min_version: SHARE_VERSION_MIN, max_version: SHARE_VERSION_MAX },
```

Mirror the surfacing in `crates/broker/src/features.rs::supported_features()` so ApiVersions reports `share.version: 0..1`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker api_catalog::tests::share_group_apis_are_advertised`
Run: `cargo test -p crabka-metadata`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/api_catalog.rs crates/metadata/src/metadata_version.rs crates/broker/src/features.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): advertise ShareGroup APIs 76/77 + share.version feature

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Share-group config

**Files:**
- Create: `crates/broker/src/coordinator/unified/share/config.rs` (new `ShareGroupConfig`)
- Modify: `crates/broker/src/coordinator/unified/share/mod.rs` (new module decl — created in Task 5; if Task 4 runs first, create a minimal `mod.rs` with `pub mod config;`)
- Modify: `crates/broker/src/config.rs` (add `pub share_group: ShareGroupConfig` to `BrokerConfig`; default in `for_tests` and prod default)
- Test: inline in `share/config.rs`

- [ ] **Step 1: Write the failing test**

In `crates/broker/src/coordinator/unified/share/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn defaults_are_kafka_ga() {
        let c = ShareGroupConfig::default();
        assert!(c.enable); // greenfield: on by default (share.version=1)
        assert!(c.heartbeat_interval == std::time::Duration::from_secs(5));
        assert!(c.session_timeout == std::time::Duration::from_secs(45));
        assert!(c.max_groups == 0 || c.max_groups > 0); // present
        assert!(c.max_size == 200);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share::config::tests::defaults_are_kafka_ga`
Expected: FAIL — type missing.

- [ ] **Step 3: Define `ShareGroupConfig`**

```rust
//! KIP-932 share-group membership configuration.
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ShareGroupConfig {
    /// Master switch; gated under the `share.version` feature.
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    /// Max simultaneous share groups (0 = unlimited for Slice A).
    pub max_groups: usize,
    /// Max members per share group.
    pub max_size: usize,
}

impl Default for ShareGroupConfig {
    fn default() -> Self {
        Self {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_secs(60),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_groups: 0,
            max_size: 200,
        }
    }
}
```

- [ ] **Step 4: Wire onto `BrokerConfig`**

In `crates/broker/src/config.rs`, add the field to `BrokerConfig`:

```rust
/// KIP-932 share-group configuration.
pub share_group: crate::coordinator::unified::share::config::ShareGroupConfig,
```

and default it (`share_group: ShareGroupConfig::default()`) in both the `for_tests` ctor and the production default.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker share::config::tests::defaults_are_kafka_ga`
Run: `cargo build -p crabka-broker`
Expected: PASS / builds.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/coordinator/unified/share/ crates/broker/src/config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): add ShareGroupConfig

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Group-type plumbing + share registry on `GroupCoordinator`

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (`GroupType::Share`, `share_groups` DashMap, `get_or_create_share`, `mark_share`, `share_group_type`)
- Create: `crates/broker/src/coordinator/unified/share/mod.rs` (module root; declares `config`, `state`, `assignor`, `persistence`, `actor`)
- Test: inline in `unified/mod.rs`

- [ ] **Step 1: Write the failing test**

In `crates/broker/src/coordinator/unified/mod.rs` tests:

```rust
#[test]
fn group_type_has_share_variant() {
    // compile-time: the variant exists and is distinct.
    let t = GroupType::Share;
    assert!(t != GroupType::Classic && t != GroupType::NextGen);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker coordinator::unified::tests::group_type_has_share_variant`
Expected: FAIL — `Share` undefined.

- [ ] **Step 3: Add `Share` to `GroupType` and the registry**

In `unified/mod.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
    Share,
}
```

Add to `GroupCoordinator`:

```rust
/// One actor per share group (KIP-932).
pub share_groups: Arc<DashMap<String, Arc<share::actor::ShareGroupActorHandle>>>,
```

(initialise in `new`). Add methods mirroring the consumer ones:

```rust
pub fn mark_share(&self, group_id: &str) {
    self.group_types.entry(group_id.to_string()).or_insert(GroupType::Share);
}

pub fn get_or_create_share(self: &Arc<Self>, group_id: &str)
    -> Arc<share::actor::ShareGroupActorHandle>
{
    if let Some(h) = self.share_groups.get(group_id) { return h.clone(); }
    let handle = Arc::new(share::actor::ShareGroupActorHandle::spawn(
        group_id.to_string(),
        Arc::new(self.config.share_clone()), // see note
        self.metadata.clone(),
        self.offsets_log.clone(),
        self.clone(),
    ));
    self.share_groups.entry(group_id.to_string()).or_insert(handle.clone());
    handle
}
```

Note: the share actor needs `ShareGroupConfig`, not `NextGenConfig`. Plumb `ShareGroupConfig` into `GroupCoordinator` construction (add a `share_config: Arc<ShareGroupConfig>` field sourced from `BrokerConfig::share_group` where `GroupCoordinator::new` is called). Adjust `new`'s signature to take it; update the single call site (find via `GroupCoordinator::new(` — likely in `coordinator/mod.rs` or `broker.rs`).

- [ ] **Step 4: Declare the share module**

Create `crates/broker/src/coordinator/unified/share/mod.rs`:

```rust
//! KIP-932 share-group membership (parallel to the consumer next-gen path).
pub mod actor;
pub mod assignor;
pub mod config;
pub mod persistence;
pub mod state;
```

and add `pub mod share;` to `crates/broker/src/coordinator/unified/mod.rs`'s module list.

- [ ] **Step 5: Run to verify it passes** (after stubs from later tasks exist this fully compiles; for now gate on the enum test)

Run: `cargo test -p crabka-broker coordinator::unified::tests::group_type_has_share_variant`
Expected: PASS (create empty stub files for `state/assignor/persistence/actor` if needed so the module compiles, to be filled by Tasks 6-9).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/coordinator/unified/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): GroupType::Share + share-group registry on GroupCoordinator

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: `ShareGroupState` + `ShareMemberState`

**Files:**
- Create/replace stub: `crates/broker/src/coordinator/unified/share/state.rs`
- Test: inline

Mirror `consumer_state::GroupState` minus all offset machinery. Members carry no assignment-ack state beyond an epoch (share assignment is non-exclusive and not revocation-tracked in Slice A).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::time::{Duration, Instant};

    #[test]
    fn add_member_bumps_nothing_until_reconcile() {
        let mut g = ShareGroupState::new("g1");
        assert!(g.group_epoch == 0);
        g.add_or_update_member(ShareMemberState::joining("m1", "c1", "h1",
            ["t1".to_string()].into_iter().collect()));
        assert!(g.members.len() == 1);
        assert!(g.dirty);
    }

    #[test]
    fn evict_expired_removes_silent_members() {
        let mut g = ShareGroupState::new("g1");
        let mut m = ShareMemberState::joining("m1","c1","h1", Default::default());
        m.last_seen = Instant::now() - Duration::from_secs(120);
        g.add_or_update_member(m);
        let evicted = g.evict_expired(Instant::now(), Duration::from_secs(45));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share::state::tests`
Expected: FAIL — types missing.

- [ ] **Step 3: Implement the state machine**

```rust
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ShareMemberState {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    pub member_epoch: i32,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
}

impl ShareMemberState {
    pub fn joining(member_id: impl Into<String>, client_id: impl Into<String>,
                   client_host: impl Into<String>, subs: HashSet<String>) -> Self {
        Self {
            member_id: member_id.into(), rack_id: None,
            client_id: client_id.into(), client_host: client_host.into(),
            subscribed_topic_names: subs, member_epoch: 0,
            assigned_partitions: HashMap::new(), last_seen: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct ShareTargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}

#[derive(Debug)]
pub struct ShareGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, ShareMemberState>,
    pub target: ShareTargetAssignment,
    pub dirty: bool,
}

impl ShareGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self { group_id: group_id.into(), group_epoch: 0, members: HashMap::new(),
               target: ShareTargetAssignment { epoch: 0, per_member: HashMap::new() }, dirty: false }
    }
    pub fn bump_epoch(&mut self) { self.group_epoch += 1; }
    pub fn add_or_update_member(&mut self, m: ShareMemberState) {
        self.members.insert(m.member_id.clone(), m);
        self.dirty = true;
    }
    pub fn remove_member(&mut self, member_id: &str) -> Option<ShareMemberState> {
        let r = self.members.remove(member_id);
        if r.is_some() { self.dirty = true; }
        r
    }
    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let expired: Vec<String> = self.members.iter()
            .filter(|(_, m)| now.duration_since(m.last_seen) > session_timeout)
            .map(|(id, _)| id.clone()).collect();
        for id in &expired { self.members.remove(id); }
        if !expired.is_empty() { self.dirty = true; }
        expired
    }
    pub fn install_target(&mut self, per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>) {
        self.target = ShareTargetAssignment { epoch: self.group_epoch, per_member };
    }
    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.member_epoch = self.group_epoch;
            if let Some(a) = self.target.per_member.get(member_id) {
                m.assigned_partitions = a.clone();
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker share::state::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/coordinator/unified/share/state.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): ShareGroupState membership state machine

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: `ShareGroupAssignor` (non-exclusive simple assignor)

**Files:**
- Create/replace stub: `crates/broker/src/coordinator/unified/share/assignor.rs`
- Test: inline

Reuse the existing `assignor::{MemberSubscription, TopicMetadata, Assignment}` types. Distribute partitions round-robin across members; **allow overlap** — when members > partitions, every member still gets at least one partition.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::unified::assignor::{MemberSubscription, TopicMetadata};
    use assert2::assert;
    use uuid::Uuid;

    fn topic(p: i32) -> (Uuid, TopicMetadata) {
        let id = Uuid::from_u128(1);
        let mut t = TopicMetadata::default();
        t.partitions_per_topic.insert(id, p);
        (id, t)
    }

    #[test]
    fn distributes_partitions_across_members() {
        let (id, topics) = topic(4);
        let members = vec![
            MemberSubscription { member_id: "m1".into(), rack_id: None, subscribed_topic_ids: vec![id] },
            MemberSubscription { member_id: "m2".into(), rack_id: None, subscribed_topic_ids: vec![id] },
        ];
        let a = ShareGroupAssignor.assign(&members, &topics);
        let total: usize = a.values().flat_map(|m| m.values()).map(|p| p.len()).sum();
        assert!(total == 4); // all 4 partitions assigned
        assert!(a["m1"][&id].len() == 2 && a["m2"][&id].len() == 2);
    }

    #[test]
    fn more_members_than_partitions_overlap() {
        let (id, topics) = topic(1);
        let members: Vec<_> = (0..3).map(|i| MemberSubscription {
            member_id: format!("m{i}"), rack_id: None, subscribed_topic_ids: vec![id] }).collect();
        let a = ShareGroupAssignor.assign(&members, &topics);
        // every member gets the single partition (share semantics permit overlap)
        for i in 0..3 { assert!(a[&format!("m{i}")][&id] == vec![0]); }
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share::assignor::tests`
Expected: FAIL — type missing.

- [ ] **Step 3: Implement**

```rust
use crate::coordinator::unified::assignor::{Assignment, MemberSubscription, TopicMetadata};
use std::collections::HashMap;

/// KIP-932 SimpleAssignor: round-robin distribution that permits overlap.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShareGroupAssignor;

impl ShareGroupAssignor {
    pub fn name(&self) -> &'static str { "simple" }

    pub fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        let mut out: Assignment = members.iter()
            .map(|m| (m.member_id.clone(), HashMap::new())).collect();
        if members.is_empty() { return out; }
        for (topic_id, &num_parts) in &topics.partitions_per_topic {
            // subscribers to this topic, in stable order
            let mut subs: Vec<&str> = members.iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str()).collect();
            subs.sort_unstable();
            if subs.is_empty() { continue; }
            if num_parts as usize >= subs.len() {
                // enough partitions: round-robin, no overlap needed
                for p in 0..num_parts {
                    let m = subs[p as usize % subs.len()];
                    out.get_mut(m).unwrap().entry(*topic_id).or_default().push(p);
                }
            } else {
                // fewer partitions than members: every member gets at least one
                // (overlap), assigned round-robin from the partition set.
                for (i, m) in subs.iter().enumerate() {
                    let p = (i as i32) % num_parts;
                    out.get_mut(*m).unwrap().entry(*topic_id).or_default().push(p);
                }
            }
        }
        out
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker share::assignor::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/coordinator/unified/share/assignor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): ShareGroupAssignor (non-exclusive simple assignor)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: Share record codecs + replay dispatch

**Files:**
- Create/replace stub: `crates/broker/src/coordinator/unified/share/persistence.rs`
- Modify: `crates/broker/src/coordinator/persistence.rs` (top-level `Key` enum: add `Share(ShareGroupKey)` + parse dispatch)
- Modify: `crates/broker/src/coordinator/bootstrap.rs` (`apply_record`/`apply_tombstone`: handle `Key::Share`)
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (replay methods: `replay_share_*`, `mark_share`, `finalize` seeds for share groups)
- Test: inline round-trip tests in `share/persistence.rs`

Mirror `persistence_next_gen.rs`. Use key-version discriminators 9–13 (free; 3,5,6,7,8 used by consumer next-gen). Reuse leaf helpers from `crate::coordinator::unified::persistence`.

- [ ] **Step 1: Write the failing round-trip test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn group_metadata_round_trip() {
        let key = ShareGroupKey::GroupMetadata { group_id: "g1".into() };
        let bytes = encode_share_key(&key);
        let (ver, body) = peek_version(&bytes);
        assert!(ver == KEY_SHARE_GROUP_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupMetadataValue { epoch: 7 };
        assert!(ShareGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_round_trip() {
        let key = ShareGroupKey::MemberMetadata { group_id: "g1".into(), member_id: "m1".into() };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(parse_share_key(ver, body).unwrap() == key);
    }
}
```

(Provide small local helpers `peek_version`/`encode_share_key` consistent with how `persistence_next_gen.rs` encodes a leading `i16` key version.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share::persistence::tests`
Expected: FAIL — types missing.

- [ ] **Step 3: Implement the codecs**

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;
use crate::error::BrokerError;
use crate::coordinator::unified::persistence::{get_i16, get_i32, get_string, put_string, /* nullable helpers */};

pub const KEY_SHARE_GROUP_METADATA: i16 = 9;
pub const KEY_SHARE_MEMBER_METADATA: i16 = 10;
pub const KEY_SHARE_TARGET_ASSIGNMENT_METADATA: i16 = 11;
pub const KEY_SHARE_TARGET_ASSIGNMENT_MEMBER: i16 = 12;
pub const KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT: i16 = 13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareGroupKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

pub fn encode_share_key(key: &ShareGroupKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        ShareGroupKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_GROUP_METADATA); put_string(&mut buf, group_id);
        }
        ShareGroupKey::MemberMetadata { group_id, member_id } => {
            buf.put_i16(KEY_SHARE_MEMBER_METADATA);
            put_string(&mut buf, group_id); put_string(&mut buf, member_id);
        }
        ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_METADATA); put_string(&mut buf, group_id);
        }
        ShareGroupKey::TargetAssignmentMember { group_id, member_id } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id); put_string(&mut buf, member_id);
        }
        ShareGroupKey::CurrentMemberAssignment { group_id, member_id } => {
            buf.put_i16(KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id); put_string(&mut buf, member_id);
        }
    }
    buf.freeze()
}

pub fn parse_share_key(version: i16, mut buf: &[u8]) -> Result<ShareGroupKey, BrokerError> {
    Ok(match version {
        KEY_SHARE_GROUP_METADATA => ShareGroupKey::GroupMetadata { group_id: get_string(&mut buf)? },
        KEY_SHARE_MEMBER_METADATA => ShareGroupKey::MemberMetadata {
            group_id: get_string(&mut buf)?, member_id: get_string(&mut buf)? },
        KEY_SHARE_TARGET_ASSIGNMENT_METADATA => ShareGroupKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)? },
        KEY_SHARE_TARGET_ASSIGNMENT_MEMBER => ShareGroupKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?, member_id: get_string(&mut buf)? },
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT => ShareGroupKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?, member_id: get_string(&mut buf)? },
        other => return Err(BrokerError::internal(format!("unknown share key version {other}"))),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupMetadataValue { pub epoch: i32 }
impl ShareGroupMetadataValue {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new(); b.put_i16(0); b.put_i32(self.epoch); b.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?; Ok(Self { epoch: get_i32(&mut buf)? })
    }
}
// ShareGroupMemberMetadataValue { rack_id, client_id, client_host, subscribed_topic_names }
// ShareGroupTargetAssignmentMetadataValue { assignment_epoch }
// ShareGroupTargetAssignmentMemberValue { topic_partitions: Vec<(Uuid, Vec<i32>)> }
// ShareGroupCurrentMemberAssignmentValue { member_epoch, assigned_partitions }
// — mirror persistence_next_gen.rs MemberMetadataValue / TargetAssignmentMemberValue
//   encode/decode exactly (i16(0) preamble, length-prefixed arrays, nullable strings).
```

Fill in the four remaining `*Value` structs by mirroring the corresponding `persistence_next_gen.rs` value codecs (drop the consumer-only `instance_id`, `server_assignor`, `subscribed_topic_regex`, `rebalance_timeout_ms`, and the revocation/pending fields). Add round-trip tests for each in Step 1's module.

- [ ] **Step 4: Extend the top-level `Key` enum + replay dispatch**

In `crates/broker/src/coordinator/persistence.rs`, add `Share(ShareGroupKey)` to the `Key` enum and dispatch it in `parse_key` by reading the leading version and routing 9–13 to `parse_share_key`. In `bootstrap.rs::apply_record`, add:

```rust
Key::Share(share_key) => apply_share_record(group_manager, share_key, value_bytes)?,
```

and in `apply_tombstone`, route `Key::Share(k)` to `ng.replay_share_tombstone(&k)`. Implement `apply_share_record` mirroring `apply_next_gen_record`: call `ng.mark_share(&group_id)` and the matching `ng.replay_share_*` method (add those replay methods + a `GroupSeed`-equivalent for share groups, or extend `GroupSeed`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker share::persistence::tests`
Run: `cargo build -p crabka-broker`
Expected: PASS / builds.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/coordinator/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): share-group record codecs + replay dispatch

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: Share-group actor (heartbeat + session tick + persistence)

**Files:**
- Create/replace stub: `crates/broker/src/coordinator/unified/share/actor.rs`
- Test: inline (drive the actor via its mpsc handle with a fake `OffsetsLog` + fake `MetadataProvider`)

Mirror `actor.rs` structure but simpler: messages are `ShareHeartbeat`, `Describe`, `Seed`, `Shutdown`. Reconcile uses `ShareGroupAssignor`. Persist via the shared `OffsetsLog` using the Task 8 codecs.

- [ ] **Step 1: Write the failing test** (single-member join → assignment)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
    use assert2::assert;
    use std::sync::Arc;

    // a fake MetadataProvider returning one topic "t1" with 4 partitions
    // (mirror the consumer actor tests' fake; build ReconcileInput with one
    //  topic_id_by_name + partitions_per_topic entry).

    #[tokio::test]
    async fn single_member_join_gets_assignment() {
        let (handle, _coord) = spawn_test_actor(/* fake md w/ t1:4 */).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle.tx.send(ShareGroupActorMessage::Heartbeat {
            request: req("g1", "", 0, &["t1"]),
            client_host: "h".into(),
            reply: tx,
        }).await.unwrap();
        let resp = rx.await.unwrap();
        assert!(resp.error_code == 0);
        assert!(resp.member_id.is_some());
        assert!(resp.member_epoch == 1);
        let total: usize = resp.assignment.unwrap().topic_partitions.iter()
            .map(|t| t.partitions.len()).sum();
        assert!(total == 4);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share::actor::tests::single_member_join_gets_assignment`
Expected: FAIL — types missing.

- [ ] **Step 3: Implement the actor**

Define the message enum, handle, spawn (mpsc channel + `tokio::spawn(actor_loop)`), and the loop with a `select!` over `rx.recv()` and a heartbeat-interval `tick` calling `handle_session_tick`. Key handler:

```rust
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::owned::share_group_heartbeat_response::{ShareGroupHeartbeatResponse, Assignment};
use crabka_protocol::owned::common::share_group_heartbeat_response::topic_partitions::TopicPartitions; // message-scoped (Task 1)

pub enum ShareGroupActorMessage {
    Heartbeat { request: ShareGroupHeartbeatRequest, client_host: String,
                reply: oneshot::Sender<ShareGroupHeartbeatResponse> },
    Describe { reply: oneshot::Sender<ShareDescribeView> },
    Seed(/* share seed */),
    Shutdown(oneshot::Sender<()>),
}

async fn handle_heartbeat(
    state: &mut ShareGroupState, config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider, offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator, req: &ShareGroupHeartbeatRequest, client_host: &str,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    // 1. leave: member_epoch == -1 → remove_member, reconcile, persist tombstones, return.
    // 2. first join: member_id empty / epoch 0 → generate UUID, add_or_update_member.
    // 3. epoch validation: known member with stale epoch → error_resp(FENCED_MEMBER_EPOCH).
    // 4. update subscription/last_seen.
    // 5. reconcile_if_dirty(state, &metadata.snapshot(), &ShareGroupAssignor) → bump epoch + install_target.
    // 6. advance_member_epoch(member_id).
    // 7. persist ShareGroup{Metadata,MemberMetadata,TargetAssignment*,CurrentMemberAssignment} via flush_pending.
    // 8. build response: member_id, member_epoch = state.group_epoch, heartbeat_interval_ms,
    //    assignment = Some(Assignment { topic_partitions: <member's assigned_partitions> }).
}
```

Provide a share-specific `reconcile_if_dirty` (or reuse `reconciler::reconcile_if_dirty` by adapting it to `ShareGroupState` — simplest is a small share reconcile fn that builds `MemberSubscription`/`TopicMetadata` from `ShareGroupState` and calls `ShareGroupAssignor`, then `bump_epoch` + `install_target`). Build `TopicPartitions { topic_id, partitions }` from `state.members[id].assigned_partitions`. Implement `handle_session_tick` calling `state.evict_expired(Instant::now(), config.session_timeout)` then reconcile + flush tombstones, mirroring the consumer actor's tick. Gate the whole actor on `config.enable`; the *handler* (Task 10) returns `UNSUPPORTED_VERSION` when disabled, so the actor can assume enabled.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker share::actor::tests`
Expected: PASS.

- [ ] **Step 5: Add multi-member + leave + eviction tests**

Add `two_members_reconcile`, `leave_removes_member` (epoch -1), and `session_tick_evicts` tests in the same module, asserting `group_epoch` advances and membership changes. Run them; expected PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/coordinator/unified/share/actor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): share-group actor (heartbeat, reconcile, session expiry)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: `ShareGroupHeartbeat(76)` handler

**Files:**
- Create: `crates/broker/src/handlers/share_group_heartbeat.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (`pub(crate) mod share_group_heartbeat;` + `t.register(76, share_group_heartbeat::handle);`)
- Test: covered by Task 12 integration; add a unit test for the disabled-feature path here.

- [ ] **Step 1: Write the failing test** (disabled feature → UNSUPPORTED_VERSION)

In `share_group_heartbeat.rs`, a unit test that constructs a `Broker` (via test config) with `share_group.enable = false` and asserts the handler returns a response with `error_code == codes::UNSUPPORTED_VERSION`. (If constructing a `Broker` in a unit test is heavy, defer this assertion to Task 12 integration and instead unit-test a small `fn check_enabled(cfg) -> Option<i16>` helper.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker share_group_heartbeat`
Expected: FAIL.

- [ ] **Step 3: Implement the handler** (mirror `consumer_group_heartbeat.rs` exactly)

```rust
use bytes::Bytes;
use futures_util::future::BoxFuture;
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse;
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;

fn error(code: i16) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse { error_code: code, ..Default::default() }
}

pub(crate) fn handle(
    broker: &Broker, version: i16, _correlation_id: i32, req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let decoded = ShareGroupHeartbeatRequest::decode(&mut &*req_bytes, version);
    let gm = broker.group_manager.clone();
    let enabled = broker.config.share_group.enable;
    Box::pin(async move {
        let req = decoded?;
        let resp = if !enabled {
            error(codes::UNSUPPORTED_VERSION)
        } else if let Some(ng) = gm.next_gen() {
            ng.mark_share(&req.group_id);
            let handle = ng.get_or_create_share(&req.group_id);
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle.tx.send(ShareGroupActorMessage::Heartbeat {
                request: req, client_host: String::new(), reply: tx,
            }).await.map_err(|_| BrokerError::internal("share actor gone"))?;
            rx.await.map_err(|_| BrokerError::internal("share actor dropped reply"))?
        } else {
            error(codes::GROUP_ID_NOT_FOUND)
        };
        let mut buf = bytes::BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Register in `handlers/mod.rs`: add the module decl and `t.register(76, share_group_heartbeat::handle);` in `build_table()`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker share_group_heartbeat`
Run: `cargo build -p crabka-broker`
Expected: PASS / builds.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): ShareGroupHeartbeat(76) handler

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: `ShareGroupDescribe(77)` handler

**Files:**
- Create: `crates/broker/src/handlers/share_group_describe.rs`
- Modify: `crates/broker/src/handlers/mod.rs` and `network::dispatch` (inline interception with `RequestContext` for the per-group Describe ACL, mirroring `describe_groups`)
- Modify: `crates/broker/src/coordinator/unified/share/actor.rs` (`ShareDescribeView` + `Describe` message produce a describe snapshot)
- Test: Task 12 integration

Depends on Task 1 (so `Member.assignment` is the correct `{topic_partitions}` shape).

- [ ] **Step 1: Write the failing test** (in Task 12's file or here): after two members join "g1", `ShareGroupDescribe(["g1"])` returns one group with `group_epoch >= 1` and 2 members. (Write it now so it fails.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --test share_groups describe`
Expected: FAIL — handler unregistered / 77 not advertised path.

- [ ] **Step 3: Implement the handler**

```rust
pub(crate) async fn handle(
    broker: &Broker, version: i16, _correlation_id: i32, req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let req = ShareGroupDescribeRequest::decode(&mut &*req_bytes, version)?;
    let mut groups = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        // ACL: Describe on Group(gid) via ctx.principal (mirror describe_groups).
        let described = match broker.group_manager.next_gen().and_then(|ng| ng.find_share(gid)) {
            Some(handle) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                handle.tx.send(ShareGroupActorMessage::Describe { reply: tx }).await
                    .map_err(|_| BrokerError::internal("share actor gone"))?;
                let view = rx.await.map_err(|_| BrokerError::internal("no reply"))?;
                view.into_described_group(gid) // builds DescribedGroup { members, group_epoch, .. }
            }
            None => DescribedGroup { error_code: codes::GROUP_ID_NOT_FOUND, group_id: gid.clone(),
                                     ..Default::default() },
        };
        groups.push(described);
    }
    let resp = ShareGroupDescribeResponse { groups, ..Default::default() };
    let mut buf = bytes::BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

Add `find_share(&self, group_id) -> Option<Arc<ShareGroupActorHandle>>` to `GroupCoordinator`. Add `ShareDescribeView` (members + epochs + assignments + assignor name + group state string) to the actor, populated from `ShareGroupState`, and `into_described_group` building `Member { member_id, rack_id, member_epoch, client_id, client_host, subscribed_topic_names, assignment: Assignment { topic_partitions }, .. }`. Wire `ShareGroupDescribe(77)` into `network::dispatch` inline-interception list (it needs `RequestContext`), NOT `build_table`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker --test share_groups describe`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/ crates/broker/src/coordinator/unified/share/actor.rs crates/broker/src/network*
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(kip-932): ShareGroupDescribe(77) handler

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 12: Integration tests + restart-replay

**Files:**
- Create: `crates/broker/tests/share_groups.rs`
- Test harness: copy `boot()` from `crates/broker/tests/consumer_group_next_gen.rs`

- [ ] **Step 1: Write the integration tests**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
use std::sync::Arc;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::owned::share_group_describe_request::ShareGroupDescribeRequest;
use assert2::assert;

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

fn hb(group: &str, member: &str, epoch: i32, topics: &[&str]) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: group.into(), member_id: member.into(), member_epoch: epoch,
        subscribed_topic_names: Some(topics.iter().map(|t| (*t).into()).collect()),
        ..Default::default()
    }
}

#[tokio::test]
async fn single_member_join_assignment() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c1").build().await.unwrap());
    create_topic(&client, "t1", 4).await; // reuse helper pattern from consumer test
    let resp = client.send(hb("g1", "", 0, &["t1"])).await.unwrap();
    assert!(resp.error_code == 0);
    assert!(resp.member_epoch == 1);
    let total: usize = resp.assignment.unwrap().topic_partitions.iter().map(|t| t.partitions.len()).sum();
    assert!(total == 4);
}

#[tokio::test]
async fn two_members_then_describe() { /* join m1, m2; ShareGroupDescribe(["g1"]) shows 2 members */ }

#[tokio::test]
async fn member_leave_epoch_minus_one() { /* join, then hb epoch -1, describe shows 0 members */ }

#[tokio::test]
async fn state_survives_restart() {
    // join a member, persist; drop broker; re-Broker::start on same dir;
    // ShareGroupDescribe shows the group recovered from __consumer_offsets replay.
}
```

(Copy `create_topic` from `consumer_group_next_gen.rs`. For `state_survives_restart`, reuse the same `TempDir` path across two `Broker::start` calls as the consumer next-gen restart test does, if one exists; otherwise drop the handle and re-start.)

- [ ] **Step 2: Run to verify they fail (then pass after wiring)**

Run: `cargo test -p crabka-broker --test share_groups`
Expected: initially FAIL for any not-yet-wired path; PASS once Tasks 9-11 are complete.

- [ ] **Step 3: Full workspace gate**

Run: `cargo fmt --all` then `cargo fmt --check`
Run: `cargo clippy --workspace --all-targets`
Run: `cargo test --workspace`
Run: `crates/protocol/regenerate.sh && git diff --exit-code crates/protocol/generated`
Expected: all PASS, no drift.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/share_groups.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(kip-932): share-group membership integration tests

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Acceptance gate (Slice A)

1. `cargo fmt --check` clean.
2. `cargo clippy --workspace --all-targets` clean.
3. `cargo test --workspace` green (new unit + integration tests).
4. No codegen drift (`regenerate.sh` + `git diff` clean).
5. `tests/share_groups.rs`: two-member join → reconcile → describe → leave → restart-replay all green.
6. ApiVersions advertises 76/77 (v1) when `share_group.enable`; `ShareGroupHeartbeat` returns `UNSUPPORTED_VERSION` when disabled.
7. `ShareGroupDescribe.Member.assignment` encodes the `{topic_partitions}` shape (codegen fix verified).

## Self-review notes

- **Spec coverage:** every Slice A spec component maps to a task — feature gate/ApiVersions (Task 3), error codes (Task 2), config (Task 4), coordinator variant (Task 5), state machine (Task 6), assignor (Task 7), persistence+replay (Task 8), actor/heartbeat lifecycle (Task 9), handlers 76/77 (Tasks 10–11), tests incl. restart (Task 12). The spec's "no FindCoordinator change" is honored (no task touches it). The codegen fix (Task 1) was added per the user's decision and unblocks correct `ShareGroupDescribe`.
- **Type consistency:** `ShareGroupActorMessage`, `ShareGroupActorHandle`, `ShareGroupState`, `ShareMemberState`, `ShareGroupAssignor`, `ShareGroupConfig`, `ShareGroupKey`, and the `mark_share`/`get_or_create_share`/`find_share`/`replay_share_*` method names are used consistently across Tasks 5–11.
- **Open implementation confirmations (resolve during execution, not blockers):** (a) exact emitted module path for message-scoped common structs (Task 1 Step 4) — the actor/handler import in Tasks 9–11 must match it; (b) whether `api_catalog.rs` is in the broker or protocol crate — research says broker; (c) the precise `GroupCoordinator::new` call site for plumbing `ShareGroupConfig` (Task 5 Step 3); (d) `BrokerError` constructor name (`internal`) — match the crate's actual helper.
