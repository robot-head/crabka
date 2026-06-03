# KIP-848 Live Classic ↔ Next-Gen Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a single Crabka consumer group migrate live, in both directions, between the classic (`JoinGroup`/`SyncGroup`/`Heartbeat`) and KIP-848 next-gen (`ConsumerGroupHeartbeat`) protocols under `group.consumer.migration.policy`, durable across coordinator restart and validated against real JVM clients.

**Architecture:** The per-group actor (`coordinator/unified/`) already owns both RPC families and a `Group { kind: Classic | Consumer }` container. We drop the registry-level type lock, let the actor flip `group.kind` in place inside the `ConsumerGroupHeartbeat` arm (upgrade) and on the last-consumer-member departure (downgrade), serve hosted classic members by mapping their RPCs onto the epoch-based reconciler with a `ClassicMemberFacade`, and persist each flip as one atomic `__consumer_offsets` batch (tombstone old-type records + write new-type records). Slice C already built the predicate, the in-place conversion `convert_classic_to_consumer`, the policy config, and `target_to_consumer_assignment`; this plan wires the triggers, the reverse conversion, persistence, and tests.

**Tech Stack:** Rust 2024, tokio actors (mpsc + oneshot), `crabka_protocol` codecs, `assert2` test macros, JVM acceptance via Docker (`apache/kafka:4.0.0`, `confluentinc/cp-kafka:7.4.0`).

**Spec:** `docs/superpowers/specs/2026-06-03-crabka-kip-848-live-migration-64d-def-design.md`

---

## File Structure

All paths under `crates/broker/src/coordinator/` unless noted.

| File | Responsibility | Change |
|------|----------------|--------|
| `unified/persistence_next_gen.rs` | k5 `MemberMetadataValue` record codec | **Modify** — add optional `ClassicMemberMetadata` block (Task 1) |
| `unified/migration.rs` | conversion predicates + functions (both directions) | **Modify** — add `convert_consumer_to_classic`, `upgrade_pending_records`, `downgrade_pending_records` (Tasks 2, 6, 8) |
| `unified/mod.rs` | actor registry, routing, replay, admin views | **Modify** — `get_or_create_group`, k3-tombstone seed removal, classic-facade replay seed, describe/list coherence (Tasks 3, 4, 9) |
| `unified/bootstrap*.rs` (replay) | reconstruct facade from k5 on replay | **Modify** — rebuild `MemberState.classic` from the new k5 block (Task 4) |
| `handlers/consumer_group_heartbeat.rs`, `handlers/join_group.rs`, `handlers/sync_group.rs`, `handlers/leave_group.rs`, `handlers/offset_fetch.rs`, `handlers/offset_commit.rs`, `txn/handlers/txn_offset_commit.rs` | route to the one actor regardless of kind | **Modify** — call `get_or_create_group` (Task 3) |
| `unified/actor.rs` | the actor loop + per-message arms | **Modify** — live-kind branching, upgrade trigger, serve hosted classic members, downgrade trigger, conversion persistence, facade snapshotting (Tasks 5–8) |
| `unified/consumer_state.rs` | `MemberState` / `ClassicMemberFacade` | **Read** — already has the fields; no change expected |
| `tests/jvm_consumer_group_next_gen.rs` (crate `crates/broker/tests/`) | JVM acceptance | **Modify** — add the same-group bidirectional roll test (Task 11) |
| `tests/migration_inproc.rs` (new, `crates/broker/tests/`) | in-process bidirectional integration | **Create** (Task 10) |

**Parallelism (per CLAUDE.md — a conflict is the *same* file edited by two tasks):**
- **Phase 0 (parallel):** Task 1 (`persistence_next_gen.rs`) ‖ Task 2 (`migration.rs`).
- **Phase 1 (sequential — both touch `mod.rs`):** Task 3 → Task 4.
- **Phase 2 (sequential — all touch `actor.rs`):** Task 5 → 6 → 7 → 8.
- **Phase 3:** Task 9 (`mod.rs`) ‖ Task 10 (new test file) ‖ Task 11 (`jvm_…` test file) — three disjoint files, parallel.

**Commands used throughout:**
- Format: `cargo fmt`
- One module's tests: `cargo test -p crabka-broker <name-substring>`
- Lint gate: `cargo clippy -p crabka-broker --all-targets -- -D warnings`
- JVM (ignored, needs Docker): `cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --ignored --nocapture <name>`

---

## Phase 0 — Foundations (Task 1 ‖ Task 2)

### Task 1: Persist the classic facade in the k5 record

**Files:**
- Modify: `crates/broker/src/coordinator/unified/persistence_next_gen.rs` (struct `MemberMetadataValue` at `:122-182`)

The k5 record currently carries only next-gen fields, so a coordinator restart loses a hosted classic member's classic state and downgrade can't be lossless. Add an optional `ClassicMemberMetadata` block, mirroring Kafka's `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`. Greenfield — change the schema, no compat flag.

- [ ] **Step 1: Write the failing round-trip test**

Add to the `#[cfg(test)] mod tests` block at the bottom of `persistence_next_gen.rs`:

```rust
#[test]
fn member_metadata_round_trips_classic_block() {
    use bytes::Bytes;
    let v = MemberMetadataValue {
        instance_id: Some("inst-a".into()),
        rack_id: None,
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: vec!["t1".into(), "t2".into()],
        subscribed_topic_regex: None,
        server_assignor: Some("uniform".into()),
        rebalance_timeout_ms: 60_000,
        classic: Some(ClassicMemberMetadata {
            session_timeout_ms: 30_000,
            supported_protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
            last_synced_assignment: Bytes::from_static(b"asn"),
        }),
    };
    let decoded = MemberMetadataValue::decode(&v.encode()).unwrap();
    assert!(decoded == v);

    // A native consumer member (no classic block) also round-trips.
    let mut native = v.clone();
    native.classic = None;
    assert!(MemberMetadataValue::decode(&native.encode()).unwrap() == native);
}
```

- [ ] **Step 2: Run it; verify it fails to compile**

Run: `cargo test -p crabka-broker member_metadata_round_trips_classic_block`
Expected: FAIL — `ClassicMemberMetadata` undefined, `MemberMetadataValue` has no field `classic`.

- [ ] **Step 3: Add the struct and field**

Add above `MemberMetadataValue`:

```rust
/// Classic-protocol sub-state for a member hosted inside an upgraded consumer
/// group (KIP-848 migration). Mirrors Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`; lets a downgrade
/// restore the classic member losslessly after a coordinator failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberMetadata {
    pub session_timeout_ms: i32,
    pub supported_protocols: Vec<(String, Bytes)>,
    pub last_synced_assignment: Bytes,
}
```

Add the field to `MemberMetadataValue` (after `rebalance_timeout_ms`):

```rust
    pub rebalance_timeout_ms: i32,
    /// `Some` iff this is a hosted classic member; `None` for a native
    /// consumer-protocol member.
    pub classic: Option<ClassicMemberMetadata>,
```

- [ ] **Step 4: Extend encode/decode**

In `MemberMetadataValue::encode`, before `buf.freeze()`:

```rust
        buf.put_i32(self.rebalance_timeout_ms);
        match &self.classic {
            None => buf.put_i8(0),
            Some(c) => {
                buf.put_i8(1);
                buf.put_i32(c.session_timeout_ms);
                let pn = i32::try_from(c.supported_protocols.len()).expect("fits");
                buf.put_i32(pn);
                for (name, meta) in &c.supported_protocols {
                    put_string(&mut buf, name);
                    put_bytes(&mut buf, meta);
                }
                put_bytes(&mut buf, &c.last_synced_assignment);
            }
        }
        buf.freeze()
```

In `MemberMetadataValue::decode`, after `let rebalance_timeout_ms = get_i32(&mut buf)?;`, replace the `Ok(Self { … })` with:

```rust
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let classic = {
            use bytes::Buf;
            if buf.remaining() < 1 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "missing classic-presence byte",
                )));
            }
            if buf.get_i8() == 0 {
                None
            } else {
                let session_timeout_ms = get_i32(&mut buf)?;
                let pn = get_i32(&mut buf)?;
                let pcap = usize::try_from(pn.max(0)).expect("non-negative");
                let mut supported_protocols = Vec::with_capacity(pcap);
                for _ in 0..pn.max(0) {
                    let name = get_string(&mut buf)?;
                    let meta = get_bytes(&mut buf)?;
                    supported_protocols.push((name, meta));
                }
                let last_synced_assignment = get_bytes(&mut buf)?;
                Some(ClassicMemberMetadata {
                    session_timeout_ms,
                    supported_protocols,
                    last_synced_assignment,
                })
            }
        };
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            rebalance_timeout_ms,
            classic,
        })
```

- [ ] **Step 5: Fix the two in-tree `MemberMetadataValue` literals**

`snapshot_seed` (`actor.rs:1088`) and `snapshot_pending_after_change` (`actor.rs:1175`) construct `MemberMetadataValue` — add `classic: None` to both literals so the crate compiles. (Task 6 replaces `None` with the real facade snapshot; leaving `None` here is correct for native members and a safe interim for this task.)

Run: `cargo build -p crabka-broker`
Expected: compiles.

- [ ] **Step 6: Run the test; verify it passes**

Run: `cargo test -p crabka-broker member_metadata_round_trips_classic_block`
Expected: PASS.

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/persistence_next_gen.rs crates/broker/src/coordinator/unified/actor.rs
git commit -m "feat(kip-848): persist classic-member facade in the k5 record"
```

---

### Task 2: Reverse conversion `convert_consumer_to_classic`

**Files:**
- Modify: `crates/broker/src/coordinator/unified/migration.rs`

Mirror of the existing `convert_classic_to_consumer`. Re-express a consumer group's members (each carrying a `ClassicMemberFacade`) as classic `Member`s, seeding each member's assignment from the server-computed target translated to a `ConsumerProtocolAssignment` blob. Pure function, unit-tested; the actor wires it in Task 8.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `migration.rs`:

```rust
#[test]
fn downgrade_re_expresses_members_as_classic() {
    use crate::coordinator::unified::consumer_state::{ClassicMemberFacade, GroupState, MemberState};
    use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;
    use std::time::{Duration, Instant};

    let t1 = Uuid([1; 16]);
    let image = ReconcileInput {
        topic_id_by_name: [("orders".to_string(), t1)].into(),
        ..Default::default()
    };
    let mut state = GroupState::new("g");
    state.group_epoch = 7;
    let mut m = MemberState {
        member_id: "m1".into(),
        instance_id: Some("inst-a".into()),
        rack_id: None,
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: ["orders".to_string()].into(),
        subscribed_topic_regex: None,
        compiled_regex: None,
        server_assignor: None,
        rebalance_timeout: Duration::from_secs(60),
        member_epoch: 7,
        previous_member_epoch: 6,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: [(t1, vec![0, 1])].into(),
        partitions_pending_revocation: std::collections::HashMap::new(),
        last_seen: Instant::now(),
        classic: Some(ClassicMemberFacade {
            generation_id: 7,
            supported_protocols: vec![("range".into(), bytes::Bytes::from_static(b"meta"))],
            session_timeout: Duration::from_secs(30),
            last_synced_assignment: bytes::Bytes::new(),
            awaiting_sync: false,
        }),
    };
    m.sync_regex_cache();
    state.add_or_update_member(m);

    let classic = convert_consumer_to_classic(&state, &image);
    assert!(classic.group_id == "g");
    assert!(classic.generation_id == 7); // seeded from the group epoch
    let member = classic.members.get("m1").expect("member preserved");
    assert!(member.group_instance_id.as_deref() == Some("inst-a"));
    assert!(member.session_timeout == Duration::from_secs(30));
    // Seed assignment is the translated target (orders → [0,1]).
    let asn = member.assignment.clone().expect("seed assignment");
    let mut cur = &asn[..];
    use bytes::Buf;
    let _ver = cur.get_i16();
    let decoded =
        crabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment::decode(
            &mut cur, 0,
        )
        .unwrap();
    assert!(decoded.assigned_partitions[0].topic == "orders");
    assert!(decoded.assigned_partitions[0].partitions == vec![0, 1]);
}
```

- [ ] **Step 2: Run it; verify it fails to compile**

Run: `cargo test -p crabka-broker downgrade_re_expresses_members_as_classic`
Expected: FAIL — `convert_consumer_to_classic` undefined.

- [ ] **Step 3: Implement the function**

Add to `migration.rs` (it already imports `ClassicState`, `ConsumerState`, `ReconcileInput`, `target_to_consumer_assignment`):

```rust
use super::classic_state::Member as ClassicMember;

/// Convert a consumer group back into a classic group (KIP-848 downgrade, 64d-E).
/// Every member is re-expressed as a classic [`ClassicMember`] restored from its
/// [`ClassicMemberFacade`]; its assignment seed is the server-computed target
/// translated to a `ConsumerProtocolAssignment` blob, so the member keeps its
/// partitions across the flip with no spurious revoke. Committed offsets live on
/// the kind-agnostic `Group` container and are untouched here.
///
/// Precondition: every member is a hosted classic member (`classic.is_some()`),
/// which holds once the last native consumer-protocol member has departed.
pub(crate) fn convert_consumer_to_classic(
    state: &ConsumerState,
    image: &ReconcileInput,
) -> ClassicState {
    let mut classic = ClassicState::new(state.group_id.clone());
    classic.generation_id = state.group_epoch.max(0);
    classic.protocol_type = Some("consumer".into());
    for (mid, m) in &state.members {
        let facade = m
            .classic
            .as_ref()
            .expect("downgrade precondition: all members are hosted classic members");
        let seed = target_to_consumer_assignment(&m.assigned_partitions, image);
        let mut cm = ClassicMember::new(
            mid.clone(),
            m.client_id.clone(),
            m.client_host.clone(),
            facade.session_timeout,
            m.rebalance_timeout,
            facade.supported_protocols.clone(),
        )
        .with_instance_id(m.instance_id.clone());
        cm.assignment = Some(seed);
        classic.add_member(cm);
    }
    // The members are already settled on their assignment; land the group in a
    // stable generation so their next classic Heartbeat/Sync reads through.
    if let Some(name) = classic
        .members
        .values()
        .flat_map(|m| m.protocols.iter().map(|(n, _)| n.clone()))
        .next()
    {
        classic.complete_rebalance(&name);
    }
    classic
}
```

Note: confirm `ClassicMember::new`, `with_instance_id`, the public `assignment`/`protocols` fields, and `ClassicState::{generation_id, protocol_type, complete_rebalance, add_member}` signatures against `classic_state.rs`; adjust field/method names to match. The shapes are exercised in `classic_ops.rs` (e.g. `Member::new(...).with_instance_id(...)` at `classic_ops.rs:117-125`, `complete_rebalance` at `:198`).

- [ ] **Step 4: Run the test; verify it passes**

Run: `cargo test -p crabka-broker downgrade_re_expresses_members_as_classic`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/migration.rs
git commit -m "feat(kip-848): add convert_consumer_to_classic downgrade conversion"
```

---

## Phase 1 — Routing & replay (Task 3 → Task 4, sequential on `mod.rs`)

### Task 3: Single `get_or_create_group`; handlers stop rejecting on kind

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (`get_or_create` `:255-298`, plus a new public method)
- Modify: `crates/broker/src/handlers/{consumer_group_heartbeat.rs:49, join_group.rs:59, sync_group.rs, leave_group.rs, offset_fetch.rs:79,298, offset_commit.rs:109}`
- Modify: `crates/broker/src/txn/handlers/txn_offset_commit.rs:112`

Today `get_or_create_classic`/`get_or_create_consumer` return `None` on a kind mismatch and handlers turn that into a wrong-protocol error. Add `get_or_create_group(group_id)` that returns the one actor for an id regardless of kind, creating it with a default kind derived from the *calling* RPC when absent. Keep the old methods for the create-with-specific-kind first-RPC paths, but they must no longer reject an existing actor of the other kind — they return the existing actor.

- [ ] **Step 1: Write the failing test**

Add to `mod.rs`'s `#[cfg(test)] mod tests`:

```rust
#[test]
fn get_or_create_group_returns_the_one_actor_regardless_of_kind() {
    let coord = test_coordinator(); // existing test helper; see other tests in this module
    // First RPC is classic → actor exists, classic-kind.
    let a = coord.get_or_create_group("g", GroupKindTag::Classic);
    // A consumer RPC for the same id must reach the SAME actor, not be rejected.
    let b = coord.get_or_create_group("g", GroupKindTag::Consumer);
    assert!(std::sync::Arc::ptr_eq(&a, &b));
}
```

If a `test_coordinator()` helper doesn't exist, mirror `make_coordinator()` from `actor.rs` tests (`actor.rs:1275-1285`).

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker get_or_create_group_returns_the_one_actor`
Expected: FAIL — `get_or_create_group` undefined.

- [ ] **Step 3: Add `get_or_create_group` and relax the lock**

In `mod.rs`, add:

```rust
/// Get the one actor for `group_id`, spawning it with `initial_kind` if absent.
/// Unlike the kind-specific helpers this NEVER rejects an existing actor of the
/// other kind — after slice 64d the actor flips kind in place, so a group is no
/// longer pinned to its spawn kind. The kind argument only decides the spawn
/// kind for a brand-new group.
#[must_use]
pub fn get_or_create_group(
    self: &Arc<Self>,
    group_id: &str,
    initial_kind: GroupKindTag,
) -> Arc<GroupActorHandle> {
    if let Some(h) = self.groups.get(group_id) {
        if !h.value().tx.is_closed() {
            return h.value().clone();
        }
        drop(h);
        self.groups.remove(group_id);
    }
    let h = Arc::new(GroupActorHandle::spawn(
        group_id.into(),
        initial_kind,
        self.config.clone(),
        self.metadata.clone(),
        self.offsets_log.clone(),
        self.clone(),
    ));
    let inserted = self.groups.entry(group_id.into()).or_insert(h).value().clone();
    if initial_kind == GroupKindTag::Consumer
        && let Some(seed) = self.cached_seed(group_id)
    {
        let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
    }
    inserted
}
```

Change `get_or_create` (`:255-298`) so the kind-mismatch branch returns the existing actor instead of `None`: delete the `if h.value().kind != kind { return None; }` and the post-insert `if inserted.kind != kind { return None; }` guards, and make `get_or_create_classic`/`get_or_create_consumer` return `Arc<GroupActorHandle>` (not `Option<…>`) by delegating to `get_or_create_group`. Update their callers (below) to drop the `let Some(...) else { reject }`.

- [ ] **Step 4: Update the handlers**

In `handlers/consumer_group_heartbeat.rs:47-49`, replace:

```rust
        let Some(handle) = coordinator.get_or_create_consumer(&req.group_id) else {
            // ... reject ...
        };
```

with:

```rust
        let handle = coordinator.get_or_create_group(&req.group_id, GroupKindTag::Consumer);
```

In `handlers/join_group.rs:59`, replace the `get_or_create_classic(&req.group_id)` + `Some` guard with:

```rust
        .get_or_create_group(&req.group_id, GroupKindTag::Classic);
```

Do the same at `sync_group.rs`, `leave_group.rs`, `offset_fetch.rs:79,298`, `offset_commit.rs:109`, `txn/handlers/txn_offset_commit.rs:112` — each currently calls a kind-specific getter; route through `get_or_create_group` with the kind matching the RPC (`Classic` for classic RPCs/offset ops on classic groups, `Consumer` for the next-gen heartbeat). Import `GroupKindTag` where needed.

The actor still replies with the right error code for an RPC its *current* kind can't serve (e.g. a classic `Heartbeat` to a consumer-kind group that doesn't host that member); Tasks 6–8 make those arms convert or serve instead of erroring.

- [ ] **Step 5: Run the test + the existing suite; verify green**

Run: `cargo test -p crabka-broker get_or_create_group_returns_the_one_actor`
Expected: PASS.
Run: `cargo test -p crabka-broker coordinator::unified`
Expected: PASS (behavior preserved — single-kind groups still served exactly as before; the only change is mismatches return the actor instead of `None`, and no test yet drives a mismatch that reaches a handler).

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/mod.rs crates/broker/src/handlers crates/broker/src/txn/handlers/txn_offset_commit.rs
git commit -m "feat(kip-848): route both RPC families to one actor via get_or_create_group"
```

---

### Task 4: Replay reconstructs the post-conversion kind (the downgrade trap)

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (`replay_next_gen_tombstone` `:587-621`)
- Modify: `crates/broker/src/coordinator/bootstrap.rs` (facade reconstruction from the k5 block)

A k3 `GroupMetadata` tombstone today only zeroes the seed epoch, leaving the group in `coordinator.seeds`; a downgraded group would replay back as an empty next-gen group. Fix: the k3 tombstone removes the seed entry entirely, so the later-written k2 record reconstructs the group as classic. Also rebuild `MemberState.classic` from the new k5 block on replay.

- [ ] **Step 1: Write the failing replay test**

Add to `bootstrap.rs`'s test module (or create one mirroring existing replay tests) a record-stream replay test:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_then_downgrade_replays_as_classic() {
    // Build a record stream: classic k2 GroupMetadata → upgrade (k2 tombstone +
    // k3/k5) → downgrade (k3 tombstone + k5 tombstone + k2 GroupMetadata).
    // Replay it and assert the group comes back CLASSIC with member "m1".
    let coord = replay_stream(&[
        rec_classic_group_metadata("g", &["m1"]),       // initial classic group
        tombstone_classic_group_metadata("g"),          // upgrade: drop k2
        rec_next_gen_group_metadata("g", 1),            // upgrade: k3
        rec_member_metadata("g", "m1"),                 // upgrade: k5
        tombstone_next_gen_group_metadata("g"),         // downgrade: drop k3
        tombstone_member_metadata("g", "m1"),           // downgrade: drop k5
        rec_classic_group_metadata("g", &["m1"]),       // downgrade: write k2
    ])
    .await;

    assert!(coord.group_type("g") != Some(GroupType::NextGen));
    let snap = coord.describe_group("g").await.expect("classic group present");
    assert!(snap.members.iter().any(|m| m.member_id == "m1"));
}
```

Use the existing replay-driving helper in `bootstrap.rs` tests; if there's no `replay_stream`, build the `Record` vec and call the same entry point `replay_records` uses (`bootstrap.rs:177-212`) then `finalize_bootstrap`. The `rec_*`/`tombstone_*` helpers encode keys via `persistence::encode_key` / `persistence_next_gen::encode_key` and the `*Value::encode()` codecs.

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker upgrade_then_downgrade_replays_as_classic`
Expected: FAIL — the group replays back as next-gen (present in `seeds`) so `group_type` is `NextGen` / `describe_group` returns `None`.

- [ ] **Step 3: Make the k3 tombstone remove the seed**

In `mod.rs` `replay_next_gen_tombstone`, special-case `GroupMetadata`:

```rust
    pub fn replay_next_gen_tombstone(&self, key: &persistence_next_gen::NextGenKey) {
        use persistence_next_gen::NextGenKey as K;
        // A GroupMetadata (k3) tombstone deletes the whole next-gen group identity
        // — used by a downgrade flip. Drop the seed entirely so bootstrap does not
        // resurrect an empty next-gen group; the later-replayed k2 GroupMetadata
        // record (log order) reconstructs it as classic.
        if let K::GroupMetadata { group_id } = key {
            self.seeds.remove(group_id);
            self.seeds_cache.remove(group_id);
            return;
        }
        // ... existing member/target/current scrub for the non-GroupMetadata keys ...
    }
```

Keep the existing per-member/target/current scrub for the other key variants.

- [ ] **Step 4: Reconstruct the facade from the k5 block on replay**

In `bootstrap.rs` where a k5 `MemberMetadataValue` is applied into the next-gen seed/`MemberState`, set `MemberState.classic` from the new block:

```rust
    classic: mm.classic.as_ref().map(|c| ClassicMemberFacade {
        generation_id: seed.group_epoch,
        supported_protocols: c.supported_protocols.clone(),
        session_timeout: std::time::Duration::from_millis(
            u64::try_from(c.session_timeout_ms).unwrap_or(30_000),
        ),
        last_synced_assignment: c.last_synced_assignment.clone(),
        awaiting_sync: true,
    }),
```

Place this wherever the replay path materializes a `MemberState` (or the `GroupSeed`'s member map that `apply_seed` consumes); follow the existing apply path. If replay stores only `MemberMetadataValue` in the seed and `apply_seed` (`actor.rs`) builds `MemberState`, add the mapping in `apply_seed` instead — keep the facade reconstruction in exactly one place.

- [ ] **Step 5: Run the test; verify it passes**

Run: `cargo test -p crabka-broker upgrade_then_downgrade_replays_as_classic`
Expected: PASS.
Run: `cargo test -p crabka-broker bootstrap`
Expected: PASS (existing replay tests still green — upgrade-only replay still yields a consumer group because its k3 is live).

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/mod.rs crates/broker/src/coordinator/bootstrap.rs
git commit -m "fix(kip-848): replay a downgraded group as classic; rebuild facade from k5"
```

---

## Phase 2 — Actor surgery (Tasks 5 → 6 → 7 → 8, sequential on `actor.rs`)

### Task 5: Actor loop branches on live `group.kind`

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (`actor_loop` `:261-474`)

Before any flip can be served, the loop must stop assuming its spawn-time `kind`. The `tick` arm's `match kind { … expect("consumer kind") }` (`:441-464`) and `tick_period` (`:277-280`) panic the instant a group flips. Make both consult the live `group`.

- [ ] **Step 1: Write the failing test**

Add to `actor.rs` tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_tick_does_not_panic_after_in_place_flip() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_group("g", GroupKindTag::Classic);
    // Force a flip by swapping the group kind via a test-only message, then let
    // a tick fire. Pre-fix, the tick arm's `expect("consumer kind")` (driven by
    // the captured spawn `kind`) panics; post-fix it dispatches on group.kind.
    handle
        .tx
        .send(GroupActorMessage::TestForceConsumerKind)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    // Actor still alive (sender not closed) ⇒ the tick did not panic the task.
    assert!(!handle.tx.is_closed());
}
```

Add a test-only message variant guarded by `#[cfg(test)]`:

```rust
    #[cfg(test)]
    TestForceConsumerKind,
```

and an arm that does `group = Group::new_consumer(group.group_id.clone());`.

- [ ] **Step 2: Run it; verify it fails (panic / closed sender)**

Run: `cargo test -p crabka-broker actor_tick_does_not_panic_after_in_place_flip`
Expected: FAIL — tick task panics on `expect("consumer kind")`, sender closes.

- [ ] **Step 3: Dispatch the tick on live `group.kind`**

Replace the `tick` arm body (`:441-464`) with a match on the live container:

```rust
            _ = tick.tick() => {
                if let Some(state) = group.as_consumer_mut() {
                    if handle_session_tick(state, &config, &*metadata, &*offsets_log, &coordinator)
                        .await
                        .is_err()
                    {
                        break;
                    }
                } else if let Some(state) = group.as_classic_mut() {
                    let gid = group.group_id.clone();
                    let dropped = state.expire_dead_members(Instant::now());
                    if !dropped.is_empty() {
                        tracing::info!(group = %gid, dropped = ?dropped, "expired members; waking joiners");
                        maybe_complete_classic(state, &mut parked.joiners);
                    }
                }
            }
```

Set a single tick period that serves both kinds — use the shorter classic cadence so classic session expiry stays responsive after a downgrade:

```rust
    let mut tick = tokio::time::interval(Duration::from_secs(1));
```

(Delete the `let tick_period = match kind { … }` block and the now-unused `kind` reads in the loop; keep the spawn-time `kind` only for the initial `Group::new_*` selection at `:270-273`.)

- [ ] **Step 4: Run the test + suite; verify green**

Run: `cargo test -p crabka-broker actor_tick_does_not_panic_after_in_place_flip`
Expected: PASS.
Run: `cargo test -p crabka-broker coordinator::unified`
Expected: PASS — consumer-group session expiry now ticks every 1s instead of `heartbeat_interval`; confirm the next-gen session-tick tests still pass (they assert on expiry occurring, not on its exact cadence). If a test asserts an exact tick cadence, adjust it to the 1s unified tick.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/actor.rs
git commit -m "refactor(kip-848): actor loop dispatches tick on live group.kind"
```

---

### Task 6: Upgrade trigger (classic → consumer) + persist the flip

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (the `Heartbeat` arm `:290-315`)
- Modify: `crates/broker/src/coordinator/unified/migration.rs` (add `upgrade_pending_records`)

When `ConsumerGroupHeartbeat` hits a classic-kind group, attempt the upgrade before serving.

- [ ] **Step 1: Write the failing test**

Add to `actor.rs` tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_heartbeat_upgrades_a_classic_group() {
    let (coord, log) = make_coordinator_with_topic("t", 2); // helper: metadata image with topic t/2 parts
    let handle = coord.get_or_create_group("g", GroupKindTag::Classic);

    // Seed one classic member subscribed to "t" (via a ClassicJoin round-trip).
    classic_join(&handle, "m-classic", "t").await;

    // A native consumer-protocol heartbeat for the same group under the default
    // bidirectional policy → the group upgrades and both members are served.
    let resp = consumer_heartbeat(&handle, "", 0, &["t"]).await;
    assert!(resp.error_code == codes::NONE);

    // The group is now consumer-kind and persisted a k2 tombstone + k3/k5.
    let describe = describe(&handle).await;
    assert!(describe.members.len() == 2);
    // The persisted batch tombstoned the classic k2 GroupMetadata.
    assert!(log.contains_tombstone_for_classic_group_metadata("g"));
}
```

Add `make_coordinator_with_topic`, `classic_join`, `consumer_heartbeat`, `describe` test helpers (small wrappers over the existing message sends already used in `actor.rs` tests). Add `contains_tombstone_for_classic_group_metadata` to the `InMemoryOffsetsLog` fake (`offsets_log::fake`) — scan appended records for a k2 key with `value: None`.

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker consumer_heartbeat_upgrades_a_classic_group`
Expected: FAIL — the heartbeat arm replies `GROUP_ID_NOT_FOUND` for a classic-kind group (no upgrade yet).

- [ ] **Step 3: Add `upgrade_pending_records` to migration.rs**

```rust
use super::actor::PendingRecords; // make PendingRecords pub(crate) if not already

/// The atomic record batch for an upgrade: tombstone the classic k2
/// GroupMetadata and write the full next-gen record set for the converted
/// group. The k2 tombstone is emitted via the classic key codec.
pub(crate) fn upgrade_pending_records(state: &ConsumerState) -> PendingRecords {
    let mut pending = super::actor::full_pending_records(state); // k3 + k5/k6/k7/k8 for every member
    pending.classic_group_metadata_tombstone = true; // new field on PendingRecords (Step 4)
    pending
}
```

If a "write the whole consumer group" helper doesn't exist, add `full_pending_records(state: &ConsumerState) -> PendingRecords` next to `snapshot_pending_after_change` in `actor.rs` that emits group+target metadata and member/current/target records for *all* members (loop over `state.members` keys, reuse the per-member construction already in `snapshot_pending_after_change:1171-1226`, and populate each `MemberMetadataValue.classic` from the member's facade).

- [ ] **Step 4: Teach `PendingRecords`/`into_batch` to emit the k2 tombstone**

In `actor.rs`, add to `PendingRecords` (`:985-993`):

```rust
    /// When set, the batch also tombstones the classic k2 `GroupMetadata` record
    /// for this group (used by an upgrade flip).
    pub classic_group_metadata_tombstone: bool,
```

In `into_batch` (`:1004-1068`), before building the `RecordBatch`, if the flag is set push a k2 tombstone:

```rust
        if self.classic_group_metadata_tombstone {
            push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group_id.into(),
                    },
                ),
                None,
            );
        }
```

Confirm the classic key encoder name/path against `persistence.rs` (the key enum is `Key::GroupMetadata { group_id }` per `persistence.rs:22-37`); add a `pub fn encode_key` there if only a decode path exists.

- [ ] **Step 5: Wire the trigger into the Heartbeat arm**

Replace the `Heartbeat` arm's early `GROUP_ID_NOT_FOUND` guard (`:291-297`) with an upgrade attempt:

```rust
                    GroupActorMessage::Heartbeat { request, client_host, reply } => {
                        // Upgrade a classic-kind group in place if policy + convertibility allow.
                        if group.is_classic() {
                            let convertible = group
                                .as_classic()
                                .is_some_and(migration::classic_is_convertible);
                            if config.migration_policy.allows_upgrade() && convertible {
                                let classic = group.as_classic().expect("classic kind");
                                let new_state = migration::convert_classic_to_consumer(classic);
                                let pending = migration::upgrade_pending_records(&new_state);
                                // Reconcile so the converted members get a target before persist.
                                let mut state = new_state;
                                reconciler::reconcile_if_dirty(
                                    &mut state, &metadata.snapshot(),
                                    config.find_assignor("uniform").expect("uniform").as_ref(),
                                );
                                if flush_pending(&state, pending, &*offsets_log, &coordinator, chrono_now_ms())
                                    .await
                                    .is_err()
                                {
                                    let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                        error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                        ..Default::default()
                                    });
                                    break;
                                }
                                *group.kind_mut() = GroupKind::Consumer(state);
                            } else {
                                let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                    error_code: codes::GROUP_ID_NOT_FOUND, // pin empirically (Open Q)
                                    ..Default::default()
                                });
                                continue;
                            }
                        }
                        let Some(state) = group.as_consumer_mut() else {
                            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                error_code: codes::GROUP_ID_NOT_FOUND,
                                ..Default::default()
                            });
                            continue;
                        };
                        match handle_heartbeat(
                            state, &config, &*metadata, &*offsets_log, &coordinator,
                            &request, &client_host,
                        ).await {
                            Ok(resp) => { let _ = reply.send(resp); }
                            Err(e) => { /* existing log-write-failure handling */ }
                        }
                    }
```

Add `kind_mut(&mut self) -> &mut GroupKind` to `group.rs` (sibling of `as_consumer_mut`). Import `GroupKind`, `migration`, `reconciler`, `chrono_now_ms`, `flush_pending` at the top of `actor.rs` (most already imported).

- [ ] **Step 6: Run the test; verify it passes**

Run: `cargo test -p crabka-broker consumer_heartbeat_upgrades_a_classic_group`
Expected: PASS.
Run: `cargo test -p crabka-broker coordinator::unified`
Expected: PASS (a `policy=disabled` coordinator still rejects — covered in Task 10).

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/actor.rs crates/broker/src/coordinator/unified/migration.rs crates/broker/src/coordinator/unified/group.rs crates/broker/src/coordinator/unified/persistence.rs
git commit -m "feat(kip-848): upgrade a classic group to consumer on ConsumerGroupHeartbeat"
```

---

### Task 7: Serve hosted classic members inside a consumer group

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (the `ClassicJoin` `:332-353`, `ClassicSync` `:354-374`, `ClassicHeartbeat` `:375-380` arms)
- Modify: `crates/broker/src/coordinator/unified/migration.rs` (add the serve helpers)

After an upgrade (or when a new classic member joins an already-upgraded group), classic RPCs must be served from the consumer-group machinery instead of erroring.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_classic_member_syncs_translated_assignment() {
    let (coord, _log) = make_coordinator_with_topic("t", 2);
    let handle = coord.get_or_create_group("g", GroupKindTag::Classic);
    classic_join(&handle, "m-classic", "t").await;
    let _ = consumer_heartbeat(&handle, "", 0, &["t"]).await; // upgrade

    // The classic member's Heartbeat now signals it must rejoin to pick up the
    // server target, then SyncGroup returns a real ConsumerProtocolAssignment.
    let hb = classic_heartbeat(&handle, "m-classic", /*generation*/ -1).await;
    assert!(hb == codes::REBALANCE_IN_PROGRESS || hb == codes::NONE);

    let join = classic_join_raw(&handle, "m-classic", "t").await;
    let sync = classic_sync(&handle, "m-classic", join.generation_id).await;
    assert!(sync.error_code == codes::NONE);
    // Assignment decodes to a non-empty ConsumerProtocolAssignment for topic "t".
    let mut cur = &sync.assignment[..];
    use bytes::Buf;
    let _v = cur.get_i16();
    let a = crabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
    assert!(a.assigned_partitions.iter().any(|tp| tp.topic == "t"));
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker hosted_classic_member_syncs_translated_assignment`
Expected: FAIL — `ClassicSync` on a consumer-kind group replies `UNKNOWN_MEMBER_ID` (`:355-360`).

- [ ] **Step 3: Add serve helpers to migration.rs**

```rust
use super::actor::{JoinResult, JoinResultMember, SyncResult};

/// Serve a classic `Heartbeat` for a hosted member: refresh liveness and signal
/// a rejoin while the member owes a sync (target advanced). Returns the error code.
pub(crate) fn serve_classic_heartbeat(state: &mut ConsumerState, member_id: &str) -> i16 {
    match state.members.get_mut(member_id) {
        None => crate::codes::UNKNOWN_MEMBER_ID,
        Some(m) => {
            m.last_seen = std::time::Instant::now();
            let owes_sync = m.classic.as_ref().is_some_and(|c| c.awaiting_sync)
                || m.member_epoch < state.group_epoch;
            if owes_sync { crate::codes::REBALANCE_IN_PROGRESS } else { crate::codes::NONE }
        }
    }
}

/// Serve a classic `JoinGroup` (rejoin or new hosted member): the group is
/// server-assigned, so return a single-member view with the member as its own
/// leader and `generation_id` = group epoch. The real assignment follows on SyncGroup.
pub(crate) fn serve_classic_join(
    state: &mut ConsumerState,
    member_id: &str,
    subscription: std::collections::HashSet<String>,
    protocols: Vec<(String, bytes::Bytes)>,
    session_timeout: std::time::Duration,
    rebalance_timeout: std::time::Duration,
    instance_id: Option<String>,
) -> JoinResult {
    // Insert or refresh the hosted classic member; a subscription change dirties
    // the group so the next reconcile recomputes the target.
    // (Build a MemberState with classic: Some(facade{awaiting_sync:true}) and call
    //  state.add_or_update_member; reuse convert's facade construction.)
    // ... see convert_classic_to_consumer for the MemberState shape ...
    JoinResult {
        error_code: crate::codes::NONE,
        generation_id: state.group_epoch,
        protocol_type: Some("consumer".into()),
        protocol_name: protocols.first().map(|(n, _)| n.clone()),
        leader: member_id.to_string(),
        member_id: member_id.to_string(),
        members: vec![JoinResultMember {
            member_id: member_id.to_string(),
            group_instance_id: instance_id,
            metadata: bytes::Bytes::new(),
        }],
    }
}

/// Serve a classic `SyncGroup` for a hosted member: translate its server target
/// to a ConsumerProtocolAssignment blob, cache it, clear awaiting_sync.
pub(crate) fn serve_classic_sync(
    state: &mut ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> SyncResult {
    let Some(m) = state.members.get(member_id) else {
        return SyncResult { error_code: crate::codes::UNKNOWN_MEMBER_ID, ..Default::default() };
    };
    let blob = target_to_consumer_assignment(&m.assigned_partitions, image);
    if let Some(m) = state.members.get_mut(member_id) {
        if let Some(c) = m.classic.as_mut() {
            c.last_synced_assignment = blob.clone();
            c.awaiting_sync = false;
        }
    }
    SyncResult {
        error_code: crate::codes::NONE,
        assignment: blob,
        protocol_type: Some("consumer".into()),
        protocol_name: None,
    }
}
```

- [ ] **Step 4: Route the classic arms to the serve helpers when consumer-kind**

In each classic arm, when `group.as_classic_mut()` is `None` (i.e. consumer-kind), serve via the helper instead of erroring. E.g. `ClassicSync`:

```rust
                    GroupActorMessage::ClassicSync { req, reply } => {
                        if let Some(state) = group.as_classic_mut() {
                            // ... existing classic path ...
                        } else if let Some(state) = group.as_consumer_mut() {
                            let result = migration::serve_classic_sync(state, &req.member_id, &metadata.snapshot());
                            let _ = reply.send(result);
                        }
                    }
```

Mirror for `ClassicJoin` (call `serve_classic_join`, decode the request's protocol metadata to a subscription via `migration::decode_consumer_subscription`) and `ClassicHeartbeat` (call `serve_classic_heartbeat`). A new classic join to a consumer-kind group dirties the group; persist the membership change with `snapshot_pending_after_change` + `flush_pending` exactly as the next-gen path does after a member add.

- [ ] **Step 5: Run the test; verify it passes**

Run: `cargo test -p crabka-broker hosted_classic_member_syncs_translated_assignment`
Expected: PASS.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/actor.rs crates/broker/src/coordinator/unified/migration.rs
git commit -m "feat(kip-848): serve hosted classic members from the consumer-group reconciler"
```

---

### Task 8: Downgrade trigger (last consumer member leaves) + persist

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (a post-membership-change re-evaluation, reachable from the consumer `handle_heartbeat` leave path, `ClassicLeave`, and session-tick eviction)
- Modify: `crates/broker/src/coordinator/unified/migration.rs` (add `downgrade_pending_records`)

After any member departs a consumer-kind group, if no native consumer-protocol member remains (every survivor is a hosted classic member) and policy allows downgrade, flip to classic.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_consumer_member_leaving_downgrades_to_classic() {
    let (coord, log) = make_coordinator_with_topic("t", 2);
    let handle = coord.get_or_create_group("g", GroupKindTag::Classic);
    classic_join(&handle, "m-classic", "t").await;
    let _ = consumer_heartbeat(&handle, "", 0, &["t"]).await; // upgrade; group now consumer-kind, hosts m-classic + the native consumer
    let native_id = /* member_id from the heartbeat response */;

    // The native consumer leaves (member_epoch = -1 leave heartbeat).
    let _ = consumer_heartbeat_leave(&handle, &native_id).await;

    // Only the hosted classic member remains → downgrade to classic.
    let snap = coord.describe_group("g").await.expect("classic group");
    assert!(snap.members.iter().any(|m| m.member_id == "m-classic"));
    assert!(coord.group_type("g") != Some(GroupType::NextGen));
    // The flip persisted k3 + k5 tombstones and a k2 GroupMetadata.
    assert!(log.contains_tombstone_for_next_gen_group_metadata("g"));
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker last_consumer_member_leaving_downgrades_to_classic`
Expected: FAIL — the group stays consumer-kind after the native member leaves.

- [ ] **Step 3: Add `downgrade_pending_records` to migration.rs**

```rust
/// The atomic record batch for a downgrade: tombstone the consumer group's k3 +
/// every member's k5/k6/k7/k8, and write the classic k2 GroupMetadata with the
/// re-expressed members. `removed` lists member ids that left before the flip
/// (their next-gen records also need tombstoning).
pub(crate) fn downgrade_pending_records(
    consumer: &ConsumerState,
    classic: &ClassicState,
) -> PendingRecords {
    let mut pending = PendingRecords {
        next_gen_group_metadata_tombstone: true, // new flag (Step 4)
        classic_group_metadata: Some(super::actor::classic_group_metadata_record(classic)),
        ..Default::default()
    };
    for mid in consumer.members.keys() {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    pending
}
```

- [ ] **Step 4: Extend `PendingRecords`/`into_batch` for the k3-tombstone + k2-write**

Add two fields to `PendingRecords`:

```rust
    /// Tombstone the next-gen k3 `GroupMetadata` (downgrade flip).
    pub next_gen_group_metadata_tombstone: bool,
    /// Write the classic k2 `GroupMetadata` value (downgrade flip).
    pub classic_group_metadata: Option<crate::coordinator::unified::persistence::GroupMetadataValue>,
```

In `into_batch`, emit the k3 tombstone (`encode_key(&NextGenKey::GroupMetadata{..})`, `None`) when the flag is set, and the k2 write (classic `encode_key` + value `encode()`) when present. Add `classic_group_metadata_record(state: &ClassicState) -> persistence::GroupMetadataValue` to `actor.rs` (or reuse the classic persistence path the classic coordinator already uses to serialize a `GroupMetadata` value — find it in `persistence.rs`).

- [ ] **Step 5: Add `maybe_downgrade` and call it after every membership change**

In `actor.rs`, add:

```rust
/// After a membership change on a consumer-kind group, downgrade to classic if no
/// native consumer-protocol member remains and policy allows it. Returns `true`
/// if a flip happened (caller must not also touch `group` as consumer after).
async fn maybe_downgrade(
    group: &mut Group,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::GroupCoordinator,
) -> Result<bool, crate::error::BrokerError> {
    let Some(state) = group.as_consumer() else { return Ok(false) };
    if !config.migration_policy.allows_downgrade() {
        return Ok(false);
    }
    let has_native = state.members.values().any(|m| m.classic.is_none());
    if has_native || state.members.is_empty() {
        return Ok(false); // still mixed, or fully empty (let normal empty-group cleanup run)
    }
    let image = metadata.snapshot();
    let classic = migration::convert_consumer_to_classic(state, &image);
    let pending = migration::downgrade_pending_records(state, &classic);
    let batch = pending.into_batch(&group.group_id, chrono_now_ms());
    offsets_log.append(batch).await?;
    coordinator.mark_classic_after_downgrade(&group.group_id); // drops the consumer seed; see note
    *group.kind_mut() = GroupKind::Classic(classic);
    Ok(true)
}
```

Call `maybe_downgrade(&mut group, …)` immediately after the consumer `Heartbeat` arm processes a leave (member_epoch < 0 or eviction), after `ClassicLeave` when the group is consumer-kind, and in the consumer session-tick eviction path. Add `mark_classic_after_downgrade` to `mod.rs` (remove the `seeds`/`seeds_cache` entry so a respawn doesn't re-hydrate as consumer, then `mark_classic`). Keep the call sites minimal — one helper, three call points.

- [ ] **Step 6: Run the test; verify it passes**

Run: `cargo test -p crabka-broker last_consumer_member_leaving_downgrades_to_classic`
Expected: PASS.
Run: `cargo test -p crabka-broker coordinator::unified`
Expected: PASS.

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/actor.rs crates/broker/src/coordinator/unified/migration.rs
git commit -m "feat(kip-848): downgrade to classic when the last consumer member leaves"
```

---

## Phase 3 — Admin coherence + tests (Task 9 ‖ Task 10 ‖ Task 11)

### Task 9: `describe`/`list` report a migrating group coherently

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (`list_groups` `:434-454`, `describe_group` `:456-469`)

These filter to `GroupKindTag::Classic` only and message `ClassicInspect`. Make them surface a group regardless of its current kind, projecting a consumer-kind group (including hosted classic members) into the same `GroupSnapshot`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reports_an_upgraded_group() {
    let (coord, _log) = make_coordinator_with_topic("t", 2);
    let handle = coord.get_or_create_group("g", GroupKindTag::Classic);
    classic_join(&handle, "m-classic", "t").await;
    let _ = consumer_heartbeat(&handle, "", 0, &["t"]).await; // upgrade → consumer-kind
    let snap = coord.describe_group("g").await.expect("group visible after upgrade");
    assert!(snap.members.len() == 2);
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `cargo test -p crabka-broker describe_reports_an_upgraded_group`
Expected: FAIL — `describe_group` returns `None` for a non-classic handle (`:459-461`).

- [ ] **Step 3: Project consumer-kind groups into the snapshot**

Drop the `if handle.kind != GroupKindTag::Classic { return None; }` guards. Send a kind-agnostic inspect: add a `GroupActorMessage::InspectAny { reply: oneshot::Sender<GroupSnapshot> }` arm in `actor.rs` that builds a `GroupSnapshot` from whichever kind the live `group` is (classic via `build_classic_view`, consumer by mapping `state.members` → `MemberSnapshot` with the translated assignment and the group epoch as `generation_id`). Have `list_groups`/`describe_group` use `InspectAny` and stop filtering by `handle.kind`. Remove the `consumer_group_ids` exclusion from the classic-only admin path if it double-counts.

- [ ] **Step 4: Run the test + the JVM-free admin tests; verify green**

Run: `cargo test -p crabka-broker describe_reports_an_upgraded_group`
Expected: PASS.
Run: `cargo test -p crabka-broker coordinator::unified`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/mod.rs crates/broker/src/coordinator/unified/actor.rs
git commit -m "feat(kip-848): describe/list report a migrating group coherently"
```

---

### Task 10: In-process bidirectional integration test

**Files:**
- Create: `crates/broker/tests/migration_inproc.rs`

End-to-end against the coordinator (no Docker), covering both flips, gap/overlap, static membership, `policy=disabled`, and replay.

- [ ] **Step 1: Write the integration tests**

Create `crates/broker/tests/migration_inproc.rs` driving a `GroupCoordinator` with a 4-partition topic image. Tests (each `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`):

```rust
// 1. upgrade_then_downgrade_round_trip:
//    classic member m1 joins → consumer member c1 heartbeats (upgrade) →
//    assert m1+c1 hold disjoint partitions covering all 4 → c1 leaves
//    (downgrade) → assert group is classic and m1 holds an assignment.
//
// 2. no_gap_or_overlap_across_flip:
//    after the upgrade, collect every assigned partition across both members;
//    assert the union == {0,1,2,3} and the pairwise intersection is empty.
//
// 3. static_member_identity_survives_both_flips:
//    m1 joins with group.instance.id = "inst-a"; upgrade; downgrade;
//    assert the restored classic member still carries instance_id "inst-a".
//
// 4. policy_disabled_keeps_groups_separate:
//    build the coordinator with migration_policy = Disabled; a consumer
//    heartbeat for a classic group is rejected (no upgrade); group stays classic.
//
// 5. committed_offsets_survive_a_flip:
//    commit an offset for (t,0) on the classic group; upgrade; assert
//    FetchCommitted still returns it; downgrade; assert it's still there.
```

Build the coordinator with the test helpers from `actor.rs`'s test module (promote `make_coordinator` to a small shared `tests/support` helper if needed, or re-create it inline — it's ~10 lines). Use raw `GroupActorMessage` sends for classic and next-gen RPCs.

- [ ] **Step 2: Run them; watch each fail then pass as you implement**

Run: `cargo test -p crabka-broker --test migration_inproc`
Expected: all 5 PASS once Tasks 1–9 are in. (If `policy=disabled` rejection uses a code other than what Kafka returns, leave a `// TODO(empirical): confirm code` and assert on rejection-not-upgrade rather than the exact code.)

- [ ] **Step 3: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/migration_inproc.rs
git commit -m "test(kip-848): in-process bidirectional migration integration suite"
```

---

### Task 11: JVM acceptance — two consumers, one group, rolled both ways

**Files:**
- Modify: `crates/broker/tests/jvm_consumer_group_next_gen.rs` (add the test + a concurrent-consumer helper)

Single-broker control-plane (runs on the Mac), `#[ignore = "requires Docker"]`. A real `cp-kafka:7.4.0` classic consumer and a real `apache/kafka:4.0.0` `group.protocol=consumer` consumer in the same group under default `bidirectional` policy.

- [ ] **Step 1: Add a concurrent-consumer helper**

Add near `docker_run` in the test file:

```rust
/// Run a console consumer in the background, returning a handle whose join
/// yields the captured stdout. Uses spawn_blocking so two consumers overlap.
fn spawn_consumer(image: &'static str, script: String) -> tokio::task::JoinHandle<String> {
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new("docker")
            .arg("run").arg("--rm").arg("--add-host=host.docker.internal:host-gateway")
            .arg(image).arg("bash").arg("-c").arg(&script)
            .output().expect("docker run");
        String::from_utf8_lossy(&out.stdout).into_owned()
    })
}
```

- [ ] **Step 2: Write the acceptance test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires Docker"]
async fn jvm_kip848_classic_and_consumer_in_one_group_migrate() {
    let (broker, _dir) = start_host_broker().await;
    // Create topic "mig" with 4 partitions and produce 8 records (2 per partition)
    // via a cp-kafka producer (reuse the existing produce helper / kafka-console-producer).
    create_topic_and_produce("mig", 4, 8).await;

    let group = "g-migrate";
    // Classic consumer (cp-kafka 7.4.0), long-lived, prints partition assignments.
    let classic = spawn_consumer(
        "confluentinc/cp-kafka:7.4.0",
        format!(
            "kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic mig --group {group} \
             --from-beginning --property print.partition=true --timeout-ms 20000 --max-messages 4"
        ),
    );
    // Give the classic member time to form the group.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    // Next-gen consumer (apache/kafka 4.0.0) joins the SAME group → upgrade.
    let nextgen = spawn_consumer(
        "apache/kafka:4.0.0",
        format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic mig \
             --group {group} --consumer-property group.protocol=consumer --from-beginning \
             --property print.partition=true --timeout-ms 20000 --max-messages 4"
        ),
    );

    let classic_out = classic.await.unwrap();
    let nextgen_out = nextgen.await.unwrap();

    // Both consumed; together they covered all 4 partitions with no overlap.
    let classic_parts = parse_partitions(&classic_out);
    let nextgen_parts = parse_partitions(&nextgen_out);
    assert!(!classic_parts.is_empty() && !nextgen_parts.is_empty(), "both members consumed");
    assert!(classic_parts.is_disjoint(&nextgen_parts), "no partition overlap across protocols");
    let union: std::collections::BTreeSet<i32> = classic_parts.union(&nextgen_parts).copied().collect();
    assert!(union == (0..4).collect(), "union covers all partitions: {union:?}");

    // kafka-consumer-groups --describe reports the migrating group coherently.
    let describe = docker_run("apache/kafka:4.0.0", &["bash","-c", &format!(
        "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --describe --group {group}"
    )]);
    assert!(String::from_utf8_lossy(&describe.stdout).contains("mig"));

    drop(broker);
}
```

Add `parse_partitions(stdout) -> BTreeSet<i32>` (parse `Partition:N` from the `print.partition=true` output) and `create_topic_and_produce` (reuse the existing topic/produce helpers in `jvm_acceptance.rs` / this file).

- [ ] **Step 3: Run it (Docker required)**

Run: `cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --ignored --nocapture jvm_kip848_classic_and_consumer_in_one_group_migrate`
Expected: PASS — both consumers print disjoint partitions covering 0–3; describe shows the group.

If concurrent-container orchestration is flaky on the Mac (e.g. the next-gen consumer can't form before the classic one's `--timeout-ms` elapses), widen the timeouts/`max-messages`, or split into "classic forms, then next-gen joins and both drain a fresh batch." If it remains flaky, mark the test `#[ignore]` with a comment that the in-process suite (Task 10) is the correctness gate and this is the interop proof — do **not** weaken the disjoint/union assertions to make it pass.

- [ ] **Step 4: Format, lint, commit**

```bash
cargo fmt
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_consumer_group_next_gen.rs
git commit -m "test(kip-848): JVM acceptance — classic + consumer-protocol members migrate in one group"
```

---

## Final: flip the KIP matrix

**Files:**
- Modify: `README.md` (lines ~229, ~416), `STATUS.md` if it tracks KIP-848

- [ ] **Step 1: Update the matrix**

Change the KIP-848 rows from `⚠️` to `✅` in both the "Consumer groups" table (`README.md:229`) and the full KIP table (`README.md:416`), and update the "Notable gaps" narrative (`README.md:108-113`) to state that live bidirectional classic↔next-gen migration is now wired and JVM-validated.

- [ ] **Step 2: Run the whole broker suite once more**

Run: `cargo test -p crabka-broker`
Expected: PASS (JVM `--ignored` tests excluded by default).
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add README.md STATUS.md
git commit -m "docs(kip-848): mark live consumer-group migration complete in the KIP matrix"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** Task 1 ↔ k5 schema; Task 2/8 ↔ downgrade; Task 3 ↔ routing; Task 4 ↔ replay trap; Task 5 ↔ live-kind actor loop; Task 6 ↔ upgrade trigger; Task 7 ↔ serving hosted classic members; Task 8 ↔ downgrade trigger; Task 9 ↔ describe/list coherence; Task 10 ↔ in-process validation (incl. static membership, policy=disabled, committed-offset preservation, gap/overlap); Task 11 ↔ JVM acceptance (slice F); Final ↔ README flip. Every spec section maps to a task.
- **Empirical open questions (resolve while implementing, per CLAUDE.md):** (1) exact rejection error code for a refused upgrade — Task 6 uses `GROUP_ID_NOT_FOUND` as a placeholder; confirm against `apache/kafka:4.0.0` and adjust. (2) Whether Kafka downgrades a group that never held a classic member — Task 8 guards on "no native member remains AND group non-empty", which won't downgrade an all-native group emptied to zero; confirm that matches Kafka. (3) Generation/epoch mapping for hosted classic members — Task 7 maps `generation_id = group_epoch`; verify via `--describe` across a roll.
- **Names to keep consistent:** `get_or_create_group`, `convert_consumer_to_classic`, `upgrade_pending_records`, `downgrade_pending_records`, `serve_classic_{join,sync,heartbeat}`, `maybe_downgrade`, `kind_mut`, `ClassicMemberMetadata`, `PendingRecords::{classic_group_metadata_tombstone, next_gen_group_metadata_tombstone, classic_group_metadata}`. Verify each method/field name against the file it lands in before use; a few sibling method names (`ClassicState::complete_rebalance`, `Member::with_instance_id`, classic `persistence::encode_key`) are referenced from existing code and must match exactly.
