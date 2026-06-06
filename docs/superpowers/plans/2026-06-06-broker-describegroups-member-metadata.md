# Broker DescribeGroups: member_metadata + protocol_data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the Crabka broker's `DescribeGroups` (API key 15) response populate each member's `member_metadata` (the JoinGroup protocol-metadata bytes) and the group's `protocol_data` (the selected protocol name), matching Apache Kafka wire byte-exactness — instead of returning them empty.

**Architecture:** Pure threading fix. The stored classic group state already retains `Member.protocol_metadata: Bytes` and `Group.protocol_name: Option<String>`, and `ClassicMemberView` already carries `protocol_metadata` — but the projection to the wire-facing `MemberSnapshot`/`GroupSnapshot` drops both, and the handler hardcodes empties. Add two fields to the snapshot structs, map them in the two snapshot builders, and wire the handler.

**Tech Stack:** Rust 2024; `crabka-broker` group coordinator (`coordinator/unified/`); `crabka_protocol::owned::describe_groups_*`. Validation: an in-process broker integration test (JoinGroup→SyncGroup→DescribeGroups, no Docker) + a `#[ignore]` cp/JVM capture.

---

## Verified current code (this worktree, off main)

- `crates/broker/src/coordinator/mod.rs:27-43` — `GroupSnapshot { group_id, state, protocol_type: Option<String>, generation_id, members: Vec<MemberSnapshot> }`; `MemberSnapshot { member_id, client_id, client_host, assignment: Vec<u8> }`.
- `crates/broker/src/coordinator/unified/actor.rs`:
  - `ClassicView { group_id, state, protocol_type, generation_id, members: Vec<ClassicMemberView> }` (162-168) — **no `protocol_name`**.
  - `ClassicMemberView { member_id, client_id, host, group_instance_id, protocol_metadata: Bytes, assignment: Option<Bytes> }` (170-178) — **already has `protocol_metadata`**.
  - `ClassicView::snapshot()` (180-205) — drops `protocol_metadata`; no `protocol_name`.
  - `build_consumer_snapshot()` (217-243) — next-gen path; `protocol_type: Some("consumer")`; drops member metadata.
  - `build_classic_view(state)` (~727-746) — builds `ClassicView` from `ClassicState`; does NOT set `protocol_name`. `ClassicState`/`Group` has `protocol_name: Option<String>`.
  - `InspectAny` handler (~564-572) — classic → `build_classic_view(state).snapshot()`; consumer → `build_consumer_snapshot(...)`.
- `crates/broker/src/handlers/describe_groups.rs:59-105` — builds `DescribedGroupMember { member_id, client_id, client_host, member_assignment, ..Default::default() }` (drops `member_metadata`); `protocol_type: snap.protocol_type.unwrap_or_else(|| "consumer".into())`; `protocol_data: String::new()`.
- Wire types `crates/protocol/generated/DescribeGroupsResponse.owned.rs`: `DescribedGroupMember.member_metadata: ::bytes::Bytes`; `DescribedGroup.protocol_data: String`, `.protocol_type: String`.

## Branch / commit / gate discipline
- Worktree `/Users/mattstone/git/crabka/.claude/worktrees/broker-describe-groups`, branch `claude/broker-describe-groups` (off main; assert NOT main). Always `git -C <worktree>`. Do NOT push (controller handles push/PR).
- Commits: `git -C <worktree> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; body ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Per change before commit: `cargo clippy --workspace --all-targets -- -D warnings` + `cargo fmt`. (Workspace clippy, not `-p` — pedantic=warn is workspace-wide and the per-target cache can mask lints; force-relint touched files.)
- Greenfield: clean field additions (no compat shims). cp/JVM is authority for the wire bytes.

---

## Task 1: thread protocol_metadata + protocol_name → snapshots → handler + in-process test

**Files:** `coordinator/mod.rs`, `coordinator/unified/actor.rs`, `handlers/describe_groups.rs`; Test: a new `crates/broker/tests/describe_groups_metadata.rs` (or extend an existing classic-group integration test).

- [ ] **Step 1: Write the failing integration test.** Model on an existing broker test that drives classic JoinGroup→SyncGroup (look at `crates/broker/tests/authorized_operations.rs` for the DescribeGroups send + the broker-boot/client helpers; find a classic JoinGroup/SyncGroup driver — e.g. a consumer-group coordinator test — to model the join/sync). Boot a broker, then for a group `g`:
  1. `JoinGroupRequest { group_id: g, protocol_type: "consumer", protocols: [{ name: "range", metadata: <KNOWN_BYTES> }], member_id: "", session_timeout_ms, .. }` → on `MEMBER_ID_REQUIRED` (79) re-send with the returned `member_id`. Capture `generation_id`, `member_id`, confirm leader.
  2. `SyncGroupRequest { group_id: g, generation_id, member_id, protocol_type: Some("consumer"), protocol_name: Some("range"), assignments: [{ member_id, assignment: <ASSIGN_BYTES> }], .. }`.
  3. `DescribeGroupsRequest { groups: [g], .. }`.
  - Assert on the single described group: `group.protocol_type == "consumer"`, `group.protocol_data == "range"`, `group.members[0].member_metadata == KNOWN_BYTES` (byte-exact), `group.members[0].member_assignment == ASSIGN_BYTES`.
  Use a fixed, recognizable `KNOWN_BYTES` (e.g. `b"\x00\x01rangemeta"`) so the echo is unambiguous.

- [ ] **Step 2: Run — expect FAIL** (`member_metadata` empty, `protocol_data` empty): `cargo test -p crabka-broker --test describe_groups_metadata -- --nocapture`.

- [ ] **Step 3: Add the snapshot fields (`coordinator/mod.rs`).**
```rust
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: crate::coordinator::unified::classic_state::GroupState,
    pub protocol_type: Option<String>,
    /// The group's selected protocol NAME (e.g. "range"/"v0"); None for an
    /// empty/dead group. Maps to DescribeGroups `protocol_data`.
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<MemberSnapshot>,
}

pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    /// The member's JoinGroup protocol metadata bytes (DescribeGroups
    /// `member_metadata`). Empty for a member that hasn't joined / next-gen.
    pub protocol_metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}
```

- [ ] **Step 4: Thread through the classic path (`actor.rs`).**
  - Add `pub protocol_name: Option<String>,` to `ClassicView`.
  - In `build_classic_view(state)` (~727), set `protocol_name: state.protocol_name.clone()` (confirm the field name on `ClassicState`; the stored classic group has `protocol_name: Option<String>`).
  - In `ClassicView::snapshot()`:
```rust
pub fn snapshot(&self) -> GroupSnapshot {
    GroupSnapshot {
        group_id: self.group_id.clone(),
        state: self.state,
        protocol_type: self.protocol_type.clone(),
        protocol_name: self.protocol_name.clone(),
        generation_id: self.generation_id,
        members: self.members.iter().map(|m| MemberSnapshot {
            member_id: m.member_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            protocol_metadata: m.protocol_metadata.to_vec(),
            assignment: m.assignment.as_ref().map(|b| b.to_vec()).unwrap_or_default(),
        }).collect(),
    }
}
```
  - In `build_consumer_snapshot()` (next-gen), set `protocol_name: None` and `protocol_metadata: Vec::new()` for each member (next-gen members carry no classic JoinGroup metadata; DescribeGroups is the classic API — Task 2's cp capture confirms whether a next-gen member shows any `member_metadata`/`protocol_data` via classic DescribeGroups, adjust if cp differs).
  - Update EVERY other `GroupSnapshot`/`MemberSnapshot` construction site so the crate compiles (grep `MemberSnapshot {` and `GroupSnapshot {` across the broker crate + tests; ListGroups also consumes `GroupSnapshot` — it just ignores the new fields).

- [ ] **Step 5: Wire the handler (`handlers/describe_groups.rs`).**
  - In the `DescribedGroupMember` map: add `member_metadata: m.protocol_metadata.into(),` (Vec<u8> → Bytes via `bytes::Bytes::from`).
  - Replace `protocol_data: String::new(),` with `protocol_data: snap.protocol_name.clone().unwrap_or_default(),`.
  - Replace `protocol_type: snap.protocol_type.unwrap_or_else(|| "consumer".into()),` with `protocol_type: snap.protocol_type.clone().unwrap_or_default(),` (Kafka returns "" for a typeless/dead group, not "consumer"; the consumer path already sets `Some("consumer")`). **If an existing test asserts "consumer" for a typeless group, that test encodes the old bug — update it, and confirm against cp in Task 2.**

- [ ] **Step 6: Run — expect PASS:** `cargo test -p crabka-broker --test describe_groups_metadata -- --nocapture`.

- [ ] **Step 7: Full gate.** `cargo test -p crabka-broker` (the existing DescribeGroups/ListGroups tests — `authorized_operations.rs` etc. — stay green; fix any that encoded the empty-metadata/"consumer"-default behavior). `cargo clippy --workspace --all-targets -- -D warnings` + `cargo fmt`.

- [ ] **Step 8: Commit** (`coordinator/mod.rs`, `coordinator/unified/actor.rs`, `handlers/describe_groups.rs`, the new test): `broker: populate DescribeGroups member_metadata + protocol_data (was empty)`.

---

## Task 2: cp/JVM DescribeGroups capture + calibration

**Files:** Create `crates/broker/tests/describe_groups_jvm.rs` (`#[ignore]` Docker) or extend an existing JVM-differential harness; calibrate the handler/snapshot if cp diverges.

- [ ] **Step 1: Capture real Kafka DescribeGroups.** Model on an existing `#[ignore]` JVM/Docker broker test (e.g. `crates/broker/tests/jvm_consumer_group_next_gen.rs` or the broker-jvm-acceptance harness). Against a real Apache Kafka / cp-kafka broker (or pointing a JVM client at the Crabka broker), form (a) a stable CLASSIC consumer group (real `kafka-console-consumer` with a partition assignor like `range`) and (b) — optional — a Schema-Registry `"sr"` group. Issue a `DescribeGroupsRequest` and record, per member: `member_metadata` bytes + `member_assignment` bytes; per group: `protocol_type`, `protocol_data`, `group_state`. Write to `crates/broker/tests/fixtures/describe_groups/*.json` (UTF-8-lossy / hex of byte fields).

- [ ] **Step 2: Run the capture (Docker).** `cargo test -p crabka-broker --test describe_groups_jvm -- --ignored --nocapture`. **If Docker is unavailable, STOP and report — leave Task 1's in-process byte-pin as the validation, note cp capture deferred to the controller.** Report: real Kafka's `protocol_data` for a `range` consumer group (expect `"range"`); the `protocol_type` (expect `"consumer"`); and CRITICALLY the `protocol_type`/`member_metadata` for an EMPTY/typeless group (settles the `unwrap_or_default` vs `"consumer"` decision) and for a next-gen member via classic DescribeGroups.

- [ ] **Step 3: Calibrate.** If cp diverges from Task 1's choices (the `protocol_type` empty-default, the next-gen `member_metadata`), adjust `handlers/describe_groups.rs` / `build_consumer_snapshot` to match cp byte-for-byte and update the Task-1 test's expected values. Report every seed→cp change.

- [ ] **Step 4: Cross-validate against Crabka.** Point the same capture at the Crabka broker (in-process or container) and assert it reproduces real Kafka's `member_metadata` + `protocol_data` + `protocol_type` byte-for-byte for the classic consumer group.

- [ ] **Step 5: Full gate + commit.** `cargo test -p crabka-broker` (+ `--ignored` if Docker), clippy + fmt. Commit: `broker: cp-validated DescribeGroups metadata/protocol-name capture`.

---

## Self-review
- **Spec coverage:** member_metadata (Task 1 Step 5) + protocol_data (Step 5) + protocol_type default (Step 5, cp-confirmed Task 2) + the coordinator snapshot threading (Steps 3-4) + cp/JVM validation (Task 2) + in-process regression test (Task 1) — all covered.
- **Scope:** classic path is fully fixed (both validation targets — classic consumer + `"sr"` — are classic groups); next-gen path threads `protocol_name`/empty metadata, with cp confirming next-gen `member_metadata` semantics. KIP-848 `ConsumerGroupDescribe` (key 69) is out of scope (different API).
- **Type consistency:** `GroupSnapshot.protocol_name: Option<String>`, `MemberSnapshot.protocol_metadata: Vec<u8>`, `ClassicView.protocol_name: Option<String>`, handler `m.protocol_metadata.into()` (Vec<u8>→Bytes), `snap.protocol_name.unwrap_or_default()` — consistent across tasks.
