# KIP-447 EOS Producer Scalability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the KIP-447 loop so a single transactional producer fences zombies via consumer-group state — wire `ConsumerGroupMetadata` from consumer → producer → `TxnOffsetCommit`, and make the group coordinator validate it (classic generation + next-gen member epoch) using the same path as a regular `OffsetCommit`.

**Architecture:** The broker already has all the fencing machinery (`classic_ops::validate_commit`, the next-gen `OffsetValidate` actor message), and the regular `OffsetCommit` handler already routes through it — but the producer never sends real metadata (`generation_id: -1`) so it's dead code. We (1) expose `ConsumerGroupMetadata` from the consumer, (2) make the producer forward it, (3) extract the classic/next-gen routing into one shared `validate_group_commit` helper that both `OffsetCommit` and `TxnOffsetCommit` call, and (4) prove fencing end-to-end.

**Tech Stack:** Rust 2024, tokio, `crabka-protocol` generated codecs, `assert2` in integration tests, in-process broker test harness (`Broker::start` + `crabka-client-core::Client`).

**Execution batches** (per `CLAUDE.md` — parallel where file sets are disjoint):
- **Batch A (parallel):** Task 1 (client-consumer) ∥ Task 2 (broker: `actor.rs` + `offset_commit.rs`)
- **Batch B (parallel):** Task 3 (broker: `txn_offset_commit.rs`, needs Task 2) ∥ Task 4 (client-producer, needs Task 1)
- **Batch C (sequential):** Task 5 (tests, needs 1+3+4) → Task 6 (docs)

---

## File Structure

| File | Responsibility | Task |
|------|----------------|------|
| `crates/client-consumer/src/group_metadata.rs` (new) | `ConsumerGroupMetadata` type | 1 |
| `crates/client-consumer/src/lib.rs` | declare + re-export the type | 1 |
| `crates/client-consumer/src/consumer.rs` | `Consumer::group_metadata()` accessor | 1 |
| `crates/broker/src/coordinator/unified/actor.rs` | shared `validate_group_commit` helper | 2 |
| `crates/broker/src/handlers/offset_commit.rs` | delegate `validate` to the shared helper | 2 |
| `crates/broker/src/txn/handlers/txn_offset_commit.rs` | use the shared helper (classic + next-gen) | 3 |
| `crates/client-producer/src/producer.rs` | accept + forward `ConsumerGroupMetadata` | 4 |
| `crates/client-producer/src/lib.rs` | re-export `ConsumerGroupMetadata` | 4 |
| `crates/broker/tests/transactions.rs` | update callers + fencing tests | 5 |
| `README.md`, `STATUS.md` | flip KIP-447 ✅ + slice entry | 6 |

**Note on commit commands:** git identity is unset locally — every commit uses `-c user.name=... -c user.email=...` overrides (never `git config`). Run `cargo fmt` before each commit (CI gates on `cargo fmt --check`).

---

## Task 1: Consumer — expose `ConsumerGroupMetadata`

**Files:**
- Create: `crates/client-consumer/src/group_metadata.rs`
- Modify: `crates/client-consumer/src/lib.rs:54-67`
- Modify: `crates/client-consumer/src/consumer.rs:410-414` (add accessor after `generation_id()`)

- [ ] **Step 1: Write the type + its failing unit test**

Create `crates/client-consumer/src/group_metadata.rs`:

```rust
//! KIP-447 consumer group metadata, handed to a transactional producer's
//! [`send_offsets_to_transaction`] so the group coordinator can fence zombie
//! producers via the consumer group's generation (classic) or member epoch
//! (KIP-848 next-gen), instead of requiring one producer per input partition.
//!
//! [`send_offsets_to_transaction`]: crabka_client_producer::Producer::send_offsets_to_transaction

/// The identity a consumer presents to a transactional producer for KIP-447
/// offset-commit fencing. Mirrors the JVM's
/// `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// The consumer group id.
    pub group_id: String,
    /// Classic-group generation id, or — for a KIP-848 next-gen group — the
    /// member epoch. Sent verbatim in `TxnOffsetCommitRequest.generation_id`
    /// (matches the JVM wire convention); the coordinator interprets it per
    /// group kind.
    pub generation_id: i32,
    /// The member id assigned by the coordinator at join time. Empty for a
    /// simple consumer (manual assignment, no group membership).
    pub member_id: String,
    /// `group.instance.id` for static members; `None` for dynamic members.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Metadata for a producer committing offsets to a group it is not a
    /// member of (manual partition assignment / simple consumer). The group
    /// coordinator applies no generation/member fencing to this shape.
    #[must_use]
    pub fn for_group(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_group_is_simple_consumer_shape() {
        let m = ConsumerGroupMetadata::for_group("g");
        assert!(m.group_id == "g");
        assert!(m.generation_id == -1);
        assert!(m.member_id.is_empty());
        assert!(m.group_instance_id.is_none());
    }
}
```

- [ ] **Step 2: Wire the module in `lib.rs`**

In `crates/client-consumer/src/lib.rs`, add `mod group_metadata;` in the `mod` block (alphabetical, after `mod coordinator;` / before `mod offset_wire;` is fine) and add the re-export next to the other `pub use` lines:

```rust
pub use group_metadata::ConsumerGroupMetadata;
```

- [ ] **Step 3: Run the unit test to verify it passes**

Run: `cargo test -p crabka-client-consumer group_metadata -- --nocapture`
Expected: PASS (`for_group_is_simple_consumer_shape`).

- [ ] **Step 4: Add the `Consumer::group_metadata()` accessor**

In `crates/client-consumer/src/consumer.rs`, add this import near the top (with the other `use crate::` lines):

```rust
use crate::group_metadata::ConsumerGroupMetadata;
```

Then inside `impl Consumer`, immediately after the `generation_id()` accessor (around line 414):

```rust
    /// KIP-447 group metadata to hand to a transactional producer's
    /// `send_offsets_to_transaction`. The generation id is the value captured
    /// at the most recent successful join (the field is not kept in sync as
    /// the coordinator rejoins — see [`Self::generation_id`]); for a stable
    /// single-member group this equals the coordinator's live generation.
    /// `group_instance_id` is always `None` — the consumer has no
    /// static-membership support yet.
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
        }
    }
```

- [ ] **Step 5: Verify the crate compiles + clippy clean**

Run: `cargo clippy -p crabka-client-consumer --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/client-consumer/src/group_metadata.rs crates/client-consumer/src/lib.rs crates/client-consumer/src/consumer.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(consumer): expose ConsumerGroupMetadata for KIP-447

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Broker — extract the shared `validate_group_commit` helper

The regular `OffsetCommit` handler's local `validate` (offset_commit.rs:280-321) routes classic-vs-next-gen validation. Move that logic into `actor.rs` as `validate_group_commit` so `TxnOffsetCommit` (Task 3) can reuse it — KIP-447 requires txn fencing be "consistent with normal offset fencing."

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (add free fn near the bottom of the module, after the actor loop)
- Modify: `crates/broker/src/handlers/offset_commit.rs:25` (import) and `:280-321` (delegate)

- [ ] **Step 1: Confirm the existing offset-commit suite is green (refactor baseline)**

Run: `cargo test -p crabka-broker --test offset_commit_handlers 2>/dev/null || cargo test -p crabka-broker --lib offset 2>/dev/null; cargo test -p crabka-broker --test transactions`
Expected: PASS (these exercise `validate` today; they must stay green across the refactor). If the exact test-target name differs, run `cargo test -p crabka-broker` and note the offset/txn tests pass.

- [ ] **Step 2: Add `validate_group_commit` to `actor.rs`**

In `crates/broker/src/coordinator/unified/actor.rs`, add this free function at module scope (e.g. just after the `GroupActorHandle` definition or at the end of the file). `codes` and `tokio::sync::oneshot` are already in scope in this module (the message enum uses `oneshot::Sender` and the actor uses `codes::*`).

```rust
/// Validate an offset commit — regular or transactional — against the group's
/// membership and generation (classic) or member epoch (KIP-848 next-gen),
/// routing by group kind. Returns `Some(error_code)` if the commit must be
/// rejected, `None` if it may proceed.
///
/// Shared by `OffsetCommit` and `TxnOffsetCommit` so the two paths fence
/// identically — KIP-447 requires transactional offset fencing to be
/// "consistent with normal offset fencing". For a simple consumer (empty
/// `member_id`, no `group_instance_id`) the classic path no-ops, so a producer
/// that supplies no group metadata is never fenced.
pub(crate) async fn validate_group_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
) -> Option<i16> {
    match handle.kind {
        GroupKindTag::Consumer => {
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::OffsetValidate {
                    member_id: member_id.to_string(),
                    member_epoch: generation_or_epoch,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return Some(codes::UNKNOWN_SERVER_ERROR);
            }
            match rx.await {
                Ok(Ok(())) => None,
                Ok(Err(code)) => Some(code),
                Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
            }
        }
        GroupKindTag::Classic => {
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::ClassicValidateCommit {
                    member_id: member_id.to_string(),
                    group_instance_id: group_instance_id.map(str::to_string),
                    generation_id: generation_or_epoch,
                    reply: tx,
                })
                .await
                .is_err()
            {
                return Some(codes::UNKNOWN_SERVER_ERROR);
            }
            rx.await.unwrap_or(Some(codes::UNKNOWN_SERVER_ERROR))
        }
    }
}
```

> If `codes` or `oneshot` turn out not to be imported at the top of `actor.rs`, add `use crate::codes;` and `use tokio::sync::oneshot;` respectively.

- [ ] **Step 3: Delegate `offset_commit::validate` to the helper**

In `crates/broker/src/handlers/offset_commit.rs`, replace the body of `validate` (lines ~280-321) with a one-line delegation, and **drop `GroupKindTag` from the import on line 25** (it becomes unused here; `GroupActorHandle`, `GroupActorMessage`, and `oneshot` are still used by `UpdateCommitted` at lines ~400-403):

Change line 25 from:
```rust
use crate::coordinator::unified::actor::{GroupActorHandle, GroupActorMessage, GroupKindTag};
```
to:
```rust
use crate::coordinator::unified::actor::{GroupActorHandle, GroupActorMessage, validate_group_commit};
```

Replace the whole `validate` fn with:
```rust
/// Validate the commit against the group's membership/epoch through its actor.
/// Returns `Some(error_code)` if the request should be rejected. Thin wrapper
/// over the shared [`validate_group_commit`] (also used by `TxnOffsetCommit`).
async fn validate(handle: &Arc<GroupActorHandle>, req: &OffsetCommitRequest) -> Option<i16> {
    validate_group_commit(
        handle,
        &req.member_id,
        req.generation_id_or_member_epoch,
        req.group_instance_id.as_deref(),
    )
    .await
}
```

(`&Arc<GroupActorHandle>` deref-coerces to the helper's `&GroupActorHandle` param — no change needed at the call site on line 116.)

- [ ] **Step 4: Run the baseline suites again to verify the refactor is behavior-preserving**

Run: `cargo test -p crabka-broker --test transactions && cargo test -p crabka-broker --lib`
Expected: PASS (same set as Step 1).

- [ ] **Step 5: Clippy clean (catches the now-unused `GroupKindTag`)**

Run: `cargo clippy -p crabka-broker --all-targets -- -D warnings`
Expected: no warnings (in particular, no unused-import warning).

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/broker/src/coordinator/unified/actor.rs crates/broker/src/handlers/offset_commit.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "refactor(broker): extract shared validate_group_commit for offset fencing

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Broker — `TxnOffsetCommit` uses the shared helper (classic + next-gen)

Replace the bespoke `ClassicInspect`-based block in `txn_offset_commit.rs` (lines 115-154) with a call to `validate_group_commit`. This (a) fixes the missing unknown-member case, (b) adds next-gen `STALE_MEMBER_EPOCH`/`FENCED_MEMBER_EPOCH` (clears the `TODO(KIP-1319 v4+)`), and (c) drops the `generation_id >= 0` gate (the helper no-ops for the simple-consumer shape).

**Depends on:** Task 2.

**Files:**
- Modify: `crates/broker/src/txn/handlers/txn_offset_commit.rs:16` (import) and `:115-154` (validation block) and the module doc (lines 8-10).

- [ ] **Step 1: Swap the import**

In `crates/broker/src/txn/handlers/txn_offset_commit.rs`, change line 16 from:
```rust
use crate::coordinator::unified::actor::{GroupActorMessage, GroupKindTag};
```
to:
```rust
use crate::coordinator::unified::actor::validate_group_commit;
```

- [ ] **Step 2: Replace the validation block**

Replace the entire block at lines 115-154 (the comment starting `// 2. KIP-1319 stale-member-epoch check ...` through the closing brace of the `if version >= 3 ...` chain) with:

```rust
    // 2. KIP-447 / KIP-1319 fencing — identical to a regular OffsetCommit
    //    (KIP-447: "consistent with normal offset fencing"). For a classic
    //    group this checks member id + group.instance.id + generation
    //    (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / FENCED_INSTANCE_ID); for a
    //    KIP-848 next-gen group the `generation_id` field carries the member
    //    epoch and we return STALE_MEMBER_EPOCH / FENCED_MEMBER_EPOCH /
    //    UNKNOWN_MEMBER_ID. A producer that supplies no metadata (empty
    //    member_id, generation_id = -1) is a simple consumer and is not fenced.
    //    The fields only exist on v3+, so older requests carry the
    //    simple-consumer defaults and no-op.
    if version >= 3
        && let Some(h) = &handle
        && let Some(code) = validate_group_commit(
            h,
            &req.member_id,
            req.generation_id,
            req.group_instance_id.as_deref(),
        )
        .await
    {
        return encode_err_all(version, &req, code);
    }
```

- [ ] **Step 3: Update the module doc comment**

In the same file, update the doc lines 8-10 to reflect that fencing is delegated. Replace:
```rust
//! Versions 0–2: non-flexible (no `generation_id`/`member_id` fields).
//! Versions 3–5: flexible (tagged fields; adds `generation_id`, `member_id`,
//!               `group_instance_id`).
```
with:
```rust
//! Versions 0–2: non-flexible (no `generation_id`/`member_id` fields).
//! Versions 3–5: flexible (tagged fields; adds `generation_id`, `member_id`,
//!               `group_instance_id`). On v3+ the consumer-group metadata is
//!               validated via the shared `validate_group_commit` (KIP-447:
//!               fencing "consistent with normal offset fencing") — classic
//!               generation or KIP-848 next-gen member epoch.
```

- [ ] **Step 4: Verify it compiles and clippy is clean**

Run: `cargo clippy -p crabka-broker --all-targets -- -D warnings`
Expected: no warnings (confirms `GroupActorMessage`/`GroupKindTag` are no longer referenced here).

- [ ] **Step 5: Run the existing transaction suite (no regressions)**

Run: `cargo test -p crabka-broker --test transactions`
Expected: PASS. (The existing `send_offsets_to_transaction_atomic_with_records` still passes default metadata — empty member_id, generation -1 — which the helper treats as a simple consumer; this gets upgraded to real metadata in Task 5.)

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/broker/src/txn/handlers/txn_offset_commit.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): fence TxnOffsetCommit via shared group validation (KIP-447 + next-gen epoch)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Producer — accept and forward `ConsumerGroupMetadata`

**Depends on:** Task 1.

**Files:**
- Modify: `crates/client-producer/src/producer.rs:14-29` (import), `:379-465` (`send_offsets_to_transaction`)
- Modify: `crates/client-producer/src/lib.rs:59-62` (re-export)

- [ ] **Step 1: Re-export the type from the producer crate**

In `crates/client-producer/src/lib.rs`, add next to the other `pub use` lines:
```rust
pub use crabka_client_consumer::ConsumerGroupMetadata;
```

- [ ] **Step 2: Import it in `producer.rs`**

In `crates/client-producer/src/producer.rs`, add near the other crate imports (after line 14 `use crabka_client_core::Client;`):
```rust
use crabka_client_consumer::ConsumerGroupMetadata;
```

- [ ] **Step 3: Change the signature and forward the metadata**

Replace `send_offsets_to_transaction` (lines 398-465) so it takes `&ConsumerGroupMetadata` instead of `group_id: &str`, populating the three KIP-447 fields. Replace the signature + body:

```rust
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: impl IntoIterator<Item = ((String, i32), i64)>,
        group_meta: &ConsumerGroupMetadata,
    ) -> Result<(), ProducerError> {
        let tid = self
            .transactional_id
            .as_deref()
            .ok_or(ProducerError::NotTransactional)?
            .to_string();
        let offsets_vec: Vec<_> = offsets.into_iter().collect();

        let (pid, epoch) = *self.txn_pid_epoch.lock().await;

        // 1. AddOffsetsToTxn → transaction coordinator.
        let coord_guard = self.txn_coord_client.lock().await;
        let coord = coord_guard
            .as_ref()
            .ok_or(ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ))?
            .clone();
        drop(coord_guard);

        let r1 = coord
            .send(AddOffsetsToTxnRequest {
                transactional_id: tid.clone(),
                producer_id: pid,
                producer_epoch: epoch,
                group_id: group_meta.group_id.clone(),
                ..Default::default()
            })
            .await?;
        if r1.error_code != 0 {
            return Err(ProducerError::Server(r1.error_code));
        }

        // 2. FindCoordinator(group_id, key_type=0 GROUP) for the group coordinator.
        let group_addr = self.find_group_coordinator(&group_meta.group_id).await?;
        let group_client = Client::builder()
            .bootstrap(group_addr)
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .build()
            .await?;

        // 3. TxnOffsetCommit → group coordinator, carrying the consumer group
        //    metadata (generation id / member id / instance id) so the
        //    coordinator can fence zombie producers via the group's own state
        //    rather than requiring one producer per input partition (KIP-447).
        let r2 = group_client
            .send(TxnOffsetCommitRequest {
                transactional_id: tid,
                producer_id: pid,
                producer_epoch: epoch,
                group_id: group_meta.group_id.clone(),
                generation_id: group_meta.generation_id,
                member_id: group_meta.member_id.clone(),
                group_instance_id: group_meta.group_instance_id.clone(),
                topics: build_topics_payload(&offsets_vec),
                ..Default::default()
            })
            .await?;

        // Check per-partition error codes.
        for topic in &r2.topics {
            for p in &topic.partitions {
                if p.error_code != 0 {
                    return Err(ProducerError::Server(p.error_code));
                }
            }
        }
        Ok(())
    }
```

- [ ] **Step 4: Update the doc comment**

Update the doc block above the function (lines 379-397): change the `group_id` parameter reference to `group_meta`. Replace the line that documents the group argument and the intro sentence so it reads:

```rust
    /// Enroll a consumer group's offsets in the current transaction, fencing
    /// zombie producers via the supplied [`ConsumerGroupMetadata`] (KIP-447).
    ///
    /// Performs two broker round-trips:
    ///
    /// 1. `AddOffsetsToTxn` → transaction coordinator (registers the group
    ///    offset commit as part of the ongoing transaction).
    /// 2. `TxnOffsetCommit` → group coordinator (commits the actual offsets
    ///    transactionally, carrying `group_meta`'s generation/member/instance
    ///    so the coordinator can fence stale producers).
```

(Leave the `# Errors` section as-is — the error variants are unchanged.)

- [ ] **Step 5: Verify the crate compiles + clippy clean**

Run: `cargo clippy -p crabka-client-producer --all-targets -- -D warnings`
Expected: no warnings. (Compilation will flag any remaining `&str` call sites — there are none in this crate; the test callers are fixed in Task 5.)

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add crates/client-producer/src/producer.rs crates/client-producer/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(producer): forward ConsumerGroupMetadata in send_offsets_to_transaction (KIP-447)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Tests — update callers + prove fencing end-to-end

**Depends on:** Tasks 1, 3, 4.

**Files:**
- Modify: `crates/broker/tests/transactions.rs` (imports at top; caller at line 446; caller at line 518; two new tests appended)

- [ ] **Step 1: Add imports for the new tests**

In `crates/broker/tests/transactions.rs`, add to the import block (after line 23 `use crabka_client_producer::{Producer, ProducerRecord};`):
```rust
use crabka_client_producer::ConsumerGroupMetadata;
```

- [ ] **Step 2: Update the happy-path caller to thread real metadata**

At line ~446, change:
```rust
            producer
                .send_offsets_to_transaction([offset_entry], "cpp-g")
                .await
                .unwrap();
```
to:
```rust
            producer
                .send_offsets_to_transaction([offset_entry], &input_consumer.group_metadata())
                .await
                .unwrap();
```

- [ ] **Step 3: Update the SASL caller (group has no live member → simple-consumer shape)**

At line ~518, change:
```rust
        .send_offsets_to_transaction([(("sasl-txn".to_string(), 0), 3i64)], "sasl-cpp-g")
```
to:
```rust
        .send_offsets_to_transaction(
            [(("sasl-txn".to_string(), 0), 3i64)],
            &ConsumerGroupMetadata::for_group("sasl-cpp-g"),
        )
```

- [ ] **Step 4: Run the updated suite to verify no regressions**

Run: `cargo test -p crabka-broker --test transactions`
Expected: PASS — including `send_offsets_to_transaction_atomic_with_records` (now sending real metadata, validated against the live classic group) and `sasl_authenticated_transactional_flow_commits`.

- [ ] **Step 5: Add the classic fencing test (write it; expect it to drive the behavior added in Task 3)**

Append to `crates/broker/tests/transactions.rs`:

```rust
// ── KIP-447 zombie fencing ──────────────────────────────────────────────────────

/// A classic-group `TxnOffsetCommit` is fenced when it carries a stale
/// generation (ILLEGAL_GENERATION) or an unknown member (UNKNOWN_MEMBER_ID),
/// and accepted when the metadata matches the live group. Driven with raw
/// `TxnOffsetCommitRequest`s so we control the metadata precisely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_classic_generation_and_member() {
    use crabka_protocol::owned::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "fence-in").await;

    // A real classic consumer joins, establishing the group's member id +
    // generation.
    let consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("fence-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["fence-in".to_string()])
        .build()
        .await
        .unwrap();
    let meta = consumer.group_metadata();
    // A non-empty member id proves the join completed; the fencing assertions
    // below hold for whatever generation the group settled on (we send
    // `generation_id + 1` for the stale case, which always mismatches).
    assert!(!meta.member_id.is_empty(), "consumer should have a member id: {meta:?}");

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    let mk = |generation_id: i32, member_id: &str| TxnOffsetCommitRequest {
        transactional_id: "fence-tid".into(),
        group_id: "fence-g".into(),
        producer_id: 0,
        producer_epoch: 0,
        generation_id,
        member_id: member_id.into(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "fence-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale generation → ILLEGAL_GENERATION (22).
    let stale = client.send(mk(meta.generation_id + 1, &meta.member_id)).await.unwrap();
    assert!(
        stale.topics[0].partitions[0].error_code == 22,
        "stale generation should be ILLEGAL_GENERATION: {stale:?}"
    );

    // Correct generation but unknown member → UNKNOWN_MEMBER_ID (25).
    let unknown = client.send(mk(meta.generation_id, "ghost-member")).await.unwrap();
    assert!(
        unknown.topics[0].partitions[0].error_code == 25,
        "unknown member should be UNKNOWN_MEMBER_ID: {unknown:?}"
    );

    // Matching metadata → accepted (NONE = 0).
    let ok = client.send(mk(meta.generation_id, &meta.member_id)).await.unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "valid metadata should commit: {ok:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
```

- [ ] **Step 6: Add the next-gen member-epoch fencing test**

Append to `crates/broker/tests/transactions.rs`:

```rust
/// A KIP-848 next-gen ("consumer"-protocol) `TxnOffsetCommit` is fenced when it
/// carries a stale member epoch (STALE_MEMBER_EPOCH) and accepted at the
/// current epoch. The member epoch travels in the `generation_id` field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_next_gen_member_epoch() {
    use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
    use crabka_protocol::owned::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ng-in").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    // Establish a next-gen group member; after the first heartbeat the member
    // is at epoch 1.
    let mut hb = ConsumerGroupHeartbeatRequest {
        group_id: "ng-g".into(),
        member_id: String::new(),
        member_epoch: 0,
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    hb.subscribed_topic_names = Some(vec!["ng-in".into()]);
    let hb_resp = client.send(hb).await.unwrap();
    assert!(hb_resp.error_code == 0, "heartbeat failed: {hb_resp:?}");
    let member_id = hb_resp.member_id.clone().unwrap();
    let epoch = hb_resp.member_epoch;
    assert!(epoch >= 1, "member should have a positive epoch: {hb_resp:?}");

    let mk = |gen: i32| TxnOffsetCommitRequest {
        transactional_id: "ng-tid".into(),
        group_id: "ng-g".into(),
        producer_id: 0,
        producer_epoch: 0,
        generation_id: gen, // carries the member epoch for next-gen groups
        member_id: member_id.clone(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "ng-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale epoch (< current) → STALE_MEMBER_EPOCH (113).
    let stale = client.send(mk(epoch - 1)).await.unwrap();
    assert!(
        stale.topics[0].partitions[0].error_code == 113,
        "stale epoch should be STALE_MEMBER_EPOCH: {stale:?}"
    );

    // Current epoch + known member → accepted (NONE = 0).
    let ok = client.send(mk(epoch)).await.unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "current epoch should commit: {ok:?}"
    );

    broker.shutdown().await;
}
```

- [ ] **Step 7: Run the two new tests**

Run: `cargo test -p crabka-broker --test transactions txn_offset_commit_fences`
Expected: PASS — `txn_offset_commit_fences_classic_generation_and_member` and `txn_offset_commit_fences_next_gen_member_epoch`.

- [ ] **Step 8: Full transactions suite + clippy on tests**

Run: `cargo test -p crabka-broker --test transactions && cargo clippy -p crabka-broker --all-targets -- -D warnings`
Expected: all PASS, no clippy warnings.

- [ ] **Step 9: Commit**

```bash
cargo fmt
git add crates/broker/tests/transactions.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-447 end-to-end metadata flow + zombie fencing

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Docs — flip the KIP-447 row + add a STATUS.md slice

**Files:**
- Modify: `README.md:400`
- Modify: `STATUS.md` (append a slice section at the end of the file)

- [ ] **Step 1: Flip the README KIP-447 row**

In `README.md`, change line 400 from `| ... | ⚠️ |` to `| ... | ✅ |`:
```markdown
| [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447) | Producer scalability for exactly-once semantics | ✅ |
```

- [ ] **Step 2: Append the STATUS.md slice**

Append to the end of `STATUS.md`:

```markdown
## Slice — KIP-447 producer scalability for EOS (2026-06-03)

- **Goal.** Promote the KIP-447 row in the README KIP table from ⚠️ to ✅.
  KIP-447 lets a single transactional producer fence zombies across all of a
  consumer group's input partitions by validating the consumer group's own
  generation / member epoch at `TxnOffsetCommit`, instead of requiring one
  producer per input partition.
- **What was missing.** The broker already had the fencing machinery
  (`classic_ops::validate_commit`, the next-gen `OffsetValidate` actor
  message), but the producer's `send_offsets_to_transaction(offsets, group_id)`
  sent `TxnOffsetCommitRequest` with `generation_id = -1` / empty `member_id` —
  so the fencing was dead code. The consumer also exposed no single
  `group_metadata()` accessor.
- **Client wiring.**
  - `crabka-client-consumer` gained a public `ConsumerGroupMetadata`
    `{ group_id, generation_id, member_id, group_instance_id }` (mirrors the
    JVM type) plus `Consumer::group_metadata()`. `group_instance_id` is always
    `None` — the consumer has no static-membership support yet.
  - `Producer::send_offsets_to_transaction` now takes
    `&ConsumerGroupMetadata` (breaking change; greenfield) and forwards the
    generation / member / instance into `TxnOffsetCommitRequest`. The type is
    re-exported from the producer crate.
- **Broker.** Extracted the classic-vs-next-gen routing from
  `offset_commit::validate` into a shared
  `coordinator::unified::actor::validate_group_commit`, and made
  `txn_offset_commit` call it — so transactional offset fencing is "consistent
  with normal offset fencing" (KIP-447's words). This:
  - fixes the previously-missing unknown-member case
    (`UNKNOWN_MEMBER_ID` when a bare `member_id` isn't in the group),
  - adds KIP-848 next-gen member-epoch fencing (`STALE_MEMBER_EPOCH` /
    `FENCED_MEMBER_EPOCH`), clearing the `TODO(KIP-1319 v4+)`,
  - drops the old `generation_id >= 0` gate (the shared helper no-ops for the
    simple-consumer shape, so a metadata-less producer is unaffected).
- **Tests.** `crates/broker/tests/transactions.rs`:
  - `send_offsets_to_transaction_atomic_with_records` now threads real
    `consumer.group_metadata()`.
  - `txn_offset_commit_fences_classic_generation_and_member` — stale
    generation → `ILLEGAL_GENERATION`, unknown member → `UNKNOWN_MEMBER_ID`,
    matching metadata → accepted.
  - `txn_offset_commit_fences_next_gen_member_epoch` — stale member epoch →
    `STALE_MEMBER_EPOCH`, current epoch → accepted.
- **Out of scope.** Consumer-side static membership (`group.instance.id`)
  configuration — the broker still validates the instance-id case for protocol
  completeness, but the consumer client always reports `None`.
```

- [ ] **Step 3: Commit**

```bash
git add README.md STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs: mark KIP-447 complete + STATUS slice

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] **Workspace fmt check:** `cargo fmt --check` → clean.
- [ ] **Workspace clippy:** `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- [ ] **Affected crates build + test:** `cargo test -p crabka-client-consumer -p crabka-client-producer -p crabka-broker` → all PASS (in particular the `transactions` suite, including the two new fencing tests and the updated happy-path/SASL tests).
- [ ] Confirm no other workspace caller of `send_offsets_to_transaction` exists outside `transactions.rs` (grep once more; bench/cli/examples currently have none).
