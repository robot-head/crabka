# KIP-516 Topic Identifiers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete KIP-516 in Crabka: an O(1) topic-id index, strict KIP-516 error semantics across the Fetch/Produce/Metadata/DeleteTopics handlers, and the OffsetCommit v10+ / OffsetFetch v8+ wire shapes.

**Architecture:** Topic UUIDs are already generated, persisted, and carried on the wire. This plan adds (1) a `topic_id → topic` index on `MetadataImage`, (2) the KIP-516 error codes, (3) a single shared strict-resolution helper, then routes each handler's topic resolution through it. Internal offset storage stays name-keyed; topic-id is a pure wire-boundary translation.

**Tech Stack:** Rust 2024, `uuid` crate (metadata layer), `crabka_protocol::primitives::uuid::Uuid` (`[u8;16]` wire type, aliased `WireUuid`), `tokio`, `assert2` for test assertions.

**Spec:** `docs/superpowers/specs/2026-05-30-crabka-kip-516-topic-ids-design.md`

---

## Conventions for every task

- **Branch:** all work stays on the current branch (`claude/loving-mirzakhani-fed4b3`). Subagents run in the main repo working tree; when a subagent does git work it MUST use `git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3` and assert the branch is `claude/loving-mirzakhani-fed4b3` before committing (do not let commits land on `main`).
- **Git identity is unset locally.** Commit with explicit overrides — never run `git config`:
  ```bash
  git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "..."
  ```
  End every commit message with:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  ```
- **Before every commit:** `cargo fmt` (CI gates on `cargo fmt --check`; clippy passing is not enough).
- **Test assertions use `assert2`:** `use assert2::assert;` then `assert!(x == y)` (not `assert_eq!`).
- **Wire ↔ metadata UUID conversion:** metadata layer keys on `uuid::Uuid`; wire layer is `WireUuid([u8;16])`. Convert with `uuid::Uuid::from_bytes(wire.0)` and `record.topic_id.into_bytes()` → `WireUuid(bytes)`.

---

## File Structure

| File | Responsibility | Task |
|------|----------------|------|
| `crates/metadata/src/image.rs` | Add `topic_ids` index + `topic_by_id`/`topic_name_by_id`; maintain in `apply()` | 1 |
| `crates/broker/src/codes.rs` | KIP-516 error code constants | 2 |
| `crates/broker/src/topic_resolve.rs` (new) | Shared strict (name, id) → `&TopicRecord` resolver | 3 |
| `crates/broker/src/lib.rs` | Register `topic_resolve` module | 3 |
| `crates/broker/src/handlers/fetch.rs` | Route v13 id resolution through helper; unknown id ⇒ `UNKNOWN_TOPIC_ID` | 4 |
| `crates/broker/src/handlers/produce.rs` | Same for Produce v13 | 5 |
| `crates/broker/src/handlers/metadata.rs` | Unknown/inconsistent requested id ⇒ topic-level error | 6 |
| `crates/broker/src/handlers/delete_topics.rs` | Delete-by-id unknown ⇒ `UNKNOWN_TOPIC_ID` | 7 |
| `crates/broker/src/handlers/offset_commit.rs`, `offset_fetch.rs`, `api_catalog.rs` | Unlock v10+/v8+, id↔name wire translation | 8 |
| `crates/broker/tests/topic_ids.rs` (new) | Integration coverage | 4–8 |

---

## Batch plan (per CLAUDE.md: parallel batches, disjoint file sets)

- **Batch 1 (parallel):** Task 1 (`image.rs`) ‖ Task 2 (`codes.rs`).
- **Between batches (orchestrator, sequential):** Task 3 (`topic_resolve.rs` + `lib.rs`) — depends on both Batch 1 outputs.
- **Batch 2 (parallel, disjoint files):** Task 4 (`fetch.rs`) ‖ Task 5 (`produce.rs`) ‖ Task 6 (`metadata.rs`) ‖ Task 7 (`delete_topics.rs`) ‖ Task 8 (`offset_commit.rs` + `offset_fetch.rs` + `api_catalog.rs`).
- Integration tests (Tasks 4–8) all live in the new `crates/broker/tests/topic_ids.rs`. To keep Batch 2 file-disjoint, **each Batch-2 task appends its own `#[tokio::test]` fns to that file under a clearly-labelled section**; the orchestrator creates the file with a skeleton in Task 3 so there are no creation races. If a subagent reports a merge conflict on this file, the orchestrator serializes those appends during review.

---

## Task 1: Topic-ID reverse index on MetadataImage

**Files:**
- Modify: `crates/metadata/src/image.rs` (struct at :56, `new()` at :88, `apply()` arms at :369 and :379, tests module at :685)

- [ ] **Step 1: Write the failing test**

Append to the `mod tests` block in `crates/metadata/src/image.rs` (after the existing tests, before the closing `}`):

```rust
    #[test]
    fn topic_by_id_resolves_and_drops_on_delete() {
        use crate::records::{MetadataRecord, TopicRecord};
        let mut img = MetadataImage::new(Uuid::nil());
        let id = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: id,
            partitions: 1,
            replication_factor: 1,
        }));

        // Resolves by id to the same record `topic(name)` returns.
        assert!(img.topic_by_id(&id).map(|t| t.name.as_str()) == Some("orders"));
        assert!(img.topic_name_by_id(&id) == Some("orders"));

        // After delete the id no longer resolves and the name index is gone.
        img.apply(&MetadataRecord::V1DeleteTopic(crate::records::DeleteTopicRecord {
            name: "orders".into(),
        }));
        assert!(img.topic_by_id(&id).is_none());
        assert!(img.topic_name_by_id(&id).is_none());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-metadata topic_by_id_resolves_and_drops_on_delete -- --nocapture`
Expected: FAIL to compile — `no method named topic_by_id found`.

- [ ] **Step 3: Add the index field**

In the `MetadataImage` struct (`crates/metadata/src/image.rs:57`), add after the `topics` field:

```rust
    topics: HashMap<String, TopicRecord>,
    /// KIP-516 reverse index: topic UUID -> topic name. Maintained in
    /// `apply()` alongside `topics`; rebuilt on snapshot replay because
    /// every record (including snapshot installs) flows through `apply()`.
    topic_ids: HashMap<Uuid, String>,
```

In `new()` (`crates/metadata/src/image.rs:90`), add after `topics: HashMap::new(),`:

```rust
            topics: HashMap::new(),
            topic_ids: HashMap::new(),
```

- [ ] **Step 4: Maintain the index in `apply()`**

Replace the `V1Topic` arm (`crates/metadata/src/image.rs:369`):

```rust
            MetadataRecord::V1Topic(t) => {
                // If a topic with this name already exists under a different
                // id, drop the stale id entry before re-indexing.
                if let Some(prev) = self.topics.get(&t.name)
                    && prev.topic_id != t.topic_id
                {
                    self.topic_ids.remove(&prev.topic_id);
                }
                self.topic_ids.insert(t.topic_id, t.name.clone());
                self.topics.insert(t.name.clone(), t.clone());
            }
```

Replace the `V1DeleteTopic` arm (`crates/metadata/src/image.rs:379`):

```rust
            MetadataRecord::V1DeleteTopic(d) => {
                if let Some(prev) = self.topics.get(&d.name) {
                    self.topic_ids.remove(&prev.topic_id);
                }
                self.topics.remove(&d.name);
                self.partitions.retain(|(t, _), _| t != &d.name);
                self.topic_configs.remove(&d.name);
            }
```

- [ ] **Step 5: Add the query methods**

Add after the `topic()` method (`crates/metadata/src/image.rs:119`):

```rust
    /// KIP-516: resolve a topic by its UUID. O(1) via the `topic_ids` index.
    #[must_use]
    pub fn topic_by_id(&self, id: &Uuid) -> Option<&TopicRecord> {
        let name = self.topic_ids.get(id)?;
        self.topics.get(name)
    }

    /// KIP-516: resolve a topic name by its UUID.
    #[must_use]
    pub fn topic_name_by_id(&self, id: &Uuid) -> Option<&str> {
        self.topic_ids.get(id).map(String::as_str)
    }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test -p crabka-metadata topic_by_id_resolves_and_drops_on_delete -- --nocapture`
Expected: PASS.

- [ ] **Step 7: Verify the whole metadata crate still builds and tests pass**

Run: `cargo test -p crabka-metadata`
Expected: all pass.

- [ ] **Step 8: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/metadata/src/image.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(metadata): topic-id reverse index on MetadataImage (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: KIP-516 error codes

**Files:**
- Modify: `crates/broker/src/codes.rs` (add near the other codes, e.g. after `INCONSISTENT_CLUSTER_ID` at :230)

- [ ] **Step 1: Write the failing test**

Add a test module at the bottom of `crates/broker/src/codes.rs` (or extend an existing `#[cfg(test)]` block if present):

```rust
#[cfg(test)]
mod kip516_codes {
    use assert2::assert;

    #[test]
    fn kip516_error_code_numbers_match_kafka() {
        assert!(super::UNKNOWN_TOPIC_ID == 100);
        assert!(super::INCONSISTENT_TOPIC_ID == 103);
        assert!(super::FETCH_SESSION_TOPIC_ID_ERROR == 106);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker kip516_error_code_numbers_match_kafka`
Expected: FAIL to compile — `cannot find value UNKNOWN_TOPIC_ID`.

- [ ] **Step 3: Add the constants**

Add after `INCONSISTENT_CLUSTER_ID` (`crates/broker/src/codes.rs:230`):

```rust
/// `UNKNOWN_TOPIC_ID` (100) — a request referenced a topic by UUID that this
/// cluster does not know about (KIP-516).
pub const UNKNOWN_TOPIC_ID: i16 = 100;
/// `INCONSISTENT_TOPIC_ID` (103) — a request supplied a topic UUID that does
/// not match the UUID stored for the named topic (KIP-516).
pub const INCONSISTENT_TOPIC_ID: i16 = 103;
/// `FETCH_SESSION_TOPIC_ID_ERROR` (106) — a fetch session referenced a topic
/// UUID that no longer resolves (e.g. recreated mid-session) (KIP-516).
pub const FETCH_SESSION_TOPIC_ID_ERROR: i16 = 106;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-broker kip516_error_code_numbers_match_kafka`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/codes.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): KIP-516 wire error codes (UNKNOWN/INCONSISTENT_TOPIC_ID)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Shared strict-resolution helper (orchestrator, between batches)

Depends on Tasks 1 and 2. Run after Batch 1 lands.

**Files:**
- Create: `crates/broker/src/topic_resolve.rs`
- Modify: `crates/broker/src/lib.rs` (add module decl after `pub mod quota;` block — keep alphabetical-ish ordering near `:159`)
- Create: `crates/broker/tests/topic_ids.rs` (skeleton only — integration tests appended in Tasks 4–8)

- [ ] **Step 1: Create the helper with its unit tests**

Create `crates/broker/src/topic_resolve.rs`:

```rust
//! KIP-516 strict topic resolution. A single place that maps a request's
//! `(name, topic_id)` pair to a `TopicRecord`, applying the KIP-516
//! strictness rules. The `Err` carries the wire error code to surface for
//! the offending topic/partition.

use crabka_metadata::{MetadataImage, TopicRecord};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::codes;

/// Resolve a requested topic to its record.
///
/// - non-zero `id`, unknown ⇒ `UNKNOWN_TOPIC_ID`
/// - non-zero `id` + non-empty `name` whose stored name differs ⇒ `INCONSISTENT_TOPIC_ID`
/// - zero `id`, `name` resolves ⇒ name path; unknown name ⇒ `UNKNOWN_TOPIC_OR_PARTITION`
pub(crate) fn resolve<'a>(
    image: &'a MetadataImage,
    name: &str,
    id: WireUuid,
) -> Result<&'a TopicRecord, i16> {
    if id != WireUuid::ZERO {
        let uuid = uuid::Uuid::from_bytes(id.0);
        match image.topic_by_id(&uuid) {
            None => Err(codes::UNKNOWN_TOPIC_ID),
            Some(t) if !name.is_empty() && t.name != name => Err(codes::INCONSISTENT_TOPIC_ID),
            Some(t) => Ok(t),
        }
    } else {
        image.topic(name).ok_or(codes::UNKNOWN_TOPIC_OR_PARTITION)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::records::{MetadataRecord, TopicRecord};

    fn image_with(name: &str, id: uuid::Uuid) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: id,
            partitions: 1,
            replication_factor: 1,
        }));
        img
    }

    #[test]
    fn resolves_by_id() {
        let id = uuid::Uuid::from_u128(7);
        let img = image_with("t", id);
        let r = resolve(&img, "", WireUuid(id.into_bytes())).unwrap();
        assert!(r.name == "t");
    }

    #[test]
    fn unknown_id_is_unknown_topic_id() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let err = resolve(&img, "", WireUuid(uuid::Uuid::from_u128(99).into_bytes())).unwrap_err();
        assert!(err == codes::UNKNOWN_TOPIC_ID);
    }

    #[test]
    fn mismatched_name_and_id_is_inconsistent() {
        let id = uuid::Uuid::from_u128(7);
        let img = image_with("t", id);
        // id resolves to "t" but the request also names "other".
        let err = resolve(&img, "other", WireUuid(id.into_bytes())).unwrap_err();
        assert!(err == codes::INCONSISTENT_TOPIC_ID);
    }

    #[test]
    fn name_only_resolves() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let r = resolve(&img, "t", WireUuid::ZERO).unwrap();
        assert!(r.name == "t");
    }

    #[test]
    fn unknown_name_is_unknown_topic_or_partition() {
        let img = image_with("t", uuid::Uuid::from_u128(7));
        let err = resolve(&img, "missing", WireUuid::ZERO).unwrap_err();
        assert!(err == codes::UNKNOWN_TOPIC_OR_PARTITION);
    }
}
```

> If `MetadataImage`/`TopicRecord`/`records` are not re-exported at the paths above, adjust the `use` lines to the real paths (check `crates/metadata/src/lib.rs` exports). `TopicRecord` is used by Task 1's tests via `crate::records::TopicRecord`, so `crabka_metadata::records::TopicRecord` is the safe import in the broker crate.

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`, add (keep near the other `mod` declarations, after `pub mod quota;` at :159):

```rust
pub mod quota;
pub(crate) mod topic_resolve;
```

- [ ] **Step 3: Create the integration test skeleton**

Create `crates/broker/tests/topic_ids.rs`:

```rust
//! KIP-516 topic-identifier integration coverage. Each handler task appends
//! its own `#[tokio::test]` fns under the labelled section below.

use assert2::assert;
mod support;

use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// Round-trip a Metadata request to learn a topic's assigned UUID.
async fn topic_id_for(client: &crabka_client_core::Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn create_topic(client: &crabka_client_core::Client, name: &str, partitions: i32) {
    client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
}

// ─────────────────────────── Task 4: Fetch ───────────────────────────
// (appended by Task 4)

// ────────────────────────── Task 5: Produce ──────────────────────────
// (appended by Task 5)

// ───────────────────────── Task 6: Metadata ──────────────────────────
// (appended by Task 6)

// ──────────────────────── Task 7: DeleteTopics ───────────────────────
// (appended by Task 7)

// ─────────────────────── Task 8: Offset APIs ─────────────────────────
// (appended by Task 8)
```

> Confirm `mod support;` resolves: the harness lives at `crates/broker/tests/support/mod.rs` and exposes `support::start().await` returning a struct with a `.client` field (see `crates/broker/tests/integration.rs`). If a `#[allow(dead_code)]` warning fires on unused helpers until later tasks land, that's expected between tasks.

- [ ] **Step 4: Build and run the helper unit tests**

Run: `cargo test -p crabka-broker topic_resolve`
Expected: 5 tests PASS.

Run: `cargo test -p crabka-broker --test topic_ids`
Expected: compiles, 0 tests run (skeleton only).

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/topic_resolve.rs crates/broker/src/lib.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): shared KIP-516 strict topic resolver + test skeleton

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Fetch handler — strict topic-id (Batch 2)

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs` (per-topic resolution loop around :202–:219)
- Test: append to `crates/broker/tests/topic_ids.rs` (Task 4 section)

**Context:** Today (`fetch.rs:205`) an unknown `topic_id` resolves to `String::new()` via `image.topics().find(...)`, so the partition lookup misses and the row gets `UNKNOWN_TOPIC_OR_PARTITION`. KIP-516 requires `UNKNOWN_TOPIC_ID`.

- [ ] **Step 1: Write the failing integration test**

Append under the Task 4 section of `crates/broker/tests/topic_ids.rs`:

```rust
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

#[tokio::test]
async fn fetch_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    create_topic(&p.client, "f_known", 1).await;

    // A random UUID the cluster has never assigned.
    let bogus = WireUuid(uuid::Uuid::from_u128(0xdead_beef).into_bytes());
    let resp = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            topics: vec![FetchTopic {
                topic: String::new(), // v13: name absent, id-only on the wire
                topic_id: bogus,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1_048_576,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("fetch");

    // Find the partition row for our topic and assert UNKNOWN_TOPIC_ID (100).
    let code = resp
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|pp| pp.error_code)
        .next()
        .expect("a partition row");
    assert!(code == 100);
}
```

> Field names: confirm the FetchResponse topic field is `responses` and the partition list is `partitions` with `error_code` (check `crates/protocol/generated/FetchResponse.owned.rs`). Adjust if the generated names differ.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-broker --test topic_ids fetch_unknown_topic_id_returns_unknown_topic_id`
Expected: FAIL — code is `3` (`UNKNOWN_TOPIC_OR_PARTITION`), not `100`.

- [ ] **Step 3: Route resolution through the helper**

In `crates/broker/src/handlers/fetch.rs`, the per-topic loop (around :202–:219) currently computes `topic_name`/`topic_id` by ad-hoc scan. Replace that block so an explicit `topic_id` that fails resolution yields a per-partition `UNKNOWN_TOPIC_ID` instead of an empty name. Concretely, compute a resolution result once per topic:

```rust
    for topic in &effective_topics {
        // KIP-516 strict resolution. v ≤ 12 sends the name (id zero);
        // v ≥ 13 sends only topic_id. An explicit, unknown id ⇒ every
        // partition row gets UNKNOWN_TOPIC_ID.
        let resolved = crate::topic_resolve::resolve(&image, &topic.topic, topic.topic_id);
        let (topic_name, topic_id, topic_id_error) = match resolved {
            Ok(rec) => (
                rec.name.clone(),
                WireUuid(rec.topic_id.into_bytes()),
                None,
            ),
            // Name-path miss keeps the legacy UNKNOWN_TOPIC_OR_PARTITION
            // behavior via the partition lookup below; only an explicit
            // bad id is surfaced as a topic-id error here.
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => {
                (topic.topic.clone(), topic.topic_id, None)
            }
            Err(code) => (topic.topic.clone(), topic.topic_id, Some(code)),
        };
```

Then in the per-partition loop, before the existing `partitions.get(&topic_name, idx)` lookup, short-circuit when `topic_id_error` is set:

```rust
            if let Some(code) = topic_id_error {
                out.error_code = code;
                pending.push(PendingRead {
                    topic_name: topic_name.clone(),
                    topic_id,
                    partition_index: idx,
                    fetch_offset,
                    max_bytes,
                    read_committed,
                    is_follower_fetch,
                    partition: None,
                    out,
                    cpu_micros: 0,
                });
                continue;
            }
```

> Match the exact `PendingRead { .. }` field set used in the adjacent `topic_denied` branch (`fetch.rs:~240`) — copy that struct literal and only change `out.error_code`. Keep the existing `topic_denied` handling intact (ACL errors still take precedence if you prefer; either order is acceptable since an unknown id can't be ACL-allowed meaningfully — choose topic-id error first for KIP-516 fidelity).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --test topic_ids fetch_unknown_topic_id_returns_unknown_topic_id`
Expected: PASS.

- [ ] **Step 5: Regression — existing fetch tests still pass**

Run: `cargo test -p crabka-broker --test integration && cargo test -p crabka-broker fetch`
Expected: all pass (the happy path — known id or name — is unchanged).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/handlers/fetch.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): Fetch returns UNKNOWN_TOPIC_ID for unknown topic_id (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Produce handler — strict topic-id (Batch 2)

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs` (per-topic resolution at :151–:165)
- Test: append to `crates/broker/tests/topic_ids.rs` (Task 5 section)

**Context:** `produce.rs:156` resolves an unknown id to `""`. KIP-516 requires `UNKNOWN_TOPIC_ID` on each partition row of that topic.

- [ ] **Step 1: Write the failing integration test**

Append under the Task 5 section of `crates/broker/tests/topic_ids.rs`:

```rust
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};

#[tokio::test]
async fn produce_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    create_topic(&p.client, "p_known", 1).await;

    let bogus = WireUuid(uuid::Uuid::from_u128(0x0bad_f00d).into_bytes());
    let resp = p
        .client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: String::new(), // v13: id-only on the wire
                topic_id: bogus,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    let code = resp
        .responses
        .iter()
        .flat_map(|t| t.partition_responses.iter())
        .map(|pr| pr.error_code)
        .next()
        .expect("a partition response");
    assert!(code == 100);
}
```

> Confirm ProduceResponse field names (`responses`, `partition_responses`, `error_code`) against `crates/protocol/generated/ProduceResponse.owned.rs`; adjust if needed.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-broker --test topic_ids produce_unknown_topic_id_returns_unknown_topic_id`
Expected: FAIL — code is `3`, not `100`.

- [ ] **Step 3: Route resolution through the helper**

In `crates/broker/src/handlers/produce.rs`, in the per-topic loop (around :151), replace the ad-hoc name resolution with a `topic_resolve::resolve` call. When it returns `Err(code)` for an explicit bad id (`UNKNOWN_TOPIC_ID` / `INCONSISTENT_TOPIC_ID`), emit the topic's response with that `error_code` on every partition and `continue` past the append path:

```rust
        let image = controller.current_image();
        let topic_name = match crate::topic_resolve::resolve(&image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.clone(),
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => topic.name.clone(), // legacy name-miss path
            Err(code) => {
                // KIP-516: explicit unknown/inconsistent topic_id.
                resp_topics.push(build_topic_error_response(topic, code));
                continue;
            }
        };
```

> Find the existing per-topic response constructor in `produce.rs` (the struct that holds `partition_responses`) and write a small local helper `build_topic_error_response(topic, code)` that returns one `partition_response` per `topic.partition_data` entry with `error_code = code`, `base_offset = -1`. Mirror the field names already used in the success path of this handler. Drop the helper next to the existing private fns at the bottom of the file.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --test topic_ids produce_unknown_topic_id_returns_unknown_topic_id`
Expected: PASS.

- [ ] **Step 5: Regression**

Run: `cargo test -p crabka-broker --test integration && cargo test -p crabka-broker produce`
Expected: all pass.

- [ ] **Step 6: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/handlers/produce.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): Produce returns UNKNOWN_TOPIC_ID for unknown topic_id (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Metadata handler — unknown/inconsistent requested id (Batch 2)

**Files:**
- Modify: `crates/broker/src/handlers/metadata.rs` (candidate-topic resolution at :56–:76 and response build around :116/:172)
- Test: append to `crates/broker/tests/topic_ids.rs` (Task 6 section)

**Context:** `metadata.rs` resolves a requested unknown id to `String::new()`, which then produces a wrong/empty topic entry. KIP-516: a requested unknown id ⇒ a topic entry with `error_code = UNKNOWN_TOPIC_ID` and the requested `topic_id` echoed. Metadata is the one handler where both `name` (v0+) and `topic_id` (v12+) are on the wire together, so it is also where `INCONSISTENT_TOPIC_ID` surfaces.

- [ ] **Step 1: Write the failing integration test**

Append under the Task 6 section of `crates/broker/tests/topic_ids.rs`:

```rust
#[tokio::test]
async fn metadata_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xfeed_face).into_bytes());

    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: None,
                topic_id: bogus,
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");

    let t = resp
        .topics
        .iter()
        .find(|t| t.topic_id == bogus)
        .expect("topic entry echoing the requested id");
    assert!(t.error_code == 100);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-broker --test topic_ids metadata_unknown_topic_id_returns_unknown_topic_id`
Expected: FAIL — no entry echoes `bogus`, or its `error_code` is `0`/`3`.

- [ ] **Step 3: Resolve requested topics strictly**

In `crates/broker/src/handlers/metadata.rs`, replace the `candidate_topics` construction (:56–:76) so each requested topic carries its resolution outcome rather than collapsing to a name string. Build a `Vec` of an enum/tuple: `(requested_name, requested_id, Result<resolved_name, error_code>)` using `crate::topic_resolve::resolve(&image, name, id)`. Then in the response-build loop (around :116/:172), for an `Err(code)` requested topic, push a `MetadataResponseTopic` with:
- `error_code: code`
- `name`: the requested name (or `None` if absent)
- `topic_id`: the requested `topic_id` (echoed)
- `partitions: Vec::new()`, `is_internal: false`

For `Ok(name)`, keep the existing partition-enumeration path, but set `topic_id` from the resolved record (`WireUuid(rec.topic_id.into_bytes())`) — Task 1's index makes this exact.

> Preserve the "list all topics when `req.topics` is `None`" branch unchanged. Only the explicitly-requested-topic path gains strict resolution.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --test topic_ids metadata_unknown_topic_id_returns_unknown_topic_id`
Expected: PASS.

- [ ] **Step 5: Regression**

Run: `cargo test -p crabka-broker --test integration && cargo test -p crabka-broker metadata`
Expected: all pass (requests by name and by known id are unchanged).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/handlers/metadata.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): Metadata returns UNKNOWN_TOPIC_ID for unknown requested id (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: DeleteTopics — delete-by-id unknown ⇒ UNKNOWN_TOPIC_ID (Batch 2)

**Files:**
- Modify: `crates/broker/src/handlers/delete_topics.rs` (id→name resolution at :38–:57, error mapping around :124)
- Test: append to `crates/broker/tests/topic_ids.rs` (Task 7 section)

**Context:** Deleting by `topic_id` (v6+) with an unknown id currently resolves to `None` and the topic gets `UNKNOWN_TOPIC_OR_PARTITION`. KIP-516 (IBP≥2.8) requires `UNKNOWN_TOPIC_ID`. The fix must distinguish "request used an id we don't know" (⇒ `UNKNOWN_TOPIC_ID`) from "request used a name we don't know" (⇒ `UNKNOWN_TOPIC_OR_PARTITION`).

- [ ] **Step 1: Write the failing integration test**

Append under the Task 7 section of `crates/broker/tests/topic_ids.rs`:

```rust
use crabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};

#[tokio::test]
async fn delete_topics_by_unknown_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xc0ff_ee00).into_bytes());

    let resp = p
        .client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: None,
                topic_id: bogus,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("delete topics");

    let r = resp.responses.first().expect("one response row");
    assert!(r.error_code == 100);
}
```

> Confirm DeleteTopicsResponse field names (`responses`, per-row `error_code`, and that the row echoes `topic_id`) against the generated struct; adjust if needed.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-broker --test topic_ids delete_topics_by_unknown_id_returns_unknown_topic_id`
Expected: FAIL — `error_code` is `3`, not `100`.

- [ ] **Step 3: Track whether each entry was requested by id**

In `crates/broker/src/handlers/delete_topics.rs`, where it builds `name_list` from `req.topics` (:38–:57), also record per-entry whether the request used a `topic_id` (i.e. `state.name` is `None`/empty and `state.topic_id != WireUuid::ZERO`). Then in the result-mapping where an unresolved entry currently becomes `UNKNOWN_TOPIC_OR_PARTITION` (~:124), choose the code:

```rust
            let code = if requested_by_id {
                codes::UNKNOWN_TOPIC_ID
            } else {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            };
```

Use the Task 1 index for the id→name lookup: replace the `image.topics().find(|t| t.topic_id.into_bytes() == state.topic_id.0)` scan with `image.topic_by_id(&uuid::Uuid::from_bytes(state.topic_id.0))`.

> Ensure the response row for an unresolved id-based delete echoes the requested `topic_id` (KIP-516 clients match responses by id).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --test topic_ids delete_topics_by_unknown_id_returns_unknown_topic_id`
Expected: PASS.

- [ ] **Step 5: Regression**

Run: `cargo test -p crabka-broker delete_topics && cargo test -p crabka-broker --test integration`
Expected: all pass (delete by name and by known id unchanged).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/handlers/delete_topics.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): DeleteTopics by unknown id returns UNKNOWN_TOPIC_ID (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: Offset APIs — unlock OffsetCommit v10+ / OffsetFetch v8+ (Batch 2)

**Files:**
- Modify: `crates/broker/src/api_catalog.rs` (offset caps at :62–:71)
- Modify: `crates/broker/src/handlers/offset_commit.rs` (decode/normalize at :37–:38, response topic_id echo)
- Modify: `crates/broker/src/handlers/offset_fetch.rs` (add v8+ `groups[]` branch)
- Test: append to `crates/broker/tests/topic_ids.rs` (Task 8 section)

**Context:** `api_catalog.rs` deliberately caps OffsetCommit at v9 and OffsetFetch at v7. Internal storage (`Group.committed_offsets: HashMap<(String,i32), OffsetEntry>`) stays name-keyed — KIP-516 never changed the `__consumer_offsets` key format. The work is: lift the caps, translate `topic_id → name` at the wire boundary for v10+ topics, implement the v8+ multi-group OffsetFetch shape, and echo `topic_id` on v10+ responses.

### 8a — Lift the version caps

- [ ] **Step 1: Replace the capped entries in `api_catalog.rs`**

Replace the two capped `ApiVersion` literals (`crates/broker/src/api_catalog.rs:55–:71`) — remove the cap comment and use the generated max:

```rust
        v!(offset_commit_request),
        v!(offset_fetch_request),
```

> `v!` expands to an `ApiVersion` using the request's `MIN_VERSION`/`MAX_VERSION`/`API_KEY` (see the macro usage for `metadata_request` etc.). This advertises the full ranges.

- [ ] **Step 2: Build and check for advertise-assertion fallout**

Run: `cargo test -p crabka-broker api_versions`
Expected: PASS. If any assertion pinned offset max to 9/7, update it to the generated `MAX_VERSION` (search `crates/broker/src/handlers/api_versions.rs` and `crates/broker/tests/api_versions_features.rs`).

### 8b — OffsetCommit v10+: id→name normalization + topic_id echo

- [ ] **Step 3: Write the failing integration test**

Append under the Task 8 section of `crates/broker/tests/topic_ids.rs` (commit round-trip uses the negotiated max version, which now carries topic_id):

```rust
use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
};

#[tokio::test]
async fn offset_commit_and_fetch_by_topic_id_round_trip() {
    let p = support::start().await;
    create_topic(&p.client, "o_topic", 1).await;
    let id = topic_id_for(&p.client, "o_topic").await;

    // Commit offset 42 by topic_id (v10+ wire shape: name empty, id set).
    p.client
        .send(OffsetCommitRequest {
            group_id: "g1".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: String::new(),
                topic_id: id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    // Fetch it back via the v8+ multi-group shape, keyed by topic_id.
    let resp = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g1".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: String::new(),
                    topic_id: id,
                    partition_indexes: vec![0],
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset fetch");

    let grp = resp.groups.iter().find(|g| g.group_id == "g1").expect("group g1");
    let t = grp.topics.iter().find(|t| t.topic_id == id).expect("topic by id");
    let part = t.partitions.first().expect("partition 0");
    assert!(part.committed_offset == 42);
    assert!(part.error_code == 0);
    assert!(t.topic_id == id); // id echoed on the response
}
```

- [ ] **Step 4: Run it to verify it fails**

Run: `cargo test -p crabka-broker --test topic_ids offset_commit_and_fetch_by_topic_id_round_trip`
Expected: FAIL — OffsetFetch ignores the `groups` array today (empty `resp.groups`), and OffsetCommit can't resolve the empty-name/id topic.

- [ ] **Step 5: Normalize OffsetCommit request names from topic_id**

In `crates/broker/src/handlers/offset_commit.rs`, immediately after `let req = OffsetCommitRequest::decode(...)?;` (:38), add a normalization pass that fills each topic's `name` from its `topic_id` and records unresolved topics:

```rust
    let mut req = req;
    // KIP-516: at v10+ the wire carries only topic_id. Translate to the
    // name-keyed offset store. Topics with an unknown id are split out and
    // surfaced as UNKNOWN_TOPIC_ID without committing.
    let mut unknown_id_topics: Vec<usize> = Vec::new();
    {
        let image = broker.controller.current_image();
        for (i, t) in req.topics.iter_mut().enumerate() {
            if t.name.is_empty() && t.topic_id != WireUuid::ZERO {
                match image.topic_name_by_id(&uuid::Uuid::from_bytes(t.topic_id.0)) {
                    Some(name) => t.name = name.to_string(),
                    None => unknown_id_topics.push(i),
                }
            }
        }
    }
```

Then, in the response-building path, for any index in `unknown_id_topics`, emit that topic's response row with `error_code = codes::UNKNOWN_TOPIC_ID` on every partition and skip it in `update_committed`/the append batch. The simplest robust approach: filter `req.topics` to the resolved set for committing, and add the unknown-id topics back as error rows when constructing the response. Ensure response topics echo `topic_id` (the response struct has a `topic_id` field; copy it from the matching request topic).

> Add `use crabka_protocol::primitives::uuid::Uuid as WireUuid;` to the handler imports if not already present.

- [ ] **Step 6: Implement the OffsetFetch v8+ `groups[]` branch**

In `crates/broker/src/handlers/offset_fetch.rs`, gate on `version >= 8`. When set, process `req.groups` and populate `resp.groups` (leaving `resp.topics` empty); otherwise keep the existing v0–v7 single-group path untouched. For each `OffsetFetchRequestGroup`:
- Run the same group-`Describe` ACL check (per group_id).
- For each `OffsetFetchRequestTopics`, resolve the name: if `topic_id != ZERO`, look it up via `image.topic_name_by_id(...)`; an unknown id ⇒ this topic's row gets `error_code = UNKNOWN_TOPIC_ID` and `-1` offsets. Otherwise use `t.name`.
- Read offsets from `g.committed_offsets.get(&(name, pid))` exactly as the v0–v7 path does.
- Build `OffsetFetchResponseGroup { group_id, topics: Vec<OffsetFetchResponseTopics>, error_code: NONE }`, where each `OffsetFetchResponseTopics` carries `name`, `topic_id` (echoed), and `partitions: Vec<OffsetFetchResponsePartitions>`.

Skeleton for the new branch (field names verified against the generated structs):

```rust
    if version >= 8 {
        use crabka_protocol::owned::offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
        };
        let image = broker.controller.current_image();
        let mut groups_out: Vec<OffsetFetchResponseGroup> = Vec::new();
        for grp in &req.groups {
            // (ACL Describe on grp.group_id; on Deny push a group row with
            //  error_code = GROUP_AUTHORIZATION_FAILED and continue.)
            let handle = broker.group_manager.get_or_create(&grp.group_id);
            let g = handle.state.lock().await;
            let mut topics_out: Vec<OffsetFetchResponseTopics> = Vec::new();
            let req_topics = grp.topics.as_deref().unwrap_or(&[]);
            for t in req_topics {
                let name = if t.topic_id != WireUuid::ZERO {
                    match image.topic_name_by_id(&uuid::Uuid::from_bytes(t.topic_id.0)) {
                        Some(n) => n.to_string(),
                        None => {
                            topics_out.push(OffsetFetchResponseTopics {
                                name: String::new(),
                                topic_id: t.topic_id,
                                partitions: t
                                    .partition_indexes
                                    .iter()
                                    .map(|&pid| OffsetFetchResponsePartitions {
                                        partition_index: pid,
                                        committed_offset: -1,
                                        committed_leader_epoch: -1,
                                        metadata: None,
                                        error_code: codes::UNKNOWN_TOPIC_ID,
                                    })
                                    .collect(),
                            });
                            continue;
                        }
                    }
                } else {
                    t.name.clone()
                };
                let partitions = t
                    .partition_indexes
                    .iter()
                    .map(|&pid| match g.committed_offsets.get(&(name.clone(), pid)) {
                        Some(e) => OffsetFetchResponsePartitions {
                            partition_index: pid,
                            committed_offset: e.offset,
                            committed_leader_epoch: e.leader_epoch,
                            metadata: Some(e.metadata.clone()),
                            error_code: codes::NONE,
                        },
                        None => OffsetFetchResponsePartitions {
                            partition_index: pid,
                            committed_offset: -1,
                            committed_leader_epoch: -1,
                            metadata: None,
                            error_code: codes::NONE,
                        },
                    })
                    .collect();
                topics_out.push(OffsetFetchResponsePartitions_wrap(name, t.topic_id, partitions));
            }
            groups_out.push(OffsetFetchResponseGroup {
                group_id: grp.group_id.clone(),
                topics: topics_out,
                error_code: codes::NONE,
            });
        }
        let resp = OffsetFetchResponse { groups: groups_out, ..Default::default() };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        return Ok(buf.freeze());
    }
```

> The `OffsetFetchResponsePartitions_wrap` placeholder above is illustrative — inline the `OffsetFetchResponseTopics { name, topic_id: t.topic_id, partitions }` literal directly; do not introduce a helper with that name. Also add the group-level `Describe` ACL check mirroring the v0–v7 preamble at `offset_fetch.rs:36–56`.

- [ ] **Step 7: Run the round-trip test to verify it passes**

Run: `cargo test -p crabka-broker --test topic_ids offset_commit_and_fetch_by_topic_id_round_trip`
Expected: PASS.

- [ ] **Step 8: Regression — legacy offset paths and group flow still pass**

Run: `cargo test -p crabka-broker offset && cargo test -p crabka-broker --test unit full_group_flow_join_sync_heartbeat_commit_fetch_leave`
Expected: all pass (v0–v7 single-group path is unchanged).

- [ ] **Step 9: Format and commit**

```bash
cargo fmt
git -C /Users/mattstone/git/crabka/.claude/worktrees/loving-mirzakhani-fed4b3 add crates/broker/src/api_catalog.rs crates/broker/src/handlers/offset_commit.rs crates/broker/src/handlers/offset_fetch.rs crates/broker/tests/topic_ids.rs
git -c user.name='Matt Stone' -c user.email='matthew.d.stone@gmail.com' commit -m "feat(broker): OffsetCommit v10+/OffsetFetch v8+ by topic_id (KIP-516)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification (orchestrator, after Batch 2)

- [ ] **Step 1: Full workspace build + clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 2: Full test suite**

Run: `cargo test --workspace`
Expected: all pass.

- [ ] **Step 3: Format check (CI gate)**

Run: `cargo fmt --check`
Expected: clean (no diff).

- [ ] **Step 4: Add an INCONSISTENT_TOPIC_ID integration test (Metadata)**

Append to `crates/broker/tests/topic_ids.rs`: create topic `incon`, fetch its real id, then send a Metadata request with `name: Some("incon")` **and** `topic_id` set to a *different* real topic's id; assert the response topic for that entry has `error_code == 103`. (The unit-level coverage in Task 3 is authoritative; this confirms the wire path surfaces it where both fields coexist on the wire at Metadata v12+.) Run it, confirm PASS, format, and commit.

- [ ] **Step 5: Update memory**

If anything non-obvious surfaced (e.g. a generated field name that differed from the plan, or an api_versions assertion that needed updating), note it for the session summary.
```
