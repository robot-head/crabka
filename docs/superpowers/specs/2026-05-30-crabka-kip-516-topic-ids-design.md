# KIP-516 — Topic Identifiers (full implementation)

**Date:** 2026-05-30
**Status:** Approved — ready for implementation plan
**Scope decision:** Core KIP-516 **plus** the offset APIs (OffsetCommit v10+, OffsetFetch
v8+). Full Kafka strictness for topic-id validation.

## Summary

Crabka already lays most of the KIP-516 groundwork: topic UUIDs are generated at
`CreateTopics`, persisted in `TopicRecord`, returned in `MetadataResponse` (v10+) and
`CreateTopicsResponse` (v7+), carried in the Produce/Fetch/OffsetCommit/OffsetFetch wire
schemas, and resolved `topic_id → name` (by linear scan) inside several handlers.

This work completes KIP-516 by adding the *correctness and completeness* layer:

1. An efficient `topic_id → topic` index in `MetadataImage` (replaces O(topics) scans).
2. The KIP-516 wire error codes and **strict** topic-id validation.
3. Strict topic-id semantics across the Fetch, Produce, Metadata, and DeleteTopics handlers.
4. The OffsetCommit v10+ and OffsetFetch v8+/v10+ wire shapes, which are currently capped
   off in `api_catalog.rs` because no topic-id index existed.

## Non-goals

- **ListOffsets** and **OffsetForLeaderEpoch** — KIP-516 never added a `topic_id` field to
  these; their schemas carry no `TopicId`. They remain name-based. Out of scope.
- **`metadata.version` feature-gating** of topic-id usage. Crabka is greenfield; topic IDs
  always exist. The only gate that matters is Kafka message-version negotiation via
  ApiVersions, which is already in place. No IBP/feature flags.
- **Changing the `__consumer_offsets` record key format.** Kafka's `OffsetCommitKey` stayed
  `(group, topic-name, partition)` through KIP-516; topic IDs were never written into the
  offsets log. Internal offset storage stays name-keyed; topic-id is a pure wire-boundary
  translation. `coordinator/unified/persistence.rs` is untouched.
- A full fetch-session topic-id-recreate state machine. The session cache key already carries
  both name and id; recreate-with-new-id is naturally a cache miss. We define
  `FETCH_SESSION_TOPIC_ID_ERROR` and apply it where a session references a now-unknown id,
  but do not build deeper session reconciliation.

## Component 1 — Topic-ID reverse index

**File:** `crates/metadata/src/image.rs`

Add an id→name index to `MetadataImage`:

```rust
topic_ids: HashMap<uuid::Uuid, String>,   // topic_id -> topic name
```

Maintained in `apply()`:
- `MetadataRecord::V1Topic(t)` → `topic_ids.insert(t.topic_id, t.name.clone())`
- `MetadataRecord::V1DeleteTopic(d)` → remove the entry whose name == `d.name`
  (look up the topic's id from `self.topics` before the name is removed, or retain by value).

New query methods:
- `pub fn topic_by_id(&self, id: &uuid::Uuid) -> Option<&TopicRecord>`
- `pub fn topic_name_by_id(&self, id: &uuid::Uuid) -> Option<&str>`

The index is rebuilt naturally on snapshot replay because it is maintained in `apply()`,
which is the single path through which all records (including snapshot installs) flow.

**Cleanup:** the existing `image.topics().find(|t| t.topic_id.into_bytes() == ...)` scans in
the Fetch/Produce/Metadata/DeleteTopics handlers are replaced with `topic_by_id` lookups.
(The metadata layer keys on `uuid::Uuid`; the wire layer uses
`crabka_protocol::primitives::uuid::Uuid` = `[u8; 16]`. Convert with
`uuid::Uuid::from_bytes(wire.0)` / `topic_id.into_bytes()`.)

## Component 2 — Error codes

**File:** `crates/broker/src/codes.rs`

Add, with their real Kafka `Errors` numbers (verified against the existing
`INCONSISTENT_CLUSTER_ID = 104`):

```rust
pub const UNKNOWN_TOPIC_ID: i16 = 100;
pub const INCONSISTENT_TOPIC_ID: i16 = 103;
pub const FETCH_SESSION_TOPIC_ID_ERROR: i16 = 106;
```

## Component 3 — Shared strict-resolution helper

**File (new):** `crates/broker/src/topic_resolve.rs`

A single function so the strictness rules live in exactly one place:

```rust
/// Resolve a (name, topic_id) pair from a request to a topic, applying KIP-516
/// strictness. `Err` carries the wire error code to return for this topic.
pub fn resolve<'a>(
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
```

Rules (the "full strictness" choice):
- non-zero id, unknown ⇒ `UNKNOWN_TOPIC_ID`
- non-zero id + non-empty name that disagrees with the stored name ⇒ `INCONSISTENT_TOPIC_ID`
- zero id, name resolves ⇒ name path; name unknown ⇒ `UNKNOWN_TOPIC_OR_PARTITION`

This is written by the orchestrator between batches (it depends on both Component 1 and 2).

## Component 4 — Wire handler strictness

Route each handler's topic resolution through Component 3 instead of the ad-hoc
`.find(...).unwrap_or_default()` that currently collapses an unknown id to `""`.

- **Fetch** (`crates/broker/src/handlers/fetch.rs`), **Produce**
  (`crates/broker/src/handlers/produce.rs`): at v13 the wire carries only `topic_id`. An
  unknown id ⇒ every partition row for that topic gets `UNKNOWN_TOPIC_ID` (today they
  silently become `UNKNOWN_TOPIC_OR_PARTITION`). The resolved name then feeds the existing
  name-keyed `PartitionRegistry` path unchanged.
- **Metadata** (`crates/broker/src/handlers/metadata.rs`): a requested unknown id ⇒ a topic
  entry with `error_code = UNKNOWN_TOPIC_ID` and the requested `topic_id` echoed back.
- **DeleteTopics** (`crates/broker/src/handlers/delete_topics.rs`): deletion by id (v6+) with
  an unknown id ⇒ `UNKNOWN_TOPIC_ID` (KIP-516 IBP≥2.8 behavior), replacing the current
  `UNKNOWN_TOPIC_OR_PARTITION`.

## Component 5 — Offset APIs

- **`crates/broker/src/api_catalog.rs`**: lift the deliberate caps. OffsetCommit →
  `owned::offset_commit_request::MAX_VERSION`; OffsetFetch →
  `owned::offset_fetch_request::MAX_VERSION`. Remove the explanatory cap comment.
- **`crates/broker/src/handlers/api_versions.rs`**: update the assertions that currently pin
  the advertised OffsetCommit/OffsetFetch max versions to 9/7.
- **OffsetFetch v8+** (`crates/broker/src/handlers/offset_fetch.rs`): implement the
  multi-group `groups[]` request/response shape the handler currently ignores (it only serves
  the legacy single-group shape today).
- **v10+ topic_id** (both `offset_commit.rs` and `offset_fetch.rs`): at the wire boundary,
  resolve `topic_id → name` via Component 3, then commit/fetch against the existing
  name-keyed store **unchanged**, and echo `topic_id` on v10+ responses. An unknown id ⇒
  that topic's entry gets `UNKNOWN_TOPIC_ID`. Translation happens before the
  classic-vs-next-gen coordinator fork, so both coordinator paths work without change.

## Data flow

```
request(topic_id [, name])
  └─ topic_resolve::resolve(image, name, id)  ── Err(code) ─→ per-topic/partition error
        └─ Ok(&TopicRecord) → name
              └─ existing name-keyed path (PartitionRegistry / offset store) [UNCHANGED]
                    └─ response echoes topic_id on versions that carry it
```

Single translation point at ingress; the entire hot path below stays name-keyed.

## Error handling

All id-failure modes funnel through Component 3, applied uniformly. Fetch-session topic-id
mismatch on recreate is naturally a cache miss (the `FetchSessionKey` already carries both
`topic_name` and `topic_id`); `FETCH_SESSION_TOPIC_ID_ERROR` is applied where a live session
references an id that no longer resolves.

## Testing (TDD)

Unit:
- `MetadataImage::topic_by_id` returns the right record; index stays consistent across
  create then delete (deleted id no longer resolves).
- `topic_resolve::resolve` strictness matrix: unknown id ⇒ `UNKNOWN_TOPIC_ID`; non-empty name
  disagreeing with stored ⇒ `INCONSISTENT_TOPIC_ID`; name-only resolves; zero/zero invalid.

Integration (`crates/broker/tests/`):
- Fetch v13 and Produce v13 by topic_id, including unknown id ⇒ `UNKNOWN_TOPIC_ID`.
- Metadata request by unknown id ⇒ topic entry with `UNKNOWN_TOPIC_ID`.
- DeleteTopics by unknown id ⇒ `UNKNOWN_TOPIC_ID`.
- OffsetCommit v10 + OffsetFetch v8 and v10 round-trip by topic_id; verify offsets stored
  and read back, topic_id echoed on responses.
- Inconsistent name+id ⇒ `INCONSISTENT_TOPIC_ID` (where a request can carry both).

## Execution plan (parallel batches, disjoint file sets)

Per CLAUDE.md: subagent-driven development in parallel batches with non-overlapping file sets.

- **Batch 1** (parallel): Component 1 (`crates/metadata/src/image.rs`) ‖ Component 2
  (`crates/broker/src/codes.rs`). Disjoint files.
- **Between batches** (orchestrator, sequential): Component 3 — create
  `crates/broker/src/topic_resolve.rs` and register the module in `crates/broker/src/lib.rs`.
  Depends on both Batch 1 outputs.
- **Batch 2** (parallel, disjoint files): Fetch (`fetch.rs`) ‖ Produce (`produce.rs`) ‖
  Metadata (`metadata.rs`) ‖ DeleteTopics (`delete_topics.rs`) ‖ Offsets (`offset_commit.rs`
  + `offset_fetch.rs` + `api_catalog.rs` + `api_versions.rs` test assertions).

Each batch is followed by review, `cargo fmt`, build, and the relevant tests before moving on.
