# KIP-447 — Producer scalability for exactly-once semantics (+ KIP-1319 member epoch on TxnOffsetCommit)

**Date:** 2026-06-03
**Status:** Approved — ready for implementation plan

## Problem

[KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447:+Producer+scalability+for+exactly+once+semantics)
lets a single transactional producer drive exactly-once consume-process-produce
across *all* of a consumer group's input partitions, instead of requiring one
producer per input partition. It does this by fencing zombie producers through
the **consumer group's** state (generation / member id / member epoch) at
`TxnOffsetCommit` time, rather than relying solely on per-`transactional.id`
producer-epoch fencing.

Crabka already has most of the broker-side machinery, but **the loop is not
closed**:

- The producer's `send_offsets_to_transaction(offsets, group_id)` sends
  `TxnOffsetCommitRequest` with `generation_id` left at its default of `-1`
  and an empty `member_id`
  (`crates/client-producer/src/producer.rs:398`,
  `crates/protocol/generated/TxnOffsetCommitRequest.owned.rs:43`).
- The broker's `txn_offset_commit` handler only fences when
  `generation_id >= 0` and the group is classic, using bespoke
  `ClassicInspect` logic
  (`crates/broker/src/txn/handlers/txn_offset_commit.rs:122`). Because the
  producer never sends a real generation, **this fencing is dead code**.
- The consumer client exposes `group_id()` / `member_id()` / `generation_id()`
  but no single `group_metadata()` accessor to hand to the producer
  (`crates/client-consumer/src/consumer.rs:397`).

KIP-447's wiki text: *"To provide fencing consistent with normal offset
fencing, member.id, group.instance.id and generation.id fields were added to
TxnOffsetCommitRequest."* The phrase **"consistent with normal offset
fencing"** is the design north star: the txn path must validate exactly the
way the regular `OffsetCommit` path does.

## Scope

In scope:

1. **Client wiring (the core KIP-447 gap).** Producer forwards consumer-group
   metadata; consumer exposes it.
2. **Classic-group fencing** on `TxnOffsetCommit`: generation id + member id +
   `group.instance.id`, including the unknown-member case the current handler
   misses.
3. **KIP-1319 / next-gen member-epoch fencing** on `TxnOffsetCommit`:
   `STALE_MEMBER_EPOCH` (113) / `FENCED_MEMBER_EPOCH` for KIP-848
   "consumer"-protocol groups. Clears the
   `TODO(KIP-1319 v4+)` at `txn_offset_commit.rs:120`.
4. **End-to-end tests** exercising both the happy path (real metadata flows)
   and zombie fencing (stale generation / unknown member / stale epoch).
5. **Docs**: README KIP-447 row ⚠️ → ✅; STATUS.md slice entry.

Out of scope (documented as follow-ups):

- Consumer-side static-membership (`group.instance.id`) configuration. The
  consumer client has no static membership today, so
  `ConsumerGroupMetadata.group_instance_id` is always `None`. The broker path
  still validates the instance-id case for protocol completeness.
- TV2 / KIP-890 producer-epoch-bump interplay (tracked separately under the
  transaction-version work).

## Components & changes

### 1. Consumer client — expose `ConsumerGroupMetadata`

`crates/client-consumer`. New public type mirroring the JVM's
`org.apache.kafka.clients.consumer.ConsumerGroupMetadata`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    /// Classic group generation id, or — for KIP-848 next-gen groups — the
    /// member epoch. Carried in the `generation_id` field of
    /// `TxnOffsetCommitRequest` either way (matches the JVM wire convention).
    pub generation_id: i32,
    pub member_id: String,
    /// `group.instance.id` for static members. Always `None` today — the
    /// consumer client has no static-membership support yet.
    pub group_instance_id: Option<String>,
}

impl Consumer {
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata { /* clone from fields */ }
}
```

The type is exported from the consumer crate root.

### 2. Producer client — forward the metadata

`crates/client-producer/src/producer.rs`. Breaking signature change
(greenfield — no overload retained, per `CLAUDE.md`):

```rust
// before: send_offsets_to_transaction(offsets, group_id: &str)
pub async fn send_offsets_to_transaction(
    &self,
    offsets: impl IntoIterator<Item = ((String, i32), i64)>,
    group_meta: &ConsumerGroupMetadata,
) -> Result<(), ProducerError>
```

- `AddOffsetsToTxn` and `FindCoordinator` use `group_meta.group_id`.
- `TxnOffsetCommitRequest` is populated with `group_meta.generation_id`,
  `group_meta.member_id`, `group_meta.group_instance_id`.
- Re-export `ConsumerGroupMetadata` from the producer crate root
  (`crabka-client-producer` already depends on `crabka-client-consumer`), so
  callers need a single import.

The client negotiates `TxnOffsetCommit` to v3+ (broker advertises through v5),
so the flexible fields are encoded on the wire.

### 3. Broker — reuse the canonical validation path

`crates/broker`. The regular `OffsetCommit` handler already routes
classic-vs-next-gen validation through the per-group actor
(`crates/broker/src/handlers/offset_commit.rs:280`):

- `GroupKindTag::Consumer` → `OffsetValidate { member_id, member_epoch }` →
  `UNKNOWN_MEMBER_ID` / `STALE_MEMBER_EPOCH` / `FENCED_MEMBER_EPOCH`
  (`actor.rs:316`).
- `GroupKindTag::Classic` → `ClassicValidateCommit` →
  `classic_ops::validate_commit` (`classic_ops.rs:424`): simple-consumer pass
  (empty member id + no instance), instance-id → current-member resolution
  (`UNKNOWN_MEMBER_ID` / `FENCED_INSTANCE_ID`), bare member-id existence
  (`UNKNOWN_MEMBER_ID`), then generation check (`ILLEGAL_GENERATION`).

**Decision: extract a shared helper.** Pull the classic/next-gen routing into
one function:

```rust
// new shared location (e.g. crates/broker/src/coordinator/group_validate.rs)
pub(crate) async fn validate_group_commit(
    handle: &Arc<GroupActorHandle>,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
) -> Option<i16>
```

Both `offset_commit::validate` and the `txn_offset_commit` handler call it.
KIP-447's "consistent with normal offset fencing" is a correctness contract;
a single helper makes divergence impossible. `txn_offset_commit` passes
`req.generation_id` as `generation_or_epoch` (it carries the member epoch for
next-gen consumers, matching the JVM).

This replaces the bespoke `ClassicInspect`-based block in `txn_offset_commit.rs`
and removes the `generation_id >= 0` gate — `validate_group_commit` already
no-ops for the simple-consumer case (empty member id + no instance), so a
producer that supplies no metadata is unaffected.

### 4. Tests

`crates/broker/tests/transactions.rs`:

- Update `send_offsets_to_transaction_atomic_with_records` to thread the real
  `consumer.group_metadata()` through the new producer signature; assert the
  commit still succeeds and `read_committed` sees the output.
- New zombie-fencing tests (classic group):
  - stale `generation_id` → `ILLEGAL_GENERATION`,
  - unknown `member_id` → `UNKNOWN_MEMBER_ID`.
- New next-gen (KIP-848 "consumer"-protocol group) test: stale member epoch in
  `generation_id` → `STALE_MEMBER_EPOCH`. (If standing up a next-gen group in
  the integration harness is impractical, cover the epoch routing at the
  handler/actor level with a unit-style test instead, and note the limitation.)

### 5. Docs

- `README.md`: flip the KIP-447 row ⚠️ → ✅.
- `STATUS.md`: add a slice entry summarizing the closed loop, the
  shared-validation refactor, the next-gen epoch addition, and the test
  coverage.

## Data flow (consume-process-produce, after the change)

```
consumer.poll() ──► records
        │
        ├─ process ──► producer.send(output records)   [transactional]
        │
        └─ producer.send_offsets_to_transaction(
                offsets, consumer.group_metadata())
                 │
                 ├─ AddOffsetsToTxn(group_id) ──► txn coordinator
                 │     (registers __consumer_offsets partition in the txn)
                 │
                 └─ TxnOffsetCommit(group_id, generation_id, member_id,
                        group_instance_id, offsets) ──► group coordinator
                          │
                          └─ validate_group_commit(...)
                               classic  → ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID
                                          / FENCED_INSTANCE_ID
                               next-gen → STALE_MEMBER_EPOCH / FENCED_MEMBER_EPOCH
                                          / UNKNOWN_MEMBER_ID
producer.commit_transaction() ──► EndTxn ──► WriteTxnMarkers to every
        registered partition incl. __consumer_offsets (atomic).
```

A zombie producer (one whose consumer was kicked out of the group by a
rebalance) carries a stale generation/epoch and is rejected at
`TxnOffsetCommit`, so its offset commit never becomes visible — even though its
`transactional.id`/producer-epoch fencing might not yet have caught it.

## Risks / notes

- **Blast radius:** the shared-helper extraction touches the working
  `offset_commit.rs`. Mitigation: the helper is a pure move of existing logic;
  the regular offset-commit suite must stay green.
- **Version negotiation:** raising no advertised maxima here, so no
  version-cap ripple (cf. the offset-API version-cap lesson). The producer
  already negotiates `TxnOffsetCommit` to the advertised max.
- **Mac multi-broker limitation:** end-to-end tests use the in-process /
  single-broker harness (data replication across JVM brokers doesn't work on
  the Mac); fencing is a coordinator-local concern, so this is fine.
