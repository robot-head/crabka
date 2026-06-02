# KIP-595 Slice 3d-2 — KIP-631-framed Log + Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Translate `MetadataRecord ↔ KraftMetadataRecord` at the engine submit/apply + snapshot boundary so the controller's log and checkpoints are genuinely KIP-631-framed, with no change to `MetadataImage` getters, broker handlers, or `submit_change`.

**Architecture:** A new `kraft_translate` module in `crates/metadata` converts each `MetadataRecord` to/from its KIP-631 `KraftMetadataRecord` counterpart (defaulting fields Crabka doesn't model, bridging `topic_id↔name` via the image). The engine and snapshot codecs call it in place of the wincode bridge. One contained image change (derive partition count from the partitions map) keeps the round-trip lossless.

**Tech Stack:** `crabka_metadata::MetadataRecord` + `MetadataImage`, `crabka_protocol::records::metadata::KraftMetadataRecord` (Slice 3d-1), the engine (`crates/raft/src/kraft/controller.rs`) + `snapshot.rs`.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice3d2-log-kip631-framing-design.md](../specs/2026-05-31-kip595-slice3d2-log-kip631-framing-design.md)

---

## Background the implementer needs

- **Scope discipline:** the `MetadataImage` getter return types, all ~25 broker handlers, and `submit_change(Vec<MetadataRecord>)` are UNCHANGED. Only the on-log/on-checkpoint bytes change. The wincode `MetadataRecord` enum stays as the internal currency. If you find yourself editing a broker handler or changing a getter's return type, STOP — that's out of scope.
- **The `KraftMetadataRecord` dispatch** (Slice 3d-1, `crates/protocol/src/records/metadata/record.rs`) has all the modeled variants + `encode_value(version)->Bytes` / `decode_value(&[u8])->(Self,i16)`. Generated record types are at `crabka_protocol::owned::<snake>::<Type>` and derive `Default`.
- **Field-mismatch reference** (from exploration — Crabka `MetadataRecord` vs KIP-631 generated type):
  - `V1Topic{name,topic_id,partitions:i32,replication_factor:i16}` ↔ `TopicRecord{name,topic_id}`. **partitions/RF dropped on encode**; on decode the image derives partition count from its partitions map (see Task 2). RF is not used by the image post-apply for the KIP-631 path (validate's RF check moves with the derive change).
  - `V1Partition{topic:String,partition,leader,replicas,isr,leader_epoch,adding_replicas,removing_replicas}` ↔ `PartitionRecord{partition_id,topic_id,replicas,isr,leader,leader_epoch,partition_epoch,adding_replicas,removing_replicas,directories,leader_recovery_state,eligible_leader_replicas,last_known_elr}`. **Key by `topic_id` (resolve name→id from the image on encode, id→name on decode)**; default the KIP-631 extras (`partition_epoch`=0, `directories`=[], `leader_recovery_state`=0, ELR=None) on encode; drop them on decode. `NodeId(u64)↔i32` casts.
  - `V1BrokerRegistration{node_id,host,port,rack,endpoints}` ↔ `RegisterBrokerRecord{broker_id,is_migrating_zk_broker,incarnation_id,broker_epoch,end_points,features,rack,fenced,in_controlled_shutdown,log_dirs,cordoned_log_dirs}`. Map node_id↔broker_id, host/port/rack/endpoints↔end_points; **default** incarnation_id=nil, broker_epoch=0, features=[], fenced=false, in_controlled_shutdown=false, log_dirs=[], cordoned=None, is_migrating_zk_broker=false; drop on decode.
  - `V1TopicConfig{topic, overrides: BTreeMap}` → **N** `ConfigRecord{resource_type=2(TOPIC), resource_name=topic, name=k, value=Some(v)}` (one per key); `V1BrokerConfig{node_id, config_name, config_value}` → `ConfigRecord{resource_type=0(BROKER), resource_name=node_id.to_string(), name=config_name, value=config_value}`. On decode, route by `resource_type` back to `V1TopicConfig`/`V1BrokerConfig`. (NOTE: a `V1TopicConfig` map of N keys becomes N `ConfigRecord`s — the engine appends each as its own record value; `from_kraft` returns one `V1*Config` per `ConfigRecord` and the image merges. Crabka's `V1TopicConfig` is a whole-map upsert; emitting per-key `V1TopicConfig{topic, {k:v}}` singletons that the image merges is the round-trip-faithful shape — confirm `image.apply(V1TopicConfig)` merges rather than replaces; if it replaces, emit a merge-friendly form or adjust apply minimally.)
  - `V1DeleteTopic{name}` ↔ `RemoveTopicRecord{topic_id}` (name→id via image on encode; id→name on decode).
  - `V1UnregisterBroker{node_id}` ↔ `UnregisterBrokerRecord{broker_id}` (u64↔i32).
  - `V1ScramCredential{user,mechanism:SaslMechanism,salt,stored_key,server_key,iterations}` ↔ `UserScramCredentialRecord{name,mechanism:i16,salt,stored_key,server_key,iterations}`; `V1DeleteScramCredential{user,mechanism}` ↔ `RemoveUserScramCredentialRecord{name,mechanism:i16}`. Map `SaslMechanism↔i16` (SCRAM-SHA-256=1, SCRAM-SHA-512=2 — confirm Crabka's enum repr).
  - `V1AccessControlEntry(AclEntry)` ↔ `AccessControlEntryRecord{principal:Principal,resource_type:i8,resource_name,pattern_type:i8,operation:i8,permission:i8}`; `V1DeleteAccessControlEntry(AclEntryFilter)` ↔ `RemoveAccessControlEntryRecord{...}`. Map the Crabka enums (`ResourceType`/`PatternType`/`AclOperation`/`PermissionType`/`KafkaPrincipal`) to the i8/Principal encodings (confirm each enum's discriminant against Kafka's).
  - `V1ClientQuota{entity:Vec<QuotaEntity>,config_key,config_value:Option<f64>}` ↔ `ClientQuotaRecord{entity:Vec<ClientQuotaEntityData{entity_type,entity_name}>,key,value:Option<f64>}`.
  - `V1DelegationToken{token_id,owner:KafkaPrincipal,hmac:Vec<u8>,...timestamps...,renewers:Vec<KafkaPrincipal>}` ↔ `DelegationTokenRecord{token_id,principal:Principal,hmac:Bytes,...,renewers:Vec<Principal>}`; `V1DeleteDelegationToken{token_id}` ↔ `RemoveDelegationTokenRecord{token_id}`.
  - `V1FeatureLevel{name,level}` ↔ `FeatureLevelRecord{name,feature_level}` (aligned).
  - `V1KRaftVersion`, `V1Voters`: **not** KraftMetadataRecord (KIP-853 raft control records — out of dispatch). These two stay wincode in the log for now (the engine writes them as it does today, OR they're handled by the control/voter path). Treat them as a passthrough: `to_kraft`/`from_kraft` return an error or a reserved sentinel and the engine keeps the existing wincode path for just these two. (Confirm whether a static-bootstrap cluster even writes V1Voters/V1KRaftVersion to the metadata log via submit_change — if they only seed bootstrap records, the live submit path may never see them.)
- The principal encoding (`Principal`) and i8 enum discriminants must match Kafka; the round-trip test catches a self-inconsistency, and the JVM byte check (Task 5) catches a JVM-mismatch for the records it can provoke.

## File Structure

| Path | Change |
|------|--------|
| `crates/metadata/src/kraft_translate.rs` (new) | `to_kraft(&MetadataRecord,&MetadataImage)->Result<KraftMetadataRecord,_>` + `from_kraft(&KraftMetadataRecord,&MetadataImage)->Result<MetadataRecord,_>` + round-trip unit tests. |
| `crates/metadata/src/lib.rs` | `pub mod kraft_translate;` + re-export. |
| `crates/metadata/src/image.rs` | derive partition count from the partitions map (validate + any internal use). |
| `crates/raft/src/kraft/controller.rs` | `on_submit_change`/`advance_and_apply` use `to_kraft`/`from_kraft` + `KraftMetadataRecord::{encode_value,decode_value}` instead of the wincode bridge. |
| `crates/raft/src/snapshot.rs` | `SnapshotWriter`/`SnapshotReader` use the translation + KIP-631 envelope. |

---

## Task 1: Translation module — the aligned-shape variants

**Files:** create `crates/metadata/src/kraft_translate.rs`; modify `crates/metadata/src/lib.rs`.

- [ ] **Step 1: Write the round-trip test harness + failing tests** for the variants whose fields align cleanly (FeatureLevel, RegisterBroker, ScramCredential ±delete, AccessControlEntry ±delete, ClientQuota, DelegationToken ±delete, UnregisterBroker, BrokerConfig). For each: build a `MetadataRecord`, `to_kraft(&rec, &image)`, `from_kraft(&kraft, &image)`, assert it equals the original.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    fn img() -> MetadataImage { MetadataImage::new(uuid::Uuid::nil()) }

    #[test]
    fn feature_level_round_trips() {
        let rec = MetadataRecord::V1FeatureLevel(FeatureLevelRecord { name: "metadata.version".into(), level: 25 });
        let k = to_kraft(&rec, &img()).unwrap();
        assert_eq!(from_kraft(&k, &img()).unwrap(), rec);
    }
    // … one per aligned variant (broker registration, scram, acl, quota, token, unregister, broker config) …
}
```

- [ ] **Step 2: Run** → FAIL (`to_kraft`/`from_kraft` undefined). `cargo test -p crabka-metadata kraft_translate`.

- [ ] **Step 3: Implement `to_kraft`/`from_kraft`** for the aligned variants per the field-mismatch reference above (enum↔i8/i16, principal, Vec<u8>↔Bytes, defaults for KIP-631 extras on `RegisterBroker`). Signature:

```rust
use crabka_protocol::records::metadata::KraftMetadataRecord;
use crate::{MetadataRecord, MetadataImage};

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("record has no KIP-631 metadata counterpart: {0}")]
    NoCounterpart(&'static str),
    #[error("unknown topic id {0} on decode")]
    UnknownTopicId(uuid::Uuid),
}

pub fn to_kraft(rec: &MetadataRecord, image: &MetadataImage) -> Result<KraftMetadataRecord, TranslateError> { /* aligned arms; topic/partition in Task 2 */ }
pub fn from_kraft(rec: &KraftMetadataRecord, image: &MetadataImage) -> Result<MetadataRecord, TranslateError> { /* inverse */ }
```

(Leave `V1Topic`/`V1Partition`/`V1DeleteTopic`/`V1TopicConfig` and `V1Voters`/`V1KRaftVersion` to Task 2/their handling; for now those arms can `todo!()`-free return `Err(NoCounterpart(..))` so the aligned tests pass — Task 2 fills them.)

- [ ] **Step 4: Run** → aligned tests PASS.

- [ ] **Step 5: Commit** `feat(metadata): MetadataRecord<->KraftMetadataRecord translation (aligned variants)`.

---

## Task 2: Topic/Partition/Config translation + image partition-count derivation

**Files:** `crates/metadata/src/kraft_translate.rs`, `crates/metadata/src/image.rs`

- [ ] **Step 1: Write failing tests** — topic create + partition + remove-topic + topic-config round-trips that require image context:

```rust
#[test]
fn topic_partition_config_round_trip_with_image_context() {
    let mut image = img();
    let tid = uuid::Uuid::from_u128(7);
    let topic = MetadataRecord::V1Topic(TopicRecord { name: "t".into(), topic_id: tid, partitions: 1, replication_factor: 1 });
    image.apply(&topic);
    // partition references topic by name in Crabka; KIP-631 by topic_id
    let part = MetadataRecord::V1Partition(PartitionRecord { topic: "t".into(), partition: 0, leader: 1, replicas: vec![1], isr: vec![1], leader_epoch: 0, adding_replicas: vec![], removing_replicas: vec![] });
    image.apply(&part);
    // to_kraft uses image for name->id; from_kraft uses image for id->name
    for rec in [&topic, &part] {
        let k = to_kraft(rec, &image).unwrap();
        assert_eq!(from_kraft(&k, &image).unwrap(), *rec);
    }
    let cfg = MetadataRecord::V1TopicConfig(TopicConfigRecord { topic: "t".into(), overrides: [("retention.ms".to_string(), "9".to_string())].into() });
    let k = to_kraft(&cfg, &image).unwrap();
    assert_eq!(from_kraft(&k, &image).unwrap(), cfg);
}

#[test]
fn image_derives_partition_count_from_partitions_map() {
    let mut image = img();
    image.apply(&MetadataRecord::V1Topic(TopicRecord { name: "t".into(), topic_id: uuid::Uuid::from_u128(7), partitions: 3, replication_factor: 1 }));
    for p in 0..3 { image.apply(&MetadataRecord::V1Partition(PartitionRecord { topic: "t".into(), partition: p, leader: 1, replicas: vec![1], isr: vec![1], leader_epoch: 0, adding_replicas: vec![], removing_replicas: vec![] })); }
    // a NEW derive-from-map accessor returns 3, independent of TopicRecord.partitions
    assert_eq!(image.topic_partition_count("t"), 3);
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

- In `image.rs`: add `pub fn topic_partition_count(&self, topic: &str) -> i32` deriving from the `partitions` map, and change `validate(V1Topic)`'s "partition count grew" check to use it (so it no longer relies on the stored `TopicRecord.partitions`). Keep `TopicRecord.partitions` populated for existing getters (handlers still read it) — the derive is only for the round-trip + validate.
- In `kraft_translate.rs`: implement the `V1Topic`/`V1Partition`/`V1DeleteTopic`/`V1TopicConfig`/`V1BrokerConfig` arms. `to_kraft`: name→topic_id via `image.topic(name).topic_id`; on decode, topic_id→name by scanning the image's topics (or a topic_id→name index — add one if scan is unacceptable). For `V1Topic` decode, reconstruct `partitions`/`replication_factor` from the image's derived count / the partition records (or set `partitions` to the image-derived count and RF from the first partition's replica count — document the reconstruction).
- For `V1TopicConfig` (map) → N `ConfigRecord`s: since `to_kraft` returns a single `KraftMetadataRecord`, emit per-key. **Decision:** translate at the record-LIST level — add `to_kraft_records(rec, image) -> Vec<KraftMetadataRecord>` (a `V1TopicConfig` with N keys yields N `Config` records; all other variants yield 1). The engine (Task 3) calls `to_kraft_records` and appends each. `from_kraft` stays 1:1 (`Config{resource_type=2}` → `V1TopicConfig{topic, {name: value}}` singleton; the image merges). Confirm `image.apply(V1TopicConfig)` MERGES the overrides map (not replaces) so per-key singletons accumulate; if it replaces, change apply to merge (contained).

- [ ] **Step 4: Run** → PASS. Run all translate + image tests.

- [ ] **Step 5: Commit** `feat(metadata): topic/partition/config translation + derived partition count`.

---

## Task 3: Wire translation into the engine

**Files:** `crates/raft/src/kraft/controller.rs`

- [ ] **Step 1:** In `on_submit_change`, replace the `to_kafka_record` (wincode) encode with: for each `MetadataRecord`, `kraft_translate::to_kraft_records(rec, &scratch_image)` → for each `KraftMetadataRecord`, `encode_value(version)` → a `Record { key:None, value:Some(bytes) }` in the batch. (Use the same scratch image the pre-validate builds, so `topic_id` lookups see records earlier in the same submit.) For `V1Voters`/`V1KRaftVersion` (if they ever reach submit_change), keep the existing wincode encoding as a documented exception.
- [ ] **Step 2:** In `advance_and_apply`, replace `from_kafka_record` (wincode) with: `KraftMetadataRecord::decode_value(value)` → `kraft_translate::from_kraft(&kraft, &next_image)` → `next_image.apply(rec)`. (`next_image` is the in-progress image being built; topic records precede their partitions in the log, so id→name resolves.)
- [ ] **Step 3:** Run the engine sim + single-node: `cargo test -p crabka-raft kraft:: --test kraft_engine_sim` — green (behavior unchanged; only log bytes differ). A single-voter `submit_change(create topic)` still commits + appears in the image.
- [ ] **Step 4: Commit** `feat(raft): engine encodes/decodes the metadata log as KIP-631 records`.

---

## Task 4: Wire translation into snapshots

**Files:** `crates/raft/src/snapshot.rs`

- [ ] **Step 1:** `SnapshotWriter::serialize`: replace the wincode value encoding with `to_kraft_records(rec, image)` → `encode_value` for each record from `image.to_records()`. `SnapshotReader::read_records`: `decode_value` → `from_kraft(&kraft, &image_being_built)` → `MetadataRecord`. (Reader returns `Vec<MetadataRecord>` as today; the engine recovery applies them.)
- [ ] **Step 2:** Run `cargo test -p crabka-raft --test snapshot` (trigger + restart recovery) — green; the checkpoint is now KIP-631-framed and restart rebuilds the same image.
- [ ] **Step 3: Commit** `feat(raft): snapshot checkpoints serialize KIP-631 records`.

---

## Task 5: JVM byte check + capstone

- [ ] **Step 1 (Docker-gated):** Boot a single-node Crabka controller, create a topic + set a config, copy its `__cluster_metadata` log, and run JVM `kafka-dump-log --cluster-metadata-decoder` on it (extend `crates/broker/tests/kraft_checkpoint_jvm.rs` or a sibling). Assert it parses real records (TOPIC_RECORD, PARTITION_RECORD, REGISTER_BROKER_RECORD, FEATURE_LEVEL_RECORD, CONFIG_RECORD, NO_OP_RECORD) with `isvalid: true` and NO format errors. (Values are defaulted — assert structure/types, not e.g. a real incarnation id.)
- [ ] **Step 2: Regression** — `cargo test -p crabka-raft` (engine sim, single_node, snapshot, kraft unit), `cargo test -p crabka-metadata` (translate round-trips + image), and the broker multi-node suites (`quorum.rs`, `leader_election.rs`) green. `cargo clippy -p crabka-metadata -p crabka-raft --tests` clean; `cargo fmt --all --check` clean.
- [ ] **Step 3: Commit** the JVM test + any fmt.

---

## Self-Review Notes

- **Spec coverage:** translation module (aligned + topic/partition/config) → Tasks 1–2; image partition-count derivation → Task 2; engine wiring → Task 3; snapshot wiring → Task 4; JVM byte check + regression → Task 5. Image getters/handlers/`submit_change` unchanged (enforced by scope guard). `V1Voters`/`V1KRaftVersion` exception documented (Tasks 1/3).
- **Round-trip test is the correctness bar** for the per-variant field mappings (the enum/i8/principal encodings are specified by the field-mismatch reference; the test catches self-inconsistency, the Task-5 JVM check catches JVM-mismatch for provokable records). The `to_kraft_records` (list-level, for the V1TopicConfig→N split) vs `from_kraft` (1:1) asymmetry is defined in Task 2.
- **Type consistency:** `to_kraft`/`to_kraft_records`/`from_kraft`/`TranslateError`/`topic_partition_count` defined once and used consistently in Tasks 3–4.
- **Green tree:** Tasks 1–2 add a module + a contained image accessor (existing tests unaffected). Tasks 3–4 swap the engine/snapshot codec — behavior identical (image/handlers unchanged), only bytes differ, so the existing suites are the regression guard. No red window.
- **Honest fidelity:** defaulted KIP-631 fields are explicit; full fidelity is Slice 6 (Task 5 asserts structure, not real field values).
