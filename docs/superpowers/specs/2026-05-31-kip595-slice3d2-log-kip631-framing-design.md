# KIP-595 Slice 3d-2 — KIP-631-framed log + snapshot (translation boundary)

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 3d migrates the metadata path toward the real KIP-631 records. 3d-1
generated the record schemas + extended the `KraftMetadataRecord` dispatch to
cover all of Crabka's value-record equivalents.

Exploration of 3d-2 surfaced a **semantic gap**: KIP-631 records carry fields
Crabka's `MetadataImage`/handlers do not model and key differently —
`RegisterBrokerRecord` (`incarnation_id`/`broker_epoch`/`features`/`fenced`/
`in_controlled_shutdown`/`log_dirs`), `PartitionRecord` (keyed by `topic_id`,
plus `partition_epoch`/`directories`/ELR), `TopicRecord` (just `{name,
topic_id}` — no partition count / replication factor), `ConfigRecord`
(one-per-key with a `resourceType`), `RemoveTopicRecord` (by `topic_id`). So a
*semantically faithful* KIP-631 log can't exist without re-modeling the image —
a large effort that genuinely belongs to **Slice 6** (mixed JVM+Crabka quorum),
where fidelity can be validated against a real JVM peer.

**Decision (locked):** 3d-2 produces **defaulted KIP-631 framing** — the log and
snapshots become genuinely KIP-631-framed (real apiKeys / `ApiMessageAndVersion`
envelope / field layout), with the fields Crabka doesn't track filled by safe
defaults on encode and dropped on decode. This makes the Crabka-only log
byte-clean and round-trip-faithful for what the image actually uses, without a
premature re-model. Full field fidelity is Slice 6.

This reframes 3d: with a translation boundary, the wincode `MetadataRecord` enum
**stays** as the image/handler internal currency. The earlier 3d-3 (handler
migration) and 3d-4 (delete the enum) are **dropped** — they presupposed full
handler migration, which the fidelity decision supersedes. **3d = 3d-1 (done) +
3d-2 (this).**

## Goal & scope

Translate `MetadataRecord ↔ KraftMetadataRecord` at the engine submit/apply +
snapshot boundary so the on-log/on-checkpoint bytes are KIP-631-framed.

**In scope:** a translation module; one contained `MetadataImage` adjustment
(derive partition count from the partitions map); wiring the translation into
the engine + snapshot; round-trip + JVM byte tests.

**Out of scope (Slice 6 / not pursued):** full KRaft-field fidelity (broker
incarnation/epoch/fenced/log_dirs, partition_epoch/ELR), migrating broker
handlers to construct `KraftMetadataRecord`, deleting the wincode enum,
`submit_change` signature change. The `MetadataImage` getters, the broker
handlers, and `submit_change(Vec<MetadataRecord>)` are **unchanged**.

## Components

### Translation module (`crates/metadata/src/kraft_translate.rs`)

- `to_kraft(rec: &MetadataRecord, image: &MetadataImage) -> KraftMetadataRecord`
  — resolves `topic_id` from the image for `V1Partition`/`V1DeleteTopic`
  (KIP-631 keys by `topic_id`); fills KIP-631-only fields with defaults
  (`incarnation_id`=nil `Uuid`, `broker_epoch`=0, `fenced`=false,
  `in_controlled_shutdown`=false, `log_dirs`=[], `partition_epoch`=0,
  `directories`=[], ELR=None); splits a `V1TopicConfig` map into N
  `ConfigRecord`s (`resourceType`=topic) and a `V1BrokerConfig` into a
  `ConfigRecord` (`resourceType`=broker); maps Crabka enums (`SaslMechanism`,
  `KafkaPrincipal`, `ResourceType`/`PatternType`/`AclOperation`/`PermissionType`)
  to the KIP-631 i8/i16/Principal encodings.
- `from_kraft(rec: &KraftMetadataRecord) -> MetadataRecord` — the inverse: drops
  the defaulted extras, maps `topic_id`→name (via the image at apply), merges
  `ConfigRecord`s back into the Crabka map shape, reverses the enum encodings.
- A `KraftMetadataRecord::Unknown` arm decodes to a passthrough — but for the
  records 3d-1 models there should be no Unknowns on the Crabka-only path.

### The one `MetadataImage` adjustment

KIP-631 `TopicRecord` has no partition count / replication factor; Crabka's
`V1Topic` carries them and `validate()` uses them (the "duplicate topic allowed
only if partition count strictly grows" rule). Make the image **derive the
partition count from its `partitions` map** rather than from `TopicRecord`
fields, so the round-trip is lossless for what the image consumes. This is a
contained change to `validate()` + any internal use — **not** the broker/ELR
re-model (deferred to Slice 6). Image getter return types are unchanged → no
broker-handler churn.

### Engine + snapshot wiring (`crates/raft/src/kraft/controller.rs`, `snapshot.rs`)

- `on_submit_change`: replace `crabka_metadata::to_kafka_record` (wincode) with
  `to_kraft(rec, &image)` → `KraftMetadataRecord::encode_value(version)` → the
  log `RecordBatch`.
- `advance_and_apply`: decode the record value via
  `KraftMetadataRecord::decode_value` → `from_kraft` → `image.apply` (image
  internals unchanged).
- `SnapshotWriter`/`SnapshotReader`: same swap — `image.to_records()` →
  `to_kraft` → KIP-631 checkpoint; read → `decode_value` → `from_kraft`.

## Data flow

```
handler → Vec<MetadataRecord> → submit_change (unchanged)
   on_submit_change: to_kraft(&image) → KraftMetadataRecord::encode_value → KraftLog batch (KIP-631)
   advance_and_apply: decode_value → from_kraft → MetadataImage::apply (unchanged getters)
   snapshot: to_records() → to_kraft → KIP-631 checkpoint ; read → decode_value → from_kraft
```

## Error handling

`to_kraft` is total for the modeled variants (defaults fill the gaps). A
`from_kraft` of an `Unknown` apiKey, or a record that can't map back (shouldn't
happen on the Crabka-only path), returns a typed `RaftError`/`KafkaRecordError`
rather than panicking. `validate` semantics are preserved (the partition-count
rule now reads the derived count).

## Acceptance / testing

- **Translation round-trip unit tests** (in `crates/metadata`): for every
  `MetadataRecord` variant, build a record → `to_kraft(&image)` → `encode_value`
  → `decode_value` → `from_kraft` → assert equals the original (modulo
  defaulted-and-dropped KIP-631 extras, which the image never reads).
- **Live-path regression (the contract):** the engine sim
  (`kraft_engine_sim.rs`), `single_node.rs`, `snapshot.rs`, and the broker
  multi-node suites (`quorum.rs`, etc.) stay green — the image/handlers are
  unchanged, so behavior is identical; only the on-log bytes change.
- **JVM byte check (Docker-gated):** a Crabka-produced metadata log /
  `bootstrap.checkpoint` now decodes in `kafka-dump-log --cluster-metadata-decoder`
  as real KIP-631 records (FeatureLevel, RegisterBroker, RegisterController,
  Topic, Partition, NoOp) with `isvalid: true`. Field *values* are defaulted
  (incarnation/epoch/etc.); Slice 6 fills them.

## Disposition

Permanent. After 3d-2 the Crabka metadata log/snapshots are KIP-631-framed; the
wincode enum remains the internal currency. Slice 4 = KIP-630 snapshots/
FetchSnapshot, Slice 5 = KIP-853 dynamic voters, Slice 6 = full KRaft-field
fidelity + the mixed JVM+Crabka quorum acceptance.
