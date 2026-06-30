# KIP-631 Slice 1 — metadata record layer + bootstrap checkpoint

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

This is Slice 1 of the KIP-595 → true-wire-compatibility program (target: a
mixed `mirror.gcr.io/apache/kafka:4.0.0` JVM + Crabka joint metadata quorum). See
`2026-05-30-kip595-slice0-jvm-fetch-spike-design.md` for the program overview
and `2026-05-30-kraft-wire-findings.md` for the captured wire facts.

Today Crabka's metadata records are a **wincode-serialized `MetadataRecord`
enum** (`crates/metadata/src/records.rs`) — Crabka-private, not Kafka-compatible.
For a mixed quorum, a JVM controller must parse the records a Crabka leader
appends (and vice versa), so the records must be the genuine KIP-631 control
records, byte-for-byte.

The protocol codegen pipeline (`crates/protocol-codegen/`) already parses Kafka
`*.json` message schemas and emits byte-exact `Encode`/`Decode` types using the
correct primitives (compact strings, uvarint, tagged fields, version gating). It
already generates `VotersRecord`, `KRaftVersionRecord`, `SnapshotHeaderRecord`,
`SnapshotFooterRecord`. The core KIP-631 record schemas are simply absent.

## Goal & scope

Build a **byte-exact KIP-631 metadata-record layer + a bootstrap-checkpoint
builder**, validated against `mirror.gcr.io/apache/kafka:4.0.0` JVM tools.

**In scope:**
- Generate the KIP-631 record types from Kafka's `common/metadata/*.json`
  schemas (fetched verbatim from apache/kafka at the 4.0.0 tag).
- A record-value **envelope codec** (`ApiMessageAndVersion` /
  `MetadataRecordSerde`) and a decode **dispatch enum**.
- A **bootstrap-checkpoint builder** matching `kafka-storage format` output.

**Explicitly NOT in scope (deferred to the state-machine slice, Slice 3):**
- Removing or replacing the wincode `MetadataRecord` enum.
- Migrating `MetadataImage::apply()` or any broker handler.
- Changing the live snapshot writer / log write path.

This keeps Slice 1 a bounded, independently-testable foundation with zero
regression risk to the running broker.

### Record set

The records in a fresh `kafka:4.0.0` format + single-node-startup log, plus
basic topic lifecycle:

- `FeatureLevelRecord` (apiKey 12)
- `RegisterControllerRecord` (apiKey 7)
- `RegisterBrokerRecord` (apiKey 0, v0–3)
- `BrokerRegistrationChangeRecord` (apiKey 8)
- `NoOpRecord` (apiKey 6)
- `TopicRecord` (apiKey 1)
- `PartitionRecord` (apiKey 2)
- `DeleteTopicRecord` (apiKey 3)
- control bodies: `LeaderChangeMessage` (control type 2),
  `BeginTransactionRecord` (apiKey 4), `EndTransactionRecord` (apiKey 5)
- already present: `SnapshotHeaderRecord` (control type 3),
  `SnapshotFooterRecord` (control type 4)

Configs, ACLs, client quotas, SCRAM, delegation tokens, `DeleteRecordsRecord`,
etc. are deferred — not exercised until much later slices.

## Components

### 1. Record schemas (generated)

Add the Kafka `common/metadata/*.json` schemas for the record set to
`crates/protocol/schemas/`, then run the existing codegen
(`tools/regenerate.sh`) to emit `<Name>.owned.rs` / `<Name>.borrowed.rs` into
`crates/protocol/generated/`. These are `"type": "data"` messages with an
`apiKey`, the same grammar the codegen already handles for `VotersRecord`.

### 2. Record-value envelope codec + dispatch

A new module in the protocol crate (`crates/protocol/src/records/metadata*`):

- **Envelope:** a record *value* is
  `frameVersion (uvarint, currently 0) + apiKey (uvarint) + apiVersion (uvarint)
  + body@apiVersion`. Verified against the captured `FeatureLevelRecord`
  (1+1+1+20 = 23 bytes). Functions: encode `(apiKey, apiVersion, &impl Encode)`
  → value `Bytes`; decode value bytes → `(frameVersion, apiKey, apiVersion,
  remaining body)`.
- **Dispatch enum** (`KraftMetadataRecord`): one variant per generated record
  type, decoded by `apiKey`, plus an `Unknown { api_key, api_version, body:
  Bytes }` arm so a forward-compatible reader does not choke on a record it does
  not model. `encode_value`/`decode_value` round-trip through the envelope.
- **Control-record framing:** control records live in a batch with the control
  bit set; the record key is `version (i16) + type (i16)` (`LeaderChange`=2,
  `SnapshotHeader`=3, `SnapshotFooter`=4) and the value is the message body.
  Reuse / extend the control-batch encoding already in
  `crates/raft/src/snapshot.rs` (move the shared helper into the protocol crate
  if cleaner).

### 3. Bootstrap-checkpoint builder

Assembles a `bootstrap.checkpoint` matching `kafka-storage format` output:

- `SnapshotHeader` control batch at base offset 0
  (`SnapshotHeaderRecord {version:0, lastContainedLogTimestamp:0}`),
- a data batch of `FeatureLevelRecord`s (`metadata.version`=25,
  `group.version`=1, `transaction.version`=2),
- `SnapshotFooter` control batch (`SnapshotFooterRecord {version:0}`).

The checkpoint does **not** wrap the feature records in a transaction — the
`BeginTransaction`/`EndTransaction` wrapper only appears in the live log at
runtime (confirmed from the Slice 0 capture).

## Data flow & home

```
Kafka *.json (4.0.0) ──codegen──▶ generated record types (Encode/Decode @version)
                                          │
record value bytes ◀── envelope codec ◀───┤  (frameVersion+apiKey+apiVer+body)
                                          │
bootstrap.checkpoint ◀── checkpoint builder (control batches + feature data batch)
```

Envelope codec, dispatch enum, and checkpoint builder live in the **protocol
crate**, alongside `RecordBatch` and the generated types, avoiding a
metadata→protocol dependency tangle. Nothing in `crates/metadata` or the broker
changes.

## Error handling

- Decode of an out-of-range `apiVersion` for a known record → typed
  `ProtocolError` (no panic), consistent with the existing codecs.
- Decode of an unknown `apiKey` → the `Unknown { api_key, api_version, body }`
  dispatch arm (forward-compatible), mirroring unknown-tagged-field handling.
- A malformed envelope (truncated varints) → typed `ProtocolError`.

## Testing / acceptance

- **Round-trip byte-identity (primary, deterministic).** Capture real
  JVM-produced bytes via Docker (as in Slice 0): a `bootstrap.checkpoint`, a
  fresh single-node live log, and a log after `kafka-topics --create` (for
  `Topic`/`Partition`). Decode every record through the generated types +
  envelope, re-encode, and assert **byte-identical** output. No timestamp
  nondeterminism because input bytes are preserved. Embed the captured fixtures
  via `include_bytes!`.
- **Generation parity (Docker-gated).** Build a `bootstrap.checkpoint` from
  scratch and assert `kafka-dump-log --cluster-metadata-decoder` parses it with
  the expected record types/values and no errors. Gated behind the existing
  Docker `#[ignore]` convention.
- **Unit tests.** Per record type: encode→decode round-trip. Envelope codec:
  frameVersion/apiKey/apiVersion round-trip, unknown-apiKey dispatch, truncation
  errors.

## Risks / notes

- **Network dependency:** fetching the exact 4.0.0 schemas needs access to the
  apache/kafka GitHub raw files. If unavailable, hand-transcribe the handful of
  schemas from the KIP — the round-trip test catches any transcription error.
- **Codegen grammar gaps:** a metadata schema may use a field type or attribute
  the codegen has not yet seen. If so, extend the codegen minimally (it already
  handles the `data` message type and tagged fields). Flag as a concern if the
  gap is large.
- **No live-path change:** the wincode `MetadataRecord` enum and the broker
  remain untouched, so there is no regression surface this slice.

## Disposition

Unlike the Slice 0 spike, this code is **permanent** — the generated records and
envelope codec are the foundation Slices 2–3 build on. The Slice 0 `kraft-spike`
feature remains throwaway and is unaffected.
