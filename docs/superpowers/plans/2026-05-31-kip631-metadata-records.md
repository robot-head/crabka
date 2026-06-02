# KIP-631 Slice 1 — Metadata Record Layer + Bootstrap Checkpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A byte-exact KIP-631 metadata-record layer — real Kafka control records (generated from JSON schemas), the `ApiMessageAndVersion` value envelope, a decode dispatch, and a `bootstrap.checkpoint` builder — validated by byte-identical round-trip of real `apache/kafka:4.0.0`-produced bytes.

**Architecture:** Add Kafka's `common/metadata/*.json` record schemas (pinned to the same sha as the rest of Crabka's schema set, `a9ce3221…`) and let `protocol-codegen` emit the types. Add an envelope codec + dispatch enum + checkpoint builder in `crates/protocol/src/records/metadata/`. No change to the live wincode `MetadataRecord` enum, `MetadataImage`, or any broker handler — that migration is Slice 3.

**Tech Stack:** Rust, the existing `crabka-protocol-codegen` pipeline, `crabka_protocol::records::{RecordBatch, Attributes}`, uvarint/tagged-field primitives, Docker + `apache/kafka:4.0.0` for fixtures and the generation-parity test.

**Spec:** [docs/superpowers/specs/2026-05-31-kip631-metadata-records-design.md](../specs/2026-05-31-kip631-metadata-records-design.md)

---

## Background the implementer needs

- **Codegen is not run at build time.** `crates/protocol/build.rs` only *asserts* that the committed `crates/protocol/generated/` tree was produced against the `sha:` in `crates/protocol/schemas/VERSION` (currently `a9ce3221537b8653448750697915607dc7936cf3`, ref 4.3.0). To add schemas you drop `*.json` into `crates/protocol/schemas/`, run `tools/regenerate.sh`, and commit the regenerated `generated/` output. **Do NOT change `schemas/VERSION`** — keeping the sha means the regenerated files still embed `a9ce3221…` and `build.rs` stays green.
- **Schema grammar.** The codegen accepts `"type"` ∈ `{request,response,header,data}` (`crates/protocol-codegen/src/ir.rs:32`). Kafka's metadata schemas use `"type":"metadata"` with a top-level `"apiKey"`. Existing Crabka record schemas (`crates/protocol/schemas/VotersRecord.json`) were adapted to `"type":"data"` with **no** `apiKey`. Follow that precedent: each fetched schema gets `"type":"metadata"`→`"type":"data"` and its top-level `"apiKey"` line removed. (Record apiKeys live in our dispatch enum, Task 4 — they are a separate namespace from RPC apiKeys and must not enter the generated `ApiKey` enum.)
- **Generated type location.** A schema named `RegisterBrokerRecord` produces `crabka_protocol::owned::register_broker_record::RegisterBrokerRecord` (and a `borrowed` twin), implementing `crabka_protocol::{Encode, Decode}` with `(&mut buf, version)`. `regenerate.sh` auto-adds the `pub mod …;` lines to `src/owned/mod.rs` and `src/borrowed/mod.rs`.
- **Record-value envelope (`MetadataRecordSerde`).** A metadata record's *value* bytes are `frameVersion (uvarint=0) + apiKey (uvarint) + apiVersion (uvarint) + body@apiVersion`. Verified against the captured `FeatureLevelRecord` (1+1+1+20 = 23 bytes; see the wire-findings doc).
- **Control records** (`LeaderChange`, `SnapshotHeader/Footer`) are not value-enveloped: they sit in a batch with the control bit set, key = `version(i16)+type(i16)`, value = message body. `crates/raft/src/snapshot.rs:123` has an `encode_control_batch` today; this slice adds an independent helper in the protocol crate (snapshot.rs is unified later, Slice 4) to keep the raft crate untouched.
- **Primitives:** uvarint at `crabka_protocol::primitives::varint::{get_uvarint,put_uvarint,uvarint_len}`; `RecordBatch`/`Record`/`Attributes` at `crabka_protocol::records::*`; `Attributes::default().with_control(true)` marks a control batch.

## Record set & Kafka source paths (pinned sha `a9ce3221537b8653448750697915607dc7936cf3`)

Base URL: `https://raw.githubusercontent.com/apache/kafka/a9ce3221537b8653448750697915607dc7936cf3/`

| Record | apiKey | Kafka path |
|--------|--------|-----------|
| RegisterBrokerRecord | 0 | `metadata/src/main/resources/common/metadata/RegisterBrokerRecord.json` |
| TopicRecord | 1 | `metadata/src/main/resources/common/metadata/TopicRecord.json` |
| PartitionRecord | 2 | `metadata/src/main/resources/common/metadata/PartitionRecord.json` |
| DeleteTopicRecord | 3 | `metadata/src/main/resources/common/metadata/DeleteTopicRecord.json` |
| BeginTransactionRecord | 4 | `metadata/src/main/resources/common/metadata/BeginTransactionRecord.json` |
| EndTransactionRecord | 5 | `metadata/src/main/resources/common/metadata/EndTransactionRecord.json` |
| NoOpRecord | 6 | `metadata/src/main/resources/common/metadata/NoOpRecord.json` |
| RegisterControllerRecord | 7 | `metadata/src/main/resources/common/metadata/RegisterControllerRecord.json` |
| BrokerRegistrationChangeRecord | 8 | `metadata/src/main/resources/common/metadata/BrokerRegistrationChangeRecord.json` |
| FeatureLevelRecord | 12 | `metadata/src/main/resources/common/metadata/FeatureLevelRecord.json` |
| LeaderChangeMessage (control) | — | `raft/src/main/resources/common/message/LeaderChangeMessage.json` |

`SnapshotHeaderRecord`/`SnapshotFooterRecord` already exist in `crates/protocol/schemas/`.

## File Structure

| Path | Responsibility |
|------|----------------|
| `crates/protocol/schemas/<Record>.json` (×11) | Adapted Kafka record schemas (codegen input). |
| `crates/protocol/generated/*` | Regenerated codegen output (committed). |
| `crates/protocol/src/records/metadata/mod.rs` | Module root + re-exports. |
| `crates/protocol/src/records/metadata/envelope.rs` | `frameVersion+apiKey+apiVersion` value codec. |
| `crates/protocol/src/records/metadata/record.rs` | `KraftMetadataRecord` dispatch enum (incl. `Unknown`). |
| `crates/protocol/src/records/metadata/control.rs` | Control-record key + control-batch helper. |
| `crates/protocol/src/records/metadata/checkpoint.rs` | `bootstrap.checkpoint` builder. |
| `crates/protocol/tests/fixtures/*.bin` | Captured JVM bytes (checkpoint, live log, topic log). |
| `crates/protocol/tests/kraft_metadata_roundtrip.rs` | Byte-identical round-trip over fixtures. |
| `crates/broker/tests/kraft_checkpoint_jvm.rs` | Docker-gated `kafka-dump-log` generation-parity test. |

---

## Task 1: Add the metadata record schemas and regenerate

**Driven inline by the controller** (network fetch + `regenerate.sh`), not a subagent — it touches the generated tree and needs the network + a codegen run.

**Files:**
- Create: `crates/protocol/schemas/{RegisterBrokerRecord,TopicRecord,PartitionRecord,DeleteTopicRecord,BeginTransactionRecord,EndTransactionRecord,NoOpRecord,RegisterControllerRecord,BrokerRegistrationChangeRecord,FeatureLevelRecord}.json`, `crates/protocol/schemas/LeaderChangeMessage.json`
- Modify (regenerated): `crates/protocol/generated/*`, `crates/protocol/src/owned/mod.rs`, `crates/protocol/src/borrowed/mod.rs`

- [ ] **Step 1: Fetch + transform each schema**

For each metadata record (NOT LeaderChangeMessage), fetch and transform `"type":"metadata"`→`"data"` and drop the top-level `"apiKey"`:

```bash
cd /Users/mattstone/git/crabka/.claude/worktrees/jovial-beaver-54b766
SHA=a9ce3221537b8653448750697915607dc7936cf3
BASE="https://raw.githubusercontent.com/apache/kafka/$SHA"
META="$BASE/metadata/src/main/resources/common/metadata"
for r in RegisterBrokerRecord TopicRecord PartitionRecord DeleteTopicRecord \
         BeginTransactionRecord EndTransactionRecord NoOpRecord \
         RegisterControllerRecord BrokerRegistrationChangeRecord FeatureLevelRecord; do
  curl -s --max-time 30 "$META/$r.json" -o "/tmp/$r.raw.json"
  python3 - "$r" <<'PY'
import sys, re
r = sys.argv[1]
src = open(f"/tmp/{r}.raw.json").read()
# Preserve the Apache license header (the // comment block) verbatim; the codegen
# strips // line comments, and existing schemas keep the header.
# Transform the JSON body: type metadata->data, remove the top-level apiKey line.
out_lines = []
for line in src.splitlines():
    if re.match(r'\s*"apiKey"\s*:', line):
        continue  # drop top-level apiKey (record apiKeys live in our dispatch enum)
    line = re.sub(r'("type"\s*:\s*)"metadata"', r'\1"data"', line)
    out_lines.append(line)
open(f"crates/protocol/schemas/{r}.json", "w").write("\n".join(out_lines) + "\n")
print(f"wrote crates/protocol/schemas/{r}.json")
PY
done
# LeaderChangeMessage is already type:data with no apiKey — fetch verbatim.
curl -s --max-time 30 "$BASE/raft/src/main/resources/common/message/LeaderChangeMessage.json" \
  -o crates/protocol/schemas/LeaderChangeMessage.json
echo "fetched LeaderChangeMessage.json"
```

If the network is unreachable, hand-transcribe each schema from the Kafka source at that sha (same content); the round-trip test (Task 7) guards correctness.

- [ ] **Step 2: Sanity-check the transform**

Run: `grep -l '"type": "data"' crates/protocol/schemas/RegisterBrokerRecord.json && ! grep -q '"apiKey"' crates/protocol/schemas/RegisterBrokerRecord.json && echo OK`
Expected: `OK` (data type, no apiKey). Spot-check `LeaderChangeMessage.json` parses: `python3 -c "import json,re; json.load(open('/dev/stdin'))" < <(sed 's://.*::' crates/protocol/schemas/LeaderChangeMessage.json)`.

- [ ] **Step 3: Regenerate**

Run: `tools/regenerate.sh`
Expected: completes; `git status` shows new `crates/protocol/generated/RegisterBrokerRecord.owned.rs` etc. and new `pub mod …;` lines in `src/owned/mod.rs` / `src/borrowed/mod.rs`. If the codegen errors on an unrecognized field attribute, extend `crates/protocol-codegen/src/ir.rs` minimally to parse it (it already handles `mapKey`, `entityType`, tagged versions) and note it as a concern.

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p crabka-protocol`
Expected: success (build.rs sha assertion passes because `schemas/VERSION` is unchanged).

- [ ] **Step 5: Confirm a generated type round-trips at the right version**

Run: `cargo test -p crabka-protocol register_broker 2>/dev/null; echo "(no test yet — just confirm the type exists)"; grep -rl "pub struct RegisterBrokerRecord" crates/protocol/generated/`
Expected: `crates/protocol/generated/RegisterBrokerRecord.owned.rs` listed.

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/schemas crates/protocol/generated crates/protocol/src/owned/mod.rs crates/protocol/src/borrowed/mod.rs
git commit -m "feat(protocol): generate KIP-631 metadata record types from kafka schemas"
```

---

## Task 2: Module scaffold for the metadata record layer

**Files:**
- Create: `crates/protocol/src/records/metadata/mod.rs`
- Modify: `crates/protocol/src/records/mod.rs` (add `pub mod metadata;`)

- [ ] **Step 1: Find the records module declaration**

Run: `grep -n "pub mod\|mod " crates/protocol/src/records/mod.rs | head`
Expected: existing submodule decls (e.g. `owned`, `header`, `crc`). You will add `pub mod metadata;` alongside them.

- [ ] **Step 2: Create the module root**

Create `crates/protocol/src/records/metadata/mod.rs`:

```rust
//! KIP-631 metadata record layer: the `ApiMessageAndVersion` value envelope,
//! a decode dispatch over the generated record types, control-record framing,
//! and a `bootstrap.checkpoint` builder. Byte-compatible with apache/kafka 4.x
//! KRaft. This is a permanent foundation (unlike the Slice 0 `kraft-spike`).

pub mod checkpoint;
pub mod control;
pub mod envelope;
pub mod record;

pub use envelope::{decode_value_header, encode_value, EnvelopeError};
pub use record::KraftMetadataRecord;
```

- [ ] **Step 3: Wire into the records module**

Add to `crates/protocol/src/records/mod.rs` (next to the other `pub mod` lines):

```rust
pub mod metadata;
```

- [ ] **Step 4: Create empty leaf files so it compiles**

Create `crates/protocol/src/records/metadata/envelope.rs`, `control.rs`, `record.rs`, `checkpoint.rs` each containing only a module doc comment line (`//! placeholder — filled in subsequent tasks.`). Then Tasks 3–6 replace them.

- [ ] **Step 5: Verify**

Run: `cargo build -p crabka-protocol`
Expected: success. (The `pub use` in mod.rs will fail until Task 3/4 add the symbols — so for THIS task, comment out the two `pub use` lines and add them in Task 4 Step 4. Leave a `// re-exports added in Task 4` note.)

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/src/records/metadata crates/protocol/src/records/mod.rs
git commit -m "feat(protocol): scaffold records::metadata module"
```

---

## Task 3: The record-value envelope codec

**Files:**
- Modify: `crates/protocol/src/records/metadata/envelope.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/protocol/src/records/metadata/envelope.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn frame_zero_apikey_apiversion_roundtrip() {
        // FeatureLevelRecord apiKey=12 apiVersion=0, body bytes b"\x01\x02".
        let body: &[u8] = &[0x01, 0x02];
        let value = encode_value(12, 0, body);
        // frameVersion(0)=1 byte, apiKey(12)=1 byte, apiVersion(0)=1 byte, +body.
        assert!(value.len() == 3 + body.len());
        let mut cur: &[u8] = &value;
        let hdr = decode_value_header(&mut cur).expect("decode header");
        assert!(hdr.frame_version == 0);
        assert!(hdr.api_key == 12);
        assert!(hdr.api_version == 0);
        assert!(cur == body);
    }

    #[test]
    fn truncated_value_errors() {
        let mut cur: &[u8] = &[]; // no frameVersion byte
        assert!(decode_value_header(&mut cur).is_err());
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-protocol frame_zero_apikey -- --nocapture`
Expected: FAIL — `encode_value`/`decode_value_header` not defined.

- [ ] **Step 3: Implement the envelope**

Replace the placeholder in `crates/protocol/src/records/metadata/envelope.rs`:

```rust
//! The KRaft metadata record-value envelope (`MetadataRecordSerde` /
//! `ApiMessageAndVersion`): a record value is
//! `frameVersion (uvarint, 0) + apiKey (uvarint) + apiVersion (uvarint) + body`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};

/// Current KRaft metadata frame version (Kafka writes 0).
pub const FRAME_VERSION: u32 = 0;

/// Decoded envelope header (everything before the message body).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueHeader {
    pub frame_version: u32,
    pub api_key: u32,
    pub api_version: u32,
}

/// Error decoding a metadata record value envelope.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("truncated metadata record value envelope")]
    Truncated,
}

/// Encode a record value: envelope header + the already-encoded `body` bytes.
#[must_use]
pub fn encode_value(api_key: u32, api_version: u32, body: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(
        uvarint_len(FRAME_VERSION) + uvarint_len(api_key) + uvarint_len(api_version) + body.len(),
    );
    put_uvarint(&mut out, FRAME_VERSION);
    put_uvarint(&mut out, api_key);
    put_uvarint(&mut out, api_version);
    out.put_slice(body);
    out.freeze()
}

/// Decode the envelope header, leaving `buf` positioned at the message body.
///
/// # Errors
/// Returns [`EnvelopeError::Truncated`] if any varint cannot be read.
pub fn decode_value_header<B: Buf>(buf: &mut B) -> Result<ValueHeader, EnvelopeError> {
    let frame_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    let api_key = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    let api_version = get_uvarint(buf).map_err(|_| EnvelopeError::Truncated)?;
    Ok(ValueHeader { frame_version, api_key, api_version })
}
```

(Confirm `get_uvarint` returns `Result<u32, ProtocolError>` and `put_uvarint(&mut B, u32)` / `uvarint_len(u32) -> usize` signatures against `crates/protocol/src/primitives/varint.rs`; adjust the `u32` types if the crate uses `i64`/`u64` there. Confirm `thiserror` is a dependency of `crates/protocol` — it is used elsewhere; if not, return a plain enum + manual `Display`/`Error`.)

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test -p crabka-protocol envelope::tests -- --nocapture`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/records/metadata/envelope.rs
git commit -m "feat(protocol): KRaft metadata record-value envelope codec"
```

---

## Task 4: The `KraftMetadataRecord` dispatch enum

**Files:**
- Modify: `crates/protocol/src/records/metadata/record.rs`
- Modify: `crates/protocol/src/records/metadata/mod.rs` (uncomment the `pub use`)

- [ ] **Step 1: Write the failing test**

Append to `crates/protocol/src/records/metadata/record.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn feature_level_record_value_roundtrips_through_dispatch() {
        use crate::owned::feature_level_record::FeatureLevelRecord;
        let rec = KraftMetadataRecord::FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_string(),
            feature_level: 25,
            ..Default::default()
        });
        let value = rec.encode_value().expect("encode");
        let decoded = KraftMetadataRecord::decode_value(&value).expect("decode");
        assert!(decoded == rec);
    }

    #[test]
    fn unknown_api_key_decodes_to_unknown_arm() {
        use crate::records::metadata::envelope::encode_value;
        // apiKey 99 is not modeled.
        let value = encode_value(99, 0, &[0xAB, 0xCD]);
        let decoded = KraftMetadataRecord::decode_value(&value).expect("decode");
        match decoded {
            KraftMetadataRecord::Unknown { api_key, api_version, body } => {
                assert!(api_key == 99);
                assert!(api_version == 0);
                assert!(body.as_ref() == &[0xAB, 0xCD]);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }
}
```

(Confirm the generated field names: read `crates/protocol/generated/FeatureLevelRecord.owned.rs` for the exact field idents — likely `name: String`, `feature_level: i16`, `unknown_tagged_fields`. Adjust the literal above to match.)

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-protocol record::tests -- --nocapture`
Expected: FAIL — `KraftMetadataRecord` not defined.

- [ ] **Step 3: Implement the dispatch enum**

Replace the placeholder in `crates/protocol/src/records/metadata/record.rs`:

```rust
//! Dispatch enum over the generated KIP-631 record types, keyed by the KRaft
//! metadata record apiKey (a namespace distinct from RPC apiKeys). Encodes and
//! decodes through the value envelope. Unknown apiKeys decode to `Unknown` so a
//! forward-compatible reader never chokes.

use bytes::Bytes;

use crate::owned::begin_transaction_record::BeginTransactionRecord;
use crate::owned::broker_registration_change_record::BrokerRegistrationChangeRecord;
use crate::owned::delete_topic_record::DeleteTopicRecord;
use crate::owned::end_transaction_record::EndTransactionRecord;
use crate::owned::feature_level_record::FeatureLevelRecord;
use crate::owned::no_op_record::NoOpRecord;
use crate::owned::partition_record::PartitionRecord;
use crate::owned::register_broker_record::RegisterBrokerRecord;
use crate::owned::register_controller_record::RegisterControllerRecord;
use crate::owned::topic_record::TopicRecord;
use crate::records::metadata::envelope::{decode_value_header, encode_value};
use crate::{Decode, Encode, ProtocolError};

/// A single KRaft metadata record (the value of one Kafka `Record`).
#[derive(Debug, Clone, PartialEq)]
pub enum KraftMetadataRecord {
    RegisterBroker(RegisterBrokerRecord),       // apiKey 0
    Topic(TopicRecord),                         // apiKey 1
    Partition(PartitionRecord),                 // apiKey 2
    DeleteTopic(DeleteTopicRecord),             // apiKey 3
    BeginTransaction(BeginTransactionRecord),   // apiKey 4
    EndTransaction(EndTransactionRecord),       // apiKey 5
    NoOp(NoOpRecord),                           // apiKey 6
    RegisterController(RegisterControllerRecord), // apiKey 7
    BrokerRegistrationChange(BrokerRegistrationChangeRecord), // apiKey 8
    FeatureLevel(FeatureLevelRecord),           // apiKey 12
    /// A record this build does not model. Body is the post-envelope bytes.
    Unknown { api_key: u32, api_version: u32, body: Bytes },
}

impl KraftMetadataRecord {
    /// The (apiKey, apiVersion) this variant encodes as. The apiVersion is the
    /// record's max supported version (Slice 1 always encodes at the highest
    /// version; readers honor whatever the envelope declares).
    fn key_and_version(&self) -> (u32, i16) {
        match self {
            Self::RegisterBroker(_) => (0, 3),
            Self::Topic(_) => (1, 0),
            Self::Partition(_) => (2, 0),
            Self::DeleteTopic(_) => (3, 0),
            Self::BeginTransaction(_) => (4, 0),
            Self::EndTransaction(_) => (5, 0),
            Self::NoOp(_) => (6, 0),
            Self::RegisterController(_) => (7, 0),
            Self::BrokerRegistrationChange(_) => (8, 0),
            Self::FeatureLevel(_) => (12, 0),
            Self::Unknown { api_key, api_version, .. } => (*api_key, *api_version as i16),
        }
    }

    /// Encode this record to its value bytes (envelope + body).
    ///
    /// # Errors
    /// Propagates a [`ProtocolError`] from the underlying message encoder.
    pub fn encode_value(&self) -> Result<Bytes, ProtocolError> {
        let (api_key, version) = self.key_and_version();
        let mut body = bytes::BytesMut::new();
        match self {
            Self::RegisterBroker(r) => r.encode(&mut body, version)?,
            Self::Topic(r) => r.encode(&mut body, version)?,
            Self::Partition(r) => r.encode(&mut body, version)?,
            Self::DeleteTopic(r) => r.encode(&mut body, version)?,
            Self::BeginTransaction(r) => r.encode(&mut body, version)?,
            Self::EndTransaction(r) => r.encode(&mut body, version)?,
            Self::NoOp(r) => r.encode(&mut body, version)?,
            Self::RegisterController(r) => r.encode(&mut body, version)?,
            Self::BrokerRegistrationChange(r) => r.encode(&mut body, version)?,
            Self::FeatureLevel(r) => r.encode(&mut body, version)?,
            Self::Unknown { body: raw, .. } => {
                return Ok(encode_value(api_key, version as u32, raw));
            }
        }
        Ok(encode_value(api_key, version as u32, &body))
    }

    /// Decode one record from its value bytes.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the envelope or body cannot be decoded.
    pub fn decode_value(value: &[u8]) -> Result<Self, ProtocolError> {
        let mut cur: &[u8] = value;
        let hdr = decode_value_header(&mut cur)
            .map_err(|_| ProtocolError::SchemaMismatch("metadata record envelope"))?;
        let v = hdr.api_version as i16;
        let rec = match hdr.api_key {
            0 => Self::RegisterBroker(RegisterBrokerRecord::decode(&mut cur, v)?),
            1 => Self::Topic(TopicRecord::decode(&mut cur, v)?),
            2 => Self::Partition(PartitionRecord::decode(&mut cur, v)?),
            3 => Self::DeleteTopic(DeleteTopicRecord::decode(&mut cur, v)?),
            4 => Self::BeginTransaction(BeginTransactionRecord::decode(&mut cur, v)?),
            5 => Self::EndTransaction(EndTransactionRecord::decode(&mut cur, v)?),
            6 => Self::NoOp(NoOpRecord::decode(&mut cur, v)?),
            7 => Self::RegisterController(RegisterControllerRecord::decode(&mut cur, v)?),
            8 => Self::BrokerRegistrationChange(BrokerRegistrationChangeRecord::decode(&mut cur, v)?),
            12 => Self::FeatureLevel(FeatureLevelRecord::decode(&mut cur, v)?),
            other => Self::Unknown {
                api_key: other,
                api_version: hdr.api_version,
                body: Bytes::copy_from_slice(cur),
            },
        };
        Ok(rec)
    }
}
```

(Confirm `ProtocolError` variant names — use whatever the crate exposes for a schema/parse error, e.g. `ProtocolError::SchemaMismatch(&'static str)` seen in generated decoders. Confirm the generated module paths via `crates/protocol/src/owned/mod.rs`.)

- [ ] **Step 4: Add the re-exports in mod.rs**

In `crates/protocol/src/records/metadata/mod.rs`, ensure these are present (uncomment from Task 2):

```rust
pub use envelope::{decode_value_header, encode_value, EnvelopeError};
pub use record::KraftMetadataRecord;
```

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `cargo test -p crabka-protocol record::tests -- --nocapture`
Expected: PASS (both).

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/src/records/metadata
git commit -m "feat(protocol): KraftMetadataRecord dispatch enum over generated records"
```

---

## Task 5: Control-record framing helper

**Files:**
- Modify: `crates/protocol/src/records/metadata/control.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/protocol/src/records/metadata/control.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Buf;

    #[test]
    fn control_key_is_version_then_type() {
        let key = control_record_key(ControlRecordType::SnapshotHeader);
        let mut cur: &[u8] = &key;
        assert!(cur.get_i16() == 0); // version
        assert!(cur.get_i16() == 3); // SnapshotHeader type
    }

    #[test]
    fn control_batch_sets_control_bit() {
        let key = control_record_key(ControlRecordType::LeaderChange);
        let batch = encode_control_batch(0, key, bytes::Bytes::from_static(b"\x00\x00"));
        // magic byte at offset 16, attributes i16 at offset 21..23; control bit = 0x20.
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        assert!(attrs & 0x20 != 0);
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-protocol control::tests -- --nocapture`
Expected: FAIL — items not defined.

- [ ] **Step 3: Implement the control framing**

Replace the placeholder in `crates/protocol/src/records/metadata/control.rs`:

```rust
//! KRaft control-record framing. Control records (LeaderChange, SnapshotHeader,
//! SnapshotFooter) live in a batch with the control bit set; the record key is
//! `version (i16) + type (i16)` and the value is the message body.

use bytes::{BufMut, Bytes, BytesMut};

use crate::records::{Attributes, Record, RecordBatch};

/// KRaft control record types (the i16 written after the i16 version in the key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ControlRecordType {
    LeaderChange = 2,
    SnapshotHeader = 3,
    SnapshotFooter = 4,
}

/// Control record key version (Kafka writes 0).
const CONTROL_KEY_VERSION: i16 = 0;

/// Build a control record key: `version(i16) + type(i16)`.
#[must_use]
pub fn control_record_key(ty: ControlRecordType) -> Bytes {
    let mut key = BytesMut::with_capacity(4);
    key.put_i16(CONTROL_KEY_VERSION);
    key.put_i16(ty as i16);
    key.freeze()
}

/// Encode a single-record control batch at `base_offset` with the control bit
/// set, returning the full v2 `RecordBatch` bytes (CRC computed by the encoder).
#[must_use]
pub fn encode_control_batch(base_offset: i64, key: Bytes, value: Bytes) -> Bytes {
    let batch = RecordBatch {
        base_offset,
        attributes: Attributes::default().with_control(true),
        records: vec![Record {
            key: Some(key),
            value: Some(value),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut out = BytesMut::new();
    batch
        .encode(&mut out)
        .expect("control batch encodes (no compression, in-range)");
    out.freeze()
}
```

(Confirm `RecordBatch`/`Record`/`Attributes` field names and `RecordBatch::encode(&self, &mut B)` against `crates/protocol/src/records/owned.rs` and `header.rs`; mirror `crates/raft/src/snapshot.rs:123` `encode_control_batch`.)

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test -p crabka-protocol control::tests -- --nocapture`
Expected: PASS (both).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/records/metadata/control.rs
git commit -m "feat(protocol): KRaft control-record framing helper"
```

---

## Task 6: Bootstrap-checkpoint builder

**Files:**
- Modify: `crates/protocol/src/records/metadata/checkpoint.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/protocol/src/records/metadata/checkpoint.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crate::records::RecordBatch;

    #[test]
    fn bootstrap_checkpoint_has_header_features_footer() {
        let bytes = build_bootstrap_checkpoint(&[
            ("metadata.version", 25),
            ("group.version", 1),
            ("transaction.version", 2),
        ]);
        // Walk batches: control header (offset 0), data batch (offsets 1..=3),
        // control footer.
        let mut cur: &[u8] = &bytes;
        let header = RecordBatch::decode(&mut cur).expect("header batch");
        assert!(header.base_offset == 0);
        assert!(header.attributes.is_control_batch());
        let data = RecordBatch::decode(&mut cur).expect("data batch");
        assert!(data.base_offset == 1);
        assert!(!data.attributes.is_control_batch());
        assert!(data.records.len() == 3);
        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        assert!(footer.attributes.is_control_batch());
        assert!(cur.is_empty());
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-protocol checkpoint::tests -- --nocapture`
Expected: FAIL — `build_bootstrap_checkpoint` not defined.

- [ ] **Step 3: Implement the builder**

Replace the placeholder in `crates/protocol/src/records/metadata/checkpoint.rs`:

```rust
//! Builds a KRaft `bootstrap.checkpoint`: a SnapshotHeader control batch, a data
//! batch of FeatureLevelRecords, and a SnapshotFooter control batch — matching
//! what `kafka-storage format` writes (minus per-run timestamps).

use bytes::{BufMut, Bytes, BytesMut};

use crate::owned::feature_level_record::FeatureLevelRecord;
use crate::owned::snapshot_footer_record::SnapshotFooterRecord;
use crate::owned::snapshot_header_record::SnapshotHeaderRecord;
use crate::records::metadata::control::{control_record_key, encode_control_batch, ControlRecordType};
use crate::records::metadata::record::KraftMetadataRecord;
use crate::records::{Record, RecordBatch};
use crate::Encode;

/// Build a `bootstrap.checkpoint` from an ordered list of `(feature_name, level)`.
#[must_use]
pub fn build_bootstrap_checkpoint(features: &[(&str, i16)]) -> Bytes {
    let mut out = BytesMut::new();

    // (1) SnapshotHeader control batch at offset 0.
    let header = SnapshotHeaderRecord { version: 0, last_contained_log_timestamp: 0, ..Default::default() };
    let mut header_body = BytesMut::new();
    header.encode(&mut header_body, 0).expect("snapshot header encodes");
    out.put_slice(&encode_control_batch(
        0,
        control_record_key(ControlRecordType::SnapshotHeader),
        header_body.freeze(),
    ));

    // (2) Data batch of FeatureLevelRecords at offset 1.
    let records: Vec<Record> = features
        .iter()
        .enumerate()
        .map(|(i, (name, level))| {
            let rec = KraftMetadataRecord::FeatureLevel(FeatureLevelRecord {
                name: (*name).to_string(),
                feature_level: *level,
                ..Default::default()
            });
            Record {
                offset_delta: i32::try_from(i).expect("few features"),
                value: Some(rec.encode_value().expect("feature record encodes")),
                ..Default::default()
            }
        })
        .collect();
    let data = RecordBatch {
        base_offset: 1,
        last_offset_delta: i32::try_from(features.len().saturating_sub(1)).unwrap_or(0),
        records,
        ..Default::default()
    };
    data.encode(&mut out).expect("feature data batch encodes");

    // (3) SnapshotFooter control batch.
    let footer_offset = 1 + features.len() as i64;
    let footer = SnapshotFooterRecord { version: 0, ..Default::default() };
    let mut footer_body = BytesMut::new();
    footer.encode(&mut footer_body, 0).expect("snapshot footer encodes");
    out.put_slice(&encode_control_batch(
        footer_offset,
        control_record_key(ControlRecordType::SnapshotFooter),
        footer_body.freeze(),
    ));

    out.freeze()
}
```

(Confirm `SnapshotHeaderRecord`/`SnapshotFooterRecord` field names from their generated files — `last_contained_log_timestamp` may be `last_contained_log_timestamp`. Confirm `RecordBatch`/`Record` field names.)

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test -p crabka-protocol checkpoint::tests -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/records/metadata/checkpoint.rs
git commit -m "feat(protocol): bootstrap.checkpoint builder"
```

---

## Task 7: Round-trip byte-identity over real JVM fixtures

**Capture driven inline by the controller** (Docker), then the test is a normal committed integration test.

**Files:**
- Create: `crates/protocol/tests/fixtures/{bootstrap_checkpoint.bin,startup_log.bin,topic_log.bin}`
- Create: `crates/protocol/tests/kraft_metadata_roundtrip.rs`

- [ ] **Step 1: Capture the fixtures (controller, Docker)**

Boot a `apache/kafka:4.0.0` node, `kafka-topics --create` a topic, then copy out the `bootstrap.checkpoint` and the `__cluster_metadata-0/*.log` (full, and a topic-bearing version). Cut each `.log` at a clean batch boundary (reuse the Slice 0 batch-walker that stops at a target offset). Save the byte slices to `crates/protocol/tests/fixtures/`. (See `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md` for the exact docker invocation and the batch-walker.)

- [ ] **Step 2: Write the round-trip test**

Create `crates/protocol/tests/kraft_metadata_roundtrip.rs`:

```rust
//! Byte-identity: decode every record/batch in a real apache/kafka:4.0.0
//! metadata log + bootstrap.checkpoint through the generated types + envelope,
//! re-encode, and assert the bytes are unchanged.
use assert2::assert;
use bytes::{Bytes, BytesMut};
use crabka_protocol::records::metadata::record::KraftMetadataRecord;
use crabka_protocol::records::RecordBatch;

/// Decode each non-control record's value via KraftMetadataRecord, re-encode,
/// and assert byte-identity. Control batches (LeaderChange/Snapshot*) are
/// asserted to re-encode identically as whole batches.
fn assert_log_roundtrips(log: &[u8]) {
    let mut cur: &[u8] = log;
    while !cur.is_empty() {
        let before_len = cur.len();
        let batch = RecordBatch::decode(&mut cur).expect("batch decodes");
        let consumed = before_len - cur.len();
        let batch_bytes = &log[log.len() - before_len..log.len() - before_len + consumed];
        // Re-encode the whole batch and assert byte-identity.
        let mut re = BytesMut::new();
        batch.encode(&mut re).expect("batch re-encodes");
        assert!(re.as_ref() == batch_bytes, "batch at base_offset {} not byte-identical", batch.base_offset);
        // For non-control batches, also round-trip each record value through the
        // metadata dispatch enum.
        if !batch.attributes.is_control_batch() {
            for rec in &batch.records {
                if let Some(value) = &rec.value {
                    let decoded = KraftMetadataRecord::decode_value(value).expect("record value decodes");
                    let reenc = decoded.encode_value().expect("record re-encodes");
                    assert!(reenc.as_ref() == value.as_ref(), "record value not byte-identical");
                }
            }
        }
    }
}

#[test]
fn bootstrap_checkpoint_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/bootstrap_checkpoint.bin"));
}

#[test]
fn startup_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/startup_log.bin"));
}

#[test]
fn topic_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/topic_log.bin"));
}
```

(Note: the `batch_bytes` slice math assumes `cur` is a subslice of `log`; if `RecordBatch::decode` consumes from a `&[u8]` cursor that stays a subslice, this holds. If not, capture `let start = log.len() - before_len;` before decode and slice `&log[start..start+consumed]`. Adjust to the real `Buf` behavior. The control-batch values for `LeaderChange`/`Snapshot*` are validated by the whole-batch byte-identity assertion; they are not run through `KraftMetadataRecord` since they are control records, not value-enveloped.)

- [ ] **Step 3: Run the round-trip tests**

Run: `cargo test -p crabka-protocol --test kraft_metadata_roundtrip -- --nocapture`
Expected: PASS. If a record value is NOT byte-identical, the generated type's field order/version gating differs from Kafka — inspect the diff, fix the schema/codegen, regenerate. This is the slice's core validation loop.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol/tests/fixtures crates/protocol/tests/kraft_metadata_roundtrip.rs
git commit -m "test(protocol): byte-identity round-trip of real kafka:4.0.0 metadata logs"
```

---

## Task 8: Docker-gated generation-parity test

**Files:**
- Create: `crates/broker/tests/kraft_checkpoint_jvm.rs`

- [ ] **Step 1: Write the test**

Create `crates/broker/tests/kraft_checkpoint_jvm.rs`:

```rust
//! Docker-gated: a Crabka-built bootstrap.checkpoint is parsed cleanly by the
//! JVM `kafka-dump-log --cluster-metadata-decoder`.
//!
//! cargo test -p crabka-broker --test kraft_checkpoint_jvm -- --ignored --nocapture
use std::io::Write;
use std::process::Command;

use assert2::assert;
use crabka_protocol::records::metadata::checkpoint::build_bootstrap_checkpoint;

const KAFKA_IMAGE: &str = "apache/kafka:4.0.0";

#[test]
#[ignore = "requires Docker"]
fn jvm_dump_log_parses_crabka_bootstrap_checkpoint() {
    let bytes = build_bootstrap_checkpoint(&[
        ("metadata.version", 25),
        ("group.version", 1),
        ("transaction.version", 2),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bootstrap.checkpoint");
    std::fs::File::create(&path).unwrap().write_all(&bytes).unwrap();

    let out = Command::new("docker")
        .args([
            "run", "--rm", "-v",
            &format!("{}:/work", dir.path().display()),
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-dump-log.sh",
            "--cluster-metadata-decoder",
            "--files", "/work/bootstrap.checkpoint",
        ])
        .output()
        .expect("docker run kafka-dump-log");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{text}");
    assert!(out.status.success(), "kafka-dump-log failed: {text}");
    assert!(text.contains("SnapshotHeader"), "missing SnapshotHeader: {text}");
    assert!(text.contains("FEATURE_LEVEL_RECORD"), "missing feature records: {text}");
    assert!(text.contains("metadata.version"), "missing metadata.version feature: {text}");
    assert!(text.contains("SnapshotFooter"), "missing SnapshotFooter: {text}");
    assert!(!text.contains("isvalid: false"), "a batch failed CRC validation: {text}");
}
```

(Confirm `tempfile` is a dev-dependency of `crates/broker` — `jvm_acceptance.rs` uses it. Confirm `crabka_protocol::records::metadata::checkpoint` is reachable from the broker crate — it depends on `crabka-protocol`.)

- [ ] **Step 2: Run it (controller, Docker)**

Run: `cargo test -p crabka-broker --test kraft_checkpoint_jvm -- --ignored --nocapture`
Expected: PASS — the JVM dumps SnapshotHeader + 3 feature records + SnapshotFooter, all `isvalid: true`. If a batch shows `isvalid: false`, the CRC/framing is wrong — debug against the round-trip fixtures.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/kraft_checkpoint_jvm.rs
git commit -m "test(broker): JVM kafka-dump-log parses Crabka bootstrap.checkpoint"
```

---

## Task 9: Capstone — fmt, clippy, regression

**Files:** none (verification only)

- [ ] **Step 1: Format + lint**

Run: `cargo fmt --all && cargo fmt --all --check && cargo clippy -p crabka-protocol --tests`
Expected: fmt clean; clippy clean (the generated tree carries its own `#![allow]` header; the hand-written `records::metadata` module must be clippy-clean).

- [ ] **Step 2: Full protocol + metadata + raft regression (nothing migrated, so all green)**

Run: `cargo test -p crabka-protocol && cargo test -p crabka-metadata && cargo test -p crabka-raft`
Expected: all pass — Slice 1 added a new module and generated types; it touched no existing code paths.

- [ ] **Step 3: Confirm build.rs sha assertion still green**

Run: `cargo build -p crabka-protocol`
Expected: success (schemas/VERSION unchanged → embedded sha matches).

- [ ] **Step 4: Commit (if fmt/clippy made changes)**

```bash
git add -A && git commit -m "chore(protocol): fmt + clippy for kip-631 metadata records" || echo "nothing to commit"
```

---

## Self-Review Notes

- **Spec coverage:** generated record schemas → Task 1; envelope codec → Task 3; dispatch enum + Unknown arm → Task 4; control framing → Task 5; bootstrap checkpoint builder → Task 6; round-trip byte-identity acceptance → Task 7; generation-parity Docker test → Task 8; unit tests per component → Tasks 3–6; no live-path change → enforced by scope (Tasks touch only new files + the generated tree). All spec sections covered.
- **Generated-type field names** (e.g. `feature_level` vs `featureLevel`, `last_contained_log_timestamp`) and exact `ProtocolError`/`RecordBatch`/`Attributes` signatures are intentionally to-be-confirmed against the freshly generated files in Tasks 3–6 — each such step says to read the generated file and adjust. These are not placeholders: the structure is fixed; only idents are codegen-determined.
- **Type consistency:** `KraftMetadataRecord`, `encode_value`/`decode_value`, `decode_value_header`/`ValueHeader`, `control_record_key`/`encode_control_batch`/`ControlRecordType`, `build_bootstrap_checkpoint` are defined once (Tasks 3–6) and referenced consistently in Tasks 6–8.
- **Inline vs subagent:** Tasks 1, 7-capture, and 8-run involve network/Docker and are controller-driven; Tasks 2–6 and the test bodies are subagent-friendly. Tasks share `records/metadata/*` so they run sequentially, not in parallel.
