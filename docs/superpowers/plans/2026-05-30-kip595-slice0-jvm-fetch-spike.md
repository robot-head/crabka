# KIP-595 Slice 0 — JVM-Fetch Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a live `apache/kafka:4.0.0` broker observer can `Fetch` and byte-decode the `__cluster_metadata-0` log from a single-node Crabka controller speaking real KRaft wire — decode-only, no registration.

**Architecture:** A feature-gated (`kraft-spike`) throwaway responder in `crates/raft`, intercepting `ApiVersions` (18) and `Fetch` (1) on the controller listener parallel to the untouched openraft path. It serves a hand-built bootstrap metadata log. The kept deliverable is a verified wire-format findings doc.

**Tech Stack:** Rust, tokio, `crabka-protocol` generated Kafka codecs (`FetchResponse`, `ApiVersionsResponse`, `RecordBatch` v2 + CRC-32C), Docker CLI via `std::process::Command`, `apache/kafka:4.0.0`.

**Spec:** [docs/superpowers/specs/2026-05-30-kip595-slice0-jvm-fetch-spike-design.md](../specs/2026-05-30-kip595-slice0-jvm-fetch-spike-design.md)

---

## File Structure

| Path | Responsibility | Disposition |
|------|----------------|-------------|
| `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md` | **Kept deliverable.** Captured wire facts + surprises. | Permanent |
| `crates/raft/Cargo.toml` | Add `kraft-spike` feature. | Permanent (1 line) |
| `crates/raft/src/kraft_spike.rs` | Throwaway: bootstrap-log builder + ApiVersions + Fetch responders. | Throwaway |
| `crates/raft/src/server.rs` | Intercept api_key 1 & 18 under `#[cfg(feature = "kraft-spike")]`. | Throwaway hook |
| `crates/raft/src/lib.rs` | `#[cfg(feature)] mod kraft_spike;` | Throwaway hook |
| `crates/broker/Cargo.toml` | `kraft-spike` feature forwarding to `crabka-raft/kraft-spike`. | Permanent (1 line) |
| `crates/broker/tests/kraft_spike_jvm.rs` | Docker-gated acceptance test. | Throwaway |

**Wire-facts table** (filled by Task 1, referenced by later tasks as named constants):

| Constant | Meaning | Filled in Task 1 |
|----------|---------|------------------|
| `FETCH_REQ_VERSION` | Fetch api_version the kafka:4.0.0 observer sends for metadata | ____ |
| `APIVERSIONS_REQ_VERSION` | ApiVersions version the observer sends | ____ |
| `CLUSTER_METADATA_TOPIC_ID` | 16-byte topic UUID for `__cluster_metadata` | ____ |
| `METADATA_VERSION_LEVEL` | FeatureLevelRecord `metadata.version` level in bootstrap | ____ |
| `BOOTSTRAP_RECORDS` | Ordered record (apiKey, version, key, value) list in the bootstrap checkpoint | ____ |
| `REQUIRED_API_KEYS` | (api_key, min, max) the observer needs advertised before it will Fetch | ____ |

---

## Task 1: Capture ground-truth JVM KRaft wire

**Goal:** Stand up a pure-JVM KRaft cluster and capture the real controller↔broker `ApiVersions` + `Fetch` metadata exchange, then populate the wire-facts table. No Crabka code yet.

**Files:**
- Create: `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md`

- [ ] **Step 1: Write the capture harness script (inline, run by hand)**

This is an investigation, not a unit test. Run a 1-controller + 1-broker JVM cluster on one Docker network and capture the broker→controller traffic with a TCP tee container. Use a combined-mode single node first (simplest), then separate roles only if needed.

```bash
# Run from repo root. Captures the metadata-fetch handshake to /tmp/kraft-cap/.
mkdir -p /tmp/kraft-cap && cd /tmp/kraft-cap
docker network create kraftcap || true

# 1) A combined controller+broker JVM node, formatted with a known cluster id.
CLUSTER_ID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
echo "CLUSTER_ID=$CLUSTER_ID" | tee cluster-id.txt

docker run -d --name jvm-kraft --network kraftcap \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://jvm-kraft:9092 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@jvm-kraft:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e CLUSTER_ID="$CLUSTER_ID" \
  apache/kafka:4.0.0
sleep 15

# 2) Dump the formatted metadata log + bootstrap checkpoint in human-readable form.
docker exec jvm-kraft /opt/kafka/bin/kafka-metadata-shell.sh \
  --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/*.checkpoint 2>/dev/null || true
docker exec jvm-kraft sh -c \
  '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
     --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log' \
  | tee dump-log.txt
docker exec jvm-kraft sh -c \
  'ls -la /var/lib/kafka/data/__cluster_metadata-0/' | tee meta-dir.txt
# bootstrap.checkpoint lives in the data dir; dump it too:
docker exec jvm-kraft sh -c \
  '/opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
     --files /var/lib/kafka/data/__cluster_metadata-0/bootstrap.checkpoint' \
  2>/dev/null | tee dump-bootstrap.txt || true
```

- [ ] **Step 2: Capture the over-the-wire Fetch via a TCP tee**

Insert a tee proxy between a *separate* JVM broker and the controller so we capture raw bytes. Use `tcpdump` inside a sidecar sharing the controller's net namespace (simplest reliable capture on Linux/Docker Desktop).

```bash
# Sidecar tcpdump sharing the JVM node's network namespace; capture controller port 9093.
docker run -d --name capds --network "container:jvm-kraft" \
  nicolaka/netshoot tcpdump -i any -w /tmp/cap.pcap 'tcp port 9093'
# Bounce the broker role so it re-fetches metadata from offset 0 (capture the cold-start Fetch).
docker restart jvm-kraft
sleep 20
docker cp capds:/tmp/cap.pcap ./kraft-9093.pcap
docker stop capds && docker rm capds
# Inspect in your tool of choice (tshark/wireshark). Kafka dissector decodes api_key/version.
tshark -r kraft-9093.pcap -O kafka 2>/dev/null | tee tshark-kafka.txt | head -200
```

- [ ] **Step 3: Populate the findings doc from the captures**

Create `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md` with the concrete values. Template:

```markdown
# KRaft wire findings (apache/kafka:4.0.0) — Slice 0 capture

Date: 2026-05-30
Source: pure-JVM 1-node KRaft cluster; pcap on controller :9093; kafka-dump-log.

## Negotiated versions (observer → controller)
- ApiVersions request version: <APIVERSIONS_REQ_VERSION>
- Fetch request version: <FETCH_REQ_VERSION>
- Response header version used for Fetch: <0 or 1>
- Response header version used for ApiVersions: 0 (Kafka special-case — confirm)

## ApiVersions the observer requires before it will Fetch
| api_key | min | max | notes |
|---------|-----|-----|-------|
| ...     | ... | ... | (from pcap ApiVersions response the JVM controller sent) |

## __cluster_metadata
- topic id (UUID): <CLUSTER_METADATA_TOPIC_ID>  (hex bytes: ...)
- partition: 0

## Bootstrap checkpoint / initial log records (in order)
| offset | apiKey | version | record type | key bytes | value bytes |
|--------|--------|---------|-------------|-----------|-------------|
| 0 | ... | ... | FeatureLevelRecord(metadata.version=<METADATA_VERSION_LEVEL>) | ... | ... |
| ... |

## First Fetch request the observer sends (decoded)
- replica_id / replica_state.replica_id: ...
- fetch_offset: 0, current_leader_epoch: ...
- topic_id present? per-partition last_fetched_epoch: ...

## First Fetch response the leader sends (decoded)
- high_watermark, last_stable_offset, log_start_offset
- current_leader{leader_id, leader_epoch}: ...
- records: the bootstrap batch (base_offset=0, magic=2, is_control_batch?)

## Surprises / undocumented behavior
- ...
```

Fill every `<...>` from the captures. **Stop and record the actual bytes** — the rest of the plan depends on these values.

- [ ] **Step 4: Commit the findings**

```bash
git add docs/superpowers/specs/2026-05-30-kraft-wire-findings.md
git commit -m "docs(kip-595): capture kafka:4.0.0 KRaft metadata-fetch wire facts"
```

---

## Task 2: Add the `kraft-spike` feature and module scaffold

**Files:**
- Modify: `crates/raft/Cargo.toml`
- Modify: `crates/broker/Cargo.toml`
- Create: `crates/raft/src/kraft_spike.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: Add the feature to the raft crate**

In `crates/raft/Cargo.toml`, add a `[features]` section (none exists today):

```toml
[features]
# Throwaway KIP-595 slice-0 spike: serves real KRaft ApiVersions+Fetch on the
# controller listener so a JVM broker observer can fetch the metadata log.
# Not wired into the openraft path; gated out of default builds.
kraft-spike = []
```

- [ ] **Step 2: Forward the feature from the broker crate**

In `crates/broker/Cargo.toml` under `[features]`, add:

```toml
kraft-spike = ["crabka-raft/kraft-spike"]
```

(The dependency is named `crabka-raft`; confirm the exact key in `[dependencies]` and match it.)

- [ ] **Step 3: Create the module scaffold**

Create `crates/raft/src/kraft_spike.rs`:

```rust
//! KIP-595 Slice 0 spike (THROWAWAY). Serves real KRaft `ApiVersions` (18)
//! and `Fetch` (1) for `__cluster_metadata-0` on the controller listener so a
//! live JVM broker observer can fetch and decode the metadata log. Decode-only:
//! no registration, no election, no writes. Deleted once findings are captured.
//!
//! All concrete wire values come from the Task 1 capture, recorded in
//! docs/superpowers/specs/2026-05-30-kraft-wire-findings.md.

use bytes::{Bytes, BytesMut};

/// Fetch api_version the kafka:4.0.0 observer sends (Task 1 finding).
pub(crate) const FETCH_REQ_VERSION: i16 = 0; // TODO Task 1: set to captured value
/// `__cluster_metadata` topic id (Task 1 finding), 16 bytes.
pub(crate) const CLUSTER_METADATA_TOPIC_ID: [u8; 16] = [0u8; 16]; // TODO Task 1
/// FeatureLevelRecord metadata.version level in the bootstrap (Task 1 finding).
pub(crate) const METADATA_VERSION_LEVEL: i16 = 0; // TODO Task 1

/// The single-voter leader's node id and epoch for this frozen spike.
pub(crate) const SPIKE_LEADER_ID: i32 = 1;
pub(crate) const SPIKE_LEADER_EPOCH: i32 = 1;
```

(These constants are placeholders **by design** — Task 5/6 fill them from Task 1's recorded findings and the iteration loop. Leaving them at 0 compiles; the acceptance test in Task 7 drives them to correct values.)

- [ ] **Step 4: Wire the module into lib.rs**

In `crates/raft/src/lib.rs`, alongside the other `mod` declarations, add:

```rust
#[cfg(feature = "kraft-spike")]
mod kraft_spike;
```

- [ ] **Step 5: Verify it compiles both ways**

Run: `cargo build -p crabka-raft && cargo build -p crabka-raft --features kraft-spike`
Expected: both succeed (the module is empty-ish but valid).

- [ ] **Step 6: Commit**

```bash
git add crates/raft/Cargo.toml crates/broker/Cargo.toml crates/raft/src/kraft_spike.rs crates/raft/src/lib.rs
git commit -m "feat(raft): scaffold kraft-spike feature for KIP-595 slice 0"
```

---

## Task 3: Build the bootstrap metadata log batch

**Goal:** Produce the byte-exact `__cluster_metadata-0` record batch (base_offset 0) containing the bootstrap records, using the real `RecordBatch` v2 encoder. Unit-test the CRC/round-trip in isolation.

**Files:**
- Modify: `crates/raft/src/kraft_spike.rs`
- Test: inline `#[cfg(test)]` in `crates/raft/src/kraft_spike.rs`

- [ ] **Step 1: Write the failing test for batch encoding**

Add to `crates/raft/src/kraft_spike.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::records::owned::RecordBatch;

    #[test]
    fn bootstrap_batch_decodes_and_crc_matches() {
        let bytes = bootstrap_log_batch();
        // The batch must re-decode cleanly (validates magic=2, CRC-32C, framing).
        let mut cur: &[u8] = &bytes;
        let batch = RecordBatch::decode(&mut cur)
            .expect("bootstrap batch must decode");
        assert_eq!(batch.base_offset, 0);
        assert_eq!(batch.magic_is_v2(), true);
        assert!(!batch.records.is_empty(), "bootstrap must contain >=1 record");
        assert!(cur.is_empty(), "no trailing bytes");
    }
}
```

(Confirm the exact `RecordBatch::decode` signature from `crates/protocol/src/records/owned.rs`; the explorer reported `encode(&self, buf)`. If `decode` takes no version, drop the version arg. If `magic_is_v2` doesn't exist, assert on the encoded magic byte at offset 16 instead: `assert_eq!(bytes[16], 2);`.)

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-raft --features kraft-spike bootstrap_batch_decodes -- --nocapture`
Expected: FAIL — `bootstrap_log_batch` not defined.

- [ ] **Step 3: Implement `bootstrap_log_batch`**

Add to `crates/raft/src/kraft_spike.rs`. This encodes the FeatureLevelRecord (and any other Task-1 bootstrap records) as KRaft metadata records (key/value = the ApiMessage frames captured in Task 1), wrapped in a v2 batch:

```rust
use crabka_protocol::records::owned::{Record, RecordBatch};
use crabka_protocol::records::Attributes;

/// One bootstrap metadata record: KRaft frames each as
/// key = `[0x00, 0x00]` (frame version 0) ; value = `[frame_version, apiKey(varint?), version, payload...]`.
/// The EXACT key/value bytes come from the Task 1 `dump-log` / pcap. Paste the
/// captured byte arrays here (per BOOTSTRAP_RECORDS in the findings doc).
fn bootstrap_records() -> Vec<Record> {
    // TODO Task 1: replace with the captured (key, value) byte arrays, in order.
    // Example shape for the FeatureLevelRecord (apiKey 12) setting metadata.version:
    let feature_level_value: &[u8] = &[/* captured value bytes */];
    let feature_level_key: &[u8] = &[/* captured key bytes (may be empty/None) */];
    vec![Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: (!feature_level_key.is_empty()).then(|| Bytes::copy_from_slice(feature_level_key)),
        value: Some(Bytes::copy_from_slice(feature_level_value)),
        headers: Vec::new(),
    }]
}

/// Encode the bootstrap batch at base_offset 0. KRaft metadata batches are NOT
/// control batches (the FeatureLevelRecord etc. are data records in the metadata
/// log); confirm `is_control_batch` from the Task 1 dump and set attributes to match.
pub(crate) fn bootstrap_log_batch() -> Bytes {
    let records = bootstrap_records();
    let last_offset_delta = (records.len() as i32) - 1;
    let batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: SPIKE_LEADER_EPOCH,
        attributes: Attributes(0), // confirm control bit from Task 1 dump
        last_offset_delta,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    };
    let mut buf = BytesMut::new();
    batch.encode(&mut buf).expect("encode bootstrap batch");
    buf.freeze()
}
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p crabka-raft --features kraft-spike bootstrap_batch_decodes -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft_spike.rs
git commit -m "feat(raft): kraft-spike bootstrap metadata log batch"
```

---

## Task 4: Implement the ApiVersions responder

**Goal:** Build a real flexible `ApiVersionsResponse` advertising the keys the observer needs (`REQUIRED_API_KEYS` from Task 1), encoded with the Kafka response-header-v0 special case.

**Files:**
- Modify: `crates/raft/src/kraft_spike.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/raft/src/kraft_spike.rs`:

```rust
#[test]
fn api_versions_response_advertises_fetch_and_apiversions() {
    use crabka_protocol::codec::Decode;
    use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
    let frame = api_versions_response_frame(/*correlation_id*/ 7, /*req_version*/ 4);
    // Skip the 4-byte length prefix + 4-byte correlation id, then the response
    // header tagged-fields byte is ABSENT for ApiVersions (v0 header special case).
    let mut cur: &[u8] = &frame[4..]; // after length prefix
    let corr = i32::from_be_bytes(cur[..4].try_into().unwrap());
    assert_eq!(corr, 7);
    cur = &cur[4..]; // ApiVersions response header is v0: no tagged-fields byte
    let resp = ApiVersionsResponse::decode(&mut cur, 4).expect("decode");
    assert_eq!(resp.error_code, 0);
    assert!(resp.api_keys.iter().any(|k| k.api_key == 1));  // Fetch
    assert!(resp.api_keys.iter().any(|k| k.api_key == 18)); // ApiVersions
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-raft --features kraft-spike api_versions_response_advertises -- --nocapture`
Expected: FAIL — `api_versions_response_frame` not defined.

- [ ] **Step 3: Implement the responder**

Add to `crates/raft/src/kraft_spike.rs`:

```rust
use crabka_protocol::codec::Encode;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};

/// (api_key, min, max) entries the kafka:4.0.0 observer requires before it will
/// Fetch. Seed with Fetch + ApiVersions; widen from the Task 1 pcap if the JVM
/// refuses to proceed (each addition is a finding).
fn required_api_keys() -> Vec<ApiVersion> {
    let mk = |k: i16, lo: i16, hi: i16| ApiVersion {
        api_key: k, min_version: lo, max_version: hi, ..Default::default()
    };
    vec![
        mk(18, 0, 4),               // ApiVersions
        mk(1, 4, FETCH_REQ_VERSION),// Fetch — max must cover the observer's version
        // TODO Task 1: add any others the observer requires (e.g. 60 DescribeQuorum?).
    ]
}

/// Full framed ApiVersions response: 4-byte length prefix + ResponseHeader v0
/// (correlation id only, NO tagged-fields byte — Kafka special-cases the
/// ApiVersions response header at v0 regardless of request flexibility) + body.
pub(crate) fn api_versions_response_frame(correlation_id: i32, req_version: i16) -> Bytes {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: required_api_keys(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    resp.encode(&mut body, req_version).expect("encode api_versions");

    let mut out = BytesMut::new();
    let len = 4 + body.len(); // correlation id + body
    out.extend_from_slice(&(len as i32).to_be_bytes());
    out.extend_from_slice(&correlation_id.to_be_bytes());
    out.extend_from_slice(&body);
    out.freeze()
}
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p crabka-raft --features kraft-spike api_versions_response_advertises -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft_spike.rs
git commit -m "feat(raft): kraft-spike ApiVersions responder"
```

---

## Task 5: Implement the KRaft Fetch responder

**Goal:** Decode the observer's `Fetch`, and frame a `FetchResponse` for `__cluster_metadata-0` carrying the bootstrap batch + `current_leader` + `high_watermark`, with the correct flexible response header.

**Files:**
- Modify: `crates/raft/src/kraft_spike.rs`

- [ ] **Step 1: Write the failing test (round-trip the framed response)**

Add to the `tests` module:

```rust
#[test]
fn fetch_response_carries_metadata_and_leader() {
    use crabka_protocol::codec::Decode;
    use crabka_protocol::owned::fetch_response::FetchResponse;
    // fetch_offset 0 → return the bootstrap batch.
    let frame = fetch_response_frame(/*correlation_id*/ 9, FETCH_REQ_VERSION, /*fetch_offset*/ 0);
    let mut cur: &[u8] = &frame[4..]; // skip length prefix
    let corr = i32::from_be_bytes(cur[..4].try_into().unwrap());
    assert_eq!(corr, 9);
    cur = &cur[4..];
    // Flexible (v12+) response header carries a tagged-fields byte (0x00) here.
    if FETCH_REQ_VERSION >= 12 { cur = &cur[1..]; }
    let resp = FetchResponse::decode(&mut cur, FETCH_REQ_VERSION).expect("decode");
    let topic = &resp.responses[0];
    let part = &topic.partitions[0];
    assert_eq!(part.error_code, 0);
    assert!(part.records.is_some(), "bootstrap records present at offset 0");
    assert_eq!(part.current_leader.leader_id, SPIKE_LEADER_ID);
    assert_eq!(part.current_leader.leader_epoch, SPIKE_LEADER_EPOCH);
}
```

(Adjust field/module paths to the real generated names — explorer reported `FetchResponse`, `PartitionData.current_leader: LeaderIdAndEpoch`, `records: Option<RecordsPayload>`.)

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-raft --features kraft-spike fetch_response_carries -- --nocapture`
Expected: FAIL — `fetch_response_frame` not defined.

- [ ] **Step 3: Implement the Fetch responder**

Add to `crates/raft/src/kraft_spike.rs`:

```rust
use crabka_protocol::owned::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData, LeaderIdAndEpoch,
};
use crabka_protocol::records::RecordsPayload;
use crabka_protocol::primitives::uuid::Uuid;

/// One metadata partition response. At fetch_offset 0 we return the bootstrap
/// batch and set hwm to its end; at any higher offset we return empty with the
/// same hwm (observer has caught up).
fn metadata_partition(fetch_offset: i64) -> PartitionData {
    let bootstrap = bootstrap_log_batch();
    let record_count = bootstrap_records().len() as i64;
    let hwm = record_count; // log end offset = number of bootstrap records
    let records = if fetch_offset == 0 {
        Some(RecordsPayload::from(bootstrap))
    } else {
        None
    };
    PartitionData {
        partition_index: 0,
        error_code: 0,
        high_watermark: hwm,
        last_stable_offset: hwm,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records,
        diverging_epoch: Default::default(),
        current_leader: LeaderIdAndEpoch {
            leader_id: SPIKE_LEADER_ID,
            leader_epoch: SPIKE_LEADER_EPOCH,
            ..Default::default()
        },
        snapshot_id: Default::default(),
        ..Default::default()
    }
}

/// Full framed Fetch response. Flexible (v12+) responses use ResponseHeader v1
/// (correlation id + empty tagged-fields byte).
pub(crate) fn fetch_response_frame(correlation_id: i32, req_version: i16, fetch_offset: i64) -> Bytes {
    let resp = FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses: vec![FetchableTopicResponse {
            topic: String::new(), // v13+ uses topic_id; leave name empty
            topic_id: Uuid::from(CLUSTER_METADATA_TOPIC_ID),
            partitions: vec![metadata_partition(fetch_offset)],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    resp.encode(&mut body, req_version).expect("encode fetch response");

    let mut out = BytesMut::new();
    let flexible = req_version >= 12;
    let header_extra = if flexible { 1 } else { 0 }; // tagged-fields byte
    let len = 4 + header_extra + body.len();
    out.extend_from_slice(&(len as i32).to_be_bytes());
    out.extend_from_slice(&correlation_id.to_be_bytes());
    if flexible {
        out.extend_from_slice(&[0u8]); // empty tagged fields in response header
    }
    out.extend_from_slice(&body);
    out.freeze()
}
```

(`RecordsPayload::from(Bytes)` — confirm the constructor in `crates/protocol/src/records/`. If it wraps raw bytes differently, adapt.)

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p crabka-raft --features kraft-spike fetch_response_carries -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft_spike.rs
git commit -m "feat(raft): kraft-spike Fetch responder for __cluster_metadata-0"
```

---

## Task 6: Hook the responders into the controller listener

**Goal:** Under `#[cfg(feature = "kraft-spike")]`, intercept api_key 1 and 18 in the listener and write the framed responses, mirroring the existing ApiVersions special-case at `server.rs:113`.

**Files:**
- Modify: `crates/raft/src/server.rs`

- [ ] **Step 1: Locate the interception point**

In `crates/raft/src/server.rs`, the per-connection loop reads `(api_key, correlation_id, body)` and special-cases `API_KEY_API_VERSIONS` around line 113, writing the full frame and `continue`-ing. We add a sibling branch BEFORE the generic `dispatch()` call. The request `api_version` is parsed at `read_one_request` (currently discarded as `_api_version`) — thread it out so the spike can echo the right version.

- [ ] **Step 2: Surface `api_version` from `read_one_request`**

Change `read_one_request` to also return `api_version`. Current signature (server.rs:150):

```rust
async fn read_one_request<S>(stream: &mut S) -> Result<(i16, i32, Bytes), RaftError>
```

Change to return the version too:

```rust
async fn read_one_request<S>(stream: &mut S) -> Result<(i16, i16, i32, Bytes), RaftError>
```

At the parse site replace `let _api_version = cur.get_i16();` with `let api_version = cur.get_i16();` and include it in the returned tuple. Update the call site in `handle_conn` to bind `let (api_key, api_version, correlation_id, body) = ...;` (the existing `API_KEY_API_VERSIONS` branch and `dispatch` call stay; just add the new binding).

- [ ] **Step 3: Add the spike interception branch**

In `handle_conn`, immediately after the existing `if api_key == API_KEY_API_VERSIONS { ... continue; }` block, add:

```rust
#[cfg(feature = "kraft-spike")]
{
    use crate::kraft_spike;
    // Real KRaft ApiVersions (18): replace the minimal openraft stub.
    if api_key == 18 {
        let frame = kraft_spike::api_versions_response_frame(correlation_id, api_version);
        stream.write_all(&frame).await.map_err(io_err)?;
        stream.flush().await.map_err(io_err)?;
        continue;
    }
    // Real KRaft Fetch (1) for __cluster_metadata-0.
    if api_key == 1 {
        // Decode just enough to learn the fetch_offset for partition 0.
        let fetch_offset = kraft_spike::fetch_offset_from_request(&body, api_version)
            .unwrap_or(0);
        let frame = kraft_spike::fetch_response_frame(correlation_id, api_version, fetch_offset);
        stream.write_all(&frame).await.map_err(io_err)?;
        stream.flush().await.map_err(io_err)?;
        continue;
    }
}
```

Note: when `kraft-spike` is enabled, the JVM sends ApiVersions at api_key 18 which is distinct from the openraft `API_KEY_API_VERSIONS` stub path — confirm `API_KEY_API_VERSIONS == 18`; if so, the `#[cfg]` branch must run BEFORE the existing stub so the real response wins. Reorder so the spike branch precedes the stub, and `#[cfg(not(feature = "kraft-spike"))]`-gate the stub to avoid a double-handle.

- [ ] **Step 4: Add the request-offset decoder to the spike module**

In `crates/raft/src/kraft_spike.rs`:

```rust
use crabka_protocol::codec::Decode;
use crabka_protocol::owned::fetch_request::FetchRequest;

/// Pull partition-0's fetch_offset out of a Fetch request; returns None if the
/// request can't be decoded or has no partitions (then caller defaults to 0).
pub(crate) fn fetch_offset_from_request(body: &[u8], version: i16) -> Option<i64> {
    let mut cur: &[u8] = body;
    let req = FetchRequest::decode(&mut cur, version).ok()?;
    req.topics.first()?.partitions.first().map(|p| p.fetch_offset)
}
```

- [ ] **Step 5: Verify it builds with and without the feature**

Run: `cargo build -p crabka-raft && cargo build -p crabka-raft --features kraft-spike`
Expected: both succeed. Then `cargo test -p crabka-raft --features kraft-spike -- --nocapture` — all unit tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/server.rs crates/raft/src/kraft_spike.rs
git commit -m "feat(raft): hook kraft-spike responders into controller listener"
```

---

## Task 7: Docker-gated JVM acceptance test + the spike iteration loop

**Goal:** Boot the spike controller in-process and a real `apache/kafka:4.0.0` broker observer in a container; assert from the broker's logs that it fetched and loaded the metadata with no format errors. Iterate bytes against the live JVM until green.

**Files:**
- Create: `crates/broker/tests/kraft_spike_jvm.rs`

- [ ] **Step 1: Write the acceptance test**

Create `crates/broker/tests/kraft_spike_jvm.rs`. Mirror `jvm_acceptance.rs` patterns: in-process broker via `BrokerConfig`/`Broker::start`, JVM via `docker run`, `host.docker.internal` networking, `#[ignore]` gating. The JVM runs as a broker-only observer pointed at the Crabka controller port (9093).

```rust
//! THROWAWAY KIP-595 slice-0 acceptance test. Requires Docker + the
//! `kraft-spike` feature: `cargo test -p crabka-broker --features kraft-spike \
//!   --test kraft_spike_jvm -- --ignored --nocapture`.
use std::process::{Command, Stdio};
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BootstrapMode};
use crabka_log::LogConfig;

const KAFKA_IMAGE: &str = "apache/kafka:4.0.0";
const CONTROLLER_PORT: u16 = 9093;

async fn start_spike_controller() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let controller_addr = format!("0.0.0.0:{CONTROLLER_PORT}").parse().unwrap();
    let config = BrokerConfig {
        broker_id: 1,
        node_id: 1,
        listen_addr: "0.0.0.0:9092".parse().unwrap(),
        advertised_listener: "host.docker.internal:9092".into(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(1, controller_addr)],
        bootstrap_mode: BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start spike controller");
    (handle, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + kraft-spike feature"]
async fn jvm_observer_fetches_metadata() {
    let (controller, _dir) = start_spike_controller().await;

    let name = "crabka-kraft-spike-obs";
    let _ = Command::new("docker").args(["rm", "-f", name]).output();

    // JVM broker observer. process.roles=broker → it fetches the metadata log
    // from the controller quorum on startup. cluster.id must be a fixed value
    // matching what the controller advertises (Task 1 finding); use the captured id.
    let cluster_id = "<CLUSTER_ID from Task 1>"; // TODO Task 1
    let status = Command::new("docker")
        .args([
            "run", "-d", "--name", name,
            "--add-host=host.docker.internal:host-gateway",
            "-e", "KAFKA_NODE_ID=2",
            "-e", "KAFKA_PROCESS_ROLES=broker",
            "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092",
            "-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:19092",
            &format!("-e=KAFKA_CONTROLLER_QUORUM_VOTERS=1@host.docker.internal:{CONTROLLER_PORT}"),
            "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            &format!("-e=CLUSTER_ID={cluster_id}"),
            KAFKA_IMAGE,
        ])
        .status().expect("docker run");
    assert!(status.success(), "failed to start JVM observer");

    // Give the observer time to connect + fetch.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let logs = Command::new("docker").args(["logs", name]).output().expect("docker logs");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
    controller.shutdown().await;

    // SUCCESS: the observer fetched + loaded the metadata, with no decode errors.
    // Exact success/failure log substrings are Task 1 / iteration findings; seed
    // with these and refine from the real JVM logs.
    let no_format_errors = !text.contains("CorruptRecordException")
        && !text.contains("Error while reading")
        && !text.contains("UnknownServerException");
    let loaded = text.contains("Loaded new metadata")  // refine from real logs
        || text.contains("metadata.version")
        || text.contains("Caught up");
    assert!(no_format_errors, "JVM reported a format/decode error:\n{text}");
    assert!(loaded, "JVM observer did not log a metadata load:\n{text}");
}
```

- [ ] **Step 2: Run the test against the live JVM**

Run: `cargo test -p crabka-broker --features kraft-spike --test kraft_spike_jvm -- --ignored --nocapture`
Expected (first run): likely FAIL — this is the spike iteration loop.

- [ ] **Step 3: The iteration loop (the heart of the spike)**

Repeat until green, recording each finding in `2026-05-30-kraft-wire-findings.md`:
1. Read the JVM observer logs (`docker logs`) for the rejection (wrong Fetch version advertised, missing api key, bad CRC, unexpected record framing, leader-epoch mismatch, cluster-id mismatch).
2. Map the rejection to a constant/byte (`FETCH_REQ_VERSION`, `required_api_keys`, `bootstrap_records`, `CLUSTER_METADATA_TOPIC_ID`, leader epoch).
3. Fix the value in `kraft_spike.rs`; re-run.
4. Record what the JVM required and why in the findings doc.

- [ ] **Step 4: Confirm success criteria**

Green = JVM observer logs show it fetched `__cluster_metadata-0`, advanced past the bootstrap records, logged a metadata load, with zero CRC/format/decode errors. This satisfies the spec's success bar.

- [ ] **Step 5: Commit the passing test + final findings**

```bash
git add crates/broker/tests/kraft_spike_jvm.rs docs/superpowers/specs/2026-05-30-kraft-wire-findings.md crates/raft/src/kraft_spike.rs
git commit -m "test(kip-595): JVM observer fetches Crabka metadata log (slice 0 spike green)"
```

---

## Task 8: Capstone — finalize findings and record disposition

**Goal:** Make the findings doc the authoritative input for slices 1–3 and decide what (if anything) of the spike code survives.

**Files:**
- Modify: `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md`

- [ ] **Step 1: Complete the findings doc**

Ensure every wire-facts row has a concrete, captured value (no `<...>` left). Add a "Implications for slices 1–3" section: which Fetch version slice 2 must implement, the exact bootstrap record set slice 1 must produce, the topic id, the leader-epoch semantics observed, and any ApiVersions gating slice 3 must satisfy.

- [ ] **Step 2: Record the spike disposition**

Add a "Disposition" section: the spike code (`kraft_spike.rs`, the `server.rs` hook, the `kraft_spike_jvm.rs` test) is throwaway and will be removed (or carried as a reference) when slice 3 lands the real state machine. The `kraft-spike` feature stays out of `default`. Note this so a future reader doesn't mistake the spike for production code.

- [ ] **Step 3: Run the full unit suite + fmt to confirm nothing regressed in default builds**

Run: `cargo fmt --all && cargo test -p crabka-raft && cargo test -p crabka-broker`
Expected: PASS (the spike is feature-gated, so default builds are unaffected). `cargo fmt --check` must be clean (CI gates on it).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-05-30-kraft-wire-findings.md
git commit -m "docs(kip-595): finalize slice-0 wire findings + spike disposition"
```

---

## Self-Review Notes

- **Spec coverage:** success criteria → Task 7; throwaway feature-gated path → Tasks 2/6; ground-truth capture → Task 1; bootstrap log builder → Task 3; Fetch responder → Task 5; ApiVersions responder → Task 4; Docker-gated test mirroring `jvm_acceptance.rs` → Task 7; kept findings doc → Tasks 1/8. All spec sections covered.
- **Capture-derived values:** `FETCH_REQ_VERSION`, `CLUSTER_METADATA_TOPIC_ID`, `METADATA_VERSION_LEVEL`, `BOOTSTRAP_RECORDS`, `REQUIRED_API_KEYS`, success/failure log substrings, and `CLUSTER_ID` are intentionally filled from Task 1 + the Task 7 iteration loop. These are the spike's deliverable, not plan placeholders — each has an explicit source and a task that pins it.
- **Type consistency:** `bootstrap_log_batch`, `bootstrap_records`, `api_versions_response_frame`, `fetch_response_frame`, `fetch_offset_from_request`, `metadata_partition` are defined once and referenced consistently. Generated-type names (`FetchResponse`, `PartitionData`, `LeaderIdAndEpoch`, `ApiVersionsResponse`, `RecordBatch`, `RecordsPayload`) come from the explorer's report; confirm exact module paths against `crates/protocol/generated/` and `crates/protocol/src/records/` when implementing.
- **Build hygiene:** every task that adds code ends with a build/test + commit; Task 8 runs `cargo fmt` (CI gate) and the default-feature suites to confirm no regression.
