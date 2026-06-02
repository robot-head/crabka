# KIP-595 Slice 2 — RPC Codec Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Byte-exactly validate the already-generated KIP-595 RPC codecs (`Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch`, `DescribeQuorum`, raft `Fetch`) by round-tripping real `apache/kafka:4.0.0` request+response frames captured from a live JVM controller quorum.

**Architecture:** Capture pcap from a 3-node JVM controller quorum, extract per-RPC request/response frames (header+body, length-prefix stripped) as fixtures, and assert each decodes+re-encodes byte-identically through the generated types. `src/` changes ONLY if a mismatch exposes a codec bug (then fix schema/codegen + regenerate, per the Slice 1 loop).

**Tech Stack:** Docker + `apache/kafka:4.0.0`, `tcpdump`/Python for capture+extraction, the generated `crabka_protocol` RPC types + `RequestHeader`/`ResponseHeader`.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice2-rpc-codec-validation-design.md](../specs/2026-05-31-kip595-slice2-rpc-codec-validation-design.md)

---

## Background the implementer needs

- The RPC types already exist: `crabka_protocol::owned::{vote_request::VoteRequest, vote_response::VoteResponse, begin_quorum_epoch_request::BeginQuorumEpochRequest, begin_quorum_epoch_response::BeginQuorumEpochResponse, end_quorum_epoch_request::EndQuorumEpochRequest, end_quorum_epoch_response::EndQuorumEpochResponse, describe_quorum_request::DescribeQuorumRequest, describe_quorum_response::DescribeQuorumResponse, fetch_request::FetchRequest, fetch_response::FetchResponse}`, all implementing `crabka_protocol::{Encode, Decode}` with `(&mut buf, version)`. Headers: `crabka_protocol::owned::request_header::RequestHeader`, `response_header::ResponseHeader`.
- **Versions / flexibility** (from the generated `MIN/MAX/FLEXIBLE_MIN`):
  - Vote 0–2, flexible_min 0; DescribeQuorum 0–2, flexible_min 0; Fetch 4–18, flexible_min 12.
  - BeginQuorumEpoch / EndQuorumEpoch 0–1, flexible_min 1.
- **Header version rule** (Kafka): for a message at `api_version`, it is *flexible* iff `api_version >= flexible_min`. The **RequestHeader** version is `2` if flexible else `1`; the **ResponseHeader** version is `1` if flexible else `0`. (RequestHeader supports v1–2; ResponseHeader v0–1.)
- A Kafka frame on the wire = `length (i32 BE) + RequestHeader/ResponseHeader + body`. Fixtures store the frame **minus** the 4-byte length prefix (i.e. header + body).
- Capture mechanics mirror Slice 0: a `tcpdump` sidecar sharing a container's net namespace (`--network container:<name>`), `nicolaka/netshoot`. The bundled tshark Kafka dissector is unreliable for 4.0 flexible protocol — parse raw payload bytes.

## File Structure

| Path | Responsibility |
|------|----------------|
| `crates/protocol/tests/fixtures/rpc/<rpc>_{request,response}.bin` | Captured JVM frames (header+body), one per RPC per direction. |
| `crates/protocol/tests/kraft_rpc_roundtrip.rs` | Decode header+body → re-encode → assert byte-identical, per fixture. |
| (only if a bug is found) `crates/protocol/schemas/*.json` + `generated/*` | Schema/codegen fix + regenerate. |

---

## Task 1: Capture RPC frames from a 3-node JVM controller quorum

**Driven inline by the controller** (Docker + pcap + iterative extraction).

**Files:** create `crates/protocol/tests/fixtures/rpc/*.bin`

- [ ] **Step 1: Boot a 3-node controller-only quorum on a shared network**

```bash
cd /tmp && rm -rf rpccap && mkdir rpccap && cd rpccap
CID=$(docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
docker network rm rpcnet 2>/dev/null; docker network create rpcnet >/dev/null
VOTERS="1@ctrl1:9093,2@ctrl2:9093,3@ctrl3:9093"
for id in 1 2 3; do
  docker run -d --name ctrl$id --hostname ctrl$id --network rpcnet \
    -e KAFKA_NODE_ID=$id -e KAFKA_PROCESS_ROLES=controller \
    -e KAFKA_LISTENERS=CONTROLLER://:9093 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=$VOTERS \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT \
    -e CLUSTER_ID="$CID" apache/kafka:4.0.0 >/dev/null
done
```

- [ ] **Step 2: Capture the election + describe + graceful-shutdown windows**

Start a tcpdump sidecar on `ctrl1` BEFORE the quorum settles is impossible (already booted); instead bounce `ctrl1` with capture attached so it re-runs the election:

```bash
docker run -d --name cap --network "container:ctrl1" nicolaka/netshoot \
  tcpdump -i any -s 0 -w /tmp/rpc.pcap 'tcp port 9093' >/dev/null 2>&1
sleep 1
docker restart ctrl1 >/dev/null   # ctrl1 re-elects → Vote + BeginQuorumEpoch + Fetch on the wire
sleep 12
# DescribeQuorum: ask the quorum via the admin tool (any reachable controller).
docker run --rm --network rpcnet apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller ctrl1:9093 describe --replication >/dev/null 2>&1 || true
sleep 2
docker stop ctrl2 >/dev/null   # graceful leader/voter shutdown → EndQuorumEpoch (if ctrl2 was leader)
sleep 4
docker stop cap >/dev/null
docker cp cap:/tmp/rpc.pcap ./rpc.pcap
docker rm cap >/dev/null
ls -la rpc.pcap
```

If `EndQuorumEpoch` is absent (ctrl2 wasn't leader), repeat stopping whichever node is leader (`kafka-metadata-quorum describe --status` shows `LeaderId`).

- [ ] **Step 3: Extract per-RPC request/response frames**

Use a Python extractor over the pcap's TCP payloads (mirror the Slice 0 raw-frame walk). For each Kafka frame: read `len(i32)`, then the body; the first 2 bytes after len are `api_key (i16)` for REQUESTS (dst port 9093). Pair RESPONSES (src port 9093) to requests by `correlation_id` (request: api_key@0, api_version@2, correlation@4; response: correlation@0). Save the first clean instance of each as `fixtures/rpc/<rpc>_{request,response}.bin` (frame minus 4-byte length). Record each frame's `api_version` in a comment block you will paste into the test's RPC table. Target api_keys: 52 Vote, 53 BeginQuorumEpoch, 54 EndQuorumEpoch, 55 DescribeQuorum, 1 Fetch.

Iterate the extractor live until all in-scope request+response pairs are captured. Verify each fixture is non-empty and the recorded `api_version` is within the type's MIN..=MAX.

- [ ] **Step 4: Tear down + record versions**

```bash
docker rm -f ctrl1 ctrl2 ctrl3 >/dev/null 2>&1; docker network rm rpcnet >/dev/null 2>&1
```

Copy fixtures into the repo (`crates/protocol/tests/fixtures/rpc/`). Note the captured api_version per RPC for Task 2's table.

---

## Task 2: Round-trip byte-identity test

**Files:** create `crates/protocol/tests/kraft_rpc_roundtrip.rs`

- [ ] **Step 1: Write the test**

```rust
//! Byte-identity: decode each captured KIP-595 RPC frame (header + body, from a
//! real apache/kafka:4.0.0 controller quorum) through the generated types and
//! re-encode, asserting the bytes are unchanged. Validates the generated RPC
//! codecs against genuine JVM wire. Fixtures captured per the slice-2 plan.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::owned::request_header::RequestHeader;
use crabka_protocol::owned::response_header::ResponseHeader;
use crabka_protocol::{Decode, Encode};

/// Decode a request frame (RequestHeader + body) and re-encode; assert identical.
fn roundtrip_request<T: Decode<'static> + Encode>(frame: &'static [u8], api_version: i16, flexible_min: i16) {
    let hdr_ver = if api_version >= flexible_min { 2 } else { 1 };
    let mut cur: &[u8] = frame;
    let hdr = RequestHeader::decode(&mut cur, hdr_ver).expect("request header decodes");
    let body = T::decode(&mut cur, api_version).expect("request body decodes");
    assert!(cur.is_empty(), "trailing bytes after request body");
    let mut out = BytesMut::new();
    hdr.encode(&mut out, hdr_ver).expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert!(out.as_ref() == frame, "request frame not byte-identical");
}

/// Decode a response frame (ResponseHeader + body) and re-encode; assert identical.
fn roundtrip_response<T: Decode<'static> + Encode>(frame: &'static [u8], api_version: i16, flexible_min: i16) {
    let hdr_ver = if api_version >= flexible_min { 1 } else { 0 };
    let mut cur: &[u8] = frame;
    let hdr = ResponseHeader::decode(&mut cur, hdr_ver).expect("response header decodes");
    let body = T::decode(&mut cur, api_version).expect("response body decodes");
    assert!(cur.is_empty(), "trailing bytes after response body");
    let mut out = BytesMut::new();
    hdr.encode(&mut out, hdr_ver).expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert!(out.as_ref() == frame, "response frame not byte-identical");
}

// api_version per RPC comes from the Task 1 capture; substitute the recorded
// values. flexible_min: Vote/DescribeQuorum 0, Fetch 12, Begin/EndQuorumEpoch 1.
use crabka_protocol::owned::vote_request::VoteRequest;
use crabka_protocol::owned::vote_response::VoteResponse;
use crabka_protocol::owned::begin_quorum_epoch_request::BeginQuorumEpochRequest;
use crabka_protocol::owned::begin_quorum_epoch_response::BeginQuorumEpochResponse;
use crabka_protocol::owned::end_quorum_epoch_request::EndQuorumEpochRequest;
use crabka_protocol::owned::end_quorum_epoch_response::EndQuorumEpochResponse;
use crabka_protocol::owned::describe_quorum_request::DescribeQuorumRequest;
use crabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse;
use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::owned::fetch_response::FetchResponse;

#[test] fn vote_request_roundtrips() {
    roundtrip_request::<VoteRequest>(include_bytes!("fixtures/rpc/vote_request.bin"), /*ver*/ 1, 0);
}
#[test] fn vote_response_roundtrips() {
    roundtrip_response::<VoteResponse>(include_bytes!("fixtures/rpc/vote_response.bin"), 1, 0);
}
#[test] fn begin_quorum_epoch_request_roundtrips() {
    roundtrip_request::<BeginQuorumEpochRequest>(include_bytes!("fixtures/rpc/begin_quorum_epoch_request.bin"), 1, 1);
}
#[test] fn begin_quorum_epoch_response_roundtrips() {
    roundtrip_response::<BeginQuorumEpochResponse>(include_bytes!("fixtures/rpc/begin_quorum_epoch_response.bin"), 1, 1);
}
#[test] fn end_quorum_epoch_request_roundtrips() {
    roundtrip_request::<EndQuorumEpochRequest>(include_bytes!("fixtures/rpc/end_quorum_epoch_request.bin"), 1, 1);
}
#[test] fn end_quorum_epoch_response_roundtrips() {
    roundtrip_response::<EndQuorumEpochResponse>(include_bytes!("fixtures/rpc/end_quorum_epoch_response.bin"), 1, 1);
}
#[test] fn describe_quorum_request_roundtrips() {
    roundtrip_request::<DescribeQuorumRequest>(include_bytes!("fixtures/rpc/describe_quorum_request.bin"), 2, 0);
}
#[test] fn describe_quorum_response_roundtrips() {
    roundtrip_response::<DescribeQuorumResponse>(include_bytes!("fixtures/rpc/describe_quorum_response.bin"), 2, 0);
}
#[test] fn fetch_request_roundtrips() {
    roundtrip_request::<FetchRequest>(include_bytes!("fixtures/rpc/fetch_request.bin"), 17, 12);
}
#[test] fn fetch_response_roundtrips() {
    roundtrip_response::<FetchResponse>(include_bytes!("fixtures/rpc/fetch_response.bin"), 17, 12);
}
```

Substitute each `api_version` with the value recorded in Task 1 (the literals above are the expected kafka:4.0.0 versions — confirm against capture). If `Decode<'static>` bound is wrong for the borrowed/owned split, use the owned types' actual `Decode` impl signature (owned types decode from any `Buf`; adjust the generic bound to match the crate's `Decode` trait, e.g. `for<'de> Decode<'de>`).

- [ ] **Step 2: Run the tests**

Run: `cargo test -p crabka-protocol --test kraft_rpc_roundtrip -- --nocapture`
Expected: all pass. A failure = a real codec bug → Task 3.

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/tests/fixtures/rpc crates/protocol/tests/kraft_rpc_roundtrip.rs
git commit -m "test(protocol): byte-identity round-trip of real KIP-595 RPC frames"
```

---

## Task 3: Fix any codec bug surfaced (conditional)

Only if Task 2 fails. For the failing RPC, dump `expected` vs `re-encoded` hex (add a temporary debug like the Slice 1 `kraft_dbg.rs`), localize the diff to a field, fix the schema (`crates/protocol/schemas/<Rpc>.json`) or the codegen, run `tools/regenerate.sh`, re-run Task 2. Commit:

```bash
git add crates/protocol/schemas crates/protocol/generated crates/protocol/src
git commit -m "fix(protocol): correct <Rpc> codec to match kafka:4.0.0 wire"
```

If no failures, skip this task (the intended result — generated codecs confirmed wire-correct).

---

## Task 4: Capstone — fmt, clippy, regression

- [ ] **Step 1:** `cargo fmt --all && cargo fmt --all --check`  → clean.
- [ ] **Step 2:** `cargo test -p crabka-protocol`  → all pass (incl. the new round-trip test + Slice 1 round-trip).
- [ ] **Step 3:** `cargo build -p crabka-protocol`  → build.rs sha assertion green (unchanged unless Task 3 regenerated, in which case schemas/VERSION stays as-is so the embedded sha still matches).
- [ ] **Step 4:** Commit any fmt changes: `git add -A && git commit -m "chore: fmt" || echo "nothing to commit"`.

---

## Self-Review Notes

- **Spec coverage:** capture harness (3-node quorum, 3 events) → Task 1; frame extractor → Task 1 Step 3; round-trip test for the 5 RPCs × 2 directions → Task 2; bug-fix loop → Task 3; FetchSnapshot/voter-RPCs deferred per spec (not in the test table). Covered.
- **Capture-derived values:** the per-RPC `api_version` literals in Task 2 are the expected kafka:4.0.0 versions; Task 1 records the actual values and Task 2 substitutes them. Not placeholders — each has a capture source.
- **Type consistency:** `roundtrip_request`/`roundtrip_response` helpers defined once, used uniformly; module paths match `src/owned/mod.rs`.
- **Inline execution:** Task 1 (Docker/pcap) and the iterative extractor are controller-driven; Task 2's test is a single committed file. Given the shape, inline execution is appropriate (a subagent cannot drive the live capture).
