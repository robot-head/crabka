# Captured-traffic Corpus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a real captured-traffic corpus with exactly one entry per supported `(api_key, version)` request/response pair, captured from genuine Kafka 4.3.0 wire traffic where realistically possible and oracle-synthesized otherwise, closing the `KNOWN_ISSUES.md` criterion-#9 deviation.

**Architecture:** A pure-Rust `kafka-tap` library tees every length-prefixed frame flowing between real JVM clients and a pinned `apache/kafka:4.3.0` broker (which advertises the tap's endpoint so all client connections traverse it). A Docker-gated `#[ignore]` harness drives a battery of JVM clients through the tap, a post-processor strips each frame's request/response header and writes one message-body `.hex`/`.toml` per `(api_key, version, direction)` (captured = `synthetic=false`), and a synthesis pass fills the remainder via the existing JVM oracle (`synthetic=true`). The always-on, JVM-free `corpus_replay.rs` decodes→re-encodes every entry and asserts full `CASES` coverage.

**Tech Stack:** Rust 2024, `bytes`, `serde`/`toml`/`hex`/`serde_json`, the existing `crabka-protocol` codecs + generated `differential_table.rs`, the existing Gradle JVM oracle, Docker + `apache/kafka:4.3.0`, GitHub Actions.

---

## Background facts the implementer must know

- **Corpus format** (`crates/protocol/tests/corpus/README.md`): each entry is two files with the same stem — `<stem>.hex` (raw **message body**, *excluding* the 4-byte length prefix **and excluding the request/response header**; whitespace ignored) and `<stem>.toml` (metadata). The lone existing entry `api_versions_request_v3_001` proves the contract: its hex `0b6c696272646b61666b6106322e342e3000` is the `ApiVersionsRequest` message alone (compact string "librdkafka", compact string "2.4.0", empty tagged fields) — no `api_key`/`correlation_id` header bytes.
- **TOML schema** (`crates/protocol/tests/corpus_replay.rs` `Meta`): `api_key: i16`, `version: i16`, `direction: "request"|"response"`, `source_kafka_version: String`, `synthetic: bool`, `description: String`.
- **`CASES` table** (`crates/protocol/generated/differential_table.rs`, generated): `Case { name: &str, api_key: i16, version: i16, kind: Kind }`, `Kind ∈ {Request, Response, RequestHeader, ResponseHeader}`. Already provides `encode_default(name, version) -> Vec<u8>` and `default_json_for(name, version) -> serde_json::Value`. **Corpus entries correspond to `Kind::Request` and `Kind::Response` cases only** — the `RequestHeader`/`ResponseHeader` cases are for the differential header test, not corpus entries (every request/response frame embeds its header inline).
- **Codegen emitter** that produces `differential_table.rs`: `crates/protocol-codegen/src/emit/differential_table.rs`. Regenerate with `tools/regenerate.sh` (or the codegen bin). `name_conv::module_name("ApiVersionsRequest") == "api_versions_request"`, `name_conv::type_name(...) == "ApiVersionsRequest"`.
- **Header versions are NOT yet exposed** anywhere in the crate. Kafka's rules, which this plan generates:
  - Request header version: `ControlledShutdownRequest` v0 → `0`; otherwise `version >= flexibleMin ? 2 : 1`.
  - Response header version: `ApiVersionsResponse` → always `0` (the famous quirk — the client parses it before negotiating); otherwise `version >= flexibleMin ? 1 : 0`.
  - `flexibleMin` per message = its `FLEXIBLE_MIN` (the codegen knows it from the IR `flexibleVersions`).
- **JVM oracle** (`crates/protocol/tests/support/oracle.rs`): line-oriented JSON-RPC over a Gradle-built binary; `oracle::shared()` returns a mutex guard; `o.encode(api_key, version, is_request, &value) -> Vec<u8>` is the op the synthesis pass uses. Built via `(cd tools/oracle && ./gradlew installDist)`.
- **Broker boot pattern**: copy `crates/broker/tests/describe_groups_jvm.rs` (`docker run -d` single-node KRaft, dual `PLAINTEXT`/`EXTERNAL` listeners, fixed host port, `docker exec` to run bundled CLI tools). The image there is `confluentinc/cp-kafka:7.4.0`; **this plan uses `apache/kafka:4.3.0`** to match `crates/protocol/schemas/VERSION` (`ref: 4.3.0`).
- **Workspace** is `members = ["crates/*"]`; the tap crate therefore lives at `crates/kafka-tap` (the design doc's `tools/kafka-tap` is reconciled to this — Task 9 fixes the spec line). It is `publish = false`.

---

## File structure

| Path | Responsibility | Tasks |
|---|---|---|
| `crates/protocol-codegen/src/emit/differential_table.rs` | Emit `roundtrip` + `request_header_version`/`response_header_version` + `strip_frame_header` alongside existing fns | 1 |
| `crates/protocol/generated/differential_table.rs` | Regenerated output of the above | 1 |
| `crates/kafka-tap/Cargo.toml` | New `publish = false` lib+bin crate | 2 |
| `crates/kafka-tap/src/frame.rs` | Framing + request/response correlation; unit-tested, no Docker | 2 |
| `crates/kafka-tap/src/lib.rs` | `spawn`/relay API: tee frames to a `Recorder`, forward bytes verbatim | 3 |
| `crates/kafka-tap/src/main.rs` | Thin standalone bin wrapper around the lib | 3 |
| `crates/protocol/tests/support/driver.rs` | Declarative JVM-client op battery (CLI tools + AdminClient) | 5 |
| `crates/protocol/tests/capture_corpus.rs` | `#[ignore]` Docker-gated harness: boot broker, run tap+driver, post-process, synthesize | 5,6 |
| `crates/protocol/tests/corpus/*.{hex,toml}` | Generated, committed corpus (captured + synthetic) | 7 |
| `crates/protocol/tests/corpus_replay.rs` | Generalized always-on round-trip + coverage-completeness gate | 4 |
| `.github/workflows/recapture-corpus.yml` | `workflow_dispatch` drift check | 8 |
| `KNOWN_ISSUES.md` | Remove the criterion-#9 deviation | 9 |
| `docs/superpowers/specs/2026-06-08-crabka-captured-traffic-corpus-design.md` | Fix `tools/kafka-tap` → `crates/kafka-tap` | 9 |

## Execution batches (per CLAUDE.md parallel rule)

- **Batch 1 (parallel, disjoint files):** Task 1 (codegen + generated), Task 2 (kafka-tap frame), Task 3 (kafka-tap lib/bin).
- **Batch 2 (after Batch 1):** Task 4 (corpus_replay — needs Task 1's generated fns).
- **Batch 3 (after Batch 1):** Task 5 + Task 6 (harness/driver/synthesis — need Task 1 + Task 3).
- **Manual generation step** (Task 7) — run the harness with Docker to produce the committed corpus; then Task 4's coverage assertion passes.
- **Batch 4:** Task 8 (workflow), Task 9 (KNOWN_ISSUES + spec fix).

---

## Task 1: Generate `roundtrip`, header-version, and `strip_frame_header`

**Files:**
- Modify: `crates/protocol-codegen/src/emit/differential_table.rs`
- Regenerate: `crates/protocol/generated/differential_table.rs`
- Test: `crates/protocol-codegen/tests/differential_table_emit.rs` (create)

The emitter currently calls `emit_cases_table`, `emit_encode_default`, `emit_default_json_for` from `emit(...)`. Add three emitters and wire them in.

- [ ] **Step 1: Write the failing emitter test**

Create `crates/protocol-codegen/tests/differential_table_emit.rs`:

```rust
//! Smoke test: the differential-table emitter produces the new dispatch fns.
use crabka_protocol_codegen::ir::{MessageSpec, MessageType, VersionRange};
use crabka_protocol_codegen::emit::differential_table;

fn req(name: &str, api_key: i16, min: i16, max: i16, flex_min: i16) -> MessageSpec {
    MessageSpec::test_request(name, api_key, VersionRange { min, max }, flex_min)
}

#[test]
fn emits_roundtrip_and_header_helpers() {
    let specs = vec![req("ApiVersionsRequest", 18, 0, 3, 3)];
    let out = differential_table::emit(&specs, "testsha");
    assert!(out.contains("pub fn roundtrip(name: &str, version: i16, bytes: &[u8]) -> Vec<u8>"));
    assert!(out.contains("pub fn request_header_version(name: &str, version: i16) -> i16"));
    assert!(out.contains("pub fn response_header_version(name: &str, version: i16) -> i16"));
    assert!(out.contains("pub fn strip_frame_header"));
    // ApiVersionsResponse special-case must be present in the emitted source.
    assert!(out.contains("\"ApiVersionsResponse\" => 0"));
}
```

> If `MessageSpec::test_request` / `VersionRange` constructors don't exist with these exact names, the implementer must check `crates/protocol-codegen/src/ir.rs` and adapt the test's spec construction to the real API (the assertions on emitted strings stay the same). Reuse whatever the existing codegen unit tests use to build a `MessageSpec`.

- [ ] **Step 2: Run it, expect failure**

Run: `cargo test -p crabka-protocol-codegen --test differential_table_emit -- --nocapture`
Expected: FAIL (emitted source lacks the new fns).

- [ ] **Step 3: Implement the emitters**

In `crates/protocol-codegen/src/emit/differential_table.rs`, add to `emit(...)` after `emit_default_json_for(&mut out, specs);`:

```rust
    emit_roundtrip(&mut out, specs);
    emit_header_versions(&mut out, specs);
    emit_strip_frame_header(&mut out);
```

Add the functions (mirror `emit_encode_default`'s spec filtering — skip `valid_versions.is_empty()`, `internal`, and `MessageType::Data`):

```rust
fn emit_roundtrip(out: &mut String, specs: &[MessageSpec]) {
    writeln!(out, "#[must_use]").unwrap();
    writeln!(out, "#[allow(clippy::too_many_lines)]").unwrap();
    writeln!(out, "pub fn roundtrip(name: &str, version: i16, bytes: &[u8]) -> Vec<u8> {{").unwrap();
    writeln!(out, "    use crabka_protocol::Decode;").unwrap();
    writeln!(out, "    match name {{").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() || s.internal { continue; }
        match s.message_type {
            MessageType::Request | MessageType::Response | MessageType::Header => {}
            MessageType::Data => continue,
        }
        let snake = name_conv::module_name(&s.name);
        let type_name = name_conv::type_name(&s.name);
        writeln!(out, "        \"{}\" => {{", s.name).unwrap();
        writeln!(out, "            let mut cur = bytes;").unwrap();
        writeln!(out, "            let msg = crabka_protocol::owned::{snake}::{type_name}::decode(&mut cur, version).unwrap();").unwrap();
        writeln!(out, "            assert!(cur.is_empty(), \"trailing bytes decoding {} v{{version}}\", );", s.name).unwrap();
        writeln!(out, "            let mut buf = BytesMut::new();").unwrap();
        writeln!(out, "            msg.encode(&mut buf, version).unwrap();").unwrap();
        writeln!(out, "            buf.to_vec()").unwrap();
        writeln!(out, "        }}").unwrap();
    }
    writeln!(out, "        _ => panic!(\"unknown message in roundtrip: {{name}}\"),").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();
}

fn emit_header_versions(out: &mut String, specs: &[MessageSpec]) {
    // request_header_version
    writeln!(out, "#[must_use]").unwrap();
    writeln!(out, "pub fn request_header_version(name: &str, version: i16) -> i16 {{").unwrap();
    writeln!(out, "    match name {{").unwrap();
    writeln!(out, "        \"ControlledShutdownRequest\" if version == 0 => 0,").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() || s.internal { continue; }
        if !matches!(s.message_type, MessageType::Request) { continue; }
        let fm = s.flexible_min(); // i16, i16::MAX if never flexible
        writeln!(out, "        \"{}\" => if version >= {fm} {{ 2 }} else {{ 1 }},", s.name).unwrap();
    }
    writeln!(out, "        _ => panic!(\"unknown request in request_header_version: {{name}}\"),").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();
    // response_header_version
    writeln!(out, "#[must_use]").unwrap();
    writeln!(out, "pub fn response_header_version(name: &str, version: i16) -> i16 {{").unwrap();
    writeln!(out, "    match name {{").unwrap();
    writeln!(out, "        \"ApiVersionsResponse\" => 0,").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() || s.internal { continue; }
        if !matches!(s.message_type, MessageType::Response) { continue; }
        if s.name == "ApiVersionsResponse" { continue; }
        let fm = s.flexible_min();
        writeln!(out, "        \"{}\" => if version >= {fm} {{ 1 }} else {{ 0 }},", s.name).unwrap();
    }
    writeln!(out, "        _ => panic!(\"unknown response in response_header_version: {{name}}\"),").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();
}

fn emit_strip_frame_header(out: &mut String) {
    writeln!(out, "/// Decode and discard the request/response header from a full frame body,").unwrap();
    writeln!(out, "/// returning the remaining message bytes. `name` is the request or response").unwrap();
    writeln!(out, "/// message name; `version` its api version.").unwrap();
    writeln!(out, "#[must_use]").unwrap();
    writeln!(out, "pub fn strip_frame_header(name: &str, version: i16, is_request: bool, frame: &[u8]) -> Vec<u8> {{").unwrap();
    writeln!(out, "    use crabka_protocol::Decode;").unwrap();
    writeln!(out, "    let mut cur = frame;").unwrap();
    writeln!(out, "    if is_request {{").unwrap();
    writeln!(out, "        let hv = request_header_version(name, version);").unwrap();
    writeln!(out, "        crabka_protocol::owned::request_header::RequestHeader::decode(&mut cur, hv).unwrap();").unwrap();
    writeln!(out, "    }} else {{").unwrap();
    writeln!(out, "        let hv = response_header_version(name, version);").unwrap();
    writeln!(out, "        crabka_protocol::owned::response_header::ResponseHeader::decode(&mut cur, hv).unwrap();").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "    cur.to_vec()").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();
}
```

> **Verify two things against the real codebase before finalizing:**
> 1. `MessageSpec` exposes the message's flexible-min. If there is no `flexible_min()` method, compute it the same way the owned/borrowed emitters do (search `crates/protocol-codegen/src/emit/` for where `FLEXIBLE_MIN` is emitted) and inline that expression instead of `s.flexible_min()`.
> 2. The owned header module path. Confirm `crabka_protocol::owned::request_header::RequestHeader` / `::response_header::ResponseHeader` resolve (`module_name("RequestHeader") == "request_header"`). If `owned/mod.rs` re-exports differently, use the resolving path.

Fix the stray `, );` in the `assert!` line above — emit it as:
`writeln!(out, "            assert!(cur.is_empty());").unwrap();`

- [ ] **Step 4: Run the emitter test, expect pass**

Run: `cargo test -p crabka-protocol-codegen --test differential_table_emit`
Expected: PASS.

- [ ] **Step 5: Regenerate the committed table and verify it compiles**

Run: `bash tools/regenerate.sh` (or the documented codegen command; check `tools/regenerate.sh` for the exact invocation).
Then: `cargo build -p crabka-protocol --tests`
Expected: `crates/protocol/generated/differential_table.rs` now contains `roundtrip`, `request_header_version`, `response_header_version`, `strip_frame_header`; the crate builds.

- [ ] **Step 6: Commit**

```bash
git add crates/protocol-codegen/src/emit/differential_table.rs \
        crates/protocol-codegen/tests/differential_table_emit.rs \
        crates/protocol/generated/differential_table.rs
git commit -m "feat(codegen): emit roundtrip + header-version + strip_frame_header for corpus"
```

---

## Task 2: `kafka-tap` crate — framing & correlation (`frame.rs`)

**Files:**
- Create: `crates/kafka-tap/Cargo.toml`
- Create: `crates/kafka-tap/src/frame.rs`
- Create: `crates/kafka-tap/src/lib.rs` (module decl only in this task)

- [ ] **Step 1: Create the crate manifest**

`crates/kafka-tap/Cargo.toml`:

```toml
[package]
name = "crabka-kafka-tap"
version = "0.1.0"
edition = "2024"
publish = false
description = "Test-only TCP tap that records Kafka wire frames for corpus capture."

[lib]
name = "crabka_kafka_tap"

[[bin]]
name = "kafka-tap"
path = "src/main.rs"

[dependencies]
# stdlib-only tap (std::net threads); no async runtime needed.
```

> Match `edition`/`version` to the workspace convention — check a sibling `crates/*/Cargo.toml`. If the workspace pins versions via `version.workspace = true`, use that form. Keep `publish = false`.

- [ ] **Step 2: Write the failing frame test**

`crates/kafka-tap/src/frame.rs` — add tests first:

```rust
//! Kafka wire framing + request/response correlation. No schema knowledge.

#[cfg(test)]
mod tests {
    use super::*;

    // A request frame body: api_key=18(i16), api_version=3(i16), correlation_id=7(i32), then payload.
    fn req_frame(api_key: i16, api_version: i16, corr: i32, payload: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&api_key.to_be_bytes());
        b.extend_from_slice(&api_version.to_be_bytes());
        b.extend_from_slice(&corr.to_be_bytes());
        b.extend_from_slice(payload);
        b
    }

    #[test]
    fn parses_request_header_prefix() {
        let body = req_frame(18, 3, 7, &[0xaa, 0xbb]);
        let p = parse_request_prefix(&body).unwrap();
        assert_eq!(p, RequestPrefix { api_key: 18, api_version: 3, correlation_id: 7 });
    }

    #[test]
    fn correlates_response_by_id() {
        let mut pending = Pending::default();
        let body = req_frame(1, 11, 42, &[]);
        let p = parse_request_prefix(&body).unwrap();
        pending.record(p.correlation_id, p.api_key, p.api_version);
        // Response body starts with correlation_id i32.
        let mut resp = Vec::new();
        resp.extend_from_slice(&42i32.to_be_bytes());
        resp.extend_from_slice(&[0x01]);
        let got = pending.take(read_correlation_id(&resp).unwrap()).unwrap();
        assert_eq!(got, (1, 11));
    }
}
```

- [ ] **Step 3: Implement `frame.rs`**

Above the tests:

```rust
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RequestPrefix {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

/// Parse the fixed request-header prefix common to every request type:
/// `api_key: i16, api_version: i16, correlation_id: i32`.
#[must_use]
pub fn parse_request_prefix(body: &[u8]) -> Option<RequestPrefix> {
    if body.len() < 8 { return None; }
    Some(RequestPrefix {
        api_key: i16::from_be_bytes([body[0], body[1]]),
        api_version: i16::from_be_bytes([body[2], body[3]]),
        correlation_id: i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
    })
}

/// Every response body begins with `correlation_id: i32`, before any tagged
/// header — true for flexible and non-flexible responses alike.
#[must_use]
pub fn read_correlation_id(body: &[u8]) -> Option<i32> {
    if body.len() < 4 { return None; }
    Some(i32::from_be_bytes([body[0], body[1], body[2], body[3]]))
}

/// Per-connection map from correlation id to the (api_key, api_version) of the
/// request that bore it, so responses can be classified.
#[derive(Default)]
pub struct Pending {
    map: HashMap<i32, (i16, i16)>,
}

impl Pending {
    pub fn record(&mut self, correlation_id: i32, api_key: i16, api_version: i16) {
        self.map.insert(correlation_id, (api_key, api_version));
    }
    #[must_use]
    pub fn take(&mut self, correlation_id: i32) -> Option<(i16, i16)> {
        self.map.remove(&correlation_id)
    }
}

/// One captured frame, emitted by the relay to the recorder spool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedFrame {
    pub api_key: i16,
    pub version: i16,
    pub is_request: bool,
    /// Full frame body, excluding the 4-byte length prefix (header + message).
    pub body: Vec<u8>,
}
```

`crates/kafka-tap/src/lib.rs` (this task: just declare the module):

```rust
//! Test-only Kafka wire tap. See `frame` for parsing/correlation and the
//! crate-level `spawn` (added in the next task) for the relay.
pub mod frame;
```

- [ ] **Step 4: Run the frame tests, expect pass**

Run: `cargo test -p crabka-kafka-tap frame`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/kafka-tap/Cargo.toml crates/kafka-tap/src/frame.rs crates/kafka-tap/src/lib.rs
git commit -m "feat(kafka-tap): framing + request/response correlation"
```

---

## Task 3: `kafka-tap` relay (`lib.rs` + `main.rs`)

**Files:**
- Modify: `crates/kafka-tap/src/lib.rs`
- Create: `crates/kafka-tap/src/main.rs`
- Test: extend `crates/kafka-tap/src/frame.rs` tests are enough for parsing; add a relay integration test in `crates/kafka-tap/tests/relay.rs`

The relay listens on a TCP port, dials the upstream broker, and for each client connection runs two threads copying bytes verbatim while teeing complete frames (length-prefixed) into a shared `Recorder`.

- [ ] **Step 1: Write the failing relay test**

`crates/kafka-tap/tests/relay.rs`:

```rust
//! End-to-end relay test against a fake upstream "broker" that echoes a
//! canned response. Verifies bytes pass through unmodified and frames are
//! recorded with correct classification.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::{spawn, Recorder};
use crabka_kafka_tap::frame::CapturedFrame;

fn framed(body: &[u8]) -> Vec<u8> {
    let mut v = (body.len() as i32).to_be_bytes().to_vec();
    v.extend_from_slice(body);
    v
}

#[test]
fn relays_and_records() {
    // Fake upstream: read one request frame, reply with a response frame whose
    // body begins with the same correlation id (42).
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let up_addr = upstream.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut s, _) = upstream.accept().unwrap();
        let mut len = [0u8; 4];
        s.read_exact(&mut len).unwrap();
        let n = i32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; n];
        s.read_exact(&mut body).unwrap();
        let mut resp_body = 42i32.to_be_bytes().to_vec();
        resp_body.push(0x99);
        s.write_all(&framed(&resp_body)).unwrap();
    });

    let recorder = Arc::new(Mutex::new(Vec::<CapturedFrame>::new()));
    let rec_for_tap: Recorder = {
        let r = recorder.clone();
        Arc::new(move |f: CapturedFrame| r.lock().unwrap().push(f))
    };
    let tap_addr = spawn("127.0.0.1:0", &up_addr.to_string(), rec_for_tap).unwrap();

    // Client request: api_key=3 (Metadata), version=12, correlation_id=42.
    let mut req_body = Vec::new();
    req_body.extend_from_slice(&3i16.to_be_bytes());
    req_body.extend_from_slice(&12i16.to_be_bytes());
    req_body.extend_from_slice(&42i32.to_be_bytes());
    req_body.push(0x11);

    let mut c = TcpStream::connect(tap_addr).unwrap();
    c.write_all(&framed(&req_body)).unwrap();
    let mut len = [0u8; 4];
    c.read_exact(&mut len).unwrap();
    let n = i32::from_be_bytes(len) as usize;
    let mut resp = vec![0u8; n];
    c.read_exact(&mut resp).unwrap();
    assert_eq!(resp, vec![0, 0, 0, 42, 0x99]); // unmodified passthrough

    std::thread::sleep(std::time::Duration::from_millis(100));
    let frames = recorder.lock().unwrap().clone();
    assert!(frames.iter().any(|f| f.is_request && f.api_key == 3 && f.version == 12));
    assert!(frames.iter().any(|f| !f.is_request && f.api_key == 3 && f.version == 12));
}
```

- [ ] **Step 2: Run it, expect failure**

Run: `cargo test -p crabka-kafka-tap --test relay`
Expected: FAIL (`spawn`/`Recorder` not defined).

- [ ] **Step 3: Implement the relay in `lib.rs`**

Replace `crates/kafka-tap/src/lib.rs` with:

```rust
//! Test-only Kafka wire tap: a TCP relay that tees complete frames to a
//! `Recorder` while forwarding bytes byte-for-byte to a real broker.
pub mod frame;

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

use frame::{parse_request_prefix, read_correlation_id, CapturedFrame, Pending};

/// Callback invoked once per fully-read frame in either direction.
pub type Recorder = Arc<dyn Fn(CapturedFrame) + Send + Sync>;

/// Bind a listener, accept connections, and relay each to `upstream`,
/// recording frames. Returns the bound local address (useful when the caller
/// passes port 0). The accept loop runs on a background thread for the
/// process lifetime.
pub fn spawn(
    listen: impl ToSocketAddrs,
    upstream: &str,
    recorder: Recorder,
) -> io::Result<std::net::SocketAddr> {
    let listener = TcpListener::bind(listen)?;
    let addr = listener.local_addr()?;
    let upstream = upstream.to_string();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { continue };
            let upstream = upstream.clone();
            let recorder = recorder.clone();
            thread::spawn(move || {
                if let Err(e) = handle_conn(client, &upstream, recorder) {
                    eprintln!("tap conn error: {e}");
                }
            });
        }
    });
    Ok(addr)
}

fn handle_conn(client: TcpStream, upstream: &str, recorder: Recorder) -> io::Result<()> {
    let server = TcpStream::connect(upstream)?;
    let pending = Arc::new(Mutex::new(Pending::default()));

    let c2s_client = client.try_clone()?;
    let c2s_server = server.try_clone()?;
    let pend_req = pending.clone();
    let rec_req = recorder.clone();
    let t = thread::spawn(move || {
        let _ = pump(c2s_client, c2s_server, true, pend_req, rec_req);
    });

    pump(server, client, false, pending, recorder)?;
    let _ = t.join();
    Ok(())
}

/// Copy length-prefixed frames from `src` to `dst`, teeing each to the
/// recorder. `is_request` selects header parsing vs correlation lookup.
fn pump(
    mut src: TcpStream,
    mut dst: TcpStream,
    is_request: bool,
    pending: Arc<Mutex<Pending>>,
    recorder: Recorder,
) -> io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = src.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(()); }
            return Err(e);
        }
        let n = i32::from_be_bytes(len_buf);
        if n < 0 { return Ok(()); }
        let mut body = vec![0u8; n as usize];
        src.read_exact(&mut body)?;
        // Forward verbatim FIRST so latency/ordering is unaffected.
        dst.write_all(&len_buf)?;
        dst.write_all(&body)?;
        dst.flush()?;
        // Then classify + record.
        if is_request {
            if let Some(p) = parse_request_prefix(&body) {
                pending.lock().unwrap().record(p.correlation_id, p.api_key, p.api_version);
                recorder(CapturedFrame { api_key: p.api_key, version: p.api_version, is_request: true, body });
            }
        } else if let Some(corr) = read_correlation_id(&body) {
            if let Some((api_key, version)) = pending.lock().unwrap().take(corr) {
                recorder(CapturedFrame { api_key, version, is_request: false, body });
            }
        }
    }
}
```

- [ ] **Step 4: Implement the thin bin `main.rs`**

```rust
//! Standalone tap: `kafka-tap <listen> <upstream> <spool.ndjson>`.
//! Writes one JSON record per frame to the spool file.
use std::io::Write;
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::{spawn, Recorder};
use crabka_kafka_tap::frame::CapturedFrame;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (listen, upstream, spool) = (&args[1], &args[2], args[3].clone());
    let file = Arc::new(Mutex::new(std::fs::File::create(&spool).unwrap()));
    let rec: Recorder = Arc::new(move |f: CapturedFrame| {
        let mut hex = String::with_capacity(f.body.len() * 2);
        for b in &f.body { use std::fmt::Write; let _ = write!(hex, "{b:02x}"); }
        let line = format!(
            "{{\"api_key\":{},\"version\":{},\"is_request\":{},\"body_hex\":\"{}\"}}\n",
            f.api_key, f.version, f.is_request, hex
        );
        file.lock().unwrap().write_all(line.as_bytes()).unwrap();
    });
    let addr = spawn(listen.as_str(), upstream, rec).unwrap();
    eprintln!("kafka-tap listening on {addr} -> {upstream}, spooling to {spool}");
    loop { std::thread::sleep(std::time::Duration::from_secs(3600)); }
}
```

- [ ] **Step 5: Run the relay test + clippy, expect pass**

Run: `cargo test -p crabka-kafka-tap --test relay`
Expected: PASS.
Run: `cargo clippy -p crabka-kafka-tap --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/kafka-tap/src/lib.rs crates/kafka-tap/src/main.rs crates/kafka-tap/tests/relay.rs
git commit -m "feat(kafka-tap): verbatim TCP relay teeing frames to a recorder"
```

---

## Task 4: Generalize `corpus_replay.rs` to all pairs + coverage assertion

**Files:**
- Modify: `crates/protocol/tests/corpus_replay.rs`
- Depends on: Task 1 (`roundtrip`, `CASES`).

The test must (a) round-trip *every* entry via `roundtrip(name, version, bytes)` keyed by `(api_key, direction)` → name, and (b) assert the corpus covers exactly the `Kind::Request`/`Kind::Response` `CASES` pairs.

- [ ] **Step 1: Rewrite the test**

Replace `crates/protocol/tests/corpus_replay.rs` with:

```rust
use assert2::assert;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/differential_table.rs"));

#[derive(Debug, Deserialize)]
struct Meta {
    api_key: i16,
    version: i16,
    direction: String,
    #[allow(dead_code)]
    source_kafka_version: String,
    #[allow(dead_code)]
    synthetic: bool,
    #[allow(dead_code)]
    description: String,
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn load_pair(stem: &Path) -> (Meta, Vec<u8>) {
    let hex_raw: String = fs::read_to_string(stem.with_extension("hex"))
        .unwrap()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let bytes = hex::decode(hex_raw).unwrap();
    let meta: Meta =
        toml::from_str(&fs::read_to_string(stem.with_extension("toml")).unwrap()).unwrap();
    (meta, bytes)
}

/// Map (api_key, direction) to the message name via the generated CASES table.
fn name_for(api_key: i16, is_request: bool) -> Option<&'static str> {
    CASES.iter().find(|c| {
        c.api_key == api_key
            && matches!(
                (c.kind, is_request),
                (Kind::Request, true) | (Kind::Response, false)
            )
    }).map(|c| c.name)
}

#[test]
fn corpus_round_trips() {
    let dir = corpus_dir();
    let mut seen: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    let mut entries = 0;
    for e in fs::read_dir(&dir).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") {
            continue;
        }
        let stem = path.with_extension("");
        let (meta, bytes) = load_pair(&stem);
        entries += 1;
        let is_request = match meta.direction.as_str() {
            "request" => true,
            "response" => false,
            other => panic!("bad direction {other} in {}", stem.display()),
        };
        let name = name_for(meta.api_key, is_request)
            .unwrap_or_else(|| panic!("no CASES name for api_key {} in {}", meta.api_key, stem.display()));
        let re = roundtrip(name, meta.version, &bytes);
        assert!(re == bytes, "byte mismatch in {} ({name} v{})", stem.display(), meta.version);
        assert!(seen.insert((meta.api_key, meta.version, is_request)),
            "duplicate corpus entry for {} v{} {}", meta.api_key, meta.version, meta.direction);
    }
    assert!(entries > 0, "corpus is empty");
}

/// The corpus must cover every Request/Response (api_key, version) pair in CASES.
#[test]
fn corpus_covers_all_pairs() {
    let mut want: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    for c in CASES {
        match c.kind {
            Kind::Request => { want.insert((c.api_key, c.version, true)); }
            Kind::Response => { want.insert((c.api_key, c.version, false)); }
            Kind::RequestHeader | Kind::ResponseHeader => {}
        }
    }
    let mut have: BTreeSet<(i16, i16, bool)> = BTreeSet::new();
    for e in fs::read_dir(corpus_dir()).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") { continue; }
        let (meta, _) = load_pair(&path.with_extension(""));
        have.insert((meta.api_key, meta.version, meta.direction == "request"));
    }
    let missing: Vec<_> = want.difference(&have).collect();
    assert!(missing.is_empty(), "corpus missing {} pair(s): {:?}", missing.len(), missing);
}
```

- [ ] **Step 2: Run the round-trip test against the existing single entry**

Run: `cargo test -p crabka-protocol --test corpus_replay corpus_round_trips`
Expected: PASS (the one `api_versions_request_v3_001` entry round-trips via `roundtrip("ApiVersionsRequest", 3, ..)`).

> `corpus_covers_all_pairs` will FAIL until Task 7 generates the full corpus. That is expected and acceptable at this point — note it and proceed. Do not weaken the assertion.

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/tests/corpus_replay.rs
git commit -m "feat(protocol): generalize corpus replay to all pairs + coverage gate"
```

---

## Task 5: Capture harness — broker boot, tap wiring, driver battery

**Files:**
- Create: `crates/protocol/tests/capture_corpus.rs`
- Create: `crates/protocol/tests/support/driver.rs`
- Modify: `crates/protocol/Cargo.toml` (add `crabka-kafka-tap` as a dev-dependency)
- Depends on: Task 1, Task 3.

This is the `#[ignore]`, Docker-gated generator. It is not run in normal CI; it produces the committed corpus.

- [ ] **Step 1: Add the dev-dependency**

In `crates/protocol/Cargo.toml` `[dev-dependencies]`:

```toml
crabka-kafka-tap = { path = "../kafka-tap" }
```

Run: `cargo build -p crabka-protocol --tests` — expected: builds.

- [ ] **Step 2: Write the broker+tap boot scaffold**

Create `crates/protocol/tests/capture_corpus.rs`. Model the docker helpers on `crates/broker/tests/describe_groups_jvm.rs` but use the 4.3.0 image and advertise the tap endpoint:

```rust
//! Docker-gated, #[ignore] corpus generator. Boots apache/kafka:4.3.0, routes
//! real JVM-client traffic through an in-process kafka-tap, captures one frame
//! per (api_key, version, direction), then synthesizes the remainder via the
//! JVM oracle. Run manually:
//!   cargo test -p crabka-protocol --test capture_corpus -- --ignored --nocapture
mod support;
use support::driver;
use support::oracle;

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::frame::CapturedFrame;
use crabka_kafka_tap::{spawn, Recorder};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/differential_table.rs"));

const IMAGE: &str = "apache/kafka:4.3.0";
const CONTAINER: &str = "crabka-corpus-capture";
const BROKER_HOST_PORT: u16 = 19092; // real broker EXTERNAL listener
const TAP_PORT: u16 = 19091;         // clients connect here; tap -> broker

fn docker_rm_f() { let _ = Command::new("docker").args(["rm", "-f", CONTAINER]).output(); }

fn docker_run_broker() {
    docker_rm_f();
    // Advertise the TAP endpoint so all client connections traverse the tap.
    // Inside-container CLI tools reach the host tap via host.docker.internal.
    let advertised = format!(
        "PLAINTEXT://localhost:9092,EXTERNAL://host.docker.internal:{TAP_PORT}"
    );
    let out = Command::new("docker").args([
        "run", "-d", "--name", CONTAINER,
        "--add-host", "host.docker.internal:host-gateway",
        "-p", &format!("{BROKER_HOST_PORT}:{BROKER_HOST_PORT}"),
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", &format!("KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:{BROKER_HOST_PORT},CONTROLLER://0.0.0.0:9093"),
        "-e", &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e", "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
        IMAGE,
    ]).output().expect("docker run");
    assert!(out.status.success(), "docker run failed: {}", String::from_utf8_lossy(&out.stderr));
}

/// Forward the host tap port into the broker so traffic the tap forwards to
/// the real EXTERNAL listener reaches it. The tap dials 127.0.0.1:BROKER_HOST_PORT.
fn tap_upstream() -> String { format!("127.0.0.1:{BROKER_HOST_PORT}") }

fn docker_available() -> bool {
    Command::new("docker").arg("info").output().map(|o| o.status.success()).unwrap_or(false)
}
```

- [ ] **Step 3: Write the driver battery**

Create `crates/protocol/tests/support/driver.rs`. Each op is a bundled-CLI invocation via `docker exec`, pointed at the tap endpoint (so the request traverses the tap). Returns nothing; success is measured by what the tap records.

```rust
//! Declarative battery of real JVM-client operations that, run against the
//! broker through the tap, emit a broad set of (api_key, version) pairs.
use std::process::Command;

/// Bootstrap the JVM CLI tools must dial so traffic traverses the tap.
/// From inside the container, the tap (on the host) is host.docker.internal:TAP_PORT.
pub const BOOTSTRAP: &str = "host.docker.internal:19091";

fn exec(container: &str, args: &[&str]) {
    let out = Command::new("docker")
        .arg("exec").arg(container).args(args)
        .output().expect("docker exec");
    // Tools may legitimately fail (e.g. describe a missing group); we only
    // care about the wire traffic they emit en route. Log, don't assert.
    if !out.status.success() {
        eprintln!("driver op {:?} stderr: {}", args, String::from_utf8_lossy(&out.stderr));
    }
}

/// Run the full battery. `bin` is the path to the CLI tools inside the image
/// (apache/kafka:4.3.0 ships them under /opt/kafka/bin).
pub fn run(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    // ApiVersions + Metadata + CreateTopics + DescribeConfigs ...
    exec(container, &[&format!("{t}/kafka-topics.sh"), "--create", "--topic", "corpus-a", "--partitions", "3", "--replication-factor", "1", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-topics.sh"), "--list", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-topics.sh"), "--describe", "--topic", "corpus-a", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-topics.sh"), "--alter", "--topic", "corpus-a", "--partitions", "5", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-configs.sh"), "--describe", "--entity-type", "topics", "--entity-name", "corpus-a", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-configs.sh"), "--alter", "--entity-type", "topics", "--entity-name", "corpus-a", "--add-config", "retention.ms=86400000", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-configs.sh"), "--describe", "--entity-type", "brokers", "--entity-name", "1", bs[0], bs[1]]);
    // Produce + Fetch + offsets.
    exec(container, &["bash", "-lc", &format!("echo 'k1:v1' | {t}/kafka-console-producer.sh --topic corpus-a --property parse.key=true --property key.separator=: --bootstrap-server {BOOTSTRAP}")]);
    exec(container, &["bash", "-lc", &format!("timeout 5 {t}/kafka-console-consumer.sh --topic corpus-a --from-beginning --max-messages 1 --bootstrap-server {BOOTSTRAP} || true")]);
    exec(container, &[&format!("{t}/kafka-get-offsets.sh"), "--topic", "corpus-a", bs[0], bs[1]]);
    // Consumer groups (creates a group, then describe/list/reset).
    exec(container, &["bash", "-lc", &format!("timeout 5 {t}/kafka-console-consumer.sh --topic corpus-a --group cg1 --from-beginning --max-messages 1 --bootstrap-server {BOOTSTRAP} || true")]);
    exec(container, &[&format!("{t}/kafka-consumer-groups.sh"), "--list", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-consumer-groups.sh"), "--describe", "--group", "cg1", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-consumer-groups.sh"), "--describe", "--group", "cg1", "--offsets", bs[0], bs[1]]);
    // ACLs.
    exec(container, &[&format!("{t}/kafka-acls.sh"), "--add", "--allow-principal", "User:alice", "--operation", "Read", "--topic", "corpus-a", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-acls.sh"), "--list", bs[0], bs[1]]);
    // Leader election / reassignment / delete-records / delete topic.
    exec(container, &[&format!("{t}/kafka-leader-election.sh"), "--election-type", "preferred", "--all-topic-partitions", bs[0], bs[1]]);
    exec(container, &[&format!("{t}/kafka-delete-records.sh"), "--offset-json-file", "/dev/stdin", bs[0], bs[1]]); // may no-op
    exec(container, &[&format!("{t}/kafka-topics.sh"), "--delete", "--topic", "corpus-a", bs[0], bs[1]]);
}
```

> The `bin` path (`/opt/kafka/bin`) and script names (`*.sh`) are correct for `apache/kafka` images. If a tool isn't present or a flag differs in 4.3.0, the implementer adjusts; ops that error are logged, not fatal — the synthesis pass (Task 6) backfills anything the battery misses.

- [ ] **Step 4: Wire the capture flow (no post-processing yet) and verify it boots**

Add to `capture_corpus.rs` a test body that boots, taps, drives, and dumps the distinct captured pairs to stderr (post-processing comes in Task 6):

```rust
#[test]
#[ignore = "requires docker + apache/kafka:4.3.0"]
fn capture_and_generate_corpus() {
    if !docker_available() { eprintln!("docker unavailable; skipping"); return; }
    docker_run_broker();
    // Wait for readiness: poll until kafka-topics --list succeeds.
    wait_ready();

    let captured: Arc<Mutex<BTreeMap<(i16, i16, bool), Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let rec: Recorder = {
        let captured = captured.clone();
        Arc::new(move |f: CapturedFrame| {
            captured.lock().unwrap()
                .entry((f.api_key, f.version, f.is_request))
                .or_insert(f.body); // keep first occurrence
        })
    };
    let addr = spawn(("127.0.0.1", TAP_PORT), &tap_upstream(), rec).unwrap();
    eprintln!("tap on {addr} -> {}", tap_upstream());

    driver::run(CONTAINER);
    std::thread::sleep(std::time::Duration::from_secs(2)); // drain in-flight frames

    let pairs = captured.lock().unwrap();
    eprintln!("captured {} distinct (api_key,version,dir) pairs", pairs.len());

    // Task 6 inserts post-processing + synthesis here.

    docker_rm_f();
}

fn wait_ready() {
    for _ in 0..60 {
        let ok = Command::new("docker")
            .args(["exec", CONTAINER, "/opt/kafka/bin/kafka-topics.sh", "--list",
                   "--bootstrap-server", "localhost:9092"])
            .output().map(|o| o.status.success()).unwrap_or(false);
        if ok { return; }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    panic!("broker not ready");
}
```

> Note `wait_ready` dials the broker's own `PLAINTEXT://localhost:9092` (not the tap) — readiness probing shouldn't pollute the capture. Only `driver::run` traffic goes through the tap.

- [ ] **Step 5: Build (do not run the ignored test in CI)**

Run: `cargo build -p crabka-protocol --tests`
Expected: builds. (Running the ignored test needs Docker; that is the manual Task 7 step.)

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/Cargo.toml crates/protocol/tests/capture_corpus.rs crates/protocol/tests/support/driver.rs
git commit -m "feat(protocol): corpus capture harness (broker boot + tap + driver battery)"
```

---

## Task 6: Post-processor + synthesis pass

**Files:**
- Modify: `crates/protocol/tests/capture_corpus.rs`
- Depends on: Task 5, Task 1.

Convert captured frames into corpus files (header stripped, `synthetic=false`), then fill every uncovered `CASES` Request/Response pair via the oracle (`synthetic=true`).

- [ ] **Step 1: Add the writer + post-processor + synthesis, replacing the Task-5 placeholder comment**

```rust
fn corpus_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b { use std::fmt::Write; let _ = write!(s, "{x:02x}"); }
    s
}

fn write_entry(api_key: i16, version: i16, is_request: bool, message_body: &[u8], synthetic: bool, desc: &str) {
    let dir = corpus_dir();
    let name = name_for(api_key, is_request)
        .unwrap_or_else(|| panic!("no CASES name for api_key {api_key}"));
    let dirn = if is_request { "request" } else { "response" };
    let stem = format!("{}_{dirn}_v{version}_001", to_snake(name));
    std::fs::write(dir.join(format!("{stem}.hex")), hex_encode(message_body)).unwrap();
    let toml = format!(
        "api_key = {api_key}\nversion = {version}\ndirection = \"{dirn}\"\nsource_kafka_version = \"4.3.0\"\nsynthetic = {synthetic}\ndescription = \"{desc}\"\n"
    );
    std::fs::write(dir.join(format!("{stem}.toml")), toml).unwrap();
}

/// Same mapping as corpus_replay::name_for, over the included CASES table.
fn name_for(api_key: i16, is_request: bool) -> Option<&'static str> {
    CASES.iter().find(|c| c.api_key == api_key
        && matches!((c.kind, is_request), (Kind::Request, true) | (Kind::Response, false)))
        .map(|c| c.name)
}

fn to_snake(name: &str) -> String {
    // Mirror name_conv::module_name: insert '_' before interior uppercase, lowercase.
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 { out.push('_'); }
            out.push(ch.to_ascii_lowercase());
        } else { out.push(ch); }
    }
    out
}
```

Then in the test body, replace the `// Task 6 inserts ...` comment with:

```rust
    // Clear any previously generated corpus so a re-run is deterministic.
    for e in std::fs::read_dir(corpus_dir()).unwrap() {
        let p = e.unwrap().path();
        if matches!(p.extension().and_then(|s| s.to_str()), Some("hex") | Some("toml")) {
            let _ = std::fs::remove_file(p);
        }
    }

    // Post-process captured frames: strip header -> message body, write synthetic=false.
    let mut covered: std::collections::BTreeSet<(i16, i16, bool)> = std::collections::BTreeSet::new();
    for (&(api_key, version, is_request), frame) in pairs.iter() {
        let Some(name) = name_for(api_key, is_request) else { continue };
        let body = strip_frame_header(name, version, is_request, frame);
        // Validate it round-trips through our codec before committing it.
        let re = roundtrip(name, version, &body);
        if re != body {
            eprintln!("WARN captured {name} v{version} {is_request} does not round-trip; skipping");
            continue;
        }
        write_entry(api_key, version, is_request, &body, false,
            &format!("{name} v{version} captured from apache/kafka:4.3.0 client traffic"));
        covered.insert((api_key, version, is_request));
    }
    eprintln!("wrote {} captured entries", covered.len());

    // Synthesis pass: fill every uncovered CASES Request/Response pair via oracle.
    let mut o = oracle::shared();
    let mut synth = 0usize;
    for c in CASES {
        let is_request = match c.kind {
            Kind::Request => true,
            Kind::Response => false,
            Kind::RequestHeader | Kind::ResponseHeader => continue,
        };
        if covered.contains(&(c.api_key, c.version, is_request)) { continue; }
        let jval = default_json_for(c.name, c.version);
        let body = o.encode(c.api_key, c.version, is_request, &jval);
        // Sanity: oracle bytes must round-trip through our codec.
        let re = roundtrip(c.name, c.version, &body);
        assert!(re == body, "synthetic {} v{} does not round-trip", c.name, c.version);
        write_entry(c.api_key, c.version, is_request, &body, true,
            &format!("{} v{} oracle-synthesized (not realistically client-emitted)", c.name, c.version));
        synth += 1;
    }
    eprintln!("wrote {synth} synthetic entries; total {} pairs", covered.len() + synth);
```

> `o.encode` returns the **message body** (the oracle's `encode` op emits the message, not a framed request) — matching the corpus contract directly, so synthetic entries need no header stripping.

- [ ] **Step 2: Build**

Run: `cargo build -p crabka-protocol --tests`
Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/tests/capture_corpus.rs
git commit -m "feat(protocol): corpus post-processor (header strip) + oracle synthesis pass"
```

---

## Task 7: Generate the committed corpus (manual Docker run)

**Files:**
- Generated: `crates/protocol/tests/corpus/*.{hex,toml}` (committed artifact)
- Depends on: Tasks 1–6.

This step runs the harness once on a machine with Docker to produce the artifact, then validates it with the JVM-free replay gate.

- [ ] **Step 1: Build the JVM oracle (needed by the synthesis pass)**

Run: `(cd tools/oracle && ./gradlew installDist --no-daemon)`
Expected: `tools/oracle/build/install/crabka-oracle/bin/crabka-oracle` exists.

- [ ] **Step 2: Pull the broker image and run the harness**

Run:
```bash
docker pull apache/kafka:4.3.0
cargo test -p crabka-protocol --test capture_corpus -- --ignored --nocapture
```
Expected: stderr shows "wrote N captured entries" then "wrote M synthetic entries; total K pairs", and `crates/protocol/tests/corpus/` now holds `K` `.hex`/`.toml` pairs. The old `api_versions_request_v3_001.*` files are gone (the harness clears the dir first).

- [ ] **Step 3: Validate the artifact with the always-on gates (no Docker/JVM)**

Run:
```bash
cargo test -p crabka-protocol --test corpus_replay
```
Expected: both `corpus_round_trips` and `corpus_covers_all_pairs` PASS. If `corpus_covers_all_pairs` reports missing pairs, the synthesis pass didn't cover them — investigate (likely a `default_json_for`/oracle encode error printed during capture) and re-run Step 2.

- [ ] **Step 4: Sanity-check captured vs synthetic split**

Run:
```bash
grep -rl 'synthetic = false' crates/protocol/tests/corpus/ | wc -l
grep -rl 'synthetic = true'  crates/protocol/tests/corpus/ | wc -l
```
Expected: a non-trivial captured count (dozens — Metadata, ApiVersions, topics/configs/acls/groups/produce/fetch/offsets families) and the remainder synthetic.

- [ ] **Step 5: Commit the generated corpus**

```bash
git add crates/protocol/tests/corpus/
git commit -m "test(protocol): regenerate full captured+synthetic corpus (4.3.0)"
```

---

## Task 8: `recapture-corpus.yml` drift-check workflow

**Files:**
- Create: `.github/workflows/recapture-corpus.yml`
- Reference: `.github/workflows/nightly-differential.yml` for the toolchain/oracle setup steps.

The job re-runs capture against the pinned image and fails if freshly-captured `synthetic=false` bytes diverge from the committed corpus. It does not auto-commit.

- [ ] **Step 1: Add a drift gate to the harness**

Add an env-guarded branch to `capture_corpus.rs` so the same test can run in "check" mode. After writing captured entries to a *temp* dir in check mode, compare against committed `synthetic=false` entries instead of overwriting. Minimal approach — add at the top of the test:

```rust
    let check_only = std::env::var("CORPUS_CHECK_ONLY").is_ok();
```

And in the post-process loop, when `check_only`, instead of `write_entry(...false...)`, compare:

```rust
        if check_only {
            let dirn = if is_request { "request" } else { "response" };
            let stem = format!("{}_{dirn}_v{version}_001", to_snake(name));
            let committed = std::fs::read_to_string(corpus_dir().join(format!("{stem}.hex")))
                .unwrap_or_default();
            let committed: String = committed.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(committed == hex_encode(&body),
                "DRIFT: {name} v{version} {dirn} differs from committed corpus");
            continue;
        }
```

Wrap the dir-clearing and synthesis pass in `if !check_only { ... }` so check mode is read-only.

Run: `cargo build -p crabka-protocol --tests` — expected: builds.

- [ ] **Step 2: Write the workflow**

`.github/workflows/recapture-corpus.yml`:

```yaml
name: recapture-corpus
on:
  workflow_dispatch:

jobs:
  drift-check:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: arduino/setup-protoc@v3
      - uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: 17
      - name: Build JVM oracle
        run: (cd tools/oracle && ./gradlew installDist --no-daemon)
      - name: Pull broker image
        run: docker pull apache/kafka:4.3.0
      - name: Capture-and-compare (fails on drift)
        env:
          CORPUS_CHECK_ONLY: "1"
        run: cargo test -p crabka-protocol --test capture_corpus -- --ignored --nocapture
      - name: Confirm committed corpus still passes the replay gate
        run: cargo test -p crabka-protocol --test corpus_replay
```

> Verify the exact `setup-protoc`/`rust-cache`/`setup-java` action versions match what `.github/workflows/nightly-differential.yml` already uses, and copy those versions to avoid CI drift.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/recapture-corpus.yml crates/protocol/tests/capture_corpus.rs
git commit -m "ci: workflow_dispatch corpus drift check against pinned 4.3.0"
```

---

## Task 9: Remove the KNOWN_ISSUES deviation + fix the spec path

**Files:**
- Modify: `KNOWN_ISSUES.md`
- Modify: `docs/superpowers/specs/2026-06-08-crabka-captured-traffic-corpus-design.md`
- Depends on: Task 7 green (corpus exists and passes).

- [ ] **Step 1: Remove the deviation section**

Delete the entire `## Captured-traffic corpus deviation from coverage acceptance criterion #9` section (and its trailing blank line) from `KNOWN_ISSUES.md`.

- [ ] **Step 2: Fix the spec's crate path**

In `docs/superpowers/specs/2026-06-08-crabka-captured-traffic-corpus-design.md`, change the `tools/kafka-tap/` references in the File layout section to `crates/kafka-tap/` (lib+bin, `publish=false`), reflecting the workspace `members = ["crates/*"]` reconciliation.

- [ ] **Step 3: Verify the issue is gone**

Run: `grep -c "acceptance criterion #9" KNOWN_ISSUES.md`
Expected: `0`.

- [ ] **Step 4: Commit**

```bash
git add KNOWN_ISSUES.md docs/superpowers/specs/2026-06-08-crabka-captured-traffic-corpus-design.md
git commit -m "docs: close criterion-#9 captured-traffic corpus deviation"
```

---

## Final verification (run after all tasks + Task 7 generation)

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace` (the corpus gates run; `capture_corpus` stays `#[ignore]`)
- [ ] `cargo test -p crabka-protocol --test corpus_replay` — both tests green
- [ ] `KNOWN_ISSUES.md` no longer lists criterion #9
- [ ] Corpus directory holds one `(api_key, version, direction)` entry per `CASES` Request/Response pair

---

## Self-review notes

- **Spec coverage:** tap (Tasks 2–3), advertised-endpoint routing (Task 5), driver battery (Task 5), header-strip to message-body (Tasks 1+6), synthesis remainder (Task 6), `synthetic` flagging (Task 6), always-on replay + coverage gate (Task 4), drift workflow (Task 8), KNOWN_ISSUES removal + superseding the lone hand entry (Tasks 6 clears dir, 9), version pin 4.3.0 throughout. All spec sections map to a task.
- **Header-version correctness** (the one true subtlety) is generated from the IR with the two documented quirks (ApiVersionsResponse→0, ControlledShutdownRequest v0→0) and exercised implicitly by every captured entry round-tripping in Task 7 Step 3 — a wrong header version would leave stray bytes and fail `roundtrip`.
- **Type consistency:** `name_for`, `to_snake`, `roundtrip`, `strip_frame_header`, `default_json_for`, `CASES`/`Kind` used identically across Tasks 4, 6, 8.
- **Known soft spots the implementer verifies against the codebase (flagged inline):** `MessageSpec` flexible-min accessor name; owned header module paths; CLI tool paths/flags in `apache/kafka:4.3.0`; exact `tools/regenerate.sh` invocation; CI action versions.
