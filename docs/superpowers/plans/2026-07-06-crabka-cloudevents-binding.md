# CloudEvents Kafka + HTTP protocol binding (MSG-2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the CNCF CloudEvents binding at the gateway — parse/emit `ce-*` attributes in binary mode (headers) and structured mode (`application/cloudevents+json`) on HTTP produce-in and webhook egress, transparent on gRPC — so a Crabka topic is a first-class CloudEvents stream, additively (non-CE traffic unchanged).

**Architecture:** One `ce_translate` module owns prefix translation (HTTP `ce-` ↔ Kafka `ce_`, `datacontenttype`↔bare `content-type`), content-mode detection (case-insensitive prefix test on `application/cloudevents`), and binary↔structured conversion. Ingress (`webhook.rs`) and egress (`outbound.rs`) both map through it; gRPC is transparent.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `axum`/`http` `HeaderMap`, `bytes`, `serde_json`, `base64`, `reqwest`, `thiserror`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-cloudevents-binding-design.md`](../specs/2026-07-06-crabka-cloudevents-binding-design.md).

**PREREQUISITE (narrow):** only the gRPC-Subscribe CE transparency test (Task 5b) is gated on **MSG-1**'s `Inbound.headers` restore. Ingress (Task 2) and webhook egress (Task 3) read `GatewayRecord.headers` / native `ConsumerRecord.headers` respectively and are buildable now.

---

## Invariants

1. **CNCF-conformant naming** — HTTP `ce-*` (hyphen) ↔ Kafka `ce_*` (underscore), swapped **only** at HTTP boundaries; `datacontenttype` ↔ bare `content-type` (never `ce_datacontenttype`).
2. **Additive / opt-in** — a non-CloudEvents produce-in is byte-for-byte unchanged; egress defaults to `Envelope` (today's behavior).
3. **Content-mode detection is a prefix test** — `application/cloudevents` (params stripped, case-insensitive), `-batch` → `415`; never exact-match.
4. **No wire/KIP/proto change** — `ce_*` are ordinary Kafka headers; structured is a raw JSON value; `Record.headers`/`Inbound.headers` already carry them.
5. **Required attrs validated on ingress-CE** — `id`/`source`/`type`/`specversion`; faults → `400`.
6. **Egress replaces, never appends, Content-Type** under CE modes.
7. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the `ce_translate` module; ingress at the two `webhook.rs` seams; egress `content_mode` + serializer; optional gRPC Send validation; the end-to-end round-trip.
- **Deferred:** batch mode (415-reject only); strict URI/RFC3339 attribute validation; auto-populate on ingress; MSG-3/4/5.

---

## File Structure & Batching

- **`crates/grpc-gateway/src/ce_translate.rs`** (new) — Batch A, no conflicts.
- **`crates/grpc-gateway/src/webhook.rs`** (+ `webhook_config.rs` reuse) — Batch B1 ingress; depends on A.
- **`crates/grpc-gateway/src/outbound.rs` + `outbound_config.rs`** — Batch B2 egress; depends on A. Disjoint from B1 → **B1 and B2 run concurrently.**
- **`crates/grpc-gateway/src/handlers.rs`** — Batch B1 (optional gRPC Send validation; same file-owner as the gRPC path).
- **`crates/grpc-gateway/tests/…`** — Batch C end-to-end.

---

## Task 1 (Batch A): The `ce_translate` module

**Files:**
- Create: `crates/grpc-gateway/src/ce_translate.rs` (+ `mod ce_translate;` in `lib.rs`)

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use assert2::{assert, let_assert};

    #[test]
    fn detect_mode_is_prefix_based() {
        assert!(matches!(detect_content_mode(Some("application/cloudevents+json"), false), IngressMode::Structured));
        assert!(matches!(detect_content_mode(Some("application/cloudevents+json; charset=UTF-8"), false), IngressMode::Structured));
        assert!(matches!(detect_content_mode(Some("application/cloudevents"), false), IngressMode::Structured));
        assert!(matches!(detect_content_mode(Some("APPLICATION/CLOUDEVENTS+JSON"), false), IngressMode::Structured));
        assert!(matches!(detect_content_mode(Some("application/cloudevents-batch+json"), false), IngressMode::Batch));
        assert!(matches!(detect_content_mode(Some("application/json"), true), IngressMode::Binary));
        assert!(matches!(detect_content_mode(Some("application/json"), false), IngressMode::NotCloudEvent));
        assert!(matches!(detect_content_mode(None, false), IngressMode::NotCloudEvent));
    }

    #[test]
    fn http_to_kafka_swaps_prefix_and_content_type() {
        let mut h = HeaderMap::new();
        h.insert("ce-id", "42".parse().unwrap());
        h.insert("ce-source", "/x".parse().unwrap());
        h.insert("content-type", "application/avro".parse().unwrap());
        let out = http_headers_to_kafka(&h).unwrap();
        assert!(out.contains(&("ce_id".to_string(), Bytes::from_static(b"42"))));
        assert!(out.contains(&("ce_source".to_string(), Bytes::from_static(b"/x"))));
        // datacontenttype: bare content-type, NOT ce_datacontenttype.
        assert!(out.contains(&("content-type".to_string(), Bytes::from_static(b"application/avro"))));
        assert!(!out.iter().any(|(k, _)| k == "ce_datacontenttype"));
    }

    #[test]
    fn kafka_to_http_round_trips_prefix() {
        let recs = vec![("ce_id".to_string(), Some(Bytes::from_static(b"42")))];
        let out = kafka_headers_to_http(&recs);
        assert!(out.iter().any(|(n, v)| n.as_str() == "ce-id" && v.as_bytes() == b"42"));
    }

    #[test]
    fn validate_binary_requires_the_four() {
        let ok = vec![
            ("ce_id".into(), Bytes::from_static(b"1")),
            ("ce_source".into(), Bytes::from_static(b"/s")),
            ("ce_type".into(), Bytes::from_static(b"t")),
            ("ce_specversion".into(), Bytes::from_static(b"1.0")),
        ];
        assert!(validate_binary_required(&ok).is_ok());
        let missing = vec![("ce_id".into(), Bytes::from_static(b"1"))];
        let_assert!(Err(CeError::MissingAttribute(_)) = validate_binary_required(&missing));
    }

    #[test]
    fn structured_from_binary_emits_data_or_data_base64() {
        let hs = vec![
            ("ce_id".to_string(), Some(Bytes::from_static(b"1"))),
            ("ce_source".to_string(), Some(Bytes::from_static(b"/s"))),
            ("ce_type".to_string(), Some(Bytes::from_static(b"t"))),
            ("ce_specversion".to_string(), Some(Bytes::from_static(b"1.0"))),
            ("content-type".to_string(), Some(Bytes::from_static(b"application/json"))),
        ];
        let j: serde_json::Value = serde_json::from_slice(&structured_from_binary(&hs, br#"{"n":7}"#)).unwrap();
        assert!(j["id"] == "1" && j["datacontenttype"] == "application/json");
        assert!(j["data"]["n"] == 7); // JSON data inlined
        let j2: serde_json::Value = serde_json::from_slice(&structured_from_binary(&hs, &[0xff, 0x00])).unwrap();
        assert!(j2["data_base64"].is_string() && j2.get("data").is_none()); // non-JSON → data_base64
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --lib ce_translate::`
Expected: FAIL — module/functions undefined.

- [ ] **Step 3: Implement the module**

```rust
//! CloudEvents (CNCF) binding translation: HTTP `ce-` <-> Kafka `ce_` prefix,
//! `datacontenttype` <-> bare `content-type`, content-mode detection, and
//! binary<->structured conversion. The single source of truth for CE conformance.

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use base64::{engine::general_purpose::STANDARD as B64STD, Engine as _};
use serde_json::json;

const CE_HTTP: &str = "ce-";
const CE_KAFKA: &str = "ce_";
const CONTENT_TYPE: &str = "content-type";
const REQUIRED: [&str; 4] = ["id", "source", "type", "specversion"];

#[derive(Debug, PartialEq, Eq)]
pub enum IngressMode { Binary, Structured, Batch, NotCloudEvent }

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CeError {
    #[error("missing required CloudEvents attribute: {0}")]
    MissingAttribute(&'static str),
    #[error("CloudEvents attribute header value is not valid UTF-8: {0}")]
    NonUtf8Attribute(String),
    #[error("unsupported CloudEvents specversion (only 1.0)")]
    UnsupportedSpecVersion,
    #[error("malformed structured CloudEvents JSON")]
    MalformedJson,
}

/// Structured iff the media type (lowercased, parameters stripped) starts with
/// `application/cloudevents`; `-batch` first. Else Binary if any `ce-*` header.
#[must_use]
pub fn detect_content_mode(media_type: Option<&str>, has_ce_header: bool) -> IngressMode {
    if let Some(mt) = media_type {
        let base = mt.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
        if base.starts_with("application/cloudevents-batch") { return IngressMode::Batch; }
        if base.starts_with("application/cloudevents") { return IngressMode::Structured; }
    }
    if has_ce_header { IngressMode::Binary } else { IngressMode::NotCloudEvent }
}

/// HTTP `ce-<n>` -> Kafka `ce_<n>`; HTTP `Content-Type` -> Kafka `content-type`.
/// # Errors
/// A `ce-*` header value that is not valid UTF-8 (a required attribute that can't decode).
pub fn http_headers_to_kafka(h: &HeaderMap) -> Result<Vec<(String, Bytes)>, CeError> {
    let mut out = Vec::new();
    for (name, value) in h {
        let n = name.as_str(); // axum lowercases HTTP header names
        if let Some(attr) = n.strip_prefix(CE_HTTP) {
            let v = value.to_str().map_err(|_| CeError::NonUtf8Attribute(n.to_string()))?;
            out.push((format!("{CE_KAFKA}{attr}"), Bytes::copy_from_slice(v.as_bytes())));
        } else if n == CONTENT_TYPE {
            if let Ok(v) = value.to_str() {
                out.push((CONTENT_TYPE.to_string(), Bytes::copy_from_slice(v.as_bytes())));
            }
        }
    }
    Ok(out)
}

/// Kafka `ce_<n>` -> HTTP `ce-<n>`; Kafka `content-type` -> HTTP `Content-Type`.
/// A `ce_*` value that is not a valid HTTP header value is skipped (Risk: skip+log).
#[must_use]
pub fn kafka_headers_to_http(h: &[(String, Option<Bytes>)]) -> Vec<(HeaderName, HeaderValue)> {
    let mut out = Vec::new();
    for (k, v) in h {
        let bytes = v.as_deref().unwrap_or(b"");
        if let Some(attr) = k.strip_prefix(CE_KAFKA) {
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_bytes(format!("{CE_HTTP}{attr}").as_bytes()),
                HeaderValue::from_bytes(bytes),
            ) {
                out.push((name, val));
            }
        } else if k == CONTENT_TYPE {
            if let Ok(val) = HeaderValue::from_bytes(bytes) {
                out.push((HeaderName::from_static("content-type"), val));
            }
        }
    }
    out
}

/// # Errors
/// A missing required attribute, or a `ce_specversion` other than `1.0` (Crabka policy).
pub fn validate_binary_required(h: &[(String, Bytes)]) -> Result<(), CeError> {
    for attr in REQUIRED {
        let key = format!("{CE_KAFKA}{attr}");
        if !h.iter().any(|(k, v)| *k == key && !v.is_empty()) {
            return Err(CeError::MissingAttribute(attr));
        }
    }
    if let Some((_, v)) = h.iter().find(|(k, _)| k == "ce_specversion") {
        if v.as_ref() != b"1.0" { return Err(CeError::UnsupportedSpecVersion); }
    }
    Ok(())
}

/// # Errors
/// Missing required attribute or wrong type in the structured JSON.
pub fn validate_structured_json(v: &serde_json::Value) -> Result<(), CeError> {
    for attr in REQUIRED {
        match v.get(attr).and_then(serde_json::Value::as_str) {
            Some(s) if !s.is_empty() => {}
            _ => return Err(CeError::MissingAttribute(attr)),
        }
    }
    if v.get("specversion").and_then(serde_json::Value::as_str) != Some("1.0") {
        return Err(CeError::UnsupportedSpecVersion);
    }
    Ok(())
}

/// Build an `application/cloudevents+json` document from `ce_*` headers + the
/// record value (`data` when the value is JSON, else CE-spec `data_base64`).
#[must_use]
pub fn structured_from_binary(headers: &[(String, Option<Bytes>)], value: &[u8]) -> Vec<u8> {
    let mut obj = serde_json::Map::new();
    for (k, v) in headers {
        let s = v.as_deref().and_then(|b| std::str::from_utf8(b).ok()).unwrap_or_default();
        if let Some(attr) = k.strip_prefix(CE_KAFKA) {
            obj.insert(attr.to_string(), json!(s));
        } else if k == CONTENT_TYPE {
            obj.insert("datacontenttype".to_string(), json!(s));
        }
    }
    match serde_json::from_slice::<serde_json::Value>(value) {
        Ok(j) => { obj.insert("data".to_string(), j); }
        Err(_) => { obj.insert("data_base64".to_string(), json!(B64STD.encode(value))); }
    }
    serde_json::to_vec(&serde_json::Value::Object(obj)).unwrap_or_default()
}
```

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --lib ce_translate::` → PASS.

```bash
git add crates/grpc-gateway/src/ce_translate.rs crates/grpc-gateway/src/lib.rs
git commit -m "feat(gateway): ce_translate — CloudEvents binding translation + detection"
```

---

## Task 2 (Batch B1): Ingress — HTTP produce-in

**Files:**
- Modify: `crates/grpc-gateway/src/webhook.rs:86-259` (both handlers, the `:198`/`:249` seams)
- Test: `crates/grpc-gateway/tests/webhook.rs` (extend)

- [ ] **Step 1: Write the failing tests**

Behavior tests (extend `tests/webhook.rs`, using the existing webhook test harness): (1) a binary CE POST (`ce-id`/`ce-source`/`ce-type`/`ce-specversion` + `Content-Type: application/avro`) produces a record carrying `ce_*` + `content-type` headers; (2) a structured POST (`Content-Type: application/cloudevents+json; charset=UTF-8`) produces the body verbatim with a single `content-type` header; (3) missing `ce-id` → `400`; (4) a non-UTF-8 `ce-*` value → `400`; (5) a `application/cloudevents-batch+json` POST → `415`; (6) a non-CE POST is byte-identical to today (record headers empty).

- [ ] **Step 2: Run to verify it fails; implement**

At each `GatewayRecord { headers: vec![], .. }` site (`webhook.rs:198`, `:249`), after the existing signature/replay/extraction (unchanged, they run first), compute the mode and fill headers/value. Sketch (both handlers share a helper):

```rust
let media = headers.get("content-type").and_then(|v| v.to_str().ok());
let has_ce = headers.iter().any(|(n, _)| n.as_str().starts_with("ce-"));
let (record_headers, record_value) = match ce_translate::detect_content_mode(media, has_ce) {
    IngressMode::Batch => return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "cloudevents batch unsupported").into_response(),
    IngressMode::Structured => {
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| /* 400 */ )?;
        ce_translate::validate_structured_json(&v).map_err(/* 400 */)?;
        (vec![("content-type".to_string(), Bytes::copy_from_slice(media.unwrap_or("application/cloudevents+json").as_bytes()))], body.clone())
    }
    IngressMode::Binary => {
        let hs = ce_translate::http_headers_to_kafka(&headers).map_err(/* 400 */)?;
        ce_translate::validate_binary_required(&hs).map_err(/* 400 */)?;
        (hs, body.clone())
    }
    IngressMode::NotCloudEvent => (vec![], body.clone()), // unchanged: today's behavior
};
// GatewayRecord { headers: record_headers, value: record_value, .. }
```

Map `CeError` → `400` via the existing error-response path (mirror the schema-validation `400` at `webhook.rs:320-325`). Keep HMAC over the raw `body` and `extract_source` unchanged (a config using a CE attribute as key/idempotency source uses the HTTP hyphen form, e.g. `header:ce-id`).

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --test webhook` → PASS.

```bash
git add crates/grpc-gateway/src/webhook.rs crates/grpc-gateway/tests/webhook.rs
git commit -m "feat(gateway): CloudEvents ingress on HTTP produce-in (binary + structured)"
```

---

## Task 3 (Batch B2): Egress — webhook `content_mode`

**Files:**
- Modify: `crates/grpc-gateway/src/outbound_config.rs:30-190` (`ContentMode` enum, config field, `compile()` guard)
- Modify: `crates/grpc-gateway/src/outbound.rs:185-296` (branch the POST construction + structured serializer)
- Test: `crates/grpc-gateway/tests/outbound.rs` (extend)

Independent of MSG-1 (reads native `ConsumerRecord.headers`).

- [ ] **Step 1: Write the failing tests**

(1) `CloudEventsBinary` egress: a record with `ce_*` headers + a `content-type` header renders a POST with `ce-*` HTTP headers, the raw value as body, `Content-Type` from the record's `content-type` header, and **no** `application/json`; (2) `CloudEventsStructured`: `Content-Type: application/cloudevents+json; charset=UTF-8` and a CE-JSON body; (3) `Envelope` mode: unchanged, incl. `application/json` (regression); (4) `compile()` rejects a static header named `content-type` or `ce-*` under a CE mode; (5) `compile()` rejects `decode_to_json` + `CloudEventsBinary`. (Unit-test the request-building against a captured `reqwest::Request` or a local `mockito`/`wiremock` server per the existing `tests/outbound.rs` pattern.)

- [ ] **Step 2: Run to verify it fails; implement**

`outbound_config.rs`: add `enum ContentMode { Envelope, CloudEventsBinary, CloudEventsStructured }` (default `Envelope`); add `content_mode: ContentMode` to `CompiledSubscription` (`:94-117`) and `#[serde(default)] content_mode` to `OutboundSubscription` (`:30-74`); in `compile()` (`:170-187`) enforce the two rejections.

`outbound.rs`: in `deliver_one`, build `body` + headers by mode. At the request seam (`:201-212`), gate the `content-type: application/json` line (`:205`) to `Envelope` only:

```rust
let mut req = http.post(&sub.target_url)
    .header("X-Crabka-Event-Id", &event_id)
    .header("X-Crabka-Timestamp", ts.to_string());
match sub.content_mode {
    ContentMode::Envelope => { req = req.header("content-type", "application/json").body(body.clone()); }
    ContentMode::CloudEventsBinary => {
        // headers as (String, Option<Bytes>) from the native ConsumerRecord
        let hs: Vec<(String, Option<Bytes>)> = rec.headers.iter().map(|h| (h.key.clone(), h.value.clone())).collect();
        for (n, v) in ce_translate::kafka_headers_to_http(&hs) { req = req.header(n, v); }
        let ct = rec.headers.iter().find(|h| h.key == "content-type").and_then(|h| h.value.as_ref());
        if let Some(ct) = ct { if let Ok(v) = HeaderValue::from_bytes(ct) { req = req.header("content-type", v); } }
        req = req.body(rec.value.clone().unwrap_or_default()); // raw data; sign over this body
    }
    ContentMode::CloudEventsStructured => {
        let hs: Vec<(String, Option<Bytes>)> = rec.headers.iter().map(|h| (h.key.clone(), h.value.clone())).collect();
        let ce_body = if is_structured_at_rest(rec) { rec.value.clone().unwrap_or_default().to_vec() }
                      else { ce_translate::structured_from_binary(&hs, rec.value.as_deref().unwrap_or_default()) };
        req = req.header("content-type", "application/cloudevents+json; charset=UTF-8").body(ce_body);
    }
}
if let Some(sig) = &sig { req = req.header("X-Crabka-Signature", sig); }
for (k, v) in &sub.headers { req = req.header(k, v); }
```

Compute `sig` over the mode-specific body (the signed-bytes-per-mode contract — document + test it). `is_structured_at_rest` = the record's `content-type` header already matches the `application/cloudevents` prefix.

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --test outbound` → PASS.

```bash
git add crates/grpc-gateway/src/outbound.rs crates/grpc-gateway/src/outbound_config.rs crates/grpc-gateway/tests/outbound.rs
git commit -m "feat(gateway): CloudEvents webhook egress (binary + structured content modes)"
```

---

## Task 4 (Batch B1, optional): gRPC Send validation flag

**Files:**
- Modify: `crates/grpc-gateway/src/handlers.rs:130-162`

- [ ] **Step 1:** Add a gateway-level `validate_cloudevents_on_send: bool` (default false). When set, after `to_gateway_record`, if the record's headers assert `ce_specversion`, run `ce_translate::validate_binary_required` and, on error, return a non-retriable `RecordResult` error (mirror the per-record error at `streaming.rs`). Default off = raw passthrough unchanged.
- [ ] **Step 2:** Test: a Send with `ce_specversion` but no `ce_id` → error when the flag is on, unchanged when off. Commit.

```bash
git add crates/grpc-gateway/src/handlers.rs
git commit -m "feat(gateway): optional CloudEvents validation on gRPC Send"
```

---

## Task 5 (Batch C): End-to-end round-trip

**Files:**
- Create: `crates/grpc-gateway/tests/cloudevents_roundtrip.rs`

- [ ] **Step 1 (5a): The HTTP produce-in → Kafka → webhook egress round-trip**

Boot a broker + gateway; POST a **binary** CloudEvent (`ce-*` hyphen) to `/v1/produce/{topic}`; assert the Kafka record carries `ce_*` (underscore) headers; configure a `CloudEventsBinary` webhook subscription to a local capture server; assert the delivered HTTP request has `ce-*` (hyphen) headers with **identical attribute values** and a record-derived `Content-Type` (no `application/json`). Add a structured round-trip asserting egress `Content-Type: application/cloudevents+json; charset=UTF-8`. This is the single test proving the prefix bridge is correct end to end.

- [ ] **Step 2 (5b, MSG-1-gated): gRPC Subscribe transparency**

Assert a record produced with `ce_id/ce_source/ce_type/ce_specversion` surfaces those exact underscore keys in the gRPC `Subscribe` `Inbound.headers`. **Gated on MSG-1** (`Inbound.headers` populated); if MSG-1 is not yet landed, mark `#[ignore]` with a comment referencing the MSG-1 plan, and enable when it lands.

- [ ] **Step 3: Run + commit**

Run: `cargo test -p crabka-grpc-gateway --test cloudevents_roundtrip` → PASS (5a; 5b when MSG-1 is in).

```bash
git add crates/grpc-gateway/tests/cloudevents_roundtrip.rs
git commit -m "test(gateway): end-to-end CloudEvents HTTP<->Kafka<->HTTP round-trip"
```

---

## Task 6: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-grpc-gateway` — PASS, incl. the `ce_translate` conformance units, ingress, egress, and the round-trip.
- [ ] **Step 4:** **Empirical `datacontenttype` check** (per CLAUDE.md "match Kafka"): against a real CloudEvents Kafka SDK / cp-kafka image, confirm interop peers emit bare `content-type` (not `ce_datacontenttype`) — document the result. Commit any formatting.

---

## Self-Review

**1. Spec coverage:** the `ce_translate` module (Task 1); ingress binary+structured+batch-415 (Task 2); egress `content_mode` binary+structured + `compile()` guard + Content-Type replacement (Task 3); optional gRPC Send validation (Task 4); the transparency note as a test (Task 5b); the end-to-end bridge proof (Task 5a); the empirical `datacontenttype` check (Task 6). Deferred set (batch support, strict validation, ingress auto-populate) untouched — Scope boundary. ✅

**2. Placeholder scan:** Task 1 is complete module code + a conformance test suite; Tasks 2/3 give concrete wiring at the named seams (`webhook.rs:198/249`, `outbound.rs:201-212/205`, `outbound_config.rs:94-117/170-187`); the two open decisions (specversion strict-reject, signed-bytes-per-mode) are stated, not hidden. No `TBD`/`TODO`.

**3. Type consistency:** `ce_translate`'s signatures (`http_headers_to_kafka -> Vec<(String,Bytes)>`, `kafka_headers_to_http(&[(String,Option<Bytes>)]) -> Vec<(HeaderName,HeaderValue)>`, `detect_content_mode -> IngressMode`, `structured_from_binary`, `CeError`) are consumed identically by ingress (Task 2, `GatewayRecord.headers: Vec<(String,Bytes)>`) and egress (Task 3, mapping native `ConsumerRecord.headers: Vec<Header{key,value:Option<Bytes>}>` into `(String,Option<Bytes>)`). `ContentMode` is defined once (Task 3) and used in `deliver_one` + `compile()`.

**4. Invariant check:** conformant naming (Task 1 round-trip test + `datacontenttype` exception); additive (Task 2 non-CE byte-identical test); prefix detection + 415 (Task 1/2); no proto/wire change (headers ride the existing maps; verified); required-attr 400s (Task 2); egress Content-Type replaced not appended (Task 3, `application/json` gated to `Envelope`). Each task green before commit.

**5. Prerequisites:** narrow — only Task 5b (gRPC-Subscribe transparency) is gated on MSG-1; ingress and webhook egress are buildable now (produce-side `GatewayRecord.headers` / native `ConsumerRecord.headers`). Batching: Task 1 (new file) → Tasks 2 (`webhook.rs`) and 3 (`outbound.rs`/`outbound_config.rs`) run concurrently on disjoint files → Task 5 crosses both.
