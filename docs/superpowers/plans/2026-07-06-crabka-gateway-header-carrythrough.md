# Gateway header carry-through (MSG-1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore record headers on the gateway's two consume-egress paths — the gRPC `Subscribe` stream (`Inbound`) and the outbound webhook envelope (`render_envelope`) — so header-riding features (CloudEvents binary mode, tracing, dedup keys) work; carry them losslessly internally with a defined, tested policy at the one lossy hop (the gRPC `map<string,bytes>`).

**Architecture:** Add a lossless `headers` field to `DecodedConsumerRecord`, copy it from the native `ConsumerRecord` in `poll()`, populate `Inbound.headers` (with a null→empty / duplicate→last-wins policy) in `inbound_from_decoded_record`, and add a `headers` array to the outbound JSON envelope in `render_envelope`. No proto change — the reshape to lossless `repeated Header` is deferred and not on MSG-2's critical path.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `prost`/Connect-RPC, `bytes`, `serde_json`, `assert2`, the in-process `Broker::start` + `ConsumeSession`/`ProduceCore` harness, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-gateway-header-carrythrough-design.md`](../specs/2026-07-06-crabka-gateway-header-carrythrough-design.md).

---

## Invariants

1. **Both egress paths carry headers** — the gRPC `Inbound` and the outbound webhook envelope.
2. **Lossless internally** — `DecodedConsumerRecord.headers` and the outbound JSON envelope preserve key + `Option<Bytes>` value, order, and duplicate keys.
3. **Defined lossy-edge policy at the gRPC map** — null value → empty bytes; duplicate key → last-wins; never a silent drop-to-empty; both tested.
4. **Behavior-tested** — assert the produced/delivered value (the `Inbound`, the rendered envelope, the polled record), never source text.
5. **Consume-egress only** — no change to `webhook.rs`/`handlers.rs`/`types.rs` (produce-in / ce-* is MSG-2); no proto change.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** `DecodedConsumerRecord.headers` + `poll()` copy; `inbound_from_decoded_record` population; `render_envelope` header array; the unit + end-to-end tests.
- **Deferred:** CloudEvents ce-* semantics (MSG-2); produce-in header parsing (MSG-2); per-offset ack (MSG-3); KEDA scaler (MSG-4); SDK (MSG-5); the `repeated Header` proto reshape (not required by MSG-2).

---

## File Structure

- **`crates/grpc-gateway/src/consume.rs`** — `DecodedConsumerRecord` (add `headers`); `poll()` (copy from native record).
- **`crates/grpc-gateway/src/streaming.rs`** — `inbound_from_decoded_record` (populate the map); its test mod (assert + fix the existing literal).
- **`crates/grpc-gateway/src/outbound.rs`** — `render_envelope` (header array + comment fix); its test mod.
- **`crates/grpc-gateway/tests/integration_consume.rs`** — end-to-end produce-with-header → poll behavior lock.

The three `DecodedConsumerRecord {` construction sites (verified: `consume.rs:80`, `streaming.rs:487`, plus the struct def at `consume.rs:16`) are all updated in Task 1; there are no others.

---

## Task 1: `DecodedConsumerRecord.headers` + Subscribe-egress population

**Files:**
- Modify: `crates/grpc-gateway/src/consume.rs:16-25` (struct), `crates/grpc-gateway/src/consume.rs:80-89` (`poll()`)
- Modify: `crates/grpc-gateway/src/streaming.rs:146-160` (`inbound_from_decoded_record`), `crates/grpc-gateway/src/streaming.rs:485-517` (test mod)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `streaming.rs` (after `inbound_carries_structured_json_and_schema_metadata`, `:517`). `Bytes` is already imported in this test mod.

```rust
    #[test]
    fn inbound_carries_record_headers() {
        let record = crate::consume::DecodedConsumerRecord {
            topic: "h".to_string(),
            partition: crate::ids::PartitionIndex(0),
            offset: crate::ids::Offset(0),
            timestamp: crate::ids::Timestamp(0),
            key: None,
            value: Bytes::from_static(b"v"),
            schema: None,
            json: None,
            headers: vec![
                ("ce-type".to_string(), Some(Bytes::from_static(b"order"))),
                ("dup".to_string(), Some(Bytes::from_static(b"a"))),
                ("dup".to_string(), Some(Bytes::from_static(b"b"))),
                ("nullv".to_string(), None),
            ],
        };

        let inbound = inbound_from_decoded_record(record);

        assert_eq!(inbound.headers.get("ce-type").map(Vec::as_slice), Some(&b"order"[..]));
        // Duplicate key: last-wins (documented proto3 map policy).
        assert_eq!(inbound.headers.get("dup").map(Vec::as_slice), Some(&b"b"[..]));
        // Null value: empty bytes (documented policy), present not dropped.
        assert_eq!(inbound.headers.get("nullv").map(Vec::as_slice), Some(&b""[..]));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --lib streaming::tests::inbound_carries_record_headers`
Expected: FAIL to compile — `DecodedConsumerRecord` has no field `headers`.

- [ ] **Step 3: Implement**

**(a)** `consume.rs:16-25` — add the field to the struct:

```rust
pub struct DecodedConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp: Timestamp,
    pub key: Option<bytes::Bytes>,
    pub value: bytes::Bytes,
    pub schema: Option<SchemaMeta>,
    pub json: Option<bytes::Bytes>,
    /// Record headers, lossless: key + optional value, order- and
    /// duplicate-preserving (the native record permits both).
    pub headers: Vec<(String, Option<bytes::Bytes>)>,
}
```

**(b)** `consume.rs:80-89` — copy from the native `ConsumerRecord` in `poll()` (add the field to the literal):

```rust
            decoded_batch.push(DecodedConsumerRecord {
                topic: r.topic,
                partition: PartitionIndex(r.partition),
                offset: Offset(r.offset),
                timestamp: Timestamp(r.timestamp),
                key: r.key,
                value,
                schema,
                json,
                headers: r.headers.into_iter().map(|h| (h.key, h.value)).collect(),
            });
```

**(c)** `streaming.rs:146-160` — populate the map under the null→empty / duplicate→last-wins policy (`.collect()` into the prost `HashMap<String, Vec<u8>>` inserts in order, so a later duplicate key overwrites the earlier — last-wins):

```rust
        headers: record
            .headers
            .into_iter()
            .map(|(k, v)| (k, v.map(|b| b.to_vec()).unwrap_or_default()))
            .collect(),
```

**(d)** `streaming.rs:487` — the existing `inbound_carries_structured_json_and_schema_metadata` test constructs a `DecodedConsumerRecord` literal; add `headers: vec![],` to it so it compiles.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-grpc-gateway --lib streaming::tests`
Expected: PASS (both `inbound_carries_record_headers` and the updated existing test).

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/consume.rs crates/grpc-gateway/src/streaming.rs
git commit -m "feat(gateway): carry record headers on the Subscribe egress path"
```

---

## Task 2: Outbound envelope header array

**Files:**
- Modify: `crates/grpc-gateway/src/outbound.rs:280-296` (`render_envelope`), `crates/grpc-gateway/src/outbound.rs:488-532` (test mod)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `outbound.rs` (after `envelope_null_value_when_empty`, `:532`). Import `Header` from `crabka_client_consumer` in the test mod (`ConsumerRecord` is already imported; add `Header` alongside it).

```rust
    #[test]
    fn envelope_carries_headers() {
        let mut rec = rec_with_value(Some(br#"{"n":1}"#));
        rec.headers = vec![
            Header { key: "ce-type".into(), value: Some(Bytes::from_static(b"order")) },
            Header { key: "nullv".into(), value: None },
        ];
        let body = render_envelope("events-3-42", &rec);
        let v: Value = serde_json::from_slice(&body).expect("envelope is JSON");
        let hs = v["headers"].as_array().expect("headers array");
        assert_eq!(hs.len(), 2);
        assert_eq!(hs[0]["key"], "ce-type");
        assert_eq!(hs[0]["value"], B64STD.encode(b"order"));
        assert_eq!(hs[1]["key"], "nullv");
        assert_eq!(hs[1]["value"], Value::Null);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --lib outbound::tests::envelope_carries_headers`
Expected: FAIL — `v["headers"]` is `Null` (no `headers` key), `.as_array()` panics.

- [ ] **Step 3: Implement**

`outbound.rs:280-296` — add the `headers` array to the envelope and correct the doc comment (values base64 to match the envelope's key convention; `null` for a null value). The `b64` helper already exists (used for `key`):

```rust
/// Render the delivery envelope as serialized JSON bytes. The value is embedded
/// as raw JSON when the record value parses as JSON, otherwise wrapped as
/// `{"_base64": "..."}`; the key is base64. Record headers are carried as an
/// ordered `headers` array of `{ "key", "value" }` (value base64, or `null`).
fn render_envelope(event_id: &str, rec: &ConsumerRecord) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "event_id": event_id,
        "topic": rec.topic,
        "partition": rec.partition,
        "offset": rec.offset,
        "timestamp_ms": rec.timestamp,
        "key": rec.key.as_ref().map(|k| b64(k)),
        "value": value_field(rec),
        "headers": rec.headers.iter().map(|h| json!({
            "key": h.key,
            "value": h.value.as_ref().map(|v| b64(v)),
        })).collect::<Vec<_>>(),
    }))
    .unwrap_or_default()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-grpc-gateway --lib outbound::tests`
Expected: PASS (the new test + the three existing envelope tests, which now also emit an empty `headers` array).

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/outbound.rs
git commit -m "feat(gateway): carry record headers on the outbound webhook envelope"
```

---

## Task 3: End-to-end header round-trip (behavior lock)

**Files:**
- Modify: `crates/grpc-gateway/tests/integration_consume.rs`

- [ ] **Step 1: Write the behavior test**

Add a test that produces a real record with a header and asserts the polled `DecodedConsumerRecord` carries it — proving `poll()` copies from the native `ConsumerRecord` end-to-end (the Task-1 unit test used a synthetic record). Mirrors `subscribe_receives_then_commits`; `GatewayRecord.headers` is `Vec<(String, Bytes)>` (non-null on the produce side).

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_carries_record_headers() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "hdr-itest".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let core = ProduceCore::new(&bootstrap, "gw-h", Arc::new(RawCodec), None)
        .await
        .unwrap();
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    core.produce(
        GatewayRecord {
            topic: "hdr-itest".into(),
            key: None,
            value: Bytes::from_static(b"h1"),
            body_structured: None,
            headers: vec![("ce-type".to_string(), Bytes::from_static(b"order"))],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &anon,
    )
    .await
    .unwrap();

    let mut session = ConsumeSession::new(
        &bootstrap,
        "gw-hdr-group",
        "gw-h",
        vec!["hdr-itest".to_string()],
        None,
        Arc::new(RawCodec),
    )
    .await
    .unwrap();

    let mut found = None;
    for _ in 0..20 {
        let batch = session.poll(Duration::from_millis(500)).await.unwrap();
        if let Some(r) = batch.into_iter().find(|r| r.value.as_ref() == b"h1".as_ref()) {
            found = Some(r);
            break;
        }
    }
    let rec = found.expect("record with header consumed");
    check!(
        rec.headers
            == vec![("ce-type".to_string(), Some(Bytes::from_static(b"order")))]
    );

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run to verify**

Run: `cargo test -p crabka-grpc-gateway --test integration_consume poll_carries_record_headers`
Expected: PASS — the produced header survives the native consumer into `DecodedConsumerRecord.headers` (this locks the `poll()` copy from Task 1 against real records; if Task 1's copy regresses, this fails).

- [ ] **Step 3: Commit**

```bash
git add crates/grpc-gateway/tests/integration_consume.rs
git commit -m "test(gateway): end-to-end record-header round-trip through consume"
```

---

## Task 4: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-grpc-gateway` (or `cargo test -p crabka-grpc-gateway`) — PASS, including the two transform unit tests + the end-to-end round-trip.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** both egress paths (Subscribe `Inbound` — Task 1; outbound envelope — Task 2); lossless internal `DecodedConsumerRecord.headers` + `poll()` copy (Task 1); defined+tested null→empty / duplicate→last-wins map policy (Task 1 unit test); lossless JSON envelope with `null` for null values (Task 2); end-to-end behavior lock (Task 3). Deferred set (ce-*/MSG-2, per-offset ack, scaler, SDK, proto reshape) untouched — Scope boundary. ✅

**2. Placeholder scan:** every step has concrete code + exact run commands + expected output. No `TBD`/`TODO`. The three `DecodedConsumerRecord` literal sites are all named and updated (struct `consume.rs:16`, `poll()` `consume.rs:80`, existing test `streaming.rs:487`).

**3. Type consistency:** `DecodedConsumerRecord.headers: Vec<(String, Option<Bytes>)>` (Task 1a) is produced by `poll()` from native `Header{key, value: Option<Bytes>}` (Task 1b) and consumed by `inbound_from_decoded_record` (Task 1c) and asserted in Task 1's test and Task 3's end-to-end. `pb::Inbound.headers` is prost `HashMap<String, Vec<u8>>` — `.get(k).map(Vec::as_slice)` in the test matches. `render_envelope` reads native `ConsumerRecord.headers: Vec<Header>` (Task 2), with `Header`/`Bytes`/`B64STD` imported in that test mod. `GatewayRecord.headers: Vec<(String, Bytes)>` (non-null) is the produce-side input in Task 3.

**4. Invariant check:** both paths carry headers (Tasks 1,2); lossless internally (Task 1a `Option<Bytes>`, Task 2 JSON); defined+tested lossy-edge policy (Task 1 test asserts last-wins + null→empty); behavior-tested not source-text (all three tasks assert delivered values); consume-egress only (no `webhook.rs`/`handlers.rs`/`types.rs`/proto changes); each task green before commit.

**5. Prerequisites:** none — every seam is on landed code (headers wire-exact at `owned.rs:18-21`, materialized at `consumer.rs:117`, proto field present at `gateway.proto:130`); nothing gated on the diskless chapter. This is the named prerequisite for MSG-2 (CloudEvents).
