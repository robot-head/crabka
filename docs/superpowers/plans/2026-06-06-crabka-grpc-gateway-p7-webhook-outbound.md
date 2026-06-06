# Crabka gRPC Gateway P7 — HTTP Webhook Outbound Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** An outbound webhook delivery subsystem: per operator-configured subscription, a consumer group tails source topics, renders each record into a signed JSON envelope, and POSTs it to a target URL with **at-least-once, per-partition-ordered** delivery — exponential-backoff retries with head-of-line blocking, dead-letter on exhaustion, backpressure, and an SSRF host allow-list. Exactly-once is impossible over HTTP, so receivers dedup on `X-Crabka-Event-Id`.

**Architecture (forced by the consumer API — no commit-specific-offset, no pause/resume):** Each subscription runs a task with its own `Consumer` (group `__crabka_grpc_wh_{name}`, source topics, manual commit). The loop is **batch-at-a-time**: `poll` → group records by `(topic,partition)` in offset order → deliver each partition's records sequentially (partitions concurrently), each record retried with exponential backoff + jitter up to `max_attempts`, then dead-lettered → once the WHOLE batch is delivered-or-DLQ'd, `commit_sync`. The batch boundary is the commit + backpressure unit; a crash before commit re-delivers the batch (at-least-once; receiver dedups). Head-of-line: a failing record blocks its partition (not others) until 2xx or DLQ.

**Tech Stack:** `crabka-client-consumer` (`Consumer`, batch `poll`, `commit_sync`), `crabka-client-producer` (DLQ), `reqwest` (P4 dep; POST + timeout + `Url::parse` for SSRF), `hmac`/`sha2`/`base64`/`serde_json`/`jsonpath-rust` (P6 deps), `toml`+`serde` config (P6 pattern). **No new deps, no broker change, no client-crate change.**

**Out of scope (documented limitations / later):** record headers in the envelope (`ConsumerRecord` exposes none — omitted); `concurrency_per_partition > 1` (config accepted, but v1 delivers sequentially within a partition = strict order); per-record cryptographic timestamp binding; a `KafkaGrpcGateway` CR (P9).

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Subagent shells reset cwd to MAIN repo — prefix Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p7`, **stacked on `claude/gateway-p6`** (#413 — unmerged; P7 reuses P6's `webhook_config` HMAC + the config-file pattern). PR bases on #413, or rebases onto `main` if #413 merges first. Assert HEAD == `claude/gateway-p7` before commit.
- **Git identity:** `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock` if it changes.
- **Broker NEVER modified.** Each task ends GREEN: `cargo test -p crabka-grpc-gateway`, `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`, `cargo fmt --check -p crabka-grpc-gateway`.

## Confirmed APIs (investigated)

- `crabka_client_consumer::Consumer::builder().bootstrap(..).client_id(..).group_id(..).subscribe(vec![..]).isolation_level(IsolationLevel::ReadCommitted).auto_offset_reset(AutoOffsetReset::Earliest).assignor(Assignor::CooperativeSticky).build().await?` (mirror `dedup/store.rs::run_ownership`). `poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`. `ConsumerRecord { topic: String, partition: i32, offset: i64, leader_epoch: i32, timestamp: i64, key: Option<Bytes>, value: Option<Bytes> }` (NO headers). `commit_sync(&self) -> Result<(), ConsumerError>` (commits ALL assigned partitions' polled position — so deliver the whole batch before committing). `close(&self)`/`assignment()`. **No commit-specific-offset; no pause/resume.**
- `crabka_client_producer::Producer` (mirror `produce.rs`/`dedup/store.rs::write_claim`): `ProducerRecord { topic, partition: Option<i32>, key: Option<Bytes>, value: Option<Bytes>, headers: Vec<Header{key:String, value:Option<Bytes>}>, timestamp_ms: Option<i64> }`; `producer.send(rec).await.await.map_err(|_| Canceled)?.map_err(Producer)?`.
- `reqwest` (gateway dep, `["json","rustls"]`): `Client::builder().timeout(Duration).build()`; `client.post(url).header(k,v).body(Vec<u8>).send().await -> Result<Response,_>`; `resp.status().is_success()`; `reqwest::Url::parse(&str)` → `url.scheme()`, `url.host_str()`.
- HMAC: P6's `webhook_config::verify_signature` uses `<Hmac<Sha256>>::new_from_slice(secret).unwrap().update(body).finalize().into_bytes()`. Add `sign_hmac_hex`/`sign_hmac_base64` there.
- `base64::engine::general_purpose::STANDARD.encode(..)`; `serde_json` for the envelope; `jsonpath_rust::{parser::parse_json_path, query::js_path_process}` for the filter (P6 `Source` already does this).
- Jitter: no `rand` dep — `std::time::SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos()` as a pseudo-random source for full-jitter backoff.
- Bin task-spawn under `shutdown: CancellationToken` (mirror `spawn_membership_reader`/`spawn_ownership_consumer`).

## File map

- Modify `crates/grpc-gateway/src/webhook_config.rs` (add `sign_hmac_hex`/`sign_hmac_base64`).
- Create `crates/grpc-gateway/src/outbound_config.rs` (subscription TOML config + compile + SSRF allow-list validation + filter compile).
- Create `crates/grpc-gateway/src/outbound.rs` (the delivery engine: `run_subscription` loop + deliver/retry/DLQ + envelope + sign + SSRF check).
- Modify `crates/grpc-gateway/src/{config.rs, lib.rs, error.rs, bin/gateway.rs}` + the `GatewayConfig` literals in `tests/{wire,streaming,forwarding,tls,forward_unit,authz,webhook}.rs`.
- Create `crates/grpc-gateway/tests/outbound.rs`.

## Batches (sequential — a layered subsystem)

- **Batch A:** Task 1 (config + HMAC sign) — foundation.
- **Batch B:** Task 2 (delivery engine) — the crux; needs T1.
- **Batch C:** Task 3 (bin wiring) — needs T2.
- **Batch D:** Task 4 (integration tests) — needs T3.

---

## Task 1: Subscription config + HMAC sign

**Files:** Create `crates/grpc-gateway/src/outbound_config.rs`; modify `src/webhook_config.rs`, `src/config.rs`, `src/lib.rs`, and the `GatewayConfig` literals in the 7 test files + bin.

- [ ] **Step 1: HMAC sign helpers** in `webhook_config.rs` (`pub(crate)`), mirroring `verify_signature`'s `Hmac<Sha256>`:

```rust
pub(crate) fn sign_hmac_hex(secret: &[u8], body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<sha2::Sha256>>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}
```

(+ a `_base64` variant via `base64::Engine` if you want both; hex is the default `X-Crabka-Signature` encoding.) Add a unit test asserting `verify_signature` accepts what `sign_hmac_hex` produces (round-trip).

- [ ] **Step 2: `outbound_config.rs`** — TOML subscription config + compile (validate SSRF allow-list + compile the optional filter JSONPath):

```rust
//! Operator-supplied outbound webhook subscriptions (TOML), compiled at load:
//! the target URL's scheme/host is checked against an allow-list (SSRF guard)
//! and any filter JSONPath is parsed once.

use serde::Deserialize;
use jsonpath_rust::parser::model::JpQuery;

#[derive(Debug, Clone, Deserialize)]
pub struct OutboundFile {
    #[serde(default)]
    pub subscriptions: Vec<OutboundSubscription>,
    /// Allowed `scheme://host` targets (SSRF allow-list). A target is permitted
    /// iff its `scheme` + `host` matches an entry. Empty ⇒ deny all (fail-closed).
    #[serde(default)]
    pub allowed_targets: Vec<AllowedTarget>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AllowedTarget {
    pub scheme: String, // "https" (recommended) or "http"
    pub host: String,   // exact host match
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutboundSubscription {
    pub name: String,
    pub source_topics: Vec<String>,
    pub target_url: String,
    pub signing_secret: Option<String>,
    pub dead_letter_topic: Option<String>,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Optional delivery filter: `header:<Name>` (record has no headers ⇒ unsupported, reject)
    /// or `json:<JSONPath>` — record delivered iff the path yields a non-null/non-false value.
    pub filter: Option<String>,
    /// Extra static headers to add to every POST.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}
fn default_max_attempts() -> u32 { 5 }
fn default_base_backoff_ms() -> u64 { 500 }
fn default_max_backoff_ms() -> u64 { 30_000 }
fn default_timeout_ms() -> u64 { 10_000 }

#[derive(Debug, Clone)]
pub struct CompiledSubscription {
    pub name: String,
    pub source_topics: Vec<String>,
    pub target_url: String,
    pub signing_secret: Option<Vec<u8>>,
    pub dead_letter_topic: Option<String>,
    pub max_attempts: u32,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub request_timeout_ms: u64,
    pub filter: Option<JpQuery>,
    pub headers: Vec<(String, String)>,
}

impl OutboundFile {
    /// Validate + compile. SSRF: every subscription's `target_url` must parse and
    /// its `(scheme, host)` must be on `allowed_targets`. Filter `header:` is
    /// rejected (records carry no headers). Returns compiled subscriptions.
    ///
    /// # Errors
    /// Human-readable message on any invalid subscription / disallowed target.
    pub fn compile(&self) -> Result<Vec<CompiledSubscription>, String> {
        let mut out = Vec::new();
        for s in &self.subscriptions {
            let ctx = format!("[outbound {}]", s.name);
            let url = reqwest::Url::parse(&s.target_url)
                .map_err(|e| format!("{ctx}: invalid target_url {:?}: {e}", s.target_url))?;
            let host = url.host_str().ok_or_else(|| format!("{ctx}: target_url has no host"))?;
            let scheme = url.scheme();
            let allowed = self.allowed_targets.iter()
                .any(|a| a.scheme.eq_ignore_ascii_case(scheme) && a.host.eq_ignore_ascii_case(host));
            if !allowed {
                return Err(format!("{ctx}: target {scheme}://{host} not in allowed_targets (SSRF guard)"));
            }
            let filter = match s.filter.as_deref() {
                None => None,
                Some(f) if f.starts_with("json:") => Some(
                    jsonpath_rust::parser::parse_json_path(&f["json:".len()..])
                        .map_err(|e| format!("{ctx}: invalid filter JSONPath: {e}"))?),
                Some(_) => return Err(format!("{ctx}: filter must be 'json:<path>' (records carry no headers)")),
            };
            out.push(CompiledSubscription {
                name: s.name.clone(),
                source_topics: s.source_topics.clone(),
                target_url: s.target_url.clone(),
                signing_secret: s.signing_secret.as_ref().map(|x| x.clone().into_bytes()),
                dead_letter_topic: s.dead_letter_topic.clone(),
                max_attempts: s.max_attempts.max(1),
                base_backoff_ms: s.base_backoff_ms.max(1),
                max_backoff_ms: s.max_backoff_ms.max(s.base_backoff_ms),
                request_timeout_ms: s.request_timeout_ms.max(1),
                filter,
                headers: s.headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            });
        }
        Ok(out)
    }
}
```

- [ ] **Step 3: Unit tests** in `outbound_config.rs`: compile a TOML sample (one subscription + an `allowed_targets` entry) succeeds; a target NOT in `allowed_targets` errors (SSRF); a `header:` filter errors; a `json:$.type` filter compiles; an unparseable `target_url` errors.

- [ ] **Step 4: config.rs + literals + lib.** `GatewayConfig` gains `pub outbound: Vec<crate::outbound_config::CompiledSubscription>` (after `webhooks`). Add `outbound: Vec::new(),` to EVERY `GatewayConfig { ... }` literal (bin + the 7 test files — grep `GatewayConfig {`). `lib.rs`: `pub mod outbound_config;`. Gates + commit `feat(gateway): outbound subscription config (TOML+SSRF allow-list) + HMAC sign`.

---

## Task 2: Delivery engine (the crux)

**Files:** Create `crates/grpc-gateway/src/outbound.rs`; modify `src/lib.rs` (`pub mod outbound;`), `src/error.rs` (if a variant helps).

- [ ] **Step 1: `outbound.rs`** — the per-subscription delivery task. Key shape (fill in the helpers):

```rust
//! Outbound webhook delivery: one task per subscription. Batch-at-a-time so the
//! commit boundary == the delivered boundary (the consumer commits the whole
//! polled position, so we deliver the whole batch before committing). Per
//! partition: deliver in offset order, retry with exponential backoff + jitter,
//! dead-letter on exhaustion. At-least-once; receivers dedup on X-Crabka-Event-Id.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Header, Producer, ProducerRecord};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::error::GatewayError;
use crate::outbound_config::CompiledSubscription;

/// Run a subscription's delivery loop until `shutdown`.
pub async fn run_subscription(
    sub: CompiledSubscription,
    bootstrap: String,
    client_id: String,
    producer: Arc<Producer>,
    shutdown: CancellationToken,
) -> Result<(), GatewayError> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(sub.request_timeout_ms))
        .build()
        .map_err(|e| GatewayError::Other(format!("build outbound http client: {e}")))?;

    let group = format!("__crabka_grpc_wh_{}", sub.name);
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group)
        .subscribe(sub.source_topics.clone())
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .assignor(crabka_client_consumer::Assignor::CooperativeSticky)
        .build()
        .await?;

    let mut poll_err = None;
    loop {
        let batch = tokio::select! {
            () = shutdown.cancelled() => break,
            b = consumer.poll(Duration::from_millis(500)) => match b {
                Ok(b) => b,
                Err(e) => { poll_err = Some(e.into()); break; }
            },
        };
        if batch.is_empty() { continue; }

        // Group by (topic, partition), preserving offset order within each.
        let mut by_part: std::collections::BTreeMap<(String, i32), Vec<ConsumerRecord>> = Default::default();
        for r in batch { by_part.entry((r.topic.clone(), r.partition)).or_default().push(r); }
        for recs in by_part.values_mut() { recs.sort_by_key(|r| r.offset); }

        // Deliver partitions concurrently; records within a partition sequentially (ordered).
        let futures = by_part.into_values().map(|recs| {
            let http = &http;
            let sub = &sub;
            let producer = &producer;
            async move {
                for rec in recs {
                    deliver_one(http, sub, &producer, &rec).await;
                }
            }
        });
        futures::future::join_all(futures).await;

        // Whole batch delivered or dead-lettered ⇒ commit the polled position.
        if let Err(e) = consumer.commit_sync().await {
            tracing::warn!(subscription = %sub.name, error = %e, "outbound commit failed; will redeliver");
        }
    }

    let _ = consumer.close().await;
    match poll_err { Some(e) => Err(e), None => Ok(()) }
}

/// Deliver one record: filter → render → sign → POST with retries → DLQ on exhaustion.
async fn deliver_one(http: &reqwest::Client, sub: &CompiledSubscription, producer: &Producer, rec: &ConsumerRecord) {
    // 1. Filter: if a filter is set and the record's JSON body doesn't match, skip (counts as delivered).
    if let Some(q) = &sub.filter {
        if !passes_filter(q, rec) { return; }
    }
    // 2. Render the envelope (value = JSON if valid else base64; key = base64).
    let event_id = format!("{}-{}-{}", rec.topic, rec.partition, rec.offset);
    let body = render_envelope(&event_id, rec); // Vec<u8> (serialized JSON)
    let ts = now_unix_ms();
    let sig = sub.signing_secret.as_ref().map(|s| crate::webhook_config::sign_hmac_hex(s, &body));

    // 3. POST with exponential backoff + jitter up to max_attempts.
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let mut req = http.post(&sub.target_url)
            .header("X-Crabka-Event-Id", &event_id)
            .header("X-Crabka-Timestamp", ts.to_string())
            .header("content-type", "application/json")
            .body(body.clone());
        if let Some(sig) = &sig { req = req.header("X-Crabka-Signature", sig); }
        for (k, v) in &sub.headers { req = req.header(k, v); }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => return, // delivered
            Ok(resp) => tracing::debug!(subscription=%sub.name, event=%event_id, status=%resp.status(), attempt, "outbound non-2xx"),
            Err(e) => tracing::debug!(subscription=%sub.name, event=%event_id, error=%e, attempt, "outbound request failed"),
        }
        if attempt >= sub.max_attempts {
            dead_letter(producer, sub, rec, &event_id).await; // exhausted ⇒ DLQ + advance
            return;
        }
        tokio::time::sleep(backoff_with_jitter(attempt, sub.base_backoff_ms, sub.max_backoff_ms)).await;
    }
}
```

- [ ] **Step 2: helpers** (same file):
  - `render_envelope(event_id, rec) -> Vec<u8>`: `serde_json::to_vec(&json!({ "event_id": event_id, "topic": rec.topic, "partition": rec.partition, "offset": rec.offset, "timestamp_ms": rec.timestamp, "key": rec.key.as_ref().map(b64), "value": value_field(rec) }))`. `value_field` = `serde_json::from_slice::<Value>(&v).unwrap_or_else(|_| json!({"_base64": b64(&v)}))` (raw JSON if valid, else base64 wrapper); `None` value ⇒ `Value::Null`. (Envelope omits record headers — `ConsumerRecord` has none.)
  - `passes_filter(q, rec) -> bool`: parse `rec.value` to `Value` (non-JSON ⇒ false), `js_path_process(q, &v)` first result is non-null/non-false (mirror the broker's `evaluate_claim_check`).
  - `backoff_with_jitter(attempt, base_ms, max_ms) -> Duration`: `let exp = base_ms.saturating_mul(2u64.saturating_pow(attempt - 1)).min(max_ms); let jitter = SystemTime nanos % (exp/2 + 1); Duration::from_millis(exp/2 + jitter)` (full-ish jitter, no `rand` dep).
  - `dead_letter(producer, sub, rec, event_id)`: if `sub.dead_letter_topic` is set, `producer.send(ProducerRecord { topic: dlq, key: rec.key.clone(), value: rec.value.clone(), headers: vec![Header{key:"x-crabka-dlq-source".into(), value:Some(Bytes::from(event_id...))}, Header{key:"x-crabka-dlq-reason".into(), value:Some("delivery exhausted".into())}], .. })`; log on error. If no DLQ topic ⇒ log + drop (advance anyway — at-least-once already satisfied by retries). Emit a `gateway::audit` style metric/log either way.
  - `now_unix_ms()`, `b64(&[u8]) -> String` (base64 STANDARD).

- [ ] **Step 3:** `lib.rs` `pub mod outbound;`. Gates (module unused until Task 3) + commit `feat(gateway): outbound delivery engine (ordered at-least-once, retry/backoff, DLQ, SSRF)`.

> VERIFY: `futures::future::join_all` — `futures`/`futures-util` is a gateway dep (used by streaming.rs). `ConsumerError -> GatewayError` (the `#[from]` exists). `Producer.send` double-await. `reqwest::RequestBuilder::body(Vec<u8>)` + `.header(name, value)`.

---

## Task 3: Binary wiring

**Files:** Modify `crates/grpc-gateway/src/bin/gateway.rs`.

- [ ] CLI arg `--outbound-webhooks-config` (env `CRABKA_GATEWAY_OUTBOUND_WEBHOOKS_CONFIG`, `Option<PathBuf>`). A `load_outbound(&args) -> anyhow::Result<Vec<CompiledSubscription>>` helper (mirror `load_webhooks`: read → `toml::from_str::<OutboundFile>` → `.compile()`). Put the result on `GatewayConfig.outbound`.
- [ ] In `run`: build a shared `Producer` for DLQ (or reuse the one `ProduceCore` holds — simplest: build a dedicated `Arc<Producer>` for DLQ, idempotent + acks=all, mirroring `dedup/store.rs::write_claim`'s builder). For each `sub` in `config.outbound`, spawn `outbound::run_subscription(sub.clone(), config.bootstrap.clone(), format!("{}-outbound-{}", config.client_id, sub.name), dlq_producer.clone(), shutdown.clone())` under a `spawn_outbound_delivery` helper (mirror `spawn_membership_reader`). Log on task exit.
- [ ] Gates + commit `feat(gateway): load outbound subscriptions + spawn delivery tasks in the binary`.

---

## Task 4: Integration tests

**Files:** Create `crates/grpc-gateway/tests/outbound.rs`.

- [ ] Harness: boot an in-process broker (`tests/forwarding.rs::boot`), create a source topic + produce records (via `crabka_client_producer::Producer` or `AdminClient` + a producer). Spin a **mock HTTP receiver** (axum `Router` on `127.0.0.1:0`, like `tests/forward_unit.rs::spawn_mock`) that records received bodies/headers and returns a configurable status (e.g. an `Arc<Mutex<Vec<Received>>>` + a status switch). Build a `CompiledSubscription` (target_url = the mock addr, allowed) and spawn `outbound::run_subscription` under a token. Tests:
  1. **`delivers_2xx`** — produce N records; the mock (always 200) receives all N, with `X-Crabka-Event-Id = topic-partition-offset`, valid `X-Crabka-Signature` (verify with the secret via `webhook_config::verify_signature`), and a well-formed JSON envelope (value parsed). Cancel; assert all received.
  2. **`retries_then_succeeds`** — mock returns 500 for the first K attempts then 200; assert the record is eventually delivered (received once-or-more; backoff observed) and not dead-lettered.
  3. **`dead_letters_on_exhaustion`** — mock always 500; `max_attempts` small, `dead_letter_topic` set; assert the record lands in the DLQ topic (consume it) with the `x-crabka-dlq-*` headers, and delivery advances (the subscription doesn't wedge).
  4. **`ordering_within_partition`** — single partition, several records; the mock records arrival order; assert events arrive in offset order.
  5. **`filter_skips_nonmatching`** — `filter = json:$.deliver`; records with `{"deliver": false}` are NOT POSTed; `{"deliver": true}` are.
  6. **`ssrf_rejected_at_compile`** — `OutboundFile::compile` rejects a target not in `allowed_targets` (unit-level, no broker).
- [ ] Stabilize: these are timing-sensitive (consume + HTTP + backoff). Generous waits (poll the mock's received-count). Re-run 3×. Gates. Commit `test(gateway): outbound delivery 2xx/retry/DLQ/ordering/filter/SSRF`.

---

## Final review + finish

Final adversarial review focusing on: (1) **at-least-once + commit boundary** — `commit_sync` only after the WHOLE batch is delivered-or-DLQ'd (a crash mid-batch re-delivers; never commits undelivered records); (2) **ordering + head-of-line** — per-partition sequential, a failing record blocks only its partition, DLQ is the escape valve; (3) **SSRF** — every target validated against the allow-list at load (fail-closed on empty), no runtime caller URLs, TLS verification on; (4) **signing** — `X-Crabka-Signature` HMAC over the exact envelope bytes; `X-Crabka-Event-Id` = topic-partition-offset for receiver dedup; (5) **DLQ** — exhaustion produces to the DLQ topic with metadata then advances (no wedge); no-DLQ-configured ⇒ drop+advance after retries; (6) no unbounded retry/﻿task leak (closes the consumer on exit; bounded by max_attempts); (7) no broker/client-crate change. Then finish the branch (push + PR stacked on #413 / rebased to main).

## Self-review notes (author)

- **Spec coverage (§8):** subscription model ✓; consumer group `__crabka_grpc_wh_{name}` ✓; JSON envelope + `X-Crabka-Event-Id`/`-Signature`/`-Timestamp` ✓; at-least-once ordered ✓; 2xx→commit, non-2xx→backoff retry head-of-line, exhaustion→DLQ→advance ✓; backpressure (batch-bounded) ✓; SSRF allow-list ✓; TLS verify on ✓; filter ✓.
- **Forced design (consumer API):** no commit-specific-offset / pause-resume ⇒ batch-at-a-time commit (the batch is the commit + backpressure unit). Documented.
- **Documented limitations:** envelope omits record headers (`ConsumerRecord` exposes none); `concurrency_per_partition>1` accepted but v1 is sequential-within-partition (strict order); jitter via `SystemTime` nanos (no `rand` dep).
- **Greenfield:** no compat shims; `outbound` defaults to empty (opt-in via the config file). No broker / client-crate change.
