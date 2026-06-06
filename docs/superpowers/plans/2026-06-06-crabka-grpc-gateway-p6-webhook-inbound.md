# Crabka gRPC Gateway P6 — HTTP Webhook Inbound Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Add an HTTP webhook-inbound front-end: operator-configured `POST /v1/webhooks/{name}` (HMAC-SHA256 signature verify → JSONPath/header idempotency + key/header mapping → the SAME produce+dedup+authz core, so provider redeliveries dedup to exactly-once into Kafka) plus a generic `POST /v1/produce/{topic}` for authenticated JSON producers.

**Architecture:** A new axum `webhook::router` merged onto the shared listener (like `forward_router`/`health`). Named endpoints come from a TOML config file (`--webhooks-config`, JSONPath compiled at load, mirroring the broker's `file_config`). Each request: size-limit → verify HMAC (constant-time) → extract `idempotency_key` (header or JSONPath) + optional record key/headers → build a `GatewayRecord{value = raw body bytes}` → `ProduceCore::produce(rec, &principal)` → `200 {partition, offset, deduplicated}`. Named endpoints produce as a configured `webhook:{name}` principal; the generic route uses the caller's mTLS/bearer principal — so P5 authz applies uniformly.

**Tech Stack:** `hmac 0.11`-era (`hmac 0.13` + `sha2 0.11`), `subtle` (constant-time eq), `jsonpath-rust 1.0`, `toml` + `serde`, axum 0.8. All workspace deps. No broker change, no new shared crate.

**Out of scope (later):** Webhook OUTBOUND delivery (P7); provider-specific signature schemes beyond generic HMAC-SHA256 (e.g. Stripe `t=,v1=` — note as a follow-up); a `KafkaGrpcGateway` CR for webhook config (P9).

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Subagent shells reset cwd to MAIN repo — prefix every Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p6`, **stacked on `claude/gateway-p5`** (#411 — unmerged; P6 builds on P5's `produce(rec, principal)`, the app assembly, and authz). PR bases on #411, or rebases onto `main` if #411 merges first. Assert `git -C <worktree> rev-parse --abbrev-ref HEAD` == `claude/gateway-p6` before every commit.
- **Git identity:** `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock`.
- **Broker NEVER modified.** Each task ends GREEN: `cargo test -p crabka-grpc-gateway`, `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`, `cargo fmt --check -p crabka-grpc-gateway`.

## Confirmed APIs (investigated)

- HMAC: `hmac::{Hmac, Mac, KeyInit}` + `sha2::Sha256`; `<Hmac<Sha256>>::new_from_slice(secret_bytes).unwrap()` → `.update(&body)` → `.finalize().into_bytes()` (32-byte `GenericArray`). Constant-time compare: `subtle::ConstantTimeEq::ct_eq(a, b).unwrap_u8() == 1`. (Pattern: `crates/security/src/scram/` + `plain.rs`.)
- JSONPath: `jsonpath_rust::parser::parse_json_path(&str) -> Result<JpQuery, _>` (compile once at config load); `jsonpath_rust::query::js_path_process(&JpQuery, &serde_json::Value) -> Result<Vec<ValueRef>, _>`; `ValueRef::val() -> &Value`; `value.as_str()`. (Pattern: `crates/security/src/oauthbearer.rs` + `broker/src/file_config.rs`.)
- Produce: `ProduceCore::produce(&self, rec: GatewayRecord, principal: &crabka_security::Principal) -> Result<RecordOutcome, GatewayError>`. `GatewayRecord { topic: String, key: Option<Bytes>, value: Bytes, headers: Vec<(String, Bytes)>, partition: Option<i32>, timestamp_ms: Option<i64>, idempotency_key: Option<String> }`. `RecordOutcome { partition: i32, offset: i64, deduplicated: bool }`. A `Some(idempotency_key)` routes through dedup (EOS); `None` → plain idempotent produce.
- App assembly (`bin/gateway.rs`): `crabka_grpc_gateway::router(state).merge(health::router(readiness)).merge(forward::forward_router(state.clone())).layer(from_fn(resolve_principal))` then optional `.layer(Extension(bearer))`. Merge `webhook::webhook_router(state.clone())` alongside `forward_router`.
- axum 0.8: `body: axum::body::Bytes` extractor (raw body, default ~2MB limit); `headers: axum::http::HeaderMap`; `Path(name): axum::extract::Path<String>`; `Extension<Arc<AppState>>` + `Option<Extension<crabka_security::Principal>>` compose. Per-handler size check is the simplest configurable limit (no `tower-http`, which is absent).
- Config-file: `toml::from_str::<T>(&std::fs::read_to_string(path)?)` with `#[derive(Deserialize)]`. `toml`, `serde`, `serde_json`, `jsonpath-rust` are workspace deps. (Mirror `broker/src/file_config.rs`.)

## File map

- Modify `crates/grpc-gateway/Cargo.toml` (+`hmac`,`sha2`,`subtle`,`jsonpath-rust`,`toml`,`serde_json` (already?), `hex`/`base64` for sig decode).
- Create `crates/grpc-gateway/src/webhook_config.rs` — TOML config types + `compile()` (JSONPath + sig settings) + HMAC `verify_signature`.
- Create `crates/grpc-gateway/src/webhook.rs` — `webhook_router`, `webhook_handler` (named), `produce_handler` (generic), the flow.
- Modify `crates/grpc-gateway/src/{config.rs, lib.rs, bin/gateway.rs}` + the `GatewayConfig` literals in `tests/{wire,streaming,forwarding,tls,forward_unit,authz}.rs`.
- Create `crates/grpc-gateway/tests/webhook.rs`.

## Batches

- **Batch A:** Task 1 (deps + `webhook_config.rs`: config types, compile, HMAC verify + unit tests) — foundation, solo.
- **Batch B:** Task 2 (`webhook.rs`: router + handlers + flow) — needs T1.
- **Batch C:** Task 3 (config field + bin wiring + literals) — needs T1+T2.
- **Batch D:** Task 4 (integration tests) — needs T3.

---

## Task 1: Deps + webhook config + signature verification

**Files:** Modify `crates/grpc-gateway/Cargo.toml`; create `crates/grpc-gateway/src/webhook_config.rs`; modify `src/lib.rs` (`pub mod webhook_config;`).

- [ ] **Step 1: Deps.** Add to `grpc-gateway/Cargo.toml` `[dependencies]` (workspace pins): `hmac = { workspace = true }`, `sha2 = { workspace = true }`, `subtle = { workspace = true }`, `jsonpath-rust = { workspace = true }`, `toml = { workspace = true }`, and a hex/base64 decoder for signatures — `hex = { workspace = true }` (VERIFY it's a workspace dep; if absent, decode hex manually or use `base64` which IS a workspace dep). `serde_json`/`serde`/`bytes` already deps. (Run `grep -E "^(hmac|sha2|subtle|jsonpath-rust|toml|hex)\b" ../../Cargo.toml` to confirm pins before referencing `workspace = true`.)

- [ ] **Step 2: `webhook_config.rs` — config types + compile.**

```rust
//! Operator-supplied webhook-endpoint config (TOML), compiled at load time:
//! JSONPath expressions are parsed once, signature settings validated. Mirrors
//! the broker's `file_config` pattern.

use std::collections::HashMap;

use jsonpath_rust::parser::model::JpQuery;
use serde::Deserialize;

/// Raw TOML form (one `[[webhooks.endpoints]]` per named endpoint).
#[derive(Debug, Clone, Deserialize)]
pub struct WebhooksFile {
    #[serde(default)]
    pub endpoints: Vec<WebhookEndpoint>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookEndpoint {
    pub name: String,
    pub target_topic: String,
    /// Service principal this endpoint produces as (authz). Default `webhook:{name}`.
    pub principal: Option<String>,
    /// HMAC-SHA256 shared secret. If set, `signature_header` is required.
    pub secret: Option<String>,
    pub signature_header: Option<String>,
    /// "hex" (default) or "base64".
    pub signature_encoding: Option<String>,
    /// Optional `sha256=` style prefix stripped before decoding (e.g. GitHub).
    pub signature_prefix: Option<String>,
    /// Optional replay guard: timestamp header + max age (seconds).
    pub timestamp_header: Option<String>,
    pub timestamp_tolerance_secs: Option<i64>,
    /// `header:<Name>` or `json:<JSONPath>`; absent ⇒ no dedup (plain produce).
    pub idempotency_source: Option<String>,
    /// Optional record-key source: `header:<Name>` or `json:<JSONPath>`.
    pub key_source: Option<String>,
    /// Max body bytes (default 1 MiB).
    pub max_body_bytes: Option<usize>,
}

/// A value source: an HTTP header or a compiled JSONPath into the JSON body.
#[derive(Debug, Clone)]
pub enum Source {
    Header(String),
    JsonPath(JpQuery),
}

impl Source {
    fn parse(spec: &str, ctx: &str) -> Result<Self, String> {
        if let Some(h) = spec.strip_prefix("header:") {
            Ok(Source::Header(h.to_string()))
        } else if let Some(jp) = spec.strip_prefix("json:") {
            let q = jsonpath_rust::parser::parse_json_path(jp)
                .map_err(|e| format!("{ctx}: invalid JSONPath {jp:?}: {e}"))?;
            Ok(Source::JsonPath(q))
        } else {
            Err(format!("{ctx}: must start with 'header:' or 'json:'"))
        }
    }
}

#[derive(Debug, Clone)]
pub enum SigEncoding {
    Hex,
    Base64,
}

/// Validated/compiled endpoint config (the runtime form, keyed by name).
#[derive(Debug, Clone)]
pub struct CompiledWebhook {
    pub target_topic: String,
    pub principal: String,
    pub secret: Option<Vec<u8>>,
    pub signature_header: Option<String>,
    pub signature_encoding: SigEncoding,
    pub signature_prefix: Option<String>,
    pub timestamp_header: Option<String>,
    pub timestamp_tolerance_secs: i64,
    pub idempotency_source: Option<Source>,
    pub key_source: Option<Source>,
    pub max_body_bytes: usize,
}

impl WebhooksFile {
    /// Compile + validate every endpoint. Returns `name -> CompiledWebhook`.
    ///
    /// # Errors
    /// Returns a human-readable message on any invalid endpoint.
    pub fn compile(&self) -> Result<HashMap<String, CompiledWebhook>, String> {
        let mut out = HashMap::new();
        for e in &self.endpoints {
            let ctx = format!("[webhooks {}]", e.name);
            if e.secret.is_some() != e.signature_header.is_some() {
                return Err(format!("{ctx}: `secret` and `signature_header` must be set together"));
            }
            let signature_encoding = match e.signature_encoding.as_deref() {
                None | Some("hex") => SigEncoding::Hex,
                Some("base64") => SigEncoding::Base64,
                Some(o) => return Err(format!("{ctx}: signature_encoding must be hex|base64, got {o:?}")),
            };
            let idempotency_source = e.idempotency_source.as_deref()
                .map(|s| Source::parse(s, &format!("{ctx}.idempotency_source"))).transpose()?;
            let key_source = e.key_source.as_deref()
                .map(|s| Source::parse(s, &format!("{ctx}.key_source"))).transpose()?;
            out.insert(e.name.clone(), CompiledWebhook {
                target_topic: e.target_topic.clone(),
                principal: e.principal.clone().unwrap_or_else(|| format!("webhook:{}", e.name)),
                secret: e.secret.as_ref().map(|s| s.clone().into_bytes()),
                signature_header: e.signature_header.clone(),
                signature_encoding,
                signature_prefix: e.signature_prefix.clone(),
                timestamp_header: e.timestamp_header.clone(),
                timestamp_tolerance_secs: e.timestamp_tolerance_secs.unwrap_or(300),
                idempotency_source,
                key_source,
                max_body_bytes: e.max_body_bytes.unwrap_or(1024 * 1024),
            });
        }
        Ok(out)
    }
}
```

- [ ] **Step 3: HMAC verify + JSONPath/header extraction helpers** (same file). `verify_signature(secret, body, provided, encoding, prefix) -> bool` computes `HMAC-SHA256(secret, body)`, decodes `provided` (after stripping `prefix`) per `encoding`, and `ConstantTimeEq`-compares. `extract_source(src: &Source, headers: &HeaderMap, body_json: Option<&Value>) -> Option<String>` reads a header (to_str) or runs the JSONPath against the parsed body (`js_path_process` → first `.as_str()`). Keep these `pub(crate)`.

- [ ] **Step 4: Unit tests** (`#[cfg(test)]` in `webhook_config.rs`): (a) `verify_signature` accepts a correct hex HMAC and rejects a tampered one + a wrong-length/garbage signature; (b) base64 encoding variant; (c) `WebhooksFile::compile` parses a TOML sample with a `json:$.id` idempotency source + a `header:X-Id` key source, defaults the principal to `webhook:{name}`, and errors on `secret` without `signature_header` and on an invalid JSONPath. Use `toml::from_str` for the parse test.

- [ ] **Step 5:** `lib.rs` `pub mod webhook_config;`. Gates + commit `feat(gateway): webhook endpoint config (TOML+JSONPath) + HMAC-SHA256 verify`.

---

## Task 2: Webhook router + handlers

**Files:** Create `crates/grpc-gateway/src/webhook.rs`; modify `src/lib.rs` (`pub mod webhook;`), `src/error.rs` (a webhook error variant if useful).

- [ ] **Step 1: `webhook.rs`.** `pub fn webhook_router(state: Arc<AppState>) -> Router` with `POST /v1/webhooks/{name}` → `webhook_handler` and `POST /v1/produce/{topic}` → `produce_handler`, `.layer(Extension(state))` (mirror `forward_router`). Response type: `#[derive(Serialize)] struct WebhookResponse { partition: i32, offset: i64, deduplicated: bool }`.

- [ ] **Step 2: `webhook_handler`** (named endpoint):
  - Extractors: `Extension(state)`, `Path(name): Path<String>`, `headers: HeaderMap`, `body: Bytes`. Return `axum::response::Response`.
  - Look up `state.config.webhooks.get(&name)` → 404 if absent.
  - Body-size: `if body.len() > cfg.max_body_bytes { return 413 }`.
  - Signature: if `cfg.signature_header` is set, read the header (401 if missing), and if `cfg.timestamp_header` is set, read+parse the timestamp and reject if older than `tolerance_secs` (401 — replay guard), then `verify_signature(secret, &body, provided, encoding, prefix)` (401 on mismatch).
  - Parse the body to `serde_json::Value` ONCE (only if a `json:` source is configured; tolerate non-JSON bodies when no JSONPath source is used).
  - Extract `idempotency_key = cfg.idempotency_source.map(|s| extract_source(...))` (if the source yields nothing ⇒ 400 "idempotency key not found"); extract optional record `key` similarly.
  - Build `GatewayRecord { topic: cfg.target_topic, key: key.map(Bytes::from), value: body, headers: vec![/* optional: a few X-Webhook-* provenance headers */], partition: None, timestamp_ms: None, idempotency_key }`.
  - Principal: `Principal { name: cfg.principal.clone(), auth_method: AuthMethod::MTls /* operator-trusted endpoint */, groups: vec![] }` — produce as the configured endpoint principal (so P5 authz can target it).
  - `state.produce.produce(rec, &principal).await` → map `Ok` to `200 Json(WebhookResponse)`, `Err(Unauthorized)` → 403, `Err(Unavailable)` → 503, other → 500.

- [ ] **Step 3: `produce_handler`** (generic `/v1/produce/{topic}`):
  - Extractors: `Extension(state)`, `Path(topic): Path<String>`, `headers: HeaderMap`, `principal: Option<Extension<Principal>>`, `body: Bytes`.
  - No HMAC (the caller is authenticated by mTLS/bearer via the P5 layer). Optional idempotency from a standard `Idempotency-Key` header (if present).
  - Build `GatewayRecord { topic, value: body, idempotency_key: header "Idempotency-Key", .. }`; principal = the extension principal or `anonymous`. `produce(rec, &principal)` → JSON response (same mapping).

- [ ] **Step 4:** `lib.rs` `pub mod webhook;`. Gates (module unused until Task 3 — `pub`, no dead-code warnings) + commit `feat(gateway): webhook inbound router + named/generic handlers`.

---

## Task 3: Config field + binary wiring

**Files:** Modify `crates/grpc-gateway/src/config.rs`, `src/bin/gateway.rs`, and the `GatewayConfig` literals in `tests/{wire,streaming,forwarding,tls,forward_unit,authz}.rs`.

- [ ] **Step 1: `config.rs`** — add `pub webhooks: std::collections::HashMap<String, crate::webhook_config::CompiledWebhook>` to `GatewayConfig` (after `authz`). Add `webhooks: HashMap::new(),` (or `Default::default()`) to EVERY `GatewayConfig { ... }` literal — bin + the 6 test files (grep `GatewayConfig {`).

- [ ] **Step 2: `bin/gateway.rs`** — add `--webhooks-config` (env `CRABKA_GATEWAY_WEBHOOKS_CONFIG`, `Option<PathBuf>`). In `main`/`run`: if set, `let f: webhook_config::WebhooksFile = toml::from_str(&std::fs::read_to_string(path)?)?; let webhooks = f.compile().map_err(|e| anyhow::anyhow!(e))?;` else `HashMap::new()`. Put it on `GatewayConfig.webhooks`. Merge the router: `.merge(webhook::webhook_router(state.clone()))` alongside `forward_router` in the `app` assembly. (The webhook routes pass THROUGH the `resolve_principal` layer — harmless; named webhooks ignore the injected principal and use their configured one.)

- [ ] **Step 3:** Gates + commit `feat(gateway): load webhooks TOML + mount webhook router in the binary`.

---

## Task 4: Integration tests

**Files:** Create `crates/grpc-gateway/tests/webhook.rs`.

- [ ] Build an `AppState` with a configured webhook endpoint (compile a `WebhooksFile` in-test or construct `CompiledWebhook` directly) + drive the routes via `tower::ServiceExt::oneshot` over `webhook::webhook_router(state)` (mirror `tests/wire.rs`/`forward_unit.rs`), with an in-process broker for `ProduceCore::new`. Tests:
  1. **valid HMAC produces** — POST a JSON body with a correct `X-Webhook-Signature` (hex HMAC computed in-test) → 200, `deduplicated=false`, record present in the topic.
  2. **invalid HMAC → 401** — tampered signature → 401, nothing produced.
  3. **JSONPath idempotency + redelivery dedups** — endpoint with `idempotency_source = json:$.id`; POST the same body twice → second is `deduplicated=true`, same offset, exactly ONE record in the topic (provider-redelivery EOS).
  4. **header idempotency** — `idempotency_source = header:X-Delivery` works.
  5. **generic `/v1/produce/{topic}`** — POST JSON (no HMAC) → 200, produced; `Idempotency-Key` header dedups on repeat.
  6. **body-size limit → 413**; **unknown endpoint → 404**.
  7. (If authz easy to wire) a SimpleAcl AppState denying the `webhook:{name}` principal → 403.
- [ ] Re-run timing-sensitive tests 3×. Gates. Commit `test(gateway): webhook inbound HMAC/JSONPath/dedup/generic-route`.

---

## Final review + finish

Final adversarial review focusing on: (1) **constant-time** signature compare (no early-return timing leak; reject wrong-length sigs safely); (2) HMAC is over the EXACT raw body bytes (before any JSON re-serialization); (3) **redelivery dedup** — a provider resend with the same idempotency_key produces exactly once (the headline §7 guarantee); (4) authz uniformity — named endpoints produce as `webhook:{name}` and the generic route uses the caller principal; (5) failure mapping (401 sig, 413 size, 404 unknown, 403 authz); (6) no broker change; (7) JSONPath compiled at load (not per-request). Then finish the branch (push + PR stacked on #411 / rebased to main).

## Self-review notes (author)

- **Spec coverage (§7):** `POST /v1/webhooks/{name}` + `POST /v1/produce/{topic}` ✓; per-endpoint topic/secret/sig-header/encoding ✓; idempotency source header|JSONPath ✓; key/header mapping (key_source; header mapping via provenance headers — minimal in v1) ✓; body-size limit ✓; verify→401, build Record(value=raw body)→produce+dedup→200 ✓; redelivery dedup ✓.
- **Replay protection:** the idempotency_key + dedup IS the exactly-once guard for provider redeliveries; the optional `timestamp_header` tolerance is an additional coarse freshness guard. Stripe-style (timestamp folded into the signed payload) is a documented follow-up.
- **Authz:** uniform with P5 — named endpoints produce as a configured `webhook:{name}` principal (operator grants ACLs to it); generic route uses the caller's mTLS/bearer principal. Default AllowAll ⇒ works out of the box.
- **Greenfield:** no compat shims; `webhooks` defaults to an empty map (front-end is opt-in via the config file).
