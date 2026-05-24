# Slice 49d — Broker: OAUTHBEARER opaque-token introspection (RFC 7662)

Status: Draft
Date: 2026-05-24
Slice: 49d
Pairs with operator slice(s): 50c (deferred; will land separately)
Umbrella: [OAUTHBEARER full-parity roadmap](2026-05-23-crabka-oauth-parity-roadmap-design.md)

## Goal

Today the broker accepts only signed JWT bearer tokens (slice 49b),
verified via JWKS. Real-world OAuth deployments (especially with
non-OIDC issuers) hand out **opaque** access tokens that the broker
cannot self-validate — it must call the IdP's RFC 7662 introspection
endpoint per token. Slice 49d adds an introspection-based validator
that runs alongside the existing signed-JWT path and supports OIDC
userinfo enrichment for principal/claim mapping.

This unblocks operator slice 50c (CRD field for
`introspectionEndpointUri` + `clientId`/`clientSecret`).

## Deliverables

1. New `OAuthBearerValidator::Introspection(IntrospectionValidator)` variant.
2. `validate` API on the enum becomes `async fn` (existing Unsecured/Signed wrap in `async {}`; Introspection truly awaits HTTP).
3. New `IntrospectionClient` trait in `crates/security` (decouples I/O from claim-checking).
4. New `ReqwestIntrospectionClient` implementation in `crates/broker` (HTTP POST to `introspection_endpoint`, optional GET to `userinfo_endpoint`).
5. New `[oauthbearer]` TOML keys (Section 2).
6. Rename `[oauthbearer].jwks_tls_trust` (slice 49c) → `idp_tls_trust`, semantically shared across JWKS + introspection + userinfo. Coordinated rename in the operator's TOML renderer (`crates/operator/src/controller/listeners.rs`).
7. SASL handler call sites: `.validate(token, now)?` → `.validate(token, now).await?` (mechanical).
8. New `IntrospectionError` enum + `AuthError::IntrospectionTransport(String)` variant for transport-error log surface.
9. Tests (Section 4).
10. STATUS.md entry.

## Non-deliverables (out of scope)

| Item | Status |
|------|--------|
| 50c — operator surface for introspection (CRD + Secret mount + reconciler) | Future slice |
| Hybrid validator (try JWT first, fall back to introspection) | Out — one validator type per listener |
| Token caching | Out — RFC 7662 §4 discourages caching without explicit TTL; SASL is once per connection so the cost is acceptable |
| `client_secret_post` / `private_key_jwt` client auth to introspection | Out — Basic Auth only |
| Outbound mTLS from broker to IdP (broker presenting its own cert) | Not in any roadmap slice |
| Per-listener introspection config | Still rejected by the slice-50 cross-listener canonical-tuple guard; lifts in future 49h |
| Per-token rate-limiting / circuit-breaking of introspection calls | Out — single reqwest timeout (default 10s) is the only safety net |

## Architecture & data flow

### Validator extension

```rust
// crates/security/src/oauthbearer.rs

pub enum OAuthBearerValidator {
    Unsecured(UnsecuredJwsValidator),       // slice 49 (dev only)
    Signed(SignedJwsValidator),             // slice 49b (JWKS-verified JWT)
    Introspection(IntrospectionValidator),  // slice 49d (NEW)
}

impl OAuthBearerValidator {
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
        match self {
            Self::Unsecured(v) => v.validate(token, now_ms),           // sync, wrapped
            Self::Signed(v)    => v.validate(token, now_ms),           // sync, wrapped
            Self::Introspection(v) => v.validate(token, now_ms).await, // truly async
        }
    }
}
```

The existing `Unsecured` / `Signed` validators' inner `validate` methods stay sync — only the enum's outer dispatch is async. Zero runtime cost for the sync paths (no real `await` happens).

### Introspection client trait

`crates/security` stays I/O-free. The trait + a mock impl live there for unit-testing; the concrete reqwest-backed impl lives in `crates/broker` (mirrors slice 49b's JWKS pattern where `crates/security` holds `Jwks*` types and `crates/broker` holds `oauth_jwks::JwksRefresher`).

```rust
#[async_trait::async_trait]
pub trait IntrospectionClient: Send + Sync + std::fmt::Debug {
    /// POST `introspection_endpoint` with the token; return the parsed
    /// JSON response. Caller checks `active` + claims.
    async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError>;

    /// GET `userinfo_endpoint` with `Authorization: Bearer <token>`;
    /// return the parsed JSON response. `Ok(None)` when the validator
    /// is configured without userinfo enrichment.
    async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError>;
}

#[derive(Debug, thiserror::Error)]
pub enum IntrospectionError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("non-2xx response: {0}")]
    Status(u16),
    #[error("invalid JSON body")]
    Parse,
}
```

### Per-token validation flow

For `Introspection(v)`:

1. `v.client.introspect(token).await` → JSON response.
2. Parse `active: bool`. If missing or `false` → `AuthError::InvalidToken`.
3. Apply temporal checks (`exp` / `iat` / `nbf` if present, skew-tolerant). RFC 7662 doesn't mandate these claims, but real-world IdPs include `exp` — honor it when present.
4. If `v.call_userinfo`: `v.client.userinfo(token).await` → merge JSON over the introspection claims. **Userinfo wins** for profile-style claims (`preferred_username`, `email`, `name`); **introspection wins** for authorization claims (`active`, `exp`, `scope`).
5. Check `required_scope` against `scope_claim_name` if configured.
6. Extract principal via `principal_claim_name` (default `sub`).

Transport failures map to `AuthError::IntrospectionTransport(String)` (carries the inner error for broker logs); the client sees a generic SASL failure code.

### Validator construction flow

Selected at broker startup by `FileOAuthBearerConfig::apply_to` based on which endpoint URI is set (Section 2). When `introspection_endpoint_uri` is set, `apply_to` reads `introspection_client_secret_path` from disk, builds the rustls `ClientConfig` if `idp_tls_trust` is set, constructs `ReqwestIntrospectionClient::new(...)`, and wires the resulting `Arc<dyn IntrospectionClient>` into the `IntrospectionValidator`.

## Config / TOML shape

```toml
[oauthbearer]
# Common across all paths (49b-era fields):
valid_issuer_uri              = "https://idp.example/realms/kafka"
expected_audience             = "kafka-broker"
principal_claim_name          = "client_id"
scope_claim_name              = "scope"
required_scope                = "kafka.write"
allowable_clock_skew_ms       = 30000

# Slice 49c (renamed in 49d): TLS trust to ANY IdP endpoint.
idp_tls_trust                 = "/etc/crabka/oauth-jwks-trust/ca.crt"

# Slice 49b path. Mutually exclusive with introspection_endpoint_uri.
jwks_endpoint_uri             = "https://idp.example/.../certs"
jwks_refresh_interval_ms      = 300000

# Slice 49d path. Mutually exclusive with jwks_endpoint_uri.
introspection_endpoint_uri    = "https://idp.example/.../token/introspect"
userinfo_endpoint_uri         = "https://idp.example/.../userinfo"           # optional
introspection_client_id       = "kafka-broker"
introspection_client_secret_path = "/etc/crabka/oauth-introspection/client-secret"
introspection_http_timeout_ms = 10000   # optional; default 10000
```

**Validator selection** in `FileOAuthBearerConfig::apply_to`:

| `jwks_endpoint_uri` | `introspection_endpoint_uri` | Selected validator |
|---|---|---|
| set | unset | `Signed` (49b) |
| unset | set | `Introspection` (49d) |
| set | set | Reject at config-load: `"jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive"` |
| unset | unset | `Unsecured` (49 — dev only) |

**Mandatory fields when `introspection_endpoint_uri` is set:**
- `introspection_client_id` (non-empty string).
- `introspection_client_secret_path` (path to a readable file; content is the password, trailing newline trimmed).

Missing either → config-load error.

**Optional:**
- `userinfo_endpoint_uri` — enables userinfo enrichment.
- `introspection_http_timeout_ms` — default 10000.
- `idp_tls_trust` — shared with JWKS path. When unset, reqwest's default webpki-roots apply.

### `jwks_tls_trust` → `idp_tls_trust` rename (coordinated)

Greenfield rename per CLAUDE.md. Touched in this slice:

| File | Change |
|------|--------|
| `crates/broker/src/file_config.rs` | `FileOAuthBearerConfig.jwks_tls_trust` field renamed; serde tag flips automatically (snake_case). Doc updated to "shared across JWKS, introspection, userinfo". |
| `crates/broker/src/config.rs` | `BrokerConfig.oauthbearer_jwks_tls_trust` → `oauthbearer_idp_tls_trust`. Default impl + `for_tests` updated. |
| `crates/broker/src/oauth_jwks.rs` | `JwksRefresher.tls_trust` field's doc updated (semantic, no rename of the field — it's a private struct field used only for the JWKS path). |
| `crates/broker/src/broker.rs` | The `JwksRefresher { tls_trust: config.oauthbearer_jwks_tls_trust.clone(), ... }` line flips to `oauthbearer_idp_tls_trust`. |
| `crates/operator/src/controller/listeners.rs` | `render_broker_toml` emits `idp_tls_trust = "..."` instead of `jwks_tls_trust = "..."`. One-line change. |
| `crates/operator/src/controller/listeners.rs` tests | The slice-50b TOML tests asserting `jwks_tls_trust = …` flip to `idp_tls_trust = …`. ~2-3 test edits. |
| `crates/broker/src/file_config.rs` tests | The slice-49c tests asserting `cfg.oauthbearer_jwks_tls_trust` flip to `cfg.oauthbearer_idp_tls_trust`. ~2 test edits. |

## Code surface — concrete shapes

### `crates/security/src/oauthbearer.rs`

```rust
#[derive(Debug, Clone)]
pub struct IntrospectionValidator {
    pub client: Arc<dyn IntrospectionClient>,
    pub principal_claim_name: String,
    pub scope_claim_name: String,
    pub required_scope: Option<String>,
    pub call_userinfo: bool,
    pub allowable_clock_skew_ms: i64,
}

impl IntrospectionValidator {
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
        let mut claims = self.client.introspect(token).await
            .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?;
        if claims.get("active").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(AuthError::InvalidToken);
        }
        check_temporal_introspection(&claims, now_ms, self.allowable_clock_skew_ms)?;
        if self.call_userinfo {
            if let Some(ui) = self.client.userinfo(token).await
                .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?
            {
                merge_userinfo_over_introspection(&mut claims, ui);
            }
        }
        check_required_scope(&claims, &self.scope_claim_name, self.required_scope.as_deref())?;
        let name = claims
            .get(&self.principal_claim_name)
            .and_then(serde_json::Value::as_str)
            .ok_or(AuthError::InvalidToken)?
            .to_string();
        Ok(Principal { name, auth_method: AuthMethod::SaslOAuthBearer })
    }
}

fn check_temporal_introspection(
    claims: &serde_json::Value,
    now_ms: i64,
    skew_ms: i64,
) -> Result<(), AuthError> {
    // RFC 7662 doesn't mandate exp/iat/nbf, but honor them when present
    // because all real IdPs include exp. Skew-tolerant — same logic as
    // SignedJwsValidator. Refactor into a shared pub(crate) helper if
    // the existing temporal check in 49b's SignedJwsValidator can be
    // lifted; otherwise inline a near-copy.
    // ...
}

fn merge_userinfo_over_introspection(
    introspection: &mut serde_json::Value,
    userinfo: serde_json::Value,
) {
    // Userinfo overrides for profile-style claims (preferred_username,
    // email, name, given_name, family_name). Introspection overrides for
    // authorization claims (active, exp, iat, scope, client_id, sub). The
    // merge mutates introspection in place.
    if let (Some(obj), serde_json::Value::Object(ui_map)) = (introspection.as_object_mut(), userinfo) {
        for (k, v) in ui_map {
            // Strategy: userinfo writes through except for the protected
            // authorization keys above (which introspection already owns).
            const RESERVED: &[&str] = &["active", "exp", "iat", "nbf", "scope", "client_id", "sub"];
            if !RESERVED.contains(&k.as_str()) {
                obj.insert(k, v);
            }
        }
    }
}
```

`AuthError` gets one new variant:

```rust
#[error("oauthbearer introspection transport: {0}")]
IntrospectionTransport(String),
```

(Treated as `InvalidToken` from the client's perspective at the SASL layer — the error variant exists to carry diagnostic detail for broker logs.)

### `crates/broker/src/oauth_introspection.rs` (new)

```rust
//! HTTP transport for RFC 7662 introspection + OIDC userinfo. Lives here
//! (not in crates/security) so the security crate stays I/O-free,
//! mirroring slice-49b's JWKS-refresher pattern.

use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use async_trait::async_trait;
use crabka_security::oauthbearer::{IntrospectionClient, IntrospectionError};

#[derive(Debug)]
pub struct ReqwestIntrospectionClient {
    client: reqwest::Client,
    introspection_endpoint: String,
    userinfo_endpoint: Option<String>,
    client_id: String,
    client_secret: String,
}

impl ReqwestIntrospectionClient {
    pub fn new(
        introspection_endpoint: String,
        userinfo_endpoint: Option<String>,
        client_id: String,
        client_secret: String,
        tls_trust: Option<&Path>,
        timeout: Duration,
    ) -> Result<Arc<dyn IntrospectionClient>, BuildError> {
        let mut builder = reqwest::Client::builder().timeout(timeout);
        if let Some(path) = tls_trust {
            let cfg = crabka_security::build_client_config_from_pem(path)
                .map_err(BuildError::Tls)?;
            builder = builder.use_preconfigured_tls((*cfg).clone());
        }
        let client = builder.build().map_err(|e| BuildError::Reqwest(e.to_string()))?;
        Ok(Arc::new(Self {
            client,
            introspection_endpoint,
            userinfo_endpoint,
            client_id,
            client_secret,
        }))
    }
}

#[async_trait]
impl IntrospectionClient for ReqwestIntrospectionClient {
    async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError> {
        let resp = self.client
            .post(&self.introspection_endpoint)
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .form(&[("token", token)])
            .send()
            .await
            .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(IntrospectionError::Status(resp.status().as_u16()));
        }
        resp.json::<serde_json::Value>().await.map_err(|_| IntrospectionError::Parse)
    }

    async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError> {
        let Some(endpoint) = &self.userinfo_endpoint else { return Ok(None); };
        let resp = self.client.get(endpoint).bearer_auth(token).send().await
            .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(IntrospectionError::Status(resp.status().as_u16()));
        }
        let json = resp.json::<serde_json::Value>().await.map_err(|_| IntrospectionError::Parse)?;
        Ok(Some(json))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("tls trust: {0}")]
    Tls(#[from] crabka_security::JwksTrustError),
    #[error("reqwest: {0}")]
    Reqwest(String),
}
```

### `crates/broker/src/file_config.rs` + `config.rs`

`FileOAuthBearerConfig` gains:
```rust
#[serde(default)]
pub introspection_endpoint_uri: Option<String>,
#[serde(default)]
pub userinfo_endpoint_uri: Option<String>,
#[serde(default)]
pub introspection_client_id: Option<String>,
#[serde(default)]
pub introspection_client_secret_path: Option<std::path::PathBuf>,
#[serde(default)]
pub introspection_http_timeout_ms: Option<u64>,
```

Plus the `jwks_tls_trust` → `idp_tls_trust` rename.

`apply_to` grows the mutually-exclusive check, reads the client-secret file (sync `std::fs::read_to_string` — fail loudly on missing/unreadable), and constructs the `IntrospectionValidator` via `ReqwestIntrospectionClient::new(...)`.

`BrokerConfig` gains nothing new at the runtime-field level beyond the rename — the `IntrospectionValidator` is held inside `oauthbearer_validator: OAuthBearerValidator`.

### `crates/broker/src/broker.rs` + SASL handler

- `JwksRefresher` literal at `broker.rs:1238` flips `tls_trust: config.oauthbearer_jwks_tls_trust.clone()` → `... .oauthbearer_idp_tls_trust.clone()`. No new spawns for Introspection (no background work — it calls inline per token).
- SASL handler call site: `validator.validate(token, now)?` → `.validate(token, now).await?`. Grep for `oauthbearer_validator.validate` in `crates/broker/src/sasl_handlers.rs` (or wherever the handler lives) and add `.await`.

## Tests

### `crates/security/src/oauthbearer.rs` (new unit tests with `MockIntrospectionClient`)

The mock impl returns pre-canned JSON keyed by token string. Lives in `#[cfg(test)] mod tests` of `oauthbearer.rs` OR in `crates/security/src/lib.rs`'s test module — wherever the existing mock patterns live.

- `introspection_active_true_with_principal_returns_ok`
- `introspection_active_false_rejected`
- `introspection_missing_active_field_rejected`
- `introspection_expired_exp_rejected`
- `introspection_required_scope_honored_string`
- `introspection_required_scope_honored_array`
- `introspection_required_scope_missing_rejected`
- `introspection_userinfo_claims_override_introspection_for_profile_keys`
- `introspection_userinfo_does_not_override_authorization_keys`
- `introspection_userinfo_disabled_when_call_userinfo_false`
- `introspection_transport_error_becomes_invalid_token_at_sasl_layer` (asserts `AuthError::IntrospectionTransport` is returned)
- `introspection_default_principal_claim_sub`
- `introspection_custom_principal_claim_client_id`

Also: extend `crates/security/src/lib.rs` integration tests for `OAuthBearerValidator::validate`'s async dispatch — the existing tests of Unsecured/Signed become `#[tokio::test]`.

### `crates/broker/src/oauth_introspection.rs` (new integration tests)

Reuse 49c's HTTPS test pattern (`tokio-rustls::TlsAcceptor` + `rcgen` self-signed cert with `127.0.0.1` SAN + hand-rolled HTTP/1.1 reply). Add an axum or hand-rolled fixture that serves the introspection + userinfo endpoints.

- `introspection_fetches_active_token_over_https_with_custom_trust` — happy path.
- `introspection_returns_inactive_when_idp_says_inactive`.
- `introspection_returns_transport_error_on_non_2xx`.
- `introspection_userinfo_endpoint_is_called_after_active_introspection`.
- `introspection_userinfo_endpoint_is_not_called_when_endpoint_unset`.
- `introspection_handles_keycloak_response_shape` — fixture matches Keycloak's actual JSON keys + types.
- `introspection_basic_auth_sent_with_configured_client_id_and_secret` — fixture asserts the `Authorization: Basic …` header.
- `introspection_form_body_token_field` — fixture asserts the request body is `token=<value>` (form-encoded).
- `introspection_respects_http_timeout` — fixture sleeps longer than the configured timeout; assert transport error.

### `crates/broker/src/file_config.rs` (new unit tests)

- `apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set`.
- `apply_to_oauthbearer_rejects_both_jwks_and_introspection_set`.
- `apply_to_oauthbearer_introspection_requires_client_id`.
- `apply_to_oauthbearer_introspection_requires_client_secret_path`.
- `apply_to_oauthbearer_introspection_with_userinfo_sets_call_userinfo_true`.
- `apply_to_oauthbearer_introspection_without_userinfo_sets_call_userinfo_false`.
- `apply_to_oauthbearer_renamed_idp_tls_trust_threads_through` (replaces the slice-49c `jwks_tls_trust` test).

### `crates/operator/src/controller/listeners.rs` (test flip)

The slice-50b TOML-render tests (`render_broker_toml_emits_jwks_tls_trust_when_trust_certs_present` etc.) flip the assertion string from `jwks_tls_trust = …` to `idp_tls_trust = …`. ~2-3 test edits.

## File-level change map

| File | Change |
|------|--------|
| `crates/security/src/oauthbearer.rs` | New `IntrospectionValidator` + `IntrospectionClient` trait + `IntrospectionError` enum; new `AuthError::IntrospectionTransport` variant; `OAuthBearerValidator::validate` becomes `async fn`; ~13 new unit tests + a `MockIntrospectionClient` test fixture |
| `crates/security/src/lib.rs` | Re-exports for the new public items |
| `crates/security/Cargo.toml` | Add `async-trait` dev-dep (if not already a dep) for the trait + mock |
| `crates/broker/src/oauth_introspection.rs` | NEW — `ReqwestIntrospectionClient`, `BuildError`; ~9 new HTTPS integration tests via tokio-rustls + rcgen |
| `crates/broker/src/lib.rs` | New `mod oauth_introspection;` |
| `crates/broker/src/file_config.rs` | 5 new `FileOAuthBearerConfig` fields; `jwks_tls_trust` → `idp_tls_trust` rename; `apply_to` validator-selection + secret-file read + `ReqwestIntrospectionClient::new` call; ~6 new unit tests + 1 rename of an existing test |
| `crates/broker/src/config.rs` | `oauthbearer_jwks_tls_trust` → `oauthbearer_idp_tls_trust` rename |
| `crates/broker/src/oauth_jwks.rs` | `JwksRefresher.tls_trust` doc-comment update; rename of caller-passed value (the field name itself can stay if it's an internal struct field — verify) |
| `crates/broker/src/broker.rs` | One line: `tls_trust: config.oauthbearer_idp_tls_trust.clone()` |
| `crates/broker/src/sasl_handlers.rs` (or wherever the OAUTHBEARER SASL handler lives) | `.validate(token, now)?` → `.validate(token, now).await?` (one or two call sites) |
| `crates/operator/src/controller/listeners.rs` | `render_broker_toml`: emit `idp_tls_trust` instead of `jwks_tls_trust` (one line); 2-3 test assertions flipped |
| `STATUS.md` | New `## Slice 49d` entry |

## Acceptance criteria

1. `cargo build -p crabka-security -p crabka-broker -p crabka-operator` clean.
2. `cargo test --workspace` passes — including existing OAUTHBEARER tests, new 49d tests, and the slice-50b operator tests that flip from `jwks_tls_trust` to `idp_tls_trust`.
3. `cargo fmt --check` + `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. CRD-drift gate clean (no operator CRD shape change — just the TOML rendered value).
5. STATUS.md updated.

## Open questions resolved during brainstorming

- **API shape.** Async `validate` on the enum (Unsecured/Signed wrap in `async {}`, Introspection truly awaits). Other approaches (parallel sync+async APIs, push-introspection-out-of-validator) were rejected for simplicity.
- **Userinfo enrichment scope.** Included in 49d (not deferred to 49g, not stubbed). Userinfo wins for profile claims; introspection wins for authorization claims (`active`, `exp`, `scope`, `client_id`, `sub`, `iat`, `nbf`).
- **TLS trust shared vs separate.** Shared — rename `jwks_tls_trust` → `idp_tls_trust`. Operator slice 50b's render-side flip is one line; tests need 2-3 string updates.
- **Client auth method.** HTTP Basic Auth only. `client_secret_post` / `private_key_jwt` deferred to a future slice if real demand emerges.
- **Caching.** None. Per-token round trip. RFC 7662 §4 discourages caching without explicit TTL.
- **Validator selection rule.** Mutually exclusive: `jwks_endpoint_uri` and `introspection_endpoint_uri` cannot both be set. Both unset → `Unsecured` (dev-only). Both set → reject at config-load.
- **Client-secret storage.** Read from file at config-load. Path-based, not literal. Mirrors `idp_tls_trust` and the slice-30 cluster-CA pattern.
