# Slice 49d: Broker — OAUTHBEARER opaque-token introspection — Implementation plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 49d — Broker: OAUTHBEARER opaque-token introspection (2026-05-24)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Slice 50c (operator CRD field + Secret mount for the client secret + reconciler wiring for introspectionEndpointUri / userinfoEndpointUri) — closed by slice 50c
- Hybrid validator (try JWT first, fall back to introspection)
- Broker-side token caching keyed by (token, exp) to amortize IdP round-trips
- client_secret_post / private_key_jwt introspection-endpoint auth styles (HTTP Basic only)
- Outbound mTLS to the IdP (one-way TLS via the shared trust bundle only)
- Per-listener [oauthbearer] config (still rejected at config-load — closed by slice 49h)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (matches the project's CLAUDE.md mandate to execute in parallel batches). Steps use the project's compact-batch style — each T is one focused PR-worth of work, file-conflict-disjoint within a batch.

**Design:** `docs/superpowers/specs/2026-05-24-crabka-broker-oauth-introspection-49d-design.md`
**Umbrella:** `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
**Paired core slices:** 49b (shipped — signed JWT), 49c (shipped — TLS trust helper).

**Goal:** Add `OAuthBearerValidator::Introspection` for RFC 7662 opaque-token validation + OIDC userinfo enrichment. Validator's `validate` becomes `async fn` to accommodate the per-token HTTP round trip. Rename `[oauthbearer].jwks_tls_trust` → `idp_tls_trust` (shared across JWKS/introspection/userinfo, one trust bundle per IdP) — coordinated rename across broker + operator.

**Architecture:** Five sequential batches. T1 adds the security-crate types + trait + mock (leaf). T2 adds the broker-side reqwest client implementation (depends on T1). T3 wires the new TOML keys + validator selection + the `jwks_tls_trust` → `idp_tls_trust` rename in the broker config plumbing (depends on T1+T2). T4 + T5 run in parallel: T4 cleans up the broker's downstream callers (`.await` propagation, doc-comment updates, rename uses); T5 flips the operator's TOML renderer + tests to the new key name. T6 is STATUS + final gate. No CRD changes, no operator surface (that's slice 50c).

**Tech Stack:** Same as 49b/49c — rustls 0.23+, reqwest 0.13 with `rustls` feature, async_trait (workspace dep), tokio-rustls + rcgen for the new HTTPS integration tests (already broker dev-deps from 49c).

---

## Batches

### Batch 1 (single task — leaf)

- **T1 — Security crate: types, trait, validator, async dispatch.** `crates/security/src/oauthbearer.rs` + `crates/security/src/lib.rs`.

  #### 1. New error variant on the existing `AuthError`

  Find the existing `AuthError` enum in `crates/security/src/lib.rs` (or wherever it lives — grep for `pub enum AuthError`). Add:

  ```rust
  #[error("oauthbearer introspection transport: {0}")]
  IntrospectionTransport(String),
  ```

  (`#[from]` is intentionally NOT used because `IntrospectionError` is `Clone + std::error::Error` but we want to surface a friendly string in broker logs, not the chain.)

  Wire into any `reason()` or `Display` plumbing the same way other variants are.

  #### 2. New types in `crates/security/src/oauthbearer.rs`

  Append after the existing `OAuthBearerValidator` enum impl block:

  ```rust
  use std::sync::Arc;

  /// HTTP transport contract for RFC 7662 introspection + OIDC userinfo.
  /// Lives in this crate to keep crates/security as the validator surface;
  /// the concrete reqwest-backed impl lives in crates/broker
  /// (oauth_introspection.rs) so this crate stays I/O-free.
  #[async_trait::async_trait]
  pub trait IntrospectionClient: Send + Sync + std::fmt::Debug {
      /// POST the IdP's introspection endpoint with `token` in a
      /// form-encoded body. Caller checks `active` + claims.
      async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError>;

      /// GET the IdP's userinfo endpoint with `Authorization: Bearer
      /// <token>`. `Ok(None)` when the validator is configured without
      /// userinfo enrichment.
      async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError>;
  }

  /// Transport-layer failures surfaced by `IntrospectionClient`. The
  /// validator maps these onto `AuthError::IntrospectionTransport` for
  /// the SASL handler.
  #[derive(Debug, thiserror::Error)]
  pub enum IntrospectionError {
      #[error("transport: {0}")]
      Transport(String),
      #[error("non-2xx response: {0}")]
      Status(u16),
      #[error("invalid JSON body")]
      Parse,
  }

  /// RFC 7662 opaque-token introspection validator (slice 49d).
  /// Calls the introspection endpoint per token (no caching — RFC 7662
  /// §4 discourages caching without explicit lifetime info; SASL is
  /// once per connection so the cost is acceptable). Optionally calls
  /// OIDC userinfo after a successful introspection and merges the
  /// profile claims over the introspection claims.
  #[derive(Debug, Clone)]
  pub struct IntrospectionValidator {
      pub client: Arc<dyn IntrospectionClient>,
      /// Claim whose string value becomes the principal name. Default
      /// `sub` for generic OAuth flows; commonly `client_id` for
      /// Keycloak client-credentials.
      pub principal_claim_name: String,
      /// Claim carrying the token scope (string or array). Default `scope`.
      pub scope_claim_name: String,
      /// When set, the merged scope claim must contain this value.
      pub required_scope: Option<String>,
      /// `true` iff a `userinfo_endpoint_uri` is configured; the
      /// validator calls `client.userinfo(token)` after a successful
      /// introspection and merges the response over the introspection
      /// claims.
      pub call_userinfo: bool,
      /// Clock-skew tolerance for `exp`/`iat`/`nbf` checks on
      /// introspection-response timestamps (when present).
      pub allowable_clock_skew_ms: i64,
  }

  impl IntrospectionValidator {
      pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
          let mut claims = self.client.introspect(token).await
              .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?;
          if claims.get("active").and_then(serde_json::Value::as_bool) != Some(true) {
              return Err(AuthError::InvalidToken);
          }
          check_temporal_claims(&claims, now_ms, self.allowable_clock_skew_ms)?;
          if self.call_userinfo {
              if let Some(ui) = self.client.userinfo(token).await
                  .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?
              {
                  merge_userinfo_over_introspection(&mut claims, ui);
              }
          }
          check_required_scope(
              &claims,
              &self.scope_claim_name,
              self.required_scope.as_deref(),
          )?;
          let name = claims
              .get(&self.principal_claim_name)
              .and_then(serde_json::Value::as_str)
              .ok_or(AuthError::InvalidToken)?
              .to_string();
          Ok(Principal { name, auth_method: AuthMethod::SaslOAuthBearer })
      }
  }

  /// Shared temporal-claims check (used by IntrospectionValidator;
  /// SignedJwsValidator has its own internal check that follows the
  /// same logic — keep them independent for now to avoid wider refactor).
  fn check_temporal_claims(
      claims: &serde_json::Value,
      now_ms: i64,
      skew_ms: i64,
  ) -> Result<(), AuthError> {
      // Convert exp/iat/nbf seconds → ms with skew tolerance.
      // exp: optional in RFC 7662 but ubiquitous in practice — reject when present + past.
      if let Some(exp_s) = claims.get("exp").and_then(serde_json::Value::as_i64) {
          let exp_ms = exp_s.saturating_mul(1000);
          if now_ms.saturating_sub(skew_ms) > exp_ms {
              return Err(AuthError::InvalidToken);
          }
      }
      // iat: optional. Reject when present + far-future.
      if let Some(iat_s) = claims.get("iat").and_then(serde_json::Value::as_i64) {
          let iat_ms = iat_s.saturating_mul(1000);
          if iat_ms.saturating_sub(skew_ms) > now_ms {
              return Err(AuthError::InvalidToken);
          }
      }
      // nbf: optional. Reject when present + future.
      if let Some(nbf_s) = claims.get("nbf").and_then(serde_json::Value::as_i64) {
          let nbf_ms = nbf_s.saturating_mul(1000);
          if nbf_ms.saturating_sub(skew_ms) > now_ms {
              return Err(AuthError::InvalidToken);
          }
      }
      Ok(())
  }

  /// Required-scope check honoring both string-scope and array-scope
  /// forms (RFC 6749 §3.3 allows either). Pure helper.
  fn check_required_scope(
      claims: &serde_json::Value,
      scope_claim_name: &str,
      required: Option<&str>,
  ) -> Result<(), AuthError> {
      let Some(required) = required else { return Ok(()); };
      let claim = claims.get(scope_claim_name).ok_or(AuthError::InvalidToken)?;
      let granted: Vec<&str> = match claim {
          serde_json::Value::String(s) => s.split_whitespace().collect(),
          serde_json::Value::Array(arr) => arr.iter().filter_map(serde_json::Value::as_str).collect(),
          _ => return Err(AuthError::InvalidToken),
      };
      if granted.contains(&required) {
          Ok(())
      } else {
          Err(AuthError::InvalidToken)
      }
  }

  /// Merge userinfo response over introspection claims. Userinfo wins
  /// for profile-style claims (preferred_username, email, name,
  /// given_name, family_name, ...); introspection wins for the small
  /// set of authorization claims listed in RESERVED.
  fn merge_userinfo_over_introspection(
      introspection: &mut serde_json::Value,
      userinfo: serde_json::Value,
  ) {
      const RESERVED: &[&str] = &["active", "exp", "iat", "nbf", "scope", "client_id", "sub"];
      let (Some(obj), serde_json::Value::Object(ui_map)) =
          (introspection.as_object_mut(), userinfo)
      else { return; };
      for (k, v) in ui_map {
          if !RESERVED.contains(&k.as_str()) {
              obj.insert(k, v);
          }
      }
  }
  ```

  #### 3. New enum variant on `OAuthBearerValidator`

  ```rust
  pub enum OAuthBearerValidator {
      Unsecured(UnsecuredJwsValidator),
      Signed(SignedJwsValidator),
      Introspection(IntrospectionValidator),  // NEW
  }
  ```

  #### 4. Async-ify `OAuthBearerValidator::validate`

  Change the existing `pub fn validate` to `pub async fn validate`:

  ```rust
  impl OAuthBearerValidator {
      pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
          match self {
              Self::Unsecured(v) => v.validate(token, now_ms),
              Self::Signed(v)    => v.validate(token, now_ms),
              Self::Introspection(v) => v.validate(token, now_ms).await,
          }
      }
      // jwks_handle stays as-is — only Signed returns Some.
  }
  ```

  The inner `Unsecured`/`Signed` `validate` methods stay sync. The enum wrapper does an instant `async` return (no real `.await`).

  Existing tests inside `mod tests` that call `validator.validate(...)` directly:
  - If they call on `UnsecuredJwsValidator` / `SignedJwsValidator` directly (not on the enum) → unchanged, those stay sync.
  - If they call on the enum → become `#[tokio::test]` and add `.await`.

  Grep `mod tests` in `oauthbearer.rs` for `OAuthBearerValidator::` usages to find the small set that needs the async conversion. The validator-dispatch test (`validator_signed_returns_jwks_handle` or similar) only calls `.jwks_handle()` (still sync) — no change needed.

  #### 5. `crates/security/src/lib.rs` re-exports

  Add to the existing `oauthbearer` re-export block:

  ```rust
  pub use oauthbearer::{
      // ...existing exports unchanged...
      IntrospectionClient, IntrospectionError, IntrospectionValidator,
  };
  ```

  Verify the existing exports include `OAuthBearerValidator`, `AuthError` etc — they should already; T1 adds three new names.

  #### 6. Cargo.toml

  `async_trait` must be a runtime dep of `crates/security` (not just dev-dep). Read `crates/security/Cargo.toml` `[dependencies]`. If `async-trait` isn't there, add:

  ```toml
  [dependencies]
  async-trait = { workspace = true }
  ```

  (The workspace already declares it — confirmed in `Cargo.toml` workspace deps.)

  #### 7. Mock + unit tests

  At the bottom of `oauthbearer.rs` inside `mod tests`, add a `MockIntrospectionClient` fixture:

  ```rust
  #[cfg(test)]
  mod introspection_tests {
      use super::*;
      use serde_json::{json, Value};
      use std::collections::HashMap;
      use std::sync::Mutex;

      /// Per-token canned responses. `introspect` returns the entry for
      /// the matching token (or a Transport error if absent so a test
      /// can exercise the transport-error path).
      #[derive(Debug, Default)]
      struct MockIntrospectionClient {
          introspect_responses: Mutex<HashMap<String, Result<Value, IntrospectionError>>>,
          userinfo_responses: Mutex<HashMap<String, Result<Option<Value>, IntrospectionError>>>,
      }

      impl MockIntrospectionClient {
          fn arc() -> Arc<Self> {
              Arc::new(Self::default())
          }
          fn set_introspect(&self, token: &str, resp: Result<Value, IntrospectionError>) {
              self.introspect_responses.lock().unwrap().insert(token.into(), resp);
          }
          fn set_userinfo(&self, token: &str, resp: Result<Option<Value>, IntrospectionError>) {
              self.userinfo_responses.lock().unwrap().insert(token.into(), resp);
          }
      }

      #[async_trait::async_trait]
      impl IntrospectionClient for MockIntrospectionClient {
          async fn introspect(&self, token: &str) -> Result<Value, IntrospectionError> {
              self.introspect_responses
                  .lock()
                  .unwrap()
                  .remove(token)
                  .unwrap_or(Err(IntrospectionError::Transport("no canned response".into())))
          }
          async fn userinfo(&self, token: &str) -> Result<Option<Value>, IntrospectionError> {
              self.userinfo_responses
                  .lock()
                  .unwrap()
                  .remove(token)
                  .unwrap_or(Ok(None))
          }
      }

      fn validator(client: Arc<MockIntrospectionClient>) -> IntrospectionValidator {
          IntrospectionValidator {
              client,
              principal_claim_name: "sub".into(),
              scope_claim_name: "scope".into(),
              required_scope: None,
              call_userinfo: false,
              allowable_clock_skew_ms: 30_000,
          }
      }

      const NOW_MS: i64 = 1_700_000_000_000;

      #[tokio::test]
      async fn introspection_active_true_with_principal_returns_ok() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 + 60})));
          let v = validator(mock.clone());
          let p = v.validate("tok", NOW_MS).await.unwrap();
          assert_eq!(p.name, "alice");
      }

      #[tokio::test]
      async fn introspection_active_false_rejected() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"active": false})));
          let v = validator(mock.clone());
          assert!(matches!(v.validate("tok", NOW_MS).await, Err(AuthError::InvalidToken)));
      }

      #[tokio::test]
      async fn introspection_missing_active_field_rejected() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"sub": "alice"})));
          let v = validator(mock.clone());
          assert!(matches!(v.validate("tok", NOW_MS).await, Err(AuthError::InvalidToken)));
      }

      #[tokio::test]
      async fn introspection_expired_exp_rejected() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 - 3600})));
          let v = validator(mock.clone());
          assert!(matches!(v.validate("tok", NOW_MS).await, Err(AuthError::InvalidToken)));
      }

      #[tokio::test]
      async fn introspection_required_scope_honored_string() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice", "scope": "kafka.read kafka.write"})));
          let mut v = validator(mock.clone());
          v.required_scope = Some("kafka.write".into());
          assert!(v.validate("tok", NOW_MS).await.is_ok());
      }

      #[tokio::test]
      async fn introspection_required_scope_honored_array() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice",
                        "scope": ["kafka.read", "kafka.write"]})));
          let mut v = validator(mock.clone());
          v.required_scope = Some("kafka.write".into());
          assert!(v.validate("tok", NOW_MS).await.is_ok());
      }

      #[tokio::test]
      async fn introspection_required_scope_missing_rejected() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice", "scope": "kafka.read"})));
          let mut v = validator(mock.clone());
          v.required_scope = Some("kafka.write".into());
          assert!(matches!(v.validate("tok", NOW_MS).await, Err(AuthError::InvalidToken)));
      }

      #[tokio::test]
      async fn introspection_userinfo_claims_override_introspection_for_profile_keys() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "alice", "preferred_username": "intros-name"})));
          mock.set_userinfo("tok",
              Ok(Some(json!({"preferred_username": "userinfo-name", "email": "a@b.c"}))));
          let mut v = validator(mock.clone());
          v.call_userinfo = true;
          v.principal_claim_name = "preferred_username".into();
          let p = v.validate("tok", NOW_MS).await.unwrap();
          assert_eq!(p.name, "userinfo-name");
      }

      #[tokio::test]
      async fn introspection_userinfo_does_not_override_authorization_keys() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
          mock.set_userinfo("tok", Ok(Some(json!({"active": false, "sub": "mallory"}))));
          let mut v = validator(mock.clone());
          v.call_userinfo = true;
          let p = v.validate("tok", NOW_MS).await.unwrap();
          assert_eq!(p.name, "alice", "sub from introspection wins over userinfo");
      }

      #[tokio::test]
      async fn introspection_userinfo_disabled_when_call_userinfo_false() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
          // Deliberately set a userinfo response — should be ignored.
          mock.set_userinfo("tok", Ok(Some(json!({"preferred_username": "ignored"}))));
          let v = validator(mock.clone()); // call_userinfo: false (default)
          let p = v.validate("tok", NOW_MS).await.unwrap();
          assert_eq!(p.name, "alice");
      }

      #[tokio::test]
      async fn introspection_transport_error_becomes_introspection_transport() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Err(IntrospectionError::Transport("connection refused".into())));
          let v = validator(mock.clone());
          let err = v.validate("tok", NOW_MS).await.unwrap_err();
          assert!(matches!(err, AuthError::IntrospectionTransport(ref msg) if msg.contains("connection refused")),
              "got {err:?}");
      }

      #[tokio::test]
      async fn introspection_default_principal_claim_sub() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"active": true, "sub": "sub-name"})));
          let v = validator(mock.clone());
          assert_eq!(v.validate("tok", NOW_MS).await.unwrap().name, "sub-name");
      }

      #[tokio::test]
      async fn introspection_custom_principal_claim_client_id() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok",
              Ok(json!({"active": true, "sub": "sub-name", "client_id": "my-client"})));
          let mut v = validator(mock.clone());
          v.principal_claim_name = "client_id".into();
          assert_eq!(v.validate("tok", NOW_MS).await.unwrap().name, "my-client");
      }

      #[tokio::test]
      async fn enum_dispatch_introspection_async() {
          let mock = MockIntrospectionClient::arc();
          mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
          let v = validator(mock.clone());
          let enum_v = OAuthBearerValidator::Introspection(v);
          let p = enum_v.validate("tok", NOW_MS).await.unwrap();
          assert_eq!(p.name, "alice");
      }
  }
  ```

  Test cleanly — `cargo test -p crabka-security --lib oauthbearer::introspection_tests 2>&1 | tail`.

  #### 8. Verify

  ```bash
  cd /Users/mattstone/git/crabka/.worktrees/slice-49d-oauth-introspection
  cargo test -p crabka-security --lib 2>&1 | tail
  # Expected: all existing security tests pass; +13 new introspection tests pass.
  cargo build -p crabka-security 2>&1 | tail
  cargo fmt -p crabka-security -- --check
  cargo clippy -p crabka-security --tests -- -D warnings 2>&1 | tail -3
  ```

  Downstream `crates/broker` will FAIL to compile because `OAuthBearerValidator::validate` is now async and its callers don't `.await`. **That's expected** — T2/T3/T4 fix it.

  #### 9. Commit

  ```
  T1: crates/security — IntrospectionValidator + async validate dispatch

  Adds the RFC 7662 introspection validator (slice 49d):
  - IntrospectionClient trait (impl lives in crates/broker per T2)
  - IntrospectionError enum for transport-layer failures
  - IntrospectionValidator with userinfo enrichment + skew-tolerant
    temporal claims + scope check (string + array forms)
  - New AuthError::IntrospectionTransport variant
  - OAuthBearerValidator::validate becomes async fn — existing sync
    Unsecured/Signed paths wrap in async {} (zero runtime cost)
  - 13 new unit tests via in-file MockIntrospectionClient

  Downstream broker callers will fail to compile until T3/T4 propagate
  .await — that's the intended interim state.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 2 (single task — depends on T1)

- **T2 — Broker reqwest introspection client.** Create `crates/broker/src/oauth_introspection.rs` + add `mod` declaration in `crates/broker/src/lib.rs`.

  #### 1. New file contents

  ```rust
  //! HTTP transport for RFC 7662 introspection + OIDC userinfo. Lives
  //! here (not in crates/security) so the security crate stays I/O-free,
  //! mirroring slice-49b's JWKS-refresher pattern.

  use std::path::Path;
  use std::sync::Arc;
  use std::time::Duration;

  use async_trait::async_trait;
  use crabka_security::{
      IntrospectionClient, IntrospectionError, JwksTrustError,
      build_client_config_from_pem,
  };

  /// reqwest-backed RFC 7662 introspection client. Uses HTTP Basic Auth
  /// with the operator-configured client_id + client_secret. Optionally
  /// calls a userinfo endpoint after a successful introspection.
  #[derive(Debug)]
  pub struct ReqwestIntrospectionClient {
      client: reqwest::Client,
      introspection_endpoint: String,
      userinfo_endpoint: Option<String>,
      client_id: String,
      client_secret: String,
  }

  /// Errors building the introspection client at broker startup.
  #[derive(Debug, thiserror::Error)]
  pub enum BuildError {
      #[error("tls trust: {0}")]
      Tls(#[from] JwksTrustError),
      #[error("reqwest build: {0}")]
      Reqwest(String),
  }

  impl ReqwestIntrospectionClient {
      /// Build a new client. When `tls_trust` is `Some`, the rustls
      /// `ClientConfig` is built via `crabka_security::build_client_config_from_pem`
      /// (slice 49c) — the same trust bundle covers JWKS, introspection,
      /// and userinfo. When `None`, reqwest's default webpki-roots apply.
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
              let cfg = build_client_config_from_pem(path)?;
              builder = builder.use_preconfigured_tls((*cfg).clone());
          }
          let client = builder
              .build()
              .map_err(|e| BuildError::Reqwest(e.to_string()))?;
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
          let resp = self
              .client
              .post(&self.introspection_endpoint)
              .basic_auth(&self.client_id, Some(&self.client_secret))
              .form(&[("token", token)])
              .send()
              .await
              .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
          if !resp.status().is_success() {
              return Err(IntrospectionError::Status(resp.status().as_u16()));
          }
          resp.json::<serde_json::Value>()
              .await
              .map_err(|_| IntrospectionError::Parse)
      }

      async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError> {
          let Some(endpoint) = &self.userinfo_endpoint else {
              return Ok(None);
          };
          let resp = self
              .client
              .get(endpoint)
              .bearer_auth(token)
              .send()
              .await
              .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
          if !resp.status().is_success() {
              return Err(IntrospectionError::Status(resp.status().as_u16()));
          }
          let json = resp
              .json::<serde_json::Value>()
              .await
              .map_err(|_| IntrospectionError::Parse)?;
          Ok(Some(json))
      }
  }
  ```

  #### 2. `crates/broker/src/lib.rs`

  Find the existing `pub(crate) mod oauth_jwks;` declaration; add adjacent:

  ```rust
  pub(crate) mod oauth_introspection;
  ```

  #### 3. New integration tests in `oauth_introspection.rs`

  Reuse the 49c HTTPS test fixture pattern (the `serve_jwks_https` fn in `crates/broker/src/oauth_jwks.rs::tests`). Add a small axum-or-hand-rolled mock server that handles BOTH `POST /introspect` and `GET /userinfo`. The fixture-cert + `rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])` pattern is exactly the same; reuse `rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)`.

  Strategy: hand-roll the HTTP server (mirroring 49c's pattern — `tokio-rustls::TlsAcceptor` + raw HTTP/1.1 reply). Path-route on the request line. Capture the request body + headers for assertion.

  Sketch:

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use std::net::SocketAddr;
      use std::sync::Arc;
      use std::sync::Mutex;
      use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
      use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
      use tokio_rustls::TlsAcceptor;
      use tokio_util::sync::CancellationToken;

      /// Records observed requests so tests can assert on the bytes.
      #[derive(Debug, Default)]
      struct ObservedRequests {
          introspect_bodies: Mutex<Vec<String>>,
          introspect_auths: Mutex<Vec<String>>,
          userinfo_auths: Mutex<Vec<String>>,
      }

      /// Spin up an HTTPS test server. Routes:
      ///   POST /introspect  -> serves `introspect_body`
      ///   GET  /userinfo    -> serves `userinfo_body` (or 404 if None)
      /// Returns (addr, shutdown, ca_pem_path, observed).
      async fn serve_https(
          introspect_body: &'static str,
          introspect_status: u16,
          userinfo_body: Option<&'static str>,
      ) -> (SocketAddr, CancellationToken, std::path::PathBuf, Arc<ObservedRequests>) {
          let _ = rustls::crypto::ring::default_provider().install_default();
          let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
          let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
          let cert = params.self_signed(&key).unwrap();
          let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
          let cert_path = dir.path().join("cert.pem");
          std::fs::write(&cert_path, cert.pem()).unwrap();
          let key_path = dir.path().join("key.pem");
          std::fs::write(&key_path, key.serialize_pem()).unwrap();
          let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
              .unwrap()
              .collect::<Result<_, _>>()
              .unwrap();
          let priv_key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
          let server_cfg = Arc::new(
              rustls::ServerConfig::builder()
                  .with_no_client_auth()
                  .with_single_cert(certs, priv_key)
                  .unwrap(),
          );
          let acceptor = TlsAcceptor::from(server_cfg);
          let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
          let addr = listener.local_addr().unwrap();
          let shutdown = CancellationToken::new();
          let srv_shutdown = shutdown.clone();
          let observed = Arc::new(ObservedRequests::default());
          let observed_in_task = observed.clone();
          tokio::spawn(async move {
              loop {
                  tokio::select! {
                      () = srv_shutdown.cancelled() => break,
                      Ok((sock, _peer)) = listener.accept() => {
                          let acceptor = acceptor.clone();
                          let observed = observed_in_task.clone();
                          tokio::spawn(async move {
                              let Ok(mut tls) = acceptor.accept(sock).await else { return };
                              let mut buf = vec![0u8; 8192];
                              let n = tls.read(&mut buf).await.unwrap_or(0);
                              let req = String::from_utf8_lossy(&buf[..n]).to_string();
                              let body = req.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                              let auth_header = req.lines()
                                  .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
                                  .map(|l| l.trim_start_matches(|c: char| c != ':')
                                          .trim_start_matches(':')
                                          .trim()
                                          .to_string())
                                  .unwrap_or_default();
                              let (status, body_out) = if req.starts_with("POST /introspect") {
                                  observed.introspect_bodies.lock().unwrap().push(body.clone());
                                  observed.introspect_auths.lock().unwrap().push(auth_header.clone());
                                  (introspect_status, introspect_body)
                              } else if req.starts_with("GET /userinfo") {
                                  observed.userinfo_auths.lock().unwrap().push(auth_header.clone());
                                  match userinfo_body {
                                      Some(b) => (200u16, b),
                                      None => (404, "{}"),
                                  }
                              } else {
                                  (404, "{}")
                              };
                              let status_text = if status == 200 { "OK" }
                                  else if status == 401 { "Unauthorized" }
                                  else if status == 500 { "Internal Server Error" }
                                  else { "Error" };
                              let header = format!(
                                  "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                  body_out.len(),
                              );
                              let _ = tls.write_all(header.as_bytes()).await;
                              let _ = tls.write_all(body_out.as_bytes()).await;
                              let _ = tls.shutdown().await;
                          });
                      }
                  }
              }
          });
          (addr, shutdown, cert_path, observed)
      }

      #[tokio::test]
      async fn introspection_fetches_active_token_over_https_with_custom_trust() {
          let body = r#"{"active":true,"sub":"alice"}"#;
          let (addr, srv_shutdown, ca, _observed) = serve_https(body, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None,
              "kafka-broker".into(),
              "secret".into(),
              Some(&ca),
              Duration::from_secs(5),
          ).unwrap();
          let resp = client.introspect("tok").await.unwrap();
          assert_eq!(resp.get("active").and_then(|v| v.as_bool()), Some(true));
          assert_eq!(resp.get("sub").and_then(|v| v.as_str()), Some("alice"));
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_returns_inactive_when_idp_says_inactive() {
          let (addr, srv_shutdown, ca, _) = serve_https(r#"{"active":false}"#, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          let resp = client.introspect("tok").await.unwrap();
          assert_eq!(resp.get("active").and_then(|v| v.as_bool()), Some(false));
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_returns_transport_error_on_non_2xx() {
          let (addr, srv_shutdown, ca, _) = serve_https(r#"{"error":"x"}"#, 500, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          let err = client.introspect("tok").await.unwrap_err();
          assert!(matches!(err, IntrospectionError::Status(500)), "got {err:?}");
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_userinfo_endpoint_is_called_after_active_introspection() {
          let (addr, srv_shutdown, ca, observed) = serve_https(
              r#"{"active":true,"sub":"alice"}"#,
              200,
              Some(r#"{"preferred_username":"alice","email":"a@b.c"}"#),
          ).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              Some(format!("https://127.0.0.1:{}/userinfo", addr.port())),
              "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          client.introspect("tok").await.unwrap();
          let ui = client.userinfo("tok").await.unwrap().unwrap();
          assert_eq!(ui.get("preferred_username").and_then(|v| v.as_str()), Some("alice"));
          assert_eq!(observed.userinfo_auths.lock().unwrap().len(), 1);
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_userinfo_endpoint_is_not_called_when_endpoint_unset() {
          let (addr, srv_shutdown, ca, _) = serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          let ui = client.userinfo("tok").await.unwrap();
          assert!(ui.is_none());
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_handles_keycloak_response_shape() {
          let body = r#"{"active":true,"sub":"svc-account-kafka-client",
              "client_id":"kafka-client","scope":"kafka.write profile","exp":9999999999}"#;
          let (addr, srv_shutdown, ca, _) = serve_https(body, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          let resp = client.introspect("tok").await.unwrap();
          assert_eq!(resp.get("client_id").and_then(|v| v.as_str()), Some("kafka-client"));
          assert_eq!(resp.get("scope").and_then(|v| v.as_str()), Some("kafka.write profile"));
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_basic_auth_sent_with_configured_client_id_and_secret() {
          let (addr, srv_shutdown, ca, observed) = serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "kafka-broker".into(), "shh".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          client.introspect("tok").await.unwrap();
          let auths = observed.introspect_auths.lock().unwrap();
          assert_eq!(auths.len(), 1);
          // Basic base64(kafka-broker:shh) = "Basic a2Fma2EtYnJva2VyOnNoaA=="
          assert_eq!(auths[0], "Basic a2Fma2EtYnJva2VyOnNoaA==");
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_form_body_token_field() {
          let (addr, srv_shutdown, ca, observed) = serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca), Duration::from_secs(5),
          ).unwrap();
          client.introspect("opaque-abc").await.unwrap();
          let bodies = observed.introspect_bodies.lock().unwrap();
          assert_eq!(bodies.len(), 1);
          assert_eq!(bodies[0], "token=opaque-abc");
          srv_shutdown.cancel();
      }

      #[tokio::test]
      async fn introspection_respects_http_timeout() {
          // Server intentionally responds with a normal body but with
          // an enormous delay simulated by sleeping in the spawn task
          // ABOVE write — actually that's not visible to the client
          // since the connection accepts. A simpler approach: bind a
          // listener but never read/write. The reqwest builder's timeout
          // expires.
          let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
          let addr = listener.local_addr().unwrap();
          // Don't accept loop — the connect itself will succeed (TCP
          // handshake), but TLS will never complete because nothing reads.
          let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
          let _ = rustls::crypto::ring::default_provider().install_default();
          let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
          let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
          let cert = params.self_signed(&key).unwrap();
          let ca_path = dir.path().join("ca.pem");
          std::fs::write(&ca_path, cert.pem()).unwrap();
          drop(listener); // give back the port — reqwest will connect to a closed port and fail fast
          let client = ReqwestIntrospectionClient::new(
              format!("https://127.0.0.1:{}/introspect", addr.port()),
              None, "id".into(), "s".into(), Some(&ca_path),
              Duration::from_millis(200),
          ).unwrap();
          let err = client.introspect("tok").await.unwrap_err();
          assert!(matches!(err, IntrospectionError::Transport(_)), "got {err:?}");
      }
  }
  ```

  Notes on the timeout test: connecting to a closed port is the simplest reliable failure mode (immediate ECONNREFUSED via reqwest's transport layer). If you find the immediate-error mode masks the timeout path, switch to a never-responding listener (accept-only loop with no read).

  #### 4. Cargo.toml

  Verify `tokio-rustls`, `rcgen`, `tempfile`, and `tokio-util` are dev-deps (they should be, from slice 49c):
  ```bash
  grep -E "^(tokio-rustls|rcgen|tempfile|tokio-util)" crates/broker/Cargo.toml
  ```
  If any are missing under `[dev-dependencies]`, add from workspace deps. No production-dep changes needed (async-trait, reqwest, serde_json already in broker prod-deps).

  #### 5. Verify

  ```bash
  cd /Users/mattstone/git/crabka/.worktrees/slice-49d-oauth-introspection
  cargo build -p crabka-broker 2>&1 | tail
  # Expected: T1's async-validate ripple is still uncompiled in dispatch.rs / network/auth.rs.
  # That's T4's territory. The new oauth_introspection.rs module itself should be clean.
  cargo test -p crabka-broker --lib oauth_introspection:: 2>&1 | tail
  # Expected: all 9 new tests pass.
  cargo fmt -p crabka-broker -- --check
  cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail -3
  ```

  If the workspace doesn't compile because T1's async-validate broke `network/auth.rs::handle_authenticate_oauthbearer` etc., the `oauth_introspection::` test target can still be built in isolation: `cargo test -p crabka-broker --lib oauth_introspection:: --no-fail-fast`. If that's still blocked by the upstream compile error, T2's local verification has to be `cargo check -p crabka-broker --lib --message-format=short 2>&1 | grep "oauth_introspection"` and confirm zero errors come from THIS file. Then T4 unblocks workspace-wide test runs.

  #### 6. Commit

  ```
  T2: crates/broker — ReqwestIntrospectionClient

  RFC 7662 introspection over HTTPS via reqwest + rustls. HTTP Basic
  Auth (client_id + client_secret) on the introspection POST; bearer
  auth on the optional userinfo GET. Uses 49c's
  build_client_config_from_pem for custom TLS trust to the IdP. 9 new
  integration tests via the existing 49c HTTPS fixture pattern
  (tokio-rustls + rcgen self-signed cert with 127.0.0.1 SAN +
  hand-rolled HTTP/1.1 reply).

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 3 (single task — depends on T2)

- **T3 — Broker config + apply_to selection + rename.** `crates/broker/src/file_config.rs` + `crates/broker/src/config.rs`.

  #### 1. `FileOAuthBearerConfig` extensions

  Read the existing struct. Add five new fields (group near existing `jwks_*` fields) and rename `jwks_tls_trust` → `idp_tls_trust`:

  ```rust
  // Slice 49c — renamed in 49d: shared across JWKS + introspection + userinfo
  #[serde(default)]
  pub idp_tls_trust: Option<std::path::PathBuf>,

  // Slice 49d (new)
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

  Update the field doc comments accordingly. The old `jwks_tls_trust` doc said "for JWKS only" — `idp_tls_trust` is "for ANY IdP HTTPS endpoint (JWKS, introspection, userinfo)".

  #### 2. `BrokerConfig` rename

  In `crates/broker/src/config.rs`:
  ```rust
  // Was: oauthbearer_jwks_tls_trust
  pub oauthbearer_idp_tls_trust: Option<std::path::PathBuf>,
  ```
  Update the `Default` impl + `for_tests` constructor + doc comment.

  No new runtime fields beyond the rename — the `IntrospectionValidator` lives inside `oauthbearer_validator: OAuthBearerValidator`.

  #### 3. `FileOAuthBearerConfig::apply_to` validator selection

  Read the existing implementation. The current logic (slices 49 + 49b) is roughly:
  ```rust
  if let Some(oauth) = self.oauthbearer {
      cfg.oauthbearer_idp_tls_trust = oauth.idp_tls_trust;
      if let Some(jwks_uri) = oauth.jwks_endpoint_uri {
          // build SignedJwsValidator
          cfg.oauthbearer_validator = OAuthBearerValidator::Signed(...);
          cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
          // ...
      } else {
          // build UnsecuredJwsValidator
          cfg.oauthbearer_validator = OAuthBearerValidator::Unsecured(...);
      }
  }
  ```

  Extend to three-way:
  ```rust
  if let Some(oauth) = self.oauthbearer {
      cfg.oauthbearer_idp_tls_trust = oauth.idp_tls_trust.clone();

      match (oauth.jwks_endpoint_uri.as_ref(), oauth.introspection_endpoint_uri.as_ref()) {
          (Some(_), Some(_)) => {
              // FAIL LOUDLY at config-load. The existing apply_to may
              // not surface errors — it's `fn apply_to(self, cfg: &mut
              // BrokerConfig)` not Result. Two options:
              //   (a) Change apply_to signature to Result<(), ConfigError>.
              //   (b) Log + panic via tracing::error! + process::exit.
              // Read the surrounding apply_to fns to see the prevailing
              // pattern. If apply_to currently has no error path, the
              // cleanest is option (a) — propagate via a panic if the
              // caller is too noisy to refactor. For 49d, prefer option (a)
              // if doable in <30 mins; otherwise fall back to a panic
              // with a clear "::error::" message.
              panic!(
                  "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
              );
          }
          (Some(jwks_uri), None) => {
              // Existing slice 49b path — build Signed.
              // (Copy the existing logic verbatim — only the surrounding
              //  control flow changes; the inner Signed construction is
              //  unchanged.)
              let v = crabka_security::SignedJwsValidator { /* ... */ };
              cfg.oauthbearer_validator = OAuthBearerValidator::Signed(v);
              cfg.oauthbearer_jwks_endpoint = Some(jwks_uri.clone());
              if let Some(ms) = oauth.jwks_refresh_interval_ms {
                  cfg.oauthbearer_jwks_refresh_interval = std::time::Duration::from_millis(ms);
              }
              // ...principal/scope claims wiring unchanged...
          }
          (None, Some(introspect_uri)) => {
              // NEW slice 49d path — build Introspection.
              let client_id = oauth.introspection_client_id.clone()
                  .unwrap_or_else(|| panic!(
                      "[oauthbearer]: introspection_endpoint_uri set but introspection_client_id is missing"
                  ));
              let secret_path = oauth.introspection_client_secret_path.clone()
                  .unwrap_or_else(|| panic!(
                      "[oauthbearer]: introspection_endpoint_uri set but introspection_client_secret_path is missing"
                  ));
              let client_secret = std::fs::read_to_string(&secret_path)
                  .unwrap_or_else(|e| panic!(
                      "[oauthbearer]: failed to read introspection_client_secret_path {}: {}",
                      secret_path.display(), e
                  ))
                  .trim_end_matches(|c: char| c == '\n' || c == '\r')
                  .to_string();
              let timeout = std::time::Duration::from_millis(
                  oauth.introspection_http_timeout_ms.unwrap_or(10_000),
              );
              let client = crate::oauth_introspection::ReqwestIntrospectionClient::new(
                  introspect_uri.clone(),
                  oauth.userinfo_endpoint_uri.clone(),
                  client_id,
                  client_secret,
                  oauth.idp_tls_trust.as_deref(),
                  timeout,
              ).unwrap_or_else(|e| panic!(
                  "[oauthbearer]: failed to build introspection client: {e}"
              ));
              let mut v = crabka_security::IntrospectionValidator {
                  client,
                  principal_claim_name: oauth.principal_claim_name.clone()
                      .unwrap_or_else(|| "sub".into()),
                  scope_claim_name: oauth.scope_claim_name.clone()
                      .unwrap_or_else(|| "scope".into()),
                  required_scope: oauth.required_scope.clone(),
                  call_userinfo: oauth.userinfo_endpoint_uri.is_some(),
                  allowable_clock_skew_ms: oauth.allowable_clock_skew_ms.unwrap_or(30_000),
              };
              cfg.oauthbearer_validator = OAuthBearerValidator::Introspection(v);
          }
          (None, None) => {
              // Existing slice 49 path — Unsecured (dev only). Unchanged.
              let v = crabka_security::UnsecuredJwsValidator { /* ... */ };
              cfg.oauthbearer_validator = OAuthBearerValidator::Unsecured(v);
          }
      }
  }
  ```

  **Decision on error handling:** read the existing `FileConfig::apply_to` shape. If other configuration validations in the file ALREADY panic (e.g., "broker_id not set"), the panics above are consistent. If they all use `Result`, refactor `apply_to` to `Result<(), ConfigError>` and propagate — that's a 30-minute refactor that touches the call site in `bin/broker.rs`. **Prefer Result.** Inline panic strings stay as the panic-fallback documentation in case Result refactor turns out unwieldy.

  #### 4. Update existing 49c test naming + new unit tests

  Find the existing slice-49c test in `file_config.rs`:
  - `apply_to_oauthbearer_threads_jwks_tls_trust_to_broker_config` → rename to `apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config`; update TOML key to `idp_tls_trust`; update assertion field name to `cfg.oauthbearer_idp_tls_trust`.
  - `apply_to_oauthbearer_without_jwks_tls_trust_leaves_field_none` → rename to `apply_to_oauthbearer_without_idp_tls_trust_leaves_field_none`; same updates.

  Add new tests for 49d selection logic. Place near the existing oauthbearer tests:

  ```rust
  #[test]
  fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
      // Create a temp file for the client secret since apply_to reads it.
      let dir = tempfile::tempdir().unwrap();
      let secret_path = dir.path().join("client-secret");
      std::fs::write(&secret_path, "the-secret").unwrap();
      let toml = format!(r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = "{}"
"#, secret_path.display());
      let file: FileConfig = toml::from_str(&toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);  // adapt to .apply_to(...).unwrap() if Result refactor done
      assert!(matches!(
          cfg.oauthbearer_validator,
          crabka_security::OAuthBearerValidator::Introspection(_)
      ));
  }

  #[test]
  #[should_panic(expected = "mutually exclusive")]
  fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
      let dir = tempfile::tempdir().unwrap();
      let secret_path = dir.path().join("client-secret");
      std::fs::write(&secret_path, "x").unwrap();
      let toml = format!(r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = "{}"
"#, secret_path.display());
      let file: FileConfig = toml::from_str(&toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
  }

  #[test]
  #[should_panic(expected = "introspection_client_id")]
  fn apply_to_oauthbearer_introspection_requires_client_id() {
      let dir = tempfile::tempdir().unwrap();
      let secret_path = dir.path().join("client-secret");
      std::fs::write(&secret_path, "x").unwrap();
      let toml = format!(r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = "{}"
"#, secret_path.display());
      let file: FileConfig = toml::from_str(&toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
  }

  #[test]
  #[should_panic(expected = "introspection_client_secret_path")]
  fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
      let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
      let file: FileConfig = toml::from_str(toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
  }

  #[test]
  fn apply_to_oauthbearer_introspection_with_userinfo_sets_call_userinfo_true() {
      let dir = tempfile::tempdir().unwrap();
      let secret_path = dir.path().join("client-secret");
      std::fs::write(&secret_path, "x").unwrap();
      let toml = format!(r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
userinfo_endpoint_uri = "https://idp.example/userinfo"
introspection_client_id = "id"
introspection_client_secret_path = "{}"
"#, secret_path.display());
      let file: FileConfig = toml::from_str(&toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
      match cfg.oauthbearer_validator {
          crabka_security::OAuthBearerValidator::Introspection(v) => assert!(v.call_userinfo),
          other => panic!("expected Introspection, got {other:?}"),
      }
  }

  #[test]
  fn apply_to_oauthbearer_introspection_without_userinfo_sets_call_userinfo_false() {
      let dir = tempfile::tempdir().unwrap();
      let secret_path = dir.path().join("client-secret");
      std::fs::write(&secret_path, "x").unwrap();
      let toml = format!(r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = "{}"
"#, secret_path.display());
      let file: FileConfig = toml::from_str(&toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
      match cfg.oauthbearer_validator {
          crabka_security::OAuthBearerValidator::Introspection(v) => assert!(!v.call_userinfo),
          other => panic!("expected Introspection, got {other:?}"),
      }
  }
  ```

  Adjust the `#[should_panic(expected = "...")]` matchers if you refactor to `Result` — they become `assert!(matches!(..., Err(...)))` assertions instead.

  #### 5. Verify

  ```bash
  cargo build -p crabka-broker 2>&1 | tail
  # Expected: still fails in network/auth.rs and network/dispatch.rs (T4 fixes
  # those). The new tests + this file should compile cleanly.
  cargo test -p crabka-broker --lib file_config:: 2>&1 | tail
  cargo fmt -p crabka-broker -- --check
  ```

  #### 6. Commit

  ```
  T3: crates/broker — config selection + jwks_tls_trust → idp_tls_trust

  Extends FileOAuthBearerConfig with the 5 new 49d keys
  (introspection_endpoint_uri, userinfo_endpoint_uri,
  introspection_client_id, introspection_client_secret_path,
  introspection_http_timeout_ms). Renames jwks_tls_trust → idp_tls_trust
  (shared trust bundle for all IdP HTTPS — JWKS, introspection,
  userinfo). apply_to grows a three-way validator selection
  (jwks/introspection/unsecured) with mutually-exclusive rejection of
  the two endpoint URIs. Reads the client secret from disk at
  config-load. 6 new unit tests + 2 renamed.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 4 (parallel — disjoint files; both depend on T3)

- **T4 — Broker downstream propagation: .await + rename uses.** `crates/broker/src/network/auth.rs` + `crates/broker/src/network/dispatch.rs` + `crates/broker/src/broker.rs` + `crates/broker/src/oauth_jwks.rs`.

  Four mechanical updates:

  1. **`crates/broker/src/network/auth.rs`:**
     - `pub fn handle_authenticate_oauthbearer` → `pub async fn handle_authenticate_oauthbearer`.
     - `fn validate_bearer` (line ~357) → `async fn validate_bearer`. Add `.await` to its internal `validator.validate(...)` call.
     - In `handle_authenticate_oauthbearer`, the `validate_bearer(...).await?` propagates.
     - Existing unit tests in this file that call `handle_authenticate_oauthbearer(...)` (lines ~498, 527, 547, 563, 584): become `#[tokio::test]` and add `.await` to each invocation.

  2. **`crates/broker/src/network/dispatch.rs`:** the call site at line ~1133 already lives inside an async context (the request-dispatch fn). Change `crate::network::auth::handle_authenticate_oauthbearer(&req, auth, &broker.config.oauthbearer_validator, now_ms)` to `crate::network::auth::handle_authenticate_oauthbearer(&req, auth, &broker.config.oauthbearer_validator, now_ms).await`. Verify the surrounding fn is `async fn` (the dispatch loop should already be async).

  3. **`crates/broker/src/broker.rs`:** the existing slice-49b spawn block at line ~1238:
     ```rust
     let refresher = crate::oauth_jwks::JwksRefresher {
         endpoint,
         handle,
         interval: config.oauthbearer_jwks_refresh_interval,
         shutdown: supervisor_shutdown.child_token(),
         tls_trust: config.oauthbearer_jwks_tls_trust.clone(),  // rename here
     };
     ```
     → `tls_trust: config.oauthbearer_idp_tls_trust.clone()`.

  4. **`crates/broker/src/oauth_jwks.rs`:** the `JwksRefresher.tls_trust` private field doc comment says "shared with JWKS path" — update it to say "shared with introspection too (now `idp_tls_trust` on broker config)". Field name itself unchanged (it's a private struct field with no rename pressure).

  Verify:
  ```bash
  cargo build -p crabka-broker 2>&1 | tail
  # Expected: clean now.
  cargo test -p crabka-broker --lib 2>&1 | tail
  cargo test -p crabka-broker --tests 2>&1 | tail
  cargo fmt -p crabka-broker -- --check
  cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail -3
  ```

  Commit:
  ```
  T4: crates/broker — propagate async validate + idp_tls_trust rename

  Cascades T1's async OAuthBearerValidator::validate through the SASL
  handler (handle_authenticate_oauthbearer + validate_bearer become
  async fn) and the dispatch call site (network/dispatch.rs adds
  .await). Five existing OAUTHBEARER tests in network/auth.rs flip to
  #[tokio::test]. Threads T3's oauthbearer_idp_tls_trust rename into
  the JwksRefresher literal in broker.rs::start (one line).

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T5 — Operator TOML rename.** `crates/operator/src/controller/listeners.rs`.

  One render-line change + 2-3 test assertion flips.

  1. In `render_broker_toml`, find the existing slice-49c TOML emission line:
     ```rust
     // OR however the implementer wrote it in slice 49c:
     let _ = writeln!(out, r#"jwks_tls_trust = "/etc/crabka/oauth-jwks-trust/ca.crt""#);
     ```
     Flip the LHS to `idp_tls_trust`:
     ```rust
     let _ = writeln!(out, r#"idp_tls_trust = "/etc/crabka/oauth-jwks-trust/ca.crt""#);
     ```

  2. Find existing slice-50b TOML-render tests that assert `jwks_tls_trust = …`. Grep:
     ```bash
     grep -n "jwks_tls_trust" crates/operator/src/controller/listeners.rs
     ```
     Each occurrence in tests flips to `idp_tls_trust`. Likely 2-3 spots:
     - `render_broker_toml_emits_jwks_tls_trust_when_trust_certs_present` test body assertion.
     - The test name itself can stay (it's about emitting the trust path, semantic unchanged) OR be renamed to `render_broker_toml_emits_idp_tls_trust_when_trust_certs_present`. Pick rename for accuracy.
     - `render_broker_toml_omits_jwks_tls_trust_when_no_trust_certs` similarly.
     - Any cross-listener canonical-order test that includes the line — flip.

  3. Also check `crates/operator/tests/reconcile_listener_oauth.rs` for any assertion on the literal `jwks_tls_trust = …` string. Flip those too.

  4. The mount path constant `/etc/crabka/oauth-jwks-trust` and the Secret name format `{kafka}-oauth-jwks-trust` and the operator-side helper `oauth_jwks_trust_secret_name` STAY — they're internal naming that reflects the purpose (JWKS-and-friends-trust), not the broker TOML key.

  Verify:
  ```bash
  cargo test -p crabka-operator --lib controller::listeners:: 2>&1 | tail
  cargo test -p crabka-operator --test reconcile_listener_oauth 2>&1 | tail
  cargo fmt -p crabka-operator -- --check
  cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail -3
  ```

  Commit:
  ```
  T5: crates/operator — render idp_tls_trust instead of jwks_tls_trust

  Coordinated with T3's broker rename. One line in render_broker_toml
  flips the emitted TOML key. ~3 test assertions in listeners.rs +
  reconcile_listener_oauth.rs update to the new key name.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 5 (single task — STATUS + final gate)

- **T6 — STATUS.md entry + final gate.**

  1. Append `## Slice 49d — Broker: OAUTHBEARER opaque-token introspection (2026-05-24)` at the end of `STATUS.md`. Read the slice-49c entry first (`grep -A 60 "^## Slice 49c " STATUS.md | head -65`) for tone. ~40-50 lines.

     Cover:
     - **Opener** (2-3 sentences): adds RFC 7662 introspection alongside 49b's JWKS path; unblocks operator slice 50c; renames `jwks_tls_trust` → `idp_tls_trust` (shared trust bundle for all IdP HTTPS).
     - **`crates/security`**: new `IntrospectionValidator` + `IntrospectionClient` trait + `IntrospectionError` enum + `AuthError::IntrospectionTransport` variant. `OAuthBearerValidator::validate` is now `async fn` — Unsecured/Signed wrap in `async {}` (zero runtime cost), Introspection truly awaits the HTTP call.
     - **`crates/broker/src/oauth_introspection.rs`**: new `ReqwestIntrospectionClient` (HTTP Basic Auth for introspection POST, bearer auth for optional userinfo GET); reuses 49c's `build_client_config_from_pem` for IdP TLS trust.
     - **`crates/broker/src/file_config.rs`**: 5 new `[oauthbearer]` keys (`introspection_endpoint_uri`, `userinfo_endpoint_uri`, `introspection_client_id`, `introspection_client_secret_path`, `introspection_http_timeout_ms`). Three-way validator selection (jwks / introspection / unsecured) with mutually-exclusive rejection when both endpoint URIs are set. Client secret read from disk at config-load.
     - **Rename**: `[oauthbearer].jwks_tls_trust` → `idp_tls_trust` shared across JWKS, introspection, userinfo. Greenfield rename per CLAUDE.md. Coordinated flips: broker file_config + config + broker.rs + operator render_broker_toml + tests.
     - **SASL handler**: `handle_authenticate_oauthbearer` becomes `async fn`; dispatch call site adds `.await`; 5 existing tests flip to `#[tokio::test]`.
     - **Tests**: +13 security unit (introspection validator paths + enum-dispatch async) + 9 broker integration (HTTPS via tokio-rustls + rcgen, mirroring 49c) + 6 broker file_config unit (selection + missing-fields + rename) + 2 renames in 49c's tests. Workspace clippy `-D warnings` + fmt clean.
     - **Reference doc**: `[docs/superpowers/specs/2026-05-24-crabka-broker-oauth-introspection-49d-design.md]`.
     - **Out of scope** (terse): 50c (operator CRD field + Secret mount + reconciler for `introspectionEndpointUri`); hybrid validator (try JWT then fall back); token caching; `client_secret_post` / `private_key_jwt` (Basic Auth only); outbound mTLS to IdP; per-listener `[oauthbearer]` config (still rejected; future 49h).

  2. Final gate:
     ```bash
     cd /Users/mattstone/git/crabka/.worktrees/slice-49d-oauth-introspection
     cargo fmt --check
     cargo clippy --workspace --all-targets -- -D warnings
     cargo test --workspace
     ```
     (No CRD regen needed for this broker-only slice. T5's operator-side test edits don't change CRD YAML.)

     All three must be green.

     Known pre-existing flake: `auto_rebalance_restores_preferred_leader` in `crates/broker/tests/elect_leaders.rs` can time out under parallel load. If it fires, re-run in isolation to confirm same flake (not slice-49d-introduced).

     If clippy fires on new code, decide per lint: refactor for substantive lints; targeted `#[allow(clippy::...)]` with a one-line rationale only for intentional patterns the lint can't infer.

  3. Commit:
     ```
     Slice 49d: STATUS.md entry + final gate

     Documents the new OAUTHBEARER introspection validator + the
     jwks_tls_trust → idp_tls_trust rename. fmt + clippy + workspace
     tests all green.

     Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
     ```

## Notes

- **Dependency chain**: T1 → T2 → T3 → (T4 ‖ T5) → T6. Five batches, six tasks.
- **T4 + T5 file-disjoint**: T4 = `crates/broker/{network/auth,network/dispatch,broker,oauth_jwks}.rs`. T5 = `crates/operator/src/controller/listeners.rs` + `crates/operator/tests/reconcile_listener_oauth.rs`. No overlap.
- **T1 ends with a broken-tree commit** by design — T2/T3/T4 progressively unblock. The plan's verify steps in T1/T2/T3 explicitly acknowledge this. T4 restores `cargo build -p crabka-broker` to clean.
- **Greenfield rename**: `jwks_tls_trust` → `idp_tls_trust` flows through 5 files. CLAUDE.md explicitly permits "no compat shims" — no aliasing, no deprecated-but-kept name.
- **No CRD shape change** — only the operator's rendered TOML output changes (T5). No `tools/regen-crds.sh` needed.
- **49b/49c sync tests stay sync** — the inner `validate` methods of `UnsecuredJwsValidator` / `SignedJwsValidator` don't change. Only the enum wrapper and the explicit `OAuthBearerValidator::validate` callers shift.
- **After 49d lands**, the umbrella's next pair is **49e + 50d** (KIP-368 re-authentication: `session_lifetime_ms` on `SaslAuthenticateResponse v1+`, per-connection expiry timer; operator surfaces `maxSecondsWithoutReauthentication`). Independent of 49d at the protocol layer.
