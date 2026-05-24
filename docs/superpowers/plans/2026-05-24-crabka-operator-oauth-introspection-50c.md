# Slice 50c: Operator — Listener OAuth introspection surface — Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (matches the project's CLAUDE.md mandate to execute in parallel batches). Steps use the project's compact-batch style — each T is one focused PR-worth of work, file-conflict-disjoint within a batch.

**Design:** `docs/superpowers/specs/2026-05-24-crabka-operator-oauth-introspection-50c-design.md`
**Umbrella:** `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
**Paired broker slice:** 49d (just shipped — broker introspection validator + `[oauthbearer]` TOML keys).

**Goal:** Operator surface for 49d's introspection. Add `accessTokenIsJwt` + `introspectionEndpointUri` / `userInfoEndpointUri` / `clientId` / `clientSecret: {secretName, key}` / `introspectionHttpTimeoutSeconds` on the OAuth listener CRD. Validate + mount the source Secret directly into broker pods. Render the 49d TOML keys. Plus a `kind-oauth-introspection` e2e variant against Keycloak.

**Architecture:** Five sequential batches. T1+T2 in parallel: CRD types and the `ReconcileError` variants. T3+T4 in parallel: validation/TOML render in `controller/listeners.rs` and the reconciler helpers in `controller/kafka.rs`. T5 alone: pod template threading in `kafka_node_pool.rs` (depends on T4's `OauthIntrospectionMount` type). T6+T7+T8 in parallel: integration tests, sample + CRD regen, kind-cluster e2e. T9 alone: STATUS + final gate.

**Tech Stack:** Same as slice 50b — kube-rs 3.x `CustomResource`, schemars 1.x, existing `tower::ServiceExt::mock_service` kube mock for integration tests, Bitnami Keycloak chart `25.2.0` for e2e (same chart slice 50b/49d's `kind-oauth` uses).

---

## Batches

### Batch 1 (parallel — disjoint files)

- **T1 — CRD types.** `crates/operator/src/crd/listener.rs`.

  1. Make `jwks_endpoint_uri` optional. Find the existing field declaration:
     ```rust
     pub jwks_endpoint_uri: String,
     ```
     Change to:
     ```rust
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub jwks_endpoint_uri: Option<String>,
     ```
     Doc-comment update: "Required when `accessTokenIsJwt: true` (the default); rejected when `accessTokenIsJwt: false`."

  2. Add 6 new fields on `ListenerAuthenticationOAuth` (grouped near the existing OAuth knobs):
     ```rust
     /// Strimzi-shape: when `true` (default), the broker validates
     /// tokens as signed JWTs against `jwksEndpointUri` (slice 49b).
     /// When `false`, the broker calls `introspectionEndpointUri` for
     /// each token (slice 49d). Drives operator-side validation:
     /// see also the cross-mode rules in the listeners reconciler.
     #[serde(default = "default_true", skip_serializing_if = "is_default_true")]
     pub access_token_is_jwt: bool,

     /// RFC 7662 introspection endpoint. Required when
     /// `accessTokenIsJwt: false`; rejected when `accessTokenIsJwt: true`.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub introspection_endpoint_uri: Option<String>,

     /// Optional OIDC userinfo endpoint. Permitted only with
     /// `accessTokenIsJwt: false`. When set, the broker calls userinfo
     /// after each successful introspection and merges the profile
     /// claims (49d's userinfo enrichment).
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub user_info_endpoint_uri: Option<String>,

     /// HTTP Basic Auth client_id the broker uses against the
     /// introspection endpoint. Required when `accessTokenIsJwt: false`.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub client_id: Option<String>,

     /// Reference to a Kubernetes `Secret` in the same namespace
     /// holding the client-secret material for the introspection
     /// endpoint's Basic Auth. The operator mounts the source Secret
     /// directly into the broker pod with a projected `items` mapping
     /// so the broker reads from a fixed path regardless of the
     /// user's source key name. Required when `accessTokenIsJwt: false`.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub client_secret: Option<OauthClientSecretRef>,

     /// Slice 49d: timeout for the introspection (and userinfo) HTTP
     /// requests, in seconds. Operator converts to ms for the broker
     /// TOML. Optional; broker default is 10 seconds. Permitted only
     /// with `accessTokenIsJwt: false`.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub introspection_http_timeout_seconds: Option<u32>,
     ```

  3. New `OauthClientSecretRef` struct (Strimzi-shape `{secretName, key}`):
     ```rust
     /// Strimzi-shape Secret reference for the OAUTHBEARER
     /// introspection client secret. The source Secret must exist in
     /// the same namespace as the `Kafka` CR.
     #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
     #[serde(rename_all = "camelCase")]
     pub struct OauthClientSecretRef {
         /// Name of the Kubernetes Secret holding the client secret.
         pub secret_name: String,
         /// Key within the Secret whose value is the client-secret
         /// material. The operator mounts this key as a file at a
         /// fixed path inside the broker pod (the user's key name is
         /// hidden from the broker via projected `items`).
         pub key: String,
     }
     ```

  4. Extend `listener_authentication_schema` to include the new sibling properties. Add inside the existing `properties` object:
     ```rust
     "accessTokenIsJwt": { "type": "boolean" },
     "introspectionEndpointUri": { "type": "string", "minLength": 1 },
     "userInfoEndpointUri": { "type": "string", "minLength": 1 },
     "clientId": { "type": "string", "minLength": 1 },
     "clientSecret": {
         "type": "object",
         "required": ["secretName", "key"],
         "properties": {
             "secretName": { "type": "string", "minLength": 1 },
             "key":        { "type": "string", "minLength": 1 }
         }
     },
     "introspectionHttpTimeoutSeconds": { "type": "integer", "minimum": 1 },
     ```
     Also: `jwksEndpointUri` was likely declared in the schema's `required` list (matching the pre-change Rust type); remove it. The schema's required set on the oauth variant is now just `validIssuerUri` — cross-mode validation is reconciler-side.

  5. New unit tests in the existing `mod auth_tests` (or wherever the OAuth-related tests live):
     - `oauth_with_access_token_is_jwt_false_introspection_round_trips`
     - `oauth_access_token_is_jwt_default_omitted_on_serialize` — default `true` serializes to nothing.
     - `oauth_client_secret_round_trips`
     - `oauth_jwks_endpoint_uri_now_optional_omits_when_none` — construct with `jwks_endpoint_uri: None`, assert JSON does not contain the key.
     - `oauth_with_userinfo_endpoint_round_trips`
     - `oauth_schema_contains_introspection_sibling_keys` — extend the existing schema-regression test from slice 50 T1 polish to assert the 6 new property keys are present.

  6. **Match-exhaustiveness sweep** — there are no exhaustive matches over `ListenerAuthenticationOAuth` field combinations in `listener.rs` itself. But every existing test fixture inside this file that constructs `ListenerAuthenticationOAuth { ... }` literally needs the 6 new fields. Grep `ListenerAuthenticationOAuth {` in `listener.rs` and add defaults to each literal:
     ```rust
     access_token_is_jwt: true,
     introspection_endpoint_uri: None,
     user_info_endpoint_uri: None,
     client_id: None,
     client_secret: None,
     introspection_http_timeout_seconds: None,
     ```
     **And** every literal needs `jwks_endpoint_uri` to flip from `"...".into()` to `Some("...".into())`.

     Also sweep `crates/operator/src/controller/listeners.rs` (lots of test fixtures), `crates/operator/tests/reconcile_listener_oauth.rs`, and `crates/operator/tests/reconcile_oauth_trust.rs` (slice 50b) for the same struct-literal updates.

  Verify:
  ```bash
  cargo test -p crabka-operator --lib crd::listener:: 2>&1 | tail
  cargo build -p crabka-operator 2>&1 | tail
  # Workspace lib build expected to fail in controller/listeners.rs +
  # controller/kafka.rs because T3+T4 haven't added the validation +
  # render code that consumes the new fields. Same for the integration
  # tests in tests/reconcile_*.rs that get fixture updates here.
  cargo fmt -p crabka-operator -- --check
  ```

  Commit on branch `slice-50c-oauth-introspection`, `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`:
  ```
  T1: CRD — ListenerAuthenticationOAuth introspection-mode fields

  Strimzi-shape additions on the oauth listener variant:
  accessTokenIsJwt (default true), introspectionEndpointUri,
  userInfoEndpointUri, clientId, clientSecret ({secretName, key}),
  introspectionHttpTimeoutSeconds. jwksEndpointUri becomes
  Option<String> (was required; greenfield breaking change).
  Cross-mode validation lives in T3 (controller/listeners.rs).

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T2 — `ReconcileError` variants.** `crates/operator/src/controller/common.rs`.

  Find the existing `pub enum ReconcileError` (or whatever the variant set is called — grep). Add four new variants near related error variants (file-Secret + oauth-trust errors, e.g., near `MissingOauthTrustSecret` from slice 50b):

  ```rust
  #[error("listener OAuth: {0}")]
  InvalidListenerOauthAccessTokenIsJwt(String),

  #[error("oauth introspection Secret '{0}' not found")]
  MissingOauthIntrospectionSecret(String),

  #[error("oauth introspection Secret '{secret}' has no key '{key}'")]
  MissingOauthIntrospectionKey { secret: String, key: String },

  #[error("oauth introspection Secret '{secret}' key '{key}' is empty")]
  EmptyOauthIntrospectionValue { secret: String, key: String },
  ```

  If the enum has a `reason()` method returning `&'static str` (it does — used by status-condition patching), add arms:
  ```rust
  ReconcileError::InvalidListenerOauthAccessTokenIsJwt(_) => "InvalidListenerOauthAccessTokenIsJwt",
  ReconcileError::MissingOauthIntrospectionSecret(_) => "MissingOauthIntrospectionSecret",
  ReconcileError::MissingOauthIntrospectionKey { .. } => "MissingOauthIntrospectionKey",
  ReconcileError::EmptyOauthIntrospectionValue { .. } => "EmptyOauthIntrospectionValue",
  ```

  Verify (only `common.rs` should compile cleanly — downstream callers use them in T3/T4):
  ```bash
  cargo check -p crabka-operator --lib --message-format=short 2>&1 | grep "error" | grep -v "crd/listener\|controller/listeners\|controller/kafka_node_pool" | head
  ```

  Commit:
  ```
  T2: controller/common — 4 new ReconcileError variants for 50c

  InvalidListenerOauthAccessTokenIsJwt + 3 introspection-secret
  validation failures (MissingOauthIntrospectionSecret,
  MissingOauthIntrospectionKey, EmptyOauthIntrospectionValue). Wired
  into the existing reason() table. Consumers come in T3 (listeners.rs
  validation) and T4 (kafka.rs reconciler helper).

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 2 (parallel — disjoint files; depends on Batch 1)

- **T3 — Listener validation + TOML render fork.** `crates/operator/src/controller/listeners.rs`.

  Read the existing file to find `validate_listeners` (per-listener validation) and `render_broker_toml` (the broker TOML emission with the `[oauthbearer]` block).

  1. **Per-listener cross-mode validation.** Add a new block inside the existing per-listener validation loop, after the existing OAuth field-presence checks (issuer/jwks-uri-scheme/refresh/scope), for each OAuth listener:

     ```rust
     // Slice 50c: cross-mode field validity.
     let cfg = match &l.authentication {
         Some(ListenerAuthentication::OAuth(c)) => c,
         _ => return Ok(()),  // not oauth — nothing to check
     };
     if cfg.access_token_is_jwt {
         if cfg.jwks_endpoint_uri.is_none() {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=true requires jwksEndpointUri",
                 l.name,
             )));
         }
         if cfg.introspection_endpoint_uri.is_some()
             || cfg.user_info_endpoint_uri.is_some()
             || cfg.client_id.is_some()
             || cfg.client_secret.is_some()
             || cfg.introspection_http_timeout_seconds.is_some()
         {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=true forbids introspection-mode fields (introspectionEndpointUri/userInfoEndpointUri/clientId/clientSecret/introspectionHttpTimeoutSeconds)",
                 l.name,
             )));
         }
     } else {
         if cfg.jwks_endpoint_uri.is_some() {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=false forbids jwksEndpointUri",
                 l.name,
             )));
         }
         if cfg.introspection_endpoint_uri.is_none() {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=false requires introspectionEndpointUri",
                 l.name,
             )));
         }
         if cfg.client_id.is_none() {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=false requires clientId",
                 l.name,
             )));
         }
         if cfg.client_secret.is_none() {
             return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(format!(
                 "listener '{}': accessTokenIsJwt=false requires clientSecret",
                 l.name,
             )));
         }
     }
     ```

     Place this BEFORE the existing scheme-check on `jwks_endpoint_uri` so the cross-mode check fires first (clearer error ordering). And — since `jwks_endpoint_uri` is now `Option<String>` — adjust any existing slice-50 OAuth validator code that did `cfg.jwks_endpoint_uri.as_str()` to use `if let Some(uri) = &cfg.jwks_endpoint_uri { ... }` patterns.

  2. **TOML render fork.** Find the `[oauthbearer]` block emission in `render_broker_toml`. The existing flow emits keys based on a single canonical OAuth config (no validator-mode branching). Restructure:

     ```rust
     // existing prelude — emit jwks_endpoint_uri, valid_issuer_uri,
     // expected_audience, principal_claim_name, scope_claim_name,
     // required_scope, jwks_refresh_interval_ms,
     // allowable_clock_skew_ms, idp_tls_trust, jwks_tls_trust(actually idp_tls_trust)
     //
     // Modify so jwks_endpoint_uri is conditional on access_token_is_jwt:
     if cfg.access_token_is_jwt {
         if let Some(uri) = &cfg.jwks_endpoint_uri {
             let _ = writeln!(out, r#"jwks_endpoint_uri = "{uri}""#);
         }
         // existing jwks_refresh_interval_ms emission stays
     } else {
         // Slice 50c introspection-mode keys, in 49d FileOAuthBearerConfig field order.
         if let Some(uri) = &cfg.introspection_endpoint_uri {
             let _ = writeln!(out, r#"introspection_endpoint_uri = "{uri}""#);
         }
         if let Some(uri) = &cfg.user_info_endpoint_uri {
             let _ = writeln!(out, r#"userinfo_endpoint_uri = "{uri}""#);
         }
         if let Some(id) = &cfg.client_id {
             let _ = writeln!(out, r#"introspection_client_id = "{id}""#);
         }
         // The clientSecret value is mounted by T5 at this fixed path.
         let _ = writeln!(out, r#"introspection_client_secret_path = "/etc/crabka/oauth-introspection/client-secret""#);
         if let Some(s) = cfg.introspection_http_timeout_seconds {
             let _ = writeln!(out, "introspection_http_timeout_ms = {}", s * 1000);
         }
     }
     ```

  3. **Per-canonical-field divergence walk.** Find the existing `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` test (slice 50 T3 polish). Add 4 new perturbations:
     ```rust
     ("access_token_is_jwt", ListenerAuthenticationOAuth { access_token_is_jwt: false, ..base.clone() }),
     ("introspection_endpoint_uri", ListenerAuthenticationOAuth {
         introspection_endpoint_uri: Some("https://different/introspect".into()),
         ..base.clone()
     }),
     ("user_info_endpoint_uri", ListenerAuthenticationOAuth {
         user_info_endpoint_uri: Some("https://different/userinfo".into()),
         ..base.clone()
     }),
     ("client_secret", ListenerAuthenticationOAuth {
         client_secret: Some(OauthClientSecretRef { secret_name: "other-secret".into(), key: "k".into() }),
         ..base.clone()
     }),
     ```

     For these to actually trigger conflict detection (vs hitting `InvalidListenerOauthAccessTokenIsJwt` first), the `base` fixture probably needs to be JWT-mode and the perturbations stay JWT-mode-compatible. But the `access_token_is_jwt: false` and introspection-mode perturbations need their own mode-consistent fixture. **Simpler**: add a SECOND divergence test specifically for introspection-mode (e.g., `validate_listeners_rejects_two_oauth_listeners_with_divergent_introspection_config`) with an introspection-mode base + introspection-mode perturbations.

  4. **TOML render tests.** Add near the existing `render_broker_toml_emits_oauthbearer_block_*` tests:
     - `render_broker_toml_emits_introspection_keys_when_introspection_mode` — full introspection-mode listener (with clientSecret, etc.); assert TOML contains `introspection_endpoint_uri`, `introspection_client_id`, `introspection_client_secret_path = "/etc/crabka/oauth-introspection/client-secret"`.
     - `render_broker_toml_omits_jwks_endpoint_uri_in_introspection_mode`
     - `render_broker_toml_emits_userinfo_endpoint_when_set`
     - `render_broker_toml_emits_introspection_http_timeout_ms_when_set` — seconds → ms conversion (e.g., 15 → 15000).
     - `render_broker_toml_oauthbearer_block_emits_introspection_keys_in_canonical_order` — pin the exact byte ordering (extend the existing slice-50b canonical-order test pattern). Order: `introspection_endpoint_uri`, `userinfo_endpoint_uri`, `introspection_client_id`, `introspection_client_secret_path`, `introspection_http_timeout_ms`.

  5. **Validation tests.** Add near the existing `validate_listeners_rejects_oauth_*` tests:
     - `validate_listeners_rejects_oauth_jwt_mode_without_jwks_endpoint_uri`
     - `validate_listeners_rejects_oauth_introspection_mode_without_endpoint_uri`
     - `validate_listeners_rejects_oauth_introspection_mode_without_client_id`
     - `validate_listeners_rejects_oauth_introspection_mode_without_client_secret`
     - `validate_listeners_rejects_oauth_jwt_mode_with_introspection_fields`
     - `validate_listeners_rejects_oauth_introspection_mode_with_jwks_endpoint_uri`
     - `validate_listeners_rejects_oauth_userinfo_endpoint_without_introspection_mode`

  Verify:
  ```bash
  cargo test -p crabka-operator --lib controller::listeners:: 2>&1 | tail
  cargo fmt -p crabka-operator -- --check
  ```

  Commit:
  ```
  T3: Listener reconciler — cross-mode validation + introspection TOML

  Adds 7 new per-listener validation rules for accessTokenIsJwt cross-
  mode invariants (4 required-when, 3 forbidden-when). Forks
  render_broker_toml's [oauthbearer] block: jwks_endpoint_uri emitted
  only when access_token_is_jwt=true; introspection_* keys emitted
  only when false. Per-canonical-field divergence walk extended.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T4 — Reconciler trust-secret validation helper + types.** `crates/operator/src/controller/kafka.rs`.

  1. New type:
     ```rust
     /// Slice 50c. Describes the source Secret the operator mounts
     /// into broker pods for OAUTHBEARER introspection client-secret.
     /// Returned by `reconcile_oauth_introspection_secret` and
     /// re-derived deterministically from the parent Kafka CR via
     /// `oauth_introspection_secret_mount` for the pool reconciler.
     #[derive(Debug, Clone)]
     pub(crate) struct OauthIntrospectionMount {
         pub secret_name: String,
         pub key: String,
     }
     ```

  2. New `pub(crate) fn oauth_introspection_secret_mount(kafka: &Kafka) -> Option<OauthIntrospectionMount>` (mirrors slice-50b's `oauth_jwks_trust_secret_name` pattern — purely deterministic, no I/O):

     ```rust
     pub(crate) fn oauth_introspection_secret_mount(kafka: &Kafka) -> Option<OauthIntrospectionMount> {
         let canonical = canonical_oauth_config(&kafka.spec.listeners)?;
         if canonical.access_token_is_jwt {
             return None;
         }
         let cs = canonical.client_secret.as_ref()?;
         Some(OauthIntrospectionMount {
             secret_name: cs.secret_name.clone(),
             key: cs.key.clone(),
         })
     }
     ```

  3. New `async fn reconcile_oauth_introspection_secret` that VALIDATES the source Secret + key:

     ```rust
     async fn reconcile_oauth_introspection_secret(
         secret_api: &Api<k8s_openapi::api::core::v1::Secret>,
         kafka: &Kafka,
         canonical: Option<&crate::crd::ListenerAuthenticationOAuth>,
     ) -> Result<Option<OauthIntrospectionMount>, ReconcileError> {
         let Some(c) = canonical else { return Ok(None); };
         if c.access_token_is_jwt {
             return Ok(None);
         }
         let cs = c.client_secret.as_ref().ok_or_else(||
             ReconcileError::InvalidListenerOauthAccessTokenIsJwt(
                 "introspection mode requires clientSecret".into(),
             )
         )?;
         let src = secret_api.get_opt(&cs.secret_name).await?
             .ok_or_else(|| ReconcileError::MissingOauthIntrospectionSecret(cs.secret_name.clone()))?;
         let val = src.data.as_ref().and_then(|d| d.get(&cs.key))
             .ok_or_else(|| ReconcileError::MissingOauthIntrospectionKey {
                 secret: cs.secret_name.clone(),
                 key: cs.key.clone(),
             })?;
         if val.0.is_empty() {
             return Err(ReconcileError::EmptyOauthIntrospectionValue {
                 secret: cs.secret_name.clone(),
                 key: cs.key.clone(),
             });
         }
         Ok(Some(OauthIntrospectionMount {
             secret_name: cs.secret_name.clone(),
             key: cs.key.clone(),
         }))
     }
     ```

     **Note**: no managed-Secret upsert. The source Secret is mounted DIRECTLY by T5's pod template via projected `items`. The helper just validates.

  4. Call-site insertion in `reconcile_kafka`. Find where the existing slice-50b `reconcile_oauth_jwks_trust` is called. Add after it (introspection validation is independent — runs even if no trust certs configured):

     ```rust
     // Slice 50c: validate the OAUTHBEARER introspection client-secret
     // Secret (when introspection is configured).
     let _oauth_introspection_mount = match reconcile_oauth_introspection_secret(
         &secret_api,
         &obj,
         oauth_canonical.as_ref(),
     )
     .await
     {
         Ok(mount) => mount,
         Err(e) => {
             patch_status_with_condition(/* same shape as the existing slice-50b call */).await?;
             return Ok(Action::requeue(Duration::from_secs(30)));
         }
     };
     ```

     The return value is captured but unused by `reconcile_kafka` itself (the pool reconciler derives the mount independently via `oauth_introspection_secret_mount`). Mark with `let _` and a comment explaining T5's pool reconciler reads it via the helper. **Or**, if there's a place in `reconcile_kafka` that already threads `oauth_jwks_trust_secret` into the StatefulSet render, also thread `oauth_introspection_mount` there. Inspect first: if the pool reconcile is fully self-contained (per T5 — kafka_node_pool.rs reads the parent Kafka CR), then `let _` is correct.

  5. New unit tests (the helper paths that don't need a kube mock):
     - `oauth_introspection_secret_mount_returns_none_when_canonical_absent`
     - `oauth_introspection_secret_mount_returns_none_when_access_token_is_jwt_true`
     - `oauth_introspection_secret_mount_returns_none_when_client_secret_absent`
     - `oauth_introspection_secret_mount_returns_some_for_introspection_config`

  Verify:
  ```bash
  cargo test -p crabka-operator --lib controller::kafka:: 2>&1 | tail
  cargo fmt -p crabka-operator -- --check
  ```

  Commit:
  ```
  T4: Reconciler — oauth introspection secret validation + helpers

  New OauthIntrospectionMount type + reconcile_oauth_introspection_secret
  async helper (validates source Secret + named key exist, returns the
  mount info; no managed-Secret upsert — T5 mounts the source Secret
  directly via projected items). New oauth_introspection_secret_mount
  pub(crate) helper deterministically derives the mount from the
  parent Kafka CR's listeners (for the pool reconciler). 3 new
  Ready=False conditions wired.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 3 (single — depends on T4's `OauthIntrospectionMount`)

- **T5 — Pod template volume + mount.** `crates/operator/src/controller/kafka_node_pool.rs`.

  1. Add `oauth_introspection_mount: Option<&OauthIntrospectionMount>` parameter to `render_storage`. When `Some(mount)`, append to `volumes`:
     ```rust
     if let Some(mount) = oauth_introspection_mount {
         volumes_array.as_array_mut()
             .expect("render_storage built `volumes` via json!([...])")
             .push(json!({
                 "name": "oauth-introspection-secret",
                 "secret": {
                     "secretName": mount.secret_name,
                     "items": [{ "key": mount.key, "path": "client-secret" }],
                     "defaultMode": 0o400_i32,
                 }
             }));
     }
     ```

  2. Add `oauth_introspection_mount_path: Option<&str>` parameter to `render_broker_container`. When `Some(path)`, append to `volume_mounts`:
     ```rust
     if let Some(path) = oauth_introspection_mount_path {
         volume_mounts.push(json!({
             "name": "oauth-introspection-secret",
             "mountPath": path,
             "readOnly": true,
         }));
     }
     ```

     The mount-path constant is `/etc/crabka/oauth-introspection`. Caller passes that literal.

  3. Call site in `kafka_node_pool.rs::reconcile` (or `render_statefulset` — wherever the storage + container render is invoked). Derive the mount via T4's helper, build both args:
     ```rust
     // Slice 50c: introspection client-secret mount (Option<...>).
     let oauth_introspection_mount =
         crate::controller::kafka::oauth_introspection_secret_mount(parent);
     let oauth_introspection_mount_path = oauth_introspection_mount
         .as_ref()
         .map(|_| "/etc/crabka/oauth-introspection");
     // existing render_storage call adds the new arg:
     let (volumes, vct) = render_storage(
         // ...existing args including oauth_jwks_trust_secret_name from slice 50b...
         oauth_introspection_mount.as_ref(),
     );
     // existing render_broker_container call adds the new arg:
     let main = render_broker_container(
         // ...existing args...
         oauth_introspection_mount_path,
     );
     ```

  4. **Update every other call site** of `render_storage` and `render_broker_container` (likely test fixtures) to pass `None` for the new arg. Audit with:
     ```bash
     grep -rn "render_storage(\|render_broker_container(" crates/operator/src crates/operator/tests | head
     ```

  5. New tests inside `#[cfg(test)] mod tests` in `kafka_node_pool.rs`:
     - `render_statefulset_mounts_oauth_introspection_secret_when_introspection_mode` — construct a Kafka CR with an introspection-mode OAuth listener + a `clientSecret` ref. Render. Assert the volumes contain `{"name":"oauth-introspection-secret","secret":{"secretName":"...","items":[{"key":"...","path":"client-secret"}],...}}` and the volumeMounts contain `{"name":"oauth-introspection-secret","mountPath":"/etc/crabka/oauth-introspection","readOnly":true}`.
     - `render_statefulset_omits_oauth_introspection_volume_when_jwt_mode` — JWT-mode listener; assert neither the volume nor the mount appears.

  Verify:
  ```bash
  cargo build -p crabka-operator 2>&1 | tail
  # Should be CLEAN now. T1+T2+T3+T4 all landed; T5 closes the last
  # signature gap (the new render_storage/render_broker_container params).
  cargo test -p crabka-operator --lib 2>&1 | tail
  cargo fmt -p crabka-operator -- --check
  cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail -3
  ```

  Commit:
  ```
  T5: Pod template — oauth-introspection-secret volume + mount

  Threads Option<&OauthIntrospectionMount> through render_storage and
  Option<&str> through render_broker_container. Source Secret mounted
  directly via projected items so the broker reads from a fixed path
  (/etc/crabka/oauth-introspection/client-secret) regardless of the
  user's source key name. Mirrors slice 50b's oauth_jwks_trust_secret
  pattern.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 4 (parallel — disjoint files; depends on T5)

- **T6 — Integration tests.** Two files:

  1. **Extend `crates/operator/tests/reconcile_listener_oauth.rs`** with one new test:
     ```rust
     #[tokio::test]
     async fn two_oauth_listeners_with_divergent_access_token_is_jwt_rejected_with_conflicting_oauth_config() {
         // Listener A: JWT mode. Listener B: introspection mode.
         // Both pass per-listener validation; both fail the
         // cross-listener canonical guard.
     }
     ```

  2. **New file `crates/operator/tests/reconcile_oauth_introspection.rs`** with 9 tests mirroring slice 50b's `reconcile_oauth_trust.rs`:
     - `oauth_introspection_validates_source_secret_and_mounts_it`
     - `oauth_introspection_missing_source_secret_rejects_with_missing_oauth_introspection_secret`
     - `oauth_introspection_missing_key_in_secret_rejects_with_missing_oauth_introspection_key`
     - `oauth_introspection_empty_key_value_rejects_with_empty_oauth_introspection_value`
     - `oauth_introspection_jwt_mode_does_not_mount_anything` — assert no `oauth-introspection-secret` volume in the StatefulSet PATCH body.
     - `oauth_introspection_managed_pod_template_mounts_secret_with_projected_items` — assert the volume's `items[0]` is `{"key":<user's key>,"path":"client-secret"}` (the projection mapping that hides the user's key name from the broker).
     - `oauth_introspection_with_userinfo_renders_userinfo_endpoint_in_toml`
     - `statefulset_mounts_oauth_introspection_secret_when_introspection_mode`
     - `statefulset_omits_oauth_introspection_volume_when_jwt_mode`

     File-header allow attribute: `#![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]` (matches slice-50b's `reconcile_oauth_trust.rs`).

     Reuse `tests/shared/*` fixtures + the `rules_for_failure_path` style introduced in slice 50b's T6.

  Verify:
  ```bash
  cargo test -p crabka-operator --test reconcile_oauth_introspection 2>&1 | tail
  cargo test -p crabka-operator --test reconcile_listener_oauth 2>&1 | tail
  ```

  Commit:
  ```
  T6: Integration tests — reconcile_oauth_introspection.rs + listener-oauth divergence

  Nine new integration tests covering the introspection-secret
  validation lifecycle: happy-path mount with projected items, three
  source-Secret failure modes, JWT-mode no-op, userinfo TOML render,
  StatefulSet mount-when-some, StatefulSet omit-when-none. Plus one
  listener-OAuth divergence test (accessTokenIsJwt true vs false
  rejected as ConflictingOAuthConfig).

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T7 — Sample manifest + CRD regen.**

  1. Append a second oauth listener block to `crates/operator/sample/oauth-listener.yaml` showing the introspection variant:
     ```yaml
     # Alternative: introspection mode (slice 50c).
     # The broker calls Keycloak's introspection endpoint for each
     # token instead of validating a signed JWT against JWKS. Set
     # `accessTokenIsJwt: false` and provide an introspectionEndpointUri,
     # clientId, and clientSecret Secret reference.
     #
     # - name: oauth-introspection
     #   port: 9098
     #   type: internal
     #   tls: true
     #   authentication:
     #     type: oauth
     #     accessTokenIsJwt: false
     #     validIssuerUri: https://keycloak.example/realms/kafka
     #     introspectionEndpointUri: https://keycloak.example/realms/kafka/protocol/openid-connect/token/introspect
     #     userInfoEndpointUri: https://keycloak.example/realms/kafka/protocol/openid-connect/userinfo
     #     validAudience: kafka-broker
     #     userNameClaim: preferred_username
     #     customClaimCheck:
     #       scope: kafka.write
     #     clientId: kafka-broker
     #     clientSecret:
     #       secretName: keycloak-introspection-secret
     #       key: secret
     ```
     Keep it COMMENTED-OUT so the manifest still applies cleanly without the user creating the Secret first (the JWT-mode block stays as the working example).

  2. Regenerate CRDs:
     ```bash
     bash tools/regen-crds.sh 2>&1 | tail
     git diff --stat deploy/crds/
     ```
     Expected: `deploy/crds/crabka.io_kafkas.yaml` only. The diff under `spec.listeners[].authentication` should add: `accessTokenIsJwt` (boolean), `introspectionEndpointUri`, `userInfoEndpointUri`, `clientId`, `clientSecret` (object with required secretName + key), `introspectionHttpTimeoutSeconds` (integer). The existing `jwksEndpointUri` should flip from `required` to `nullable: true`.

  3. Validate the sample still parses:
     ```bash
     cat crates/operator/sample/oauth-listener.yaml | python3 -c "import sys, yaml; docs = list(yaml.safe_load_all(sys.stdin)); print(f'{len(docs)} docs: {[d.get(\"kind\") for d in docs]}')"
     ```
     Expected: `3 docs: ['Kafka', 'KafkaNodePool', 'KafkaUser']`.

  Commit:
  ```
  T7: Sample manifest + regenerate CRD YAML

  Adds a commented-out introspection-mode listener block to
  oauth-listener.yaml (next to the existing JWT example).
  Regenerates deploy/crds/crabka.io_kafkas.yaml with the 6 new
  introspection sibling properties + jwksEndpointUri flipped from
  required to nullable.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T8 — kind-oauth-introspection e2e.** `.github/workflows/operator-e2e.yml`.

  Clone the existing `kind-oauth` job (slice 50b's HTTPS Keycloak setup, ~lines 1960-end). Place adjacent as a new top-level `jobs.kind-oauth-introspection`. Gating:
  ```yaml
  if: ${{ github.event_name == 'push' || contains(github.event.pull_request.labels.*.name, 'e2e-oauth-introspection') }}
  needs: build-images
  ```

  Diffs from the existing `kind-oauth` job:

  1. **Realm bootstrap** (kcadm step) extends with:
     ```bash
     # Existing kafka-client (used by the producer Jobs) stays.
     # Create a SECOND confidential client for the broker's
     # introspection calls.
     kcadm.sh create clients -r kafka -s clientId=kafka-broker \
       -s publicClient=false -s serviceAccountsEnabled=true \
       -s standardFlowEnabled=false -s directAccessGrantsEnabled=false
     BROKER_CID=$(kcadm.sh get clients -r kafka -q clientId=kafka-broker --fields id --format csv --noquotes | tail -1)
     BROKER_SECRET=$(kcadm.sh get clients/$BROKER_CID/client-secret -r kafka --fields value --format csv --noquotes | tail -1)
     echo -n "$BROKER_SECRET" > /tmp/kc-out/broker-client-secret
     ```
     Then `kubectl cp` the broker-client-secret file out of the pod and create a kube Secret in `default` ns:
     ```bash
     kubectl create secret generic keycloak-introspection-secret \
       --namespace=default \
       --from-file=secret=/tmp/local/broker-client-secret
     ```

  2. **Kafka CR YAML** (the `kubectl apply -f -` heredoc) flips OAuth listener to introspection mode:
     ```yaml
     authentication:
       type: oauth
       accessTokenIsJwt: false
       validIssuerUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka
       introspectionEndpointUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka/protocol/openid-connect/token/introspect
       userInfoEndpointUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka/protocol/openid-connect/userinfo
       validAudience: kafka-broker
       userNameClaim: preferred_username
       customClaimCheck: { scope: kafka.write }
       clientId: kafka-broker
       clientSecret:
         secretName: keycloak-introspection-secret
         key: secret
       tlsTrustedCertificates:
         - secretName: keycloak-ca
           certificate: tls.crt
     ```
     (The `validIssuerUri` and the JWT-mode `tlsTrustedCertificates` block stay — both modes use the trust bundle for HTTPS.)

  3. **Producer Jobs**: UNCHANGED. The token endpoint URL is the same; the producer's JAAS doesn't change whether the broker validates via JWT or introspection.

  4. **WeakAuth assertion**: stays inverted (no HTTP URLs).

  5. **Diagnostics step**: include `kubectl get secret keycloak-introspection-secret -n default -o yaml` in the failure-collection block.

  6. **Cluster name**: pick a distinct cluster name for the kind cluster (e.g. `crabka-oauth-introspection-e2e`) so it doesn't collide with `kind-oauth`.

  Verify locally (can't run kind):
  ```bash
  python3 -c "import yaml; w=yaml.safe_load(open('.github/workflows/operator-e2e.yml')); print('jobs:', list(w['jobs'].keys()))"
  # Expect 'kind-oauth-introspection' in the list (7 jobs total now).
  actionlint .github/workflows/operator-e2e.yml 2>&1 | head || echo "actionlint not available"
  ```

  Commit:
  ```
  T8: kind-oauth-introspection e2e — introspection-mode Keycloak

  Clones kind-oauth's HTTPS Keycloak setup, adds a kafka-broker
  confidential Keycloak client (for the broker's introspection calls),
  captures its client secret into a kube Secret, and flips the Kafka
  CR to accessTokenIsJwt: false + introspectionEndpointUri +
  userInfoEndpointUri + clientId + clientSecret ref. Producer Jobs
  unchanged. Label-gated e2e-oauth-introspection + push: main.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 5 (single — STATUS + final gate)

- **T9 — STATUS.md entry + final gate.**

  1. Append `## Slice 50c — Operator: Listener OAuth introspection surface (2026-05-24)` at the end of `STATUS.md`. Read slice 50b's entry first (`grep -A 70 "^## Slice 50b " STATUS.md | head -75`) for tone. ~50-60 lines.

     Cover:
     - **Opener** (2-3 sentences): surfaces 49d's broker introspection on the CRD; explicit `accessTokenIsJwt` for Strimzi parity; new `kind-oauth-introspection` e2e variant.
     - **CRD changes**: `jwksEndpointUri` now `Option<String>`; 6 new fields + `OauthClientSecretRef` struct; updated hand-rolled schema.
     - **Reconciler**: 4 new Ready=False reasons (`InvalidListenerOauthAccessTokenIsJwt`, `MissingOauthIntrospectionSecret`, `MissingOauthIntrospectionKey`, `EmptyOauthIntrospectionValue`). New `OauthIntrospectionMount` struct + `oauth_introspection_secret_mount` pub(crate) helper + `reconcile_oauth_introspection_secret` async validator.
     - **TOML render fork** in `controller/listeners.rs::render_broker_toml`: `access_token_is_jwt: true` → emit `jwks_endpoint_uri = ...`; `false` → emit introspection-mode keys in 49d field order.
     - **Pod template**: source Secret mounted DIRECTLY via projected `items` so the broker reads from `/etc/crabka/oauth-introspection/client-secret` regardless of the user's source key. No managed-Secret intermediate (unlike slice 50b's trust-bundle concat).
     - **E2E**: new `kind-oauth-introspection` job. Adds a second Keycloak client `kafka-broker` (confidential, service-account) for introspection auth. Label-gated `e2e-oauth-introspection`.
     - **Tests**: 6 new `crd::listener` unit + 7 new `controller::listeners` validation + 5 new `controller::listeners` TOML render + 4 new `controller::kafka` unit (helper paths) + 2 new `controller::kafka_node_pool` (pod-mount) + 1 new `tests/reconcile_listener_oauth.rs` extension + 9 new `tests/reconcile_oauth_introspection.rs`. Workspace clippy `-D warnings` + fmt clean.
     - **Reference doc**: `[docs/superpowers/specs/2026-05-24-crabka-operator-oauth-introspection-50c-design.md]`.
     - **Out of scope**: per-listener introspection config (still rejected; future 49h); source-Secret reflector for instant rotation; cross-namespace Secret refs; `client_secret_post` / `private_key_jwt` (49d Basic-Auth only); outbound mTLS to IdP; operator-managed Keycloak client provisioning (ops bootstrap the IdP's `kafka-broker` client out-of-band).

  2. Final gate:
     ```bash
     cd /Users/mattstone/git/crabka/.worktrees/slice-50c-oauth-introspection
     cargo fmt --check
     cargo clippy --workspace --all-targets -- -D warnings
     cargo test --workspace
     bash tools/regen-crds.sh && git diff --exit-code -- deploy/crds/
     ```
     All four must be green. Known pre-existing flake: `auto_rebalance_restores_preferred_leader` in `crates/broker/tests/elect_leaders.rs` can time out under parallel load. If it fires, re-run in isolation.

  Commit:
  ```
  Slice 50c: STATUS.md entry + final gate

  Documents the new operator introspection surface and the
  kind-oauth-introspection e2e. fmt + clippy + workspace tests + CRD
  drift gate all green.

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

## Notes

- **Dependency chain**: (T1 ‖ T2) → (T3 ‖ T4) → T5 → (T6 ‖ T7 ‖ T8) → T9. Five batches, nine tasks.
- **T1 + T2 file-disjoint**: T1 = `crd/listener.rs`, T2 = `controller/common.rs`. Both leaves.
- **T3 + T4 file-disjoint**: T3 = `controller/listeners.rs`, T4 = `controller/kafka.rs`. Both depend on T1 + T2.
- **T5 sequential**: needs T4's `OauthIntrospectionMount` type signature.
- **T6 + T7 + T8 file-disjoint**: T6 = new `tests/reconcile_oauth_introspection.rs` + `tests/reconcile_listener_oauth.rs` extension; T7 = sample + `deploy/crds/*.yaml`; T8 = `.github/workflows/operator-e2e.yml`. All depend on T5.
- **CLAUDE.md greenfield rename**: `jwksEndpointUri` flips from required to optional. No backward-compat alias.
- **Worktree dependency on slice 49d**: this plan assumes 49d is in main (PR #175 merged). If the implementer creates the slice-50c worktree before 49d merges, branch from `slice-49d-oauth-introspection` instead of `main`, OR rebase onto `main` after 49d merges. The reconciler tests' TOML round-trip via `crabka_broker::file_config::FileConfig` requires 49d's new keys + `idp_tls_trust` rename to be available.
- **Test fixture sweep in T1**: every existing `ListenerAuthenticationOAuth { ... }` literal in operator code/tests needs 6 new default field initializers AND `jwks_endpoint_uri: "...".into()` → `jwks_endpoint_uri: Some("...".into())`. Mechanical but pervasive (probably ~15-20 sites across `controller/listeners.rs`, `tests/reconcile_listener_oauth.rs`, `tests/reconcile_oauth_trust.rs`). T1 must touch ALL of them to keep the build green at HEAD.
- **No JVM differential test for this slice** — the JVM admin tools don't read OAuth listener config; the e2e is the integration check.
- **After 50c lands**, the umbrella's next pair is **49e + 50d** (KIP-368 SASL re-authentication — broker adds `session_lifetime_ms` on `SaslAuthenticateResponse v1+` + connection expiry timer; operator surfaces `maxSecondsWithoutReauthentication`).
