# Slice 50: Operator — Listener OAuth + `KafkaUser` tls-external — Implementation plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 50 — Operator: Listener OAuth + `KafkaUser` tls-external (2026-05-23)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Listener tlsTrustedCertificates for custom CA trust to the IdP (closed by slices 49c + 50b)
- Opaque-token introspection (closed by slices 49d + 50c)
- KIP-368 re-authentication (maxSecondsWithoutReauthentication, closed by slices 49e + 50d)
- PLAIN-with-OAuth-token + tokenEndpointUri (closed by slices 49f + 50e — slice 49f is indefinitely deferred per slice 49g/49h notes)
- The remaining Strimzi long-tail — groupsClaim, fallback-username chain, validTokenType, multi-rule customClaimCheck, JWKS refresh policy knobs, jwksIgnoreKeyUse (closed by slices 49g + 50f)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (matches the project's CLAUDE.md mandate to execute in parallel batches). Steps use the project's compact-batch style — each T is one focused PR-worth of work, file-conflict-disjoint within a batch.

**Design:** `docs/superpowers/specs/2026-05-23-crabka-operator-listener-user-oauth-50-design.md`
**Umbrella:** `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`

**Goal:** Operator surface for the OAUTHBEARER core work landed in 49/49b. Add an `oauth` listener authentication variant + a `tls-external` `KafkaUser` authentication variant. CRD field surface is exactly what 49b's broker validator already honors — no half-finished fields, no broker changes.

**Architecture:** Two file-disjoint halves (listener side: `listener.rs` + `listeners.rs`; user side: `user.rs` CRD + `user.rs` controller) that can be implemented in parallel batches. The listener reconciler renders a broker-global `[oauthbearer]` TOML block (mirroring `crates/broker/src/file_config.rs::FileOAuthBearerConfig` 1:1) and appends `OAUTHBEARER` to the per-listener `sasl_mechanisms` list. The user reconciler grows a third arm — `TlsExternal` — that runs the existing ACL + quota reconciliation under the bare-name principal `User:<metadata.name>` and skips Secret / cert provisioning entirely. E2E uses the Bitnami Keycloak chart in kind, gated to `push: main` + PRs labeled `e2e-oauth`.

**Tech stack:** Same as existing operator slices — kube-rs 3.x `CustomResource`, schemars 1.x, serde, TOML rendering via plain `String` writeln, integration tests under `crates/operator/tests/`, kind + Bitnami Keycloak chart for e2e.

---

## Batches

### Batch 1 (parallel — disjoint files)

- **T1 — Listener OAuth CRD.** `crates/operator/src/crd/listener.rs`:
  - Add struct
    ```rust
    #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    pub struct ListenerAuthenticationOAuth {
        pub valid_issuer_uri: String,
        pub jwks_endpoint_uri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub valid_audience: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub user_name_claim: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub custom_claim_check: Option<OAuthCustomClaimCheck>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub jwks_refresh_seconds: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub max_clock_skew_seconds: Option<u32>,
        #[serde(default = "default_true", skip_serializing_if = "is_default_true")]
        pub enable_oauth_bearer: bool,
    }
    fn default_true() -> bool { true }
    fn is_default_true(b: &bool) -> bool { *b }

    #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    pub struct OAuthCustomClaimCheck {
        pub scope: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub scope_claim: Option<String>,
    }
    ```
  - Change `ListenerAuthentication` from `Copy` to non-`Copy` (the new variant carries a `String`); add the new variant:
    ```rust
    #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
    #[serde(tag = "type")]
    #[schemars(schema_with = "listener_authentication_schema")]
    pub enum ListenerAuthentication {
        #[serde(rename = "tls")]    Tls,
        #[serde(rename = "scram-sha-512")] ScramSha512,
        #[serde(rename = "scram-sha-256")] ScramSha256,
        #[serde(rename = "oauth")]  OAuth(ListenerAuthenticationOAuth),
    }
    ```
    (Drop the `Copy` derive on the enum. The follow-up Copy → non-Copy mechanical fixes — `controller/listeners.rs::sasl_mechanism` (takes by value), `controller/listeners.rs::listener_protocol` (matches `(l.tls, l.authentication)`), `controller/listeners.rs::validate_listeners` (uses `matches!(l.authentication, Some(ListenerAuthentication::Tls))` which is fine because `matches!` doesn't move), and any `Listener.authentication` consumer that takes by value rather than by reference — are T3's problem. T1 just needs the enum + its in-file tests to compile.)
  - Extend `listener_authentication_schema` so the discriminator `enum` lists `tls`, `scram-sha-512`, `scram-sha-256`, `oauth`, and add sibling property schemas for `validIssuerUri`, `jwksEndpointUri`, `validAudience`, `userNameClaim`, `customClaimCheck`, `jwksRefreshSeconds`, `maxClockSkewSeconds`, `enableOauthBearer`.
  - Unit tests (in-file, `mod auth_tests`):
    - `oauth_authentication_round_trips_full_config`
    - `oauth_authentication_round_trips_minimum_required`
    - `oauth_with_custom_claim_check_round_trips`
    - `oauth_default_enable_omitted_on_serialize`
    - `oauth_enable_false_round_trips`
    - `oauth_unknown_subfield_rejected`
  - Update every `Listener { authentication: …, .. }` and `ListenerAuthentication::…` literal already in the file (and any consumer that destructures with `match` exhaustively — `controller/listeners.rs`) is T3's problem, but make sure this T compiles by adding any local fixture updates needed inside `listener.rs` itself.

- **T2 — KafkaUser tls-external CRD.** `crates/operator/src/crd/user.rs`:
  - Add variant:
    ```rust
    #[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(tag = "type", rename_all = "kebab-case")]
    #[schemars(schema_with = "authentication_schema")]
    pub enum Authentication {
        #[serde(rename = "scram-sha-512")] ScramSha512(ScramSha512Auth),
        #[serde(rename = "tls")]            Tls(TlsAuth),
        #[serde(rename = "tls-external")]   TlsExternal,
    }
    ```
  - Extend `authentication_schema` `type.enum` to include `"tls-external"`. No sibling property changes (tls-external carries no fields).
  - Extend `KafkaUserStatus`:
    ```rust
    /// Slice 50: `true` once a credential-less user
    /// (`type: tls-external`) has been reconciled. Surfaces in
    /// `kubectl describe ku` so operators can tell at a glance that
    /// the operator does not own this user's credentials.
    #[serde(default)]
    pub external: bool,
    ```
  - In-file unit tests:
    - `tls_external_round_trips`
    - `tls_external_with_quotas_and_acls_round_trips`
    - `tls_external_status_external_field_emitted_when_true`
    - `tls_external_status_external_field_omits_default_false`  (mirror the `scramSha512`/`tls` pattern: emit `false` as a literal)
    - `tls_external_minimum_spec_parses` — `{"authentication":{"type":"tls-external"}}`
  - Match exhaustiveness sweep: any `match authentication { ScramSha512 … | Tls … }` literal inside `user.rs` itself gets a `TlsExternal` arm too (this is T1's tactic — make the file compile on its own; T4 handles `controller/user.rs`).

### Batch 2 (parallel — disjoint files; depends on T1 + T2)

- **T3 — Listener reconciler + TOML render.** `crates/operator/src/controller/listeners.rs`:
  - Update `listener_protocol(l: &Listener) -> ListenerProtocol`: the existing body matches `(l.tls, l.authentication)` which works only while the enum is `Copy`. Change to `match (l.tls, &l.authentication) { ... }` and update each arm pattern: `None | Some(Tls)` → `None | Some(&Tls)`, `Some(ScramSha512 | ScramSha256)` → `Some(&ScramSha512 | &ScramSha256)`, and add `Some(&OAuth(_))` arms mapping to `SaslPlaintext` (if `!l.tls`) / `SaslSsl` (if `l.tls`). Same shape as the SCRAM arms.
  - Update `fn sasl_mechanism(auth: ListenerAuthentication) -> Option<SaslMechanism>` so it takes `&ListenerAuthentication` (no longer `Copy`) and returns `Some(SaslMechanism::OAuthBearer)` for `OAuth(cfg) if cfg.enable_oauth_bearer` and `None` for `OAuth(cfg) if !cfg.enable_oauth_bearer` (lets operators keep the config block but disable the listener mechanism — symmetric with Strimzi). Update the one call site in `render_broker_toml`.
  - Add a new `ValidationError` variant set + reason strings:
    - `ListenerOauthRequiresTransportTls(String)` → reason `"ListenerOauthRequiresTransportTls"`
    - `ListenerOauthIssuerUriEmpty(String)` → reason `"ListenerOauthInvalidUri"` (issuer is matched as a literal string against the token `iss` claim — any scheme; only require non-empty)
    - `ListenerOauthJwksUriBadScheme(String)` → reason `"ListenerOauthInvalidUri"` (must parse as `http://` or `https://`; reject everything else)
    - `ListenerOauthJwksRefreshTooSmall { listener: String, got: u32 }` → reason `"ListenerOauthInvalidRefresh"` (must be ≥ 30)
    - `ListenerOauthCustomClaimCheckScopeEmpty(String)` → reason `"ListenerOauthInvalidScope"`
    - `ConflictingOAuthListenerConfig` → reason `"ConflictingOAuthConfig"`, message `"all OAuth listeners must share identical config (per-listener OAuth is a future broker slice)"`
  - Extend `weak_auth_warnings(&[Listener]) -> Vec<String>` to also emit a warning for any OAuth listener whose `jwksEndpointUri` starts with `http://`: `"listener '<name>' has http:// JWKS endpoint; key material traverses the network in cleartext. Consider https."` This is per-listener and accumulates in the existing `WeakAuth` Event flow (no new Event code).
  - Extend `validate_listeners` to run the per-listener OAuth checks (in the existing per-listener loop) and a new post-loop pass that collects all OAuth listener configs and asserts they are pairwise `==` (use `Eq` on `ListenerAuthenticationOAuth` *with `enable_oauth_bearer` masked out* — divergent `enableOauthBearer` is allowed since it only affects per-listener mechanism enable; everything else must match). Easiest implementation: collect distinct `OAuthCanonical { valid_issuer_uri, jwks_endpoint_uri, valid_audience, user_name_claim, custom_claim_check, jwks_refresh_seconds, max_clock_skew_seconds }` values; if `> 1` distinct, return `ConflictingOAuthListenerConfig`.
  - In `render_broker_toml`, after the `[server_properties]` block and before the `[tls_config]` block, emit a single `[oauthbearer]` section whenever **any** listener has `OAuth(_)` authentication. Use the canonical config from the validation pass. Emit each key only when the CRD set the corresponding field. Sample bytes:
    ```toml
    [oauthbearer]
    jwks_endpoint_uri = "<jwksEndpointUri>"
    valid_issuer_uri = "<validIssuerUri>"
    expected_audience = "<validAudience>"                    # only if Some
    principal_claim_name = "<userNameClaim>"                 # only if Some
    scope_claim_name = "<customClaimCheck.scopeClaim>"       # only if Some
    required_scope = "<customClaimCheck.scope>"              # only if customClaimCheck Some
    jwks_refresh_interval_ms = <jwksRefreshSeconds * 1000>   # only if Some
    allowable_clock_skew_ms = <maxClockSkewSeconds * 1000>   # only if Some
    ```
    Render keys in the exact above order so the byte output stays stable (slice-21 hash invariant).
  - Function signature change: `render_broker_toml` now needs the listener list to find OAuth config (it already has `&[Listener]`). No new argument required.
  - In-file unit tests under `mod toml_rendering_tests`:
    - `render_broker_toml_emits_oauthbearer_block_for_oauth_listener` — full-config listener, assert all eight keys present.
    - `render_broker_toml_omits_oauthbearer_optional_keys_when_unset` — minimum listener (jwks + issuer only), assert other keys absent.
    - `render_broker_toml_appends_oauthbearer_to_listener_sasl_mechanisms` — `sasl_config = { enabled_mechanisms = ["OAUTHBEARER"] }`.
    - `render_broker_toml_with_enable_false_keeps_oauthbearer_block_but_omits_mechanism` — block present, `sasl_config` absent on that listener.
    - `render_broker_toml_does_not_emit_oauthbearer_block_when_no_oauth_listener`.
    - `render_broker_toml_oauthbearer_block_parses_with_broker_FileConfig` — sanity round-trip into `crabka_broker::file_config::FileConfig`, assert `FileOAuthBearerConfig` fields match.
    - `render_broker_toml_oauthbearer_render_is_deterministic` — two calls produce byte-identical output.
  - Validation tests under the existing test module:
    - `validate_listeners_rejects_oauth_without_tls`
    - `validate_listeners_accepts_oauth_with_http_jwks_uri`  (HTTP is allowed)
    - `validate_listeners_rejects_oauth_with_ftp_jwks_uri`  (only http/https accepted)
    - `validate_listeners_rejects_oauth_with_empty_issuer_uri`
    - `validate_listeners_accepts_oauth_with_non_uri_issuer_string`  (issuer is matched as literal text against `iss` claim)
    - `validate_listeners_rejects_oauth_with_short_jwks_refresh`
    - `validate_listeners_rejects_oauth_custom_claim_check_with_empty_scope`
    - `validate_listeners_accepts_two_oauth_listeners_with_identical_config`
    - `validate_listeners_accepts_two_oauth_listeners_differing_only_in_enable_oauth_bearer`
    - `validate_listeners_rejects_two_oauth_listeners_with_divergent_config`
    - `weak_auth_warnings_emitted_for_oauth_with_http_jwks_uri`
    - `weak_auth_warnings_empty_for_oauth_with_https_jwks_uri`
  - Sweep any local fixtures in `listeners.rs` that construct `Listener { authentication: …, .. }` so the file builds.

- **T4 — KafkaUser controller TlsExternal arm.** `crates/operator/src/controller/user.rs`:
  - Update `principal_for(name, auth)` to handle `Authentication::TlsExternal` → `format!("User:{name}")` (bare-name principal, same as SCRAM).
  - In `reconcile`, extend the credential-provisioning `match &obj.spec.authentication { … }` block to add a `TlsExternal` arm that:
    - Does no Secret work.
    - Returns `None` for `tls_not_after`.
    - Drops straight through to the existing ACL + quota reconciliation (steps 8 + 9, already principal-driven).
  - In the finalizer delete path, extend the `if matches!(&obj.spec.authentication, Authentication::ScramSha512(_)) { …scram delete… }` block to *not* try a SCRAM delete for `TlsExternal` (the explicit shape is correct already since it's gated on `ScramSha512`; just verify TlsExternal doesn't accidentally trip a SCRAM delete). ACL + quota best-effort cleanup is principal-keyed and Just Works.
  - Update `StatusPatch` to carry a new `external: bool` field; thread it through `patch_status` so the rendered JSON includes `"external": p.external || prior_external` (mirror the `scram_sha512` / `tls` sticky pattern).
  - Update every call site of `StatusPatch { … }` in `user.rs` (there are ≈6 sites) to include `external: false` for failure paths and `external: matches!(spec.authentication, Authentication::TlsExternal)` for the success path. Compute `prior_external` at the top of `reconcile` like the other priors.
  - Update `is_tls` / `is_scram` lines at the success-path StatusPatch to also compute `is_external = matches!(&obj.spec.authentication, Authentication::TlsExternal)`. For TlsExternal users, `secret_name` stays `None` (the existing `if p.scram_sha512 || p.tls` already handles that — don't add `|| p.external`).
  - Update the requeue-cadence `match`:
    ```rust
    let requeue = match &obj.spec.authentication {
        Authentication::ScramSha512(_) => Duration::from_mins(1),
        Authentication::Tls(_)         => Duration::from_hours(6),
        Authentication::TlsExternal    => Duration::from_mins(1),
    };
    ```
  - Update in-file unit tests under `mod tests`:
    - `principal_for_tls_external_uses_bare_name` — `principal_for("alice", &Authentication::TlsExternal) == "User:alice"`.
    - `validate_spec_accepts_tls_external_with_acls_and_quotas`
    - `validate_spec_accepts_tls_external_with_no_authorization_no_quotas`
  - Match-exhaustiveness sweep across `user.rs`: every `match … { ScramSha512(_) | Tls(_) }` and `matches!` over the enum needs to handle `TlsExternal`. Audit before touching any other file.

- **T5 — Sample manifest + CRD regen.** Disjoint from T3/T4 (different files):
  - Create `crates/operator/sample/oauth-listener.yaml` — a self-contained `Kafka` + `KafkaNodePool` + `KafkaUser` triplet exercising one `oauth` listener and one `tls-external` user. (Match the directory + filename pattern used by other sample manifests in the repo. If `crates/operator/sample/` doesn't exist yet, create it; otherwise use the existing path. Search with `find crates/operator -name 'sample*' -type d`.)
  - Sample contents (use as a literal starting point — adjust paths for whatever sample dir already exists):
    ```yaml
    apiVersion: crabka.io/v1alpha1
    kind: Kafka
    metadata: { name: demo, namespace: default }
    spec:
      kafkaVersion: "0.1.1"
      listeners:
        - { name: PLAIN, port: 9092, type: internal, tls: false }
        - name: oauth
          port: 9096
          type: internal
          tls: true
          authentication:
            type: oauth
            validIssuerUri: https://keycloak.example/realms/kafka
            jwksEndpointUri: https://keycloak.example/realms/kafka/protocol/openid-connect/certs
            validAudience: kafka-broker
            userNameClaim: preferred_username
            customClaimCheck:
              scope: kafka.write
      interBrokerListenerName: PLAIN
    ---
    apiVersion: crabka.io/v1alpha1
    kind: KafkaNodePool
    metadata: { name: brokers, namespace: default, labels: { crabka.io/cluster: demo } }
    spec:
      roles: [Controller, Broker]
      replicas: 1
      nodeIdStart: 0
      storage: { type: PersistentClaim, size: 1Gi, deleteClaim: true }
    ---
    apiVersion: crabka.io/v1alpha1
    kind: KafkaUser
    metadata: { name: alice, namespace: default, labels: { crabka.io/cluster: demo } }
    spec:
      authentication: { type: tls-external }
      authorization:
        type: simple
        acls:
          - resource: { type: topic, name: e2e }
            operations: [Read, Describe, Write]
    ```
  - Regenerate CRDs:
    ```bash
    cargo run -p crabka-operator --bin crabka-operator-gen-crds -- deploy/crds/
    # OR if the project uses a shell wrapper, prefer it:
    bash tools/regen-crds.sh 2>/dev/null || cargo run --bin crabka-operator-gen-crds -- deploy/crds/
    ```
    Confirm the diff covers `deploy/crds/crabka.io_kafkas.yaml` (listener.authentication.oauth) and `deploy/crds/crabka.io_kafkausers.yaml` (authentication.tls-external + status.external). No other files should change.
  - Commit the regenerated YAML.

### Batch 3 (parallel — disjoint files; depends on Batch 2)

- **T6 — Listener reconcile integration tests.** Create `crates/operator/tests/reconcile_listener_oauth.rs`. Use `crates/operator/tests/reconcile_listener_auth.rs` (the slice-31 file) as the structural template — same shared helpers, same `tower::ServiceExt::mock_service` style kube client. Cases:
  - `oauth_listener_renders_oauthbearer_toml_block`
  - `oauth_listener_appends_oauthbearer_to_sasl_mechanisms`
  - `oauth_listener_with_enable_false_omits_mechanism_but_keeps_config_block`
  - `oauth_listener_without_tls_rejected_with_ListenersValid_false`
  - `oauth_listener_with_http_jwks_uri_reconciles_but_emits_WeakAuth_event`
  - `oauth_listener_with_ftp_jwks_uri_rejected`
  - `oauth_listener_with_empty_issuer_uri_rejected`
  - `oauth_listener_with_jwks_refresh_below_30_rejected`
  - `oauth_listener_custom_claim_check_empty_scope_rejected`
  - `two_oauth_listeners_with_identical_config_reconcile_clean`
  - `two_oauth_listeners_differing_only_in_enable_oauth_bearer_reconcile_clean`
  - `two_oauth_listeners_with_divergent_config_rejected_with_ConflictingOAuthConfig`

- **T7 — KafkaUser tls-external integration tests.** Create `crates/operator/tests/reconcile_user_tls_external.rs`. Use `crates/operator/tests/reconcile_user.rs` as the structural template. Cases (in-cluster kube mock + admin-client FIFO mock from `crates/operator/tests/shared/`):
  - `tls_external_user_creates_no_secret` — assert no `Secret` PATCH is queued; the FIFO admin mock only sees `DescribeAcls` / `CreateAcls` / (optionally) `DescribeClientQuotas` / `AlterClientQuotas`.
  - `tls_external_user_reconciles_acls_under_bare_name_principal` — the `CreateAcls` request payload's `principal` field equals `"User:<metadata.name>"`.
  - `tls_external_user_reconciles_quotas_under_bare_name_principal` — `AlterClientQuotas` request payload uses `<metadata.name>` as the user-quota key.
  - `tls_external_user_status_reports_external_true_and_tls_principal_and_no_secret` — assert the patched status has `external: true`, `tlsPrincipal: User:<name>`, `secret: None`, `scramSha512: false`, `tls: false`.
  - `tls_external_user_with_no_authorization_and_no_quotas_still_reaches_Ready_True`.
  - `tls_external_user_finalizer_does_not_call_alter_user_scram_credentials` — the FIFO admin mock asserts only ACL + quota cleanup calls during finalizer.

### Batch 4 (sequential — touches large shared files)

- **T8 — Keycloak kind e2e.** `.github/workflows/operator-e2e.yml`: add a new top-level job `kind-oauth`, structurally mirroring `kind-listener-auth` (lines 1264–1934). Outer gating on the job:
  ```yaml
  if: ${{ github.event_name == 'push' || contains(github.event.pull_request.labels.*.name, 'e2e-oauth') }}
  needs: build-images
  ```
  Job steps (after the standard `Create kind cluster` + `Load images into kind` + `Install CRDs + chart` block reused from `kind-listener-auth`):

  1. **Install Bitnami Keycloak chart.** Pin to a known-good version at execution time — check `https://artifacthub.io/packages/helm/bitnami/keycloak` for the latest stable tag the day this slice ships, and commit the literal version into the workflow (no floating `latest`). HTTP only — see the slice 50 design's `http://` JWKS note for why HTTPS-to-Keycloak in kind is blocked on 49c:
     ```bash
     helm repo add bitnami https://charts.bitnami.com/bitnami
     helm install kc bitnami/keycloak --namespace keycloak --create-namespace \
       --version <PIN_AT_EXECUTION_TIME> \
       --set auth.adminUser=admin \
       --set auth.adminPassword=admin \
       --set service.type=ClusterIP \
       --set tls.enabled=false \
       --set production=false \
       --set proxy=edge
     kubectl rollout status -n keycloak statefulset/kc-keycloak --timeout=600s
     ```
  2. **Bootstrap realm.** `kubectl exec -n keycloak kc-keycloak-0 -- bash -c '…'` with a `kcadm.sh` script that:
     - Logs in: `/opt/bitnami/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080/ --realm master --user admin --password admin`
     - Creates realm `kafka`: `kcadm.sh create realms -s realm=kafka -s enabled=true`
     - Creates client `kafka-broker` (audience): `kcadm.sh create clients -r kafka -s clientId=kafka-broker -s publicClient=true`
     - Creates client `kafka-client` (confidential, client-credentials):
       ```bash
       kcadm.sh create clients -r kafka \
         -s clientId=kafka-client -s publicClient=false \
         -s serviceAccountsEnabled=true \
         -s 'redirectUris=["*"]'
       # capture client secret
       CID=$(kcadm.sh get clients -r kafka -q clientId=kafka-client --fields id --format csv --noquotes | tail -1)
       SECRET=$(kcadm.sh get clients/$CID/client-secret -r kafka --fields value --format csv --noquotes | tail -1)
       ```
     - Creates client scope `kafka.write` + audience-mapping for `kafka-broker` (so `aud` includes `kafka-broker` and `scope` includes `kafka.write`).
     - Creates user `alice` + sets password `alicepw` + assigns default scope `kafka.write`.
     - Writes `$SECRET` and the alice password to a kubectl-created Secret `keycloak-test-creds` in the `default` namespace so later steps can read them.
  3. **Apply `Kafka` + `KafkaNodePool` + `KafkaUser`.** Use the same shape as `crates/operator/sample/oauth-listener.yaml` (T5), with the URIs pointed at the in-cluster Keycloak Service: `validIssuerUri: http://kc-keycloak.keycloak.svc.cluster.local:8080/realms/kafka` and `jwksEndpointUri: http://kc-keycloak.keycloak.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/certs`. The operator accepts these (per T3's relaxed validation) and emits a `WeakAuth` warning Event — expected and fine in CI. Token endpoint for the producer JAAS config (step 5) is `http://kc-keycloak.keycloak.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token`.
  4. **Wait for `Kafka demo` `Ready=True`.** Same retry loop as `kind-listener-auth`.
  5. **Produce-OK Job.** Use `apache/kafka:3.8.0` image; configure SASL JAAS:
     ```
     security.protocol=SASL_SSL
     sasl.mechanism=OAUTHBEARER
     sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
     sasl.oauthbearer.token.endpoint.url=http://kc-keycloak.keycloak.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token
     sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
       clientId="kafka-client" clientSecret="$SECRET" scope="kafka.write";
     ```
     The Kafka client speaks `SASL_SSL` to the **broker** (the listener still has `tls: true` for broker-client transport TLS, served by the operator-issued cluster CA). The token endpoint URL is a separate HTTP call the client makes to Keycloak, unrelated to broker transport TLS — Keycloak is HTTP-only inside the cluster (see step 1).
     Job script: create topic `e2e` (use `kafka-topics.sh --bootstrap-server demo-kafka-bootstrap:9096`), then produce one record via `kafka-console-producer.sh`. Expect job success.
  6. **Produce-Reject Job (wrong scope).** Same image, but request a token for a scope `kafka.read` not granted by the user. Expect job failure with a `org.apache.kafka.common.errors.SaslAuthenticationException` (mirror the `mtls-producer-nocert` polling pattern in the existing workflow).
  7. **Collect diagnostics on failure** (mirror existing pattern: operator logs, broker logs, Keycloak logs, all job logs, Kafka + KafkaUser yaml).
  8. **Upload diagnostics artifact** `operator-e2e-oauth-diagnostics`.

- **T9 — STATUS + final gate.** `STATUS.md`: add `## Slice 50 — Operator: Listener OAuth + `KafkaUser` tls-external (2026-05-23)` entry following the existing format (deliverables → tests → known follow-ups → reference doc). The "known follow-ups" section should explicitly list 49c/50b/49d/50c/49e/50d/49f/50e/49g/50f as the umbrella's sub-slices.
  Final pass:
  ```
  cargo fmt --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  bash tools/regen-crds.sh 2>/dev/null || cargo run --bin crabka-operator-gen-crds -- deploy/crds/
  git diff --exit-code deploy/crds/
  ```
  Commit + open PR.

## Notes

- Greenfield: no compat shims. Drop `Copy` on `ListenerAuthentication`, update the few call sites that took it by value, move on.
- File-disjoint batches: T1↔T2 and T3↔T4↔T5 and T6↔T7 are pairwise disjoint within their batches. T8 + T9 each touch single large shared files (`operator-e2e.yml`, `STATUS.md`); run sequentially.
- The umbrella spec commits to `tls-external` as **the** OAuth user model forever — no `oauth` `KafkaUser` variant in any future slice. T2 reflects this; T7 tests it.
- The conflict-detection in T3 has an escape valve: when 49h (per-listener `[oauthbearer]`) eventually lands, `ConflictingOAuthListenerConfig` becomes dead code and gets deleted — at that point per-listener `[oauthbearer.<name>]` blocks replace the broker-global one.
- The Keycloak e2e uses HTTP for in-cluster IdP traffic (chart `tls.enabled=false`). Broker ↔ client transport still uses TLS via the operator-issued cluster CA — only the broker → Keycloak JWKS fetch and the client → Keycloak token endpoint call go over HTTP. This is fine for kind CI and intentionally calls into T3's `WeakAuth` Event path (the e2e should assert the `WeakAuth` Event is emitted, mirroring the existing SCRAM-without-TLS Event assertion in `kind-listener-auth`). HTTPS-to-Keycloak is a 49c/50b problem.
