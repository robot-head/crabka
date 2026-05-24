# Slice 50c — Operator: Listener OAuth introspection surface

Status: Draft
Date: 2026-05-24
Slice: 50c
Pairs with broker slice(s): 49d (already shipped — broker introspection validator + `[oauthbearer]` TOML keys)
Umbrella: [OAUTHBEARER full-parity roadmap](2026-05-23-crabka-oauth-parity-roadmap-design.md)

## Goal

Surface 49d's broker-side opaque-token introspection (RFC 7662 + OIDC
userinfo) on the operator's `Kafka.spec.listeners[].authentication`
oauth CRD. Operators choose between JWT-signed and introspection-mode
validation via Strimzi's `accessTokenIsJwt` flag; the operator
validates the source `clientSecret` Secret, mounts it directly into
broker pods, and renders the introspection-mode broker TOML keys.

A second kind-cluster e2e job (`kind-oauth-introspection`) proves the
full operator + broker stack works end-to-end against Keycloak in
introspection mode.

## Deliverables

1. **CRD changes** on `ListenerAuthenticationOAuth` (Strimzi-shape parity):
   - `jwksEndpointUri` becomes `Option<String>` (was required; greenfield breaking change).
   - `accessTokenIsJwt: bool` (default `true`).
   - `introspectionEndpointUri: Option<String>`.
   - `userInfoEndpointUri: Option<String>`.
   - `clientId: Option<String>`.
   - `clientSecret: Option<OauthClientSecretRef { secretName, key }>`.
   - `introspectionHttpTimeoutSeconds: Option<u32>`.
2. **New supporting struct** `OauthClientSecretRef`.
3. **Cross-mode validation**: 4 new failure-mode `Ready=False` reasons.
4. **Cross-listener canonical**: new fields join the canonical tuple automatically via derived `Eq`.
5. **Reconciler**: validate the source Secret + named key exist; thread an `OauthIntrospectionMount` through to the StatefulSet renderer.
6. **Pod template**: direct mount of the user's source Secret with projected `items` mapping (no managed-Secret intermediate — unlike slice 50b's trust-bundle concat).
7. **Broker TOML**: render introspection-mode keys when `accessTokenIsJwt: false`; suppress `jwks_endpoint_uri` in introspection mode.
8. **Sample manifest** updated with an introspection-mode example.
9. **Regenerated CRD YAML** + the existing slice-50/50b sample stays JWT-mode.
10. **New `kind-oauth-introspection` e2e job** alongside the existing `kind-oauth` JWT-mode job. Label-gated `e2e-oauth-introspection`.
11. **STATUS.md entry**.

## Non-deliverables (deferred)

| Item | Status |
|------|--------|
| Per-listener introspection config (different IdPs per listener) | Still rejected by the cross-listener canonical guard; future 49h |
| Operator-managed Keycloak client-credentials provisioning | Operator does NOT create the IdP's `kafka-broker` client; ops bootstrap that out-of-band |
| Operator-managed Secret rename (`{kafka}-oauth-jwks-trust` and `/etc/crabka/oauth-jwks-trust/`) | Internal naming stays — already broadened to "IdP trust" semantically in slice 49d |
| Source-Secret reflector for instant client-secret rotation pickup | Pod-restart-driven rotation only; reflector deferred |
| Cross-namespace Secret refs | Same-namespace only |
| Token caching at the operator level | Not applicable — broker decided no-cache in 49d |
| `client_secret_post` / `private_key_jwt` auth methods | Basic Auth only |
| Outbound mTLS from broker to IdP | Not in any roadmap slice |

## Cross-mode validation (the explicit `accessTokenIsJwt` semantic)

| `accessTokenIsJwt` | Required fields | Forbidden fields |
|---|---|---|
| `true` (default) | `jwksEndpointUri` | `introspectionEndpointUri`, `userInfoEndpointUri`, `clientId`, `clientSecret`, `introspectionHttpTimeoutSeconds` |
| `false` | `introspectionEndpointUri`, `clientId`, `clientSecret` | `jwksEndpointUri`. `userInfoEndpointUri` + `introspectionHttpTimeoutSeconds` are permitted-but-optional. |

Validation fails fast with `Ready=False reason=…` (one of the four new reasons). Reconciler does NOT continue past this point if invalid.

### New `Ready=False` reason strings

- `InvalidListenerOauthAccessTokenIsJwt` — required field missing for the selected mode, OR a forbidden field is set.
- `MissingOauthIntrospectionSecret` — `clientSecret.secretName` doesn't exist in the namespace.
- `MissingOauthIntrospectionKey` — Secret exists; key absent.
- `EmptyOauthIntrospectionValue` — key value is zero bytes.

## CRD shape

```yaml
# Kafka.spec.listeners[].authentication
# JWT mode (existing — 49b/50/50b — unchanged):
type: oauth
accessTokenIsJwt: true              # default; can omit
validIssuerUri: https://idp.example/realms/kafka
jwksEndpointUri: https://idp.example/realms/kafka/protocol/openid-connect/certs
validAudience: kafka-broker
userNameClaim: preferred_username
customClaimCheck: { scope: kafka.write }
tlsTrustedCertificates:
  - secretName: keycloak-ca
    certificate: tls.crt

# Introspection mode (new — 50c):
type: oauth
accessTokenIsJwt: false
validIssuerUri: https://idp.example/realms/kafka     # same
introspectionEndpointUri: https://idp.example/realms/kafka/protocol/openid-connect/token/introspect
userInfoEndpointUri: https://idp.example/realms/kafka/protocol/openid-connect/userinfo  # optional
validAudience: kafka-broker
userNameClaim: preferred_username
customClaimCheck: { scope: kafka.write }
clientId: kafka-broker                # Basic-Auth client_id
clientSecret:                         # Strimzi-shape Secret ref
  secretName: keycloak-introspection-secret
  key: secret
introspectionHttpTimeoutSeconds: 10   # optional
tlsTrustedCertificates:
  - secretName: keycloak-ca
    certificate: tls.crt
```

Rust:

```rust
// crates/operator/src/crd/listener.rs

pub struct ListenerAuthenticationOAuth {
    pub valid_issuer_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,        // was String — now Option

    #[serde(default = "default_true", skip_serializing_if = "is_default_true")]
    pub access_token_is_jwt: bool,                // NEW; default true

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_endpoint_uri: Option<String>,  // NEW

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_info_endpoint_uri: Option<String>,   // NEW

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,                // NEW

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<OauthClientSecretRef>,  // NEW

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_http_timeout_seconds: Option<u32>,  // NEW

    // ...existing fields (valid_audience, user_name_claim,
    //   custom_claim_check, tls_trusted_certificates, enable_oauth_bearer,
    //   jwks_refresh_seconds, max_clock_skew_seconds) unchanged...
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OauthClientSecretRef {
    pub secret_name: String,
    pub key: String,
}
```

The hand-rolled `listener_authentication_schema` is extended with sibling properties for each new field. The `clientSecret` sub-object is modeled as `{type: object, required: ["secretName", "key"], properties: {secretName: {type: string, minLength: 1}, key: {type: string, minLength: 1}}}`.

### Helpers (file-local)

```rust
fn default_true() -> bool { true }
fn is_default_true(b: &bool) -> bool { *b }
```

(Already exist in `listener.rs` from slice 50's `enable_oauth_bearer` plumbing — reuse.)

## Reconciler pipeline

In `controller/kafka.rs::reconcile_kafka`, after the existing listener
validation step and AFTER the existing slice-50b
`reconcile_oauth_jwks_trust` call (the two can run in either order
since they touch disjoint Secret namespaces):

```rust
// Slice 50c: validate the OAUTHBEARER introspection client-secret
// Secret exists + has the named key. Returns Some(mount) when
// introspection is configured + source validates; None otherwise
// (JWT mode or no oauth listener).
let oauth_introspection_mount = match reconcile_oauth_introspection_secret(
    &secret_api,
    &obj,
    oauth_canonical.as_ref(),
).await {
    Ok(mount) => mount,
    Err(e) => {
        patch_status_with_condition(/* Ready=False, reason=e.reason(), msg */).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
};
```

`OauthIntrospectionMount` carries the source Secret's `name` + `key` (the same data carried on the CRD's `clientSecret`):

```rust
#[derive(Debug, Clone)]
pub(crate) struct OauthIntrospectionMount {
    pub secret_name: String,
    pub key: String,
}
```

A `pub(crate) fn oauth_introspection_secret_mount(kafka: &Kafka) -> Option<OauthIntrospectionMount>` helper derives the same value deterministically from the parent Kafka CR's listeners (mirrors slice 50b's `oauth_jwks_trust_secret_name` pattern). `kafka_node_pool.rs::reconcile` calls this helper to know what to mount; `controller/kafka.rs::reconcile_kafka` calls the validating helper (`reconcile_oauth_introspection_secret`) to make sure the source Secret exists at reconcile time.

The validating helper:

```rust
async fn reconcile_oauth_introspection_secret(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    canonical: Option<&ListenerAuthenticationOAuth>,
) -> Result<Option<OauthIntrospectionMount>, ReconcileError> {
    let Some(c) = canonical else { return Ok(None); };
    if c.access_token_is_jwt { return Ok(None); }
    let Some(client_secret) = c.client_secret.as_ref() else {
        // Should have been caught by per-listener validation; defensive.
        return Err(ReconcileError::InvalidListenerOauthAccessTokenIsJwt(
            "introspection mode requires clientSecret".into(),
        ));
    };
    let src = secret_api.get_opt(&client_secret.secret_name).await?
        .ok_or_else(|| ReconcileError::MissingOauthIntrospectionSecret(
            client_secret.secret_name.clone()
        ))?;
    let val = src.data.as_ref().and_then(|d| d.get(&client_secret.key))
        .ok_or_else(|| ReconcileError::MissingOauthIntrospectionKey {
            secret: client_secret.secret_name.clone(),
            key: client_secret.key.clone(),
        })?;
    if val.0.is_empty() {
        return Err(ReconcileError::EmptyOauthIntrospectionValue {
            secret: client_secret.secret_name.clone(),
            key: client_secret.key.clone(),
        });
    }
    Ok(Some(OauthIntrospectionMount {
        secret_name: client_secret.secret_name.clone(),
        key: client_secret.key.clone(),
    }))
}
```

NOTE: no managed-Secret upsert — `reconcile_oauth_introspection_secret` is validation-only. The source Secret is mounted DIRECTLY into the broker pod via `kafka_node_pool.rs::render_storage` (T3).

## Pod template

In `controller/kafka_node_pool.rs`:

- `render_storage(..., oauth_introspection_mount: Option<&OauthIntrospectionMount>)` — new parameter (positioned after the existing `oauth_jwks_trust_secret` arg).
- When `Some(mount)`, appends to volumes:
  ```json
  {
    "name": "oauth-introspection-secret",
    "secret": {
      "secretName": mount.secret_name,
      "items": [{ "key": mount.key, "path": "client-secret" }],
      "defaultMode": 0o400
    }
  }
  ```

- `render_broker_container(..., oauth_introspection_mount: Option<&str>)` — new parameter (the mount path). When `Some("/etc/crabka/oauth-introspection")`, appends to volumeMounts:
  ```json
  { "name": "oauth-introspection-secret",
    "mountPath": "/etc/crabka/oauth-introspection",
    "readOnly": true }
  ```

The projected `items` mapping is the key piece: the user's source Secret can have any key name, but inside the pod the file is always `/etc/crabka/oauth-introspection/client-secret`. The broker reads from that fixed path regardless of the user's source-key naming.

## Broker TOML rendering

In `controller/listeners.rs::render_broker_toml`, the existing
`[oauthbearer]` block emission is extended:

- When canonical OAuth config has `access_token_is_jwt: true` (existing behavior): emit `jwks_endpoint_uri = …` as today (slice 49b).
- When `access_token_is_jwt: false`: do NOT emit `jwks_endpoint_uri`. Instead emit (in this exact order, matching 49d's `FileOAuthBearerConfig` field order):
  ```toml
  introspection_endpoint_uri = "<introspectionEndpointUri>"
  userinfo_endpoint_uri = "<userInfoEndpointUri>"               # only if Some
  introspection_client_id = "<clientId>"
  introspection_client_secret_path = "/etc/crabka/oauth-introspection/client-secret"
  introspection_http_timeout_ms = <introspectionHttpTimeoutSeconds * 1000>   # only if Some
  ```

  Existing keys (`valid_issuer_uri`, `expected_audience`, `principal_claim_name`, `scope_claim_name`, `required_scope`, `allowable_clock_skew_ms`, `idp_tls_trust`) are emitted unchanged in both modes.

## File-level change map

| File | Change |
|------|--------|
| `crates/operator/src/crd/listener.rs` | Make `jwks_endpoint_uri` `Option<String>`; 6 new fields on `ListenerAuthenticationOAuth`; new `OauthClientSecretRef` struct; extend hand-rolled schema; ~6 new round-trip tests + extend schema-regression test |
| `crates/operator/src/controller/listeners.rs` | Per-listener cross-mode validation (4 new failure-mode reasons); TOML render fork by `access_token_is_jwt`; per-canonical-field divergence walk extended with 4 new perturbations |
| `crates/operator/src/controller/kafka.rs` | New `reconcile_oauth_introspection_secret` async helper (validation-only, no managed Secret upsert); new `OauthIntrospectionMount` pub(crate) struct; new `oauth_introspection_secret_mount` pub(crate) helper for the pool reconciler; call-site insertion in `reconcile_kafka` |
| `crates/operator/src/controller/kafka_node_pool.rs` | `Option<&OauthIntrospectionMount>` param on `render_storage`; `Option<&str>` mount-path param on `render_broker_container`; pool reconciler derives the mount via the new helper |
| `crates/operator/src/controller/common.rs` | 4 new `ReconcileError` variants for the failure modes |
| `crates/operator/sample/oauth-listener.yaml` | Add a second oauth listener block (introspection mode) so users can copy from either example |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated |
| `crates/operator/tests/reconcile_listener_oauth.rs` | Extend canonical-divergence walk + add the "two listeners with divergent access_token_is_jwt rejected" test |
| `crates/operator/tests/reconcile_oauth_introspection.rs` (new) | Reconcile-level integration: Secret-validation paths + pod-mount assertions (9 tests) |
| `.github/workflows/operator-e2e.yml` | New `kind-oauth-introspection` job, label-gated `e2e-oauth-introspection` + `push: main`. Realm bootstrap captures the `kafka-broker` confidential-client secret into `keycloak-introspection-secret` (default ns). Kafka CR uses introspection mode |
| `STATUS.md` | New `## Slice 50c` entry |

## Test plan

### Unit tests in `crd/listener.rs`

- `oauth_with_access_token_is_jwt_false_introspection_round_trips`
- `oauth_access_token_is_jwt_default_omitted_on_serialize`
- `oauth_client_secret_round_trips`
- `oauth_jwks_endpoint_uri_now_optional_omits_when_none`
- `oauth_with_userinfo_endpoint_round_trips`
- `oauth_schema_contains_introspection_sibling_keys`

### Validation tests in `controller/listeners.rs`

- `validate_listeners_rejects_oauth_jwt_mode_without_jwks_endpoint_uri`
- `validate_listeners_rejects_oauth_introspection_mode_without_endpoint_uri`
- `validate_listeners_rejects_oauth_introspection_mode_without_client_id`
- `validate_listeners_rejects_oauth_introspection_mode_without_client_secret`
- `validate_listeners_rejects_oauth_jwt_mode_with_introspection_fields`
- `validate_listeners_rejects_oauth_introspection_mode_with_jwks_endpoint_uri`
- `validate_listeners_rejects_oauth_userinfo_endpoint_without_introspection_mode`
- Extend existing `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` perturbations vec: 4 new entries (`access_token_is_jwt`, `introspection_endpoint_uri`, `user_info_endpoint_uri`, `client_secret`).

### TOML render tests in `controller/listeners.rs`

- `render_broker_toml_emits_introspection_keys_when_introspection_mode`
- `render_broker_toml_omits_jwks_endpoint_uri_in_introspection_mode`
- `render_broker_toml_emits_userinfo_endpoint_when_set`
- `render_broker_toml_emits_introspection_http_timeout_ms_when_set`
- `render_broker_toml_oauthbearer_block_emits_introspection_keys_in_canonical_order` — pin the exact byte sequence (slice-21 hash invariant). Order: `introspection_endpoint_uri`, `userinfo_endpoint_uri`, `introspection_client_id`, `introspection_client_secret_path`, `introspection_http_timeout_ms`.

### Integration tests in `tests/reconcile_oauth_introspection.rs` (new)

- `oauth_introspection_validates_source_secret_and_mounts_it`
- `oauth_introspection_missing_source_secret_rejects_with_missing_oauth_introspection_secret`
- `oauth_introspection_missing_key_in_secret_rejects_with_missing_oauth_introspection_key`
- `oauth_introspection_empty_key_value_rejects_with_empty_oauth_introspection_value`
- `oauth_introspection_jwt_mode_does_not_mount_anything`
- `oauth_introspection_managed_pod_template_mounts_secret_with_projected_items`
- `oauth_introspection_with_userinfo_renders_userinfo_endpoint_in_toml`
- `statefulset_mounts_oauth_introspection_secret_when_introspection_mode`
- `statefulset_omits_oauth_introspection_volume_when_jwt_mode`

### Extension in `tests/reconcile_listener_oauth.rs`

- `two_oauth_listeners_with_divergent_access_token_is_jwt_rejected_with_conflicting_oauth_config`

### Kind e2e — `kind-oauth-introspection`

Clones the existing `kind-oauth` job (slice 50b's Keycloak HTTPS setup). Changes:

1. **Realm bootstrap** adds:
   - Create a new confidential client `kafka-broker` (the client the BROKER uses to call introspection; distinct from `kafka-client` which is the client the PRODUCER uses to obtain tokens).
   - Enable that client's "Service Accounts" so it can issue introspection requests.
   - Capture its client secret via `kcadm.sh get clients/$ID/client-secret`.
   - Create a kube Secret `keycloak-introspection-secret` in `default` ns with the value under key `secret`.

2. **Kafka CR YAML**:
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

3. **Producer Jobs**: unchanged from `kind-oauth` (token endpoint URL is the same; JVM producer doesn't care how the broker validates).

4. **WeakAuth assertion**: stays inverted (no HTTP URLs anywhere).

5. **Gating**: `if: github.event_name == 'push' || contains(github.event.pull_request.labels.*.name, 'e2e-oauth-introspection')`. Adds ~5 min CI on `push: main` + opt-in PRs.

## Acceptance criteria

1. `cargo build -p crabka-operator` clean.
2. `cargo test --workspace` passes (new + existing tests).
3. `cargo fmt --check` + `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. CRD-drift gate clean.
5. Sample manifest applies cleanly against the regenerated CRD.
6. `kind-oauth-introspection` e2e: structurally valid, label-gated, mirrors the existing `kind-oauth` pattern.
7. STATUS.md updated.

## Open questions resolved during brainstorming

- **`accessTokenIsJwt` explicit vs inferred.** Explicit (Strimzi parity) — operator validates the mode-required-fields combination at reconcile time.
- **`clientSecret` shape.** Strimzi `{secretName, key}` Secret ref.
- **Client-secret mount strategy.** Direct mount of the user's source Secret via projected `items` (no managed-Secret intermediate — unlike slice 50b's trust-bundle which concatenates multiple PEMs). One Secret, one key, one file: `/etc/crabka/oauth-introspection/client-secret`.
- **`jwksEndpointUri` required vs optional.** Optional. Greenfield breaking change permitted by CLAUDE.md.
- **Per-listener config divergence.** Still rejected by the cross-listener canonical guard. The 4 new fields join the canonical tuple automatically via derived `Eq`.
- **E2E variant scope.** New `kind-oauth-introspection` job alongside the existing `kind-oauth` (JWT-mode) job. Both Keycloak-backed, both label-gated.
