# Slice 50 — Operator: Listener OAuth + `KafkaUser` tls-external

Status: Draft
Date: 2026-05-23
Slice: 50
Pairs with broker slice(s): 49b (already shipped)
Umbrella: [OAUTHBEARER full-parity roadmap](2026-05-23-crabka-oauth-parity-roadmap-design.md)

## Goal

Land the operator-side surface for OAUTHBEARER on top of the broker work
already in slice 49b. After this slice, a user can declare a `Kafka` CR
with an OAuth listener pointed at any IdP that publishes a JWKS endpoint
and signs JWTs with RS256 or ES256; and a `KafkaUser` CR with
`type: tls-external` to bind ACLs / quotas to an OAuth principal whose
JWT `userNameClaim` equals `metadata.name`.

This is the first operator slice of the OAUTHBEARER parity umbrella. It
covers **only the field surface that 49b's validator already honors** —
nothing half-finished, nothing dead. Sub-slices 50b–50f extend the CRD as
their broker counterparts (49c–49g) land.

## Deliverables

1. **`KafkaListenerAuthenticationOAuth` CRD variant** on
   `Kafka.spec.listeners[].authentication`.
2. **`tls-external` `KafkaUser` authentication variant** that provisions
   ACLs and quotas with no Secret / cert generation.
3. **Listener reconciler** renders the broker TOML `[oauthbearer]` block
   and appends `OAUTHBEARER` to the listener's `sasl_mechanisms` list.
4. **`KafkaUser` reconciler** new code path for `tls-external` users
   skipping credential provisioning.
5. **Sample manifest** `crates/operator/sample/oauth-listener.yaml`.
6. **Unit + integration tests** covering CRD round-trips, reconciler
   intent (TOML rendering, conflict detection, validation rejections,
   no-Secret-created assertion for tls-external users).
7. **Kind-cluster end-to-end** using Bitnami Keycloak chart: realm
   bootstrap, OAuth listener config, a Job that mints a token via
   Keycloak's token endpoint and produces / consumes against the broker.
   Opt-in via PR label `e2e-oauth`; runs on every push to `main`.
8. **CRD YAML regeneration** for both `Kafka` and `KafkaUser` CRDs.
9. **STATUS.md entry** under `## Slice 50`.

## Non-deliverables (deferred to umbrella sub-slices)

| Field / behavior | Lands in |
|------------------|----------|
| `tlsTrustedCertificates` (custom CA bundle for IdP HTTPS) | 50b (paired with 49c) |
| `introspectionEndpointUri`, `userInfoEndpointUri`, `clientId`, `clientSecret`, `accessTokenIsJwt`, `checkAccessTokenType` | 50c (paired with 49d) |
| `maxSecondsWithoutReauthentication` (KIP-368) | 50d (paired with 49e) |
| `enablePlain`, `tokenEndpointUri` (PLAIN-with-OAuth-token) | 50e (paired with 49f) |
| `groupsClaim`, `groupsClaimDelimiter`, `fallbackUserNameClaim`, `fallbackUserNamePrefix`, `validTokenType`, full-shape `customClaimCheck`, `jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`, `jwksIgnoreKeyUse` | 50f (paired with 49g) |
| Two OAuth listeners with divergent config on one broker | Future broker slice (likely 49h); slice 50 rejects this at reconcile |
| `KafkaUser.authentication: oauth` variant | Never — the umbrella commits to `tls-external` as the OAuth user model |
| Inter-broker OAuth (`serverBearerTokenLocation`) | Broker runtime shipped later; operator CRD/rendering is outside slice 50 |

## CRD shape

### `Kafka.spec.listeners[].authentication: oauth`

```yaml
authentication:
  type: oauth
  validIssuerUri: https://keycloak.example/realms/kafka
  jwksEndpointUri: https://keycloak.example/realms/kafka/protocol/openid-connect/certs
  validAudience: kafka-cluster                  # optional
  userNameClaim: preferred_username             # optional; default "sub"
  customClaimCheck:                             # optional
    scope: kafka.write                          # required when customClaimCheck present
    scopeClaim: scope                           # optional; default "scope"
  jwksRefreshSeconds: 300                       # optional; default 300
  maxClockSkewSeconds: 60                       # optional; default 60
  enableOauthBearer: true                       # optional; default true
```

Rust:

```rust
// crates/operator/src/crd/listener.rs

pub enum ListenerAuthentication {
    Tls,
    ScramSha512,
    ScramSha256,
    OAuth(ListenerAuthenticationOAuth),
}

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

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OAuthCustomClaimCheck {
    /// Required scope value. The token's `scopeClaim` (default `scope`)
    /// must contain this value. Strimzi's `customClaimCheck` supports a
    /// fuller JsonPath-ish expression language; slice 50 narrows it to
    /// "scope contains X" because that's what 49b's validator honors.
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_claim: Option<String>,
}
```

The enum's `#[schemars(schema_with = …)]` must continue to use the
hand-rolled structural schema (kube-rs 3.x `StructuralSchemaRewriter`
panics on default `oneOf` shapes that share a `type` discriminator with
differing `enum` values — same workaround already used for the SCRAM /
TLS variants). Extend the schema to include `oauth` in the discriminator
`enum` and add `validIssuerUri`, `jwksEndpointUri`, `validAudience`,
`userNameClaim`, `customClaimCheck`, `jwksRefreshSeconds`,
`maxClockSkewSeconds`, `enableOauthBearer` as siblings of `type`.

### `KafkaUser.spec.authentication: tls-external`

```yaml
authentication:
  type: tls-external
authorization:
  type: simple
  acls:
    - resource: { type: topic, name: orders }
      operations: [Read, Describe]
quotas:
  producerByteRate: 1048576
```

Rust:

```rust
// crates/operator/src/crd/user.rs

pub enum Authentication {
    ScramSha512(ScramSha512Auth),
    Tls(TlsAuth),
    TlsExternal,
}
```

The hand-rolled `authentication_schema` adds `tls-external` to the
discriminator `enum`. No new sibling properties.

## Reconciler behavior

### Listener reconciler (`crates/operator/src/controller/listeners.rs`)

**Validation** (pre-render, per listener):

| Check | Failure → status condition |
|-------|----------------------------|
| `oauth` only on `tls: true` listeners | `Ready=False reason=InvalidListenerAuth message="OAuth requires tls: true"` |
| `validIssuerUri` is non-empty (any scheme; the broker matches it as a literal string against the JWT `iss` claim) | `Ready=False reason=InvalidListenerAuth message="validIssuerUri is required"` |
| `jwksEndpointUri` parses as `http://…` or `https://…` (the broker uses plain reqwest, which accepts either; 49b is webpki-only for HTTPS) | `Ready=False reason=InvalidListenerAuth message="jwksEndpointUri must be http or https"` |
| `jwksRefreshSeconds >= 30` if set | `Ready=False reason=InvalidListenerAuth message="jwksRefreshSeconds must be >= 30"` |
| `customClaimCheck.scope` non-empty if `customClaimCheck` is present | `Ready=False reason=InvalidListenerAuth message="customClaimCheck.scope is required"` |

**`http://` JWKS endpoint warning.** Like SCRAM-without-TLS, an
`http://` `jwksEndpointUri` is accepted but emits a `WeakAuth`
Kubernetes Event on the `Kafka` resource: "listener `<name>` has
http:// JWKS endpoint; key material traverses the network in cleartext.
Consider https." Mirrors the existing `weak_auth_warnings` flow for
SCRAM in `crates/operator/src/controller/listeners.rs`. Slice 49c will
add custom TLS trust to the IdP; until then, in-cluster HTTPS-to-IdP
requires the IdP to use a webpki-trusted cert (which Keycloak in kind
does not by default — hence the slice 50 Keycloak e2e uses HTTP).

**Cross-listener validation** (post per-listener validation):

| Check | Failure → status condition |
|-------|----------------------------|
| At most one distinct `(validIssuerUri, jwksEndpointUri, validAudience, userNameClaim, customClaimCheck, jwksRefreshSeconds, maxClockSkewSeconds)` tuple across all OAuth listeners | `Ready=False reason=ConflictingOAuthConfig message="all OAuth listeners must share identical config (per-listener OAuth is a future broker slice)"` |

`enableOauthBearer: false` is **not** part of the conflict tuple — toggling
it only affects whether `OAUTHBEARER` is added to the listener's
`sasl_mechanisms` list, not the broker-global `[oauthbearer]` config
section.

**TOML rendering** (broker TOML written into the `ConfigMap`):

For each OAuth listener with `enable_oauth_bearer == true`, append
`OAUTHBEARER` to its `sasl_mechanisms` list (the existing per-listener
mechanism resolution already exists for SCRAM — extend it).

If **any** OAuth listener exists across the cluster (regardless of
`enableOauthBearer`), emit one broker-global `[oauthbearer]` block
populated from the (canonical, single) OAuth config:

```toml
[oauthbearer]
jwks_endpoint_uri        = "https://…/certs"           # always
valid_issuer_uri         = "https://…/realms/kafka"    # always
expected_audience        = "kafka-cluster"             # omit if validAudience absent
principal_claim_name     = "preferred_username"        # omit if userNameClaim absent (broker default `sub`)
scope_claim_name         = "scope"                     # omit if customClaimCheck.scopeClaim absent (broker default `scope`)
required_scope           = "kafka.write"               # omit if customClaimCheck absent
jwks_refresh_interval_ms = 300000                      # omit if jwksRefreshSeconds absent (broker default 300000)
allowable_clock_skew_ms  = 60000                       # omit if maxClockSkewSeconds absent (broker default 30000)
```

Field names mirror `crates/broker/src/file_config.rs`'s
`FileOAuthBearerConfig` keys verbatim (verified against the 49b
implementation: `jwks_endpoint_uri`, `valid_issuer_uri`,
`expected_audience`, `principal_claim_name`, `scope_claim_name`,
`required_scope`, `jwks_refresh_interval_ms`, `allowable_clock_skew_ms`).
No new broker TOML keys are introduced — slice 50 is purely operator
surface.

The operator emits a key **only when the CRD sets the corresponding
field**, so a minimal `oauth` CRD that sets just `validIssuerUri` and
`jwksEndpointUri` produces a two-key `[oauthbearer]` block, and the
broker fills the rest from its own defaults. This keeps the operator's
defaults and the broker's defaults from drifting.

**Status** (per listener):
- Existing `ListenerStatus.bootstrap_servers` + `addresses` unchanged.
- No OAuth-specific status fields on `ListenerStatus` in slice 50 — when
  50c lands and the operator manages an introspection client Secret,
  surface its status then.

### `KafkaUser` reconciler (`crates/operator/src/controller/user.rs`)

For `Authentication::TlsExternal`:
- Skip Secret generation (the existing SCRAM/TLS code paths don't run).
- Skip cert issuance.
- Reconcile ACLs from `spec.authorization` against the broker, binding
  to principal `User:<metadata.name>`. Reuse the existing ACL diff /
  apply code with the new principal source.
- Reconcile quotas from `spec.quotas` against the broker's
  `(user)` quota entity for `User:<metadata.name>`. Reuse the existing
  quota diff / apply code.

**Status** (`KafkaUserStatus`):
- `scram_sha512 = false`.
- `tls = false`.
- `secret = None` (no Secret created).
- `tls_cert_not_after = None`.
- `tls_principal = Some("User:<metadata.name>")` — the principal under
  which ACLs were provisioned. Surfaces in `kubectl describe ku` so
  operators can debug "why isn't my ACL matching".
- `quotas_in_sync = true` once quotas reconciled (or `true` immediately
  if `spec.quotas` is absent — same as today's SCRAM users).
- New bool field `external: bool` (default `false`) — `true` once a
  `tls-external` user has been successfully reconciled. Makes it obvious
  in `kubectl describe ku` that this user has no credentials managed by
  the operator. Add this to `KafkaUserStatus`.

## File-level change map

| File | Change |
|------|--------|
| `crates/operator/src/crd/listener.rs` | New `ListenerAuthenticationOAuth` struct, new `OAuthCustomClaimCheck` struct, new `OAuth` enum variant, extend `listener_authentication_schema` |
| `crates/operator/src/crd/user.rs` | New `TlsExternal` enum variant, extend `authentication_schema`, add `external: bool` to `KafkaUserStatus` |
| `crates/operator/src/controller/listeners.rs` | Per-listener OAuth validation, cross-listener conflict detection, TOML rendering for `[oauthbearer]`, append `OAUTHBEARER` to per-listener `sasl_mechanisms` |
| `crates/operator/src/controller/user.rs` | New code path for `TlsExternal`: skip credential provisioning, run ACL + quota reconciliation, populate status with `external: true` + `tls_principal: User:<name>` |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated |
| `deploy/crds/crabka.io_kafkausers.yaml` | Regenerated |
| `crates/operator/sample/oauth-listener.yaml` | New sample manifest |
| `crates/operator/tests/reconcile_listener_oauth.rs` | New integration test file |
| `crates/operator/tests/reconcile_user_tls_external.rs` | New integration test file |
| `tools/e2e-kind/oauth_keycloak.sh` | New kind e2e script |
| `.github/workflows/ci.yml` | New `oauth-e2e` job, label-gated `e2e-oauth` + main push |
| `STATUS.md` | New `## Slice 50` entry |

The listener half (`listener.rs` + `listeners.rs`) and the `KafkaUser`
half (`user.rs` CRD + `user.rs` controller) touch disjoint file sets and
can be implemented as parallel subagent batches with no conflicts. The
e2e script + CI workflow form a third independent batch.

## Test plan

### Unit tests (in-place additions to existing files)

- `crd/listener.rs`:
  - `oauth_authentication_round_trips` — full-config round-trip through serde
  - `oauth_authentication_minimum_round_trips` — only required fields
  - `oauth_with_custom_claim_check_round_trips`
  - `oauth_default_enable_is_omitted_on_serialize`
  - `oauth_enable_false_round_trips`
- `crd/user.rs`:
  - `tls_external_round_trips`
  - `tls_external_with_quotas_and_acls_round_trips`
  - `tls_external_status_external_field_emitted`

### Integration tests (new files under `crates/operator/tests/`)

- `reconcile_listener_oauth.rs`:
  - `oauth_listener_renders_oauthbearer_toml_block`
  - `oauth_listener_appends_oauthbearer_to_sasl_mechanisms`
  - `oauth_listener_with_enable_false_omits_mechanism_but_keeps_config_block`
  - `oauth_listener_without_tls_rejected`
  - `oauth_listener_with_http_jwks_uri_rejected`
  - `oauth_listener_with_short_jwks_refresh_rejected`
  - `two_oauth_listeners_with_identical_config_accepted`
  - `two_oauth_listeners_with_divergent_config_rejected_with_conflict_condition`
  - `oauth_listener_custom_claim_check_with_empty_scope_rejected`
- `reconcile_user_tls_external.rs`:
  - `tls_external_user_creates_no_secret`
  - `tls_external_user_reconciles_acls_under_bare_name_principal`
  - `tls_external_user_reconciles_quotas_under_bare_name_principal`
  - `tls_external_user_status_reports_external_true_and_tls_principal`
  - `tls_external_user_with_no_authorization_and_no_quotas_still_reaches_ready`

### Kind end-to-end (`tools/e2e-kind/oauth_keycloak.sh`)

1. Spin up kind cluster (reuse existing setup script).
2. Install Bitnami Keycloak chart, pinned version (we pick the version
   at plan time — needs to be a known-good single-pod dev install).
3. Wait for Keycloak `Ready`.
4. `kubectl exec` into the Keycloak pod and run `kcadm.sh` to:
   - Create realm `kafka`.
   - Create client `kafka-broker` (public, audience for `validAudience`).
   - Create client `kafka-client` (confidential, client-credentials grant,
     scope `kafka.write`).
   - Create user `alice` (password grant, default scope `kafka.write`).
5. Apply `Kafka` CR with one OAuth listener pointed at Keycloak.
6. Apply `KafkaUser` `alice` with `authentication: {type: tls-external}`
   and ACL allowing `Read` + `Describe` + `Write` on topic `e2e`.
7. Apply `KafkaTopic` `e2e`.
8. Run a Job using Apache Kafka JVM `kafka-console-producer.sh` /
   `kafka-console-consumer.sh` image, configured with OAUTHBEARER and a
   token grabbed from Keycloak via `curl`. Produce one record, consume
   one record. Pass.
9. Run a second Job with a token from a client that lacks the
   `kafka.write` scope. Assert authentication fails.
10. Tear down.

### CI gating

Add a new job `oauth-e2e` to `.github/workflows/ci.yml`:
- Runs on push to `main`.
- Runs on PRs labeled `e2e-oauth`.
- Skipped on every other PR (Keycloak boot + realm bootstrap adds ~3-4
  minutes; not worth burning on every commit).

## Acceptance criteria

1. `cargo build -p crabka-operator` and `cargo test -p crabka-operator`
   pass.
2. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D
   warnings` pass.
3. CRD-drift gate (`cargo xtask gen-crds && git diff --exit-code`)
   passes.
4. New unit tests above all pass.
5. New integration tests above all pass.
6. Kind e2e `oauth-e2e` job passes on `main` push and on
   PRs labeled `e2e-oauth`: realm bootstrap succeeds, listener
   reconciles to `Ready=True`, producer with scoped token succeeds,
   producer with unscoped token fails with auth error.
7. Sample manifest `crates/operator/sample/oauth-listener.yaml` applies
   cleanly against a kind cluster with operator installed.
8. STATUS.md updated with a `## Slice 50` entry following the existing
   format.

## Open questions resolved during brainstorming

- **Why `tls-external` instead of an `oauth` `KafkaUser` variant?** Matches
  Strimzi's actual enum (which has no `oauth` variant); one variant covers
  both OAuth users and future external-CA mTLS users; future-proofs against
  a proliferation of credential-less user variants. The umbrella roadmap
  commits to this as the final OAuth user model — no `oauth` variant in
  any future slice.
- **What about per-listener `[oauthbearer]` config?** Slice 49b shipped
  `[oauthbearer]` as broker-global. Slice 50 accepts this and rejects
  two-listener-divergent-config at reconcile time with a clear status
  condition. A future broker slice (49h or similar) can lift this when
  there's a real user request — at that point slice 50's conflict-detection
  code goes away.
- **Custom-claim-check shape.** Strimzi exposes a JsonPath-ish expression
  language; 49b's validator only honors "scope-claim contains required
  scope". Slice 50 narrows the CRD field to that shape and leaves the
  full expression language to 50f (paired with 49g).

## Out of scope

- All Strimzi fields in the non-deliverables table above.
- Cross-realm token federation.
- Token caching / introspection caching policy.
- OAuth metrics beyond what 49b's auth handler already emits.
- Migration tooling from a Strimzi-managed OAuth listener.
- `KafkaUser.authentication: oauth` variant (the umbrella explicitly
  commits to `tls-external` as the OAuth user model forever).
