# Schema Registry Slice 8: Security Completeness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add JWKS-backed Bearer auth (Part A), secured SR→broker operator rendering (Part B), and cert-manager Certificate CR creation (Part C) to the Schema Registry crate and operator.

**Architecture:** SR-crate changes (Tasks 1–3) are fully independent of operator changes (Tasks 4–6). Within the operator, Tasks 4–5–6 touch the same files so they run sequentially in separate batches. Batch 1 runs Tasks 1 and 4 in parallel; Batch 2 runs Tasks 2 and 5 in parallel; Batch 3 runs Tasks 3 and 6 in parallel.

**Tech Stack:** `crabka-security` (`JwksHandle`, `SignedJwsValidator`, `Jwks`), `reqwest` (JWKS HTTP fetch, already a dep), `ring` (JWT signing in integration tests, added as dev-dep), `kube::api::DynamicObject` + `kube::core::{ApiResource, GroupVersionKind}` (cert-manager Certificate CRs, same pattern as `metrics.rs`), `clap` (new CLI flags in the binary).

---

## File Map

| File | What changes |
|---|---|
| `crates/schema-registry/src/cli.rs` | New types `JwksHandleForRefresh` / `SecurityOutput`; JWKS fields on `SecurityCliInput`; `build_bearer` extended; `build_security` returns `SecurityOutput` |
| `crates/schema-registry/src/bin/schema-registry.rs` | New `--bearer-jwks-*` clap flags; `security_input()` maps them; `main()` destructures `SecurityOutput`; `run_jwks_refresher` task |
| `crates/schema-registry/tests/security.rs` | JWKS integration tests: signed-token → 200, unsigned → 401, wrong-issuer → 401 |
| `crates/schema-registry/Cargo.toml` | Add `ring = { workspace = true }` to `[dev-dependencies]` |
| `crates/operator/src/crd/schema_registry.rs` | `BearerMode::Jwks`; JWKS fields on `BearerAuthn`; new `SchemaRegistryKafkaClient` / `KafkaClientSasl` / `KafkaClientTls`; `kafka_client` on spec; `SchemaRegistryTls.secret_name: String → Option<String>`; new `CertManagerIssuerRef`; `issuer_ref` on tls |
| `deploy/crds/crabka.io_schemaregistries.yaml` | Regenerated — never hand-edit |
| `crates/operator/src/controller/schema_registry.rs` | `build_args_and_mounts` returns 4-tuple (adds extra_env); kafka_client rendering; JWKS bearer rendering; `apply_certificate_cr`; reconciler `issuer_ref` flow; `build_args_and_mounts` accepts `tls_secret: Option<&str>` |
| `crates/operator/tests/reconcile_schema_registry.rs` | Fix `sr()` helper; fix `full_security_fields_render_to_args_and_mounts`; add kafka_client, JWKS bearer, cert-manager tests |
| `charts/crabka-operator/templates/clusterrole.yaml` | Add `cert-manager.io/certificates` RBAC rule |

---

## ═══ BATCH 1 — parallel: Tasks 1 and 4 ═══

### Task 1: `cli.rs` — JWKS types + bearer

**Files:**
- Modify: `crates/schema-registry/src/cli.rs`

- [ ] **Step 1: Write the failing unit tests first**

Add these test functions inside the existing `#[cfg(test)] mod tests` block at the bottom of `crates/schema-registry/src/cli.rs`:

```rust
#[test]
fn bearer_jwks_off_variant_returns_none_handle() {
    let s = sec(&input());
    assert!(s.bearer.is_none());
    // no jwks_handle emitted when bearer=off (the default)
}

#[test]
fn bearer_jwks_builds_signed_validator() {
    let i = SecurityCliInput {
        bearer: "jwks".into(),
        jwks_endpoint_uri: Some("https://idp.example.com/.well-known/jwks.json".into()),
        ..input()
    };
    let out = build_security(&i).unwrap();
    assert!(out.bearer.is_some());
    assert!(out.jwks_handle.is_some());
    let h = out.jwks_handle.unwrap();
    assert_eq!(h.endpoint_uri, "https://idp.example.com/.well-known/jwks.json");
    assert_eq!(h.refresh_ms, 60_000); // default
}

#[test]
fn bearer_jwks_missing_endpoint_errors() {
    let i = SecurityCliInput {
        bearer: "jwks".into(),
        jwks_endpoint_uri: None, // required for jwks mode
        ..input()
    };
    let err = build_security(&i).unwrap_err().to_string();
    assert!(err.contains("--bearer-jwks-endpoint-uri"), "got: {err}");
}

#[test]
fn bearer_jwks_sets_issuer_and_audience() {
    let i = SecurityCliInput {
        bearer: "jwks".into(),
        jwks_endpoint_uri: Some("https://idp/jwks".into()),
        jwks_valid_issuer: Some("https://idp".into()),
        jwks_expected_audience: Some("kafka-sr".into()),
        jwks_principal_claim: Some("email".into()),
        ..input()
    };
    let out = build_security(&i).unwrap();
    let h = out.jwks_handle.unwrap();
    assert_eq!(h.endpoint_uri, "https://idp/jwks");
    // Bearer config is set
    assert!(out.bearer.is_some());
}

#[test]
fn bearer_jwks_custom_refresh_ms() {
    let i = SecurityCliInput {
        bearer: "jwks".into(),
        jwks_endpoint_uri: Some("https://idp/jwks".into()),
        jwks_refresh_ms: Some(120_000),
        ..input()
    };
    let out = build_security(&i).unwrap();
    assert_eq!(out.jwks_handle.unwrap().refresh_ms, 120_000);
}
```

Note: these won't compile yet (types and fields don't exist).

- [ ] **Step 2: Run to verify the tests fail**

```
cargo test -p crabka-schema-registry --lib 2>&1 | head -30
```

Expected: compile error — `SecurityOutput`, `JwksHandleForRefresh`, `jwks_endpoint_uri`, etc. do not exist.

- [ ] **Step 3: Add the new types and `sec()` test helper**

In `crates/schema-registry/src/cli.rs`, at the top of the file (after the existing `use` lines and before the `SecurityCliInput` struct definition), add:

```rust
use crabka_security::{Jwks, JwksHandle, SignedJwsValidator};
```

(Extend the existing `use crabka_security::{...}` import to add `Jwks, JwksHandle, SignedJwsValidator`.)

Then, **after** the `SecurityCliInput` struct definition and **before** `build_security`, add the two new public types:

```rust
/// JWKS key-set handle plus the metadata the binary needs to drive the
/// periodic refresh task. Returned by [`build_security`] when
/// `--bearer=jwks` is configured; the binary spawns `run_jwks_refresher`.
pub struct JwksHandleForRefresh {
    /// The live key-set cell shared with the `SignedJwsValidator`.
    pub handle: JwksHandle,
    /// URL of the JWKS endpoint (`--bearer-jwks-endpoint-uri`).
    pub endpoint_uri: String,
    /// Optional CA bundle trusted for the JWKS HTTPS connection.
    pub ca_path: Option<std::path::PathBuf>,
    /// Refresh interval in milliseconds. Default 60 000.
    pub refresh_ms: u64,
}

/// Return value of [`build_security`]: the assembled [`SecurityConfig`] plus,
/// when `--bearer=jwks` is set, a [`JwksHandleForRefresh`] the binary must
/// hand to `run_jwks_refresher`.
pub struct SecurityOutput {
    pub config: SecurityConfig,
    pub jwks_handle: Option<JwksHandleForRefresh>,
}

// For convenient field access in unit tests — mirrors the old return type.
impl std::ops::Deref for SecurityOutput {
    type Target = SecurityConfig;
    fn deref(&self) -> &SecurityConfig { &self.config }
}
```

- [ ] **Step 4: Add JWKS fields to `SecurityCliInput`**

Extend the `SecurityCliInput` struct to add the new fields after `bearer_principal_claim`:

```rust
    /// Bearer-token mode: `off` | `unsecured` | `jwks`.
    pub bearer: String,
    /// JWT claim whose value becomes the principal name (Bearer mode).
    pub bearer_principal_claim: String,
    // ── JWKS fields (bearer = "jwks" only) ─────────────────────────────────
    /// URL of the JWKS endpoint (required when bearer = "jwks").
    pub jwks_endpoint_uri: Option<String>,
    /// Expected token `iss` claim. Absent = no issuer check.
    pub jwks_valid_issuer: Option<String>,
    /// Expected token `aud` value. Absent = no audience check.
    pub jwks_expected_audience: Option<String>,
    /// CA bundle (PEM) trusted for the JWKS HTTPS connection.
    pub jwks_ca: Option<std::path::PathBuf>,
    /// Override the JWT claim used as the principal name (defaults to
    /// `bearer_principal_claim`).
    pub jwks_principal_claim: Option<String>,
    /// JWKS refresh interval in milliseconds. Default 60 000.
    pub jwks_refresh_ms: Option<u64>,
```

- [ ] **Step 5: Change `build_security` return type and update `build_bearer`**

Replace the existing `build_security` and `build_bearer` functions:

```rust
/// Assemble [`SecurityOutput`] (= [`SecurityConfig`] + optional JWKS refresh
/// handle) from [`SecurityCliInput`].
///
/// # Errors
///
/// Returns an error for an invalid `bearer`/`tls_client_auth`/
/// `kafka_security_protocol`/`kafka_sasl_mechanism` value, a `tls_cert` set
/// without `tls_key` (or vice versa), a `SASL_*` protocol missing its SASL
/// username/password, or `bearer=jwks` without `jwks_endpoint_uri`.
pub fn build_security(input: &SecurityCliInput) -> anyhow::Result<SecurityOutput> {
    let (bearer, jwks_handle) = build_bearer(input)?;
    Ok(SecurityOutput {
        config: SecurityConfig {
            require_auth: input.require_auth,
            realm: input.realm.clone(),
            basic: build_basic(input),
            bearer,
            tls: build_tls(input)?,
            authz: build_authz(input),
            client: build_client_security(input)?,
        },
        jwks_handle,
    })
}

/// Build [`BearerAuthConfig`] from `bearer`. `off` ⇒ `(None, None)`;
/// `unsecured` ⇒ a dev `UnsecuredJwsValidator` (no refresh handle);
/// `jwks` ⇒ a `SignedJwsValidator` backed by a fresh empty `JwksHandle`
/// plus the refresh metadata.
fn build_bearer(
    input: &SecurityCliInput,
) -> anyhow::Result<(Option<BearerAuthConfig>, Option<JwksHandleForRefresh>)> {
    match input.bearer.as_str() {
        "off" => Ok((None, None)),
        "unsecured" => {
            let validator =
                OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
                    principal_claim_name: input.bearer_principal_claim.clone(),
                    ..Default::default()
                });
            Ok((Some(BearerAuthConfig { validator: Arc::new(validator) }), None))
        }
        "jwks" => {
            let (cfg, refresh) = build_bearer_jwks(input)?;
            Ok((Some(cfg), Some(refresh)))
        }
        other => anyhow::bail!("invalid --bearer: {other} (want off|unsecured|jwks)"),
    }
}

/// Build a [`SignedJwsValidator`]-backed bearer config with a fresh empty
/// [`JwksHandle`]. The binary must spawn `run_jwks_refresher` to populate it.
fn build_bearer_jwks(
    input: &SecurityCliInput,
) -> anyhow::Result<(BearerAuthConfig, JwksHandleForRefresh)> {
    let endpoint_uri = input
        .jwks_endpoint_uri
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "--bearer-jwks-endpoint-uri is required when --bearer=jwks"
            )
        })?
        .clone();

    let handle = JwksHandle::new(Jwks::empty());
    let mut validator = SignedJwsValidator::new(handle.clone());
    validator.principal_claim_name = input
        .jwks_principal_claim
        .clone()
        .unwrap_or_else(|| input.bearer_principal_claim.clone());
    validator.valid_issuer = input.jwks_valid_issuer.clone();
    validator.expected_audience = input.jwks_expected_audience.clone();

    let cfg = BearerAuthConfig {
        validator: Arc::new(OAuthBearerValidator::Signed(validator)),
    };
    let refresh = JwksHandleForRefresh {
        handle,
        endpoint_uri,
        ca_path: input.jwks_ca.clone(),
        refresh_ms: input.jwks_refresh_ms.unwrap_or(60_000),
    };
    Ok((cfg, refresh))
}
```

- [ ] **Step 6: Add `sec()` helper to the test module and update existing test call sites**

In the `#[cfg(test)] mod tests` block, add this helper BEFORE all the existing test functions:

```rust
/// Shortcut: build + unwrap + extract config. Keeps existing tests
/// readable after `build_security` started returning `SecurityOutput`.
fn sec(input: &SecurityCliInput) -> SecurityConfig {
    build_security(input).unwrap().config
}
```

Then do a search-replace inside the `mod tests` block only:
- Replace `build_security(&input()).unwrap()` with `sec(&input())`
- Replace `build_security(&i).unwrap()` with `sec(&i)`
- Leave all `build_security(...).unwrap_err()` calls unchanged (error path still works).

Also update the `input()` helper to add the new JWKS fields (they all default to `None` / defaults so `..Default::default()` will work if you update `SecurityCliInput` to derive `Default`). Confirm the `#[derive(Debug, Default, Clone)]` on `SecurityCliInput` still compiles with the new `Option` fields (it will, since `Option<T>: Default`).

- [ ] **Step 7: Run tests and verify all pass**

```
cargo test -p crabka-schema-registry --lib 2>&1 | tail -20
```

Expected: all tests pass, including the 4 new JWKS tests.

- [ ] **Step 8: Run clippy and fmt**

```
cargo clippy -p crabka-schema-registry --all-targets -D warnings 2>&1 | tail -20
cargo fmt -p crabka-schema-registry
```

Expected: no warnings or errors.

- [ ] **Step 9: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/schema-registry/src/cli.rs
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
feat(schema-registry): add JwksHandleForRefresh + SecurityOutput to cli.rs

Extends `build_security` to return `SecurityOutput` (SecurityConfig +
optional JWKS refresh handle) and adds `--bearer=jwks` support backed
by `SignedJwsValidator::new(JwksHandle::new(Jwks::empty()))`.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: CRD — new types + `secret_name` optionality

**Files:**
- Modify: `crates/operator/src/crd/schema_registry.rs`
- Regenerate: `deploy/crds/crabka.io_schemaregistries.yaml`

- [ ] **Step 1: Write the failing compile-level test**

Add a check in `crates/operator/tests/reconcile_schema_registry.rs` (you can run this to confirm the CRD structs don't exist yet):

```
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | head -5
```

Note the compilation succeeds currently; after adding the new fields in Step 3 and updating the `sr()` helper (Task 5), tests will still need to compile.

- [ ] **Step 2: Add `BearerMode::Jwks` and JWKS fields to `BearerAuthn`**

In `crates/operator/src/crd/schema_registry.rs`, change:

```rust
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    Unsecured,
}
```

to:

```rust
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    /// Dev-only: accept unsigned JWTs (no signature verification).
    Unsecured,
    /// Production: verify JWT signatures against a remote JWKS endpoint.
    Jwks,
}
```

Change `BearerAuthn` from:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BearerAuthn {
    pub mode: BearerMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,
}
```

to:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BearerAuthn {
    /// Bearer mode. `Unsecured` = dev only; `Jwks` = production.
    pub mode: BearerMode,
    /// JWT claim used as the principal name. Default `sub`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,
    // ── JWKS fields (mode = Jwks only) ────────────────────────────────────
    /// JWKS endpoint URL (required when mode = Jwks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,
    /// Expected token `iss` claim. Absent = no issuer check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_valid_issuer: Option<String>,
    /// Expected token `aud` value. Absent = no audience check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expected_audience: Option<String>,
    /// Secret name whose `ca.crt` key is mounted and passed as
    /// `--bearer-jwks-ca`. For private IdP CAs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_tls_secret_name: Option<String>,
    /// Override JWT claim used as the principal name for JWKS mode
    /// (defaults to `principalClaim`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_principal_claim: Option<String>,
    /// JWKS refresh interval in milliseconds. Default 60 000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_refresh_ms: Option<i64>,
}
```

- [ ] **Step 3: Add kafka-client structs and `kafka_client` field on spec**

Add the following new structs **after** `SchemaRegistryAuthz` and **before** `SchemaRegistryStatus`:

```rust
/// SR → broker client security. Maps to the binary's `--kafka-*` flags.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryKafkaClient {
    /// Kafka security protocol: `PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` |
    /// `SASL_SSL`. Default `PLAINTEXT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_protocol: Option<String>,
    /// SASL credentials (required for `SASL_*` protocols).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl: Option<KafkaClientSasl>,
    /// Broker TLS settings (required for `SSL` / `SASL_SSL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<KafkaClientTls>,
}

/// SASL credentials for the SR → broker connection.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientSasl {
    /// SASL mechanism: `PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`.
    pub mechanism: String,
    /// Name of the `Opaque` Secret holding `username` and `password` keys.
    /// The operator injects them as `SCHEMA_REGISTRY_KAFKA_SASL_USERNAME` /
    /// `SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD` env vars via `valueFrom.secretKeyRef`.
    pub secret_ref: String,
}

/// TLS settings for the SR → broker connection.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientTls {
    /// Secret containing `ca.crt` trusted for the broker's server cert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_secret_name: Option<String>,
    /// TLS SNI override for the broker connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name_override: Option<String>,
}
```

Then add `kafka_client` to `SchemaRegistrySpec` (after `group_id`, before `tls`):

```rust
    /// SR → broker client security. Absent = PLAINTEXT (the default for
    /// managed clusters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_client: Option<SchemaRegistryKafkaClient>,
```

- [ ] **Step 4: Change `SchemaRegistryTls.secret_name` to `Option<String>` and add cert-manager fields**

Replace `SchemaRegistryTls` and add `CertManagerIssuerRef`:

```rust
/// Server TLS configuration. Exactly one of `secretName` or `issuerRef`
/// must be set (they are mutually exclusive).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Secret (type `kubernetes.io/tls`) with `tls.crt` + `tls.key`.
    /// Mutually exclusive with `issuerRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// cert-manager issuer reference. When set the reconciler creates a
    /// `cert-manager.io/v1` `Certificate` CR and gates on the resulting
    /// Secret. Mutually exclusive with `secretName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_ref: Option<CertManagerIssuerRef>,
    /// Client-cert mode. Default `Disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth: Option<TlsClientAuth>,
    /// Secret with `ca.crt` to verify client certs (required when
    /// `clientAuth` != Disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca_secret_name: Option<String>,
}

/// Reference to a cert-manager `Issuer` or `ClusterIssuer`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertManagerIssuerRef {
    /// Issuer or ClusterIssuer name.
    pub name: String,
    /// Resource kind: `Issuer` (default) or `ClusterIssuer`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// API group. Default `cert-manager.io`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}
```

- [ ] **Step 5: Regenerate the CRD YAML**

```
cargo run -p crabka-operator -- gen-crds deploy/crds 2>&1
```

Expected: `deploy/crds/crabka.io_schemaregistries.yaml` is updated. Verify it contains `jwksEndpointUri`, `kafkaClient`, `secretName` as optional, `issuerRef`.

- [ ] **Step 6: Clippy check**

```
cargo clippy -p crabka-operator --all-targets -D warnings 2>&1 | tail -20
```

Expected: no errors. If `BearerMode::Jwks` triggers a non-exhaustive match warning in the controller, note the location for Task 5.

- [ ] **Step 7: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/operator/src/crd/schema_registry.rs \
    deploy/crds/crabka.io_schemaregistries.yaml
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
feat(operator/crd): add JWKS bearer, kafka-client, cert-manager TLS to SchemaRegistry CRD

- BearerMode::Jwks variant + JWKS fields on BearerAuthn
- SchemaRegistryKafkaClient / KafkaClientSasl / KafkaClientTls
- SchemaRegistryTls.secret_name: String -> Option<String>
- CertManagerIssuerRef + issuer_ref on SchemaRegistryTls
- Regen CRD YAML

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## ═══ BATCH 2 — parallel: Tasks 2 and 5 ═══

### Task 2: Binary — JWKS clap flags + refresh task

**Files:**
- Modify: `crates/schema-registry/src/bin/schema-registry.rs`

**Depends on:** Task 1 (uses `SecurityOutput`, `JwksHandleForRefresh`).

- [ ] **Step 1: Add JWKS fields to `Args`**

In `crates/schema-registry/src/bin/schema-registry.rs`, after the `bearer_principal_claim` field in `Args`, add:

```rust
    // ── JWKS Bearer ─────────────────────────────────────────────────────────
    /// JWKS endpoint URL (required when --bearer=jwks).
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_ENDPOINT_URI")]
    bearer_jwks_endpoint_uri: Option<String>,
    /// Expected token `iss` claim. Absent = no issuer check.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_VALID_ISSUER")]
    bearer_jwks_valid_issuer: Option<String>,
    /// Expected token `aud` value. Absent = no audience check.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_EXPECTED_AUDIENCE")]
    bearer_jwks_expected_audience: Option<String>,
    /// PEM CA bundle trusted for the JWKS HTTPS endpoint.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_CA")]
    bearer_jwks_ca: Option<PathBuf>,
    /// Override JWT principal-claim name for JWKS mode.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_PRINCIPAL_CLAIM")]
    bearer_jwks_principal_claim: Option<String>,
    /// JWKS refresh interval in milliseconds. Default 60 000.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_REFRESH_MS")]
    bearer_jwks_refresh_ms: Option<u64>,
```

- [ ] **Step 2: Update `Args::security_input()`**

In `security_input()`, add the new JWKS field mappings after `bearer_principal_claim`:

```rust
            jwks_endpoint_uri: self.bearer_jwks_endpoint_uri.clone(),
            jwks_valid_issuer: self.bearer_jwks_valid_issuer.clone(),
            jwks_expected_audience: self.bearer_jwks_expected_audience.clone(),
            jwks_ca: self.bearer_jwks_ca.clone(),
            jwks_principal_claim: self.bearer_jwks_principal_claim.clone(),
            jwks_refresh_ms: self.bearer_jwks_refresh_ms,
```

- [ ] **Step 3: Update `main()` to use `SecurityOutput`**

Change this section:

```rust
    let security = crabka_schema_registry::cli::build_security(&args.security_input())?;
    let cfg = RegistryConfig {
        // ...
        security,
    };
```

to:

```rust
    use crabka_schema_registry::cli::{JwksHandleForRefresh, SecurityOutput};
    let SecurityOutput { config: security, jwks_handle } =
        crabka_schema_registry::cli::build_security(&args.security_input())?;
    let cfg = RegistryConfig {
        // ...
        security,
    };
```

- [ ] **Step 4: Spawn the JWKS refresh task after `shutdown` is created**

After the `let shutdown = CancellationToken::new();` line, add:

```rust
    // ── JWKS refresh task (bearer=jwks only) ────────────────────────────────
    if let Some(jwks) = jwks_handle {
        let shutdown_for_jwks = shutdown.clone();
        tokio::spawn(async move {
            run_jwks_refresher(jwks, shutdown_for_jwks).await;
        });
    }
```

- [ ] **Step 5: Add `run_jwks_refresher` and `build_jwks_client` at the bottom of the file**

```rust
/// Periodically fetch the JWKS endpoint and refresh the live key-set handle.
/// Fetches immediately on startup, then once per `jwks.refresh_ms`.
async fn run_jwks_refresher(
    jwks: crabka_schema_registry::cli::JwksHandleForRefresh,
    cancel: tokio_util::sync::CancellationToken,
) {
    use crabka_security::Jwks;
    use std::time::Duration;

    let client = build_jwks_client(&jwks.ca_path).unwrap_or_else(|e| {
        tracing::error!(error = %e, "JWKS client build failed; using default TLS roots");
        reqwest::Client::new()
    });
    loop {
        match client.get(&jwks.endpoint_uri).send().await {
            Ok(resp) if resp.status().is_success() => match resp.text().await {
                Ok(text) => match Jwks::from_json(&text, true) {
                    Ok(new_keys) => {
                        jwks.handle.store(new_keys);
                        tracing::debug!(uri = %jwks.endpoint_uri, "JWKS refreshed");
                    }
                    Err(e) => tracing::warn!(
                        error = %e, uri = %jwks.endpoint_uri, "JWKS parse error"
                    ),
                },
                Err(e) => tracing::warn!(error = %e, "JWKS response body read error"),
            },
            Ok(resp) => tracing::warn!(
                status = %resp.status(), uri = %jwks.endpoint_uri, "JWKS endpoint error"
            ),
            Err(e) => {
                tracing::warn!(error = %e, uri = %jwks.endpoint_uri, "JWKS fetch failed")
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(jwks.refresh_ms)) => {}
        }
    }
}

fn build_jwks_client(
    ca_path: &Option<std::path::PathBuf>,
) -> anyhow::Result<reqwest::Client> {
    let Some(path) = ca_path else {
        return Ok(reqwest::Client::new());
    };
    let pem = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("read JWKS CA {}: {e}", path.display()))?;
    let cert = reqwest::Certificate::from_pem(&pem)
        .map_err(|e| anyhow::anyhow!("parse JWKS CA PEM: {e}"))?;
    reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .map_err(|e| anyhow::anyhow!("build JWKS reqwest client: {e}"))
}
```

- [ ] **Step 6: Run clippy + fmt**

```
cargo clippy -p crabka-schema-registry --all-targets -D warnings 2>&1 | tail -20
cargo fmt -p crabka-schema-registry
```

- [ ] **Step 7: Build the binary to verify it compiles**

```
cargo build -p crabka-schema-registry 2>&1 | tail -10
```

- [ ] **Step 8: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/schema-registry/src/bin/schema-registry.rs
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
feat(schema-registry/bin): add --bearer-jwks-* flags + JWKS refresh task

Wires --bearer=jwks through the binary: adds clap flags, maps them into
SecurityCliInput, destructures SecurityOutput from build_security, and
spawns run_jwks_refresher to periodically populate the live JwksHandle.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Controller — kafka_client + JWKS bearer rendering

**Files:**
- Modify: `crates/operator/src/controller/schema_registry.rs`
- Modify: `crates/operator/tests/reconcile_schema_registry.rs`

**Depends on:** Task 4 (uses new CRD types).

- [ ] **Step 1: Write the failing tests first**

Add these test functions to `crates/operator/tests/reconcile_schema_registry.rs` **before** the last closing brace:

```rust
#[tokio::test]
async fn kafka_client_missing_when_absent() {
    // Default sr() has kafka_client: None — no --kafka-* args should appear.
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed.iter().find(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
    }).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array().unwrap();
    let joined = args.iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    assert!(!joined.contains("--kafka-security-protocol"));
    assert!(!joined.contains("--kafka-sasl-mechanism"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn kafka_client_sasl_ssl_renders_to_args_and_env() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.kafka_client = Some(crabka_operator::crd::SchemaRegistryKafkaClient {
        security_protocol: Some("SASL_SSL".into()),
        sasl: Some(crabka_operator::crd::KafkaClientSasl {
            mechanism: "PLAIN".into(),
            secret_ref: "kafka-creds".into(),
        }),
        tls: Some(crabka_operator::crd::KafkaClientTls {
            ca_secret_name: Some("kafka-ca".into()),
            server_name_override: Some("broker.internal".into()),
        }),
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed.iter().find(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
    }).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let c = &body["spec"]["template"]["spec"]["containers"][0];
    let joined = c["args"].as_array().unwrap()
        .iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    // Args
    assert!(joined.contains("--kafka-security-protocol=SASL_SSL"), "joined: {joined}");
    assert!(joined.contains("--kafka-sasl-mechanism=PLAIN"), "joined: {joined}");
    assert!(joined.contains("--kafka-tls-ca=/etc/sr/kafka-tls/ca.crt"), "joined: {joined}");
    assert!(joined.contains("--kafka-tls-server-name=broker.internal"), "joined: {joined}");
    // Env: SASL creds via secretKeyRef
    let env = c["env"].as_array().unwrap();
    let sasl_user = env.iter().find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME").unwrap();
    assert_eq!(sasl_user["valueFrom"]["secretKeyRef"]["name"], "kafka-creds");
    assert_eq!(sasl_user["valueFrom"]["secretKeyRef"]["key"], "username");
    let sasl_pass = env.iter().find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD").unwrap();
    assert_eq!(sasl_pass["valueFrom"]["secretKeyRef"]["name"], "kafka-creds");
    assert_eq!(sasl_pass["valueFrom"]["secretKeyRef"]["key"], "password");
    // Volume + mount for kafka-tls CA
    let vols = body["spec"]["template"]["spec"]["volumes"].as_array().unwrap();
    assert!(vols.iter().any(|v| v["name"] == "kafka-tls"), "expected kafka-tls volume");
    let mounts = c["volumeMounts"].as_array().unwrap();
    assert!(mounts.iter().any(|m| m["mountPath"] == "/etc/sr/kafka-tls"), "expected kafka-tls mount");
}

#[tokio::test]
async fn bearer_jwks_renders_to_args() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.authentication = Some(crabka_operator::crd::SchemaRegistryAuthn {
        require_auth: false,
        realm: None,
        basic: None,
        bearer: Some(crabka_operator::crd::BearerAuthn {
            mode: crabka_operator::crd::BearerMode::Jwks,
            principal_claim: None,
            jwks_endpoint_uri: Some("https://idp.example.com/jwks".into()),
            jwks_valid_issuer: Some("https://idp.example.com".into()),
            jwks_expected_audience: Some("kafka-sr".into()),
            jwks_tls_secret_name: None,
            jwks_principal_claim: Some("email".into()),
            jwks_refresh_ms: Some(30_000),
        }),
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed.iter().find(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
    }).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let joined = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array().unwrap()
        .iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    assert!(joined.contains("--bearer=jwks"), "joined: {joined}");
    assert!(joined.contains("--bearer-jwks-endpoint-uri=https://idp.example.com/jwks"), "joined: {joined}");
    assert!(joined.contains("--bearer-jwks-valid-issuer=https://idp.example.com"), "joined: {joined}");
    assert!(joined.contains("--bearer-jwks-expected-audience=kafka-sr"), "joined: {joined}");
    assert!(joined.contains("--bearer-jwks-principal-claim=email"), "joined: {joined}");
    assert!(joined.contains("--bearer-jwks-refresh-ms=30000"), "joined: {joined}");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -20
```

Expected: compile error — `kafka_client` field missing from struct literal in `sr()` helper; and new test functions reference `SchemaRegistryKafkaClient` etc.

- [ ] **Step 3: Fix `sr()` helper and the existing `full_security_fields_render_to_args_and_mounts` test**

In `crates/operator/tests/reconcile_schema_registry.rs`:

**Fix `sr()` helper** — add `kafka_client: None` to `SchemaRegistrySpec`:

```rust
fn sr(name: &str, cluster: Option<&str>) -> SchemaRegistry {
    let mut cr = SchemaRegistry::new(
        name,
        SchemaRegistrySpec {
            replicas: 1,
            image: None,
            bootstrap_servers: None,
            schemas_topic: None,
            schemas_topic_replication_factor: Some(1),
            group_id: None,
            kafka_client: None,   // NEW
            tls: None,
            authentication: None,
            authorization: None,
            resources: None,
        },
    );
    // ...
```

**Fix `full_security_fields_render_to_args_and_mounts`** — change `secret_name: "sr-tls".into()` to `secret_name: Some("sr-tls".into())`:

```rust
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: Some("sr-tls".into()),   // was: "sr-tls".into()
        issuer_ref: None,                      // NEW
        client_auth: Some(crabka_operator::crd::TlsClientAuth::Required),
        client_ca_secret_name: Some("sr-client-ca".into()),
    });
```

- [ ] **Step 4: Update `build_args_and_mounts` to return a 4-tuple and add kafka_client + JWKS rendering**

Replace `build_args_and_mounts` in `crates/operator/src/controller/schema_registry.rs`:

```rust
/// Build the container args, Secret volumes, volumeMounts, and extra env
/// entries (secretKeyRef-based) from the spec.
///
/// Returns `(args, volumes, mounts, extra_env)`.
/// - `args`: `--flag=value` strings for the container `args` list.
/// - `volumes`: pod-level `volumes[]` JSON objects (Secret volumes).
/// - `mounts`: container-level `volumeMounts[]` JSON objects.
/// - `extra_env`: additional `env[]` entries (secretKeyRef, etc.) appended
///   to the fixed POD_NAME + SCHEMA_REGISTRY_ADVERTISED_URL entries.
fn build_args_and_mounts(
    obj: &SchemaRegistry,
    bootstrap: &str,
) -> (
    Vec<String>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
) {
    let s = &obj.spec;
    let mut a: Vec<String> = Vec::new();
    a.push(format!("--bootstrap-servers={bootstrap}"));
    a.push(format!("--listen-addr=0.0.0.0:{SR_PORT}"));
    if let Some(t) = &s.schemas_topic {
        a.push(format!("--schemas-topic={t}"));
    }
    if let Some(rf) = s.schemas_topic_replication_factor {
        a.push(format!("--schemas-topic-rf={rf}"));
    }
    if let Some(g) = &s.group_id {
        a.push(format!("--group-id={g}"));
    }

    let mut volumes = Vec::new();
    let mut mounts = Vec::new();
    let mut extra_env: Vec<serde_json::Value> = Vec::new();

    // Server TLS
    if let Some(tls) = &s.tls {
        if let Some(sn) = &tls.secret_name {
            a.push("--tls-cert=/etc/sr/tls/tls.crt".into());
            a.push("--tls-key=/etc/sr/tls/tls.key".into());
            volumes.push(json!({ "name": "tls", "secret": { "secretName": sn } }));
            mounts.push(json!({ "name": "tls", "mountPath": "/etc/sr/tls", "readOnly": true }));
        }
        let mode = match tls.client_auth.unwrap_or(TlsClientAuth::Disabled) {
            TlsClientAuth::Disabled => "disabled",
            TlsClientAuth::Optional => "optional",
            TlsClientAuth::Required => "required",
        };
        a.push(format!("--tls-client-auth={mode}"));
        if let Some(ca) = &tls.client_ca_secret_name {
            a.push("--tls-client-ca=/etc/sr/client-ca/ca.crt".into());
            volumes.push(json!({ "name": "client-ca", "secret": { "secretName": ca } }));
            mounts.push(
                json!({ "name": "client-ca", "mountPath": "/etc/sr/client-ca", "readOnly": true }),
            );
        }
    }

    // Authentication
    if let Some(authn) = &s.authentication {
        if authn.require_auth {
            a.push("--require-auth".into());
        }
        if let Some(r) = &authn.realm {
            a.push(format!("--realm={r}"));
        }
        if let Some(b) = &authn.basic {
            let key = b.users_secret_key.clone().unwrap_or_else(|| "users".into());
            a.push("--basic-auth-file=/etc/sr/basic/users".into());
            volumes.push(json!({ "name": "basic", "secret": {
                "secretName": b.users_secret_name,
                "items": [{ "key": key, "path": "users" }]
            }}));
            mounts.push(json!({ "name": "basic", "mountPath": "/etc/sr/basic", "readOnly": true }));
        }
        if let Some(bearer) = &authn.bearer {
            match bearer.mode {
                BearerMode::Unsecured => {
                    a.push("--bearer=unsecured".into());
                    if let Some(pc) = &bearer.principal_claim {
                        a.push(format!("--bearer-principal-claim={pc}"));
                    }
                }
                BearerMode::Jwks => {
                    a.push("--bearer=jwks".into());
                    if let Some(uri) = &bearer.jwks_endpoint_uri {
                        a.push(format!("--bearer-jwks-endpoint-uri={uri}"));
                    }
                    if let Some(iss) = &bearer.jwks_valid_issuer {
                        a.push(format!("--bearer-jwks-valid-issuer={iss}"));
                    }
                    if let Some(aud) = &bearer.jwks_expected_audience {
                        a.push(format!("--bearer-jwks-expected-audience={aud}"));
                    }
                    if let Some(pc) = bearer.jwks_principal_claim.as_ref()
                        .or(bearer.principal_claim.as_ref())
                    {
                        a.push(format!("--bearer-jwks-principal-claim={pc}"));
                    }
                    if let Some(ms) = bearer.jwks_refresh_ms {
                        a.push(format!("--bearer-jwks-refresh-ms={ms}"));
                    }
                    if let Some(ca_sn) = &bearer.jwks_tls_secret_name {
                        a.push("--bearer-jwks-ca=/etc/sr/jwks-ca/ca.crt".into());
                        volumes.push(json!({ "name": "jwks-ca", "secret": { "secretName": ca_sn } }));
                        mounts.push(json!({ "name": "jwks-ca", "mountPath": "/etc/sr/jwks-ca", "readOnly": true }));
                    }
                }
            }
        }
    }

    // Authorization
    if let Some(az) = &s.authorization {
        if az.enabled {
            a.push("--authz".into());
        }
        for u in &az.super_users {
            a.push(format!("--super-user={u}"));
        }
        if let Some(r) = az.acl_refresh_seconds {
            a.push(format!("--acl-refresh-secs={r}"));
        }
    }

    // SR → broker kafka-client security
    if let Some(kc) = &s.kafka_client {
        let proto = kc
            .security_protocol
            .as_deref()
            .unwrap_or("PLAINTEXT");
        a.push(format!("--kafka-security-protocol={proto}"));
        if let Some(sasl) = &kc.sasl {
            a.push(format!("--kafka-sasl-mechanism={}", sasl.mechanism));
            // Inject credentials as env via secretKeyRef (the binary reads
            // SCHEMA_REGISTRY_KAFKA_SASL_USERNAME / _PASSWORD env vars).
            extra_env.push(json!({
                "name": "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME",
                "valueFrom": { "secretKeyRef": { "name": sasl.secret_ref, "key": "username" } }
            }));
            extra_env.push(json!({
                "name": "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD",
                "valueFrom": { "secretKeyRef": { "name": sasl.secret_ref, "key": "password" } }
            }));
        }
        if let Some(tls) = &kc.tls {
            if tls.ca_secret_name.is_some() {
                a.push("--kafka-tls-ca=/etc/sr/kafka-tls/ca.crt".into());
            }
            if let Some(sn) = &tls.server_name_override {
                a.push(format!("--kafka-tls-server-name={sn}"));
            }
            if let Some(ca_sn) = &tls.ca_secret_name {
                volumes.push(
                    json!({ "name": "kafka-tls", "secret": { "secretName": ca_sn } }),
                );
                mounts.push(
                    json!({ "name": "kafka-tls", "mountPath": "/etc/sr/kafka-tls", "readOnly": true }),
                );
            }
        }
    }

    (a, volumes, mounts, extra_env)
}
```

- [ ] **Step 5: Update `render_deployment` to thread the extra env**

Update the `render_deployment` function signature and body to use the 4-tuple:

```rust
fn render_deployment(
    obj: &SchemaRegistry,
    bootstrap: &str,
    image: &str,
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let ns = obj
        .meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".into());
    let selector = selector_labels(obj);
    let (args, volumes, mounts, extra_env) = build_args_and_mounts(obj, bootstrap);
    let advertised = format!(
        "{}://$(POD_NAME).{}.{}.svc.cluster.local:{SR_PORT}",
        scheme(obj),
        headless_name(&name),
        ns
    );
    // Fixed env entries + any secretKeyRef extras from build_args_and_mounts.
    let mut env = vec![
        json!({ "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }),
        json!({ "name": "SCHEMA_REGISTRY_ADVERTISED_URL", "value": advertised }),
    ];
    env.extend(extra_env);
    let dep = serde_json::from_value(json!({
        "metadata": {
            "name": deployment_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": meta_labels(obj),
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "replicas": obj.spec.replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "volumes": volumes,
                    "containers": [{
                        "name": "schema-registry",
                        "image": image,
                        "args": args,
                        "env": env,
                        "ports": [{ "name": "rest", "containerPort": SR_PORT, "protocol": "TCP" }],
                        "volumeMounts": mounts,
                        "readinessProbe": { "tcpSocket": { "port": SR_PORT }, "initialDelaySeconds": 2, "periodSeconds": 5 },
                        "livenessProbe": { "tcpSocket": { "port": SR_PORT }, "initialDelaySeconds": 5, "periodSeconds": 10 },
                        "resources": obj.spec.resources.clone().unwrap_or_default(),
                    }],
                }
            }
        }
    }))?;
    Ok(dep)
}
```

Note: `scheme(obj)` still works correctly since it checks `obj.spec.tls.is_some()`. This will be refined in Task 6 for the `issuer_ref` path.

Also add the CRD import — ensure `BearerMode` is imported:
```rust
use crate::crd::{BearerMode, Kafka, SchemaRegistry, SchemaRegistryStatus, TlsClientAuth};
```

- [ ] **Step 6: Run tests**

```
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -20
```

Expected: all tests pass including the 3 new ones.

- [ ] **Step 7: Run clippy + fmt**

```
cargo clippy -p crabka-operator --all-targets -D warnings 2>&1 | tail -20
cargo fmt -p crabka-operator
```

- [ ] **Step 8: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/operator/src/controller/schema_registry.rs \
    crates/operator/tests/reconcile_schema_registry.rs
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
feat(operator/controller): render kafka-client + JWKS bearer args in SchemaRegistry

build_args_and_mounts now returns (args, volumes, mounts, extra_env):
- kafka_client -> --kafka-security-protocol/--kafka-sasl-mechanism/--kafka-tls-*
  + SASL creds via valueFrom.secretKeyRef env vars
- BearerMode::Jwks -> --bearer=jwks + all --bearer-jwks-* flags
- JWKS CA secret -> mounted + --bearer-jwks-ca arg

Fixes existing test: SchemaRegistryTls.secret_name is now Option<String>.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## ═══ BATCH 3 — parallel: Tasks 3 and 6 ═══

### Task 3: JWKS integration tests

**Files:**
- Modify: `crates/schema-registry/Cargo.toml`
- Modify: `crates/schema-registry/tests/security.rs`

**Depends on:** Tasks 1 and 2.

- [ ] **Step 1: Add `ring` as dev-dependency**

In `crates/schema-registry/Cargo.toml`, add to `[dev-dependencies]`:

```toml
ring = { workspace = true }
```

- [ ] **Step 2: Verify the test file compiles with the new dep**

```
cargo test -p crabka-schema-registry --test security --no-run 2>&1 | tail -5
```

- [ ] **Step 3: Add JWT minting helpers and JWKS integration tests to `tests/security.rs`**

Add the following **after** the last existing test function (before the end of the file). Note these helpers re-implement the `mint_rs256` logic from `crates/security/src/jwks.rs` tests using the same static private key:

```rust
// ── JWKS test helpers ────────────────────────────────────────────────────────

/// Static RSA-2048 PKCS#8 private key — same constant as in
/// `crates/security/src/jwks.rs` tests, reproduced here because that
/// module's helper functions are `pub(crate)` and not accessible from
/// outside the security crate. Production never uses a private key;
/// this is only for generating test tokens.
const RSA_PKCS8_B64: &str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC1Ekoc++7sSsH55QXBCq/aj71helk6ZCTkzYxfLRZXbox0FcV7vOkLNodetJLY7nAUekZLltQ7Q6FJ42geqGV+vgttF63Ue9OP24mPmn/OiFqVYhBaJDRI5BMBLCqZbUfpNBDh7ZOCczwlX8Z5FQS0QJBA4F26H9AKzFRvofwHFk1wxqiGdgwDyClgi+eDnhEGGhBEHuTl1edvTRif88rLDfPHKG1TRqKC6LMXCZQdNy7lrDEGPKHqfW4mb2mq7Vj6h2Jjv+1SpsSxdqX8Tsua4/LrAKvFIXfoZAnjzhACbhXqf1DdSdInZ0i1adY8JpgJQ+WtJ0i9aIOnnmDYwgMvAgMBAAECggEAHqBqUr62Kdd3Odpn/7/cAL7hTHSSVRMNPnoZ7RtGNSGothXcolJQpKnjebxXPkQORxhrfWuUmDWXOVUyjkTzbd2dNyWTLGaJYULD4LtENN3RXIUKuQR4p3+US1V6Gxtl12cMF/rEQYNWQAgUHPTWJ9rny2Fn2Qx6dukauwsOAvCU47fL873sm06SYgPJsLm7MKVeifl8dDudgpURxeC9z37cm9kjjE6n6aiBTNAuBEkMaAbcfgJ0RZfzaMo7IpsOeyOwp932JDlKROpQWKA+lz08YzhkU81qHJYOS/js2F0jxzFz31D9IN+OLu7vRCANFLJl/qnin1JEgVPh7gxSKQKBgQDfrQEsutvH1746ytfE+4jUXyv7Fuaz9MML8uaJbC4hMFdCJuMuLBY07bDE23+4byuWY7JHrgsLRaZ+qpNGWs3LH2x6xsHiK8Ivpuy8TVUJ6hgkPK1cr8yUJxaDcyV8tJAZ+mFmyyWx7wUdlgJFCa2MQF1HnrlBKZvSLWV4CjctZQKBgQDPPR2wLwyk6JlyapsVnCpNBGcXqbJxPh1TM7uPqlODxTzegUK+TMJDZ840u2aBNXf2D5WIJMl+/ohYefOOqK9z2OJUGObnJMgGusH04rdbBoDCdBwfwjiluU7vxbuQKBu8JNXzeb7HJhmgxtXWdJuFYcYbmGu8leFvmUxZTPRfAwKBgQCm6Gpf/m/SiGMjbAnmq+xGzV38V/J/hr2lRPRSx68EhRYX/vy3j55ikJu/yitcbViROIPoiS8kkizTiGWtskSuthw04ev74btd46n0OaCjbVPmdoDHEUgPpbtfC6WFkReWyweztRPD2yBuG2pGKhqe9cilkQOcZHgqNkXpdXYHIQKBgQCO0BQkdNfm0O/l3DdRdhPkjVMqCGSTC3YT/0OS5pK07PhccYF4ONdqsh91UWt7QUiRBf5LGubMoEV/i1LfjbmTQPP/dkWxJjS+Bndg9dfbX6jd2DwFWsfE1OXj8ESoPCuYxV23cr+Y59WjaUK1jhgam9106N3d0P/Q8zidFZ4V1wKBgQDFvIqMLnpaInWhb7kP+X6o0tPQSg+6odMWPnjhwnpSIiUjPUTZV4ijc/d1tPsUemFQxDe+ZreQXDMVGcAVldFnoEMyL8iAtMAHtsSmq2E80RNZfc6nUgy5esQ9rJeX2pH9aZCVvKv6iVTeUtAxS+ltjmEG9BSEI2WQI1WDzPbKiA==";

/// Decode the RSA-2048 PKCS#8 DER bytes.
fn rsa_pkcs8_der() -> Vec<u8> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    STANDARD.decode(RSA_PKCS8_B64).unwrap()
}

/// Mint an RS256 JWT signed by [`RSA_PKCS8_B64`] and return
/// `(token, jwks_json)` where the JWKS contains the matching public key.
///
/// `claims_json` is the payload JSON (as a string). Set `exp` to a far
/// future Unix timestamp so the token doesn't expire during tests.
fn mint_rs256_for_test(kid: &str, claims_json: &str) -> (String, String) {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use ring::rand::SystemRandom;
    use ring::signature::{KeyPair as _, RsaKeyPair, RSA_PKCS1_SHA256};

    let der = rsa_pkcs8_der();
    let kp = RsaKeyPair::from_pkcs8(&der).unwrap();
    let header = format!("{{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"{kid}\"}}");
    let signing_input = format!(
        "{}.{}",
        B64.encode(header.as_bytes()),
        B64.encode(claims_json.as_bytes()),
    );
    let mut sig = vec![0u8; kp.public().modulus_len()];
    kp.sign(&RSA_PKCS1_SHA256, &SystemRandom::new(), signing_input.as_bytes(), &mut sig)
        .unwrap();
    let token = format!("{signing_input}.{}", B64.encode(&sig));

    // Extract n, e from the PKCS#1 public key DER embedded in the PKCS#8.
    let (n, e) = split_pkcs1_for_jwks(kp.public().as_ref());
    let jwks = format!(
        "{{\"keys\":[{{\"kty\":\"RSA\",\"kid\":\"{kid}\",\"alg\":\"RS256\",\"use\":\"sig\",\"n\":\"{}\",\"e\":\"{}\"}}]}}",
        B64.encode(&n),
        B64.encode(&e),
    );
    (token, jwks)
}

/// Extract (n, e) big-endian unsigned integers from a PKCS#1 RSAPublicKey DER.
fn split_pkcs1_for_jwks(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    fn read_len(b: &[u8]) -> (usize, usize) {
        if b[0] & 0x80 == 0 { (b[0] as usize, 1) }
        else {
            let nb = (b[0] & 0x7f) as usize;
            let mut l = 0usize;
            for i in 0..nb { l = (l << 8) | b[1+i] as usize; }
            (l, 1 + nb)
        }
    }
    fn read_int(der: &[u8], p: &mut usize) -> Vec<u8> {
        assert_eq!(der[*p], 0x02);
        *p += 1;
        let (len, adv) = read_len(&der[*p..]);
        *p += adv;
        let mut bytes = der[*p..*p+len].to_vec();
        *p += len;
        if bytes.first() == Some(&0) { bytes.remove(0); } // strip DER sign byte
        bytes
    }
    let mut p = 0usize;
    assert_eq!(der[p], 0x30); p += 1;
    let (_, adv) = read_len(&der[p..]); p += adv;
    let n = read_int(der, &mut p);
    let e = read_int(der, &mut p);
    (n, e)
}

/// Start an SR node with JWKS Bearer auth. The caller provides a pre-loaded
/// `JwksHandle` (bypassing HTTP; the handle is populated synchronously).
async fn start_jwks_node(bootstrap: &str, jwks_handle: crabka_security::JwksHandle, valid_issuer: Option<String>) -> Node {
    use crabka_schema_registry::config::BearerAuthConfig;
    use crabka_security::{OAuthBearerValidator, SignedJwsValidator};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());

    let mut validator = SignedJwsValidator::new(jwks_handle);
    if let Some(iss) = valid_issuer {
        validator.valid_issuer = Some(iss);
    }

    let cfg = RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-jwks-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig {
            require_auth: true,
            realm: "test".into(),
            basic: None,
            bearer: Some(BearerAuthConfig {
                validator: Arc::new(OAuthBearerValidator::Signed(validator)),
            }),
            tls: None,
            authz: None,
            client: None,
        },
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let primary = Election::start(&cfg, cancel.clone()).await.unwrap();

    let auth = AuthState {
        basic: None,
        bearer: cfg.security.bearer.as_ref().map(|b| b.validator.clone()),
        require_auth: true,
        realm: "test".into(),
    };
    let fwd = ForwardState {
        primary: primary.clone(),
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let app: Router = rest::router_with_security(
        AppState { store: store.clone() },
        SecurityLayers { auth, authz: None, forward: fwd },
    );
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        rest::serve::serve_http(listener, app, serve_cancel).await.ok();
    });
    Node { port, _store: store, primary, cancel }
}

// ── JWKS integration tests ────────────────────────────────────────────────────

#[tokio::test]
async fn jwks_bearer_valid_signed_token_returns_200(broker: &str) {
    // Broker fixture: use an in-process Broker from other tests in this file.
    // This test is intentionally a standalone async fn that receives the
    // bootstrap string from the shared broker (see the #[tokio::test] wrapper
    // for other tests). Since security.rs boots its own broker per test via
    // `Broker::start`, we mirror that pattern.
    let broker = {
        let b_cfg = BrokerConfig { ..BrokerConfig::default() };
        let (broker, addr) = {
            let b = Broker::start(BrokerConfig::default()).await.unwrap();
            let addr = b.local_addr().to_string();
            (b, addr)
        };
        (broker, addr)
    };
    let (bootstrap, _b) = (broker.1.clone(), broker.0);
    let (token, jwks_json) = mint_rs256_for_test("k1", r#"{"sub":"alice","exp":9999999999}"#);
    let handle = crabka_security::JwksHandle::new(
        crabka_security::Jwks::from_json(&jwks_json, true).unwrap(),
    );
    let node = start_jwks_node(&bootstrap, handle, None).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    // Valid signed token → 200
    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {token}"))
        .body(SCHEMA_BODY)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "expected 200, got {} with valid signed token", resp.status());

    node.cancel.cancel();
}

#[tokio::test]
async fn jwks_bearer_unsigned_token_returns_401() {
    let broker = Broker::start(BrokerConfig::default()).await.unwrap();
    let bootstrap = broker.local_addr().to_string();

    let (_token, jwks_json) = mint_rs256_for_test("k1", r#"{"sub":"alice","exp":9999999999}"#);
    let handle = crabka_security::JwksHandle::new(
        crabka_security::Jwks::from_json(&jwks_json, true).unwrap(),
    );
    let node = start_jwks_node(&bootstrap, handle, None).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    // Unsigned (alg:none) JWT — the Signed validator must reject it.
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    let unsigned_token = format!(
        "{}.{}.{}",
        B64.encode(br#"{"alg":"none","typ":"JWT"}"#),
        B64.encode(br#"{"sub":"alice","exp":9999999999}"#),
        "",
    );
    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {unsigned_token}"))
        .body(SCHEMA_BODY)
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "expected 401 for unsigned token, got {}", resp.status());

    node.cancel.cancel();
}

#[tokio::test]
async fn jwks_bearer_wrong_issuer_returns_401() {
    let broker = Broker::start(BrokerConfig::default()).await.unwrap();
    let bootstrap = broker.local_addr().to_string();

    let (token, jwks_json) = mint_rs256_for_test(
        "k1",
        r#"{"sub":"alice","iss":"https://wrong-idp.example.com","exp":9999999999}"#,
    );
    let handle = crabka_security::JwksHandle::new(
        crabka_security::Jwks::from_json(&jwks_json, true).unwrap(),
    );
    // Configure node to require iss = "https://idp.example.com"
    let node = start_jwks_node(
        &bootstrap,
        handle,
        Some("https://idp.example.com".into()),
    ).await;
    let base = format!("http://127.0.0.1:{}", node.port);
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/subjects/s/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .header("Authorization", format!("Bearer {token}"))
        .body(SCHEMA_BODY)
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "expected 401 for wrong issuer, got {}", resp.status());

    node.cancel.cancel();
}
```

Note: the tests use `crabka_security::JwksHandle` and `crabka_security::Jwks`, which are already in scope via the existing `use crabka_security::*` imports. You may need to add `use crabka_security::{Jwks, JwksHandle};` if not already imported.

Also, the tests need `BrokerConfig::default()` — check how the existing tests in `security.rs` start a broker:

The existing tests construct a `Broker` via:
```rust
let (broker, _) = crabka_broker::Broker::start(BrokerConfig::default()).await.unwrap();
```

Match that pattern. The `BrokerConfig` import is available via `use crabka_broker::{Broker, BrokerConfig}`.

- [ ] **Step 4: Run the JWKS integration tests**

```
cargo test -p crabka-schema-registry --test security jwks_ -- --nocapture 2>&1 | tail -30
```

Expected: all 3 `jwks_*` tests pass.

- [ ] **Step 5: Run the full security test suite to confirm no regressions**

```
cargo test -p crabka-schema-registry --test security 2>&1 | tail -20
```

- [ ] **Step 6: Clippy + fmt**

```
cargo clippy -p crabka-schema-registry --all-targets -D warnings 2>&1 | tail -20
cargo fmt -p crabka-schema-registry
```

- [ ] **Step 7: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/schema-registry/Cargo.toml \
    crates/schema-registry/tests/security.rs
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
test(schema-registry): add JWKS bearer integration tests

Three in-process tests (valid-token→200, unsigned→401, wrong-issuer→401)
build SecurityConfig directly with a pre-loaded JwksHandle, bypassing
the HTTP refresh path. JWT signing uses ring (added as dev-dep).

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: cert-manager Certificate CRs + ClusterRole

**Files:**
- Modify: `crates/operator/src/controller/schema_registry.rs`
- Modify: `crates/operator/tests/reconcile_schema_registry.rs`
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

**Depends on:** Tasks 4 and 5.

- [ ] **Step 1: Write the failing tests first**

Add these test functions to `crates/operator/tests/reconcile_schema_registry.rs`:

```rust
#[tokio::test]
async fn secret_name_and_issuer_ref_mutual_exclusion() {
    // Both secretName AND issuerRef set → InvalidSpec status, no children applied.
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: Some("explicit-secret".into()),
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: None,
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    // No Deployment or Service apply.
    assert!(!observed.iter().any(|r| r.uri().to_string().contains("/deployments/")));
    let patch = observed.iter().find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status")).unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"].as_array().unwrap()
        .iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["reason"], "InvalidSpec");
}

#[tokio::test]
async fn issuer_ref_creates_certificate_cr_and_waits() {
    // issuerRef set, no secretName → reconciler must PATCH the Certificate CR
    // and requeue (WaitingForCert) because the Secret does not exist yet.
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: None,
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: Some("ClusterIssuer".into()),
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![
        // cert-manager Certificate CR SSA PATCH
        MockRule {
            method: Method::PATCH,
            path_substr: "/certificates/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"apiVersion":"cert-manager.io/v1","kind":"Certificate","metadata":{"name":"sr1-sr"}})),
        },
        // Secret GET (404 = not yet provisioned)
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/sr1-sr-tls".into(),
            response: json_response(404, &serde_json::json!({"kind":"Status","status":"Failure","reason":"NotFound"})),
        },
        // Status PATCH (WaitingForCert)
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    // Certificate CR PATCH must have happened.
    assert!(observed.iter().any(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/certificates/sr1-sr")),
        "expected Certificate CR PATCH");
    // No Deployment (waiting for cert).
    assert!(!observed.iter().any(|r| r.uri().to_string().contains("/deployments/")),
        "expected no deployment while WaitingForCert");
    let patch = observed.iter().find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status")).unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"].as_array().unwrap()
        .iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["reason"], "WaitingForCert");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn issuer_ref_with_cert_secret_ready_renders_deployment() {
    // issuerRef set, Secret exists → full reconcile (Certificate CR + deployment).
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: None,
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: None,   // default Issuer
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/certificates/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"apiVersion":"cert-manager.io/v1","kind":"Certificate","metadata":{"name":"sr1-sr"}})),
        },
        // Secret exists (cert ready)
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/sr1-sr-tls".into(),
            response: json_response(200, &serde_json::json!({"kind":"Secret","metadata":{"name":"sr1-sr-tls"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(200, &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}})),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(200, &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}})),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed.iter().find(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
    }).unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let joined = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array().unwrap()
        .iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" ");
    // The TLS cert/key args must use the cert-manager-provisioned secret name.
    assert!(joined.contains("--tls-cert=/etc/sr/tls/tls.crt"), "joined: {joined}");
    let vols = body["spec"]["template"]["spec"]["volumes"].as_array().unwrap();
    let tls_vol = vols.iter().find(|v| v["name"] == "tls").unwrap();
    assert_eq!(tls_vol["secret"]["secretName"], "sr1-sr-tls");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -20
```

Expected: compile error or runtime panic — `CertManagerIssuerRef` exists (Task 4) but the reconciler doesn't handle `issuer_ref` yet.

- [ ] **Step 3: Add DynamicObject imports to `schema_registry.rs`**

In `crates/operator/src/controller/schema_registry.rs`, update the imports:

```rust
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Secret, Service};
use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::runtime::controller::{Action, Controller};
// ... rest unchanged
use crate::controller::common::{FIELD_MANAGER, ReconcileError, apply_object, condition, owner_ref, patch_status};
use crate::crd::{BearerMode, CertManagerIssuerRef, Kafka, SchemaRegistry, SchemaRegistryStatus, TlsClientAuth};
```

- [ ] **Step 4: Add `apply_certificate_cr`**

Add this function after `build_args_and_mounts` and before `set_status`:

```rust
/// SSA-apply a `cert-manager.io/v1` `Certificate` CR. Uses
/// [`DynamicObject`] so we don't need a cert-manager Rust crate.
///
/// The Certificate targets `secretName`, and sets two DNS SANs:
/// - `*.<name>-sr-headless.<ns>.svc.cluster.local` (per-pod headless DNS)
/// - `<name>-sr.<ns>.svc.cluster.local` (ClusterIP service DNS)
async fn apply_certificate_cr(
    client: &kube::Client,
    ns: &str,
    name: &str,
    cert_secret_name: &str,
    issuer: &CertManagerIssuerRef,
    owner: &SchemaRegistry,
) -> Result<(), ReconcileError> {
    let kind = issuer.kind.as_deref().unwrap_or("Issuer");
    let group = issuer.group.as_deref().unwrap_or("cert-manager.io");
    let cert_name = format!("{name}-sr");
    let body = serde_json::json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": cert_name,
            "namespace": ns,
            "ownerReferences": [owner_ref::<SchemaRegistry>(owner)?],
        },
        "spec": {
            "secretName": cert_secret_name,
            "issuerRef": {
                "name": issuer.name,
                "kind": kind,
                "group": group,
            },
            "dnsNames": [
                format!("*.{}.{}.svc.cluster.local", headless_name(name), ns),
                format!("{}.{}.svc.cluster.local", service_name(name), ns),
            ],
        }
    });
    let gvk = GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate");
    let ar = ApiResource::from_gvk_with_plural(&gvk, "certificates");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &ar);
    let obj: DynamicObject = serde_json::from_value(body)?;
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    match api.patch(&cert_name, &pp, &Patch::Apply(&obj)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(status)) if status.code == 404 => Err(
            ReconcileError::Malformed("cert-manager Certificate CRD not installed".into()),
        ),
        Err(e) => Err(e.into()),
    }
}
```

- [ ] **Step 5: Update `reconcile` to resolve TLS secret and handle `issuer_ref`**

Replace step 3 of the existing `reconcile` function. Currently:

```rust
    // 3. Render + apply children (Deployment + 2 Services).
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);

    let headless = render_headless_service(&obj)?;
    apply_object(&svc_api, &headless_name(&name), &headless).await?;
    let clusterip = render_clusterip_service(&obj)?;
    apply_object(&svc_api, &service_name(&name), &clusterip).await?;
    let image = obj
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_schema_registry_image.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let deployment = render_deployment(&obj, &bootstrap, &image)?;
    apply_object(&dep_api, &deployment_name(&name), &deployment).await?;
```

Replace with:

```rust
    // 3. Resolve TLS secret name (handles issuerRef path + mutual-exclusion).
    let tls_secret_name: Option<String> = if let Some(tls) = &obj.spec.tls {
        match (&tls.secret_name, &tls.issuer_ref) {
            (Some(_), Some(_)) => {
                set_status(
                    &sr_api,
                    &name,
                    &obj,
                    "InvalidSpec",
                    "spec.tls.secretName and spec.tls.issuerRef are mutually exclusive",
                    None,
                    None,
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            (None, None) => {
                set_status(
                    &sr_api,
                    &name,
                    &obj,
                    "InvalidSpec",
                    "spec.tls must set either secretName or issuerRef",
                    None,
                    None,
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            (Some(sn), None) => Some(sn.clone()),
            (None, Some(issuer)) => {
                let cert_secret = format!("{name}-sr-tls");
                apply_certificate_cr(&ctx.client, &ns, &name, &cert_secret, issuer, &obj)
                    .await?;
                // Gate on the Secret being provisioned by cert-manager.
                let secret_api: Api<Secret> =
                    Api::namespaced(ctx.client.clone(), &ns);
                if secret_api.get_opt(&cert_secret).await?.is_none() {
                    set_status(
                        &sr_api,
                        &name,
                        &obj,
                        "WaitingForCert",
                        &format!(
                            "waiting for cert-manager to provision Secret {cert_secret}"
                        ),
                        None,
                        None,
                    )
                    .await?;
                    return Ok(Action::requeue(Duration::from_secs(10)));
                }
                Some(cert_secret)
            }
        }
    } else {
        None
    };

    // 4. Render + apply children (Deployment + 2 Services).
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);

    let headless = render_headless_service(&obj)?;
    apply_object(&svc_api, &headless_name(&name), &headless).await?;
    let clusterip = render_clusterip_service(&obj)?;
    apply_object(&svc_api, &service_name(&name), &clusterip).await?;
    let image = obj
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_schema_registry_image.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let deployment = render_deployment(&obj, &bootstrap, &image, tls_secret_name.as_deref())?;
    apply_object(&dep_api, &deployment_name(&name), &deployment).await?;
```

Renumber remaining steps 4→5 in the existing reconcile body.

- [ ] **Step 6: Update `render_deployment` and `build_args_and_mounts` signatures for `tls_secret`**

Change `render_deployment` signature and the call to `build_args_and_mounts`:

```rust
fn render_deployment(
    obj: &SchemaRegistry,
    bootstrap: &str,
    image: &str,
    tls_secret: Option<&str>,       // NEW: resolved from reconciler
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let ns = obj
        .meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".into());
    let selector = selector_labels(obj);
    let (args, volumes, mounts, extra_env) =
        build_args_and_mounts(obj, bootstrap, tls_secret);   // pass resolved name
    let scheme = if tls_secret.is_some() { "https" } else { "http" };
    let advertised = format!(
        "{}://$(POD_NAME).{}.{}.svc.cluster.local:{SR_PORT}",
        scheme,
        headless_name(&name),
        ns
    );
    // ... rest unchanged
```

Change `build_args_and_mounts` signature to accept the resolved secret name:

```rust
fn build_args_and_mounts(
    obj: &SchemaRegistry,
    bootstrap: &str,
    tls_secret: Option<&str>,       // NEW: replaces reading tls.secret_name
) -> (
    Vec<String>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
) {
    // ...
    // Server TLS — use the resolved tls_secret instead of tls.secret_name
    if let Some(tls) = &s.tls {
        if let Some(sn) = tls_secret {    // ← use param, not tls.secret_name
            a.push("--tls-cert=/etc/sr/tls/tls.crt".into());
            a.push("--tls-key=/etc/sr/tls/tls.key".into());
            volumes.push(json!({ "name": "tls", "secret": { "secretName": sn } }));
            mounts.push(json!({ "name": "tls", "mountPath": "/etc/sr/tls", "readOnly": true }));
        }
        // client_auth and client_ca still from tls struct
        let mode = match tls.client_auth.unwrap_or(TlsClientAuth::Disabled) {
            TlsClientAuth::Disabled => "disabled",
            TlsClientAuth::Optional => "optional",
            TlsClientAuth::Required => "required",
        };
        a.push(format!("--tls-client-auth={mode}"));
        if let Some(ca) = &tls.client_ca_secret_name {
            a.push("--tls-client-ca=/etc/sr/client-ca/ca.crt".into());
            volumes.push(json!({ "name": "client-ca", "secret": { "secretName": ca } }));
            mounts.push(
                json!({ "name": "client-ca", "mountPath": "/etc/sr/client-ca", "readOnly": true }),
            );
        }
    }
    // ... Authentication / Authorization / kafka_client sections unchanged
```

Also remove or update `scheme(obj)` — it's no longer called from `render_deployment` (the `let scheme` is now inline). Delete or `#[allow(dead_code)]` the `scheme` function if it's no longer used.

- [ ] **Step 7: Add cert-manager RBAC rule to ClusterRole**

In `charts/crabka-operator/templates/clusterrole.yaml`, add **before** the closing `{{- end }}`:

```yaml
  # Slice 8: SR cert-manager Certificate CR management.
  - apiGroups: ["cert-manager.io"]
    resources: ["certificates"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

- [ ] **Step 8: Run all operator tests**

```
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -20
```

Expected: all tests pass, including the 3 new cert-manager tests.

- [ ] **Step 9: Run clippy + fmt**

```
cargo clippy -p crabka-operator --all-targets -D warnings 2>&1 | tail -20
cargo fmt -p crabka-operator
```

- [ ] **Step 10: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
    crates/operator/src/controller/schema_registry.rs \
    crates/operator/tests/reconcile_schema_registry.rs \
    charts/crabka-operator/templates/clusterrole.yaml
git -C /Users/mattstone/git/crabka \
    -c user.name="Matthew Stone" \
    -c user.email="matthew.d.stone@gmail.com" \
    commit -m "$(cat <<'EOF'
feat(operator): cert-manager Certificate CR support for SchemaRegistry TLS

Reconciler now resolves tls_secret_name from either spec.tls.secretName
(explicit) or spec.tls.issuerRef (cert-manager path):
- apply_certificate_cr: SSA-applies a cert-manager.io/v1 Certificate CR
  via DynamicObject (no cert-manager Rust dep needed)
- WaitingForCert gate: requeues until Secret exists
- InvalidSpec: secretName + issuerRef mutually exclusive
- ClusterRole: add cert-manager.io/certificates RBAC rule

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Final verification

After all 6 tasks complete, run the full workspace check:

```bash
# From /Users/mattstone/git/crabka
cargo clippy --workspace --all-targets -D warnings 2>&1 | tail -10
cargo fmt --check
cargo test -p crabka-schema-registry --lib 2>&1 | tail -5
cargo test -p crabka-operator --test reconcile_schema_registry 2>&1 | tail -5
```

All four commands should exit 0 with no warnings.

### Notes for implementers

1. **`SchemaRegistryTls.secret_name: String → Option<String>`** is a greenfield breaking change (no back-compat shims). The `sr()` test helper's `tls: None` field needs `kafka_client: None` added; `full_security_fields_render_to_args_and_mounts` needs `secret_name: Some("sr-tls".into())`. Both fixes are in Task 5.

2. **`SecurityOutput` `Deref<Target = SecurityConfig>`** lets all existing test assertions (`s.require_auth`, `s.bearer`, etc.) work without change after replacing `build_security(&input()).unwrap()` with `sec(&input())` in the tests module.

3. **JWKS handle identity**: `JwksHandle` is `Arc`-backed. `JwksHandle::new(Jwks::empty())` creates a fresh cell; `handle.clone()` in `build_bearer_jwks` shares that same cell with the `SignedJwsValidator`. When the refresh task calls `handle.store(new_keys)`, the validator sees the update immediately.

4. **`scheme()` removal**: After Task 6 changes `render_deployment` to compute `scheme` inline from `tls_secret`, the standalone `scheme(obj: &SchemaRegistry) -> &'static str` function becomes dead code. Remove it or it will fail clippy's `dead_code` lint.

5. **Dynamic API URL for cert-manager mocks**: The `DynamicObject` API URL is `/apis/cert-manager.io/v1/namespaces/{ns}/certificates/{name}`. The mock `path_substr` `"/certificates/sr1-sr"` will match this URL.

6. **`reqwest::Certificate::from_pem` in reqwest 0.13**: This requires the `rustls` feature (already present in the schema-registry Cargo.toml). If the API has changed in 0.13, use `reqwest::Client::builder().use_rustls_tls()` and load the CA manually via `rustls::RootCertStore`.
