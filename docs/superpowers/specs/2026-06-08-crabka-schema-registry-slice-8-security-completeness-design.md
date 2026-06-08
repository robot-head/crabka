# Schema Registry Slice 8 — Security Completeness Design

## Goal

Wire the three security gaps left deferred after slice 7:

1. **JWKS-backed Bearer auth** — the `crabka-security` `SignedJwsValidator` + `JwksHandle` exist but are not wired into the SR CLI or CRD; `BearerMode` is `Unsecured`-only today.
2. **Secured SR→broker client** — the SR CLI already has `--kafka-*` flags for SASL/TLS, but the operator never renders them; the Kafka cluster's security is invisible to the SR Deployment.
3. **Cert-manager serving-cert provisioning** — the SR CRD requires a user-supplied `kubernetes.io/tls` Secret today; this slice adds an `issuerRef` path that lets cert-manager provision the serving cert automatically.

All three changes are **additive**: absent fields leave existing behavior untouched.

---

## Architecture

```
slice 8
├── A: JWKS Bearer
│   ├── crates/schema-registry/src/cli.rs        (new jwks mode + config fields)
│   ├── crates/schema-registry/src/bin/schema-registry.rs  (jwks refresh task)
│   └── crates/operator/src/crd/schema_registry.rs         (BearerMode::Jwks + fields)
│       + controller/schema_registry.rs          (render jwks args+mounts)
│
├── B: Secured SR→Broker
│   ├── crates/operator/src/crd/schema_registry.rs         (kafkaClient stanza)
│   └── crates/operator/src/controller/schema_registry.rs  (render --kafka-* args)
│
└── C: Cert-Manager Certificate CRs
    ├── crates/operator/src/crd/schema_registry.rs         (issuerRef on SchemaRegistryTls)
    ├── crates/operator/src/controller/schema_registry.rs  (Certificate CR SSA + pending gate)
    └── charts/crabka-operator/templates/clusterrole.yaml  (certificates.cert-manager.io)
```

The crate boundary is clear:

- `crabka-schema-registry` owns the runtime: JWKS validator wiring + refresh task.
- `crabka-operator` owns the packaging: CRD field additions + operator rendering.

No new crate dependencies are introduced by Parts A or B. Part C uses `kube::core::DynamicObject` (already in `kube`, already a dependency) to SSA-apply `cert-manager.io/v1 Certificate` CRs without pulling in a cert-manager Rust crate.

---

## Part A: JWKS Bearer

### CRD changes

`crates/operator/src/crd/schema_registry.rs`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum BearerMode {
    /// No signature validation — accept any well-formed JWT. Dev/test only.
    Unsecured,
    /// RS256/ES256 signature validation against a remote JWKS endpoint.
    Jwks,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BearerAuthn {
    pub mode: BearerMode,
    // ── JWKS mode fields (ignored when mode == Unsecured) ──
    /// HTTPS URL of the JWKS document (required when mode == Jwks).
    pub jwks_endpoint_uri: Option<String>,
    /// Expected `iss` claim value. None = any issuer accepted.
    pub valid_issuer: Option<String>,
    /// Expected `aud` claim value. None = audience not validated.
    pub expected_audience: Option<String>,
    /// Secret holding `ca.crt` — PEM CA trusted for the JWKS endpoint TLS.
    /// None = system roots.
    pub jwks_tls_secret_name: Option<String>,
    /// Claim used as the authenticated principal name. Default: `"sub"`.
    pub principal_claim: Option<String>,
    /// JWKS refresh interval in ms. Default: 300 000 (5 min).
    pub jwks_refresh_ms: Option<i64>,
}
```

### CLI changes

`SecurityCliInput` gains six new fields:

```rust
pub jwks_endpoint_uri: Option<String>,
pub jwks_valid_issuer: Option<String>,
pub jwks_expected_audience: Option<String>,
pub jwks_ca: Option<PathBuf>,        // PEM CA for JWKS endpoint TLS
pub jwks_principal_claim: Option<String>,
pub jwks_refresh_ms: Option<u64>,
```

`Args` in `bin/schema-registry.rs` gains the matching clap fields with
`env = "SCHEMA_REGISTRY_BEARER_JWKS_*"` vars.

`build_bearer` in `cli.rs` handles a third case:

```rust
"jwks" => {
    let endpoint = input.jwks_endpoint_uri
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--bearer-jwks-endpoint-uri required for --bearer=jwks"))?
        .to_owned();
    // Build JwksHandle + SignedJwsValidator; return handle alongside the validator
    // so run.rs can start the refresh task.
    build_jwks(input, endpoint)?
}
```

Because the refresh task needs the `JwksHandle`, `build_security` (in `cli.rs`) returns a new `SecurityOutput` (also defined in `cli.rs`):

```rust
/// Returned by `build_security`; lives in `cli.rs` alongside `SecurityCliInput`.
pub struct SecurityOutput {
    pub config: SecurityConfig,
    /// Non-None only when bearer mode is `jwks`. Pass to `run_jwks_refresher`.
    pub jwks_handle: Option<JwksHandleForRefresh>,
}

/// All data the binary needs to spawn a JWKS background refresh task.
/// Defined in `cli.rs`; consumed by `run.rs`/`main`.
pub struct JwksHandleForRefresh {
    pub handle: JwksHandle,
    pub endpoint_uri: String,
    /// PEM CA file for the JWKS endpoint TLS (None = system roots).
    pub ca_path: Option<PathBuf>,
    pub refresh_ms: u64,
}
```

### Binary: JWKS refresh task

`run.rs` (or `main`) checks `security_output.jwks_handle`:

```rust
if let Some(jwks) = security_output.jwks_handle {
    let cancel = shutdown.clone();
    tokio::spawn(async move {
        run_jwks_refresher(jwks, cancel).await;
    });
}
```

`run_jwks_refresher` does an HTTP GET on the endpoint URL every `refresh_ms`, parses the JWKS document, calls `handle.store(keys)`, and exits on cancel. It logs a warning (not an error) on transient fetch failures so a momentary IdP hiccup does not crash the SR.

### Operator rendering

When `spec.authentication.bearer.mode == Jwks`, `build_args_and_mounts` renders:

```
--bearer=jwks
--bearer-jwks-endpoint-uri=<jwksEndpointUri>
--bearer-jwks-valid-issuer=<validIssuer>          # if set
--bearer-jwks-expected-audience=<expectedAudience> # if set
--bearer-jwks-principal-claim=<principalClaim>    # if set
--bearer-jwks-refresh-ms=<jwksRefreshMs>          # if set
```

If `jwksTlsSecretName` is set, mount the Secret at `/etc/sr/jwks-tls/` and add:

```
--bearer-jwks-ca=/etc/sr/jwks-tls/ca.crt
```

### Validation

New CLI unit tests in `cli.rs` for the `jwks` build path (no network — just assert `SecurityOutput.config` fields). The `tests/security.rs` integration test adds a JWKS case: spin up a minimal JWKS HTTP endpoint serving a test RS256 key pair, configure SR with `--bearer=jwks`, assert a signed JWT passes and an unsigned / wrong-iss JWT gets 401. Reuse `crabka_security::ca` test key material.

---

## Part B: Secured SR→Broker (Operator)

### CRD changes

New top-level field on `SchemaRegistrySpec`:

```rust
pub kafka_client: Option<SchemaRegistryKafkaClient>,
```

New types:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryKafkaClient {
    /// Kafka transport protocol. Default: `PLAINTEXT`.
    /// One of: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.
    pub security_protocol: Option<String>,
    /// SASL settings. Required when securityProtocol is `SASL_PLAINTEXT` or `SASL_SSL`.
    pub sasl: Option<KafkaClientSasl>,
    /// TLS settings. Required when securityProtocol is `SSL` or `SASL_SSL`.
    pub tls: Option<KafkaClientTls>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientSasl {
    /// `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`.
    pub mechanism: String,
    /// Secret with keys `username` and `password`.
    pub secret_ref: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientTls {
    /// Secret with key `ca.crt` — PEM CA trusting the broker's server cert.
    pub ca_secret_name: Option<String>,
    /// Optional TLS SNI / server name override.
    pub server_name_override: Option<String>,
}
```

### Operator rendering

`build_args_and_mounts` gains a `kafka_client` section:

```
--kafka-security-protocol=<securityProtocol>   # always, default PLAINTEXT

# When sasl is set:
# Mount Secret at /etc/sr/kafka-sasl/; use valueFrom.secretKeyRef for env
SCHEMA_REGISTRY_KAFKA_SASL_USERNAME = secretKeyRef(sasl.secretRef, "username")
SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD = secretKeyRef(sasl.secretRef, "password")
--kafka-sasl-mechanism=<sasl.mechanism>

# When tls.caSecretName is set:
# Mount Secret at /etc/sr/kafka-tls/; add arg:
--kafka-tls-ca=/etc/sr/kafka-tls/ca.crt

# When tls.serverNameOverride is set:
--kafka-tls-server-name=<serverNameOverride>
```

Environment variable injection (not file mounts) is used for SASL credentials because:
- The SR binary already reads `SCHEMA_REGISTRY_KAFKA_SASL_USERNAME` / `SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD`.
- No new file-path flags needed.
- Kubernetes log scrubbers can redact env values; file-based passwords require manual scrubbing.

### Reconciler tests

`tests/reconcile_schema_registry.rs` gets two new cases:

1. `kafka_client_sasl_ssl_renders_to_args_and_env` — assert that `kafkaClient: {securityProtocol: SASL_SSL, sasl: ..., tls: ...}` produces the correct `--kafka-*` args, volumes, and `env` entries.
2. `kafka_client_missing_when_absent` — assert that omitting `kafkaClient` produces no `--kafka-*` args (backward-compat gate).

---

## Part C: Cert-Manager Certificate CRs

### CRD changes

`SchemaRegistryTls` gains an `issuer_ref` field and relaxes `secret_name` to `Option`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Name of a `kubernetes.io/tls` Secret to use as the serving cert.
    /// Mutually exclusive with `issuerRef`.
    pub secret_name: Option<String>,
    /// cert-manager issuer reference. When set, the reconciler creates a
    /// `Certificate` CR and derives the serving-cert Secret name automatically.
    /// Mutually exclusive with `secretName`.
    pub issuer_ref: Option<CertManagerIssuerRef>,
    pub client_auth: Option<TlsClientAuth>,
    pub client_ca_secret_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertManagerIssuerRef {
    pub name: String,
    /// `"Issuer"` or `"ClusterIssuer"`. Default: `"ClusterIssuer"`.
    pub kind: Option<String>,
    /// API group. Default: `"cert-manager.io"`.
    pub group: Option<String>,
}
```

### Reconciler flow

`reconcile` checks `spec.tls.issuer_ref` before rendering the Deployment:

```rust
let tls_secret_name: Option<String> = match (&spec.tls.secret_name, &spec.tls.issuer_ref) {
    (Some(s), None) => Some(s.clone()),
    (None, Some(issuer)) => {
        let cert_secret = format!("{}-sr-serving", obj.name_any());
        apply_certificate_cr(ctx, &obj, &cert_secret, issuer).await?;
        // Gate: if the Secret hasn't been provisioned yet, set condition + requeue.
        if ctx.client.get_opt::<Secret>(&cert_secret, ns).await?.is_none() {
            set_status(&obj, ctx, Some("WaitingForCert"), "cert-manager has not yet provisioned the serving-cert Secret").await?;
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
        Some(cert_secret)
    }
    (Some(_), Some(_)) => {
        set_status(&obj, ctx, Some("InvalidSpec"), "spec.tls.secretName and spec.tls.issuerRef are mutually exclusive").await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    (None, None) => None, // no TLS
};
```

`apply_certificate_cr` SSA-applies a `DynamicObject`:

```rust
let cert_api_resource = ApiResource {
    group: "cert-manager.io".to_string(),
    version: "v1".to_string(),
    kind: "Certificate".to_string(),
    api_version: "cert-manager.io/v1".to_string(),
    plural: "certificates".to_string(),
};
let api = Api::<DynamicObject>::namespaced_with(ctx.client.clone(), ns, &cert_api_resource);
// Build DynamicObject with serde_json::json! body; SSA apply.
```

DNS SANs include:
- `*.{name}-sr-headless.{ns}.svc.cluster.local` (per-pod headless addresses)
- `{name}-sr.{ns}.svc.cluster.local` (ClusterIP service)

### ClusterRole addition

`charts/crabka-operator/templates/clusterrole.yaml` gains:

```yaml
- apiGroups: ["cert-manager.io"]
  resources: ["certificates"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

### Operator RBAC in `deploy/`

The operator's `ClusterRole` YAML in `deploy/` (generated or hand-maintained — check repo) gets the same addition.

### Reconciler tests

`tests/reconcile_schema_registry.rs`:

1. `issuer_ref_creates_certificate_cr` — mock context has no `{name}-sr-serving` Secret → reconciler calls SSA on the Certificate CR → returns requeue action.
2. `issuer_ref_with_cert_ready_renders_deployment` — mock context has the `{name}-sr-serving` Secret → full Deployment rendered with correct TLS mount.
3. `secret_name_and_issuer_ref_mutual_exclusion` — both set → `InvalidSpec` condition, requeue.

---

## Testing strategy

| Layer | What | Where |
|---|---|---|
| CLI unit | `build_bearer("jwks", ...)` builds correct `SecurityConfig`; missing endpoint → error | `cli.rs` `mod tests` |
| CLI unit | SASL + TLS `SecurityCliInput` → correct `ClientSecurity` (already tested; gate only) | `cli.rs` existing |
| SR integration | JWKS: in-process `axum` server serving a hardcoded RS256 JWKS (using `crabka_security::ca` test key material); assert 200/401 for signed/unsigned/wrong-issuer tokens | `tests/security.rs` new case |
| Operator unit | `kafka_client_sasl_ssl` renders correct args + env + mounts | `tests/reconcile_schema_registry.rs` |
| Operator unit | `issuer_ref` path: pending-cert gate + ready-cert renders Deployment | `tests/reconcile_schema_registry.rs` |
| CI | Existing `kind-schema-registry` e2e passes (smoke test: no regression) | `operator-e2e.yml` |
| CI | New `kind-schema-registry-jwks` e2e: SR with JWKS bearer + self-signed JWKS server in cluster | `operator-e2e.yml` (optional; can defer to follow-up) |

The cert-manager e2e is **optional** in this slice (cert-manager isn't installed in the base kind cluster). The unit/mock tests fully gate the reconciler path. A future slice can add a cert-manager kind job.

---

## File map

| File | Action | Notes |
|---|---|---|
| `crates/schema-registry/src/cli.rs` | Modify | JWKS fields + `build_bearer` jwks case + `SecurityOutput` type |
| `crates/schema-registry/src/bin/schema-registry.rs` | Modify | New jwks flags; refresh task spawn |
| `crates/schema-registry/tests/security.rs` | Modify | JWKS integration test case |
| `crates/operator/src/crd/schema_registry.rs` | Modify | `BearerMode::Jwks`, `BearerAuthn` fields, `SchemaRegistryKafkaClient*`, `CertManagerIssuerRef`, `issuer_ref` on `SchemaRegistryTls` |
| `crates/operator/src/controller/schema_registry.rs` | Modify | Render JWKS/kafka-client args+mounts; cert-manager `apply_certificate_cr` |
| `crates/operator/tests/reconcile_schema_registry.rs` | Modify | 5 new reconciler tests |
| `deploy/crds/crabka.io_schemaregistries.yaml` | Generated | `cargo run -p crabka-operator -- gen-crds deploy/crds` |
| `charts/crabka-operator/templates/clusterrole.yaml` | Modify | Add `certificates.cert-manager.io` rule |

---

## Constraints

- **No backward-compat shims.** `BearerMode::Unsecured` stays as a variant (it's valid); `BearerMode::Jwks` is a new additive variant. The `SchemaRegistrySpec.kafka_client` and `SchemaRegistryTls.issuer_ref` fields are `Option` — absent = today's behavior.
- **CRD YAML is always generated**, never hand-edited.
- **Clippy gate:** `cargo clippy -p crabka-schema-registry --all-targets -D warnings` + `cargo clippy -p crabka-operator --all-targets -D warnings` before each commit.
- **`cargo fmt` before each commit.**
- **Commit identity:** `-c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"` + `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>` trailer.
- **cert-manager kind e2e deferred** to a follow-up slice; the reconciler unit tests are the gate for Part C.
