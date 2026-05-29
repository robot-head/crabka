# Operator GSSAPI (Kerberos) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose SASL/GSSAPI (Kerberos) — already implemented in the broker library — through the broker's TOML config surface and the Kubernetes operator's CRDs, for both client-facing listener auth and inter-broker auth.

**Architecture:** The broker handshake logic (accept + initiate) is done. This work adds (1) broker TOML surface — `[gssapi]` and `[inter_broker_credentials]` blocks in `FileConfig`; (2) operator CRD surface — a `ListenerAuthentication::Gssapi` variant, `Kafka.spec.interBrokerKerberos`, and `Kafka.spec.krb5ConfSecretRef`; (3) reconciliation that renders those TOML blocks, mounts the keytab/krb5.conf Secrets, and validates the config. Every piece mirrors the existing OAUTHBEARER implementation, which is the reference template throughout.

**Tech Stack:** Rust, `serde`/`toml`, `kube-rs` + `schemars` (CRDs), `k8s-openapi`, the workspace's `crabka-security` GSSAPI crate.

**Design spec:** [docs/superpowers/specs/2026-05-29-crabka-operator-gssapi-kerberos-design.md](../specs/2026-05-29-crabka-operator-gssapi-kerberos-design.md)

---

## The TOML contract (shared reference — both sides MUST match exactly)

The broker (Task 1) parses these blocks; the operator (Task 4) renders them. Key names must be byte-identical. The keytab is always mounted at the fixed path `/etc/crabka/gssapi-keytab/keytab`.

```toml
# Broker-global GSSAPI accept config. Emitted when any listener is type:gssapi.
[gssapi]
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
service_name = "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"          # omitted when unset
kdc = "tcp://kdc:88"           # omitted when unset

# Inter-broker initiate credentials. Emitted only when the inter-broker
# listener is type:gssapi. Reuses the same keytab mount.
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
```

`krb5.conf` is **not** a TOML key — when `spec.krb5ConfSecretRef` is set the operator mounts it at `/etc/crabka/krb5/krb5.conf` and sets the `KRB5_CONFIG` env var on the broker container (Task 5).

---

## File structure & responsibilities

| File | Responsibility | Task |
|---|---|---|
| `crates/broker/src/file_config.rs` | `FileGssapiConfig`, `FileInterBrokerCredentials`; `[gssapi]`/`[inter_broker_credentials]` parse + `apply_to` mapping | 1 |
| `crates/operator/src/crd/listener.rs` | `ListenerAuthentication::Gssapi`, `ListenerAuthenticationGssapi`, `KeytabSecretRef`, schema | 2 |
| `crates/operator/src/crd/kafka.rs` | `spec.interBrokerKerberos` (`InterBrokerKerberos`), `spec.krb5ConfSecretRef` (`Krb5ConfSecretRef`) | 2 |
| `crates/operator/src/controller/listeners.rs` | `sasl_mechanism`/`listener_protocol` arms (T2); validation (T3); `[gssapi]`/`[inter_broker_credentials]` render (T4) | 2,3,4 |
| `crates/operator/src/controller/common.rs` | `ReconcileError` variants (T5); `render_broker_toml` call-site (T4) | 4,5 |
| `crates/operator/src/controller/kafka.rs` | keytab/krb5 mount helpers, IB-kerberos extraction, Secret-existence checks, render call-site (T4/T5) | 4,5 |
| `crates/operator/src/controller/kafka_node_pool.rs` | keytab + krb5.conf volumes/mounts, `KRB5_CONFIG` env | 5 |
| `crates/operator/tests/reconcile_listener_gssapi.rs` (new) | operator integration tests | 6 |
| `README.md` | feature/KIP table updates | 7 |

## Execution batches (per CLAUDE.md — parallel where file sets don't overlap)

- **Batch 1 (parallel):** Task 1 (broker, `file_config.rs`) ∥ Task 2 (operator CRD + compile cascade). Different crates/files.
- **Batch 2:** Task 3 (validation, `listeners.rs`) — after T2.
- **Batch 3:** Task 4 (render + call-sites, `listeners.rs`/`common.rs`/`kafka.rs`) — after T3 (shares `listeners.rs`).
- **Batch 4:** Task 5 (mounts/env + Secret-existence, `kafka_node_pool.rs`/`kafka.rs`/`common.rs`) — after T4 (shares `kafka.rs`).
- **Batch 5 (parallel):** Task 6 (integration tests) ∥ Task 7 (README).

> **Why the operator chain is sequential:** Tasks 2–5 all touch `listeners.rs` and/or `kafka.rs` (large multi-responsibility files). They cannot run concurrently without edit conflicts. Task 1 (broker crate) and Task 7 (README) are the only freely parallelizable pieces.

All commands below assume the worktree root as CWD. After every code change run `cargo fmt` before committing (CI gates on `cargo fmt --check`). Commit with the repo's identity override:
`git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit ...`

---

## Task 1: Broker TOML surface — `[gssapi]` + `[inter_broker_credentials]`

**Files:**
- Modify: `crates/broker/src/file_config.rs` (add structs + `FileConfig` fields + `apply_to` mapping; tests in the same file's `#[cfg(test)] mod tests`)

**Reference:** `FileOAuthBearerConfig` (`file_config.rs:299`) and its `apply_to` handling; `BrokerConfig.gssapi` (`config.rs:255`) of type `crabka_security::gssapi::GssapiConfig`; `InterBrokerCredentials::Gssapi` (`config.rs:60`). `GssapiConfig` fields: `keytab_path: PathBuf`, `service_name: String`, `principal_to_local_rules: Vec<name::Rule>`, `realm: Option<String>`, `kdc: Option<String>`. Rule parsing API: `crabka_security::gssapi::name::Rule::parse(&str) -> Result<Rule, NameError>`.

- [ ] **Step 1: Write failing tests for `[gssapi]` parsing + mapping**

Add to the `mod tests` block in `crates/broker/src/file_config.rs`:

```rust
#[test]
fn apply_to_gssapi_maps_all_fields() {
    let src = r#"
broker_id = 1
[gssapi]
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
service_name = "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"
kdc = "tcp://kdc:88"
"#;
    let file: FileConfig = toml::from_str(src).expect("parse [gssapi]");
    let mut cfg = crate::config::BrokerConfig::default();
    file.apply_to(&mut cfg).expect("apply [gssapi]");
    let g = cfg.gssapi.expect("gssapi config present");
    assert_eq!(g.keytab_path, std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"));
    assert_eq!(g.service_name, "kafka");
    assert_eq!(g.principal_to_local_rules.len(), 2);
    assert_eq!(g.realm.as_deref(), Some("EXAMPLE.COM"));
    assert_eq!(g.kdc.as_deref(), Some("tcp://kdc:88"));
}

#[test]
fn apply_to_gssapi_defaults_service_name_to_kafka() {
    let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["DEFAULT"]
"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = crate::config::BrokerConfig::default();
    file.apply_to(&mut cfg).unwrap();
    assert_eq!(cfg.gssapi.unwrap().service_name, "kafka");
}

#[test]
fn apply_to_gssapi_rejects_malformed_rule() {
    let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["NOT_A_RULE:::"]
"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = crate::config::BrokerConfig::default();
    let err = file.apply_to(&mut cfg).unwrap_err();
    assert!(matches!(err, FileConfigError::InvalidConfig(_)));
}

#[test]
fn apply_to_inter_broker_credentials_gssapi() {
    let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = crate::config::BrokerConfig::default();
    file.apply_to(&mut cfg).unwrap();
    match cfg.inter_broker_credentials.expect("ib creds present") {
        crate::config::InterBrokerCredentials::Gssapi {
            keytab_path, client_principal, service_name, kdc_url,
        } => {
            assert_eq!(keytab_path, std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"));
            assert_eq!(client_principal, "kafka@EXAMPLE.COM");
            assert_eq!(service_name, "kafka");
            assert_eq!(kdc_url, "tcp://kdc:88");
        }
        other => panic!("expected Gssapi, got {other:?}"),
    }
}

#[test]
fn apply_to_inter_broker_credentials_rejects_unknown_type() {
    let src = r#"
[inter_broker_credentials]
type = "carrier-pigeon"
"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = crate::config::BrokerConfig::default();
    assert!(file.apply_to(&mut cfg).is_err());
}
```

- [ ] **Step 2: Run the tests; confirm they fail to compile** (the structs/fields don't exist yet)

Run: `cargo test -p crabka-broker --lib file_config::tests::apply_to_gssapi_maps_all_fields 2>&1 | tail -20`
Expected: compile error — `FileConfig` has no field `gssapi` / `inter_broker_credentials`.

- [ ] **Step 3: Add the two `FileConfig` struct fields**

In `crates/broker/src/file_config.rs`, inside `pub struct FileConfig { ... }` (after the `oauthbearer` field ~line 78):

```rust
    /// SASL/GSSAPI (Kerberos) accept-path config. Broker-global —
    /// there is one `[gssapi]` block per broker. Relevant when a listener
    /// enables the `GSSAPI` mechanism.
    #[serde(default)]
    pub gssapi: Option<FileGssapiConfig>,

    /// Credentials this broker uses to authenticate *to* peer brokers
    /// (inter-broker initiate path). Only the `gssapi` variant is supported.
    #[serde(default)]
    pub inter_broker_credentials: Option<FileInterBrokerCredentials>,
```

- [ ] **Step 4: Add the `FileGssapiConfig` and `FileInterBrokerCredentials` structs**

Add near `FileOAuthBearerConfig` (e.g. after it, ~line 426):

```rust
/// TOML shape of `[gssapi]`. Maps to
/// [`crabka_security::gssapi::GssapiConfig`]. `principal_to_local_rules`
/// are parsed into `name::Rule` at `apply_to` time.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileGssapiConfig {
    pub keytab_path: std::path::PathBuf,
    /// `sasl.kerberos.service.name`. Defaults to `"kafka"` when omitted.
    #[serde(default)]
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order (first match wins).
    #[serde(default)]
    pub principal_to_local_rules: Vec<String>,
    #[serde(default)]
    pub realm: Option<String>,
    #[serde(default)]
    pub kdc: Option<String>,
}

/// TOML shape of `[inter_broker_credentials]`. A `type` discriminator
/// selects the variant; only `gssapi` is implemented (PLAIN/SCRAM
/// inter-broker over TOML is intentionally not exposed — see plan §Non-goals).
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FileInterBrokerCredentials {
    Gssapi {
        keytab_path: std::path::PathBuf,
        client_principal: String,
        #[serde(default)]
        service_name: Option<String>,
        kdc_url: String,
    },
}
```

- [ ] **Step 5: Map both blocks in `apply_to`**

In `FileConfig::apply_to` (before the final `Ok(())`), add:

```rust
        if let Some(g) = self.gssapi {
            let mut rules = Vec::with_capacity(g.principal_to_local_rules.len());
            for spec in &g.principal_to_local_rules {
                let rule = crabka_security::gssapi::name::Rule::parse(spec).map_err(|e| {
                    FileConfigError::InvalidConfig(format!(
                        "[gssapi]: invalid principal_to_local rule {spec:?}: {e}"
                    ))
                })?;
                rules.push(rule);
            }
            cfg.gssapi = Some(crabka_security::gssapi::GssapiConfig {
                keytab_path: g.keytab_path,
                service_name: g.service_name.unwrap_or_else(|| "kafka".to_string()),
                principal_to_local_rules: rules,
                realm: g.realm,
                kdc: g.kdc,
            });
        }

        if let Some(ib) = self.inter_broker_credentials {
            cfg.inter_broker_credentials = Some(match ib {
                FileInterBrokerCredentials::Gssapi {
                    keytab_path,
                    client_principal,
                    service_name,
                    kdc_url,
                } => crate::config::InterBrokerCredentials::Gssapi {
                    keytab_path,
                    client_principal,
                    service_name: service_name.unwrap_or_else(|| "kafka".to_string()),
                    kdc_url,
                },
            });
        }
```

> Note: `GssapiConfig` does NOT derive `Default`/`PartialEq` necessarily — the tests above only read fields, so no extra derives are needed on the security type. If `cfg.inter_broker_credentials` doesn't exist as a `BrokerConfig` field, confirm it does at `config.rs:218` (`inter_broker_credentials: Option<InterBrokerCredentials>`) — it does.

- [ ] **Step 6: Run the tests; confirm they pass**

Run: `cargo test -p crabka-broker --lib file_config 2>&1 | tail -25`
Expected: all `apply_to_gssapi_*` and `apply_to_inter_broker_credentials_*` tests PASS.

- [ ] **Step 7: fmt + commit**

```bash
cargo fmt
git add crates/broker/src/file_config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(broker): [gssapi] + [inter_broker_credentials] TOML config surface"
```

---

## Task 2: Operator CRD surface + compile cascade

**Files:**
- Modify: `crates/operator/src/crd/listener.rs` (enum variant, struct, `KeytabSecretRef`, schema)
- Modify: `crates/operator/src/crd/kafka.rs` (`interBrokerKerberos`, `krb5ConfSecretRef`, + 5 `KafkaSpec {` literals in this file)
- Modify: `crates/operator/src/controller/listeners.rs` (`sasl_mechanism`, `listener_protocol` arms)
- Modify: every other file with a `KafkaSpec {` literal (compile cascade — see Step 6)

**Goal of this task:** introduce the full CRD surface and make the operator crate **compile and pass existing tests** with the new variant/fields handled minimally. Behavior (validation, rendering, mounting) lands in Tasks 3–5.

**Reference:** `ListenerAuthenticationOAuth` + `listener_authentication_schema()` (`listener.rs:152`/`:356`); `OauthClientSecretRef` (`listener.rs:344`) for the `{secretName, key}` ref shape.

- [ ] **Step 1: Add `KeytabSecretRef` + `ListenerAuthenticationGssapi` + enum variant**

In `crates/operator/src/crd/listener.rs`, add the variant to `ListenerAuthentication` (after `OAuth(...)`, ~line 144):

```rust
    #[serde(rename = "gssapi")]
    Gssapi(ListenerAuthenticationGssapi),
```

Add the structs (after `OauthClientSecretRef`, ~line 354):

```rust
/// Config for `authentication: { type: gssapi }`. Full parity with the
/// broker's `GssapiConfig`. The reconciler renders these into the
/// broker-global `[gssapi]` TOML block and appends `GSSAPI` to the
/// listener's `sasl_mechanisms`. `[gssapi]` is broker-global, so all
/// GSSAPI listeners on a cluster must agree (validated in T3).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationGssapi {
    /// Secret (same namespace as the `Kafka` CR) holding the service
    /// keytab. Mounted into broker pods at a fixed path via projected items.
    pub keytab_secret_ref: KeytabSecretRef,
    /// `sasl.kerberos.service.name` (the SPN primary). Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order; first match wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm (used when a principal omits its realm).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    /// KDC endpoint (e.g. `tcp://kdc:88`) for the initiate path; falls
    /// back to krb5.conf discovery when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kdc: Option<String>,
}

/// Reference to a Secret (same namespace as the `Kafka` CR) holding a
/// Kerberos keytab. The operator mounts `key` at a fixed in-pod path so
/// the broker reads it regardless of the user's key name.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeytabSecretRef {
    pub secret_name: String,
    pub key: String,
}
```

- [ ] **Step 2: Update `listener_authentication_schema()`**

In the hand-written schema (`listener.rs:356`): add `"gssapi"` to the `type` enum list, and add the GSSAPI sibling-field properties. Change the enum line to:

```rust
                "enum": ["tls", "scram-sha-512", "scram-sha-256", "oauth", "gssapi"],
```

And add these properties inside the `"properties": { ... }` object (alongside the OAuth ones):

```rust
            "keytabSecretRef": {
                "type": "object",
                "required": ["secretName", "key"],
                "properties": {
                    "secretName": { "type": "string", "minLength": 1 },
                    "key":        { "type": "string", "minLength": 1 },
                },
            },
            "serviceName": { "type": "string", "minLength": 1 },
            "principalToLocalRules": {
                "type": "array",
                "items": { "type": "string", "minLength": 1 },
            },
            "realm": { "type": "string", "minLength": 1 },
            "kdc": { "type": "string", "minLength": 1 },
```

> The `schema_with` workaround means schemars never derives from the struct — the JSON above is the source of truth for the CRD. There is no separate "serviceName" collision with OAuth fields; reused names (none here) would just share a property definition.

- [ ] **Step 3: Add `InterBrokerKerberos` + `Krb5ConfSecretRef` to `KafkaSpec`**

In `crates/operator/src/crd/kafka.rs`, add two fields to `KafkaSpec` (after `tiered_storage`, before `tracing`; keep them `Option` + `skip_serializing_if`):

```rust
    /// Inter-broker Kerberos initiate config. Required when
    /// `interBrokerListenerName` resolves to a `type: gssapi` listener;
    /// supplies the shared client principal + KDC. The keytab is reused
    /// from that listener's `keytabSecretRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_kerberos: Option<InterBrokerKerberos>,

    /// Optional process-wide `krb5.conf`. Mounted into broker pods and
    /// pointed at via `KRB5_CONFIG`; serves both accept and initiate paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub krb5_conf_secret_ref: Option<Krb5ConfSecretRef>,
```

Add the structs (near the other spec sub-structs in `kafka.rs`):

```rust
/// Inter-broker GSSAPI initiate config. Single shared client principal
/// cluster-wide (no per-broker host-templated SPNs).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InterBrokerKerberos {
    /// Principal every broker authenticates as when dialing peers, e.g.
    /// `kafka@EXAMPLE.COM`. Must exist in the shared keytab.
    pub client_principal: String,
    /// Target SPN primary. Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// KDC endpoint, e.g. `tcp://kdc:88`.
    pub kdc_url: String,
}

/// Reference to a Secret holding a `krb5.conf`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Krb5ConfSecretRef {
    pub secret_name: String,
    pub key: String,
}
```

> If `crd/listener.rs` types need re-exporting (`crate::crd::ListenerAuthenticationGssapi`, `KeytabSecretRef`), add them to the `pub use` block in `crates/operator/src/crd/mod.rs` alongside `ListenerAuthenticationOAuth` (grep `pub use` in that file and mirror).

- [ ] **Step 4: Handle the new listener variant in the two exhaustive matches**

In `crates/operator/src/controller/listeners.rs`:

`sasl_mechanism` (line 33) — add arm:
```rust
        ListenerAuthentication::Gssapi(_) => Some(SaslMechanism::Gssapi),
```

`listener_protocol` (line 19) — extend the `import` and arms so GSSAPI behaves like SCRAM/OAuth:
```rust
    use ListenerAuthentication::{Gssapi, OAuth, ScramSha256, ScramSha512, Tls};
    match (l.tls, &l.authentication) {
        (false, None) => ListenerProtocol::Plaintext,
        (true, None | Some(Tls)) => ListenerProtocol::Ssl,
        (false, Some(ScramSha512 | ScramSha256 | OAuth(_) | Gssapi(_))) => {
            ListenerProtocol::SaslPlaintext
        }
        (true, Some(ScramSha512 | ScramSha256 | OAuth(_) | Gssapi(_))) => ListenerProtocol::SaslSsl,
        (false, Some(Tls)) => unreachable!(
            "validation rejects mTLS without transport TLS; saw listener '{}'",
            l.name
        ),
    }
```

- [ ] **Step 5: Build the operator crate; let the compiler list every broken `KafkaSpec {` literal**

Run: `cargo build -p crabka-operator 2>&1 | grep -E "missing field|error\[" | sort -u`
Expected: errors of the form `missing fields `inter_broker_kerberos` and `krb5_conf_secret_ref` in initializer of `KafkaSpec``, one per literal.

- [ ] **Step 6: Add the two new fields to every `KafkaSpec {` literal in src**

For each `KafkaSpec { ... }` literal the compiler flagged in `crates/operator/src`, add:
```rust
            inter_broker_kerberos: None,
            krb5_conf_secret_ref: None,
```
Sites (confirm via `grep -rn "KafkaSpec {" crates/operator/src`): `crd/kafka.rs` (5), `controller/common.rs` (7), `controller/kafka.rs` (1), `controller/kafka_node_pool.rs` (1), `controller/listeners.rs` (1), `controller/metrics.rs` (1), `controller/network_policy.rs` (1), `controller/topic.rs` (1).

Run: `cargo build -p crabka-operator 2>&1 | tail -5`
Expected: builds clean (warnings ok).

- [ ] **Step 7: Add the two new fields to every `KafkaSpec {` literal in tests**

Run `cargo test -p crabka-operator --no-run 2>&1 | grep -E "missing field" | sort -u` and add the same two `None` fields to each flagged test literal. Sites: `tests/reconcile_ca.rs` (2), `reconcile_ca_rotation.rs`, `reconcile_inter_broker_mtls.rs`, `reconcile_kafka.rs` (6), `reconcile_kafka_authorization.rs`, `reconcile_listener_auth.rs`, `reconcile_listener_ingress.rs`, `reconcile_listener_oauth.rs`, `reconcile_oauth_introspection.rs`, `reconcile_oauth_trust.rs`. Also the shared helper at `tests/reconcile_listener_auth.rs:kafka_cr_with_listeners` and any `tests/shared/mod.rs` builder (`grep -rn "KafkaSpec {" crates/operator/tests`).

Run: `cargo test -p crabka-operator --no-run 2>&1 | tail -5`
Expected: compiles.

- [ ] **Step 8: Run the full operator test suite to confirm no regression**

Run: `cargo test -p crabka-operator 2>&1 | tail -15`
Expected: all existing tests PASS (GSSAPI behavior not yet exercised).

- [ ] **Step 9: Regenerate CRD YAML if the repo commits it**

Check: `grep -rln "scram-sha-512" crates/operator --include=*.yaml` or look for a CRD-dump xtask (`grep -rn "crd" crates/operator/src/bin 2>/dev/null; ls crates/operator`). If a committed CRD manifest or a `cargo run --bin ...crd` generator exists, regenerate it so the `gssapi` enum value appears. If not, skip.

- [ ] **Step 10: fmt + commit**

```bash
cargo fmt
git add crates/operator
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(operator): GSSAPI CRD surface (listener variant, interBrokerKerberos, krb5ConfSecretRef)"
```

---

## Task 3: Operator validation (`ListenersValid`)

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` (`ValidationError` enum + `reason()`/`message()` + `validate_listeners` + a `gssapi_canonical` helper + unit tests in the same file)

**Reference:** the OAuth validation block (`listeners.rs:237`), `oauth_canonical` (`:203`) + `ConflictingOAuthListenerConfig` (`:100`/`:135`/`:185`), and the `ValidationError` enum (`:53`). `validate_listeners(listeners, inter_broker_listener_name)` signature at `:214`.

- [ ] **Step 1: Write failing validation unit tests**

Add to the `mod tests` in `listeners.rs` (use the existing test helpers in that module for building `Listener`s; mirror nearby OAuth tests):

```rust
#[test]
fn gssapi_listener_without_keytab_is_invalid() {
    // keytabSecretRef.secretName empty
    let g = crate::crd::ListenerAuthenticationGssapi {
        keytab_secret_ref: crate::crd::KeytabSecretRef { secret_name: String::new(), key: "k".into() },
        service_name: None, principal_to_local_rules: vec![], realm: None, kdc: None,
    };
    let l = test_listener_internal("gss", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    assert_eq!(
        validate_listeners(&[l], None).unwrap_err().reason(),
        "ListenerGssapiKeytabSecretMissing"
    );
}

#[test]
fn gssapi_listener_with_bad_rule_is_invalid() {
    let g = gssapi_cfg_with_rules(vec!["NOT_A_RULE:::".into()]);
    let l = test_listener_internal("gss", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    assert_eq!(
        validate_listeners(&[l], None).unwrap_err().reason(),
        "ListenerGssapiInvalidRule"
    );
}

#[test]
fn divergent_gssapi_listeners_conflict() {
    let a = gssapi_cfg_with_service("kafka");
    let b = gssapi_cfg_with_service("other");
    let la = test_listener_internal("g1", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(a)));
    let lb = test_listener_internal("g2", 9093, false, Some(crate::crd::ListenerAuthentication::Gssapi(b)));
    assert_eq!(
        validate_listeners(&[la, lb], None).unwrap_err().reason(),
        "ListenerGssapiConfigConflict"
    );
}

#[test]
fn gssapi_listener_allows_plaintext_and_ssl() {
    // GSSAPI brings its own RFC 4752 security layer — TLS not required.
    let g = gssapi_cfg_with_service("kafka");
    let l = test_listener_internal("g", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    validate_listeners(&[l], None).expect("plaintext+gssapi is valid");
}
```

Add small local test builders next to the tests (or inline the structs):
```rust
#[cfg(test)]
fn gssapi_cfg_with_service(svc: &str) -> crate::crd::ListenerAuthenticationGssapi {
    crate::crd::ListenerAuthenticationGssapi {
        keytab_secret_ref: crate::crd::KeytabSecretRef { secret_name: "kt".into(), key: "keytab".into() },
        service_name: Some(svc.into()),
        principal_to_local_rules: vec!["DEFAULT".into()],
        realm: None, kdc: None,
    }
}
#[cfg(test)]
fn gssapi_cfg_with_rules(rules: Vec<String>) -> crate::crd::ListenerAuthenticationGssapi {
    let mut c = gssapi_cfg_with_service("kafka");
    c.principal_to_local_rules = rules;
    c
}
```
> If a `test_listener_internal(name, port, tls, auth)` helper doesn't already exist in this module's tests, reuse whatever the nearby OAuth tests use to build a `Listener` (grep for `fn ` in the test module). Match the existing pattern rather than inventing a new one.

- [ ] **Step 2: Run; confirm failure**

Run: `cargo test -p crabka-operator --lib controller::listeners::tests::gssapi 2>&1 | tail -20`
Expected: compile error (no `ListenerGssapi*` variants) or assertion failure.

- [ ] **Step 3: Add `ValidationError` variants + `reason()` + `message()`**

In the `ValidationError` enum (`listeners.rs:53`):
```rust
    /// `type: gssapi` listener missing `keytabSecretRef` (secretName/key).
    ListenerGssapiKeytabSecretMissing(String),
    /// A `principalToLocalRules` entry failed `auth_to_local` parsing.
    /// String carries listener name + the offending rule.
    ListenerGssapiInvalidRule(String),
    /// Two or more GSSAPI listeners declare differing config. The broker
    /// `[gssapi]` block is broker-global, so divergence isn't representable.
    ConflictingGssapiListenerConfig,
```

In `reason()`:
```rust
            Self::ListenerGssapiKeytabSecretMissing(_) => "ListenerGssapiKeytabSecretMissing",
            Self::ListenerGssapiInvalidRule(_) => "ListenerGssapiInvalidRule",
            Self::ConflictingGssapiListenerConfig => "ListenerGssapiConfigConflict",
```

In `message()`:
```rust
            Self::ListenerGssapiKeytabSecretMissing(n) => {
                format!("listener '{n}': authentication.type=gssapi requires keytabSecretRef.secretName and .key")
            }
            Self::ListenerGssapiInvalidRule(msg) => msg.clone(),
            Self::ConflictingGssapiListenerConfig => {
                "all GSSAPI listeners must share identical config (the broker [gssapi] block is broker-global)".to_string()
            }
```

- [ ] **Step 4: Add the `gssapi_canonical` helper + validation logic**

Add near `oauth_canonical` (`:203`):
```rust
/// Canonical form for cross-listener GSSAPI conflict detection. The broker
/// `[gssapi]` block is broker-global, so every field must agree across
/// GSSAPI listeners. Compare the whole struct.
#[must_use]
fn gssapi_canonical(cfg: &crate::crd::ListenerAuthenticationGssapi) -> crate::crd::ListenerAuthenticationGssapi {
    cfg.clone()
}
```

In `validate_listeners`, inside the per-listener loop (alongside the OAuth `if let`), add:
```rust
        if let Some(ListenerAuthentication::Gssapi(cfg)) = &l.authentication {
            if cfg.keytab_secret_ref.secret_name.is_empty() || cfg.keytab_secret_ref.key.is_empty() {
                return Err(ValidationError::ListenerGssapiKeytabSecretMissing(l.name.clone()));
            }
            for spec in &cfg.principal_to_local_rules {
                if crabka_security::gssapi::name::Rule::parse(spec).is_err() {
                    return Err(ValidationError::ListenerGssapiInvalidRule(format!(
                        "listener '{}': invalid principalToLocalRules entry {spec:?}",
                        l.name
                    )));
                }
            }
        }
```

After the per-listener loop (mirroring the OAuth conflict pass that dedups `oauth_canonical` results), add a GSSAPI conflict check:
```rust
    // Broker-global [gssapi] block: all GSSAPI listeners must agree.
    let mut gssapi_canon: Option<crate::crd::ListenerAuthenticationGssapi> = None;
    for l in listeners {
        if let Some(ListenerAuthentication::Gssapi(cfg)) = &l.authentication {
            let canon = gssapi_canonical(cfg);
            match &gssapi_canon {
                None => gssapi_canon = Some(canon),
                Some(prev) if *prev != canon => {
                    return Err(ValidationError::ConflictingGssapiListenerConfig);
                }
                Some(_) => {}
            }
        }
    }
```
> Add `use crate::crd::ListenerAuthentication;` items as needed — the module already imports `ListenerAuthentication`. `ListenerAuthenticationGssapi`/`KeytabSecretRef` come via `crate::crd::` (re-exported in Task 2 Step 3).

- [ ] **Step 5: Add the "inter-broker GSSAPI requires interBrokerKerberos" validator**

`validate_listeners` is listener-only (and called from ~40 sites), so add a small standalone validator instead of growing its signature. It needs the resolved inter-broker listener + `spec.interBrokerKerberos`. Add the `ValidationError` variant:

```rust
    /// The inter-broker listener is `type: gssapi` but
    /// `spec.interBrokerKerberos` is absent — brokers would have no
    /// client principal/KDC to initiate with.
    InterBrokerGssapiRequiresKerberosConfig(String),
```
`reason()`: `Self::InterBrokerGssapiRequiresKerberosConfig(_) => "InterBrokerGssapiRequiresKerberosConfig",`
`message()`:
```rust
            Self::InterBrokerGssapiRequiresKerberosConfig(n) => format!(
                "interBrokerListenerName='{n}' is type=gssapi but spec.interBrokerKerberos is not set"
            ),
```
The validator (takes a bool so it stays decoupled from the spec type):
```rust
/// When the resolved inter-broker listener uses GSSAPI, `spec.interBrokerKerberos`
/// must be present. `ib_kerberos_present` is `spec.inter_broker_kerberos.is_some()`.
#[allow(dead_code)]
pub fn validate_inter_broker_gssapi(
    listeners: &[Listener],
    inter_broker_listener_name: &str,
    ib_kerberos_present: bool,
) -> Result<(), ValidationError> {
    let ib_is_gssapi = listeners.iter().any(|l| {
        l.name == inter_broker_listener_name
            && matches!(l.authentication, Some(ListenerAuthentication::Gssapi(_)))
    });
    if ib_is_gssapi && !ib_kerberos_present {
        return Err(ValidationError::InterBrokerGssapiRequiresKerberosConfig(
            inter_broker_listener_name.to_string(),
        ));
    }
    Ok(())
}
```
Add a unit test:
```rust
#[test]
fn inter_broker_gssapi_without_kerberos_config_is_invalid() {
    let g = gssapi_cfg_with_service("kafka");
    let l = test_listener_internal("ib", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    assert_eq!(
        validate_inter_broker_gssapi(&[l.clone()], "ib", false).unwrap_err().reason(),
        "InterBrokerGssapiRequiresKerberosConfig"
    );
    validate_inter_broker_gssapi(&[l], "ib", true).expect("ok when interBrokerKerberos present");
}
```
> This validator is *wired into the reconcile path in Task 5 Step 5* (right where `validate_listeners` is already called at `kafka.rs:729`), since that's where `spec.inter_broker_kerberos` is in scope.

- [ ] **Step 6: Run the new tests + the full validation suite**

Run: `cargo test -p crabka-operator --lib controller::listeners::tests 2>&1 | tail -20`
Expected: new `gssapi_*` tests (incl. `inter_broker_gssapi_without_kerberos_config_is_invalid`) PASS; all prior validation tests still PASS.

- [ ] **Step 7: fmt + commit**

```bash
cargo fmt
git add crates/operator/src/controller/listeners.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(operator): validate GSSAPI listeners (keytab, rule parse, conflict, inter-broker kerberos required)"
```

---

## Task 4: Render `[gssapi]` + `[inter_broker_credentials]` TOML

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` (`render_broker_toml`: append `GSSAPI` to `sasl_mechanisms`, emit the two blocks, add one param; update its in-file test call-sites)
- Modify: `crates/operator/src/controller/common.rs` (call-site at `:317`)
- Modify: `crates/operator/src/controller/kafka.rs` (no render call-site there for the live path — verify; the live render call is in `common.rs`)

**Reference:** the `[oauthbearer]` render block (`listeners.rs:3008`), the `sasl_config` emission (`:2863`), `toml_string_array` (`:2775`).

The `[gssapi]` block is derived entirely from the listeners (like OAuth — no new param). The `[inter_broker_credentials]` block needs `spec.interBrokerKerberos`, so `render_broker_toml` gains exactly **one** new parameter: `inter_broker_kerberos: Option<&crate::crd::kafka::InterBrokerKerberos>`.

- [ ] **Step 1: Write failing render unit tests**

Add to `listeners.rs` `mod tests` (mirror the OAuth render tests' setup; the GSSAPI keytab mount path constant is `/etc/crabka/gssapi-keytab/keytab`):

```rust
#[test]
fn render_emits_gssapi_block_and_appends_mechanism() {
    let g = gssapi_cfg_with_service("kafka"); // helper from Task 3
    let l = test_listener_internal("gss", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    let addrs = single_addr("gss", "gss.svc", 9092); // mirror existing addr helper
    let toml = render_broker_toml(
        0, &[l], &addrs, "gss", &Default::default(), None, None, false, None, None, None,
    );
    assert!(toml.contains("[gssapi]"), "toml:\n{toml}");
    assert!(toml.contains(r#"keytab_path = "/etc/crabka/gssapi-keytab/keytab""#));
    assert!(toml.contains(r#"service_name = "kafka""#));
    assert!(toml.contains(r#"principal_to_local_rules = ["DEFAULT"]"#));
    // listener's mechanism list carries GSSAPI
    assert!(toml.contains(r#"enabled_mechanisms = ["GSSAPI"]"#));
    // no inter-broker block without interBrokerKerberos
    assert!(!toml.contains("[inter_broker_credentials]"));
}

#[test]
fn render_emits_inter_broker_credentials_when_ib_listener_is_gssapi() {
    let g = gssapi_cfg_with_service("kafka");
    let l = test_listener_internal("gss", 9092, false, Some(crate::crd::ListenerAuthentication::Gssapi(g)));
    let addrs = single_addr("gss", "gss.svc", 9092);
    let ibk = crate::crd::kafka::InterBrokerKerberos {
        client_principal: "kafka@EXAMPLE.COM".into(),
        service_name: Some("kafka".into()),
        kdc_url: "tcp://kdc:88".into(),
    };
    let toml = render_broker_toml(
        0, &[l], &addrs, "gss", &Default::default(), None, None, false, None, None, Some(&ibk),
    );
    assert!(toml.contains("[inter_broker_credentials]"), "toml:\n{toml}");
    assert!(toml.contains(r#"type = "gssapi""#));
    assert!(toml.contains(r#"client_principal = "kafka@EXAMPLE.COM""#));
    assert!(toml.contains(r#"kdc_url = "tcp://kdc:88""#));
}
```
> Match `single_addr`/`test_listener_internal` to whatever the existing OAuth render tests use (grep the test module). The point is the assertions on emitted TOML.

- [ ] **Step 2: Run; confirm failure** (arity mismatch — `render_broker_toml` takes 10 args, tests pass 11)

Run: `cargo test -p crabka-operator --lib controller::listeners::tests::render_emits_gssapi 2>&1 | tail -15`
Expected: compile error on arg count.

- [ ] **Step 3: Add the parameter + append `GSSAPI` mechanism + emit blocks**

Add the param to `render_broker_toml` (after `tiered_storage`):
```rust
    inter_broker_kerberos: Option<&crate::crd::kafka::InterBrokerKerberos>,
```

The `sasl_config` line already emits `mech.wire_name()` for any mechanism returned by `sasl_mechanism` — which now returns `GSSAPI` (Task 2). So `enabled_mechanisms = ["GSSAPI"]` is emitted automatically; no change needed at `:2863`. (Verify in the test.)

Add the broker-global `[gssapi]` block after the `[oauthbearer]` block (~after `:3108`). Derive it from the first GSSAPI listener (validation guarantees agreement):
```rust
    // Broker-global [gssapi] block. Emitted when any listener is type:gssapi.
    // Per-listener divergence is rejected by validate_listeners, so the first
    // GSSAPI listener's config is unambiguous here. Keytab is mounted at a
    // fixed path by kafka_node_pool.rs.
    if let Some(g) = listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::Gssapi(c)) => Some(c),
        _ => None,
    }) {
        let _ = writeln!(out, "[gssapi]");
        let _ = writeln!(out, r#"keytab_path = "/etc/crabka/gssapi-keytab/keytab""#);
        let svc = g.service_name.as_deref().unwrap_or("kafka");
        let _ = writeln!(out, "service_name = \"{svc}\"");
        let _ = writeln!(
            out,
            "principal_to_local_rules = {}",
            toml_string_array(&g.principal_to_local_rules)
        );
        if let Some(realm) = &g.realm {
            let _ = writeln!(out, "realm = \"{realm}\"");
        }
        if let Some(kdc) = &g.kdc {
            let _ = writeln!(out, "kdc = \"{kdc}\"");
        }
        out.push('\n');
    }

    // Inter-broker initiate credentials. Emitted only when the inter-broker
    // listener is type:gssapi AND spec.interBrokerKerberos is provided
    // (validate_listeners + kafka.rs guarantee both when we reach here).
    let ib_is_gssapi = listeners.iter().any(|l| {
        l.name == inter_broker_listener_name
            && matches!(l.authentication, Some(ListenerAuthentication::Gssapi(_)))
    });
    if ib_is_gssapi
        && let Some(ibk) = inter_broker_kerberos
    {
        let _ = writeln!(out, "[inter_broker_credentials]");
        let _ = writeln!(out, r#"type = "gssapi""#);
        let _ = writeln!(out, r#"keytab_path = "/etc/crabka/gssapi-keytab/keytab""#);
        let _ = writeln!(out, "client_principal = \"{}\"", ibk.client_principal);
        let svc = ibk.service_name.as_deref().unwrap_or("kafka");
        let _ = writeln!(out, "service_name = \"{svc}\"");
        let _ = writeln!(out, "kdc_url = \"{}\"", ibk.kdc_url);
        out.push('\n');
    }
```
> The keytab mount path appears twice — extract a `const GSSAPI_KEYTAB_PATH: &str = "/etc/crabka/gssapi-keytab/keytab";` near `TIER_STORAGE_PATH` in `listeners.rs` and use it in both places (and Task 5 reuses it for the volumeMount path's `<mount>/keytab`).

- [ ] **Step 4: Update `render_broker_toml`'s in-file test call-sites to pass the new arg**

Every `render_broker_toml(...)` call in `listeners.rs` tests (≈40, see `grep -n "render_broker_toml(" crates/operator/src/controller/listeners.rs`) needs a trailing `None` (or `Some(&ibk)` for the two new tests). For multi-line calls add `None,` as the last arg before `)`; for the one-line calls at `:3184`/`:3185` append `, None`.

Run: `cargo build -p crabka-operator --tests 2>&1 | grep -E "this function takes|arguments" | head`
Expected: only the genuinely-missed call-sites remain; fix until clean.

- [ ] **Step 5: Update the live call-site in `common.rs`**

`crates/operator/src/controller/common.rs:317` — thread the spec field. Just above the call (near `let tiered_storage = ...`):
```rust
    let inter_broker_kerberos = owner.spec.inter_broker_kerberos.as_ref();
```
Add `inter_broker_kerberos,` as the final argument to the `render_broker_toml(...)` call.

- [ ] **Step 6: Run render tests + full build**

Run: `cargo test -p crabka-operator --lib controller::listeners::tests::render 2>&1 | tail -20`
Expected: new render tests PASS; existing render tests PASS.
Run: `cargo build -p crabka-operator --tests 2>&1 | tail -3` → clean.

- [ ] **Step 7: fmt + commit**

```bash
cargo fmt
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/common.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(operator): render [gssapi] + [inter_broker_credentials] broker TOML"
```

---

## Task 5: Mount keytab + krb5.conf; validate Secret existence

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs` (mount-info helpers + Secret-existence checks + thread mounts into the pod render)
- Modify: `crates/operator/src/controller/kafka_node_pool.rs` (`render_broker_container` + `render_storage` params; keytab + krb5.conf volumes/mounts; `KRB5_CONFIG` env)
- Modify: `crates/operator/src/controller/common.rs` (`ReconcileError` variants)

**Reference:** the OAuth introspection Secret path end-to-end — `oauth_introspection_secret_mount` + `OauthIntrospectionMount` (`kafka.rs:417`/`:433`), `reconcile_oauth_introspection_secret` existence check (`kafka.rs:527`), the projected-items volume (`kafka_node_pool.rs:644`), the volumeMount (`:447`), and `ReconcileError::MissingOauthIntrospectionSecret` (`common.rs:84`). The keytab follows the *introspection* pattern (user Secret, projected items, fixed path); krb5.conf follows the same shape with its own path.

- [ ] **Step 1: Write failing integration-style unit tests in `kafka.rs`**

Add to `kafka.rs` `mod tests` (mirror nearby `oauth_introspection_secret_mount` tests):
```rust
#[test]
fn gssapi_keytab_mount_extracted_from_listener() {
    let g = crate::crd::ListenerAuthenticationGssapi {
        keytab_secret_ref: crate::crd::KeytabSecretRef { secret_name: "kt".into(), key: "krb5.keytab".into() },
        service_name: None, principal_to_local_rules: vec!["DEFAULT".into()], realm: None, kdc: None,
    };
    let k = kafka_with_listeners(vec![
        listener_with_auth("gss", Some(crate::crd::ListenerAuthentication::Gssapi(g)))
    ]);
    let mount = gssapi_keytab_mount(&k).expect("keytab mount present");
    assert_eq!(mount.secret_name, "kt");
    assert_eq!(mount.key, "krb5.keytab");
}

#[test]
fn no_keytab_mount_without_gssapi_listener() {
    let k = kafka_with_listeners(vec![listener_with_auth("plain", None)]);
    assert!(gssapi_keytab_mount(&k).is_none());
}
```
> Reuse the test helpers already present in `kafka.rs` tests (grep `fn kafka_with_listeners`/`fn listener_with_auth`; the OAuth tests around `:1851` use similar builders — match them).

- [ ] **Step 2: Run; confirm failure** (`gssapi_keytab_mount` undefined)

Run: `cargo test -p crabka-operator --lib controller::kafka::tests::gssapi_keytab_mount 2>&1 | tail -15`
Expected: unresolved name.

- [ ] **Step 3: Add the mount-info helpers in `kafka.rs`**

```rust
/// In-pod mount info for the GSSAPI keytab. `key` is the user's source
/// key; mounted via projected items to a fixed path so the broker reads
/// `/etc/crabka/gssapi-keytab/keytab` regardless of key name.
pub(crate) struct GssapiKeytabMount {
    pub secret_name: String,
    pub key: String,
}

/// The keytab Secret ref from the (first) GSSAPI listener, or `None` when
/// no listener is `type: gssapi`. Validation guarantees all GSSAPI
/// listeners agree, so the first is canonical.
pub(crate) fn gssapi_keytab_mount(kafka: &Kafka) -> Option<GssapiKeytabMount> {
    kafka.spec.listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::Gssapi(c)) => Some(GssapiKeytabMount {
            secret_name: c.keytab_secret_ref.secret_name.clone(),
            key: c.keytab_secret_ref.key.clone(),
        }),
        _ => None,
    })
}

/// krb5.conf Secret ref, when `spec.krb5ConfSecretRef` is set.
pub(crate) fn krb5_conf_mount(kafka: &Kafka) -> Option<(String, String)> {
    kafka.spec.krb5_conf_secret_ref.as_ref().map(|r| (r.secret_name.clone(), r.key.clone()))
}
```

- [ ] **Step 4: Add `ReconcileError` variants in `common.rs`**

In `ReconcileError` (`common.rs:38`), next to the OAuth-secret variants:
```rust
    /// `type: gssapi` listener references a keytab Secret that doesn't exist.
    MissingGssapiKeytabSecret(String),
    /// keytab Secret exists but lacks the referenced key.
    MissingGssapiKeytabKey { secret: String, key: String },
    /// `spec.krb5ConfSecretRef` references a Secret/key that doesn't exist.
    MissingKrb5ConfSecret(String),
```
Add `Display`/`reason` arms to match however the other `Missing*Secret` variants are surfaced (grep `MissingOauthIntrospectionSecret` across `common.rs` and mirror every match it appears in — `Display`, any `reason()`/condition mapping).

- [ ] **Step 5: Add Secret-existence reconcile check in the reconcile path**

In `reconcile` (or wherever `reconcile_oauth_introspection_secret` is awaited in `kafka.rs`), add an async check that mirrors it: when `gssapi_keytab_mount(kafka)` is `Some`, `secret_api.get_opt(&secret_name)` → `MissingGssapiKeytabSecret` if absent; verify the `key` is present in the Secret's `data` → `MissingGssapiKeytabKey`. Do the same for `krb5_conf_mount`. Mirror the exact `get_opt(...).await?.ok_or_else(...)` shape used at `kafka.rs:544`.

```rust
    if let Some(m) = gssapi_keytab_mount(obj) {
        let secret = secret_api
            .get_opt(&m.secret_name)
            .await
            .map_err(ReconcileError::Kube)?
            .ok_or_else(|| ReconcileError::MissingGssapiKeytabSecret(m.secret_name.clone()))?;
        let has_key = secret.data.as_ref().is_some_and(|d| d.contains_key(&m.key))
            || secret.string_data.as_ref().is_some_and(|d| d.contains_key(&m.key));
        if !has_key {
            return Err(ReconcileError::MissingGssapiKeytabKey { secret: m.secret_name, key: m.key });
        }
    }
    if let Some((name, key)) = krb5_conf_mount(obj) {
        let secret = secret_api.get_opt(&name).await.map_err(ReconcileError::Kube)?
            .ok_or_else(|| ReconcileError::MissingKrb5ConfSecret(name.clone()))?;
        let has_key = secret.data.as_ref().is_some_and(|d| d.contains_key(&key))
            || secret.string_data.as_ref().is_some_and(|d| d.contains_key(&key));
        if !has_key {
            return Err(ReconcileError::MissingKrb5ConfSecret(name));
        }
    }
```
> Match the real variable names (`obj`/`kafka`, the `secret_api` handle, and the `ReconcileError::Kube` wrapper) to what `reconcile_oauth_introspection_secret`'s caller uses. Place this check before the ConfigMap/StatefulSet render so a missing Secret fails the reconcile early, exactly like OAuth.

Also wire the Task 3 Step 5 validator here. Right after the existing `validate_listeners(...)` call (`kafka.rs:729`), add:
```rust
    crate::controller::listeners::validate_inter_broker_gssapi(
        &obj.spec.listeners,
        &inter_broker_name, // the already-resolved effective inter-broker listener name
        obj.spec.inter_broker_kerberos.is_some(),
    )
    .map_err(/* same condition-surfacing path validate_listeners' result uses */)?;
```
Match how the `validate_listeners` result is currently mapped into the `ListenersValid` condition / `ReconcileError` and reuse that exact path for this call. Use whatever local already holds the resolved inter-broker listener name (the value passed to `render_broker_toml`).

- [ ] **Step 6: Thread mounts into the pod render (kafka_node_pool.rs)**

Add params to `render_broker_container` (`:257`) and `render_storage` (`:532`):
```rust
    gssapi_keytab_secret: Option<&str>,   // source Secret name; key handled via items
    gssapi_keytab_key: Option<&str>,
    krb5_conf_secret: Option<&str>,
    krb5_conf_key: Option<&str>,
```
(or pass small structs — match the style of the OAuth params already there: `oauth_introspection_mount_path: Option<&str>` for the container, `oauth_introspection_mount: Option<&OauthIntrospectionMount>` for storage. Prefer reusing `GssapiKeytabMount`-style refs.)

In `render_broker_container`, append volumeMounts (after the OAuth introspection mount block ~`:447`):
```rust
    if gssapi_keytab_secret.is_some() {
        volume_mounts.push(json!({
            "name": "gssapi-keytab",
            "mountPath": "/etc/crabka/gssapi-keytab",
            "readOnly": true,
        }));
    }
    if krb5_conf_secret.is_some() {
        volume_mounts.push(json!({
            "name": "krb5-conf",
            "mountPath": "/etc/crabka/krb5",
            "readOnly": true,
        }));
        env.push(json!({ "name": "KRB5_CONFIG", "value": "/etc/crabka/krb5/krb5.conf" }));
    }
```

In `render_storage`, append projected-items volumes (after the OAuth introspection volume ~`:644`):
```rust
    if let (Some(secret), Some(key)) = (gssapi_keytab_secret, gssapi_keytab_key) {
        volumes.as_array_mut().expect("volumes is array").push(json!({
            "name": "gssapi-keytab",
            "secret": {
                "secretName": secret,
                "items": [{ "key": key, "path": "keytab" }],
                "defaultMode": 0o400_i32,
            }
        }));
    }
    if let (Some(secret), Some(key)) = (krb5_conf_secret, krb5_conf_key) {
        volumes.as_array_mut().expect("volumes is array").push(json!({
            "name": "krb5-conf",
            "secret": {
                "secretName": secret,
                "items": [{ "key": key, "path": "krb5.conf" }],
                "defaultMode": 0o400_i32,
            }
        }));
    }
```
Update the two private call-sites (`render_broker_container` at `:804`, `render_storage` at `:906`) to pass the new values, threading them from the `Kafka`/spec available in `render_statefulset` (compute via `gssapi_keytab_mount` / `krb5_conf_mount` from `kafka.rs`, or inline the same extraction). Pass `None` where unset.

- [ ] **Step 7: Write a node-pool render unit test**

Add to `kafka_node_pool.rs` `mod tests` (mirror the OAuth introspection mount test ~`:2600`):
```rust
#[test]
fn gssapi_keytab_volume_and_mount_present() {
    // Build a statefulset for a Kafka with a gssapi listener + keytab Secret,
    // then assert the pod spec carries the gssapi-keytab volume (projected
    // items key->"keytab") and the container volumeMount at
    // /etc/crabka/gssapi-keytab.
    // ... use the existing statefulset-render test harness in this module ...
}
```
> Follow the exact harness the OAuth introspection test uses (it builds a `Kafka`, calls the statefulset render, and walks the JSON). Assert: a volume named `gssapi-keytab` with `secret.items[0].path == "keytab"`, and a volumeMount `mountPath == "/etc/crabka/gssapi-keytab"`. Add an analogous krb5.conf assertion gated on `krb5ConfSecretRef`.

- [ ] **Step 8: Run kafka.rs + node_pool tests + full operator suite**

Run: `cargo test -p crabka-operator 2>&1 | tail -20`
Expected: new mount/existence tests PASS; entire suite green.

- [ ] **Step 9: fmt + commit**

```bash
cargo fmt
git add crates/operator/src/controller/kafka.rs crates/operator/src/controller/kafka_node_pool.rs crates/operator/src/controller/common.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(operator): mount GSSAPI keytab + krb5.conf, validate Secret existence"
```

---

## Task 6: End-to-end operator integration tests

**Files:**
- Create: `crates/operator/tests/reconcile_listener_gssapi.rs`

**Reference:** `crates/operator/tests/reconcile_listener_oauth.rs` (closest analogue — full reconcile with mocked kube API) and `reconcile_listener_auth.rs` (SCRAM/mTLS). Use `tests/shared/mod.rs` helpers (`build_ctx`, `fake_kafka_body`, `fake_secret_body`, `happy_path_rules`, `json_response`, etc.).

- [ ] **Step 1: Write the integration tests**

Create `crates/operator/tests/reconcile_listener_gssapi.rs` covering:
1. **Happy path (client listener):** a `Kafka` with a `type: gssapi` internal listener + an existing keytab Secret reconciles successfully; the rendered `broker-N.toml` ConfigMap contains `[gssapi]` with `keytab_path = "/etc/crabka/gssapi-keytab/keytab"` and `enabled_mechanisms = ["GSSAPI"]`; the StatefulSet carries the `gssapi-keytab` volume + mount.
2. **Missing keytab Secret:** same CR but the keytab Secret is absent → reconcile fails with `MissingGssapiKeytabSecret` (assert via the surfaced `ListenersValid`/condition or the `Err` reason, matching how the OAuth introspection test asserts `MissingOauthIntrospectionSecret`).
3. **Inter-broker GSSAPI:** `interBrokerListenerName` points at the gssapi listener + `spec.interBrokerKerberos` set → the ConfigMap TOML contains `[inter_broker_credentials]` with `type = "gssapi"` and the client principal.
4. **krb5.conf:** `spec.krb5ConfSecretRef` set (+ Secret exists) → StatefulSet carries the `krb5-conf` volume/mount and the container has `KRB5_CONFIG=/etc/crabka/krb5/krb5.conf`.

Model each test on the structure of `reconcile_listener_oauth.rs` (set up `MockRule`s for the GETs/PATCHes the reconcile performs, including the keytab Secret GET). Build the `Kafka` CR with the GSSAPI listener via the local helper.

- [ ] **Step 2: Run the new test file**

Run: `cargo test -p crabka-operator --test reconcile_listener_gssapi 2>&1 | tail -30`
Expected: all four scenarios PASS. (Iterate on the `MockRule` set until the reconcile's API calls are satisfied — compare against the OAuth test's rule list.)

- [ ] **Step 3: fmt + commit**

```bash
cargo fmt
git add crates/operator/tests/reconcile_listener_gssapi.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(operator): end-to-end GSSAPI listener + inter-broker reconcile"
```

---

## Task 7: Documentation — the feature/KIP tables

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the stale rows**

- `README.md:235` Security table: `| SASL/GSSAPI (Kerberos) | ❌ |` → `| ✅ |` (broker GSSAPI shipped in #295).
- `README.md:280` operator table: `| Listener auth wiring (TLS / SCRAM) | ✅ |` → `| Listener auth wiring (TLS / SCRAM / OAuth / Kerberos) | ✅ |` (also fixes the already-stale missing OAuth claim).
- `README.md:91` `crabka-security` crate row: add `SASL/GSSAPI` to the listed mechanisms.
- `README.md:73` roadmap sentence: remove `SASL/GSSAPI (Kerberos)` from the "still cooking" list.
- `README.md:419` KIP-12 (`SSL & SASL/Kerberos`): `⚠️` → `✅`.

- [ ] **Step 2: Sanity-check the tables render**

Run: `grep -nE "GSSAPI|Kerberos|Listener auth wiring|KIP-12" README.md`
Expected: every line reflects the updates above; no stray `❌`/`⚠️` for GSSAPI rows.

- [ ] **Step 3: Commit**

```bash
git add README.md
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "docs: mark SASL/GSSAPI (Kerberos) + operator Kerberos listener auth as shipped"
```

---

## Final verification (after all tasks)

- [ ] `cargo fmt --check` (CI gate)
- [ ] `cargo clippy --workspace --all-targets 2>&1 | tail -20` — no new warnings in touched files
- [ ] `cargo test -p crabka-broker --lib file_config 2>&1 | tail -5`
- [ ] `cargo test -p crabka-operator 2>&1 | tail -15`
- [ ] Re-read the design spec §1–§7 and confirm each rendered TOML key matches the broker `FileConfig` parser exactly (the TOML contract at the top of this plan).
