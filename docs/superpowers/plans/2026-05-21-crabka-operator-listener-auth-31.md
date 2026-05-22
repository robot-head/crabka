# Slice 31 — Operator: Listener auth wiring (TLS + SCRAM) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface Crabka's per-listener auth (TLS, mTLS, SCRAM-SHA-512, SCRAM-SHA-256) through `Kafka.spec.listeners[].authentication`, render per-listener broker TOML, extend per-broker cert SANs for external listeners, and prove end-to-end with kind e2e for SCRAM-SSL internal, mTLS internal, and SCRAM-SSL NodePort.

**Architecture:** Strimzi-shape `authentication: { type: tls | scram-sha-512 | scram-sha-256 }` field on `Listener`. Listener `tls: bool` and `authentication` are orthogonal. `controller/listeners.rs::render_broker_toml` emits new per-listener `tls_config` and `sasl_config` inline TOML tables. Broker `file_config.rs` parses them; `network/listener.rs` resolves per-listener TLS acceptor + SASL mechanism with fallback to the slice-30 top-level `[tls_config]` (preserved for the inter-broker setup). Per-broker cert SAN list extended via new `extra_sans` parameter to `issue_broker_cert`, computed by the listener reconciler from observed NodePort and LoadBalancer addresses. Cert reissue triggers on SAN-list change. Listener auth changes go through slice-21 config-hash rolling restart (free).

**Tech Stack:** Rust 1.x workspace, `kube-rs` operator, `serde` / `toml`, `rcgen` for cert issuance, `tokio_rustls` for TLS, `tower` mock for K8s API in integration tests. kind + cp-kafka 6.1.1 JVM client for e2e.

**Reference design doc:** `docs/superpowers/specs/2026-05-21-crabka-operator-listener-auth-31-design.md`

**File map (in dispatch-batch order):**

| Batch | Task | Crate / file(s)                                            | What changes                                                                            |
|------:|-----:|------------------------------------------------------------|-----------------------------------------------------------------------------------------|
|     1 |   1  | `crates/operator/src/crd/listener.rs`                      | Add `ListenerAuthentication` enum + field on `Listener`; remove "tls must be false" doc |
|     1 |   2  | `crates/broker/src/file_config.rs` + `crates/broker/src/config.rs` | Per-listener `tls_config` + `sasl_config` on `FileListener` + `ListenerSpec` + converter |
|     1 |   3  | `crates/operator/src/controller/cluster_ca.rs`             | `issue_broker_cert` gains `extra_sans: &[SubjectAltName]` parameter (behavior unchanged) |
|     2 |   4  | `crates/operator/src/controller/listeners.rs`              | Validation rules update, `listener_protocol` mapping fn, `render_broker_toml` per-listener TLS/SASL emission |
|     2 |   5  | `crates/broker/src/network/listener.rs` (+ minor `auth.rs`) | Per-listener TLS acceptor + per-listener SASL mechanism plumbing                        |
|     3 |   6  | `crates/operator/src/controller/listeners.rs` + `crates/operator/src/controller/kafka.rs` | `compute_extra_sans` helper + wire into reconcile loop                                  |
|     3 |   7  | `crates/operator/src/controller/cluster_ca.rs` + `crates/operator/src/controller/kafka.rs` | Cert reissue on SAN-list change (compare against stored SAN list)                       |
|     4 |   8  | `crates/operator/src/controller/listeners.rs` + `crates/operator/src/controller/kafka.rs` | `WeakAuth` Event, `ListenerValidationFailed` + `WaitingForLoadBalancerIp` status conditions |
|     5 |   9  | `crates/operator/tests/reconcile_listener_auth.rs` (new)   | Integration tests via FIFO-mock K8s harness                                             |
|     5 |  10  | `.github/workflows/operator-e2e.yml` + helm chart samples  | Three kind e2e scenarios + the Kafka CRs / KafkaUsers they need                         |
|     6 |  11  | `STATUS.md` + `charts/crabka-operator/values.yaml` sample  | Slice-31 STATUS.md entry + sample listener-auth in chart                                |

**Batches 1–2 dispatch in parallel within the batch** (no file overlap). Batches 3–6 sequential.

**Per-commit git author:** this repo has no local `user.name`/`user.email` set. Every commit must use `-c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"` overrides. Do **not** run `git config`.

**Commit message style:** `Slice 31: <area> — <subject>` (mirrors slice 30 pattern). Each task's commit step shows the exact message to use.

---

## Batch 1 — Foundation primitives (parallel-safe)

### Task 1: CRD — add `ListenerAuthentication` enum and field

**Files:**
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Write the failing tests at the bottom of `crates/operator/src/crd/listener.rs`** (if there's no existing `#[cfg(test)] mod tests`, add one):

```rust
#[cfg(test)]
mod auth_tests {
    use super::*;

    #[test]
    fn listener_round_trips_with_tls_authentication() {
        let l: Listener = serde_yaml::from_str(
            r#"
name: mtls
port: 9095
type: internal
tls: true
authentication:
  type: tls
"#,
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::Tls));
    }

    #[test]
    fn listener_round_trips_with_scram_sha_512_authentication() {
        let l: Listener = serde_yaml::from_str(
            r#"
name: scram
port: 9094
type: internal
tls: true
authentication:
  type: scram-sha-512
"#,
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::ScramSha512));
    }

    #[test]
    fn listener_round_trips_with_scram_sha_256_authentication() {
        let l: Listener = serde_yaml::from_str(
            r#"
name: scram256
port: 9094
type: internal
tls: true
authentication:
  type: scram-sha-256
"#,
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::ScramSha256));
    }

    #[test]
    fn listener_round_trips_with_no_authentication() {
        let l: Listener = serde_yaml::from_str(
            r#"
name: plain
port: 9092
type: internal
"#,
        )
        .unwrap();
        assert!(l.authentication.is_none());
    }

    #[test]
    fn unknown_authentication_type_rejected() {
        let err = serde_yaml::from_str::<Listener>(
            r#"
name: bad
port: 9092
type: internal
authentication:
  type: oauth
"#,
        )
        .err();
        assert!(err.is_some(), "unknown auth type should fail to deserialize");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test -p crabka-operator --lib crd::listener::auth_tests
```

Expected: FAIL — `ListenerAuthentication` does not exist; `authentication` field does not exist.

- [ ] **Step 3: Add the enum and field**

In `crates/operator/src/crd/listener.rs`, after the `BrokerOverride` struct (after L110), add:

```rust
/// Slice 31 — per-listener authentication. Optional. Absent means
/// anonymous (combined with `tls: bool` controls whether transport
/// is encrypted but no client identity is required).
///
/// - `Tls`: mutual TLS — client must present a cert signed by the
///   clients CA. Requires `Listener.tls = true`. Principal becomes
///   `User:CN=<cert subject CN>`.
/// - `ScramSha512`: SASL/SCRAM-SHA-512. Credentials provisioned by
///   `KafkaUser` (slice 36). Principal becomes `User:<username>`.
/// - `ScramSha256`: SASL/SCRAM-SHA-256, same shape.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ListenerAuthentication {
    Tls,
    ScramSha512,
    ScramSha256,
}
```

Then modify the `Listener` struct (L8-40). Replace the existing comment on `tls`:

```rust
/// Transport-level TLS. When `true`, the listener uses the per-broker
/// keystore signed by the cluster CA (slice 30) and clients must speak
/// TLS to connect. Independent of `authentication` — a `tls: true`
/// listener with no `authentication` is anonymous over TLS.
#[serde(default)]
pub tls: bool,
```

And insert the new field immediately after `tls` and before `configuration`:

```rust
/// Slice 31 — per-listener authentication. Absent = anonymous.
/// `type: tls` requires `tls: true`.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub authentication: Option<ListenerAuthentication>,
```

- [ ] **Step 4: Run tests to verify they pass**

```
cargo test -p crabka-operator --lib crd::listener::auth_tests
```

Expected: PASS — all 5 tests pass.

- [ ] **Step 5: Verify the CRD YAML output regenerates cleanly**

If the operator has a CRD-YAML regenerate step (check `crates/operator/build.rs` or a `tools/` script — there's likely a `regenerate.sh` similar to the protocol-codegen one). If so:

```
cargo build -p crabka-operator
ls -la charts/crabka-operator/templates/crds/kafka.crabka.io_kafkas.yaml 2>/dev/null && echo "regenerate the CRD YAML to include the new field"
```

If a regeneration tool exists (search `tools/regenerate*.sh`), run it and `git diff` the CRD YAML to confirm `authentication` appears under `listeners[].properties`. If the chart's CRD YAML is hand-maintained, add the new schema fragment by mirroring the surrounding pattern.

- [ ] **Step 6: Commit**

```
git add crates/operator/src/crd/listener.rs charts/crabka-operator/templates/crds/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: CRD — add Listener.authentication field"
```

---

### Task 2: Broker config — per-listener TLS + SASL structs

**Files:**
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Write the failing TOML-parse test in `crates/broker/src/file_config.rs`**

Find the existing `#[cfg(test)] mod tests` at the bottom (or add one). Add:

```rust
#[cfg(test)]
mod listener_auth_tests {
    use super::*;

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.listeners.len(), 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        assert_eq!(data_tls.cert_path, std::path::PathBuf::from("/tls/broker.crt"));
        assert_eq!(data_tls.key_path, std::path::PathBuf::from("/tls/broker.key"));
        assert_eq!(
            data_tls.client_ca_path.as_deref(),
            Some(std::path::Path::new("/tls/clients-ca.crt"))
        );
        assert_eq!(data_tls.client_auth, FileClientAuthMode::Required);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "None" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert_eq!(sasl.enabled_mechanisms, vec![crabka_security::SaslMechanism::ScramSha512]);
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        // Slice 30 emitted top-level [tls_config] for the controller
        // listener. Must continue to work.
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}
```

- [ ] **Step 2: Run to verify they fail**

```
cargo test -p crabka-broker --lib file_config::listener_auth_tests
```

Expected: FAIL — `FileListener` has no `tls_config` / `sasl_config` fields; `FileListenerTlsConfig` / `FileListenerSaslConfig` types don't exist.

- [ ] **Step 3: Add new types in `crates/broker/src/file_config.rs`**

After `FileTlsConfig` (L41-49) and `FileClientAuthMode` (find it nearby — likely L51-57), add:

```rust
/// Slice 31 — per-listener TLS material. Optional. When `Some`,
/// overrides the top-level `[tls_config]` for this listener only.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileListenerTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    #[serde(default)]
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

/// Slice 31 — per-listener SASL configuration.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileListenerSaslConfig {
    #[serde(default)]
    pub enabled_mechanisms: Vec<crabka_security::SaslMechanism>,
}
```

Modify `FileListener` (L59-65):

```rust
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FileListener {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
    /// Slice 31 — per-listener TLS overrides the top-level
    /// `[tls_config]`. Inline-table syntax preferred in rendered TOML.
    #[serde(default)]
    pub tls_config: Option<FileListenerTlsConfig>,
    /// Slice 31 — per-listener SASL mechanism scope.
    #[serde(default)]
    pub sasl_config: Option<FileListenerSaslConfig>,
}
```

- [ ] **Step 4: Run tests to verify parsing now succeeds**

```
cargo test -p crabka-broker --lib file_config::listener_auth_tests
```

Expected: PASS — all 3 tests pass.

- [ ] **Step 5: Extend `ListenerSpec` (runtime) in `crates/broker/src/config.rs`**

Modify L17-27:

```rust
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
    /// Slice 31 — per-listener TLS material. When `None`, the broker
    /// falls back to `BrokerConfig.tls_config` (slice 30 inter-broker).
    pub tls_config: Option<TlsConfig>,
    /// Slice 31 — per-listener SASL mechanisms. When `None`, the broker
    /// falls back to `BrokerConfig.enabled_sasl_mechanisms` (slice 12).
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
}
```

The `TlsConfig` and `SaslMechanism` types are imported from `crabka_security` already (per the existing imports at L1-10). If they aren't, add them to the use statement.

- [ ] **Step 6: Update the `apply_to` / converter that turns `FileConfig` into `BrokerConfig`**

Find the function that builds `ListenerSpec` from `FileListener` (likely in `crates/broker/src/file_config.rs` `apply_to()` around L81-125, or in `crates/broker/src/config.rs`). At every `ListenerSpec { ... }` construction, add the two new fields:

```rust
ListenerSpec {
    name: fl.name.clone(),
    bind_addr: fl.bind_addr,
    advertised: fl.advertised.clone(),
    protocol: fl.protocol,
    tls_config: fl.tls_config.as_ref().map(|t| TlsConfig {
        cert_path: t.cert_path.clone(),
        key_path: t.key_path.clone(),
        client_ca_path: t.client_ca_path.clone(),
        client_auth: t.client_auth.into(),  // FileClientAuthMode → ClientAuth
    }),
    sasl_mechanisms: fl.sasl_config.as_ref().map(|s| s.enabled_mechanisms.clone()),
}
```

The exact field set of `TlsConfig` is whatever the existing slice-30 top-level conversion uses — copy from the existing `apply_to`'s top-level `tls_config` branch. Likewise the `FileClientAuthMode → ClientAuth` conversion (or whatever the existing top-level path uses).

- [ ] **Step 7: Build the whole broker crate**

```
cargo build -p crabka-broker
```

Expected: SUCCESS. If there are errors at `ListenerSpec` construction sites that don't supply the new fields, fix each by adding `tls_config: None, sasl_mechanisms: None` (these are the pre-slice-31 default).

- [ ] **Step 8: Run all broker tests to confirm no regression**

```
cargo test -p crabka-broker --lib
```

Expected: PASS.

- [ ] **Step 9: Commit**

```
git add crates/broker/src/file_config.rs crates/broker/src/config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: broker — per-listener TLS + SASL config types"
```

---

### Task 3: `issue_broker_cert` — add `extra_sans` parameter (behavior unchanged)

**Files:**
- Modify: `crates/operator/src/controller/cluster_ca.rs`
- Modify: all callers (likely only `crates/operator/src/controller/ca.rs` or `kafka.rs` — find via `grep`)

- [ ] **Step 1: Add a unit test in `crates/operator/src/controller/cluster_ca.rs`**

At the bottom (or in the existing `#[cfg(test)] mod tests`), add:

```rust
#[cfg(test)]
mod san_tests {
    use super::*;

    #[test]
    fn issue_broker_cert_includes_extra_sans_in_leaf() {
        let cluster_ca = generate_test_ca("cluster-ca", 365);  // helper from slice 30 test code
        let extra = vec![
            SubjectAltName::DnsName("broker-0.example.com".into()),
            SubjectAltName::IpAddress("203.0.113.10".parse().unwrap()),
        ];
        let internal_sans = vec![SubjectAltName::DnsName("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &extra,        // NEW arg
            365,
        )
        .unwrap();
        // Parse the leaf cert and assert SAN list contains all three.
        let parsed = parse_pem_certificate(&leaf.cert_pem);
        let sans = parsed.subject_alternative_names();
        assert!(sans.iter().any(|s| matches!(s, ParsedSan::DnsName(n) if n == "internal.svc")));
        assert!(sans.iter().any(|s| matches!(s, ParsedSan::DnsName(n) if n == "broker-0.example.com")));
        assert!(sans.iter().any(|s| matches!(s, ParsedSan::IpAddress(ip) if ip.to_string() == "203.0.113.10")));
    }

    #[test]
    fn issue_broker_cert_with_empty_extra_sans_matches_slice30_output() {
        let cluster_ca = generate_test_ca("cluster-ca", 365);
        let internal_sans = vec![SubjectAltName::DnsName("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &[],            // empty extra
            365,
        )
        .unwrap();
        let parsed = parse_pem_certificate(&leaf.cert_pem);
        let sans = parsed.subject_alternative_names();
        assert_eq!(sans.len(), 1);  // only internal — no extras
    }
}
```

If `generate_test_ca` / `parse_pem_certificate` / `ParsedSan` helpers don't exist, add them as helpers in the same `#[cfg(test)]` module — use whatever crate the slice-30 ca tests use (likely `x509-parser`). Mirror the slice-30 `cluster_ca` tests for cert-parsing helpers.

- [ ] **Step 2: Run to verify failure**

```
cargo test -p crabka-operator --lib controller::cluster_ca::san_tests
```

Expected: FAIL — `issue_broker_cert` signature only takes 5 args (no `extra_sans`).

- [ ] **Step 3: Add `extra_sans` parameter to `issue_broker_cert`**

In `crates/operator/src/controller/cluster_ca.rs` find the `pub fn issue_broker_cert(...)` definition (the test in step 1 reveals current arity is 5: `cert_pem, key_pem, cn, sans, validity`). New signature:

```rust
pub(crate) fn issue_broker_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    base_sans: &[SubjectAltName],
    extra_sans: &[SubjectAltName],   // NEW — slice 31 external advertised addrs
    validity_days: u32,
) -> Result<BrokerCert, ReconcileError> {
    let mut all_sans: Vec<SubjectAltName> = base_sans.iter().cloned().collect();
    for s in extra_sans {
        if !all_sans.contains(s) {
            all_sans.push(s.clone());
        }
    }
    // existing body but using `&all_sans` instead of `sans`
    ...
}
```

`SubjectAltName` already has `PartialEq` (or add `#[derive(PartialEq)]` if missing — required for `contains`). Slice-30 likely already has it.

- [ ] **Step 4: Update every caller**

```
grep -rn "issue_broker_cert(" crates/operator/src/
```

For each call site, add `&[]` as the new 5th positional arg (to preserve slice-30 behavior for now — slice 31's later tasks will replace these with real extra-SANs):

```rust
let leaf = issue_broker_cert(
    &cluster_ca.cert_pem,
    &cluster_ca.key_pem,
    &req.cn,
    &req.sans,
    &[],                  // extra_sans — populated by later tasks
    validity,
)?;
```

- [ ] **Step 5: Build + run all operator tests**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib
```

Expected: PASS — both new SAN tests and all prior slice-30 cluster_ca + ca tests pass unchanged.

- [ ] **Step 6: Commit**

```
git add crates/operator/src/controller/cluster_ca.rs crates/operator/src/controller/ca.rs crates/operator/src/controller/kafka.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: issue_broker_cert takes extra_sans (behavior unchanged)"
```

(Only include kafka.rs / ca.rs in the `add` set if they were actually modified.)

---

## Batch 2 — Logic on foundation (parallel-safe across crates)

### Task 4: Operator validation + protocol mapping + render extension

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`

- [ ] **Step 1: Add failing validation tests in `crates/operator/src/controller/listeners.rs`** (at the existing `#[cfg(test)] mod tests`)

```rust
#[test]
fn validate_listeners_rejects_mtls_without_transport_tls() {
    let listeners = vec![Listener {
        name: "bad".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::Tls),
        configuration: None,
        network_policy_peers: None,
    }];
    let err = validate_listeners(&listeners, None).unwrap_err();
    assert!(matches!(err, ValidationError::ListenerMtlsRequiresTransportTls(ref n) if n == "bad"));
}

#[test]
fn validate_listeners_accepts_scram_without_tls() {
    let listeners = vec![Listener {
        name: "scram".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::ScramSha512),
        configuration: None,
        network_policy_peers: None,
    }];
    validate_listeners(&listeners, None).unwrap();
}

#[test]
fn validate_listeners_accepts_tls_without_auth() {
    let listeners = vec![Listener {
        name: "tls".into(),
        port: 9093,
        type_: ListenerType::Internal,
        tls: true,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];
    validate_listeners(&listeners, None).unwrap();
}

#[test]
fn validate_listeners_accepts_mtls_with_tls() {
    let listeners = vec![Listener {
        name: "mtls".into(),
        port: 9095,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::Tls),
        configuration: None,
        network_policy_peers: None,
    }];
    validate_listeners(&listeners, None).unwrap();
}

#[test]
fn validate_listeners_accepts_scram_with_tls() {
    let listeners = vec![Listener {
        name: "scram".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::ScramSha256),
        configuration: None,
        network_policy_peers: None,
    }];
    validate_listeners(&listeners, None).unwrap();
}

#[test]
fn listener_protocol_table_all_legal_tuples() {
    use crabka_security::ListenerProtocol::*;
    let cases = [
        (false, None, Plaintext),
        (true,  None, Ssl),
        (false, Some(ListenerAuthentication::ScramSha512), SaslPlaintext),
        (false, Some(ListenerAuthentication::ScramSha256), SaslPlaintext),
        (true,  Some(ListenerAuthentication::ScramSha512), SaslSsl),
        (true,  Some(ListenerAuthentication::ScramSha256), SaslSsl),
        (true,  Some(ListenerAuthentication::Tls), Ssl),
    ];
    for (tls, auth, expected) in cases {
        let l = Listener {
            name: "x".into(),
            port: 1,
            type_: ListenerType::Internal,
            tls,
            authentication: auth,
            configuration: None,
            network_policy_peers: None,
        };
        assert_eq!(listener_protocol(&l), expected, "tls={tls}, auth={auth:?}");
    }
}
```

- [ ] **Step 2: Add snapshot test for render_broker_toml with SCRAM-SSL listener**

```rust
#[test]
fn render_broker_toml_emits_scram_ssl_listener_with_inline_configs() {
    use std::collections::BTreeMap;
    let listeners = vec![Listener {
        name: "scram".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::ScramSha512),
        configuration: None,
        network_policy_peers: None,
    }];
    let mut addrs = BTreeMap::new();
    addrs.insert(
        "scram".to_string(),
        AdvertisedAddress { host: "broker-0".into(), port: 9094 },
    );
    let toml = render_broker_toml(
        0,
        &listeners,
        &addrs,
        "scram",
        &BTreeMap::new(),
        Some(&BrokerTlsRender {
            controller_listener_protocol: "Ssl".into(),
            cert_path: "/etc/crabka/broker-tls/0.crt".into(),
            key_path: "/etc/crabka/broker-tls/0.key".into(),
            client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
            client_auth: "Required".into(),
        }),
        &BTreeMap::new(),  // NEW arg — clients_ca_paths_per_broker map
    );
    assert!(toml.contains("protocol = \"SaslSsl\""));
    assert!(toml.contains("tls_config = { cert_path = \"/etc/crabka/broker-tls/0.crt\""));
    assert!(toml.contains("sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-512\"] }"));
    // Top-level [tls_config] for inter-broker is still emitted.
    assert!(toml.contains("[tls_config]"));
}

#[test]
fn render_broker_toml_emits_mtls_listener_with_client_auth_required() {
    use std::collections::BTreeMap;
    let listeners = vec![Listener {
        name: "mtls".into(),
        port: 9095,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::Tls),
        configuration: None,
        network_policy_peers: None,
    }];
    let mut addrs = BTreeMap::new();
    addrs.insert(
        "mtls".to_string(),
        AdvertisedAddress { host: "broker-0".into(), port: 9095 },
    );
    let mut clients_ca_per_broker = BTreeMap::new();
    clients_ca_per_broker.insert(0, "/etc/crabka/clients-ca/ca.crt".to_string());
    let toml = render_broker_toml(
        0,
        &listeners,
        &addrs,
        "mtls",
        &BTreeMap::new(),
        None,                       // no inter-broker TLS in this fixture
        &clients_ca_per_broker,
    );
    assert!(toml.contains("protocol = \"Ssl\""));
    assert!(toml.contains("client_ca_path = \"/etc/crabka/clients-ca/ca.crt\""));
    assert!(toml.contains("client_auth = \"Required\""));
}
```

- [ ] **Step 3: Run tests to verify they fail**

```
cargo test -p crabka-operator --lib controller::listeners
```

Expected: FAIL — `ValidationError::ListenerMtlsRequiresTransportTls` doesn't exist, `listener_protocol` function doesn't exist, `render_broker_toml` signature is wrong arity, TOML doesn't contain expected per-listener blocks.

- [ ] **Step 4: Update `ValidationError`** in `crates/operator/src/controller/listeners.rs` (L17-28):

Add a new variant:

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ValidationError {
    // ... existing variants ...
    #[error("listener '{0}': authentication.type=tls requires tls: true")]
    ListenerMtlsRequiresTransportTls(String),
}
```

Update the `reason()` method (L32-44) to map the new variant to a stable reason string `"ListenerMtlsRequiresTransportTls"`.

- [ ] **Step 5: Remove the slice-25 `tls: true` rejection and add the new validation**

In `validate_listeners` (L84-145), replace the `if l.tls { return Err(...) }` block (L102-104) with:

```rust
for l in listeners {
    // Slice 31: data-plane TLS is allowed; mTLS requires transport TLS.
    if matches!(l.authentication, Some(ListenerAuthentication::Tls)) && !l.tls {
        return Err(ValidationError::ListenerMtlsRequiresTransportTls(l.name.clone()));
    }
    match l.type_ {
        ListenerType::Ingress => {
            return Err(ValidationError::IngressDeferred(l.name.clone()));
        }
        ListenerType::Route => {
            return Err(ValidationError::RouteDeferred(l.name.clone()));
        }
        _ => {}
    }
    // ... rest of loop body unchanged ...
}
```

Delete the `TlsNotYetSupported` variant from `ValidationError` and the corresponding match arm in `reason()`.

- [ ] **Step 6: Add `listener_protocol` and `sasl_mechanism` helper functions** (near the top of `controller/listeners.rs`, after the `use` statements and before `validate_listeners`):

```rust
use crate::crd::ListenerAuthentication;
use crabka_security::{ListenerProtocol, SaslMechanism};

/// Slice 31 — derive Kafka listener protocol from the CRD's
/// `(tls, authentication)` pair. Panics if called on
/// `(tls=false, auth=tls)`; validation rejects that combination.
pub(crate) fn listener_protocol(l: &Listener) -> ListenerProtocol {
    use ListenerAuthentication::*;
    use ListenerProtocol::*;
    match (l.tls, l.authentication) {
        (false, None) => Plaintext,
        (true, None) => Ssl,
        (false, Some(ScramSha512 | ScramSha256)) => SaslPlaintext,
        (true, Some(ScramSha512 | ScramSha256)) => SaslSsl,
        (true, Some(Tls)) => Ssl,
        (false, Some(Tls)) => unreachable!(
            "validation rejects mTLS without transport TLS; saw listener '{}'",
            l.name
        ),
    }
}

/// Slice 31 — single-valued: SCRAM-512, SCRAM-256, or none.
fn sasl_mechanism(auth: ListenerAuthentication) -> Option<SaslMechanism> {
    use ListenerAuthentication::*;
    match auth {
        ScramSha512 => Some(SaslMechanism::ScramSha512),
        ScramSha256 => Some(SaslMechanism::ScramSha256),
        Tls => None,
    }
}
```

- [ ] **Step 7: Extend `render_broker_toml` to emit per-listener TLS/SASL blocks**

Modify the signature (currently at L1058-1118) to add `clients_ca_paths_per_broker: &BTreeMap<i32, String>`:

```rust
pub fn render_broker_toml(
    broker_id: i32,
    listeners: &[Listener],
    addresses_per_listener: &std::collections::BTreeMap<String, AdvertisedAddress>,
    inter_broker_listener_name: &str,
    server_properties: &std::collections::BTreeMap<String, String>,
    tls: Option<&BrokerTlsRender>,
    clients_ca_paths_per_broker: &std::collections::BTreeMap<i32, String>,  // NEW
) -> String {
```

Replace the per-listener emission (L1085-1093, the `for l in listeners { ... }` loop) with:

```rust
for l in listeners {
    let adv = addresses_per_listener
        .get(&l.name)
        .map(|a| format!("{}:{}", a.host, a.port))
        .unwrap_or_default();
    let proto = listener_protocol(l);
    let _ = writeln!(out, "[[listeners]]");
    let _ = writeln!(out, "name = \"{}\"", l.name);
    let _ = writeln!(out, "bind_addr = \"0.0.0.0:{}\"", l.port);
    let _ = writeln!(out, "advertised = \"{adv}\"");
    let _ = writeln!(out, "protocol = \"{proto:?}\"");

    if l.tls {
        let cert_path = format!("/etc/crabka/broker-tls/{broker_id}.crt");
        let key_path = format!("/etc/crabka/broker-tls/{broker_id}.key");
        let needs_client_ca = matches!(l.authentication, Some(ListenerAuthentication::Tls));
        let client_auth = if needs_client_ca { "Required" } else { "None" };
        if needs_client_ca {
            let client_ca = clients_ca_paths_per_broker
                .get(&broker_id)
                .cloned()
                .unwrap_or_else(|| "/etc/crabka/clients-ca/ca.crt".into());
            let _ = writeln!(
                out,
                "tls_config = {{ cert_path = \"{cert_path}\", key_path = \"{key_path}\", client_ca_path = \"{client_ca}\", client_auth = \"{client_auth}\" }}"
            );
        } else {
            let _ = writeln!(
                out,
                "tls_config = {{ cert_path = \"{cert_path}\", key_path = \"{key_path}\", client_auth = \"{client_auth}\" }}"
            );
        }
    }

    if let Some(auth) = l.authentication {
        if let Some(mech) = sasl_mechanism(auth) {
            let _ = writeln!(
                out,
                "sasl_config = {{ enabled_mechanisms = [\"{}\"] }}",
                mech.wire_name()
            );
        }
    }

    out.push('\n');
}
```

Note: TOML enum serialization via `Debug` (`{proto:?}`) yields `Plaintext`/`Ssl`/`SaslPlaintext`/`SaslSsl` which match the `ListenerProtocol` enum's serde repr — verify with an explicit format if `serde::Serialize` uses different casing. If `Debug` and serde disagree, use `serde_plain::to_string(&proto).unwrap()` or a `Display` impl.

- [ ] **Step 8: Update all callers of `render_broker_toml`**

```
grep -rn "render_broker_toml(" crates/operator/src/
```

For each caller (notably `controller/common.rs::render_configmap` at L202-243), thread through the new `clients_ca_paths_per_broker` parameter. Sites that don't yet have data can pass `&BTreeMap::new()` — Task 6 will wire real values.

- [ ] **Step 9: Update `controller/common.rs::render_configmap` signature** to thread the new map through:

```rust
pub(crate) fn render_configmap(
    owner: &Kafka,
    listeners: &[crate::crd::Listener],
    addresses_per_broker: &std::collections::BTreeMap<i32, std::collections::BTreeMap<String, crate::controller::listeners::AdvertisedAddress>>,
    inter_broker_listener_name: &str,
    tls_per_broker: Option<&std::collections::BTreeMap<i32, crate::controller::listeners::BrokerTlsRender>>,
    clients_ca_paths_per_broker: &std::collections::BTreeMap<i32, String>,  // NEW
) -> Result<ConfigMap, ReconcileError> {
```

Pass it through to `render_broker_toml`. Update the call site in `controller/kafka.rs` (the `apply_cm` closure at L631-643) to pass `&BTreeMap::new()` for now.

- [ ] **Step 10: Run all operator tests**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib
```

Expected: PASS for new tests; slice-30 snapshot tests may need updating if `render_broker_toml` output bytes changed. Update snapshots in-place (verify the diff is the protocol-string change from hardcoded `"Plaintext"` to the derived value, not anything unexpected).

- [ ] **Step 11: Commit**

```
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/common.rs crates/operator/src/controller/kafka.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: controller — validation, protocol mapping, per-listener TOML"
```

---

### Task 5: Broker — per-listener TLS acceptor + per-listener SASL mechanism

**Files:**
- Modify: `crates/broker/src/network/listener.rs` (accept-loop)
- Possibly modify: `crates/broker/src/network/auth.rs` (`ConnectionAuth::new`) to accept the per-listener mechanism list

- [ ] **Step 1: Find the accept-loop TLS resolution and SASL initialization sites**

```
grep -n "tls_config\|TlsAcceptor\|ConnectionAuth::new\|enabled_sasl_mechanisms" crates/broker/src/network/listener.rs crates/broker/src/network/auth.rs
```

Identify (a) where `BrokerConfig.tls_config` is consulted to build the `TlsAcceptor` per listener, and (b) where `ConnectionAuth` is initialized per connection with the SASL mechanism list.

- [ ] **Step 2: Write failing unit tests in `crates/broker/src/network/listener.rs`**

```rust
#[cfg(test)]
mod per_listener_config_tests {
    use super::*;
    use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};
    use std::path::PathBuf;

    fn test_listener_spec(
        protocol: ListenerProtocol,
        tls: Option<TlsConfig>,
        sasl: Option<Vec<SaslMechanism>>,
    ) -> crate::config::ListenerSpec {
        crate::config::ListenerSpec {
            name: "test".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "localhost:9094".into(),
            protocol,
            tls_config: tls,
            sasl_mechanisms: sasl,
        }
    }

    #[test]
    fn per_listener_tls_config_overrides_top_level() {
        let per_listener_tls = TlsConfig {
            cert_path: PathBuf::from("/per-listener.crt"),
            key_path: PathBuf::from("/per-listener.key"),
            client_ca_path: None,
            client_auth: crabka_security::ClientAuth::None,
        };
        let top_level = TlsConfig {
            cert_path: PathBuf::from("/top-level.crt"),
            key_path: PathBuf::from("/top-level.key"),
            client_ca_path: None,
            client_auth: crabka_security::ClientAuth::None,
        };
        let spec = test_listener_spec(ListenerProtocol::Ssl, Some(per_listener_tls.clone()), None);
        let resolved = resolve_tls_for_listener(&spec, Some(&top_level));
        assert_eq!(resolved.unwrap().cert_path, per_listener_tls.cert_path);
    }

    #[test]
    fn per_listener_tls_falls_back_to_top_level_when_absent() {
        let top_level = TlsConfig {
            cert_path: PathBuf::from("/top-level.crt"),
            key_path: PathBuf::from("/top-level.key"),
            client_ca_path: None,
            client_auth: crabka_security::ClientAuth::None,
        };
        let spec = test_listener_spec(ListenerProtocol::Ssl, None, None);
        let resolved = resolve_tls_for_listener(&spec, Some(&top_level));
        assert_eq!(resolved.unwrap().cert_path, top_level.cert_path);
    }

    #[test]
    fn tls_listener_without_any_config_errors() {
        let spec = test_listener_spec(ListenerProtocol::Ssl, None, None);
        let resolved = resolve_tls_for_listener(&spec, None);
        assert!(resolved.is_err());
    }

    #[test]
    fn per_listener_sasl_mechanisms_override_broker_default() {
        let per_listener = vec![SaslMechanism::ScramSha512];
        let broker_default = vec![SaslMechanism::Plain, SaslMechanism::ScramSha256];
        let spec = test_listener_spec(
            ListenerProtocol::SaslSsl,
            None,
            Some(per_listener.clone()),
        );
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert_eq!(resolved, &per_listener);
    }

    #[test]
    fn per_listener_sasl_falls_back_to_broker_default_when_absent() {
        let broker_default = vec![SaslMechanism::ScramSha512];
        let spec = test_listener_spec(ListenerProtocol::SaslSsl, None, None);
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert_eq!(resolved, &broker_default);
    }
}
```

- [ ] **Step 3: Run to verify failure**

```
cargo test -p crabka-broker --lib network::listener::per_listener_config_tests
```

Expected: FAIL — `resolve_tls_for_listener` and `resolve_sasl_mechanisms_for_listener` don't exist.

- [ ] **Step 4: Add the resolvers in `crates/broker/src/network/listener.rs`**

```rust
use crabka_security::{SaslMechanism, TlsConfig};
use crate::config::ListenerSpec;
use crate::BrokerError;

/// Slice 31 — choose TLS material for a listener: per-listener first,
/// then fall back to the broker-wide top-level config (slice 30 inter-broker).
/// Errors if the listener requires TLS and neither source has config.
pub(crate) fn resolve_tls_for_listener<'a>(
    spec: &'a ListenerSpec,
    top_level: Option<&'a TlsConfig>,
) -> Result<&'a TlsConfig, BrokerError> {
    if let Some(per_listener) = &spec.tls_config {
        return Ok(per_listener);
    }
    top_level.ok_or_else(|| {
        BrokerError::ConfigInvariant(format!(
            "listener '{}' requires TLS but no tls_config (per-listener or top-level) is set",
            spec.name
        ))
    })
}

/// Slice 31 — per-listener SASL mechanism list, fall back to broker default.
pub(crate) fn resolve_sasl_mechanisms_for_listener<'a>(
    spec: &'a ListenerSpec,
    broker_default: &'a [SaslMechanism],
) -> &'a [SaslMechanism] {
    spec.sasl_mechanisms
        .as_deref()
        .unwrap_or(broker_default)
}
```

If `BrokerError::ConfigInvariant` doesn't exist, use whatever the existing error variant for config violations is — find via `grep -rn "BrokerError::" crates/broker/src/`.

- [ ] **Step 5: Wire the resolvers into the accept loop**

Find the accept-loop function (likely `bind_listener` or `accept_loop` in the same file). Wherever it currently reads `broker_config.tls_config`, replace with `resolve_tls_for_listener(&listener_spec, broker_config.tls_config.as_ref())?`. Wherever it currently reads `broker_config.enabled_sasl_mechanisms`, replace with `resolve_sasl_mechanisms_for_listener(&listener_spec, &broker_config.enabled_sasl_mechanisms)`.

For TLS specifically: the `TlsAcceptor` is built per listener at startup; build it from the resolved `TlsConfig` instead of the broker-wide one. The slice-29 / slice-33 hot-reload path needs to watch the per-listener cert path when per-listener config is in effect.

For SASL: pass the resolved mechanism slice into the `ConnectionAuth::new` (or equivalent) at accept time. If the constructor currently takes `&[SaslMechanism]` from `BrokerConfig.enabled_sasl_mechanisms`, no API change needed — just swap the argument.

- [ ] **Step 6: Add a test that exercises a SASL handshake against a SCRAM-512-only listener and rejects SCRAM-256**

This belongs in `crates/broker/tests/auth_handlers.rs` (existing slice-12 test file) or a new `crates/broker/tests/per_listener_sasl.rs`. Mirror the existing handshake test structure:

```rust
#[tokio::test]
async fn per_listener_scram_sha_512_only_rejects_scram_sha_256_handshake() {
    let broker = test_broker_with_listener(ListenerProtocol::SaslSsl, vec![SaslMechanism::ScramSha512]).await;
    let mut conn = broker.connect_data_plane().await;
    let response = conn.send(SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-256".into(),
    }).await;
    assert_eq!(response.error_code, ErrorCode::UnsupportedSaslMechanism);
}
```

If `test_broker_with_listener` doesn't exist, mirror the closest existing helper. Use `cargo test -p crabka-broker --test auth_handlers` to confirm the existing harness shape first.

- [ ] **Step 7: Add a clients-CA mTLS rejection test in `crates/broker/tests/`**

```rust
#[tokio::test]
async fn data_plane_mtls_listener_rejects_cluster_ca_signed_cert() {
    let cluster_ca = generate_test_ca();
    let clients_ca = generate_test_ca();
    let broker = test_broker_with_mtls_listener(&clients_ca).await;
    let cluster_ca_signed_client_cert = clients_ca.issue_user_cert("not-a-real-user");  // intentionally wrong CA
    // Override: actually issue from cluster_ca to test the rejection
    let cluster_ca_signed = cluster_ca.issue_user_cert("user-x");
    let result = broker.connect_with_client_cert(&cluster_ca_signed).await;
    assert!(matches!(result, Err(e) if e.to_string().contains("certificate verification failed")));
}
```

(If existing test infrastructure doesn't provide `test_broker_with_mtls_listener`, build it as a helper; mirror the slice-29 / slice-30 in-process broker harness.)

- [ ] **Step 8: Build + test the whole broker crate**

```
cargo build -p crabka-broker
cargo test -p crabka-broker
```

Expected: PASS. All slice-12 / slice-29 / slice-33 tests continue green.

- [ ] **Step 9: Commit**

```
git add crates/broker/src/network/listener.rs crates/broker/src/network/auth.rs crates/broker/tests/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: broker — per-listener TLS acceptor + SASL mechanism resolution"
```

---

## Batch 3 — Reconcile wiring (sequential — both tasks touch kafka.rs)

### Task 6: SAN extra-sans computation + plumbing into reconcile

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` (add `compute_extra_sans`)
- Modify: `crates/operator/src/controller/kafka.rs` (call it; pass to cert issuance + render_broker_toml)
- Modify: `crates/operator/src/controller/cluster_ca.rs` test helpers if needed

- [ ] **Step 1: Add `compute_extra_sans` unit tests in `crates/operator/src/controller/listeners.rs`**

```rust
#[test]
fn compute_extra_sans_internal_only_returns_empty() {
    let listeners = vec![Listener {
        name: "internal".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];
    let observed = ListenerObservedAddresses::default();  // no NodePort, no LB
    let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
    assert!(sans.is_empty());
}

#[test]
fn compute_extra_sans_nodeport_includes_node_external_addrs() {
    let listeners = vec![Listener {
        name: "ext".into(),
        port: 9094,
        type_: ListenerType::Nodeport,
        tls: true,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];
    let observed = ListenerObservedAddresses {
        nodeport_node_addresses: vec![
            NodeAddress::ExternalIp("203.0.113.10".parse().unwrap()),
            NodeAddress::ExternalDns("node1.example.com".into()),
        ],
        ..Default::default()
    };
    let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
    assert!(sans.contains(&SubjectAltName::IpAddress("203.0.113.10".parse().unwrap())));
    assert!(sans.contains(&SubjectAltName::DnsName("node1.example.com".into())));
}

#[test]
fn compute_extra_sans_loadbalancer_includes_per_broker_and_bootstrap_ips() {
    let listeners = vec![Listener {
        name: "lb".into(),
        port: 9094,
        type_: ListenerType::Loadbalancer,
        tls: true,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];
    let mut observed = ListenerObservedAddresses::default();
    observed.lb_per_broker.insert(0, vec![LbIngress::Ip("203.0.113.20".parse().unwrap())]);
    observed.lb_bootstrap = vec![LbIngress::Ip("203.0.113.30".parse().unwrap())];
    let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
    assert!(sans.contains(&SubjectAltName::IpAddress("203.0.113.20".parse().unwrap())));
    assert!(sans.contains(&SubjectAltName::IpAddress("203.0.113.30".parse().unwrap())));
}

#[test]
fn compute_extra_sans_loadbalancer_pending_returns_sans_not_ready() {
    let listeners = vec![Listener {
        name: "lb".into(),
        port: 9094,
        type_: ListenerType::Loadbalancer,
        tls: true,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];
    let observed = ListenerObservedAddresses::default();  // empty — LB not ready
    let result = compute_extra_sans(0, &listeners, &observed);
    assert!(matches!(result, Err(SanComputationError::SansNotReady { broker_id: 0, .. })));
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p crabka-operator --lib controller::listeners::compute_extra_sans
```

Expected: FAIL — function and supporting types don't exist.

- [ ] **Step 3: Add the helper types and function**

In `crates/operator/src/controller/listeners.rs`:

```rust
use std::collections::BTreeMap;
use std::net::IpAddr;

/// Slice 31 — observed addresses from listener Services, fed into the
/// per-broker SAN computation.
#[derive(Debug, Clone, Default)]
pub(crate) struct ListenerObservedAddresses {
    /// NodePort listeners: every Node's external addresses (cluster-wide).
    pub nodeport_node_addresses: Vec<NodeAddress>,
    /// LoadBalancer listeners: per-broker LB ingress.
    pub lb_per_broker: BTreeMap<i32, Vec<LbIngress>>,
    /// LoadBalancer bootstrap LB ingress (cluster-wide).
    pub lb_bootstrap: Vec<LbIngress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeAddress {
    ExternalIp(IpAddr),
    ExternalDns(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LbIngress {
    Ip(IpAddr),
    Hostname(String),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum SanComputationError {
    #[error("LoadBalancer ingress not ready for broker {broker_id} on listener '{listener}'")]
    SansNotReady { broker_id: i32, listener: String },
}

/// Slice 31 — assemble extra SANs for broker N from observed listener
/// Services. Returns sorted, deduped Vec<SubjectAltName>.
pub(crate) fn compute_extra_sans(
    broker_id: i32,
    listeners: &[Listener],
    observed: &ListenerObservedAddresses,
) -> Result<Vec<crate::controller::cluster_ca::SubjectAltName>, SanComputationError> {
    use crate::controller::cluster_ca::SubjectAltName;
    let mut sans: Vec<SubjectAltName> = Vec::new();
    for l in listeners {
        if !l.tls {
            continue;  // listener doesn't use TLS — no SAN needed
        }
        match l.type_ {
            ListenerType::Internal | ListenerType::Ingress | ListenerType::Route => {}
            ListenerType::Nodeport => {
                for addr in &observed.nodeport_node_addresses {
                    match addr {
                        NodeAddress::ExternalIp(ip) => sans.push(SubjectAltName::IpAddress(*ip)),
                        NodeAddress::ExternalDns(d) => sans.push(SubjectAltName::DnsName(d.clone())),
                    }
                }
                // Per-broker BrokerOverride.advertised_host
                if let Some(cfg) = &l.configuration {
                    for ovr in &cfg.brokers {
                        if ovr.broker == broker_id {
                            if let Some(h) = &ovr.advertised_host {
                                sans.push(SubjectAltName::DnsName(h.clone()));
                            }
                        }
                    }
                }
            }
            ListenerType::Loadbalancer => {
                let per_broker = observed.lb_per_broker.get(&broker_id);
                let bootstrap = &observed.lb_bootstrap;
                if per_broker.is_none() || per_broker.unwrap().is_empty() {
                    return Err(SanComputationError::SansNotReady {
                        broker_id,
                        listener: l.name.clone(),
                    });
                }
                for ingress in per_broker.unwrap().iter().chain(bootstrap.iter()) {
                    match ingress {
                        LbIngress::Ip(ip) => sans.push(SubjectAltName::IpAddress(*ip)),
                        LbIngress::Hostname(h) => sans.push(SubjectAltName::DnsName(h.clone())),
                    }
                }
            }
        }
    }
    sans.sort();
    sans.dedup();
    Ok(sans)
}
```

`SubjectAltName` needs `Ord + PartialOrd + Clone + PartialEq + Eq` derives in `cluster_ca.rs` — add them if missing.

- [ ] **Step 4: Run unit tests to verify pass**

```
cargo test -p crabka-operator --lib controller::listeners::compute_extra_sans
```

Expected: PASS.

- [ ] **Step 5: Wire `compute_extra_sans` into the reconcile loop**

In `crates/operator/src/controller/kafka.rs` reconcile fn (L450+):

After the listener-services reconciliation (around L656-664 where external Services are applied) and before the cert issuance call (around L600-606, but reorder — cert must come AFTER observations), add:

```rust
// Slice 31: observe listener Service status for SAN computation.
let observed = observe_listener_addresses(&ctx, &ns, &name, &effective_listeners).await?;

let extra_sans_per_broker: BTreeMap<i32, Vec<SubjectAltName>> = brokers
    .iter()
    .map(|b| (b.broker_id, compute_extra_sans(b.broker_id, &effective_listeners, &observed)))
    .filter_map(|(id, result)| match result {
        Ok(sans) => Some((id, sans)),
        Err(SanComputationError::SansNotReady { broker_id, listener }) => {
            tracing::info!(broker_id, %listener, "LB ingress not ready; skipping cert SAN extension for this broker");
            // Task 8 will record WaitingForLoadBalancerIp condition.
            None
        }
    })
    .collect();
```

Then change the `issue_broker_cert` call (find it in the cert-issuance loop around L600) to pass `&extra_sans_per_broker.get(&id).cloned().unwrap_or_default()`.

- [ ] **Step 6: Implement `observe_listener_addresses`**

Add to `controller/listeners.rs`:

```rust
pub(crate) async fn observe_listener_addresses(
    ctx: &crate::context::Context,
    namespace: &str,
    cluster_name: &str,
    listeners: &[Listener],
) -> Result<ListenerObservedAddresses, ReconcileError> {
    use kube::Api;
    use k8s_openapi::api::core::v1::{Node, Service};

    let mut out = ListenerObservedAddresses::default();
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
    let node_api: Api<Node> = Api::all(ctx.client.clone());

    let needs_node_addrs = listeners.iter().any(|l| l.type_ == ListenerType::Nodeport && l.tls);
    if needs_node_addrs {
        // Enumerate Node addresses (mirror slice-25's existing logic — find via `grep -n "ExternalIP" crates/operator/src/`)
        let nodes = node_api.list(&Default::default()).await?;
        for node in nodes {
            if let Some(status) = node.status {
                for addr in status.addresses.unwrap_or_default() {
                    match addr.type_.as_str() {
                        "ExternalIP" => {
                            if let Ok(ip) = addr.address.parse() {
                                out.nodeport_node_addresses.push(NodeAddress::ExternalIp(ip));
                            }
                        }
                        "ExternalDNS" | "Hostname" => {
                            out.nodeport_node_addresses.push(NodeAddress::ExternalDns(addr.address));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    for l in listeners {
        if l.type_ != ListenerType::Loadbalancer || !l.tls {
            continue;
        }
        // Per-broker LB Services
        for broker_id in /* enumerate broker ids — pass in or compute */ 0..3 {
            let svc_name = format!("{cluster_name}-kafka-{broker_id}-{}", l.name);
            if let Ok(svc) = svc_api.get(&svc_name).await {
                if let Some(status) = svc.status {
                    if let Some(lb) = status.load_balancer {
                        for ingress in lb.ingress.unwrap_or_default() {
                            if let Some(ip) = ingress.ip {
                                if let Ok(ip) = ip.parse() {
                                    out.lb_per_broker.entry(broker_id).or_default().push(LbIngress::Ip(ip));
                                }
                            }
                            if let Some(hn) = ingress.hostname {
                                out.lb_per_broker.entry(broker_id).or_default().push(LbIngress::Hostname(hn));
                            }
                        }
                    }
                }
            }
        }
        // Bootstrap LB
        let bootstrap_svc_name = format!("{cluster_name}-kafka-bootstrap-{}", l.name);
        if let Ok(svc) = svc_api.get(&bootstrap_svc_name).await {
            if let Some(status) = svc.status {
                if let Some(lb) = status.load_balancer {
                    for ingress in lb.ingress.unwrap_or_default() {
                        if let Some(ip) = ingress.ip {
                            if let Ok(ip) = ip.parse() {
                                out.lb_bootstrap.push(LbIngress::Ip(ip));
                            }
                        }
                        if let Some(hn) = ingress.hostname {
                            out.lb_bootstrap.push(LbIngress::Hostname(hn));
                        }
                    }
                }
            }
        }
    }

    Ok(out)
}
```

The broker-id enumeration and the Service-name template must match slice-25's existing naming — verify by reading the slice-25 listener-Service reconciler in `controller/listeners.rs`. If it exposes a helper like `per_broker_external_service_name(cluster, broker_id, listener_name)`, reuse it.

- [ ] **Step 7: Update `render_configmap` call site in `kafka.rs` to pass `clients_ca_paths_per_broker`**

In the `apply_cm` closure (L631-643), replace the `&BTreeMap::new()` placeholder from Task 4 with a real map populated when at least one listener has `authentication.type: tls`:

```rust
let clients_ca_paths_per_broker: BTreeMap<i32, String> = brokers
    .iter()
    .filter(|_| effective_listeners.iter().any(|l| matches!(l.authentication, Some(ListenerAuthentication::Tls))))
    .map(|b| (b.broker_id, "/etc/crabka/clients-ca/ca.crt".to_string()))
    .collect();
```

Pass `&clients_ca_paths_per_broker` to `render_configmap`.

- [ ] **Step 8: Build + test the operator crate**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib
```

Expected: PASS — all lib unit tests including the new `compute_extra_sans` cases.

- [ ] **Step 9: Commit**

```
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/kafka.rs crates/operator/src/controller/cluster_ca.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: extra SAN computation + plumbing into reconcile"
```

---

### Task 7: Cert reissue on SAN-list change

**Files:**
- Modify: `crates/operator/src/controller/cluster_ca.rs` (extend `ensure_broker_keystore`)
- Modify: `crates/operator/src/controller/ca.rs` if `ensure_broker_certs` lives there
- Modify: `crates/operator/src/controller/kafka.rs` to pass through

- [ ] **Step 1: Write failing tests**

In `crates/operator/src/controller/cluster_ca.rs`'s test module:

```rust
#[test]
fn ensure_broker_keystore_reissues_when_extra_sans_change() {
    // First call: no extras. Second call: extras added.
    let secret_api = mock_secret_api();
    let cluster_ca = generate_test_ca("cluster-ca", 365);
    let req = BrokerCertRequest {
        broker_id: 0,
        cn: "broker-0".into(),
        sans: vec![SubjectAltName::DnsName("internal.svc".into())],
    };

    let first = ensure_broker_keystore(&secret_api, "c1", &cluster_ca, &[req.clone()], &BTreeMap::new()).unwrap();
    let first_cert_pem = first[0].cert_pem.clone();

    let mut extras = BTreeMap::new();
    extras.insert(0, vec![SubjectAltName::DnsName("broker-0.example.com".into())]);
    let second = ensure_broker_keystore(&secret_api, "c1", &cluster_ca, &[req], &extras).unwrap();
    assert_ne!(second[0].cert_pem, first_cert_pem, "extra-SAN change should trigger reissue");
}

#[test]
fn ensure_broker_keystore_reuses_when_san_list_unchanged() {
    let secret_api = mock_secret_api();
    let cluster_ca = generate_test_ca("cluster-ca", 365);
    let req = BrokerCertRequest {
        broker_id: 0,
        cn: "broker-0".into(),
        sans: vec![SubjectAltName::DnsName("internal.svc".into())],
    };
    let extras = BTreeMap::from([(0, vec![SubjectAltName::DnsName("ext.example.com".into())])]);

    let first = ensure_broker_keystore(&secret_api, "c1", &cluster_ca, &[req.clone()], &extras).unwrap();
    let first_cert_pem = first[0].cert_pem.clone();
    let second = ensure_broker_keystore(&secret_api, "c1", &cluster_ca, &[req], &extras).unwrap();
    assert_eq!(second[0].cert_pem, first_cert_pem, "unchanged SAN list should reuse cert");
}
```

`mock_secret_api()` needs to be implementable in this test module — look at how the slice-30 ca tests stub the Secret API (probably via a fake client wrapper). If the slice-30 tests use the full FIFO-mock harness, you may need to move these tests to the integration test file instead. Pragmatic: if the lib-test harness is awkward, defer these to the `reconcile_listener_auth.rs` integration tests (Task 9) and add a smaller `#[test]` here that checks just the SAN-list-comparison helper as a pure function (extract one).

- [ ] **Step 2: Run to verify failure**

```
cargo test -p crabka-operator --lib controller::cluster_ca
```

Expected: FAIL.

- [ ] **Step 3: Extend `ensure_broker_keystore` to track SAN list per broker**

The existing logic (L280-381 in `cluster_ca.rs`) reissues on scale-up. Extend the per-broker decision to compare the requested SAN list (`base_sans + extras_per_broker[id]`) against the SAN list embedded in the existing Secret. To avoid round-tripping through the cert (parsing the existing PEM), store a canonical SAN-list string in the Secret as an annotation key (e.g. `crabka.io/cert-sans-sha256`).

```rust
pub(crate) fn ensure_broker_keystore(
    secret_api: &Api<Secret>,
    cluster: &str,
    cluster_ca: &ClusterCa,
    requests: &[BrokerCertRequest],
    extras_per_broker: &BTreeMap<i32, Vec<SubjectAltName>>,
    validity_days: u32,
) -> Result<Vec<BrokerCert>, ReconcileError> {
    // ... existing read-or-create Secret logic ...

    for req in requests {
        let extras = extras_per_broker.get(&req.broker_id).cloned().unwrap_or_default();
        let mut all_sans = req.sans.clone();
        for s in &extras {
            if !all_sans.contains(s) {
                all_sans.push(s.clone());
            }
        }
        all_sans.sort();
        let san_digest = sha256_of_sans(&all_sans);  // hex string

        let existing_entry = existing_secret_entries.get(&req.broker_id);
        let needs_reissue = match existing_entry {
            None => true,
            Some(e) => e.san_digest.as_deref() != Some(san_digest.as_str())
                       || cert_near_expiry(&e.cert_pem),
        };

        if needs_reissue {
            let cert = issue_broker_cert(
                &cluster_ca.cert_pem,
                &cluster_ca.key_pem,
                &req.cn,
                &req.sans,
                &extras,
                validity_days,
            )?;
            new_secret_entries.insert(req.broker_id, EntryWithDigest { cert, san_digest });
        } else {
            // reuse
        }
    }

    // ... existing Secret apply logic, writing both cert PEM and san_digest ...
}

fn sha256_of_sans(sans: &[SubjectAltName]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for s in sans {  // already sorted
        match s {
            SubjectAltName::DnsName(d) => { h.update(b"DNS:"); h.update(d.as_bytes()); }
            SubjectAltName::IpAddress(ip) => { h.update(b"IP:"); h.update(ip.to_string().as_bytes()); }
        }
        h.update(b"\n");
    }
    format!("{:x}", h.finalize())
}
```

The exact storage location for `san_digest` — Secret key or annotation — depends on the slice-30 Secret schema. If slice-30's Secret already has per-broker entries with parseable JSON metadata, add a `sans` field. Otherwise add an annotation on the Secret like `crabka.io/cert-sans-{id}=<digest>`.

- [ ] **Step 4: Update callers to pass `extras_per_broker`**

In `controller/kafka.rs`, change the existing `ensure_broker_keystore(... )` call to thread the `extra_sans_per_broker` map computed in Task 6.

- [ ] **Step 5: Build + test**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib
cargo test -p crabka-operator --test reconcile_ca
cargo test -p crabka-operator --test reconcile_inter_broker_mtls
```

Expected: PASS. Pre-existing slice-30 integration tests must still pass — they implicitly pass empty `extras_per_broker`, so the behavior matches slice-30.

- [ ] **Step 6: Commit**

```
git add crates/operator/src/controller/cluster_ca.rs crates/operator/src/controller/kafka.rs crates/operator/src/controller/ca.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: reissue broker cert on SAN-list change"
```

---

## Batch 4 — Status conditions + Events

### Task 8: `WeakAuth` Event + status conditions

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` (event helper)
- Modify: `crates/operator/src/controller/kafka.rs` (emit on reconcile)

- [ ] **Step 1: Add failing tests** in `crates/operator/src/controller/listeners.rs`

```rust
#[test]
fn weak_auth_emitted_for_scram_without_tls() {
    let listeners = vec![Listener {
        name: "scram-plain".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::ScramSha512),
        configuration: None,
        network_policy_peers: None,
    }];
    let warnings = weak_auth_warnings(&listeners);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("scram-plain"));
    assert!(warnings[0].contains("cleartext"));
}

#[test]
fn no_weak_auth_for_scram_with_tls() {
    let listeners = vec![Listener {
        name: "scram-tls".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::ScramSha512),
        configuration: None,
        network_policy_peers: None,
    }];
    let warnings = weak_auth_warnings(&listeners);
    assert!(warnings.is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p crabka-operator --lib controller::listeners::weak_auth
```

Expected: FAIL — `weak_auth_warnings` doesn't exist.

- [ ] **Step 3: Add `weak_auth_warnings`** in `controller/listeners.rs`:

```rust
/// Slice 31 — return one warning message per SaslPlaintext listener.
pub(crate) fn weak_auth_warnings(listeners: &[Listener]) -> Vec<String> {
    listeners
        .iter()
        .filter(|l| {
            !l.tls
                && matches!(
                    l.authentication,
                    Some(ListenerAuthentication::ScramSha512 | ListenerAuthentication::ScramSha256)
                )
        })
        .map(|l| {
            format!(
                "listener '{}' has SCRAM auth without transport TLS; credentials traverse the network in cleartext during the SCRAM exchange. Consider tls: true.",
                l.name
            )
        })
        .collect()
}
```

- [ ] **Step 4: Wire into reconcile**

In `controller/kafka.rs`'s `reconcile`, after listener validation:

```rust
for warning in weak_auth_warnings(&effective_listeners) {
    ctx.recorder
        .publish(
            &Event {
                action: "WeakAuth".into(),
                reason: "WeakAuth".into(),
                note: Some(warning),
                type_: EventType::Warning,
                secondary: None,
            },
            &obj.object_ref(&()),
        )
        .await
        .ok();
}
```

(Use the existing `Recorder` pattern — find via `grep -n "publish\|recorder" crates/operator/src/`. If the operator doesn't yet have an event recorder, use whatever the slice-30 `ByoCaExpiringSoon` event uses.)

- [ ] **Step 5: Add `ListenerValidationFailed` and `WaitingForLoadBalancerIp` status conditions**

Where the existing slice-30 reconcile writes status conditions (likely a `conditions.push(condition(...))` in `reconcile` at L726+), add:

```rust
if let Err(ve) = &validation {
    conditions.push(condition(
        "ListenerValidationFailed",
        "True",
        ve.reason(),
        &ve.to_string(),
    ));
    // Existing slice-25 pattern: skip ConfigMap render + StatefulSet bump.
    // Return early after writing status.
} else {
    conditions.push(condition("ListenerValidationFailed", "False", "Valid", "all listeners valid"));
}
```

For the LB-pending case, in Task 6's `extra_sans_per_broker` filter loop, collect the brokers with `SansNotReady` and emit:

```rust
if !lb_pending_brokers.is_empty() {
    let listener_names: Vec<&str> = lb_pending_brokers.iter().map(|(_, l)| l.as_str()).collect();
    conditions.push(condition(
        "WaitingForLoadBalancerIp",
        "True",
        "LoadBalancerPending",
        &format!("LB ingress not ready for brokers: {lb_pending_brokers:?}"),
    ));
}
```

- [ ] **Step 6: Build + test**

```
cargo build -p crabka-operator
cargo test -p crabka-operator --lib
```

Expected: PASS.

- [ ] **Step 7: Commit**

```
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/kafka.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: WeakAuth Event + listener validation / LB-pending conditions"
```

---

## Batch 5 — Integration tests + e2e

### Task 9: Operator integration tests (`reconcile_listener_auth.rs`)

**Files:**
- Create: `crates/operator/tests/reconcile_listener_auth.rs`

- [ ] **Step 1: Scaffold the test file**

Create `crates/operator/tests/reconcile_listener_auth.rs` mirroring the structure of `reconcile_inter_broker_mtls.rs`:

```rust
#[path = "shared/mod.rs"]
mod shared;

use std::sync::Arc;

use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{
    Kafka, KafkaSpec, Listener, ListenerAuthentication, ListenerType,
};
use http::{Method, Response};
use serde_json::json;
use shared::{MockRule, MockState, build_ctx, fake_pool_list_item};

fn kafka_cr_with_listeners(name: &str, namespace: &str, listeners: Vec<Listener>) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners,
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

fn internal_listener_with_auth(auth: Option<ListenerAuthentication>, tls: bool) -> Listener {
    Listener {
        name: "data".into(),
        port: 9094,
        type_: ListenerType::Internal,
        tls,
        authentication: auth,
        configuration: None,
        network_policy_peers: None,
    }
}
```

- [ ] **Step 2: Implement test 1 — SCRAM-SHA-512 over TLS renders SaslSsl**

```rust
#[tokio::test]
async fn scram_sha_512_internal_listener_renders_sasl_ssl() {
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let mut rules = shared::happy_path_rules("c1", "ns", &items);
    let (ctx, state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::ScramSha512), true)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let cm_body = shared::find_configmap_apply(&observed, "c1-broker-config").expect("ConfigMap");
    let toml_keys = shared::extract_data_keys(&cm_body);
    let broker_toml = toml_keys.get("broker-0.toml").expect("broker-0.toml present");
    assert!(broker_toml.contains("protocol = \"SaslSsl\""), "TOML: {broker_toml}");
    assert!(broker_toml.contains("tls_config = {"));
    assert!(broker_toml.contains("sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-512\"] }"));
}
```

`shared::find_configmap_apply` and `shared::extract_data_keys` may not exist yet — add them to `shared/mod.rs` if missing (mirror the pattern used in `reconcile_inter_broker_mtls.rs` for asserting on ConfigMap bodies).

- [ ] **Step 3: Implement test 2 — mTLS renders client_auth Required**

```rust
#[tokio::test]
async fn mtls_internal_listener_renders_client_auth_required() {
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let rules = shared::happy_path_rules("c1", "ns", &items);
    let (ctx, state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::Tls), true)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let cm_body = shared::find_configmap_apply(&observed, "c1-broker-config").unwrap();
    let broker_toml = shared::extract_data_keys(&cm_body).remove("broker-0.toml").unwrap();
    assert!(broker_toml.contains("protocol = \"Ssl\""));
    assert!(broker_toml.contains("client_ca_path = \"/etc/crabka/clients-ca/ca.crt\""));
    assert!(broker_toml.contains("client_auth = \"Required\""));
}
```

- [ ] **Step 4: Implement test 3 — SCRAM-SHA-256 (cheap verification)**

```rust
#[tokio::test]
async fn scram_sha_256_renders_sasl_ssl_with_256_mechanism() {
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let rules = shared::happy_path_rules("c1", "ns", &items);
    let (ctx, state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::ScramSha256), true)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let cm_body = shared::find_configmap_apply(&observed, "c1-broker-config").unwrap();
    let broker_toml = shared::extract_data_keys(&cm_body).remove("broker-0.toml").unwrap();
    assert!(broker_toml.contains("sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-256\"] }"));
}
```

- [ ] **Step 5: Implement test 4 — SCRAM listener with no users still reconciles**

```rust
#[tokio::test]
async fn scram_listener_without_kafkausers_still_reconciles() {
    // Confirms the reconciler doesn't fail with "no SCRAM users".
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let rules = shared::happy_path_rules("c1", "ns", &items);
    let (ctx, _state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::ScramSha512), true)],
    );
    reconcile(Arc::new(kafka), ctx).await.expect("should reconcile without users");
}
```

- [ ] **Step 6: Implement test 5 — mTLS without TLS fails validation, sets status**

```rust
#[tokio::test]
async fn listener_mtls_requires_tls_validation_error_surfaces_status() {
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let rules = shared::happy_path_rules("c1", "ns", &items);
    let (ctx, state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::Tls), false)],  // tls: false + mtls
    );
    let _result = reconcile(Arc::new(kafka), ctx).await;

    let observed = state.take_observed();
    let status_patch = shared::find_status_patch(&observed, "c1").expect("status patch");
    assert!(format!("{status_patch:?}").contains("ListenerMtlsRequiresTransportTls"));
    assert!(format!("{status_patch:?}").contains("ListenerValidationFailed"));
}
```

- [ ] **Step 7: Implement test 6 — auth change bumps config-hash**

```rust
#[tokio::test]
async fn auth_change_bumps_config_hash() {
    // Reconcile once with SCRAM, again with mTLS, assert StatefulSet
    // pod-template-hash annotation changed.
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let rules_scram = shared::happy_path_rules("c1", "ns", &items);
    let (ctx_a, state_a) = build_ctx("ns", rules_scram);
    let kafka_a = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::ScramSha512), true)],
    );
    reconcile(Arc::new(kafka_a), ctx_a).await.unwrap();
    let hash_a = shared::extract_pod_template_hash(&state_a.take_observed(), "c1-brokers-0");

    let rules_mtls = shared::happy_path_rules("c1", "ns", &items);
    let (ctx_b, state_b) = build_ctx("ns", rules_mtls);
    let kafka_b = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![internal_listener_with_auth(Some(ListenerAuthentication::Tls), true)],
    );
    reconcile(Arc::new(kafka_b), ctx_b).await.unwrap();
    let hash_b = shared::extract_pod_template_hash(&state_b.take_observed(), "c1-brokers-0");

    assert_ne!(hash_a, hash_b, "auth change should produce different config-hash");
}
```

- [ ] **Step 8: Implement test 7 — NodePort external SAN extension**

```rust
#[tokio::test]
async fn nodeport_listener_external_san_added_to_per_broker_cert() {
    let items = vec![fake_pool_list_item("brokers", "ns", "c1", 1, 1)];
    let mut rules = shared::happy_path_rules("c1", "ns", &items);
    // Add Node list mock with an external IP
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/api/v1/nodes".into(),
        response: Response::builder().status(200).body(
            serde_json::to_vec(&json!({
                "kind": "NodeList",
                "apiVersion": "v1",
                "items": [{
                    "metadata": {"name": "node1"},
                    "status": {"addresses": [
                        {"type": "ExternalIP", "address": "203.0.113.10"}
                    ]}
                }]
            })).unwrap(),
        ).unwrap(),
    });
    let (ctx, state) = build_ctx("ns", rules);

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns",
        vec![Listener {
            name: "ext".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: true,
            authentication: Some(ListenerAuthentication::ScramSha512),
            configuration: None,
            network_policy_peers: None,
        }],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let keystore = shared::find_secret_apply(&observed, "c1-kafka-brokers").unwrap();
    let cert_pem = shared::extract_secret_value(&keystore, "0.crt").unwrap();
    let parsed_sans = shared::parse_cert_sans(&cert_pem);
    assert!(parsed_sans.iter().any(|s| s.contains("203.0.113.10")));
}
```

Helpers in italics need to be added to `shared/mod.rs` if not present — mirror what `reconcile_inter_broker_mtls.rs` already does for ConfigMap/Secret extraction.

- [ ] **Step 9: Build + run the integration tests**

```
cargo test -p crabka-operator --test reconcile_listener_auth
```

Expected: PASS for all seven tests.

- [ ] **Step 10: Commit**

```
git add crates/operator/tests/reconcile_listener_auth.rs crates/operator/tests/shared/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: integration tests — reconcile_listener_auth"
```

---

### Task 10: kind e2e — three scenarios

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`
- Create / modify: helm chart sample manifests for the three e2e scenarios

- [ ] **Step 1: Inspect the existing kind e2e structure**

```
cat .github/workflows/operator-e2e.yml
ls tests/e2e/ 2>/dev/null || find . -path ./target -prune -o -name "kafka-cr*.yaml" -print
```

Identify (a) where existing e2e applies a `Kafka` CR and (b) where it asserts (kubectl get / wait / produce-consume Job).

- [ ] **Step 2: Add scenario A — SCRAM-SHA-512 over TLS, internal listener**

Add to `.github/workflows/operator-e2e.yml` after the existing scenarios:

```yaml
- name: e2e — SCRAM-SHA-512 over TLS, internal listener
  run: |
    kubectl apply -f tests/e2e/scram-tls-internal/kafka.yaml
    kubectl apply -f tests/e2e/scram-tls-internal/kafkauser.yaml
    kubectl wait --for=condition=Ready kafka/c1 --timeout=300s
    kubectl wait --for=jsonpath='{.status.username}'=alice kafkauser/alice --timeout=60s

    # Successful produce — uses alice's SCRAM password from the Secret
    kubectl apply -f tests/e2e/scram-tls-internal/producer-job.yaml
    kubectl wait --for=condition=complete job/producer-success --timeout=120s

    # Failure produce — anonymous (no creds)
    kubectl apply -f tests/e2e/scram-tls-internal/producer-job-anonymous.yaml
    # Job retries 3 times before giving up; ensure it failed.
    kubectl wait --for=condition=failed job/producer-anonymous --timeout=120s

    kubectl delete -f tests/e2e/scram-tls-internal/
```

Create the four manifest files under `tests/e2e/scram-tls-internal/`:

`kafka.yaml`:
```yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata:
  name: c1
spec:
  kafkaVersion: "0.1.1"
  listeners:
    - name: internal
      port: 9092
      type: internal
      tls: true
      authentication:
        type: scram-sha-512
```

`kafkauser.yaml`:
```yaml
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
  labels:
    crabka.io/cluster: c1
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: "*", patternType: prefix}
        operations: [Read, Write, Create, Describe]
```

`producer-job.yaml`:
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: producer-success
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: producer
          image: confluentinc/cp-kafka:6.1.1
          env:
            - name: SCRAM_PASSWORD
              valueFrom:
                secretKeyRef: {name: alice, key: password}
          command:
            - bash
            - -c
            - |
              cat >/tmp/client.props <<EOF
              security.protocol=SASL_SSL
              sasl.mechanism=SCRAM-SHA-512
              sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="$SCRAM_PASSWORD";
              ssl.truststore.location=/etc/crabka/cluster-ca/ca.jks
              ssl.truststore.password=changeit
              EOF
              # cluster-ca PEM → JKS conversion at runtime
              keytool -import -trustcacerts -noprompt -alias clusterca -file /etc/crabka/cluster-ca/ca.crt -keystore /etc/crabka/cluster-ca/ca.jks -storepass changeit
              echo "hello" | kafka-console-producer --broker-list c1-kafka-bootstrap:9092 --topic test --producer.config /tmp/client.props
          volumeMounts:
            - name: cluster-ca
              mountPath: /etc/crabka/cluster-ca
      volumes:
        - name: cluster-ca
          secret: {secretName: c1-cluster-ca-cert}
```

`producer-job-anonymous.yaml`: same as above but omit the `sasl.*` props and use plaintext bootstrap port — must fail.

- [ ] **Step 3: Scenario B — mTLS, internal listener**

Add another workflow block + manifests under `tests/e2e/mtls-internal/`. KafkaUser uses `authentication: type: tls`. Producer Job mounts the user Secret's `user.crt` / `user.key` and points `ssl.keystore.location` / `ssl.keystore.password` at them after converting PEM→PKCS12 with `openssl`. Verifies (a) cert-presenting Job succeeds, (b) no-cert Job fails.

- [ ] **Step 4: Scenario C — SCRAM-SHA-512 over TLS, NodePort listener**

Add manifests under `tests/e2e/scram-tls-nodeport/`. The Kafka CR has:

```yaml
spec:
  listeners:
    - name: external
      port: 9094
      type: nodeport
      tls: true
      authentication:
        type: scram-sha-512
```

The producer Job runs against the NodePort hostname (kind node IP + assigned NodePort). The workflow extracts the NodePort:

```bash
NODE_PORT=$(kubectl get svc c1-kafka-0-external -o jsonpath='{.spec.ports[0].nodePort}')
KIND_NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
sed -e "s/__BOOTSTRAP__/${KIND_NODE_IP}:${NODE_PORT}/" tests/e2e/scram-tls-nodeport/producer-job.template.yaml | kubectl apply -f -
```

The Job's `kafka-console-producer --broker-list ${KIND_NODE_IP}:${NODE_PORT}` runs in a pod on the kind node so it can resolve the node IP. Verifies the cert's SAN includes the kind node's IP (JVM hostname verification passes).

- [ ] **Step 5: Run the e2e workflow locally** if you have kind installed:

```
.github/workflows/scripts/run-e2e-locally.sh 2>/dev/null || echo "run via PR CI instead"
```

Most likely you'll PUSH the changes and watch the GitHub Actions e2e job for real validation.

- [ ] **Step 6: Commit**

```
git add .github/workflows/operator-e2e.yml tests/e2e/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: kind e2e — SCRAM+TLS internal, mTLS internal, SCRAM+TLS NodePort"
```

---

## Batch 6 — Documentation

### Task 11: STATUS.md slice-31 entry + Helm chart sample

**Files:**
- Modify: `STATUS.md`
- Modify: `charts/crabka-operator/values.yaml` (sample CR if it ships one)
- Possibly modify: `bench/manifests/crabka/kafka-cr-3broker-rf3.yaml` (bench sample)

- [ ] **Step 1: Append slice-31 entry to `STATUS.md`**

After the slice-30 block (ending around L1386), append:

```markdown
## Slice 31 — Operator: Listener auth wiring (TLS + SCRAM) (2026-05-21)

- New `Kafka.spec.listeners[].authentication`: Strimzi-shape
  `{ type: tls | scram-sha-512 | scram-sha-256 }`. mTLS (`type: tls`)
  requires `tls: true` on the listener.
- Operator emits per-listener inline TOML `tls_config = { ... }` /
  `sasl_config = { enabled_mechanisms = [...] }` blocks inside each
  `[[listeners]]` entry. Slice-30 top-level `[tls_config]` (controller
  / inter-broker) is preserved as a fallback.
- Broker `file_config.rs` parses the new per-listener blocks; accept
  loop resolves TLS material and SASL mechanisms per listener with
  fallback to broker-wide defaults. Inter-broker continues to read
  the top-level config; no slice-30 regression.
- Per-broker cert SAN list extended at issuance time with external
  advertised addresses computed from observed NodePort
  (`Node.status.addresses` with `type: ExternalIP`/`Hostname`) and
  LoadBalancer (`Service.status.loadBalancer.ingress[]`) state.
  `issue_broker_cert` gains an `extra_sans: &[SubjectAltName]`
  parameter; cert reissue is triggered when the SAN-list digest
  changes vs the cluster's `<cluster>-kafka-brokers` Secret entry.
- Validation: `tls: false + auth: tls` → `ListenerMtlsRequiresTransportTls`
  with `ListenerValidationFailed=True` status condition. SCRAM without
  TLS is accepted but produces a `WeakAuth` Warning Event each reconcile.
- LB ingress pending: per-broker cert issuance for affected brokers is
  skipped this reconcile (issued with internal SANs only) and
  `WaitingForLoadBalancerIp=True` is surfaced; reconcile requeues.
- Listener-auth changes flow through slice-21's config-hash → ordered
  rolling restart. Free — the rendered TOML is already in the hash.
- 5 operator unit-test files updated, 1 new (`reconcile_listener_auth.rs`).
  7 integration scenarios cover SCRAM-SSL render, mTLS render, SCRAM-256
  render, empty-credential reconcile, mTLS-without-TLS validation
  surfacing, auth-change config-hash bump, NodePort SAN extension.
- Broker: 2 new unit tests for TLS/SASL resolver fallback, 2 new
  end-to-end tests for per-listener SASL mechanism gating and clients-CA
  truststore rejection.
- kind e2e: 3 new scenarios — SCRAM-SHA-512 over TLS (internal),
  mTLS (internal), SCRAM-SHA-512 over TLS (NodePort, exercises SAN
  extension end-to-end).
- Out of scope: BYO server cert (`brokerCertChainAndKey`), OAuth/OIDC
  listener auth (slice 49), custom authentication plugin, Ingress/Route
  listener TLS (slice 27), non-disruptive auth hot-reload, PKCS#12
  user keystore bundle (slice-37 follow-up).
```

- [ ] **Step 2: Update Helm chart sample / bench manifest with one listener-auth example**

Edit `bench/manifests/crabka/kafka-cr-3broker-rf3.yaml` to demonstrate the new field (this manifest is also what new users tend to copy):

```yaml
spec:
  listeners:
    - name: internal
      port: 9092
      type: internal
      tls: true
      authentication:
        type: scram-sha-512
```

(Only add the listener block if the file doesn't already specify listeners. If it does, leave the existing config alone and add a comment with the new shape.)

- [ ] **Step 3: Final acceptance gate**

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: PASS on all three.

- [ ] **Step 4: Commit**

```
git add STATUS.md bench/manifests/crabka/kafka-cr-3broker-rf3.yaml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 31: STATUS.md entry + sample CR"
```

---

## Plan self-review notes (for the implementer)

- **CRD field name:** `authentication` (matches `KafkaUser.spec.authentication` shape in `crates/operator/src/crd/user.rs` L79-87). Not `auth`.
- **Enum variant names:** `Tls`, `ScramSha512`, `ScramSha256` (PascalCase Rust; serde renames to `tls`, `scram-sha-512`, `scram-sha-256` via `tag = "type"`).
- **The `unreachable!` in `listener_protocol`:** safe because `validate_listeners` runs before render; if a code path ever ends up rendering pre-validation, the panic will surface the bug at first reconcile.
- **`SubjectAltName` derives:** `Clone`, `PartialEq`, `Eq`, `PartialOrd`, `Ord` needed for the dedup / digest paths. Add them in the same commit that introduces `compute_extra_sans`.
- **Inline TOML tables for per-listener config:** chosen to avoid ambiguity with the array-of-tables `[[listeners]]`. `[listeners.tls_config]` after `[[listeners]]` is technically valid TOML but inline tables read more clearly and serde-toml parses them without subtleties.
- **Top-level `[tls_config]` back-compat:** the slice-30 controller / inter-broker setup continues to work. Per-listener wins when present; otherwise the broker falls back to the top-level. Tested explicitly in `top_level_tls_config_still_parses_back_compat`.
- **`extra_sans` `&[]` placeholder in Task 3:** intentional. Task 3 changes the signature without behavior change so the change is reviewable in isolation. Task 6 plumbs real values through.
- **`apply_to`** (if the conversion lives there): mirror the existing top-level TLS conversion exactly when wiring per-listener `tls_config`. Don't introduce a new conversion idiom.
- **kind e2e cluster-CA → JKS conversion:** the producer Job has to convert PEM → JKS at runtime because `kafka-console-producer` (JVM) takes JKS. The Job shown uses `keytool`. Alternative: bake a sidecar that does the conversion. Pick whichever pattern existing slice-30 e2e tests use (look for `keytool` in `tests/e2e/` or `.github/workflows/`).
- **`weak_auth_warnings`** intentionally emits one warning per reconcile per offending listener — loud is the point. If this becomes too noisy, dedupe in a follow-up.
- **Git author override on every commit:** the local repo has no `user.name` / `user.email`. CLAUDE.md forbids modifying git config. Use `git -c user.name="..." -c user.email="..." commit ...` always.

---

## Spec coverage check

Re-mapping every spec section to a task:

| Spec section                                                                 | Task(s)        |
|------------------------------------------------------------------------------|----------------|
| CRD shape (`ListenerAuthentication` enum, `authentication` field)            | T1             |
| Listener → broker protocol mapping (`listener_protocol`, `sasl_mechanism`)   | T4             |
| Validation rules table (incl. `ListenerMtlsRequiresTransportTls`)            | T4             |
| Removal of slice-25 `TlsNotYetSupported` hard rejection                      | T4             |
| Rendered broker TOML — per-listener TLS + SASL blocks                        | T4             |
| Broker-side per-listener TLS + SASL config (parsing + runtime types)         | T2             |
| Broker accept-loop per-listener TLS acceptor + SASL mechanism resolution     | T5             |
| Cluster CA SAN expansion (`issue_broker_cert` signature)                     | T3, T6         |
| `compute_extra_sans` for NodePort / LoadBalancer                             | T6             |
| Cert reissue on SAN-list change                                              | T7             |
| Pod spec / volume mounts (no changes — slice 30 already mounts clients-ca)   | — (no task)    |
| Reconcile pipeline ordering (listener-svcs → CA → broker-config)             | T6, T7         |
| `WeakAuth` Warning Event                                                     | T8             |
| `ListenerValidationFailed` status condition                                  | T8             |
| `WaitingForLoadBalancerIp` status condition                                  | T8             |
| Operator unit tests (CRD round-trip, validation, mapping, render snapshots)  | T1, T4         |
| Operator integration tests (7 scenarios in `reconcile_listener_auth.rs`)    | T9             |
| Broker unit tests (TLS/SASL resolver, top-level back-compat)                 | T2, T5         |
| Broker end-to-end tests (per-listener SASL gating, clients-CA rejection)     | T5             |
| kind e2e (3 scenarios)                                                       | T10            |
| STATUS.md slice-31 entry                                                     | T11            |
| Sample listener-auth in Helm chart / bench manifest                          | T11            |

No gaps.
