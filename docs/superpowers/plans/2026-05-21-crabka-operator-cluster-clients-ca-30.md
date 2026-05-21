# Crabka Operator Slice 30 — Cluster CA + clients CA generation (plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Per CLAUDE.md, dispatch tasks within a batch in parallel; sequential between batches. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Operator owns the full PKI lifecycle — two CAs (cluster + clients), per-broker keystore signed by the cluster CA, inter-broker mTLS on by default, declarative `validityDays` / `renewalDays` / BYO toggle on `Kafka.spec.{clusterCa,clientsCa}`, and a Helm-chart-shipped CronJob that drives leaf renewal.

**Spec:** [`docs/superpowers/specs/2026-05-21-crabka-operator-cluster-clients-ca-30-design.md`](../specs/2026-05-21-crabka-operator-cluster-clients-ca-30-design.md).

**Architecture:** `crates/security::ca` grows two pure helpers (`generate_cluster_ca`, `issue_broker_cert`). `crates/broker::file_config::FileConfig` grows the `controller_listener_protocol` + `tls_config` keys (broker side of inter-broker mTLS). A new `crates/operator/src/controller/cluster_ca.rs` module owns all reconciler-side PKI logic; the slice-37 `ensure_clients_ca` lazy-bootstrap helper in `user_tls.rs` is deleted outright (greenfield, per CLAUDE.md). The CronJob calls the operator binary with a new `ca-renewal-check` subcommand.

**Tech stack:** Rust 2024, `rcgen` 0.13, `x509-parser`, `kube-rs`, `k8s-openapi`, `schemars`, `crabka-security`, `crabka-broker`, Helm.

---

## Batch overview

| Batch | Tasks | Files (disjoint within batch) | Parallel? |
|---|---|---|---|
| 1 | T1, T2, T3 | `crates/security/src/ca.rs` ‖ `crates/broker/src/file_config.rs` ‖ `crates/operator/src/crd/{ca.rs,mod.rs,kafka.rs}` | yes |
| 2 | T4 | `crates/operator/src/controller/{cluster_ca.rs,mod.rs,user_tls.rs,user.rs}` | — (depends on T1, T3) |
| 3 | T5, T6, T7 | `controller/listeners.rs::render_broker_toml` ‖ `controller/common.rs::combined_config_hash` ‖ `controller/kafka_node_pool.rs::render_statefulset` | yes (depends on T2, T4) |
| 4 | T8 | `controller/kafka.rs` reconciler wiring + status population | — (depends on T4–T7) |
| 5 | T9 | `crates/operator/src/main.rs` CronJob CLI subcommand | — (depends on T4) |
| 6 | T10, T11, T12, T13 | `tests/reconcile_ca.rs` ‖ `tests/reconcile_inter_broker_mtls.rs` ‖ `tests/ca_renewal_cronjob.rs` ‖ `charts/crabka-operator/templates/*` | yes (depends on T8, T9) |
| 7 | T14, T15 | `deploy/crds/crabka.io_kafkas.yaml` (regen) ‖ `STATUS.md` | yes |

---

## Task 1 — `crates/security/src/ca.rs`: cluster CA + broker leaf cert helpers

**Files:**
- Modify: `crates/security/src/ca.rs`

- [ ] **Step 1.1: Add `SubjectAltName` enum**

In `crates/security/src/ca.rs`, after the existing `CaMaterial` struct, add:

```rust
use std::net::IpAddr;

/// SAN entry for a leaf cert. ECDSA leaf certs accept any mix of DNS
/// names and IP addresses; the broker-cert path uses a mix.
#[derive(Debug, Clone)]
pub enum SubjectAltName {
    Dns(String),
    Ip(IpAddr),
}
```

- [ ] **Step 1.2: Add `BrokerCert` output struct**

Below `UserCert`:

```rust
/// A broker leaf cert (server + client cert in one).
pub struct BrokerCert {
    pub cert_pem: String,
    pub key_pem: String,
    pub not_after: String,
}
```

- [ ] **Step 1.3: Add `generate_cluster_ca`**

Copy the body of `generate_clients_ca` and change the `OrganizationalUnitName` push — clients CA carries no OU today, so just add `OU=cluster` on the cluster CA so the two are distinguishable in audit chains:

```rust
/// Generate a self-signed cluster CA. Same shape as
/// [`generate_clients_ca`] (ECDSA P-256, CA:TRUE, KU keyCertSign +
/// cRLSign) but the subject DN carries `OU=cluster` so the cluster CA
/// and clients CA are trivially distinguishable in cert chains and
/// audit logs.
pub fn generate_cluster_ca(cn: &str, validity_days: u32) -> Result<CaMaterial, CaError> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

    let mut params = CertificateParams::new(Vec::<String>::new())?;
    let (not_before, not_after) = validity_window(validity_days)?;
    params.not_before = not_before;
    params.not_after = not_after;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn.push(DnType::OrganizationName, "crabka");
    dn.push(DnType::OrganizationalUnitName, "cluster");
    params.distinguished_name = dn;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let cert = params.self_signed(&key)?;

    Ok(CaMaterial {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}
```

- [ ] **Step 1.4: Add `issue_broker_cert`**

Signs a leaf with both `serverAuth` and `clientAuth` EKU, KU `digitalSignature + keyEncipherment`, and the caller-supplied SAN list:

```rust
use rcgen::SanType;

/// Sign a broker leaf cert: server cert + client cert in one
/// (EKU = serverAuth + clientAuth, KU = digitalSignature +
/// keyEncipherment). SANs accept a mix of DNS names and IPs. ECDSA
/// P-256.
pub fn issue_broker_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    sans: &[SubjectAltName],
    validity_days: u32,
) -> Result<BrokerCert, CaError> {
    let ca_key = KeyPair::from_pem(ca_key_pem)?;
    let ca_params = CertificateParams::from_ca_cert_pem(ca_cert_pem)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

    let mut params = CertificateParams::new(Vec::<String>::new())?;
    let (not_before, not_after) = validity_window(validity_days)?;
    params.not_before = not_before;
    params.not_after = not_after;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    params.distinguished_name = dn;

    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params.subject_alt_names = sans
        .iter()
        .map(|s| match s {
            SubjectAltName::Dns(d) => SanType::DnsName(d.parse().expect("valid Ia5String")),
            SubjectAltName::Ip(ip) => SanType::IpAddress(*ip),
        })
        .collect();

    let leaf = params.signed_by(&leaf_key, &ca_cert, &ca_key)?;
    let not_after_str = not_after.format(&Rfc3339)?;

    Ok(BrokerCert {
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
        not_after: not_after_str,
    })
}
```

- [ ] **Step 1.5: Tests — `generate_cluster_ca`**

In the existing `#[cfg(test)] mod tests` block, add:

```rust
#[test]
fn generate_cluster_ca_carries_ou_cluster() {
    let ca = generate_cluster_ca("c1", 365).expect("generate cluster CA");
    let der = pem_to_der(&ca.cert_pem);
    let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse cluster CA DER");
    let subject = cert.subject().to_string();
    assert!(subject.contains("CN=c1"), "subject was {subject}");
    assert!(subject.contains("O=crabka"), "subject was {subject}");
    assert!(subject.contains("OU=cluster"), "subject was {subject}");
    let bc = cert
        .basic_constraints()
        .expect("BC parse")
        .expect("BC present");
    assert!(bc.value.ca, "CA bit must be true on cluster CA");
}

#[test]
fn clients_ca_does_not_carry_ou_cluster() {
    let ca = generate_clients_ca("root", 365).expect("generate clients CA");
    let der = pem_to_der(&ca.cert_pem);
    let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse");
    let subject = cert.subject().to_string();
    assert!(
        !subject.contains("OU=cluster"),
        "clients CA must not carry OU=cluster; subject={subject}"
    );
}
```

- [ ] **Step 1.6: Tests — `issue_broker_cert` SANs + EKU**

```rust
#[test]
fn issue_broker_cert_has_server_and_client_auth_eku() {
    use std::net::Ipv4Addr;
    let ca = generate_cluster_ca("c1", 365).expect("CA");
    let sans = vec![
        SubjectAltName::Dns("c1-broker-0.c1-broker.default.svc.cluster.local".into()),
        SubjectAltName::Dns("c1-broker-0".into()),
        SubjectAltName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    let b = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "c1-broker-0", &sans, 365)
        .expect("issue broker cert");

    let der = pem_to_der(&b.cert_pem);
    let (_, leaf) = X509Certificate::from_der(der.as_ref()).expect("parse leaf");

    let eku = leaf
        .extended_key_usage()
        .expect("EKU parse")
        .expect("EKU present");
    assert!(eku.value.server_auth, "broker leaf must carry serverAuth");
    assert!(eku.value.client_auth, "broker leaf must carry clientAuth");

    // SANs round-trip.
    let san_ext = leaf
        .subject_alternative_name()
        .expect("SAN parse")
        .expect("SAN present");
    let general_names: Vec<_> = san_ext.value.general_names.iter().collect();
    assert!(general_names.iter().any(|gn| matches!(
        gn,
        x509_parser::extensions::GeneralName::DNSName(s) if *s == "c1-broker-0"
    )));
    assert!(general_names.iter().any(|gn| matches!(
        gn,
        x509_parser::extensions::GeneralName::IPAddress(_)
    )));
}

#[test]
fn issue_broker_cert_chains_to_cluster_ca() {
    let ca = generate_cluster_ca("c1", 365).expect("CA");
    let sans = vec![SubjectAltName::Dns("c1-broker-0".into())];
    let b = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "c1-broker-0", &sans, 365).expect("leaf");

    let leaf_der = pem_to_der(&b.cert_pem);
    let (_, leaf) = X509Certificate::from_der(leaf_der.as_ref()).expect("parse leaf");
    let ca_der = pem_to_der(&ca.cert_pem);
    let (_, ca_x509) = X509Certificate::from_der(ca_der.as_ref()).expect("parse CA");

    leaf.verify_signature(Some(ca_x509.public_key()))
        .expect("leaf signature must verify against cluster CA pubkey");
}
```

- [ ] **Step 1.7: Run + verify**

```bash
cargo test -p crabka-security ca::
```

Expected: PASS (existing 5 + 4 new = 9 tests).

- [ ] **Step 1.8: Commit**

```bash
git add crates/security/src/ca.rs
git commit -m "Slice 30/1: crabka-security — generate_cluster_ca + issue_broker_cert + SubjectAltName"
```

---

## Task 2 — `crates/broker/src/file_config.rs`: TLS keys

**Files:**
- Modify: `crates/broker/src/file_config.rs`

- [ ] **Step 2.1: Add `controller_listener_protocol` + `tls_config` fields to `FileConfig`**

```rust
use crabka_security::TlsConfig;

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileConfig {
    pub broker_id: Option<i32>,
    pub log_dir: Option<String>,
    pub inter_broker_listener_name: Option<String>,
    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Slice 30: controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[serde(default)]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// Slice 30: TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    #[serde(default)]
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}
```

- [ ] **Step 2.2: Propagate in `apply_to`**

In `FileConfig::apply_to`, after the existing `server_properties` line, add:

```rust
if let Some(proto) = self.controller_listener_protocol
    && cfg.controller_listener_protocol == defaults.controller_listener_protocol
{
    cfg.controller_listener_protocol = proto;
}
if let Some(tls) = self.tls_config
    && cfg.tls_config.is_none()
{
    use crabka_security::tls::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
    cfg.tls_config = Some(BrokerTlsConfig {
        cert_path: tls.cert_path,
        key_path: tls.key_path,
        client_ca_path: tls.client_ca_path,
        client_auth: match tls.client_auth {
            FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
            FileClientAuthMode::Optional => ClientAuthMode::Optional,
            FileClientAuthMode::Required => ClientAuthMode::Required,
        },
    });
}
```

Note: the existing `BrokerConfig` already carries `controller_listener_protocol` and `tls_config` (slice 12b). This step only wires the TOML surface to them. If `TlsConfig`'s actual field names differ from the above, mirror what `crates/security/src/tls.rs::TlsConfig` defines — the test in Step 2.4 will catch mismatch.

- [ ] **Step 2.3: Test — TLS keys round-trip**

In the existing `#[cfg(test)] mod tests`, add:

```rust
#[test]
fn tls_keys_round_trip() {
    let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/crabka/broker-tls/0.crt"
key_path  = "/etc/crabka/broker-tls/0.key"
client_ca_path = "/etc/crabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
    let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
    assert_eq!(
        cfg.controller_listener_protocol,
        Some(ListenerProtocol::Ssl)
    );
    let tls = cfg.tls_config.expect("tls_config present");
    assert_eq!(tls.cert_path, std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt"));
    assert_eq!(tls.client_auth, FileClientAuthMode::Required);
}

#[test]
fn tls_keys_absent_round_trips() {
    let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
    let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
    assert_eq!(cfg.controller_listener_protocol, None);
    assert!(cfg.tls_config.is_none());
}

#[test]
fn apply_to_propagates_tls_config() {
    let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
    let file: FileConfig = toml::from_str(src).expect("parse");
    let mut cfg = crate::config::BrokerConfig::default();
    file.apply_to(&mut cfg);
    assert_eq!(
        cfg.controller_listener_protocol,
        crabka_security::ListenerProtocol::Ssl
    );
    let tls = cfg.tls_config.expect("tls_config propagated");
    assert_eq!(tls.cert_path, std::path::PathBuf::from("/c"));
}
```

- [ ] **Step 2.4: Run + verify**

```bash
cargo test -p crabka-broker file_config::
```

Expected: PASS (existing + 3 new = N+3 tests).

- [ ] **Step 2.5: Commit**

```bash
git add crates/broker/src/file_config.rs
git commit -m "Slice 30/2: broker FileConfig — controller_listener_protocol + tls_config keys"
```

---

## Task 3 — `crates/operator/src/crd/ca.rs` + `KafkaSpec` fields

**Files:**
- Create: `crates/operator/src/crd/ca.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/crd/kafka.rs`

- [ ] **Step 3.1: Create `crd/ca.rs`**

```rust
//! Slice 30: `Kafka.spec.clusterCa` + `Kafka.spec.clientsCa` schema.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Per-CA declarative config. Strimzi-shaped.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthority {
    /// When `true` (default), the operator generates and renews this CA.
    /// When `false`, the operator expects the CA `Secret` pair to be
    /// pre-created by the cluster admin and refuses to overwrite them.
    /// Renewal of BYO CAs is the admin's responsibility; the CronJob
    /// skips them and emits an Event when they're nearing expiry.
    #[serde(default = "default_generate")]
    pub generate_certificate_authority: bool,

    /// Cert validity in days. Default 365.
    #[serde(default = "default_validity_days")]
    pub validity_days: u32,

    /// Window before `notAfter` in which the renewal CronJob will
    /// reissue leaf certs. Default 30.
    #[serde(default = "default_renewal_days")]
    pub renewal_days: u32,
}

#[must_use]
const fn default_generate() -> bool {
    true
}
#[must_use]
const fn default_validity_days() -> u32 {
    365
}
#[must_use]
const fn default_renewal_days() -> u32 {
    30
}

impl Default for CertificateAuthority {
    fn default() -> Self {
        Self {
            generate_certificate_authority: default_generate(),
            validity_days: default_validity_days(),
            renewal_days: default_renewal_days(),
        }
    }
}

/// Status surface for a single CA. Populated by the reconciler from the
/// parsed CA cert + the CRD spec.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthorityStatus {
    /// RFC3339 `notAfter` of the current CA cert.
    pub not_after: String,
    /// `true` when the operator generated this CA (i.e.
    /// `generateCertificateAuthority == true`); `false` for BYO.
    pub generated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_strimzi() {
        let d = CertificateAuthority::default();
        assert!(d.generate_certificate_authority);
        assert_eq!(d.validity_days, 365);
        assert_eq!(d.renewal_days, 30);
    }

    #[test]
    fn deserialize_empty_object_uses_defaults() {
        let v: CertificateAuthority = serde_json::from_value(serde_json::json!({})).expect("parse");
        assert_eq!(v, CertificateAuthority::default());
    }

    #[test]
    fn byo_round_trips() {
        let v: CertificateAuthority = serde_json::from_value(serde_json::json!({
            "generateCertificateAuthority": false,
            "validityDays": 90,
            "renewalDays": 7,
        }))
        .expect("parse");
        assert!(!v.generate_certificate_authority);
        assert_eq!(v.validity_days, 90);
        assert_eq!(v.renewal_days, 7);
    }
}
```

- [ ] **Step 3.2: Register the module**

In `crates/operator/src/crd/mod.rs`, after the existing `pub mod` lines, add:

```rust
pub mod ca;
pub use ca::{CertificateAuthority, CertificateAuthorityStatus};
```

- [ ] **Step 3.3: Add CA fields to `KafkaSpec`**

In `crates/operator/src/crd/kafka.rs`, locate `KafkaSpec` and add the two fields next to the existing `listeners` / `network_policy` / `metrics_config` fields:

```rust
/// Slice 30: per-cluster CA used for inter-broker mTLS + broker certs.
/// Absent → fully-defaulted `CertificateAuthority` (operator-generated,
/// 365/30 days).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub cluster_ca: Option<crate::crd::CertificateAuthority>,

/// Slice 30: per-cluster CA used to sign `KafkaUser` TLS certs (slice
/// 37). Absent → fully-defaulted `CertificateAuthority`.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub clients_ca: Option<crate::crd::CertificateAuthority>,
```

- [ ] **Step 3.4: Add status fields to `KafkaStatus`**

In the same file's `KafkaStatus` struct, add:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub cluster_ca: Option<crate::crd::CertificateAuthorityStatus>,

#[serde(default, skip_serializing_if = "Option::is_none")]
pub clients_ca: Option<crate::crd::CertificateAuthorityStatus>,
```

- [ ] **Step 3.5: Test — KafkaSpec parses with and without CA fields**

In `crates/operator/src/crd/kafka.rs`'s existing `#[cfg(test)] mod tests`, add:

```rust
#[test]
fn kafka_spec_parses_without_ca_fields() {
    let v: KafkaSpec = serde_json::from_value(serde_json::json!({
        "kafkaVersion": "3.7.0",
        "replicas": 1,
    }))
    .expect("parse minimal spec");
    assert!(v.cluster_ca.is_none());
    assert!(v.clients_ca.is_none());
}

#[test]
fn kafka_spec_parses_with_ca_fields() {
    let v: KafkaSpec = serde_json::from_value(serde_json::json!({
        "kafkaVersion": "3.7.0",
        "replicas": 1,
        "clusterCa": { "validityDays": 30 },
        "clientsCa": { "generateCertificateAuthority": false },
    }))
    .expect("parse with CAs");
    assert_eq!(v.cluster_ca.as_ref().unwrap().validity_days, 30);
    assert!(!v.clients_ca.as_ref().unwrap().generate_certificate_authority);
}
```

- [ ] **Step 3.6: Run + verify**

```bash
cargo test -p crabka-operator crd::
```

Expected: PASS (existing + 5 new tests).

- [ ] **Step 3.7: Commit**

```bash
git add crates/operator/src/crd/ca.rs crates/operator/src/crd/mod.rs crates/operator/src/crd/kafka.rs
git commit -m "Slice 30/3: KafkaSpec.{clusterCa,clientsCa} + CertificateAuthority schema"
```

---

## Task 4 — `crates/operator/src/controller/cluster_ca.rs`: get-or-create + keystore + renewal

**Files:**
- Create: `crates/operator/src/controller/cluster_ca.rs`
- Modify: `crates/operator/src/controller/mod.rs`
- Modify: `crates/operator/src/controller/user_tls.rs` (delete `ensure_clients_ca`)
- Modify: `crates/operator/src/controller/user.rs` (re-point caller)

- [ ] **Step 4.1: Create `cluster_ca.rs` — constants + Secret name helpers**

```rust
//! Slice 30: cluster CA + clients CA lifecycle.
//!
//! Owns:
//! - the per-cluster `cluster CA` Secret pair (private key + public cert),
//! - the per-cluster `clients CA` Secret pair (formerly in `user_tls.rs`),
//! - the per-cluster broker-keystore Secret (`<cluster>-kafka-brokers`),
//! - the pure `renew_if_expiring` predicate (called by both the
//!   reconciler-side `ensure_*` helpers and the `ca-renewal-check`
//!   CronJob subcommand),
//! - the `run_renewal_check` entrypoint for the CronJob.

use std::collections::BTreeMap;
use std::net::IpAddr;

use crabka_security::ca::{
    self, BrokerCert, CaMaterial, SubjectAltName, generate_clients_ca, generate_cluster_ca,
    issue_broker_cert,
};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, Patch, PatchParams};
use kube::{Resource, ResourceExt as _};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::controller::common::{FIELD_MANAGER, ReconcileError, owner_ref};
use crate::crd::{CertificateAuthority, Kafka};

/// Suffix on the cluster-CA private-key Secret.
pub(crate) const CLUSTER_CA_KEY_SUFFIX: &str = "-cluster-ca";
/// Suffix on the cluster-CA public-cert Secret.
pub(crate) const CLUSTER_CA_CERT_SUFFIX: &str = "-cluster-ca-cert";
/// Suffix on the clients-CA private-key Secret (preserved from slice 37
/// — Strimzi-shaped, never breaks per CLAUDE.md greenfield).
pub(crate) const CLIENTS_CA_KEY_SUFFIX: &str = "-clients-ca";
/// Suffix on the clients-CA public-cert Secret.
pub(crate) const CLIENTS_CA_CERT_SUFFIX: &str = "-clients-ca-cert";
/// Suffix on the per-cluster broker keystore Secret. Holds
/// `<broker_id>.crt` + `<broker_id>.key` for every replica.
pub(crate) const BROKER_KEYSTORE_SUFFIX: &str = "-kafka-brokers";

/// Default validity for CAs themselves: 10 years (matches the slice-37
/// lazy-bootstrap). Per-CA `validity_days` overrides this in `ensure_*_ca`
/// only when `generateCertificateAuthority == true`; the same default
/// applies to BYO CAs that were never inspected.
const CA_VALIDITY_DAYS: u32 = 10 * 365;

#[must_use]
pub(crate) fn cluster_ca_key_name(cluster: &str) -> String {
    format!("{cluster}{CLUSTER_CA_KEY_SUFFIX}")
}
#[must_use]
pub(crate) fn cluster_ca_cert_name(cluster: &str) -> String {
    format!("{cluster}{CLUSTER_CA_CERT_SUFFIX}")
}
#[must_use]
pub(crate) fn clients_ca_key_name(cluster: &str) -> String {
    format!("{cluster}{CLIENTS_CA_KEY_SUFFIX}")
}
#[must_use]
pub(crate) fn clients_ca_cert_name(cluster: &str) -> String {
    format!("{cluster}{CLIENTS_CA_CERT_SUFFIX}")
}
#[must_use]
pub(crate) fn broker_keystore_name(cluster: &str) -> String {
    format!("{cluster}{BROKER_KEYSTORE_SUFFIX}")
}
```

- [ ] **Step 4.2: Add the BYO-aware `ensure_ca` core helper**

Below the constants:

```rust
/// What the operator did this reconcile for a given CA. Status fodder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaAction {
    GeneratedNew,
    Reused,
    /// BYO CA — Secrets exist + parseable.
    AdoptedByo,
}

#[derive(Debug, Clone)]
pub(crate) struct CaOutcome {
    pub material: CaMaterial,
    pub action: CaAction,
    /// RFC3339 `notAfter` parsed from the cert.
    pub not_after: String,
    /// `false` when BYO.
    pub generated: bool,
}

/// Discriminator for the two CAs. Used only for error messages and
/// status condition names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WhichCa {
    Cluster,
    Clients,
}

impl WhichCa {
    pub(crate) fn cn_suffix(self) -> &'static str {
        match self {
            Self::Cluster => "-cluster-ca",
            Self::Clients => "-clients-ca",
        }
    }
    pub(crate) fn condition_name(self) -> &'static str {
        match self {
            Self::Cluster => "ClusterCaReady",
            Self::Clients => "ClientsCaReady",
        }
    }
}

/// Read the embedded `ca.crt` / `ca.key` PEM from a Secret. None when
/// the key is absent or not UTF-8 (treated as "regenerate").
fn read_pem_key(secret: &Secret, key: &str) -> Option<String> {
    let data = secret.data.as_ref()?;
    let bytes = &data.get(key)?.0;
    String::from_utf8(bytes.clone()).ok()
}

/// Parse `notAfter` from a PEM cert. Returns `ReconcileError::CertParse`
/// on a malformed cert. Used by both the reconciler and the CronJob.
pub(crate) fn cert_not_after(pem: &str) -> Result<OffsetDateTime, ReconcileError> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use x509_parser::prelude::FromDer;
    use x509_parser::prelude::X509Certificate;
    let der = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or_else(|| ReconcileError::CertParse("no PEM block".into()))?
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    OffsetDateTime::from_unix_timestamp(cert.validity().not_after.timestamp())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))
}

/// Get-or-create a CA Secret pair (private key + public cert).
///
/// - When `spec.generateCertificateAuthority == true` and both Secrets
///   are present + parseable: reuse (CaAction::Reused).
/// - When both are absent: generate (CaAction::GeneratedNew). PATCHes
///   both Secrets via SSA, owner-ref'd to the parent `Kafka`.
/// - When one is present and the other is missing OR malformed: treat
///   as fully-missing and regenerate (paired bootstrap is atomic).
/// - When `spec.generateCertificateAuthority == false`: BOTH Secrets
///   MUST be present and parseable, else `ReconcileError::ByoCaMissing`.
pub(crate) async fn ensure_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    spec: &CertificateAuthority,
    which: WhichCa,
) -> Result<CaOutcome, ReconcileError> {
    let cluster = kafka.name_any();
    let (key_name, cert_name) = match which {
        WhichCa::Cluster => (cluster_ca_key_name(&cluster), cluster_ca_cert_name(&cluster)),
        WhichCa::Clients => (clients_ca_key_name(&cluster), clients_ca_cert_name(&cluster)),
    };

    let existing_key = secret_api.get_opt(&key_name).await?;
    let existing_cert = secret_api.get_opt(&cert_name).await?;

    if let (Some(k), Some(c)) = (&existing_key, &existing_cert)
        && let (Some(key_pem), Some(cert_pem)) =
            (read_pem_key(k, "ca.key"), read_pem_key(c, "ca.crt"))
    {
        let not_after = cert_not_after(&cert_pem)?
            .format(&Rfc3339)
            .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
        let action = if spec.generate_certificate_authority {
            CaAction::Reused
        } else {
            CaAction::AdoptedByo
        };
        return Ok(CaOutcome {
            material: CaMaterial { cert_pem, key_pem },
            action,
            not_after,
            generated: spec.generate_certificate_authority,
        });
    }

    if !spec.generate_certificate_authority {
        return Err(ReconcileError::ByoCaMissing {
            which: which.condition_name().into(),
        });
    }

    let cn = format!("{cluster}{}", which.cn_suffix());
    let material = match which {
        WhichCa::Cluster => generate_cluster_ca(&cn, CA_VALIDITY_DAYS)?,
        WhichCa::Clients => generate_clients_ca(&cn, CA_VALIDITY_DAYS)?,
    };

    let key_secret = render_ca_secret(kafka, &key_name, "ca.key", &material.key_pem, "ca-key")?;
    let cert_secret = render_ca_secret(
        kafka,
        &cert_name,
        "ca.crt",
        &material.cert_pem,
        "ca-cert",
    )?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&key_name, &params, &Patch::Apply(&key_secret))
        .await?;
    secret_api
        .patch(&cert_name, &params, &Patch::Apply(&cert_secret))
        .await?;

    let not_after = cert_not_after(&material.cert_pem)?
        .format(&Rfc3339)
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;

    Ok(CaOutcome {
        material,
        action: CaAction::GeneratedNew,
        not_after,
        generated: true,
    })
}

fn render_ca_secret(
    kafka: &Kafka,
    name: &str,
    key: &str,
    pem: &str,
    secret_type_label: &str,
) -> Result<Secret, ReconcileError> {
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/secret-type".into(), secret_type_label.into());
    labels.insert(
        "crabka.io/cluster".into(),
        Kafka::resource_name_unchecked(kafka).into(),
    );
    let mut annotations = BTreeMap::new();
    annotations.insert(
        "crabka.io/strictly-operator-managed".into(),
        "true".into(),
    );
    let mut data = BTreeMap::new();
    data.insert(key.to_string(), ByteString(pem.as_bytes().to_vec()));
    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: kafka.meta().namespace.clone(),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}
```

- [ ] **Step 4.3: Add `ReconcileError::ByoCaMissing`**

In `crates/operator/src/controller/common.rs`, locate the `ReconcileError` enum and add (in alphabetical-friendly order; keep variants grouped logically):

```rust
#[error("BYO CA missing: {which} requires pre-existing Secret pair (generateCertificateAuthority=false)")]
ByoCaMissing { which: String },
#[error("BYO CA malformed: {which}: {reason}")]
ByoCaMalformed { which: String, reason: String },
```

(`ByoCaMalformed` is unused in this slice but reserved for the `cert_not_after` parse-error path. Keep alongside `ByoCaMissing` to colocate BYO error surface.)

- [ ] **Step 4.4: Public-facing helpers `ensure_cluster_ca` + `ensure_clients_ca`**

Append to `cluster_ca.rs`:

```rust
pub(crate) async fn ensure_cluster_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
) -> Result<CaOutcome, ReconcileError> {
    let spec = kafka.spec.cluster_ca.clone().unwrap_or_default();
    ensure_ca(secret_api, kafka, &spec, WhichCa::Cluster).await
}

pub(crate) async fn ensure_clients_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
) -> Result<CaOutcome, ReconcileError> {
    let spec = kafka.spec.clients_ca.clone().unwrap_or_default();
    ensure_ca(secret_api, kafka, &spec, WhichCa::Clients).await
}
```

- [ ] **Step 4.5: `ensure_broker_keystore`**

```rust
/// Status surface from `ensure_broker_keystore`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrokerKeystoreStatus {
    pub issued: Vec<i32>,
    pub reused: Vec<i32>,
    pub pruned: Vec<i32>,
}

/// Per-replica SAN list. Brokers reach each other via the headless
/// service so SANs cover:
/// - `<cluster>-broker-<id>.<cluster>-broker.<ns>.svc.cluster.local` (FQDN)
/// - `<cluster>-broker-<id>` (short)
/// - `<cluster>-broker.<ns>.svc.cluster.local` (headless service)
/// - `127.0.0.1` (loopback for local kube tests)
fn broker_sans(cluster: &str, namespace: &str, broker_id: i32) -> Vec<SubjectAltName> {
    let pod = format!("{cluster}-broker-{broker_id}");
    let pod_fqdn = format!("{pod}.{cluster}-broker.{namespace}.svc.cluster.local");
    let headless = format!("{cluster}-broker.{namespace}.svc.cluster.local");
    vec![
        SubjectAltName::Dns(pod_fqdn),
        SubjectAltName::Dns(pod),
        SubjectAltName::Dns(headless),
        SubjectAltName::Ip(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ]
}

/// Get-or-create the per-cluster broker keystore Secret.
///
/// Idempotent across reconciles: existing leaf cert entries are
/// **never** renewed by this function (the CronJob owns renewal). Only
/// missing or out-of-range broker ids cause changes:
/// - `broker_ids` that already have `<id>.crt` + `<id>.key` keys: reuse.
/// - missing `broker_ids`: issue new leaf cert signed by `cluster_ca`,
///   add to the Secret data map.
/// - extra entries that aren't in `broker_ids`: remove (replica
///   scale-down).
pub(crate) async fn ensure_broker_keystore(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    broker_ids: &[i32],
    cluster_ca: &CaMaterial,
) -> Result<BrokerKeystoreStatus, ReconcileError> {
    let cluster = kafka.name_any();
    let namespace = kafka.meta().namespace.clone().unwrap_or_default();
    let name = broker_keystore_name(&cluster);

    let validity = kafka
        .spec
        .cluster_ca
        .as_ref()
        .map(|c| c.validity_days)
        .unwrap_or(365);

    let existing = secret_api.get_opt(&name).await?;
    let mut data: BTreeMap<String, ByteString> = existing
        .as_ref()
        .and_then(|s| s.data.clone())
        .unwrap_or_default();

    let mut issued = Vec::new();
    let mut reused = Vec::new();

    for &id in broker_ids {
        let crt_key = format!("{id}.crt");
        let key_key = format!("{id}.key");
        if data.contains_key(&crt_key) && data.contains_key(&key_key) {
            reused.push(id);
            continue;
        }
        let cn = format!("{cluster}-broker-{id}");
        let sans = broker_sans(&cluster, &namespace, id);
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &cn,
            &sans,
            validity,
        )?;
        data.insert(crt_key, ByteString(leaf.cert_pem.into_bytes()));
        data.insert(key_key, ByteString(leaf.key_pem.into_bytes()));
        issued.push(id);
    }

    // Prune entries for retired broker ids.
    let want_keys: std::collections::HashSet<String> = broker_ids
        .iter()
        .flat_map(|id| [format!("{id}.crt"), format!("{id}.key")])
        .collect();
    let mut pruned_ids = std::collections::BTreeSet::new();
    data.retain(|k, _| {
        if want_keys.contains(k) {
            true
        } else if let Some((id_str, _)) = k.split_once('.')
            && let Ok(id) = id_str.parse::<i32>()
        {
            pruned_ids.insert(id);
            false
        } else {
            true
        }
    });
    let pruned: Vec<i32> = pruned_ids.into_iter().collect();

    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/secret-type".into(), "broker-keystore".into());
    labels.insert("crabka.io/cluster".into(), cluster.clone());

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    };

    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&name, &params, &Patch::Apply(&secret))
        .await?;

    Ok(BrokerKeystoreStatus {
        issued,
        reused,
        pruned,
    })
}
```

- [ ] **Step 4.6: `renew_if_expiring` — the pure renewal predicate**

```rust
/// Pure predicate. Called by the reconciler (never — reconciler only
/// *creates*) and by the CronJob (always — CronJob is the renewal lane).
#[must_use]
pub fn renew_if_expiring(
    cert_pem: &str,
    renewal_days: u32,
    now: OffsetDateTime,
) -> Result<bool, ReconcileError> {
    let not_after = cert_not_after(cert_pem)?;
    Ok(not_after - now <= time::Duration::days(i64::from(renewal_days)))
}
```

- [ ] **Step 4.7: `run_renewal_check` — CronJob entry point**

```rust
use k8s_openapi::api::core::v1::Event;
use kube::api::{ListParams, PostParams};

/// Iterate every Kafka CR in scope and:
/// - Reissue each broker leaf whose `notAfter` is within `renewalDays`.
/// - Flag each CA whose `notAfter` is within `renewalDays` via Event +
///   status condition `CaRotationRequired=True` (BYO → `ByoCaExpiringSoon`).
///
/// Returns `Ok(())` even when individual clusters fail; per-cluster
/// errors are logged + emitted as Events. The CronJob pod exits 0
/// unless something fatal (kube-apiserver unreachable) happens.
pub async fn run_renewal_check(
    client: kube::Client,
    namespace: Option<&str>,
) -> Result<(), ReconcileError> {
    let kafkas: Api<Kafka> = if let Some(ns) = namespace {
        Api::namespaced(client.clone(), ns)
    } else {
        Api::all(client.clone())
    };
    let list = kafkas.list(&ListParams::default()).await?;
    for kafka in list {
        if let Err(e) = renew_one(&client, &kafka).await {
            tracing::error!(
                cluster = %kafka.name_any(),
                error = %e,
                "ca-renewal-check: cluster failed"
            );
        }
    }
    Ok(())
}

async fn renew_one(client: &kube::Client, kafka: &Kafka) -> Result<(), ReconcileError> {
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    let cluster = kafka.name_any();
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), &ns);
    let now = OffsetDateTime::now_utc();

    // Read CAs (don't generate — that's the reconciler's job).
    let cluster_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Cluster).await?;
    let _clients_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Clients).await?;

    let cluster_ca_spec = kafka.spec.cluster_ca.clone().unwrap_or_default();
    let clients_ca_spec = kafka.spec.clients_ca.clone().unwrap_or_default();

    flag_ca_if_expiring(client, kafka, &cluster_ca.cert_pem, &cluster_ca_spec, WhichCa::Cluster, now).await?;
    if let Some(_) = read_existing_ca_optional(&secret_api, &cluster, WhichCa::Clients).await? {
        // Borrow the same flag function; we re-read clients_ca above as a sanity check.
    }

    // Renew broker leafs against the still-valid cluster CA.
    if cluster_ca_spec.generate_certificate_authority {
        renew_broker_leafs(
            client,
            kafka,
            &cluster_ca,
            cluster_ca_spec.renewal_days,
            cluster_ca_spec.validity_days,
            now,
        )
        .await?;
    }
    Ok(())
}

async fn read_existing_ca(
    secret_api: &Api<Secret>,
    cluster: &str,
    which: WhichCa,
) -> Result<CaMaterial, ReconcileError> {
    let (key_name, cert_name) = match which {
        WhichCa::Cluster => (cluster_ca_key_name(cluster), cluster_ca_cert_name(cluster)),
        WhichCa::Clients => (clients_ca_key_name(cluster), clients_ca_cert_name(cluster)),
    };
    let key_secret = secret_api
        .get_opt(&key_name)
        .await?
        .ok_or_else(|| ReconcileError::CertParse(format!("Secret {key_name} missing")))?;
    let cert_secret = secret_api
        .get_opt(&cert_name)
        .await?
        .ok_or_else(|| ReconcileError::CertParse(format!("Secret {cert_name} missing")))?;
    let key_pem = read_pem_key(&key_secret, "ca.key")
        .ok_or_else(|| ReconcileError::CertParse(format!("{key_name} ca.key unreadable")))?;
    let cert_pem = read_pem_key(&cert_secret, "ca.crt")
        .ok_or_else(|| ReconcileError::CertParse(format!("{cert_name} ca.crt unreadable")))?;
    Ok(CaMaterial { cert_pem, key_pem })
}

async fn read_existing_ca_optional(
    secret_api: &Api<Secret>,
    cluster: &str,
    which: WhichCa,
) -> Result<Option<CaMaterial>, ReconcileError> {
    match read_existing_ca(secret_api, cluster, which).await {
        Ok(m) => Ok(Some(m)),
        Err(_) => Ok(None),
    }
}

async fn flag_ca_if_expiring(
    client: &kube::Client,
    kafka: &Kafka,
    ca_cert_pem: &str,
    spec: &CertificateAuthority,
    which: WhichCa,
    now: OffsetDateTime,
) -> Result<(), ReconcileError> {
    if !renew_if_expiring(ca_cert_pem, spec.renewal_days, now)? {
        return Ok(());
    }
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    let reason = if spec.generate_certificate_authority {
        match which {
            WhichCa::Cluster => "CaRotationRequired",
            WhichCa::Clients => "CaRotationRequired",
        }
    } else {
        "ByoCaExpiringSoon"
    };
    emit_event(
        client,
        &ns,
        kafka,
        "Warning",
        reason,
        &format!(
            "CA {} is expiring within renewalDays; rotation is {}",
            which.condition_name(),
            if spec.generate_certificate_authority {
                "deferred until slice 34"
            } else {
                "the cluster admin's responsibility (BYO)"
            }
        ),
    )
    .await
}

async fn renew_broker_leafs(
    client: &kube::Client,
    kafka: &Kafka,
    cluster_ca: &CaMaterial,
    renewal_days: u32,
    validity_days: u32,
    now: OffsetDateTime,
) -> Result<(), ReconcileError> {
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    let cluster = kafka.name_any();
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), &ns);
    let name = broker_keystore_name(&cluster);
    let Some(mut secret) = secret_api.get_opt(&name).await? else {
        return Ok(());
    };
    let Some(mut data) = secret.data.take() else {
        return Ok(());
    };

    let mut renewed_ids = Vec::new();
    let crt_keys: Vec<String> = data
        .keys()
        .filter(|k| k.ends_with(".crt"))
        .cloned()
        .collect();
    for crt_key in crt_keys {
        let Some((id_str, _)) = crt_key.split_once('.') else {
            continue;
        };
        let Ok(id) = id_str.parse::<i32>() else {
            continue;
        };
        let Some(cert_bytes) = data.get(&crt_key) else {
            continue;
        };
        let Ok(cert_pem) = std::str::from_utf8(&cert_bytes.0) else {
            continue;
        };
        if !renew_if_expiring(cert_pem, renewal_days, now)? {
            continue;
        }
        let cn = format!("{cluster}-broker-{id}");
        let sans = broker_sans(&cluster, &ns, id);
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &cn,
            &sans,
            validity_days,
        )?;
        data.insert(crt_key.clone(), ByteString(leaf.cert_pem.into_bytes()));
        data.insert(format!("{id}.key"), ByteString(leaf.key_pem.into_bytes()));
        renewed_ids.push(id);
    }
    if renewed_ids.is_empty() {
        return Ok(());
    }
    secret.data = Some(data);
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&name, &params, &Patch::Apply(&secret))
        .await?;

    for id in renewed_ids {
        emit_event(
            client,
            &ns,
            kafka,
            "Normal",
            "BrokerCertRenewed",
            &format!("broker={id} reissued by ca-renewal-check"),
        )
        .await?;
    }
    Ok(())
}

async fn emit_event(
    client: &kube::Client,
    namespace: &str,
    kafka: &Kafka,
    type_: &str,
    reason: &str,
    message: &str,
) -> Result<(), ReconcileError> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
    let now = chrono::Utc::now();
    let event = Event {
        metadata: ObjectMeta {
            generate_name: Some("crabka-ca-renewal-".into()),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        type_: Some(type_.into()),
        reason: Some(reason.into()),
        message: Some(message.into()),
        involved_object: k8s_openapi::api::core::v1::ObjectReference {
            api_version: Some("crabka.io/v1alpha1".into()),
            kind: Some("Kafka".into()),
            name: Some(kafka.name_any()),
            namespace: Some(namespace.into()),
            uid: kafka.meta().uid.clone(),
            ..Default::default()
        },
        event_time: Some(MicroTime(now)),
        action: Some("RenewalCheck".into()),
        reporting_component: Some("crabka-operator/ca-renewal-check".into()),
        reporting_instance: Some(
            std::env::var("POD_NAME").unwrap_or_else(|_| "crabka-operator-renewal".into()),
        ),
        ..Default::default()
    };
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    api.create(&PostParams::default(), &event).await?;
    Ok(())
}
```

- [ ] **Step 4.8: Register the module**

In `crates/operator/src/controller/mod.rs`, add (alphabetical-ish next to existing pubs):

```rust
pub mod cluster_ca;
```

- [ ] **Step 4.9: Delete `ensure_clients_ca` from `user_tls.rs`**

Remove the entire `ensure_clients_ca` function from `crates/operator/src/controller/user_tls.rs`, along with the two `CLIENTS_CA_*_SUFFIX` constants (they now live in `cluster_ca.rs`). The `read_pem_key` helper used inside also gets removed if its only call site was `ensure_clients_ca`.

- [ ] **Step 4.10: Update the caller in `user.rs`**

In `crates/operator/src/controller/user.rs`, change the call from `crate::controller::user_tls::ensure_clients_ca(...)` to:

```rust
let ca_outcome = crate::controller::cluster_ca::ensure_clients_ca(&secret_api, &kafka).await?;
let ca_material = ca_outcome.material;
```

(`ca_material` is what the per-user cert path expects.)

- [ ] **Step 4.11: Unit tests — `renew_if_expiring`**

At the end of `cluster_ca.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_security::ca::{generate_clients_ca, issue_user_cert};

    #[test]
    fn renews_when_within_window() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 5).expect("leaf");
        let now = OffsetDateTime::now_utc();
        assert!(renew_if_expiring(&user.cert_pem, 30, now).expect("predicate"));
    }

    #[test]
    fn does_not_renew_when_comfortably_in_future() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("leaf");
        let now = OffsetDateTime::now_utc();
        assert!(!renew_if_expiring(&user.cert_pem, 30, now).expect("predicate"));
    }

    #[test]
    fn renews_when_already_past() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 1).expect("leaf");
        // Pretend "now" is 10 days in the future — cert already expired.
        let now = OffsetDateTime::now_utc() + time::Duration::days(10);
        assert!(renew_if_expiring(&user.cert_pem, 30, now).expect("predicate"));
    }
}
```

- [ ] **Step 4.12: Build + run targeted tests**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator controller::cluster_ca::
cargo test -p crabka-operator controller::user::    # ensure caller updated cleanly
```

Expected: all green.

- [ ] **Step 4.13: Commit**

```bash
git add crates/operator/src/controller/cluster_ca.rs \
        crates/operator/src/controller/mod.rs \
        crates/operator/src/controller/user_tls.rs \
        crates/operator/src/controller/user.rs \
        crates/operator/src/controller/common.rs
git commit -m "Slice 30/4: controller/cluster_ca — ensure_{cluster,clients,broker_keystore}_ca + renewal predicate; migrate clients_ca out of user_tls"
```

---

## Task 5 — `controller/listeners.rs::render_broker_toml`: emit `controller_listener_protocol` + `[tls_config]`

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`

- [ ] **Step 5.1: Extend the signature**

Replace the existing `render_broker_toml` signature with:

```rust
pub fn render_broker_toml(
    broker_id: i32,
    listeners: &[Listener],
    addresses_per_listener: &std::collections::BTreeMap<String, AdvertisedAddress>,
    inter_broker_listener_name: &str,
    server_properties: &std::collections::BTreeMap<String, String>,
    tls: Option<&BrokerTlsRender>,
) -> String {
```

Add the struct above the function:

```rust
/// Slice 30: inputs to render the broker config-file's TLS block for a
/// single broker. The operator builds this once per reconcile and feeds
/// it into every per-broker TOML — only the leaf cert paths differ per
/// broker (the cert files are addressed by broker id inside the same
/// mount).
#[derive(Debug, Clone)]
pub struct BrokerTlsRender {
    /// e.g. `"Ssl"` or `"SaslSsl"`. Written as the
    /// `controller_listener_protocol = "<v>"` line.
    pub controller_listener_protocol: String,
    /// Path to the broker's own cert (e.g. `/etc/crabka/broker-tls/0.crt`).
    pub cert_path: String,
    /// Path to the broker's own private key.
    pub key_path: String,
    /// Path to the cluster CA cert used to verify peer client certs.
    pub client_ca_path: String,
    /// `"Required"` for inter-broker mTLS.
    pub client_auth: String,
}
```

- [ ] **Step 5.2: Write the TLS block when present**

Inside `render_broker_toml`, after the existing `server_properties` rendering, append:

```rust
    if let Some(tls) = tls {
        let _ = writeln!(
            out,
            "\ncontroller_listener_protocol = \"{}\"",
            tls.controller_listener_protocol
        );
        let _ = writeln!(out, "\n[tls_config]");
        let _ = writeln!(out, "cert_path = \"{}\"", tls.cert_path);
        let _ = writeln!(out, "key_path = \"{}\"", tls.key_path);
        let _ = writeln!(out, "client_ca_path = \"{}\"", tls.client_ca_path);
        let _ = writeln!(out, "client_auth = \"{}\"", tls.client_auth);
    }
```

- [ ] **Step 5.3: Update all callers**

In the same file's existing tests, pass `None` for `tls` in every call. In `crates/operator/src/controller/common.rs::render_configmap`, find the `render_broker_toml(...)` call and pass an extra `tls` parameter — but at this task's granularity, the call site still has no `tls` source, so pass `None`. T8 wires the real value in.

```rust
let toml = crate::controller::listeners::render_broker_toml(
    *broker_id,
    listeners,
    addrs,
    inter_broker_listener_name,
    &server_properties,
    None,   // T8 will supply BrokerTlsRender once cluster_ca lands in the pipeline
);
```

- [ ] **Step 5.4: Test — TLS block round-trips with broker FileConfig**

In the `toml_rendering_tests` block:

```rust
#[test]
fn render_with_tls_block_round_trips_with_broker_fileconfig() {
    let mut addrs = std::collections::BTreeMap::new();
    addrs.insert(
        "PLAIN".into(),
        AdvertisedAddress {
            host: "demo-0.svc.local".into(),
            port: 9092,
        },
    );
    let listeners = vec![synthesized_default_listener()];
    let props = std::collections::BTreeMap::new();
    let tls = BrokerTlsRender {
        controller_listener_protocol: "Ssl".into(),
        cert_path: "/etc/crabka/broker-tls/0.crt".into(),
        key_path: "/etc/crabka/broker-tls/0.key".into(),
        client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
        client_auth: "Required".into(),
    };
    let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props, Some(&tls));

    let parsed: crabka_broker::file_config::FileConfig =
        toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
    assert_eq!(
        parsed.controller_listener_protocol,
        Some(crabka_security::ListenerProtocol::Ssl)
    );
    let parsed_tls = parsed.tls_config.expect("tls_config emitted");
    assert_eq!(
        parsed_tls.cert_path,
        std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt")
    );
}

#[test]
fn render_without_tls_omits_tls_block() {
    let mut addrs = std::collections::BTreeMap::new();
    addrs.insert(
        "PLAIN".into(),
        AdvertisedAddress { host: "h".into(), port: 9092 },
    );
    let listeners = vec![synthesized_default_listener()];
    let props = std::collections::BTreeMap::new();
    let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props, None);
    assert!(!toml_str.contains("[tls_config]"));
    assert!(!toml_str.contains("controller_listener_protocol"));
}
```

- [ ] **Step 5.5: Run + verify**

```bash
cargo test -p crabka-operator controller::listeners::
```

Expected: PASS.

- [ ] **Step 5.6: Commit**

```bash
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/common.rs
git commit -m "Slice 30/5: render_broker_toml — controller_listener_protocol + [tls_config]"
```

---

## Task 6 — `controller/common.rs::combined_config_hash`: include cluster CA cert

**Files:**
- Modify: `crates/operator/src/controller/common.rs`

- [ ] **Step 6.1: Extend the signature**

Change `combined_config_hash`:

```rust
pub fn combined_config_hash(spec: &crate::crd::KafkaSpec, cluster_ca_cert_pem: Option<&str>) -> String {
```

- [ ] **Step 6.2: Add the fourth segment**

After the existing `metrics_part`, add:

```rust
    let ca_part = cluster_ca_cert_pem.unwrap_or("");
```

Replace the existing collapse-to-config-only fast path:

```rust
    if intent.is_empty() && metrics_part.is_empty() && ca_part.is_empty() {
        return config_hash(&config_part);
    }
```

Replace the existing buffer assembly to append `ca_part`:

```rust
    let mut buf = String::with_capacity(
        config_part.len() + 3 + intent.len() + metrics_part.len() + ca_part.len(),
    );
    buf.push_str(&config_part);
    buf.push('\x1F');
    buf.push_str(&intent);
    buf.push('\x1F');
    buf.push_str(metrics_part);
    buf.push('\x1F');
    buf.push_str(ca_part);
    config_hash(&buf)
```

- [ ] **Step 6.3: Update the sole caller**

In `crates/operator/src/controller/kafka.rs`, find the `let cfg_hash = common::combined_config_hash(&obj.spec);` line and change it to:

```rust
let cfg_hash = common::combined_config_hash(&obj.spec, None);
```

(T8 will replace `None` with the real cluster CA cert PEM once it lands in scope.)

- [ ] **Step 6.4: Tests — hash changes on CA cert change, stable otherwise**

In the existing `#[cfg(test)] mod config_hash_tests`:

```rust
#[test]
fn combined_hash_changes_when_cluster_ca_cert_changes() {
    let spec = crate::crd::KafkaSpec::default();
    let h_none = combined_config_hash(&spec, None);
    let h_a = combined_config_hash(&spec, Some("-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----\n"));
    let h_b = combined_config_hash(&spec, Some("-----BEGIN CERTIFICATE-----\nB\n-----END CERTIFICATE-----\n"));
    assert_ne!(h_none, h_a, "absent vs present CA must differ");
    assert_ne!(h_a, h_b, "different CA PEM must differ");
}

#[test]
fn combined_hash_stable_under_broker_keystore_changes() {
    // The keystore Secret's contents are never inputs to
    // combined_config_hash (slice 33 hot-reload handles leaf renewal).
    // This test guards against a future regression where someone wires
    // a keystore digest into the hash.
    let spec = crate::crd::KafkaSpec::default();
    let h1 = combined_config_hash(&spec, Some("ca-pem"));
    let h2 = combined_config_hash(&spec, Some("ca-pem"));
    assert_eq!(h1, h2);
}
```

- [ ] **Step 6.5: Run + verify**

```bash
cargo test -p crabka-operator controller::common::config_hash
cargo build -p crabka-operator
```

Expected: PASS + clean build.

- [ ] **Step 6.6: Commit**

```bash
git add crates/operator/src/controller/common.rs crates/operator/src/controller/kafka.rs
git commit -m "Slice 30/6: combined_config_hash — fourth segment (cluster CA cert PEM)"
```

---

## Task 7 — `controller/kafka_node_pool.rs::render_statefulset`: volume mounts for CA + keystore

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

- [ ] **Step 7.1: Add three volume mounts on the broker container**

Locate the broker container's `volume_mounts` block. Append:

```rust
volume_mounts.push(VolumeMount {
    name: "cluster-ca-cert".into(),
    mount_path: "/etc/crabka/cluster-ca".into(),
    read_only: Some(true),
    ..Default::default()
});
volume_mounts.push(VolumeMount {
    name: "broker-tls".into(),
    mount_path: "/etc/crabka/broker-tls".into(),
    read_only: Some(true),
    ..Default::default()
});
volume_mounts.push(VolumeMount {
    name: "clients-ca-cert".into(),
    mount_path: "/etc/crabka/clients-ca".into(),
    read_only: Some(true),
    ..Default::default()
});
```

(`VolumeMount` should already be in scope. If not, add `use k8s_openapi::api::core::v1::VolumeMount;`.)

- [ ] **Step 7.2: Add three volumes on the pod**

First, identify how `render_statefulset` already obtains the parent
cluster name. Read `crates/operator/src/controller/kafka_node_pool.rs`
around the `render_statefulset` signature; the function takes the
parent `Kafka` (or a derived "cluster name" string) so the existing
code already has either a `kafka: &Kafka` parameter (use
`kafka.name_any()`) or a `cluster_name: &str` parameter (use directly).
Bind it locally:

```rust
let cluster_name = kafka.name_any();  // or use the existing local — match the caller
```

Then, in the `volumes` block, append:

```rust
volumes.push(Volume {
    name: "cluster-ca-cert".into(),
    secret: Some(SecretVolumeSource {
        secret_name: Some(format!("{cluster_name}-cluster-ca-cert")),
        default_mode: Some(0o400),
        ..Default::default()
    }),
    ..Default::default()
});
volumes.push(Volume {
    name: "broker-tls".into(),
    secret: Some(SecretVolumeSource {
        secret_name: Some(format!("{cluster_name}-kafka-brokers")),
        default_mode: Some(0o400),
        ..Default::default()
    }),
    ..Default::default()
});
volumes.push(Volume {
    name: "clients-ca-cert".into(),
    secret: Some(SecretVolumeSource {
        secret_name: Some(format!("{cluster_name}-clients-ca-cert")),
        default_mode: Some(0o400),
        ..Default::default()
    }),
    ..Default::default()
});
```

(Imports: `Volume`, `SecretVolumeSource` from `k8s_openapi::api::core::v1`.)

- [ ] **Step 7.3: Tests — volumes + volumeMounts present**

In the existing `#[cfg(test)] mod` block:

```rust
#[test]
fn render_statefulset_mounts_cluster_ca_and_broker_tls_secrets() {
    let (kafka, pool, _shared) = /* reuse the existing test fixture helper */;
    let ss = render_statefulset(&kafka, &pool, /* other args */).expect("render");
    let container = &ss.spec.unwrap().template.spec.unwrap().containers[0];
    let mounts: Vec<&str> = container
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .map(|m| m.mount_path.as_str())
        .collect();
    assert!(mounts.contains(&"/etc/crabka/cluster-ca"));
    assert!(mounts.contains(&"/etc/crabka/broker-tls"));
    assert!(mounts.contains(&"/etc/crabka/clients-ca"));
}

#[test]
fn render_statefulset_volume_secret_names_match_cluster() {
    let (kafka, pool, _shared) = /* fixture */;
    let cluster = kafka.metadata.name.clone().unwrap();
    let ss = render_statefulset(&kafka, &pool, /* other args */).expect("render");
    let volumes = ss
        .spec
        .unwrap()
        .template
        .spec
        .unwrap()
        .volumes
        .unwrap_or_default();
    let names: Vec<String> = volumes
        .iter()
        .filter_map(|v| v.secret.as_ref().and_then(|s| s.secret_name.clone()))
        .collect();
    assert!(names.contains(&format!("{cluster}-cluster-ca-cert")));
    assert!(names.contains(&format!("{cluster}-kafka-brokers")));
    assert!(names.contains(&format!("{cluster}-clients-ca-cert")));
}
```

(Adapt fixture-creation to the existing pattern used by the slice-20/21 tests in this same file.)

- [ ] **Step 7.4: Run + verify**

```bash
cargo test -p crabka-operator controller::kafka_node_pool::render_statefulset
```

Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add crates/operator/src/controller/kafka_node_pool.rs
git commit -m "Slice 30/7: StatefulSet template — mount cluster-ca-cert, broker-tls, clients-ca-cert Secrets"
```

---

## Task 8 — `controller/kafka.rs`: wire ensure_*_ca + ensure_broker_keystore into reconcile, populate status

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/src/controller/common.rs` (pass `tls: Option<&BrokerTlsRender>` through `render_configmap`)

- [ ] **Step 8.1: Extend `render_configmap` to accept a per-broker TLS render**

In `common.rs`, change the signature:

```rust
pub(crate) fn render_configmap(
    owner: &Kafka,
    listeners: &[crate::crd::Listener],
    addresses_per_broker: &std::collections::BTreeMap<
        i32,
        std::collections::BTreeMap<String, crate::controller::listeners::AdvertisedAddress>,
    >,
    inter_broker_listener_name: &str,
    tls_per_broker: Option<&std::collections::BTreeMap<i32, crate::controller::listeners::BrokerTlsRender>>,
) -> Result<ConfigMap, ReconcileError> {
```

In the per-broker loop, look up the TLS struct for the current broker id and pass it through:

```rust
for (broker_id, addrs) in addresses_per_broker {
    let tls_for_broker = tls_per_broker.and_then(|m| m.get(broker_id));
    let toml = crate::controller::listeners::render_broker_toml(
        *broker_id,
        listeners,
        addrs,
        inter_broker_listener_name,
        &server_properties,
        tls_for_broker,
    );
    data.insert(format!("broker-{broker_id}.toml"), toml);
}
```

- [ ] **Step 8.2: In `kafka.rs::reconcile_kafka`, call `ensure_cluster_ca` + `ensure_clients_ca` early**

After the existing `secret_api` is bound and `ensure_cluster_id_secret` is called, add:

```rust
let cluster_ca_outcome = cluster_ca::ensure_cluster_ca(&secret_api, &obj).await?;
let clients_ca_outcome = cluster_ca::ensure_clients_ca(&secret_api, &obj).await?;
```

(Add `use crate::controller::cluster_ca;` at the top of the file.)

- [ ] **Step 8.3: Compute `cfg_hash` with the cluster CA cert PEM**

Replace `let cfg_hash = common::combined_config_hash(&obj.spec, None);` with:

```rust
let cfg_hash =
    common::combined_config_hash(&obj.spec, Some(&cluster_ca_outcome.material.cert_pem));
```

- [ ] **Step 8.4: Issue per-broker keystore Secret after broker enumeration**

After `let brokers = enumerate_brokers(&name, &ns, &pool_items);`, add:

```rust
let broker_ids: Vec<i32> = brokers.iter().map(|b| b.broker_id).collect();
let _keystore_status = cluster_ca::ensure_broker_keystore(
    &secret_api,
    &obj,
    &broker_ids,
    &cluster_ca_outcome.material,
)
.await?;
```

- [ ] **Step 8.5: Build the per-broker `BrokerTlsRender` map**

Just before the `apply_cm` closure (the ConfigMap is rendered from inside it):

```rust
let tls_per_broker: std::collections::BTreeMap<i32, listeners::BrokerTlsRender> = broker_ids
    .iter()
    .map(|&id| {
        (
            id,
            listeners::BrokerTlsRender {
                controller_listener_protocol: "Ssl".into(),
                cert_path: format!("/etc/crabka/broker-tls/{id}.crt"),
                key_path: format!("/etc/crabka/broker-tls/{id}.key"),
                client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
                client_auth: "Required".into(),
            },
        )
    })
    .collect();
```

Pass `Some(&tls_per_broker)` to `common::render_configmap` inside `apply_cm`:

```rust
let apply_cm = async |listeners_for_cm: &[Listener],
                      addresses: &BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>|
       -> Result<(), ReconcileError> {
    let cm = common::render_configmap(
        &obj,
        listeners_for_cm,
        addresses,
        &inter_broker_name,
        Some(&tls_per_broker),
    )?;
    apply_object(&cm_api, &cm_name(&name), &cm).await?;
    Ok(())
};
```

- [ ] **Step 8.6: Update status to carry CA fields + new conditions**

In the status-update block, populate:

```rust
status.cluster_ca = Some(crate::crd::CertificateAuthorityStatus {
    not_after: cluster_ca_outcome.not_after.clone(),
    generated: cluster_ca_outcome.generated,
});
status.clients_ca = Some(crate::crd::CertificateAuthorityStatus {
    not_after: clients_ca_outcome.not_after.clone(),
    generated: clients_ca_outcome.generated,
});

conditions.push(condition(
    "ClusterCaReady",
    "True",
    "CaReady",
    "cluster CA Secret pair present and parseable",
));
conditions.push(condition(
    "ClientsCaReady",
    "True",
    "CaReady",
    "clients CA Secret pair present and parseable",
));
```

If `ensure_*_ca` returned `ByoCaMissing`, the error short-circuits the reconcile before this point — surface a `False`-status condition with reason `ByoCaMissing` via the existing error-path matchers in `reconcile_kafka` (mirrors how `validate_listeners` errors are turned into `ListenersValid=False`). Add a branch in the existing error-bridging match for `ReconcileError::ByoCaMissing { which }`:

```rust
ReconcileError::ByoCaMissing { which } => {
    conditions.push(condition(
        which.as_str(),
        "False",
        "ByoCaMissing",
        "spec.{clusterCa|clientsCa}.generateCertificateAuthority=false but the CA Secret pair is absent",
    ));
}
```

- [ ] **Step 8.7: `Ready` precondition update**

Where the existing `Ready=True` is asserted, AND in the predicates `ClusterCaReady` and `ClientsCaReady`. Treat them like the existing `ListenersValid`/`ListenersReady` gate.

- [ ] **Step 8.8: Build + run targeted tests**

```bash
cargo build -p crabka-operator
cargo test -p crabka-operator controller::
```

Expected: PASS (any existing tests against `combined_config_hash` / `render_configmap` must be updated to pass `None` / new params; do those updates as part of this step).

- [ ] **Step 8.9: Commit**

```bash
git add crates/operator/src/controller/kafka.rs crates/operator/src/controller/common.rs
git commit -m "Slice 30/8: reconcile_kafka — wire CA ensure_* + keystore + TLS render + status conditions"
```

---

## Task 9 — `crates/operator/src/main.rs`: `ca-renewal-check` subcommand

**Files:**
- Modify: `crates/operator/src/main.rs`

- [ ] **Step 9.1: Add the subcommand**

In the `Command` enum:

```rust
/// Slice 30: scan all (or one namespace's) Kafka CRs and reissue any
/// broker leaf certs within renewalDays of expiry. Designed to be
/// driven by the CronJob shipped in the operator Helm chart.
CaRenewalCheck(CaRenewalCheckArgs),
```

Add an arg struct:

```rust
#[derive(Debug, clap::Args)]
struct CaRenewalCheckArgs {
    /// Run scoped to a single namespace. Default: cluster-scoped.
    #[arg(long, env = "WATCH_NAMESPACE")]
    namespace: Option<String>,
}
```

- [ ] **Step 9.2: Wire to `controller::cluster_ca::run_renewal_check`**

```rust
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install default rustls CryptoProvider");

    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run::run(args.config).await,
        Command::GenCrds { out_dir } => gen_crds::write_all(&out_dir),
        Command::CaRenewalCheck(args) => {
            tracing_subscriber::fmt::init();
            let client = kube::Client::try_default().await?;
            crabka_operator::controller::cluster_ca::run_renewal_check(
                client,
                args.namespace.as_deref(),
            )
            .await
            .map_err(anyhow::Error::from)
        }
    }
}
```

- [ ] **Step 9.3: Make `controller::cluster_ca` re-exported at the crate root**

In `crates/operator/src/lib.rs`, ensure `pub mod controller;` is present (it is). The `controller::cluster_ca` path then reaches the new module.

- [ ] **Step 9.4: Test — CLI parses the subcommand**

In the existing `#[cfg(test)] mod tests` for the CLI (or add one if missing in `main.rs` style — these often live in `config.rs`'s test module; mirror that pattern), add a `Cli::parse_from(["bin", "ca-renewal-check", "--namespace", "demo"])` round-trip test.

- [ ] **Step 9.5: Build + test**

```bash
cargo build -p crabka-operator
cargo run -p crabka-operator -- ca-renewal-check --help    # smoke-only, won't connect to apiserver
```

Expected: CLI accepts the subcommand and prints help.

- [ ] **Step 9.6: Commit**

```bash
git add crates/operator/src/main.rs crates/operator/src/lib.rs
git commit -m "Slice 30/9: operator CLI — ca-renewal-check subcommand"
```

---

## Task 10 — Integration tests: `reconcile_ca.rs`

**Files:**
- Create: `crates/operator/tests/reconcile_ca.rs`

- [ ] **Step 10.1: Boilerplate + shared fixture imports**

Mirror the existing `tests/reconcile_user.rs` setup (fake kube client, `kube::Client` from a shared in-memory store, `Kafka` CR fixture builder). Reuse the existing `tests/support/*` helpers if any.

- [ ] **Step 10.2: Test 1 — default flow creates 5 Secrets**

```rust
#[tokio::test]
async fn default_flow_creates_cluster_ca_clients_ca_and_broker_keystore() {
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(3).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let secrets = list_secrets(&store, "default");
    assert!(secrets.contains_key("c1-cluster-ca"));
    assert!(secrets.contains_key("c1-cluster-ca-cert"));
    assert!(secrets.contains_key("c1-clients-ca"));
    assert!(secrets.contains_key("c1-clients-ca-cert"));
    let ks = secrets
        .get("c1-kafka-brokers")
        .expect("broker keystore Secret");
    for id in 0..3 {
        assert!(ks.data_keys().contains(&format!("{id}.crt")));
        assert!(ks.data_keys().contains(&format!("{id}.key")));
    }
}
```

(Helpers `fake_kube_client`, `kafka_cr_builder`, `reconcile_once`, `list_secrets`, `data_keys` may need to be added to a `tests/support/` module — model them on the slice-37 `reconcile_user.rs` pattern.)

- [ ] **Step 10.3: Test 2 — broker certs verify against cluster CA**

```rust
#[tokio::test]
async fn broker_leaf_certs_chain_to_cluster_ca() {
    use crabka_security::ca::SubjectAltName;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use x509_parser::prelude::{FromDer, X509Certificate};
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let secrets = list_secrets(&store, "default");
    let ks = secrets.get("c1-kafka-brokers").unwrap();
    let ca_cert_pem = secrets
        .get("c1-cluster-ca-cert")
        .unwrap()
        .get_string("ca.crt");
    let leaf_pem = ks.get_string("0.crt");

    let leaf_der = CertificateDer::pem_slice_iter(leaf_pem.as_bytes())
        .next().unwrap().unwrap();
    let ca_der = CertificateDer::pem_slice_iter(ca_cert_pem.as_bytes())
        .next().unwrap().unwrap();
    let (_, leaf) = X509Certificate::from_der(leaf_der.as_ref()).unwrap();
    let (_, ca) = X509Certificate::from_der(ca_der.as_ref()).unwrap();
    leaf.verify_signature(Some(ca.public_key())).expect("chains to CA");
    // SAN includes the per-pod FQDN.
    let san = leaf.subject_alternative_name().unwrap().unwrap();
    assert!(san.value.general_names.iter().any(|gn| matches!(
        gn,
        x509_parser::extensions::GeneralName::DNSName(s)
        if s.contains("c1-broker-0")
    )));
}
```

- [ ] **Step 10.4: Test 3 — scale-up adds entries, doesn't reissue existing**

```rust
#[tokio::test]
async fn scale_up_appends_new_broker_entries() {
    let (client, store) = fake_kube_client();
    let mut kafka = kafka_cr_builder("c1", "default").replicas(3).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile 1");

    let secrets1 = list_secrets(&store, "default");
    let crt0_before = secrets1.get("c1-kafka-brokers").unwrap().get_string("0.crt");

    kafka.spec.replicas = Some(5);
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile 2");

    let secrets2 = list_secrets(&store, "default");
    let ks2 = secrets2.get("c1-kafka-brokers").unwrap();
    let crt0_after = ks2.get_string("0.crt");
    assert_eq!(crt0_before, crt0_after, "existing broker cert must not be reissued");
    for id in 0..5 {
        assert!(ks2.data_keys().contains(&format!("{id}.crt")));
    }
}
```

- [ ] **Step 10.5: Test 4 — scale-down prunes entries**

```rust
#[tokio::test]
async fn scale_down_prunes_broker_entries() {
    let (client, store) = fake_kube_client();
    let mut kafka = kafka_cr_builder("c1", "default").replicas(5).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile 1");

    kafka.spec.replicas = Some(3);
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile 2");

    let secrets = list_secrets(&store, "default");
    let ks = secrets.get("c1-kafka-brokers").unwrap();
    for id in 0..3 {
        assert!(ks.data_keys().contains(&format!("{id}.crt")));
    }
    for id in 3..5 {
        assert!(!ks.data_keys().contains(&format!("{id}.crt")), "broker {id} pruned");
    }
}
```

- [ ] **Step 10.6: Test 5 — BYO mode adopts pre-existing Secrets, doesn't overwrite**

```rust
#[tokio::test]
async fn byo_mode_adopts_preexisting_cluster_ca() {
    use crabka_security::ca::generate_cluster_ca;
    let (client, store) = fake_kube_client();
    let user_ca = generate_cluster_ca("user-supplied", 365).unwrap();
    // Pre-create the two cluster CA Secrets in the store.
    seed_secret(&store, "default", "c1-cluster-ca", "ca.key", &user_ca.key_pem);
    seed_secret(&store, "default", "c1-cluster-ca-cert", "ca.crt", &user_ca.cert_pem);

    let mut kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    kafka.spec.cluster_ca = Some(CertificateAuthority {
        generate_certificate_authority: false,
        validity_days: 365,
        renewal_days: 30,
    });
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let secrets = list_secrets(&store, "default");
    assert_eq!(
        secrets.get("c1-cluster-ca").unwrap().get_string("ca.key"),
        user_ca.key_pem,
        "BYO key must not be overwritten"
    );
    assert_eq!(
        secrets.get("c1-cluster-ca-cert").unwrap().get_string("ca.crt"),
        user_ca.cert_pem,
    );
    // Broker keystore still gets signed against the user's CA.
    assert!(secrets.contains_key("c1-kafka-brokers"));
}
```

- [ ] **Step 10.7: Test 6 — BYO mode without pre-existing Secrets errors**

```rust
#[tokio::test]
async fn byo_mode_without_secrets_errors() {
    let (client, _store) = fake_kube_client();
    let mut kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    kafka.spec.cluster_ca = Some(CertificateAuthority {
        generate_certificate_authority: false,
        ..Default::default()
    });
    apply_kafka(&client, &kafka).await;
    let err = reconcile_once(&client, &kafka)
        .await
        .expect_err("must error");
    assert!(
        matches!(err, ReconcileError::ByoCaMissing { ref which } if which == "ClusterCaReady"),
        "got {err:?}"
    );
}
```

- [ ] **Step 10.8: Test 7 — reconciler does NOT renew valid-but-aging leafs**

```rust
#[tokio::test]
async fn reconciler_does_not_renew_aging_leaf_certs() {
    use crabka_security::ca::{generate_cluster_ca, issue_broker_cert, SubjectAltName};
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    // Pre-seed: cluster CA + a leaf cert with notAfter only 5 days out
    // (well inside the 30-day renewal window).
    let ca = generate_cluster_ca("c1-cluster-ca", 365).unwrap();
    seed_secret(&store, "default", "c1-cluster-ca", "ca.key", &ca.key_pem);
    seed_secret(&store, "default", "c1-cluster-ca-cert", "ca.crt", &ca.cert_pem);
    let leaf = issue_broker_cert(
        &ca.cert_pem, &ca.key_pem, "c1-broker-0",
        &[SubjectAltName::Dns("c1-broker-0".into())], 5,
    ).unwrap();
    seed_secret_multi(&store, "default", "c1-kafka-brokers", &[
        ("0.crt", &leaf.cert_pem),
        ("0.key", &leaf.key_pem),
    ]);

    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let secrets = list_secrets(&store, "default");
    let ks = secrets.get("c1-kafka-brokers").unwrap();
    assert_eq!(
        ks.get_string("0.crt"),
        leaf.cert_pem,
        "reconciler must not renew aging leafs — that's the CronJob's job"
    );
}
```

- [ ] **Step 10.9: Run + verify**

```bash
cargo test -p crabka-operator --test reconcile_ca
```

Expected: 7 tests PASS.

- [ ] **Step 10.10: Commit**

```bash
git add crates/operator/tests/reconcile_ca.rs crates/operator/tests/support/*.rs   # if support helpers were added
git commit -m "Slice 30/10: integration tests — reconcile_ca (CA + keystore + BYO)"
```

---

## Task 11 — Integration tests: `reconcile_inter_broker_mtls.rs`

**Files:**
- Create: `crates/operator/tests/reconcile_inter_broker_mtls.rs`

- [ ] **Step 11.1: Test 1 — broker config-file carries TLS block**

```rust
#[tokio::test]
async fn rendered_broker_config_carries_controller_listener_protocol_ssl_and_tls_block() {
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(2).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let cms = list_configmaps(&store, "default");
    let cm = cms.get("c1-broker-config").unwrap();
    for id in [0, 1] {
        let toml = cm.data_get(&format!("broker-{id}.toml"));
        assert!(toml.contains("controller_listener_protocol = \"Ssl\""));
        assert!(toml.contains("[tls_config]"));
        assert!(toml.contains(&format!("cert_path = \"/etc/crabka/broker-tls/{id}.crt\"")));
        assert!(toml.contains(&format!("key_path = \"/etc/crabka/broker-tls/{id}.key\"")));
        assert!(toml.contains("client_ca_path = \"/etc/crabka/cluster-ca/ca.crt\""));
        assert!(toml.contains("client_auth = \"Required\""));
        // Round-trips with the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig = toml::from_str(toml).unwrap();
        assert!(parsed.tls_config.is_some());
    }
}
```

- [ ] **Step 11.2: Test 2 — `listeners[].tls=true` still rejected**

```rust
#[tokio::test]
async fn data_plane_tls_listener_still_rejected_in_slice_30() {
    let (client, _store) = fake_kube_client();
    let mut kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    kafka.spec.listeners.push(Listener {
        name: "external-tls".into(),
        port: 9093,
        type_: ListenerType::Internal,
        tls: true,
        configuration: None,
        network_policy_peers: None,
    });
    apply_kafka(&client, &kafka).await;
    let err = reconcile_once(&client, &kafka).await.expect_err("must reject");
    // Existing slice-25 behavior — captured here to guard against drift.
    assert!(format!("{err:?}").contains("TlsNotYetSupported"));
}
```

- [ ] **Step 11.3: Test 3 — StatefulSet template mounts all three Secrets**

```rust
#[tokio::test]
async fn statefulset_mounts_cluster_ca_broker_tls_clients_ca() {
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile");

    let sss = list_statefulsets(&store, "default");
    let ss = sss
        .values()
        .find(|s| s.metadata.name.as_deref().unwrap_or("").starts_with("c1-"))
        .expect("at least one StatefulSet");
    let pod_spec = ss.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
    let mounts: Vec<&str> = pod_spec.containers[0]
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .map(|m| m.mount_path.as_str())
        .collect();
    for expected in [
        "/etc/crabka/cluster-ca",
        "/etc/crabka/broker-tls",
        "/etc/crabka/clients-ca",
    ] {
        assert!(mounts.contains(&expected), "expected {expected}, got {mounts:?}");
    }
    let volumes = pod_spec.volumes.as_ref().unwrap();
    let secret_names: Vec<&str> = volumes
        .iter()
        .filter_map(|v| v.secret.as_ref().and_then(|s| s.secret_name.as_deref()))
        .collect();
    assert!(secret_names.contains(&"c1-cluster-ca-cert"));
    assert!(secret_names.contains(&"c1-kafka-brokers"));
    assert!(secret_names.contains(&"c1-clients-ca-cert"));
}
```

- [ ] **Step 11.4: Test 4 — idempotency: two reconciles produce byte-identical broker config-file**

```rust
#[tokio::test]
async fn render_is_idempotent_across_reconciles() {
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    apply_kafka(&client, &kafka).await;
    reconcile_once(&client, &kafka).await.expect("reconcile 1");
    let toml1 = list_configmaps(&store, "default")
        .get("c1-broker-config").unwrap()
        .data_get("broker-0.toml").to_string();
    reconcile_once(&client, &kafka).await.expect("reconcile 2");
    let toml2 = list_configmaps(&store, "default")
        .get("c1-broker-config").unwrap()
        .data_get("broker-0.toml").to_string();
    assert_eq!(toml1, toml2);
}
```

- [ ] **Step 11.5: Run + verify**

```bash
cargo test -p crabka-operator --test reconcile_inter_broker_mtls
```

Expected: 4 tests PASS.

- [ ] **Step 11.6: Commit**

```bash
git add crates/operator/tests/reconcile_inter_broker_mtls.rs
git commit -m "Slice 30/11: integration tests — inter-broker mTLS render + idempotency"
```

---

## Task 12 — Integration tests: `ca_renewal_cronjob.rs`

**Files:**
- Create: `crates/operator/tests/ca_renewal_cronjob.rs`

- [ ] **Step 12.1: Test 1 — reissues leafs within renewal window**

```rust
#[tokio::test]
async fn cronjob_reissues_aging_broker_leafs() {
    use crabka_security::ca::{generate_cluster_ca, issue_broker_cert, SubjectAltName};
    use crabka_operator::controller::cluster_ca::run_renewal_check;
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(1).build();
    apply_kafka(&client, &kafka).await;

    // Seed: CA + a leaf with notAfter 5 days out (inside default 30-day window).
    let ca = generate_cluster_ca("c1-cluster-ca", 365).unwrap();
    seed_secret(&store, "default", "c1-cluster-ca", "ca.key", &ca.key_pem);
    seed_secret(&store, "default", "c1-cluster-ca-cert", "ca.crt", &ca.cert_pem);
    let leaf_before = issue_broker_cert(
        &ca.cert_pem, &ca.key_pem, "c1-broker-0",
        &[SubjectAltName::Dns("c1-broker-0".into())], 5,
    ).unwrap();
    seed_secret_multi(&store, "default", "c1-kafka-brokers", &[
        ("0.crt", &leaf_before.cert_pem),
        ("0.key", &leaf_before.key_pem),
    ]);

    run_renewal_check(client.clone(), Some("default")).await.expect("renewal check");

    let secrets = list_secrets(&store, "default");
    let ks = secrets.get("c1-kafka-brokers").unwrap();
    let crt_after = ks.get_string("0.crt");
    assert_ne!(crt_after, leaf_before.cert_pem, "leaf must be reissued");

    let events = list_events(&store, "default");
    assert!(events.iter().any(|e| e.reason.as_deref() == Some("BrokerCertRenewed")));
}
```

- [ ] **Step 12.2: Test 2 — flags expiring cluster CA without rotating**

```rust
#[tokio::test]
async fn cronjob_flags_expiring_cluster_ca_without_rotating() {
    use crabka_security::ca::generate_cluster_ca;
    use crabka_operator::controller::cluster_ca::run_renewal_check;
    let (client, store) = fake_kube_client();
    let kafka = kafka_cr_builder("c1", "default").replicas(0).build();
    apply_kafka(&client, &kafka).await;

    // Seed: a CA with notAfter only 25 days out — within the 30-day renewal window.
    let ca = generate_cluster_ca("c1-cluster-ca", 25).unwrap();
    seed_secret(&store, "default", "c1-cluster-ca", "ca.key", &ca.key_pem);
    seed_secret(&store, "default", "c1-cluster-ca-cert", "ca.crt", &ca.cert_pem);
    // Clients CA: comfortably far in future.
    let clients_ca = crabka_security::ca::generate_clients_ca("c1-clients-ca", 3650).unwrap();
    seed_secret(&store, "default", "c1-clients-ca", "ca.key", &clients_ca.key_pem);
    seed_secret(&store, "default", "c1-clients-ca-cert", "ca.crt", &clients_ca.cert_pem);

    run_renewal_check(client.clone(), Some("default")).await.expect("renewal check");

    let after = list_secrets(&store, "default");
    assert_eq!(
        after.get("c1-cluster-ca-cert").unwrap().get_string("ca.crt"),
        ca.cert_pem,
        "expiring CA must NOT be rotated by slice 30"
    );
    let events = list_events(&store, "default");
    assert!(events.iter().any(|e| e.reason.as_deref() == Some("CaRotationRequired")));
}
```

- [ ] **Step 12.3: Test 3 — BYO CA expiring emits ByoCaExpiringSoon event**

```rust
#[tokio::test]
async fn cronjob_byo_ca_expiring_emits_byo_event() {
    use crabka_security::ca::generate_cluster_ca;
    use crabka_operator::controller::cluster_ca::run_renewal_check;
    let (client, store) = fake_kube_client();
    let mut kafka = kafka_cr_builder("c1", "default").replicas(0).build();
    kafka.spec.cluster_ca = Some(CertificateAuthority {
        generate_certificate_authority: false,
        ..Default::default()
    });
    apply_kafka(&client, &kafka).await;
    let ca = generate_cluster_ca("user-supplied", 25).unwrap();
    seed_secret(&store, "default", "c1-cluster-ca", "ca.key", &ca.key_pem);
    seed_secret(&store, "default", "c1-cluster-ca-cert", "ca.crt", &ca.cert_pem);
    let clients_ca = crabka_security::ca::generate_clients_ca("c1-clients-ca", 3650).unwrap();
    seed_secret(&store, "default", "c1-clients-ca", "ca.key", &clients_ca.key_pem);
    seed_secret(&store, "default", "c1-clients-ca-cert", "ca.crt", &clients_ca.cert_pem);

    run_renewal_check(client.clone(), Some("default")).await.expect("renewal check");

    let events = list_events(&store, "default");
    assert!(events.iter().any(|e| e.reason.as_deref() == Some("ByoCaExpiringSoon")));
    assert!(!events.iter().any(|e| e.reason.as_deref() == Some("CaRotationRequired")),
        "BYO CAs don't emit CaRotationRequired — that's only for operator-managed");
}
```

- [ ] **Step 12.4: Run + verify**

```bash
cargo test -p crabka-operator --test ca_renewal_cronjob
```

Expected: 3 tests PASS.

- [ ] **Step 12.5: Commit**

```bash
git add crates/operator/tests/ca_renewal_cronjob.rs
git commit -m "Slice 30/12: integration tests — ca-renewal-check subcommand"
```

---

## Task 13 — Helm chart: CronJob, SA, RBAC, values

**Files:**
- Create: `charts/crabka-operator/templates/cronjob-ca-renewal.yaml`
- Create: `charts/crabka-operator/templates/serviceaccount-renewal.yaml`
- Create: `charts/crabka-operator/templates/clusterrole-renewal.yaml`
- Create: `charts/crabka-operator/templates/clusterrolebinding-renewal.yaml`
- Modify: `charts/crabka-operator/values.yaml`

- [ ] **Step 13.1: `cronjob-ca-renewal.yaml`**

```yaml
{{- if .Values.caRenewal.enabled }}
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ include "crabka-operator.fullname" . }}-ca-renewal
  labels: {{- include "crabka-operator.labels" . | nindent 4 }}
spec:
  schedule: {{ .Values.caRenewal.schedule | quote }}
  startingDeadlineSeconds: {{ .Values.caRenewal.startingDeadlineSeconds }}
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        metadata:
          labels: {{- include "crabka-operator.selectorLabels" . | nindent 12 }}
        spec:
          serviceAccountName: {{ include "crabka-operator.fullname" . }}-ca-renewal
          restartPolicy: Never
          containers:
            - name: ca-renewal-check
              image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
              imagePullPolicy: {{ .Values.image.pullPolicy }}
              args: ["ca-renewal-check"]
              env:
                - name: RUST_LOG
                  value: {{ .Values.caRenewal.logFilter | quote }}
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
              resources: {{- toYaml .Values.caRenewal.resources | nindent 16 }}
{{- end }}
```

- [ ] **Step 13.2: `serviceaccount-renewal.yaml`**

```yaml
{{- if and .Values.caRenewal.enabled .Values.rbac.create -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "crabka-operator.fullname" . }}-ca-renewal
  labels: {{- include "crabka-operator.labels" . | nindent 4 }}
{{- end }}
```

- [ ] **Step 13.3: `clusterrole-renewal.yaml`**

```yaml
{{- if and .Values.caRenewal.enabled .Values.rbac.create -}}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "crabka-operator.fullname" . }}-ca-renewal
  labels: {{- include "crabka-operator.labels" . | nindent 4 }}
rules:
  - apiGroups: ["crabka.io"]
    resources: ["kafkas", "kafkas/status"]
    verbs: ["get", "list", "watch", "patch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch", "patch", "update"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
{{- end }}
```

- [ ] **Step 13.4: `clusterrolebinding-renewal.yaml`**

```yaml
{{- if and .Values.caRenewal.enabled .Values.rbac.create -}}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "crabka-operator.fullname" . }}-ca-renewal
  labels: {{- include "crabka-operator.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "crabka-operator.fullname" . }}-ca-renewal
subjects:
  - kind: ServiceAccount
    name: {{ include "crabka-operator.fullname" . }}-ca-renewal
    namespace: {{ .Release.Namespace }}
{{- end }}
```

- [ ] **Step 13.5: `values.yaml` additions**

Append:

```yaml
caRenewal:
  enabled: true
  schedule: "0 2 * * *"
  startingDeadlineSeconds: 600
  logFilter: "info,kube_client::client::builder=warn"
  resources:
    requests:
      cpu: 50m
      memory: 64Mi
    limits:
      cpu: 500m
      memory: 256Mi
```

- [ ] **Step 13.6: Helm lint**

```bash
helm lint charts/crabka-operator/
helm template charts/crabka-operator/ | grep -E "kind: CronJob"
```

Expected: lint clean; `kind: CronJob` appears once.

- [ ] **Step 13.7: Add kind-e2e assertions for the new Secrets**

In `.github/workflows/operator-e2e.yml`, locate the section that waits
for the `Ready` condition on the test `Kafka` CR. Add two assertions
right after it:

```yaml
      - name: Assert CA + broker keystore Secrets exist
        run: |
          kubectl get secret c1-cluster-ca-cert c1-clients-ca-cert c1-kafka-brokers
          kubectl exec c1-broker-0 -- ls /etc/crabka/cluster-ca /etc/crabka/broker-tls
```

(`c1` is the test cluster name used elsewhere in the workflow; mirror
what's actually there. If the test cluster has a different name, use it.)

- [ ] **Step 13.8: Commit**

```bash
git add charts/crabka-operator/templates/cronjob-ca-renewal.yaml \
        charts/crabka-operator/templates/serviceaccount-renewal.yaml \
        charts/crabka-operator/templates/clusterrole-renewal.yaml \
        charts/crabka-operator/templates/clusterrolebinding-renewal.yaml \
        charts/crabka-operator/values.yaml \
        .github/workflows/operator-e2e.yml
git commit -m "Slice 30/13: Helm chart — ca-renewal CronJob + SA + RBAC + values + kind-e2e Secret assertions"
```

---

## Task 14 — Regenerate the `Kafka` CRD YAML

**Files:**
- Modify: `deploy/crds/crabka.io_kafkas.yaml`

- [ ] **Step 14.1: Run the generator**

```bash
cargo run -p crabka-operator --bin crabka-operator -- gen-crds --out-dir deploy/crds
```

- [ ] **Step 14.2: Diff-review**

```bash
git diff deploy/crds/crabka.io_kafkas.yaml
```

Expected: the diff adds the `clusterCa` and `clientsCa` properties under `spec` (and the status fields under `status`). No other CRD files should change.

- [ ] **Step 14.3: Commit**

```bash
git add deploy/crds/crabka.io_kafkas.yaml
git commit -m "Slice 30/14: regenerate Kafka CRD YAML with clusterCa / clientsCa fields"
```

---

## Task 15 — `STATUS.md` entry

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 15.1: Append the slice entry**

After the most recent slice entry (currently `Slice 37`), append:

```markdown
## Slice 30 — Operator: Cluster CA + clients CA generation (2026-05-21)

- New `Kafka.spec.clusterCa` + `Kafka.spec.clientsCa`: Strimzi-shaped
  `CertificateAuthority { generateCertificateAuthority, validityDays,
  renewalDays }`, default `(true, 365, 30)`. `clientsCa` replaces the
  slice-37 lazy-bootstrap path (deleted outright — greenfield).
- Operator generates and rotates per-broker keystore (`<cluster>-kafka-brokers`)
  signed by the cluster CA. Inter-broker mTLS on by default: the broker
  controller listener terminates TLS with `client_auth=Required` and the
  cluster CA cert as the truststore. Renewal of leaf certs is handled by
  a new CronJob (`crabka-operator ca-renewal-check`) shipped in the Helm
  chart with a dedicated ServiceAccount + narrower RBAC.
- BYO CAs (`generateCertificateAuthority: false`) — operator validates
  pre-existing Secret pair and refuses to overwrite; CronJob emits
  `ByoCaExpiringSoon` Events when nearing expiry.
- CA-itself expiry handled disruptively in this slice: `CaRotationRequired=True`
  status condition + Event, no auto-rotation. Slice 34 owns the
  multi-generation trust bundle + zero-downtime rotation.
- Slice-21 config-hash gains a fourth segment (cluster CA cert PEM) so
  CA changes force a cluster roll. Leaf cert renewal piggybacks on slice
  33's cert hot-reload — no restart.
- 14 new tests across security/operator unit + 3 operator integration
  test files (`reconcile_ca`, `reconcile_inter_broker_mtls`,
  `ca_renewal_cronjob`).
- Out of scope: data-plane listener TLS (slice 31), non-disruptive CA
  rotation (slice 34), PKCS#12 keystore output, MaintenanceTimeWindows.
```

- [ ] **Step 15.2: Commit**

```bash
git add STATUS.md
git commit -m "Slice 30/15: STATUS.md — operator cluster + clients CA entry"
```

---

## Acceptance

Run the full test matrix to confirm slice 30 is wired correctly:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
helm lint charts/crabka-operator/
```

All four must be green. The fourth catches Helm chart syntax errors that
otherwise only surface at `helm install` time.

Manual smoke (optional, requires a kind cluster):

```bash
kind create cluster --name crabka-slice30
helm install --create-namespace --namespace crabka-operator \
    -f charts/crabka-operator/values.yaml \
    crabka charts/crabka-operator/
kubectl apply -f - <<EOF
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: c1, namespace: default }
spec:
  kafkaVersion: "3.7.0"
  replicas: 1
EOF
kubectl wait --for=condition=Ready --timeout=120s kafka/c1
kubectl get secret c1-cluster-ca-cert c1-clients-ca-cert c1-kafka-brokers
kubectl exec c1-broker-0 -- ls /etc/crabka/cluster-ca /etc/crabka/broker-tls
```

Expected: all three Secrets exist, `Ready` flips True, broker pod has
the CA cert and its own keystore mounted.
