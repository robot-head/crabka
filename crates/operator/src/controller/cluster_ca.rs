//! Slice 30: cluster CA + clients CA lifecycle.
//!
//! Owns:
//! - the per-cluster `cluster CA` Secret pair (private key + public cert),
//! - the per-cluster `clients CA` Secret pair (formerly in `user_tls.rs`),
//! - the per-cluster broker-keystore Secret (`<cluster>-kafka-brokers`),
//! - the pure `renew_if_expiring` predicate (called by both the
//!   reconciler-side `ensure_*` helpers and the `ca-renewal-check`
//!   `CronJob` subcommand),
//! - the `run_renewal_check` entrypoint for the `CronJob`.

use std::collections::BTreeMap;
use std::net::IpAddr;

use crabka_security::ca::{
    CaMaterial, SubjectAltName, generate_clients_ca, generate_cluster_ca, issue_broker_cert,
};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, Patch, PatchParams};
use kube::{Resource, ResourceExt as _};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::controller::common::{FIELD_MANAGER, ReconcileError, owner_ref, read_pem_key};
use crate::crd::{CertificateAuthority, Kafka};

pub(crate) const CLUSTER_CA_KEY_SUFFIX: &str = "-cluster-ca";
pub(crate) const CLUSTER_CA_CERT_SUFFIX: &str = "-cluster-ca-cert";
pub(crate) const CLIENTS_CA_KEY_SUFFIX: &str = "-clients-ca";
pub(crate) const CLIENTS_CA_CERT_SUFFIX: &str = "-clients-ca-cert";
pub(crate) const BROKER_KEYSTORE_SUFFIX: &str = "-kafka-brokers";

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

// ---------------------------------------------------------------------------
// CaOutcome + supporting types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaAction {
    GeneratedNew,
    Reused,
    AdoptedByo,
}

#[derive(Debug, Clone)]
pub(crate) struct CaOutcome {
    pub material: CaMaterial,
    #[allow(dead_code)]
    pub action: CaAction,
    pub not_after: String,
    pub generated: bool,
}

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

const SECRET_TYPE_CA_KEY: &str = "ca-key";
const SECRET_TYPE_CA_CERT: &str = "ca-cert";
const SECRET_TYPE_BROKER_KEYSTORE: &str = "broker-keystore";

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

pub(crate) async fn ensure_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    spec: &CertificateAuthority,
    which: WhichCa,
) -> Result<CaOutcome, ReconcileError> {
    let cluster = kafka.name_any();
    let (key_name, cert_name) = match which {
        WhichCa::Cluster => (
            cluster_ca_key_name(&cluster),
            cluster_ca_cert_name(&cluster),
        ),
        WhichCa::Clients => (
            clients_ca_key_name(&cluster),
            clients_ca_cert_name(&cluster),
        ),
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
        WhichCa::Cluster => generate_cluster_ca(&cn, spec.validity_days)?,
        WhichCa::Clients => generate_clients_ca(&cn, spec.validity_days)?,
    };

    let key_secret = render_ca_secret(
        kafka,
        &key_name,
        "ca.key",
        &material.key_pem,
        SECRET_TYPE_CA_KEY,
    )?;
    let cert_secret = render_ca_secret(
        kafka,
        &cert_name,
        "ca.crt",
        &material.cert_pem,
        SECRET_TYPE_CA_CERT,
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
    let cluster = kafka.name_any();
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/secret-type".into(), secret_type_label.into());
    labels.insert("crabka.io/cluster".into(), cluster);
    let mut annotations = BTreeMap::new();
    annotations.insert("crabka.io/strictly-operator-managed".into(), "true".into());
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

// ---------------------------------------------------------------------------
// Public ensure_cluster_ca + ensure_clients_ca
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Broker keystore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct BrokerKeystoreStatus {
    pub issued: Vec<i32>,
    pub reused: Vec<i32>,
    pub pruned: Vec<i32>,
}

/// Per-broker cert request. Caller supplies the CN and SAN list that must match
/// what peer brokers will actually dial (i.e., real pod FQDN derived from the
/// `StatefulSet` name `{cluster}-{pool_name}` and ordinal).
#[derive(Debug, Clone)]
pub(crate) struct BrokerCertRequest {
    pub broker_id: i32,
    pub cn: String,
    pub sans: Vec<SubjectAltName>,
    /// Extra SANs for external listeners (e.g. `NodePort` node addresses,
    /// `LoadBalancer` IPs). Empty when no external TLS listeners are configured.
    pub extra_sans: Vec<SubjectAltName>,
}

#[allow(dead_code, clippy::too_many_lines)]
pub(crate) async fn ensure_broker_keystore(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    requests: &[BrokerCertRequest],
    cluster_ca: &CaMaterial,
) -> Result<BrokerKeystoreStatus, ReconcileError> {
    let cluster = kafka.name_any();
    let namespace = kafka.meta().namespace.clone().unwrap_or_default();
    let name = broker_keystore_name(&cluster);

    let validity = kafka
        .spec
        .cluster_ca
        .as_ref()
        .map_or(365, |c| c.validity_days);

    let existing = secret_api.get_opt(&name).await?;
    let mut data: BTreeMap<String, ByteString> = existing
        .as_ref()
        .and_then(|s| s.data.clone())
        .unwrap_or_default();

    let mut issued = Vec::new();
    let mut reused = Vec::new();

    for req in requests {
        let id = req.broker_id;
        let crt_key = format!("{id}.crt");
        let key_key = format!("{id}.key");
        let digest_key = format!("{id}.sans-digest");

        let requested_digest = compute_san_digest(&req.sans, &req.extra_sans);

        let has_cert = data.contains_key(&crt_key) && data.contains_key(&key_key);
        let stored_digest = data.get(&digest_key).and_then(|b| {
            std::str::from_utf8(&b.0)
                .ok()
                .map(std::borrow::ToOwned::to_owned)
        });

        let needs_reissue = !has_cert
            || stored_digest.is_none()
            || stored_digest.as_deref() != Some(&requested_digest);

        if !needs_reissue {
            reused.push(id);
            continue;
        }
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &req.cn,
            &req.sans,
            &req.extra_sans,
            validity,
        )?;
        data.insert(crt_key, ByteString(leaf.cert_pem.into_bytes()));
        data.insert(key_key, ByteString(leaf.key_pem.into_bytes()));
        data.insert(digest_key, ByteString(requested_digest.into_bytes()));
        issued.push(id);
    }

    let want_keys: std::collections::HashSet<String> = requests
        .iter()
        .flat_map(|req| {
            let id = req.broker_id;
            [
                format!("{id}.crt"),
                format!("{id}.key"),
                format!("{id}.sans-digest"),
            ]
        })
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
    labels.insert(
        "crabka.io/secret-type".into(),
        SECRET_TYPE_BROKER_KEYSTORE.into(),
    );
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

// ---------------------------------------------------------------------------
// SAN-list digest
// ---------------------------------------------------------------------------

/// SHA-256 digest of the canonical-form SAN list (sorted, deduped).
/// Used to detect when the SAN list for a broker has changed vs the
/// cert currently stored in the Secret, triggering a reissue.
#[must_use]
pub fn compute_san_digest(base_sans: &[SubjectAltName], extras: &[SubjectAltName]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let mut all: Vec<&SubjectAltName> = base_sans.iter().chain(extras.iter()).collect();
    all.sort();
    all.dedup();
    let mut h = Sha256::new();
    for s in all {
        match s {
            SubjectAltName::Dns(d) => {
                h.update(b"DNS:");
                h.update(d.as_bytes());
            }
            SubjectAltName::Ip(ip) => {
                h.update(b"IP:");
                h.update(ip.to_string().as_bytes());
            }
        }
        h.update(b"\n");
    }
    let result = h.finalize();
    result.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

// ---------------------------------------------------------------------------
// Renewal predicate
// ---------------------------------------------------------------------------

pub fn renew_if_expiring(
    cert_pem: &str,
    renewal_days: u32,
    now: OffsetDateTime,
) -> Result<bool, ReconcileError> {
    let not_after = cert_not_after(cert_pem)?;
    Ok(not_after - now <= time::Duration::days(i64::from(renewal_days)))
}

// ---------------------------------------------------------------------------
// CronJob entrypoint: run_renewal_check
// ---------------------------------------------------------------------------

use k8s_openapi::api::core::v1::Event;
use kube::api::{ListParams, PostParams};

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

    let cluster_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Cluster).await?;
    let clients_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Clients).await?;

    let cluster_ca_spec = kafka.spec.cluster_ca.clone().unwrap_or_default();
    let clients_ca_spec = kafka.spec.clients_ca.clone().unwrap_or_default();

    flag_ca_if_expiring(
        client,
        kafka,
        &cluster_ca.cert_pem,
        &cluster_ca_spec,
        WhichCa::Cluster,
        now,
    )
    .await?;
    flag_ca_if_expiring(
        client,
        kafka,
        &clients_ca.cert_pem,
        &clients_ca_spec,
        WhichCa::Clients,
        now,
    )
    .await?;

    renew_broker_leafs(
        client,
        kafka,
        &cluster_ca,
        cluster_ca_spec.renewal_days,
        cluster_ca_spec.validity_days,
        now,
    )
    .await?;
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
    let key_secret =
        secret_api
            .get_opt(&key_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: key_name.clone(),
            })?;
    let cert_secret =
        secret_api
            .get_opt(&cert_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: cert_name.clone(),
            })?;
    let key_pem = read_pem_key(&key_secret, "ca.key")
        .ok_or_else(|| ReconcileError::CertParse(format!("{key_name} ca.key unreadable")))?;
    let cert_pem = read_pem_key(&cert_secret, "ca.crt")
        .ok_or_else(|| ReconcileError::CertParse(format!("{cert_name} ca.crt unreadable")))?;
    Ok(CaMaterial { cert_pem, key_pem })
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
    if spec.generate_certificate_authority {
        // Operator-managed CA: emit a Warning event AND patch the status
        // condition so controllers watching the CR can react.
        let which_str = match which {
            WhichCa::Cluster => "Cluster",
            WhichCa::Clients => "Clients",
        };
        let reason = format!("{which_str}CaExpiringSoon");
        let message = format!(
            "CA {} is expiring within renewalDays; \
             automatic rotation not yet implemented; \
             replace the Secret pair manually if needed",
            which.condition_name()
        );
        emit_event(
            client,
            &ns,
            kafka,
            "Warning",
            "CaRotationRequired",
            &message,
            "crabka-ca-renewal-",
            "RenewalCheck",
            "crabka-operator/ca-renewal-check",
        )
        .await?;

        // Patch the Kafka CR status with CaRotationRequired=True.
        // Read existing conditions first so we don't wipe other condition types.
        let kafka_api: Api<Kafka> = Api::namespaced(client.clone(), &ns);
        let existing = kafka_api.get_status(&kafka.name_any()).await?;
        let mut conditions: Vec<serde_json::Value> = existing
            .status
            .as_ref()
            .map(|s| {
                s.conditions
                    .iter()
                    .filter(|c| c.type_ != "CaRotationRequired")
                    .map(|c| {
                        serde_json::json!({
                            "type": c.type_,
                            "status": c.status,
                            "reason": c.reason,
                            "message": c.message,
                            "lastTransitionTime": c.last_transition_time,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        conditions.push(serde_json::json!({
            "type": "CaRotationRequired",
            "status": "True",
            "reason": reason,
            "message": message,
            "lastTransitionTime": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }));
        let patch = serde_json::json!({ "status": { "conditions": conditions } });
        kafka_api
            .patch_status(
                &kafka.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await?;
    } else {
        // BYO CA: event only, no status condition (spec: BYO emits only the Event).
        emit_event(
            client,
            &ns,
            kafka,
            "Warning",
            "ByoCaExpiringSoon",
            &format!(
                "CA {} is expiring within renewalDays; \
                 rotation is the cluster admin's responsibility (BYO)",
                which.condition_name()
            ),
            "crabka-ca-renewal-",
            "RenewalCheck",
            "crabka-operator/ca-renewal-check",
        )
        .await?;
    }
    Ok(())
}

/// Extract the CN (from the subject) and the SAN list (from the SAN extension)
/// out of an existing broker leaf cert PEM. Used by `renew_broker_leafs` so the
/// renewal `CronJob` preserves the exact identity originally issued by the reconciler
/// rather than reconstructing it from scratch (which would be fragile w.r.t. pool
/// names and ordinals the `CronJob` doesn't have access to).
fn read_existing_cn_and_sans(
    cert_pem: &str,
) -> Result<(String, Vec<SubjectAltName>), ReconcileError> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::{FromDer, X509Certificate};

    let der = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .next()
        .ok_or_else(|| ReconcileError::CertParse("no PEM block in broker cert".into()))?
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;

    // Extract CN from subject.
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| ReconcileError::CertParse("broker cert has no CN in subject".into()))?
        .to_string();

    // Extract SANs from the SubjectAltName extension.
    let sans: Vec<SubjectAltName> = cert
        .subject_alternative_name()
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?
        .map(|san_ext| {
            san_ext
                .value
                .general_names
                .iter()
                .filter_map(|gn| match gn {
                    GeneralName::DNSName(s) => Some(SubjectAltName::Dns(s.to_string())),
                    GeneralName::IPAddress(bytes) => {
                        // x509_parser gives raw bytes: 4 bytes = IPv4, 16 = IPv6.
                        let bytes: &[u8] = bytes;
                        match bytes.len() {
                            4 => {
                                let arr: [u8; 4] = bytes.try_into().ok()?;
                                Some(SubjectAltName::Ip(IpAddr::V4(arr.into())))
                            }
                            16 => {
                                let arr: [u8; 16] = bytes.try_into().ok()?;
                                Some(SubjectAltName::Ip(IpAddr::V6(arr.into())))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    Ok((cn, sans))
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
        .filter(|k| {
            std::path::Path::new(k.as_str())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("crt"))
        })
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
        let (cn, sans) = match read_existing_cn_and_sans(cert_pem) {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(
                    cluster = %cluster,
                    broker_id = id,
                    error = %e,
                    "ca-renewal-check: could not parse CN/SANs from existing broker cert; skipping renewal"
                );
                continue;
            }
        };
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &cn,
            &sans,
            &[],
            validity_days,
        )?;
        data.insert(crt_key.clone(), ByteString(leaf.cert_pem.into_bytes()));
        data.insert(format!("{id}.key"), ByteString(leaf.key_pem.into_bytes()));
        let digest = compute_san_digest(&sans, &[]);
        data.insert(format!("{id}.sans-digest"), ByteString(digest.into_bytes()));
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
            "crabka-ca-renewal-",
            "RenewalCheck",
            "crabka-operator/ca-renewal-check",
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // shared event helper; arity reflects the K8s Event fields
pub(crate) async fn emit_event(
    client: &kube::Client,
    namespace: &str,
    kafka: &Kafka,
    type_: &str,
    reason: &str,
    message: &str,
    generate_name: &str,
    action: &str,
    reporting_component: &str,
) -> Result<(), ReconcileError> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
    use k8s_openapi::jiff::Timestamp;
    let now = Timestamp::now();
    let event = Event {
        metadata: ObjectMeta {
            generate_name: Some(generate_name.into()),
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
        action: Some(action.into()),
        reporting_component: Some(reporting_component.into()),
        reporting_instance: Some(
            std::env::var("POD_NAME").unwrap_or_else(|_| "crabka-operator-renewal".into()),
        ),
        ..Default::default()
    };
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    api.create(&PostParams::default(), &event).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_security::ca::{generate_clients_ca, generate_cluster_ca, issue_user_cert};

    /// A CA generated with `validity_days = 30` must have `notAfter` within
    /// [29, 31] days of now (allowing for a second of clock skew in CI).
    #[test]
    fn ca_validity_days_is_honored() {
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::pem::PemObject;
        use x509_parser::prelude::{FromDer, X509Certificate};

        let ca = generate_cluster_ca("test-cluster-ca", 30).expect("CA");
        let der = CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes())
            .next()
            .expect("PEM block")
            .expect("valid PEM");
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("valid DER");
        let not_after = OffsetDateTime::from_unix_timestamp(cert.validity().not_after.timestamp())
            .expect("valid timestamp");
        let now = OffsetDateTime::now_utc();
        let days_remaining = (not_after - now).whole_days();
        assert!(
            (29..=31).contains(&days_remaining),
            "expected ~30 days remaining, got {days_remaining}"
        );
    }

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
        let now = OffsetDateTime::now_utc() + time::Duration::days(10);
        assert!(renew_if_expiring(&user.cert_pem, 30, now).expect("predicate"));
    }
}

#[cfg(test)]
mod reissue_tests {
    use super::compute_san_digest;
    use crabka_security::ca::SubjectAltName;

    #[test]
    fn san_digest_changes_when_extras_differ() {
        let base = vec![SubjectAltName::Dns("internal.svc".into())];
        let no_extras = compute_san_digest(&base, &[]);
        let with_extras =
            compute_san_digest(&base, &[SubjectAltName::Dns("broker-0.example.com".into())]);
        assert_ne!(no_extras, with_extras);
    }

    #[test]
    fn san_digest_stable_for_same_inputs_in_different_order() {
        let a = vec![
            SubjectAltName::Dns("a.example.com".into()),
            SubjectAltName::Dns("b.example.com".into()),
        ];
        let b = vec![
            SubjectAltName::Dns("b.example.com".into()),
            SubjectAltName::Dns("a.example.com".into()),
        ];
        assert_eq!(compute_san_digest(&a, &[]), compute_san_digest(&b, &[]));
    }

    #[test]
    fn san_digest_dedupes_overlap_between_base_and_extras() {
        let base = vec![SubjectAltName::Dns("internal.svc".into())];
        let extras = vec![SubjectAltName::Dns("internal.svc".into())];
        let single = compute_san_digest(&base, &[]);
        let with_dup_extra = compute_san_digest(&base, &extras);
        assert_eq!(
            single, with_dup_extra,
            "duplicate extras should not change digest"
        );
    }
}

#[cfg(test)]
mod san_tests {
    use crabka_security::ca::{SubjectAltName, generate_cluster_ca, issue_broker_cert};
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::{FromDer, X509Certificate};

    fn parse_cert_sans(cert_pem: &str) -> Vec<String> {
        let der = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
            .next()
            .expect("PEM block")
            .expect("valid PEM");
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("valid DER");
        cert.subject_alternative_name()
            .expect("SAN parse")
            .map(|san_ext| {
                san_ext
                    .value
                    .general_names
                    .iter()
                    .map(|gn| match gn {
                        GeneralName::DNSName(s) => format!("DNS:{s}"),
                        GeneralName::IPAddress(bytes) => {
                            let bytes: &[u8] = bytes;
                            match bytes.len() {
                                4 => {
                                    let arr: [u8; 4] = bytes.try_into().expect("4 bytes");
                                    format!("IP:{}", std::net::IpAddr::V4(arr.into()))
                                }
                                16 => {
                                    let arr: [u8; 16] = bytes.try_into().expect("16 bytes");
                                    format!("IP:{}", std::net::IpAddr::V6(arr.into()))
                                }
                                _ => "IP:unknown".to_string(),
                            }
                        }
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn issue_broker_cert_includes_extra_sans_in_leaf() {
        let cluster_ca = generate_cluster_ca("test-san-ca", 365).expect("test CA");
        let extra = vec![
            SubjectAltName::Dns("broker-0.example.com".into()),
            SubjectAltName::Ip("203.0.113.10".parse().unwrap()),
        ];
        let internal_sans = vec![SubjectAltName::Dns("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &extra,
            365,
        )
        .unwrap();
        let parsed_sans = parse_cert_sans(&leaf.cert_pem);
        assert!(parsed_sans.iter().any(|s| s == "DNS:internal.svc"));
        assert!(parsed_sans.iter().any(|s| s == "DNS:broker-0.example.com"));
        assert!(parsed_sans.iter().any(|s| s == "IP:203.0.113.10"));
    }

    #[test]
    fn issue_broker_cert_empty_extra_sans_yields_base_sans_only() {
        let cluster_ca = generate_cluster_ca("test-san-ca", 365).expect("test CA");
        let internal_sans = vec![SubjectAltName::Dns("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &[],
            365,
        )
        .unwrap();
        let parsed = parse_cert_sans(&leaf.cert_pem);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0], "DNS:internal.svc");
    }
}
