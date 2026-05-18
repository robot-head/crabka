//! Slice 37: TLS-auth helpers for the `KafkaUser` reconciler.
//!
//! Owns:
//! - the per-cluster clients CA (Secret bootstrap, lazy create),
//! - per-user X.509 cert issuance + renewal,
//! - the per-user TLS-credential Secret render.
//!
//! `controller/user.rs` dispatches into here from its reconcile pipeline
//! when `spec.authentication` is `Authentication::Tls(_)`.

use std::collections::BTreeMap;

use crabka_security::ca::{self, CaMaterial};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, Patch, PatchParams};
use kube::{Resource, ResourceExt as _};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::controller::common::{FIELD_MANAGER, ReconcileError, owner_ref};
use crate::crd::user::TlsAuth;
use crate::crd::{Kafka, KafkaUser};

/// Default cert lifetime (days) when `TlsAuth::validity_days` is absent.
pub(crate) const DEFAULT_VALIDITY_DAYS: u32 = 365;
/// Default renewal window (days) when `TlsAuth::renewal_days` is absent.
pub(crate) const DEFAULT_RENEWAL_DAYS: u32 = 30;
/// Suffix on the per-cluster clients-CA private-key Secret.
pub(crate) const CLIENTS_CA_KEY_SUFFIX: &str = "-clients-ca";
/// Suffix on the per-cluster clients-CA public-cert Secret.
pub(crate) const CLIENTS_CA_CERT_SUFFIX: &str = "-clients-ca-cert";

/// Outcome of `ensure_user_cert_secret`. Drives the status update.
#[derive(Debug, Clone)]
pub(crate) struct UserCertStatus {
    /// RFC3339 `notAfter` from the (newly issued or reused) cert.
    pub not_after: String,
    /// Whether the operator issued a new cert this reconcile.
    /// Pure observability; not load-bearing.
    pub issued_new: bool,
}

/// Get-or-create both clients-CA Secrets for the given cluster.
/// Both Secrets are owner-ref'd to the parent `Kafka`. Returns the CA
/// PEM material so the caller can use it to sign user certs.
///
/// When the private-key Secret exists but the cert Secret doesn't (or
/// vice-versa, e.g., a partial hand-edit), the operator treats it as
/// "regenerate both". Slice 30 will replace this with full
/// rotation/renewal semantics.
pub(crate) async fn ensure_clients_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
) -> Result<CaMaterial, ReconcileError> {
    let cluster = kafka.name_any();
    let key_name = format!("{cluster}{CLIENTS_CA_KEY_SUFFIX}");
    let cert_name = format!("{cluster}{CLIENTS_CA_CERT_SUFFIX}");

    let existing_key = secret_api.get_opt(&key_name).await?;
    let existing_cert = secret_api.get_opt(&cert_name).await?;

    if let (Some(k), Some(c)) = (&existing_key, &existing_cert)
        && let (Some(key_pem), Some(cert_pem)) =
            (read_pem_key(k, "ca.key"), read_pem_key(c, "ca.crt"))
    {
        return Ok(CaMaterial { cert_pem, key_pem });
    }

    // Either Secret missing or malformed → regenerate both.
    let cn = format!("{cluster}-clients-ca");
    let material = ca::generate_clients_ca(&cn, 10 * 365).map_err(ReconcileError::Ca)?;

    let key_secret = render_clients_ca_secret(
        kafka,
        &key_name,
        "ca.key",
        &material.key_pem,
        "clients-ca-key",
    )?;
    let cert_secret = render_clients_ca_secret(
        kafka,
        &cert_name,
        "ca.crt",
        &material.cert_pem,
        "clients-ca-cert",
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
    Ok(material)
}

/// Get-or-create the per-user cert Secret. Idempotent: if the existing
/// Secret carries a cert whose `notAfter` is more than `renewal_days`
/// in the future, returns its status unchanged. Otherwise issues a new
/// cert and PATCH-applies the Secret.
pub(crate) async fn ensure_user_cert_secret(
    secret_api: &Api<Secret>,
    obj: &KafkaUser,
    ca_material: &CaMaterial,
    auth: &TlsAuth,
) -> Result<UserCertStatus, ReconcileError> {
    let name = obj.name_any();
    let validity = auth.validity_days.unwrap_or(DEFAULT_VALIDITY_DAYS);
    let renewal = auth.renewal_days.unwrap_or(DEFAULT_RENEWAL_DAYS);

    if let Some(existing) = secret_api.get_opt(&name).await?
        && let Some(not_after) = read_user_cert_not_after(&existing)
        && !is_cert_expiring_soon(&not_after, renewal, OffsetDateTime::now_utc())
    {
        return Ok(UserCertStatus {
            not_after: format_rfc3339(not_after)?,
            issued_new: false,
        });
    }

    let user_cert =
        ca::issue_user_cert(&ca_material.cert_pem, &ca_material.key_pem, &name, validity)
            .map_err(ReconcileError::Ca)?;

    let secret = render_user_cert_secret(obj, &user_cert, &ca_material.cert_pem)?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&name, &params, &Patch::Apply(&secret))
        .await?;
    Ok(UserCertStatus {
        not_after: user_cert.not_after,
        issued_new: true,
    })
}

/// Compose Kafka principal for a TLS user (`User:CN=<name>`). The
/// SCRAM path lives in `controller/user.rs::principal_for` for now;
/// Batch B2 unifies them.
#[must_use]
pub(crate) fn tls_principal(name: &str) -> String {
    format!("User:CN={name}")
}

/// Pure: is `not_after` within `renewal_days` of `now`?
#[must_use]
pub(crate) fn is_cert_expiring_soon(
    not_after: &OffsetDateTime,
    renewal_days: u32,
    now: OffsetDateTime,
) -> bool {
    let window = time::Duration::days(i64::from(renewal_days));
    *not_after <= now + window
}

fn format_rfc3339(t: OffsetDateTime) -> Result<String, ReconcileError> {
    t.format(&Rfc3339)
        .map_err(|e| ReconcileError::CertParse(format!("rfc3339 format: {e}")))
}

fn read_pem_key(secret: &Secret, key: &str) -> Option<String> {
    let data = secret.data.as_ref()?;
    let bs = data.get(key)?;
    std::str::from_utf8(&bs.0).ok().map(str::to_string)
}

/// Parse `user.crt` PEM out of an existing user Secret and return
/// the cert's `notAfter` as a `time::OffsetDateTime`. Returns `None`
/// if the key is missing, the PEM is malformed, or the cert won't
/// parse — caller treats `None` as "reissue".
fn read_user_cert_not_after(secret: &Secret) -> Option<OffsetDateTime> {
    let pem = read_pem_key(secret, "user.crt")?;
    cert_not_after_from_pem(&pem)
}

fn cert_not_after_from_pem(pem: &str) -> Option<OffsetDateTime> {
    use x509_parser::pem::parse_x509_pem;
    let (_, p) = parse_x509_pem(pem.as_bytes()).ok()?;
    let cert = p.parse_x509().ok()?;
    let ts = cert.validity().not_after.timestamp();
    OffsetDateTime::from_unix_timestamp(ts).ok()
}

fn render_clients_ca_secret(
    kafka: &Kafka,
    name: &str,
    key: &str,
    value_pem: &str,
    component: &str,
) -> Result<Secret, ReconcileError> {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    labels.insert("crabka.io/cluster".into(), kafka.name_any());
    labels.insert("crabka.io/component".into(), component.into());

    let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
    data.insert(key.into(), ByteString(value_pem.as_bytes().to_vec()));

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: kafka.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

fn render_user_cert_secret(
    obj: &KafkaUser,
    user_cert: &ca::UserCert,
    ca_cert_pem: &str,
) -> Result<Secret, ReconcileError> {
    let name = obj.name_any();
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), "crabka-broker".into());
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    if let Some(cluster) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster"))
    {
        labels.insert("crabka.io/cluster".into(), cluster.clone());
    }
    labels.insert("crabka.io/user".into(), name.clone());
    labels.insert("crabka.io/auth".into(), "tls".into());

    let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
    data.insert(
        "user.crt".into(),
        ByteString(user_cert.cert_pem.as_bytes().to_vec()),
    );
    data.insert(
        "user.key".into(),
        ByteString(user_cert.key_pem.as_bytes().to_vec()),
    );
    data.insert("ca.crt".into(), ByteString(ca_cert_pem.as_bytes().to_vec()));

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: obj.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![user_owner_ref(obj)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

fn user_owner_ref(obj: &KafkaUser) -> Result<OwnerReference, ReconcileError> {
    let uid = obj
        .meta()
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    Ok(OwnerReference {
        api_version: <KafkaUser as Resource>::api_version(&()).to_string(),
        kind: <KafkaUser as Resource>::kind(&()).to_string(),
        name: obj.name_any(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_principal_format() {
        assert_eq!(tls_principal("alice"), "User:CN=alice");
    }

    #[test]
    fn is_cert_expiring_soon_boundary_cases() {
        let now = OffsetDateTime::now_utc();

        let in_60_days = now + time::Duration::days(60);
        assert!(
            !is_cert_expiring_soon(&in_60_days, 30, now),
            "60d > 30d window: not expiring"
        );

        let in_30_days = now + time::Duration::days(30);
        assert!(
            is_cert_expiring_soon(&in_30_days, 30, now),
            "30d == 30d window: at boundary, treat as expiring (<=)"
        );

        let in_1_day = now + time::Duration::days(1);
        assert!(
            is_cert_expiring_soon(&in_1_day, 30, now),
            "1d within 30d window: expiring"
        );

        let yesterday = now - time::Duration::days(1);
        assert!(
            is_cert_expiring_soon(&yesterday, 30, now),
            "already past notAfter: expiring"
        );
    }

    #[test]
    fn cert_not_after_round_trips() {
        let ca = ca::generate_clients_ca("test-root", 365).expect("ca");
        let before = OffsetDateTime::now_utc();
        let user = ca::issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("leaf");

        let parsed = cert_not_after_from_pem(&user.cert_pem).expect("notAfter parses");
        let expected = before + time::Duration::days(365);

        let delta = (parsed - expected).whole_seconds().abs();
        assert!(
            delta <= 5,
            "notAfter delta {delta}s exceeds ±5s tolerance (parsed={parsed}, expected={expected})"
        );
    }
}
