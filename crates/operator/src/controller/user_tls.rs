//! TLS-auth helpers for the `KafkaUser` reconciler.
//!
//! Owns:
//! - per-user X.509 cert issuance + renewal,
//! - the per-user TLS-credential Secret render.
//!
//! `controller/user.rs` dispatches into here from its reconcile pipeline
//! when `spec.authentication` is `Authentication::Tls(_)`.

use std::collections::BTreeMap;

use crabka_security::ca::{self, CaMaterial};
use k8s_openapi::{
    ByteString,
    api::core::v1::Secret,
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, Patch, PatchParams},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    controller::common::{FIELD_MANAGER, ReconcileError, read_pem_key},
    crd::{KafkaUser, user::TlsAuth},
};

/// Default cert lifetime (days) when `TlsAuth::validity_days` is absent.
pub(crate) const DEFAULT_VALIDITY_DAYS: u32 = 365;
/// Default renewal window (days) when `TlsAuth::renewal_days` is absent.
pub(crate) const DEFAULT_RENEWAL_DAYS: u32 = 30;

/// Outcome of `ensure_user_cert_secret`. Drives the status update.
#[derive(Debug, Clone)]
pub(crate) struct UserCertStatus {
    /// RFC3339 `notAfter` from the (newly issued or reused) cert.
    pub not_after: String,
    /// Whether the operator issued a new cert this reconcile.
    /// Pure observability; not load-bearing.
    pub issued_new: bool,
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
    use assert2::assert;

    use super::*;

    #[test]
    fn tls_principal_format() {
        assert!(tls_principal("alice") == "User:CN=alice");
    }

    #[test]
    fn is_cert_expiring_soon_boundary_cases() {
        let now = OffsetDateTime::now_utc();

        for (days_from_now, want, why) in [
            (60, false, "60d > 30d window: not expiring"),
            (
                30,
                true,
                "30d == 30d window: at boundary, treat as expiring (<=)",
            ),
            (1, true, "1d within 30d window: expiring"),
            (-1, true, "already past notAfter: expiring"),
        ] {
            let not_after = now + time::Duration::days(days_from_now);
            assert!(is_cert_expiring_soon(&not_after, 30, now) == want, "{why}");
        }
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

    #[test]
    fn cert_not_after_from_pem_returns_none_on_malformed_input() {
        // The last case is valid PEM framing with a garbage body —
        // exercises the parse_x509 failure branch.
        for (name, input) in [
            ("not PEM framing", "not a pem"),
            ("empty input", ""),
            (
                "PEM framing with invalid certificate body",
                "-----BEGIN CERTIFICATE-----\nQUFB\n-----END CERTIFICATE-----",
            ),
        ] {
            assert!(cert_not_after_from_pem(input).is_none(), "case {name}");
        }
    }

    #[test]
    fn read_pem_key_cases() {
        for (name, data, expected) in [
            (
                "key present",
                Some(BTreeMap::from([(
                    "ca.key".to_string(),
                    ByteString(b"abc".to_vec()),
                )])),
                Some("abc"),
            ),
            ("Secret data missing", None, None),
            (
                "requested key missing",
                Some(BTreeMap::from([(
                    "other".to_string(),
                    ByteString(b"abc".to_vec()),
                )])),
                None,
            ),
            (
                "key is not UTF-8",
                Some(BTreeMap::from([(
                    "ca.key".to_string(),
                    ByteString(vec![0xFF, 0xFE, 0xFD]),
                )])),
                None,
            ),
        ] {
            let secret = Secret {
                data,
                ..Default::default()
            };
            assert_eq!(
                read_pem_key(&secret, "ca.key").as_deref(),
                expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn read_user_cert_not_after_returns_none_when_user_crt_missing() {
        let mut data = BTreeMap::new();
        data.insert("user.key".into(), ByteString(b"junk".to_vec()));
        let s = Secret {
            data: Some(data),
            ..Default::default()
        };
        assert!(read_user_cert_not_after(&s).is_none());
    }

    #[test]
    fn format_rfc3339_round_trips() {
        let t = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("unix ts");
        let s = format_rfc3339(t).expect("formats");
        assert!(s == "2023-11-14T22:13:20Z");
    }

    fn dummy_ku() -> KafkaUser {
        let mut ku = KafkaUser::new(
            "alice",
            crate::crd::KafkaUserSpec {
                authentication: crate::crd::Authentication::Tls(TlsAuth::default()),
                authorization: None,
                quotas: None,
            },
        );
        ku.metadata.namespace = Some("ns".into());
        ku.metadata.uid = Some("user-uid".into());
        let mut labels = BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), "demo".into());
        ku.metadata.labels = Some(labels);
        ku
    }

    #[test]
    fn render_user_cert_secret_cases() {
        for (name, cluster, cert_pem, key_pem, ca_pem) in [
            (
                "cluster label populated",
                Some("demo"),
                "CERT",
                "KEY",
                "CA-CERT",
            ),
            ("cluster label absent", None, "C", "K", "CA"),
        ] {
            let mut ku = dummy_ku();
            ku.metadata.labels = cluster.map(|value| {
                BTreeMap::from([("crabka.io/cluster".to_string(), value.to_string())])
            });
            let user_cert = ca::UserCert {
                cert_pem: cert_pem.into(),
                key_pem: key_pem.into(),
                not_after: "2027-01-01T00:00:00Z".into(),
            };
            let actual = render_user_cert_secret(&ku, &user_cert, ca_pem).expect("renders");

            let mut labels = BTreeMap::from([
                (
                    "app.kubernetes.io/name".to_string(),
                    "crabka-broker".to_string(),
                ),
                (
                    "app.kubernetes.io/managed-by".to_string(),
                    "crabka-operator".to_string(),
                ),
                ("crabka.io/user".to_string(), "alice".to_string()),
                ("crabka.io/auth".to_string(), "tls".to_string()),
            ]);
            if let Some(cluster) = cluster {
                labels.insert("crabka.io/cluster".to_string(), cluster.to_string());
            }
            let expected = Secret {
                metadata: ObjectMeta {
                    name: Some("alice".into()),
                    namespace: Some("ns".into()),
                    labels: Some(labels),
                    owner_references: Some(vec![OwnerReference {
                        api_version: "crabka.io/v1alpha1".into(),
                        block_owner_deletion: Some(true),
                        controller: Some(true),
                        kind: "KafkaUser".into(),
                        name: "alice".into(),
                        uid: "user-uid".into(),
                    }]),
                    ..Default::default()
                },
                type_: Some("Opaque".into()),
                data: Some(BTreeMap::from([
                    (
                        "user.crt".to_string(),
                        ByteString(cert_pem.as_bytes().to_vec()),
                    ),
                    (
                        "user.key".to_string(),
                        ByteString(key_pem.as_bytes().to_vec()),
                    ),
                    ("ca.crt".to_string(), ByteString(ca_pem.as_bytes().to_vec())),
                ])),
                ..Default::default()
            };
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn user_owner_ref_errors_on_missing_uid() {
        let mut ku = dummy_ku();
        ku.metadata.uid = None;
        assert!(matches!(
            user_owner_ref(&ku),
            Err(ReconcileError::MissingUid)
        ));
    }

    #[test]
    fn user_owner_ref_carries_block_owner_deletion() {
        let ku = dummy_ku();
        let owner = user_owner_ref(&ku).expect("owner ref");
        assert!(
            owner
                == OwnerReference {
                    api_version: "crabka.io/v1alpha1".into(),
                    block_owner_deletion: Some(true),
                    controller: Some(true),
                    kind: "KafkaUser".into(),
                    name: "alice".into(),
                    uid: "user-uid".into(),
                }
        );
    }
}
