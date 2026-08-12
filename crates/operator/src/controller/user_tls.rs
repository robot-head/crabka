//! TLS-auth helpers for the `KafkaUser` reconciler.
//!
//! This module owns:
//! - the issuance and the renewal of the X.509 cert of each user,
//! - the render of the TLS-credential Secret of each user.
//!
//! `controller/user.rs` calls into this module from its reconcile pipeline
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
    controller::{
        cluster_ca::{self, WhichCa},
        common::{FIELD_MANAGER, ReconcileError, read_pem_key},
    },
    crd::{Authentication, Kafka, KafkaUser, user::TlsAuth},
};

/// Default cert lifetime in days, used when `TlsAuth::validity_days` is
/// absent.
pub(crate) const DEFAULT_VALIDITY_DAYS: u32 = 365;
/// Default renewal window in days, used when `TlsAuth::renewal_days` is
/// absent.
pub(crate) const DEFAULT_RENEWAL_DAYS: u32 = 30;

/// Outcome of `ensure_user_cert_secret`. The status update reads it.
#[derive(Debug, Clone)]
pub(crate) struct UserCertStatus {
    /// RFC3339 `notAfter` from the cert, whether the operator issued it
    /// now or reused it.
    pub not_after: String,
    /// Whether the operator issued a new cert in this reconcile. This
    /// field is for observability only. No logic depends on it.
    pub issued_new: bool,
}

/// Loads the active clients CA without advancing its staged rotation and then
/// reconciles one user's TLS Secret.
pub(crate) async fn reconcile_user_cert_secret(
    secret_api: &Api<Secret>,
    obj: &KafkaUser,
    kafka: &Kafka,
    auth: &TlsAuth,
) -> Result<UserCertStatus, ReconcileError> {
    let ca = cluster_ca::reconcile_ca(
        secret_api,
        kafka,
        WhichCa::Clients,
        false,
        false,
        false,
        OffsetDateTime::now_utc(),
    )
    .await?;
    ensure_user_cert_secret(
        secret_api,
        obj,
        &ca.signing_material,
        &ca.trust_bundle_pem,
        auth,
    )
    .await
}

/// Gets the cert Secret of one user, or creates it.
///
/// The function is idempotent. When the existing Secret carries a cert
/// whose `notAfter` is more than `renewal_days` in the future, the
/// function returns its status unchanged. If not, the function issues a
/// new cert and applies the Secret with a PATCH.
pub(crate) async fn ensure_user_cert_secret(
    secret_api: &Api<Secret>,
    obj: &KafkaUser,
    ca_material: &CaMaterial,
    ca_trust_bundle_pem: &str,
    auth: &TlsAuth,
) -> Result<UserCertStatus, ReconcileError> {
    let name = obj.name_any();
    let validity = auth.validity_days.unwrap_or(DEFAULT_VALIDITY_DAYS);
    let renewal = auth.renewal_days.unwrap_or(DEFAULT_RENEWAL_DAYS);

    if let Some(existing) = secret_api.get_opt(&name).await?
        && let Some(not_after) = read_user_cert_not_after(&existing)
        && !is_cert_expiring_soon(&not_after, renewal, OffsetDateTime::now_utc())
        && read_pem_key(&existing, "user.crt")
            .is_some_and(|cert| cert_is_signed_by(&cert, &ca_material.cert_pem))
    {
        if read_pem_key(&existing, "ca.crt").as_deref() != Some(ca_trust_bundle_pem) {
            let patch = Secret {
                data: Some(
                    [(
                        "ca.crt".into(),
                        ByteString(ca_trust_bundle_pem.as_bytes().to_vec()),
                    )]
                    .into(),
                ),
                ..Default::default()
            };
            secret_api
                .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                .await?;
        }
        return Ok(UserCertStatus {
            not_after: format_rfc3339(not_after)?,
            issued_new: false,
        });
    }

    let user_cert =
        ca::issue_user_cert(&ca_material.cert_pem, &ca_material.key_pem, &name, validity)
            .map_err(ReconcileError::Ca)?;

    let secret = render_user_cert_secret(obj, &user_cert, ca_trust_bundle_pem)?;
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

/// Reissues each live TLS user's certificate against the active clients CA.
///
/// Already-updated certificates are reused after signature verification. A
/// retry after a partial Kubernetes failure therefore only patches the users
/// that still carry an old-key certificate.
pub(crate) async fn reissue_tls_user_cert_secrets(
    secret_api: &Api<Secret>,
    users: &[KafkaUser],
    ca_material: &CaMaterial,
    ca_trust_bundle_pem: &str,
) -> Result<usize, ReconcileError> {
    let mut issued = 0;
    for user in users
        .iter()
        .filter(|user| user.meta().deletion_timestamp.is_none())
    {
        let Authentication::Tls(auth) = &user.spec.authentication else {
            continue;
        };
        let status =
            ensure_user_cert_secret(secret_api, user, ca_material, ca_trust_bundle_pem, auth)
                .await?;
        issued += usize::from(status.issued_new);
    }
    Ok(issued)
}

/// Composes the Kafka principal for a TLS user, which is
/// `User:CN=<name>`.
///
/// The SCRAM path lives in `controller/user.rs::principal_for` today.
/// Batch B2 joins the two.
#[must_use]
pub(crate) fn tls_principal(name: &str) -> String {
    format!("User:CN={name}")
}

/// Reports whether `not_after` is within `renewal_days` of `now`. This
/// function is pure.
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

/// Parses the `user.crt` PEM out of an existing user Secret and returns
/// the `notAfter` of the cert as a `time::OffsetDateTime`.
///
/// This function returns `None` when the key is absent, when the PEM is
/// malformed, and when the cert does not parse. The caller reads `None` as
/// an instruction to issue a new cert.
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

fn cert_is_signed_by(cert_pem: &str, ca_cert_pem: &str) -> bool {
    use x509_parser::pem::parse_x509_pem;

    let Ok((_, cert_pem)) = parse_x509_pem(cert_pem.as_bytes()) else {
        return false;
    };
    let Ok(cert) = cert_pem.parse_x509() else {
        return false;
    };
    let Ok((_, ca_pem)) = parse_x509_pem(ca_cert_pem.as_bytes()) else {
        return false;
    };
    let Ok(ca) = ca_pem.parse_x509() else {
        return false;
    };
    cert.verify_signature(Some(ca.public_key())).is_ok()
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
        for input in [
            "not a pem",
            "",
            "-----BEGIN CERTIFICATE-----\nQUFB\n-----END CERTIFICATE-----",
        ] {
            assert!(cert_not_after_from_pem(input).is_none(), "case {input:?}");
        }
    }

    #[test]
    fn user_cert_matches_only_its_signing_ca_key() {
        let old_ca = ca::generate_clients_ca("old", 365).expect("old CA");
        let new_ca = ca::generate_clients_ca("new", 365).expect("new CA");
        let user = ca::issue_user_cert(&old_ca.cert_pem, &old_ca.key_pem, "alice", 365)
            .expect("user cert");

        assert!(cert_is_signed_by(&user.cert_pem, &old_ca.cert_pem));
        assert!(!cert_is_signed_by(&user.cert_pem, &new_ca.cert_pem));
    }

    #[test]
    fn malformed_certificate_never_matches_ca() {
        let ca = ca::generate_clients_ca("ca", 365).expect("CA");
        assert!(!cert_is_signed_by("bad cert", &ca.cert_pem));
        assert!(!cert_is_signed_by(&ca.cert_pem, "bad CA"));
    }

    #[test]
    fn read_pem_key_returns_some_when_present() {
        let mut data = BTreeMap::new();
        data.insert("ca.key".into(), ByteString(b"abc".to_vec()));
        let s = Secret {
            data: Some(data),
            ..Default::default()
        };
        assert!(read_pem_key(&s, "ca.key").as_deref() == Some("abc"));
    }

    #[test]
    fn read_pem_key_returns_none_when_data_missing() {
        let s = Secret::default();
        assert!(read_pem_key(&s, "ca.key").is_none());
    }

    #[test]
    fn read_pem_key_returns_none_when_key_missing() {
        let mut data = BTreeMap::new();
        data.insert("other".into(), ByteString(b"abc".to_vec()));
        let s = Secret {
            data: Some(data),
            ..Default::default()
        };
        assert!(read_pem_key(&s, "ca.key").is_none());
    }

    #[test]
    fn read_pem_key_returns_none_on_non_utf8() {
        let mut data = BTreeMap::new();
        data.insert("ca.key".into(), ByteString(vec![0xFF, 0xFE, 0xFD]));
        let s = Secret {
            data: Some(data),
            ..Default::default()
        };
        assert!(read_pem_key(&s, "ca.key").is_none());
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
    fn render_user_cert_secret_carries_three_keys_and_tls_auth_label() {
        let ku = dummy_ku();
        let user_cert = ca::UserCert {
            cert_pem: "CERT".into(),
            key_pem: "KEY".into(),
            not_after: "2027-01-01T00:00:00Z".into(),
        };
        let secret = render_user_cert_secret(&ku, &user_cert, "CA-CERT").expect("renders");

        assert!(secret.metadata.name.as_deref() == Some("alice"));
        assert!(secret.metadata.namespace.as_deref() == Some("ns"));
        let labels = secret.metadata.labels.as_ref().expect("labels");
        for (key, want) in [
            ("crabka.io/auth", "tls"),
            ("crabka.io/user", "alice"),
            ("crabka.io/cluster", "demo"),
        ] {
            assert!(
                labels.get(key).map(String::as_str) == Some(want),
                "label {key:?}"
            );
        }
        let owners = secret.metadata.owner_references.as_ref().expect("owner");
        assert!(
            *owners
                == vec![OwnerReference {
                    api_version: "crabka.io/v1alpha1".into(),
                    block_owner_deletion: Some(true),
                    controller: Some(true),
                    kind: "KafkaUser".into(),
                    name: "alice".into(),
                    uid: "user-uid".into(),
                }]
        );
        let data = secret.data.as_ref().expect("data");
        for (key, want) in [
            ("user.crt", b"CERT".as_slice()),
            ("user.key", b"KEY".as_slice()),
            ("ca.crt", b"CA-CERT".as_slice()),
        ] {
            assert!(
                data.get(key).map(|bs| bs.0.as_slice()) == Some(want),
                "data key {key:?}"
            );
        }
    }

    #[test]
    fn render_user_cert_secret_omits_cluster_label_when_label_absent() {
        let mut ku = dummy_ku();
        ku.metadata.labels = None;
        let user_cert = ca::UserCert {
            cert_pem: "C".into(),
            key_pem: "K".into(),
            not_after: "2027-01-01T00:00:00Z".into(),
        };
        let secret = render_user_cert_secret(&ku, &user_cert, "CA").expect("renders");
        let labels = secret.metadata.labels.as_ref().expect("labels");
        assert!(!labels.contains_key("crabka.io/cluster"));
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
