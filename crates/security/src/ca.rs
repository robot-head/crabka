//! Pure X.509 CA + leaf-cert generation for the operator's
//! clients-CA bootstrap (slice 37). Reusable by inter-broker mTLS
//! (slice 30) and cert hot-reload tests (slice 33).
//!
//! No async, no I/O — these helpers return PEM-encoded material
//! that callers persist to Kubernetes Secrets, files, or anywhere
//! else.

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
};
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

#[derive(Debug, Error)]
pub enum CaError {
    #[error("rcgen: {0}")]
    Rcgen(#[from] rcgen::Error),
    #[error("time math overflow")]
    TimeOverflow,
    #[error("time format: {0}")]
    TimeFormat(#[from] time::error::Format),
}

/// Self-signed clients-CA material.
pub struct CaMaterial {
    pub cert_pem: String,
    pub key_pem: String,
}

/// A leaf cert issued by a clients-CA. `not_after` is RFC3339.
pub struct UserCert {
    pub cert_pem: String,
    pub key_pem: String,
    pub not_after: String,
}

fn validity_window(validity_days: u32) -> Result<(OffsetDateTime, OffsetDateTime), CaError> {
    let not_before = OffsetDateTime::now_utc();
    let not_after = not_before
        .checked_add(Duration::days(i64::from(validity_days)))
        .ok_or(CaError::TimeOverflow)?;
    Ok((not_before, not_after))
}

/// Generate a self-signed clients CA with `Subject = CN=<cn>, O=crabka`,
/// `BasicConstraints: CA:TRUE`, and `KeyUsage = keyCertSign|cRLSign`.
/// ECDSA P-256.
pub fn generate_clients_ca(cn: &str, validity_days: u32) -> Result<CaMaterial, CaError> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

    let mut params = CertificateParams::new(Vec::<String>::new())?;
    let (not_before, not_after) = validity_window(validity_days)?;
    params.not_before = not_before;
    params.not_after = not_after;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn.push(DnType::OrganizationName, "crabka");
    params.distinguished_name = dn;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let cert = params.self_signed(&key)?;

    Ok(CaMaterial {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Sign a leaf client cert with `Subject = CN=<cn>` (bare RDN —
/// matches Strimzi, avoids RFC 2253 vs 4514 ordering ambiguity).
/// `ExtendedKeyUsage = clientAuth`, `KeyUsage = digitalSignature|keyEncipherment`.
/// ECDSA P-256.
pub fn issue_user_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    validity_days: u32,
) -> Result<UserCert, CaError> {
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
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

    let leaf = params.signed_by(&leaf_key, &ca_cert, &ca_key)?;
    let not_after_str = not_after.format(&Rfc3339)?;

    Ok(UserCert {
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
        not_after: not_after_str,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use x509_parser::prelude::FromDer;
    use x509_parser::prelude::X509Certificate;

    fn pem_to_der(pem: &str) -> CertificateDer<'static> {
        CertificateDer::pem_slice_iter(pem.as_bytes())
            .next()
            .expect("at least one PEM cert")
            .expect("valid PEM cert")
    }

    #[test]
    fn generate_clients_ca_round_trips() {
        let validity_days: u32 = 365;
        let ca = generate_clients_ca("root", validity_days).expect("generate CA");

        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));

        let der = pem_to_der(&ca.cert_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse CA DER");

        let subject = cert.subject().to_string();
        assert!(subject.contains("CN=root"), "subject was {subject}");
        assert!(subject.contains("O=crabka"), "subject was {subject}");

        let bc = cert
            .basic_constraints()
            .expect("basic constraints parse")
            .expect("basic constraints present");
        assert!(bc.value.ca, "CA bit must be true on clients CA");

        let validity = cert.validity();
        let span = validity.not_after.timestamp() - validity.not_before.timestamp();
        let expected = i64::from(validity_days) * 86_400;
        let tolerance: i64 = 60;
        assert!(
            (span - expected).abs() <= tolerance,
            "validity span {span}s expected ~{expected}s"
        );
    }

    #[test]
    fn issue_user_cert_signed_by_ca_and_bare_cn() {
        let ca = generate_clients_ca("root", 365).expect("generate CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("issue leaf");

        let leaf_der = pem_to_der(&user.cert_pem);
        let (_, leaf) = X509Certificate::from_der(leaf_der.as_ref()).expect("parse leaf DER");
        assert_eq!(leaf.subject().to_string(), "CN=alice");

        let ca_der = pem_to_der(&ca.cert_pem);
        let (_, ca_x509) = X509Certificate::from_der(ca_der.as_ref()).expect("parse CA DER");

        leaf.verify_signature(Some(ca_x509.public_key()))
            .expect("leaf signature must verify against CA pubkey");
    }

    #[test]
    fn issue_user_cert_dn_matches_extract_principal() {
        let ca = generate_clients_ca("root", 365).expect("generate CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("issue leaf");

        let der = pem_to_der(&user.cert_pem);
        let dn = crate::extract_principal_from_cert(der.as_ref()).expect("extract principal");
        assert_eq!(dn, "CN=alice");
    }

    #[test]
    fn extended_key_usage_is_client_auth_on_leaf() {
        let ca = generate_clients_ca("root", 365).expect("generate CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("issue leaf");

        let der = pem_to_der(&user.cert_pem);
        let (_, leaf) = X509Certificate::from_der(der.as_ref()).expect("parse leaf DER");
        let eku = leaf
            .extended_key_usage()
            .expect("EKU parse")
            .expect("EKU present");
        assert!(eku.value.client_auth, "client_auth must be set on leaf EKU");
    }

    #[test]
    fn each_generate_is_unique() {
        let a = generate_clients_ca("x", 365).expect("generate CA a");
        let b = generate_clients_ca("x", 365).expect("generate CA b");
        assert_ne!(
            a.cert_pem, b.cert_pem,
            "each CA must have unique serial/key"
        );
        assert_ne!(a.key_pem, b.key_pem, "each CA must have a unique key");
    }
}
