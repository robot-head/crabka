//! Pure X.509 CA + leaf-cert generation for the operator's
//! clients-CA bootstrap. Inter-broker mTLS and cert hot-reload tests
//! can reuse it.
//!
//! No async, no I/O. These helpers return PEM-encoded material
//! that callers persist to Kubernetes Secrets, files, or anywhere
//! else.

use std::net::IpAddr;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SanType,
};
use thiserror::Error;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

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
#[derive(Debug, Clone)]
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

/// SAN entry for a leaf cert. ECDSA leaf certs accept any mix of DNS
/// names and IP addresses, and the broker-cert path uses a mix.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SubjectAltName {
    Dns(String),
    Ip(IpAddr),
}

/// A broker leaf cert, which is a server cert and a client cert in one.
pub struct BrokerCert {
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
// Clients-CA issuance. skip_all keeps the generated private key (a local)
// out of span fields; only the non-sensitive CN + validity are recorded.
// `err` surfaces rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
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

/// Generate a self-signed cluster CA.
///
/// The shape is the same as [`generate_clients_ca`]: ECDSA P-256, CA:TRUE, and
/// KU keyCertSign + cRLSign. The subject DN carries `OU=cluster`, so a reader
/// can tell the cluster CA and the clients CA apart in cert chains and audit
/// logs.
// Cluster-CA issuance. skip_all keeps the generated private key (a local)
// out of span fields; only the non-sensitive CN + validity are recorded.
// `err` surfaces rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
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

/// Re-sign a cluster CA cert with an existing key, a same-key renewal.
///
/// This function generates a fresh self-signed cert with the SAME subject DN,
/// `CN=<cn>, O=crabka, OU=cluster`, plus `CA:TRUE`, `KU keyCertSign|cRLSign`,
/// and a new `validityDays` window. It keys the cert with `key_pem` and not
/// with a freshly generated key. The public key (SPKI) and the subject DN are
/// identical to the cert this one replaces, so leaf certs issued under the old
/// cert still chain to the renewed one. The renewal is non-disruptive. This
/// function returns the cert PEM only, because the caller already holds the
/// key.
// Cluster-CA same-key renewal. skip_all keeps the `key_pem` secret out of span
// fields; only the non-sensitive CN + validity are recorded. `err` surfaces
// rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
pub fn renew_cluster_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String, CaError> {
    renew_ca(key_pem, cn, validity_days, true)
}

/// Re-sign a clients CA cert with an existing key, a same-key renewal.
///
/// This works like [`renew_cluster_ca`] but uses the clients-CA subject DN,
/// which has no `OU=cluster`.
// Clients-CA same-key renewal. skip_all keeps the `key_pem` secret out of span
// fields; only the non-sensitive CN + validity are recorded. `err` surfaces
// rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
pub fn renew_clients_ca(key_pem: &str, cn: &str, validity_days: u32) -> Result<String, CaError> {
    renew_ca(key_pem, cn, validity_days, false)
}

fn renew_ca(key_pem: &str, cn: &str, validity_days: u32, cluster: bool) -> Result<String, CaError> {
    let key = KeyPair::from_pem(key_pem)?;

    let mut params = CertificateParams::new(Vec::<String>::new())?;
    let (not_before, not_after) = validity_window(validity_days)?;
    params.not_before = not_before;
    params.not_after = not_after;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn.push(DnType::OrganizationName, "crabka");
    if cluster {
        dn.push(DnType::OrganizationalUnitName, "cluster");
    }
    params.distinguished_name = dn;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let cert = params.self_signed(&key)?;
    Ok(cert.pem())
}

/// Sign a broker leaf cert: a server cert and a client cert in one.
///
/// The cert carries `EKU = serverAuth + clientAuth` and
/// `KU = digitalSignature + keyEncipherment`. SANs accept a mix of DNS names
/// and IPs. The key is ECDSA P-256.
///
/// This function merges `base_sans` and `extra_sans` into a single SAN list and
/// drops duplicates silently. `extra_sans` holds entries such as the external
/// advertised addresses for `NodePort` or `LoadBalancer` listeners.
// Broker leaf-cert issuance. skip_all keeps the signing `ca_key_pem` secret +
// the generated leaf key (a local) out of span fields; only the non-sensitive
// CN + validity are recorded. `err` surfaces rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn issue_broker_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    base_sans: &[SubjectAltName],
    extra_sans: &[SubjectAltName],
    validity_days: u32,
) -> Result<BrokerCert, CaError> {
    let mut all_sans: Vec<SubjectAltName> = base_sans.to_vec();
    for s in extra_sans {
        if !all_sans.contains(s) {
            all_sans.push(s.clone());
        }
    }

    let ca_key = KeyPair::from_pem(ca_key_pem)?;
    let ca_issuer = Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)?;

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
    params.subject_alt_names = all_sans
        .iter()
        .map(|s| match s {
            SubjectAltName::Dns(d) => SanType::DnsName(d.parse().expect("valid Ia5String")),
            SubjectAltName::Ip(ip) => SanType::IpAddress(*ip),
        })
        .collect();

    let leaf = params.signed_by(&leaf_key, &ca_issuer)?;
    let not_after_str = not_after.format(&Rfc3339)?;

    Ok(BrokerCert {
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
        not_after: not_after_str,
    })
}

/// Sign a leaf client cert with `Subject = CN=<cn>`.
///
/// The subject is a bare RDN. This matches Strimzi and avoids the RFC 2253
/// against 4514 ordering ambiguity. The cert carries
/// `ExtendedKeyUsage = clientAuth` and
/// `KeyUsage = digitalSignature|keyEncipherment`. The key is ECDSA P-256.
// User leaf-cert issuance. skip_all keeps the signing `ca_key_pem` secret + the
// generated leaf key (a local) out of span fields; only the non-sensitive CN +
// validity are recorded. `err` surfaces rcgen / time failures (Debug).
#[tracing::instrument(level = "info", skip_all, fields(cn = %cn, validity_days), err)]
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
pub fn issue_user_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    validity_days: u32,
) -> Result<UserCert, CaError> {
    let ca_key = KeyPair::from_pem(ca_key_pem)?;
    let ca_issuer = Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)?;

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

    let leaf = params.signed_by(&leaf_key, &ca_issuer)?;
    let not_after_str = not_after.format(&Rfc3339)?;

    Ok(UserCert {
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
        not_after: not_after_str,
    })
}

#[cfg(test)]
mod tests {

    use rustls::pki_types::{CertificateDer, pem::PemObject};
    use x509_parser::prelude::{FromDer, X509Certificate};

    use super::*;

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

        assert2::assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert2::assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));

        let der = pem_to_der(&ca.cert_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse CA DER");

        let subject = cert.subject().to_string();
        assert2::assert!(subject.contains("CN=root"));
        assert2::assert!(subject.contains("O=crabka"));

        let bc = cert
            .basic_constraints()
            .expect("basic constraints parse")
            .expect("basic constraints present");
        assert2::assert!(bc.value.ca);

        let validity = cert.validity();
        let span = validity.not_after.timestamp() - validity.not_before.timestamp();
        let expected = i64::from(validity_days) * 86_400;
        let tolerance: i64 = 60;
        assert2::assert!((span - expected).abs() <= tolerance);
    }

    #[test]
    fn issue_user_cert_signed_by_ca_and_bare_cn() {
        let ca = generate_clients_ca("root", 365).expect("generate CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("issue leaf");

        let leaf_der = pem_to_der(&user.cert_pem);
        let (_, leaf) = X509Certificate::from_der(leaf_der.as_ref()).expect("parse leaf DER");
        assert2::assert!(leaf.subject().to_string() == "CN=alice");

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
        assert2::assert!(dn == "CN=alice");
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
        assert2::assert!(eku.value.client_auth);
    }

    #[test]
    fn each_generate_is_unique() {
        let a = generate_clients_ca("x", 365).expect("generate CA a");
        let b = generate_clients_ca("x", 365).expect("generate CA b");
        assert2::assert!(a.cert_pem != b.cert_pem);
        assert2::assert!(a.key_pem != b.key_pem);
    }

    #[test]
    fn generate_cluster_ca_carries_ou_cluster() {
        let ca = generate_cluster_ca("c1", 365).expect("generate cluster CA");
        let der = pem_to_der(&ca.cert_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse cluster CA DER");
        let subject = cert.subject().to_string();
        for part in ["CN=c1", "O=crabka", "OU=cluster"] {
            assert2::assert!(subject.contains(part));
        }
        let bc = cert
            .basic_constraints()
            .expect("BC parse")
            .expect("BC present");
        assert2::assert!(bc.value.ca);
    }

    #[test]
    fn clients_ca_does_not_carry_ou_cluster() {
        let ca = generate_clients_ca("root", 365).expect("generate clients CA");
        let der = pem_to_der(&ca.cert_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse");
        let subject = cert.subject().to_string();
        assert2::assert!(!subject.contains("OU=cluster"));
    }

    #[test]
    fn issue_broker_cert_has_server_and_client_auth_eku() {
        use std::net::Ipv4Addr;
        let ca = generate_cluster_ca("c1", 365).expect("CA");
        let sans = vec![
            SubjectAltName::Dns("c1-broker-0.c1-broker.default.svc.cluster.local".into()),
            SubjectAltName::Dns("c1-broker-0".into()),
            SubjectAltName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ];
        let b = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "c1-broker-0", &sans, &[], 365)
            .expect("issue broker cert");

        let der = pem_to_der(&b.cert_pem);
        let (_, leaf) = X509Certificate::from_der(der.as_ref()).expect("parse leaf");

        let eku = leaf
            .extended_key_usage()
            .expect("EKU parse")
            .expect("EKU present");
        assert2::assert!(eku.value.server_auth);
        assert2::assert!(eku.value.client_auth);

        let san_ext = leaf
            .subject_alternative_name()
            .expect("SAN parse")
            .expect("SAN present");
        let general_names: Vec<_> = san_ext.value.general_names.iter().collect();
        assert2::assert!(general_names.iter().any(|gn| matches!(
            gn,
            x509_parser::extensions::GeneralName::DNSName(s) if *s == "c1-broker-0"
        )));
        assert2::assert!(
            general_names
                .iter()
                .any(|gn| matches!(gn, x509_parser::extensions::GeneralName::IPAddress(_)))
        );
    }

    #[test]
    fn issue_broker_cert_deduplicates_extra_sans() {
        let ca = generate_cluster_ca("c1", 365).expect("CA");
        let base_sans = vec![SubjectAltName::Dns("c1-broker-0".into())];
        let extra_sans = vec![
            SubjectAltName::Dns("external.example".into()),
            SubjectAltName::Dns("c1-broker-0".into()),
            SubjectAltName::Dns("external.example".into()),
        ];

        let cert = issue_broker_cert(
            &ca.cert_pem,
            &ca.key_pem,
            "c1-broker-0",
            &base_sans,
            &extra_sans,
            365,
        )
        .expect("issue broker cert");

        let der = pem_to_der(&cert.cert_pem);
        let (_, leaf) = X509Certificate::from_der(der.as_ref()).expect("parse leaf");
        let san_ext = leaf
            .subject_alternative_name()
            .expect("SAN parse")
            .expect("SAN present");
        let dns_names: Vec<_> = san_ext
            .value
            .general_names
            .iter()
            .filter_map(|gn| match gn {
                x509_parser::extensions::GeneralName::DNSName(name) => Some(*name),
                _ => None,
            })
            .collect();

        assert2::assert!(dns_names == vec!["c1-broker-0", "external.example"]);
    }

    fn spki_der(cert_pem: &str) -> Vec<u8> {
        let der = pem_to_der(cert_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse");
        cert.public_key().raw.to_vec()
    }

    #[test]
    fn renew_cluster_ca_reuses_key_and_preserves_subject() {
        let orig = generate_cluster_ca("c1-cluster-ca", 30).expect("CA");
        let renewed_pem = renew_cluster_ca(&orig.key_pem, "c1-cluster-ca", 365).expect("renew");

        // Same public key (same SPKI) — the renewal reuses the key.
        assert2::assert!(spki_der(&orig.cert_pem) == spki_der(&renewed_pem));

        // Same subject DN, including OU=cluster.
        let der = pem_to_der(&renewed_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse renewed");
        let subject = cert.subject().to_string();
        for part in ["CN=c1-cluster-ca", "O=crabka", "OU=cluster"] {
            assert2::assert!(subject.contains(part));
        }
        assert2::assert!(
            cert.basic_constraints()
                .expect("BC")
                .expect("BC present")
                .value
                .ca
        );
    }

    #[test]
    fn renew_cluster_ca_extends_validity() {
        let orig = generate_cluster_ca("c1-cluster-ca", 30).expect("CA");
        let renewed_pem = renew_cluster_ca(&orig.key_pem, "c1-cluster-ca", 365).expect("renew");

        let span = |pem: &str| {
            let der = pem_to_der(pem);
            let (_, c) = X509Certificate::from_der(der.as_ref()).expect("parse");
            c.validity().not_after.timestamp() - c.validity().not_before.timestamp()
        };
        assert2::assert!(span(&renewed_pem) > span(&orig.cert_pem));
    }

    #[test]
    fn leaf_under_old_cert_verifies_against_renewed_cert() {
        // A leaf issued by the ORIGINAL cluster CA must verify against the
        // RENEWED cert's public key — this is what makes same-key renewal
        // non-disruptive (existing broker leafs keep chaining).
        let orig = generate_cluster_ca("c1-cluster-ca", 30).expect("CA");
        let sans = vec![SubjectAltName::Dns("c1-broker-0".into())];
        let leaf = issue_broker_cert(&orig.cert_pem, &orig.key_pem, "c1-broker-0", &sans, &[], 30)
            .expect("leaf");
        let renewed_pem = renew_cluster_ca(&orig.key_pem, "c1-cluster-ca", 365).expect("renew");

        let leaf_der = pem_to_der(&leaf.cert_pem);
        let (_, leaf_x509) = X509Certificate::from_der(leaf_der.as_ref()).expect("parse leaf");
        let renewed_der = pem_to_der(&renewed_pem);
        let (_, renewed_ca) =
            X509Certificate::from_der(renewed_der.as_ref()).expect("parse renewed");

        leaf_x509
            .verify_signature(Some(renewed_ca.public_key()))
            .expect("leaf must verify against the renewed CA public key");
    }

    #[test]
    fn renew_clients_ca_has_no_ou_cluster() {
        let orig = generate_clients_ca("c1-clients-ca", 30).expect("CA");
        let renewed_pem = renew_clients_ca(&orig.key_pem, "c1-clients-ca", 365).expect("renew");
        assert2::assert!(spki_der(&orig.cert_pem) == spki_der(&renewed_pem));
        let der = pem_to_der(&renewed_pem);
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("parse");
        assert2::assert!(!cert.subject().to_string().contains("OU=cluster"));
    }

    #[test]
    fn issue_broker_cert_chains_to_cluster_ca() {
        let ca = generate_cluster_ca("c1", 365).expect("CA");
        let sans = vec![SubjectAltName::Dns("c1-broker-0".into())];
        let b = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "c1-broker-0", &sans, &[], 365)
            .expect("leaf");

        let leaf_der = pem_to_der(&b.cert_pem);
        let (_, leaf) = X509Certificate::from_der(leaf_der.as_ref()).expect("parse leaf");
        let ca_der = pem_to_der(&ca.cert_pem);
        let (_, ca_x509) = X509Certificate::from_der(ca_der.as_ref()).expect("parse CA");

        leaf.verify_signature(Some(ca_x509.public_key()))
            .expect("leaf signature must verify against cluster CA pubkey");
    }
}
