//! mTLS principal extraction. Given a DER-encoded X.509 client
//! certificate, derive the principal name the broker uses for ACL
//! lookups.
//!
//! Kafka's `DefaultKafkaPrincipalBuilder` uses the cert's Subject
//! Distinguished Name (RFC 2253 format, e.g. `CN=alice,OU=foo,O=org`)
//! as the principal name. Operators and `KafkaUser` mTLS provisioning
//! pin ACLs by that DN, so we match it byte-for-byte.

use x509_parser::prelude::FromDer;
use x509_parser::prelude::X509Certificate;

/// Parse `cert_der` and return the Subject DN in RFC 2253 format.
/// Returns `None` if the bytes don't parse as a valid X.509 cert.
#[must_use]
pub fn extract_principal_from_cert(cert_der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    Some(cert.subject().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    #[test]
    fn extracts_subject_dn_from_fixture_client_cert() {
        let pem = include_bytes!("../tests/fixtures/dev_client_cert.pem");
        let mut iter = CertificateDer::pem_slice_iter(pem);
        let cert: CertificateDer<'static> = iter.next().expect("at least one cert").unwrap();
        let dn = extract_principal_from_cert(cert.as_ref()).expect("parse fixture cert");
        // openssl-rendered subject is CN=test-client,OU=integration,O=crabka.
        // x509-parser preserves that order (most-significant first),
        // comma-separated, no spaces. Operators pin this DN verbatim in
        // ACLs and super_users.
        assert!(dn == "CN=test-client,OU=integration,O=crabka");
    }

    #[test]
    fn malformed_cert_returns_none() {
        assert!(extract_principal_from_cert(b"not-a-cert").is_none());
        assert!(extract_principal_from_cert(&[]).is_none());
    }
}
