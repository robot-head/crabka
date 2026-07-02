//! Regression: when an operator-rendered broker config sets
//! `[oauthbearer] introspection_endpoint_uri`, `FileConfig::apply_to`
//! builds the introspection client BEFORE `Broker::start` has had a
//! chance to install the rustls process-level `CryptoProvider`. The
//! `reqwest::Client::builder().build()` path inside
//! `ReqwestIntrospectionClient::new` reaches into
//! `rustls::ClientConfig::builder` → `CryptoProvider::get_default`,
//! which panics with "Could not automatically determine the
//! process-level `CryptoProvider` from Rustls crate features" when no
//! provider is installed. Each integration-test file is its own
//! process, so this file deliberately does not install the provider
//! anywhere else — if the eager install inside the client constructor
//! is ever removed, this test panics on startup just like the broker
//! binary did in the kind-oauth-introspection e2e job.

use assert2::assert;
use crabka_broker::{config::BrokerConfig, file_config::FileConfig};

/// A self-contained CA cert. Avoids depending on the security crate's
/// fixtures (which live behind `#[cfg(test)]`-only paths).
const DEV_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIB4zCCAYmgAwIBAgIUcSDwFlx+8XhU+aAAtS17F6TnHQgwCgYIKoZIzj0EAwIw\n\
FTETMBEGA1UEAwwKY3JhYmthLWRldjAgFw0yNjA1MTUwNTA2MzNaGA8yMTI2MDQy\n\
MTA1MDYzM1owFTETMBEGA1UEAwwKY3JhYmthLWRldjBZMBMGByqGSM49AgEGCCqG\n\
SM49AwEHA0IABIzQW9UMYH1u0MSki3EBCJ4qIrjV67hAJv79lFJCGZCbl+pwVhYS\n\
wLP4u7jqwoM8qFg68ZBGFVrcGulQs3UYTCujgbQwgbEwHQYDVR0OBBYEFGZZjSyx\n\
/Lc2SUwBiFQ/VcKLRSTJMB8GA1UdIwQYMBaAFGZZjSyx/Lc2SUwBiFQ/VcKLRSTJ\n\
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUF\n\
BwMBMDwGA1UdEQQ1MDOCCmNyYWJrYS1kZXaCFGhvc3QuZG9ja2VyLmludGVybmFs\n\
gglsb2NhbGhvc3SHBH8AAAEwCgYIKoZIzj0EAwIDSAAwRQIgcO9PwkwNCPt589FM\n\
OnP9WAj0vBqSLWmYcm6N6hNz8KcCIQDDXGEYKtNkfUzKrICS2v40ybJqzyZ9cbaR\n\
Shzd0RKUbQ==\n\
-----END CERTIFICATE-----\n";

#[test]
fn introspection_validator_builds_without_an_externally_installed_crypto_provider() {
    let dir = tempfile::tempdir().expect("tempdir");
    let secret_path = dir.path().join("client-secret");
    std::fs::write(&secret_path, "the-secret").expect("write secret");
    // The kind-oauth-introspection Kafka CR sets `tlsTrustedCertificates`
    // on the listener, which the operator renders to an `idp_tls_trust`
    // PEM path in the broker TOML. That path drives
    // `ReqwestIntrospectionClient::new` through `build_client_config_from_pem`
    // → `rustls::ClientConfig::builder()`, which is the actual panic
    // site without the eager `CryptoProvider` install. The PEM only
    // needs to parse — the resulting rustls config never sees the
    // network in this test.
    let ca_path = dir.path().join("idp-ca.pem");
    std::fs::write(&ca_path, DEV_CERT_PEM).expect("write idp-ca");

    let toml = format!(
        r#"
[oauthbearer]
introspection_endpoint_uri       = "https://idp.example/introspect"
introspection_client_id          = "kafka-broker"
introspection_client_secret_path = '{secret}'
idp_tls_trust                    = '{ca}'
"#,
        secret = secret_path.display(),
        ca = ca_path.display(),
    );
    let file: FileConfig = toml::from_str(&toml).expect("parse FileConfig");
    let mut cfg = BrokerConfig::default();
    // Pre-fix, this `apply_to` panicked inside
    // `rustls::ClientConfig::builder()` (called from
    // `build_client_config_from_pem` when `idp_tls_trust` is set)
    // because no `CryptoProvider` was installed yet. Post-fix,
    // `ReqwestIntrospectionClient::new` installs it idempotently and
    // `apply_to` returns Ok.
    file.apply_to(&mut cfg).expect("apply_to should not panic");
    assert!(matches!(
        cfg.oauthbearer_validator,
        crabka_security::OAuthBearerValidator::Introspection(_)
    ));
}
