use std::path::PathBuf;

use crabka_admin_ui::auth::{LoginSuccess, build_scram_sha512_security};
use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_client_core::security::SaslCredentials;
use crabka_security::{ListenerProtocol, SaslMechanism};

#[test]
fn build_security_uses_scram_sha512_only() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };

    let security = build_scram_sha512_security(&cfg, "alice", "secret");

    assert_eq!(security.protocol, ListenerProtocol::SaslPlaintext);
    assert!(security.tls.is_none());
    assert!(security.sasl_host.is_none());
    assert!(matches!(
        security.sasl,
        Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            ref username,
            ref password,
        }) if username == "alice" && password == "secret"
    ));
}

#[test]
fn build_security_preserves_sasl_ssl_tls_material() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9093".to_string()],
        security: BrokerSecurityConfig::SaslSsl {
            trust_roots_pem: Some(PathBuf::from("ca.pem")),
            server_name: "broker.example.test".to_string(),
            client_identity: Some((PathBuf::from("client.crt"), PathBuf::from("client.key"))),
        },
        ..AdminUiConfig::default()
    };

    let security = build_scram_sha512_security(&cfg, "carol", "top-secret");
    let tls = security.tls.expect("SASL_SSL carries TLS config");

    assert_eq!(security.protocol, ListenerProtocol::SaslSsl);
    assert_eq!(tls.trust_roots_pem, Some(PathBuf::from("ca.pem")));
    assert_eq!(tls.server_name, "broker.example.test");
    assert_eq!(
        tls.client_identity,
        Some((PathBuf::from("client.crt"), PathBuf::from("client.key")))
    );
    assert!(security.sasl_host.is_none());
    assert!(matches!(
        security.sasl,
        Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            ref username,
            ref password,
        }) if username == "carol" && password == "top-secret"
    ));
}

#[test]
fn login_success_debug_redacts_session_id() {
    let success = LoginSuccess {
        username: "alice".to_string(),
        principal: "User:alice".to_string(),
        session_id: "raw-session-cookie-value".to_string(),
    };

    let debug = format!("{success:?}");

    assert!(debug.contains("alice"));
    assert!(debug.contains("User:alice"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("raw-session-cookie-value"));
}
