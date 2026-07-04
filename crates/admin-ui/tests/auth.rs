use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use crabka_admin_ui::auth::{
    AuthService, LoginBroker, LoginRequest, LoginSuccess, build_scram_sha512_security,
};
use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_admin_ui::error::UiError;
use crabka_admin_ui::session::{SessionId, SessionStore};
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

#[tokio::test]
async fn login_uses_broker_probe_and_creates_session() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };
    let sessions = SessionStore::new(Duration::from_mins(1));
    let broker = RecordingLoginBroker::default();
    let service = AuthService::new_with_broker(&cfg, &sessions, &broker);

    let success = service
        .login(LoginRequest {
            username: "alice".to_string(),
            password: "secret".to_string(),
        })
        .await
        .expect("broker probe succeeds");

    assert_eq!(success.username, "alice");
    assert_eq!(success.principal, "User:alice");
    assert_eq!(
        broker
            .calls
            .lock()
            .expect("calls lock is not poisoned")
            .as_slice(),
        &[(
            vec!["127.0.0.1:9092".to_string()],
            "alice".to_string(),
            "secret".to_string(),
        )]
    );

    let session_id = SessionId::try_from(success.session_id.as_str()).expect("session id is valid");
    let session = sessions.get(&session_id).expect("login creates session");
    assert_eq!(session.user.username, "alice");
    assert_eq!(session.user.principal, "User:alice");
}

#[derive(Default)]
struct RecordingLoginBroker {
    calls: Mutex<Vec<(Vec<String>, String, String)>>,
}

impl LoginBroker for RecordingLoginBroker {
    fn check_login<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("calls lock is not poisoned")
                .push((
                    cfg.bootstrap_addrs.clone(),
                    username.to_string(),
                    password.to_string(),
                ));
            Ok(())
        })
    }
}
