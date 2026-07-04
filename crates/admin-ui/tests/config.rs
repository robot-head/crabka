use std::net::SocketAddr;
use std::process::Command;

use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig, ConfigError};

#[test]
fn default_config_targets_local_server_and_requires_bootstrap() {
    let cfg = AdminUiConfig::default();

    assert_eq!(
        cfg.listen_addr,
        "127.0.0.1:8088".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(cfg.cluster_name, "local");
    assert!(cfg.bootstrap_addrs.is_empty());

    let error = cfg.validate().expect_err("empty bootstrap is invalid");
    assert!(matches!(error, ConfigError::MissingBootstrap));
}

#[test]
fn validates_single_cluster_sasl_plaintext_config() {
    let cfg = AdminUiConfig {
        cluster_name: "dev".to_string(),
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };

    let validated = cfg.validate().expect("config is valid");
    assert_eq!(validated.cluster_name, "dev");
    assert_eq!(validated.bootstrap_addrs, ["127.0.0.1:9092"]);
}

#[test]
fn from_env_rejects_empty_tls_server_name_for_sasl_ssl() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("empty_tls_server_name_from_env_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_CONFIG_EMPTY_TLS_CHILD", "1")
        .env("CRABKA_ADMIN_UI_BOOTSTRAP", "127.0.0.1:9092")
        .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_SSL")
        .env("CRABKA_ADMIN_UI_TLS_SERVER_NAME", "")
        .env_remove("CRABKA_ADMIN_UI_LISTEN_ADDR")
        .output()
        .expect("child test process runs");

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn empty_tls_server_name_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_EMPTY_TLS_CHILD").is_none() {
        return;
    }

    let error = AdminUiConfig::from_env().expect_err("empty TLS server name is invalid");
    assert!(matches!(error, ConfigError::MissingTlsServerName));
}

#[test]
fn from_env_rejects_whitespace_tls_server_name_for_sasl_ssl() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("whitespace_tls_server_name_from_env_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_CONFIG_WHITESPACE_TLS_CHILD", "1")
        .env("CRABKA_ADMIN_UI_BOOTSTRAP", "127.0.0.1:9092")
        .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_SSL")
        .env("CRABKA_ADMIN_UI_TLS_SERVER_NAME", " \t ")
        .env_remove("CRABKA_ADMIN_UI_LISTEN_ADDR")
        .output()
        .expect("child test process runs");

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn whitespace_tls_server_name_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_WHITESPACE_TLS_CHILD").is_none() {
        return;
    }

    let error = AdminUiConfig::from_env().expect_err("blank TLS server name is invalid");
    assert!(matches!(error, ConfigError::MissingTlsServerName));
}

#[test]
fn validate_rejects_manual_sasl_ssl_empty_server_name() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslSsl {
            trust_roots_pem: None,
            server_name: String::new(),
            client_identity: None,
        },
        ..AdminUiConfig::default()
    };

    let error = cfg
        .validate()
        .expect_err("empty TLS server name is invalid");
    assert!(matches!(error, ConfigError::MissingTlsServerName));
}

#[test]
fn validate_rejects_manual_sasl_ssl_whitespace_server_name() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslSsl {
            trust_roots_pem: None,
            server_name: " \t ".to_string(),
            client_identity: None,
        },
        ..AdminUiConfig::default()
    };

    let error = cfg
        .validate()
        .expect_err("blank TLS server name is invalid");
    assert!(matches!(error, ConfigError::MissingTlsServerName));
}
