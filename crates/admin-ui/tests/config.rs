use std::{net::SocketAddr, path::PathBuf, process::Command};

use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig, ConfigError};

#[test]
fn default_config_targets_local_server_and_requires_bootstrap() {
    let cfg = AdminUiConfig::default();

    assert_eq!(
        cfg,
        AdminUiConfig {
            listen_addr: "127.0.0.1:8088".parse::<SocketAddr>().unwrap(),
            cluster_name: "local".to_string(),
            bootstrap_addrs: Vec::new(),
            security: BrokerSecurityConfig::SaslPlaintext,
            session_ttl_seconds: 28_800,
        }
    );

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

    let expected = cfg.clone();
    let validated = cfg.validate().expect("config is valid");
    assert_eq!(validated, expected);
}

#[test]
fn from_env_parses_sasl_ssl_tls_config() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("sasl_ssl_tls_config_from_env_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_CONFIG_SASL_SSL_CHILD", "1")
        .env("CRABKA_ADMIN_UI_LISTEN_ADDR", "127.0.0.1:18088")
        .env("CRABKA_ADMIN_UI_CLUSTER_NAME", "staging")
        .env("CRABKA_ADMIN_UI_BOOTSTRAP", "broker-1:9092, 127.0.0.1:9093")
        .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_SSL")
        .env("CRABKA_ADMIN_UI_TLS_TRUST_ROOTS_PEM", "ca.pem")
        .env("CRABKA_ADMIN_UI_TLS_SERVER_NAME", "broker.example.test")
        .env("CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM", "client.crt")
        .env("CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM", "client.key")
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
fn sasl_ssl_tls_config_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_SASL_SSL_CHILD").is_none() {
        return;
    }

    let cfg = AdminUiConfig::from_env().expect("env config is valid");
    assert_eq!(
        cfg,
        AdminUiConfig {
            listen_addr: "127.0.0.1:18088".parse::<SocketAddr>().unwrap(),
            cluster_name: "staging".to_string(),
            bootstrap_addrs: vec!["broker-1:9092".to_string(), "127.0.0.1:9093".to_string()],
            security: BrokerSecurityConfig::SaslSsl {
                trust_roots_pem: Some(PathBuf::from("ca.pem")),
                server_name: "broker.example.test".to_string(),
                client_identity: Some((PathBuf::from("client.crt"), PathBuf::from("client.key"))),
            },
            session_ttl_seconds: 28_800,
        }
    );
}

#[test]
fn from_env_rejects_blank_bootstrap_entry() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("blank_bootstrap_entry_from_env_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_CONFIG_BLANK_BOOTSTRAP_CHILD", "1")
        .env("CRABKA_ADMIN_UI_BOOTSTRAP", "broker-1:9092,,broker-2:9093")
        .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
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
fn blank_bootstrap_entry_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_BLANK_BOOTSTRAP_CHILD").is_none() {
        return;
    }

    let error = AdminUiConfig::from_env().expect_err("blank bootstrap entry is invalid");
    assert!(matches!(error, ConfigError::InvalidBootstrapAddr(addr) if addr.is_empty()));
}

#[test]
fn validate_rejects_malformed_bootstrap_entries() {
    for invalid_addr in [
        "",
        ":9092",
        "broker-1",
        "broker-1:",
        "broker-1:notaport",
        "broker-1:70000",
    ] {
        let cfg = AdminUiConfig {
            bootstrap_addrs: vec![invalid_addr.to_string()],
            ..AdminUiConfig::default()
        };

        let error = cfg
            .validate()
            .expect_err("malformed bootstrap entry is invalid");
        assert!(
            matches!(&error, ConfigError::InvalidBootstrapAddr(addr) if addr == invalid_addr),
            "expected invalid bootstrap error for {invalid_addr:?}, got {error:?}"
        );
    }
}

#[test]
fn validates_multi_bootstrap_hostnames_and_ip_literals() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["broker-1:9092".to_string(), "127.0.0.1:9093".to_string()],
        ..AdminUiConfig::default()
    };

    let validated = cfg.validate().expect("multi-bootstrap config is valid");
    assert_eq!(
        validated.bootstrap_addrs,
        ["broker-1:9092", "127.0.0.1:9093"]
    );
}

#[test]
fn from_env_rejects_blank_tls_server_name_cases() {
    for (name, server_name) in [("empty", ""), ("whitespace", " \t ")] {
        let output = Command::new(std::env::current_exe().expect("test binary path is available"))
            .arg("--exact")
            .arg("blank_tls_server_name_from_env_child")
            .arg("--nocapture")
            .env("CRABKA_ADMIN_UI_CONFIG_BLANK_TLS_CHILD", "1")
            .env("CRABKA_ADMIN_UI_BOOTSTRAP", "127.0.0.1:9092")
            .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_SSL")
            .env("CRABKA_ADMIN_UI_TLS_SERVER_NAME", server_name)
            .env_remove("CRABKA_ADMIN_UI_LISTEN_ADDR")
            .output()
            .expect("child test process runs");
        assert!(
            output.status.success(),
            "case {name}: child test failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn blank_tls_server_name_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_BLANK_TLS_CHILD").is_none() {
        return;
    }

    let error = AdminUiConfig::from_env().expect_err("blank TLS server name is invalid");
    assert!(matches!(error, ConfigError::MissingTlsServerName));
}

#[test]
fn from_env_rejects_incomplete_tls_client_identity() {
    for (name, cert, key) in [
        ("certificate without key", Some("client.crt"), None),
        ("key without certificate", None, Some("client.key")),
    ] {
        let mut command =
            Command::new(std::env::current_exe().expect("test binary path is available"));
        command
            .arg("--exact")
            .arg("incomplete_tls_client_identity_from_env_child")
            .arg("--nocapture")
            .env("CRABKA_ADMIN_UI_CONFIG_INCOMPLETE_TLS_IDENTITY_CHILD", "1")
            .env("CRABKA_ADMIN_UI_BOOTSTRAP", "127.0.0.1:9092")
            .env("CRABKA_ADMIN_UI_SECURITY_PROTOCOL", "SASL_SSL")
            .env("CRABKA_ADMIN_UI_TLS_SERVER_NAME", "localhost")
            .env_remove("CRABKA_ADMIN_UI_LISTEN_ADDR");
        match cert {
            Some(cert) => command.env("CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM", cert),
            None => command.env_remove("CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM"),
        };
        match key {
            Some(key) => command.env("CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM", key),
            None => command.env_remove("CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM"),
        };

        let output = command.output().expect("child test process runs");
        assert!(
            output.status.success(),
            "case {name}: child test failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn incomplete_tls_client_identity_from_env_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_CONFIG_INCOMPLETE_TLS_IDENTITY_CHILD").is_none() {
        return;
    }

    let error = AdminUiConfig::from_env().expect_err("incomplete TLS identity is invalid");
    assert!(matches!(error, ConfigError::IncompleteTlsClientIdentity));
}

#[test]
fn validate_rejects_manual_sasl_ssl_blank_server_names() {
    for (name, server_name) in [("empty", ""), ("whitespace", " \t ")] {
        let cfg = AdminUiConfig {
            bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
            security: BrokerSecurityConfig::SaslSsl {
                trust_roots_pem: None,
                server_name: server_name.to_string(),
                client_identity: None,
            },
            ..AdminUiConfig::default()
        };

        let error = cfg
            .validate()
            .expect_err("blank TLS server name is invalid");
        assert_eq!(error, ConfigError::MissingTlsServerName, "case {name}");
    }
}
