use std::net::SocketAddr;

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
