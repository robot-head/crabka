//! Runtime configuration for one admin UI instance.

use std::net::SocketAddr;
use std::path::PathBuf;

use crabka_client_core::security::TlsConnectorConfig;
use crabka_security::ListenerProtocol;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerSecurityConfig {
    SaslPlaintext,
    SaslSsl {
        trust_roots_pem: Option<PathBuf>,
        server_name: String,
        client_identity: Option<(PathBuf, PathBuf)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUiConfig {
    pub listen_addr: SocketAddr,
    pub cluster_name: String,
    pub bootstrap_addrs: Vec<String>,
    pub security: BrokerSecurityConfig,
    pub session_ttl_seconds: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("at least one CRABKA_ADMIN_UI_BOOTSTRAP address is required")]
    MissingBootstrap,
    #[error("CRABKA_ADMIN_UI_LISTEN_ADDR is invalid: {0}")]
    InvalidListenAddr(String),
    #[error("CRABKA_ADMIN_UI_SECURITY_PROTOCOL must be SASL_PLAINTEXT or SASL_SSL")]
    InvalidSecurityProtocol,
    #[error("CRABKA_ADMIN_UI_TLS_SERVER_NAME is required for SASL_SSL")]
    MissingTlsServerName,
}

impl Default for AdminUiConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8088".parse().expect("static socket addr parses"),
            cluster_name: "local".to_string(),
            bootstrap_addrs: Vec::new(),
            security: BrokerSecurityConfig::SaslPlaintext,
            session_ttl_seconds: 8 * 60 * 60,
        }
    }
}

impl AdminUiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut cfg = Self::default();

        if let Ok(raw) = std::env::var("CRABKA_ADMIN_UI_LISTEN_ADDR") {
            cfg.listen_addr = raw
                .parse()
                .map_err(|_| ConfigError::InvalidListenAddr(raw.clone()))?;
        }
        if let Ok(name) = std::env::var("CRABKA_ADMIN_UI_CLUSTER_NAME") {
            cfg.cluster_name = name;
        }
        if let Ok(addrs) = std::env::var("CRABKA_ADMIN_UI_BOOTSTRAP") {
            cfg.bootstrap_addrs = addrs
                .split(',')
                .map(str::trim)
                .filter(|addr| !addr.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Ok(protocol) = std::env::var("CRABKA_ADMIN_UI_SECURITY_PROTOCOL") {
            cfg.security = match protocol.as_str() {
                "SASL_PLAINTEXT" => BrokerSecurityConfig::SaslPlaintext,
                "SASL_SSL" => BrokerSecurityConfig::SaslSsl {
                    trust_roots_pem: std::env::var_os("CRABKA_ADMIN_UI_TLS_TRUST_ROOTS_PEM")
                        .map(PathBuf::from),
                    server_name: std::env::var("CRABKA_ADMIN_UI_TLS_SERVER_NAME")
                        .map_err(|_| ConfigError::MissingTlsServerName)?,
                    client_identity: match (
                        std::env::var_os("CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM"),
                        std::env::var_os("CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM"),
                    ) {
                        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
                        _ => None,
                    },
                },
                _ => return Err(ConfigError::InvalidSecurityProtocol),
            };
        }

        cfg.validate()
    }

    pub fn validate(self) -> Result<Self, ConfigError> {
        if self.bootstrap_addrs.is_empty() {
            return Err(ConfigError::MissingBootstrap);
        }
        if let BrokerSecurityConfig::SaslSsl { server_name, .. } = &self.security
            && server_name.trim().is_empty()
        {
            return Err(ConfigError::MissingTlsServerName);
        }

        Ok(self)
    }
}

impl BrokerSecurityConfig {
    #[must_use]
    pub fn listener_protocol(&self) -> ListenerProtocol {
        match self {
            Self::SaslPlaintext => ListenerProtocol::SaslPlaintext,
            Self::SaslSsl { .. } => ListenerProtocol::SaslSsl,
        }
    }

    #[must_use]
    pub fn tls(&self) -> Option<TlsConnectorConfig> {
        match self {
            Self::SaslPlaintext => None,
            Self::SaslSsl {
                trust_roots_pem,
                server_name,
                client_identity,
            } => Some(TlsConnectorConfig {
                trust_roots_pem: trust_roots_pem.clone(),
                server_name: server_name.clone(),
                client_identity: client_identity.clone(),
            }),
        }
    }
}
