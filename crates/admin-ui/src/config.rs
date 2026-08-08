//! Runtime configuration for one admin UI instance.

use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::Parser;
use crabka_client_core::security::TlsConnectorConfig;
use crabka_security::ListenerProtocol;
use crabka_units::{parse, prelude::*};
use thiserror::Error;

/// Default maximum size of an authenticated mutation JSON body.
pub const DEFAULT_MUTATION_JSON_BODY_LIMIT: ByteSize = mebibytes(1);

/// Default server-side lifetime for an authenticated admin UI session.
pub const DEFAULT_SESSION_TTL: Time = hours(8);

/// Default Kafka request timeout for admin UI topic mutations.
pub const DEFAULT_TOPIC_MUTATION_TIMEOUT: Time = secs(30);

fn parse_body_limit(input: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(input).map_err(|error| error.to_string())?;
    let lowered = value.bytes_usize();
    if u64::try_from(lowered).is_ok_and(|bytes| ByteSize::from_bytes(bytes) == value) {
        Ok(value)
    } else {
        Err("body limit must be a whole byte count representable by usize".to_string())
    }
}

fn parse_session_ttl(input: &str) -> Result<Time, String> {
    let value = parse::positive_time(input).map_err(|error| error.to_string())?;
    let duration =
        Duration::try_from_secs_f64(value.secs_f64()).map_err(|error| error.to_string())?;
    Instant::now()
        .checked_add(duration)
        .map(|_| value)
        .ok_or_else(|| "session TTL exceeds the platform monotonic clock".to_string())
}

fn parse_topic_mutation_timeout(input: &str) -> Result<Time, String> {
    let value = parse::positive_time(input).map_err(|error| error.to_string())?;
    let millis = value.secs_f64() * 1_000.0;
    if millis.fract() == 0.0 && millis <= f64::from(i32::MAX) {
        Ok(value)
    } else {
        Err("topic mutation timeout must be a whole i32 millisecond count".to_string())
    }
}

/// Command-line and environment inputs owned by the admin UI runtime.
#[derive(Debug, Clone, Parser)]
#[command(name = "crabka-admin-ui")]
pub struct AdminUiRuntimeArgs {
    /// Maximum size of an authenticated mutation JSON body.
    #[arg(
        long,
        env = "CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT",
        default_value = "1MiB",
        value_parser = parse_body_limit
    )]
    pub mutation_json_body_limit: ByteSize,

    /// Server-side lifetime for an authenticated session.
    #[arg(
        long,
        env = "CRABKA_ADMIN_UI_SESSION_TTL",
        default_value = "8h",
        value_parser = parse_session_ttl
    )]
    pub session_ttl: Time,

    /// Kafka request timeout for topic mutations.
    #[arg(
        long,
        env = "CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT",
        default_value = "30s",
        value_parser = parse_topic_mutation_timeout
    )]
    pub topic_mutation_timeout: Time,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerSecurityConfig {
    SaslPlaintext,
    SaslSsl {
        trust_roots_pem: Option<PathBuf>,
        server_name: String,
        client_identity: Option<(PathBuf, PathBuf)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdminUiConfig {
    pub listen_addr: SocketAddr,
    pub cluster_name: String,
    pub bootstrap_addrs: Vec<String>,
    pub security: BrokerSecurityConfig,
    pub session_ttl: Time,
    pub mutation_json_body_limit: ByteSize,
    pub topic_mutation_timeout: Time,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("at least one CRABKA_ADMIN_UI_BOOTSTRAP address is required")]
    MissingBootstrap,
    #[error("CRABKA_ADMIN_UI_BOOTSTRAP contains an invalid address: {0}")]
    InvalidBootstrapAddr(String),
    #[error("CRABKA_ADMIN_UI_LISTEN_ADDR is invalid: {0}")]
    InvalidListenAddr(String),
    #[error("CRABKA_ADMIN_UI_SECURITY_PROTOCOL must be SASL_PLAINTEXT or SASL_SSL")]
    InvalidSecurityProtocol,
    #[error("CRABKA_ADMIN_UI_TLS_SERVER_NAME is required for SASL_SSL")]
    MissingTlsServerName,
    #[error(
        "CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM and CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM must be set together"
    )]
    IncompleteTlsClientIdentity,
}

impl Default for AdminUiConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8088".parse().expect("static socket addr parses"),
            cluster_name: "local".to_string(),
            bootstrap_addrs: Vec::new(),
            security: BrokerSecurityConfig::SaslPlaintext,
            session_ttl: DEFAULT_SESSION_TTL,
            mutation_json_body_limit: DEFAULT_MUTATION_JSON_BODY_LIMIT,
            topic_mutation_timeout: DEFAULT_TOPIC_MUTATION_TIMEOUT,
        }
    }
}

impl AdminUiConfig {
    /// # Errors
    /// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
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
                        (None, None) => None,
                        _ => return Err(ConfigError::IncompleteTlsClientIdentity),
                    },
                },
                _ => return Err(ConfigError::InvalidSecurityProtocol),
            };
        }

        cfg.validate()
    }

    /// # Errors
    /// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
    pub fn validate(self) -> Result<Self, ConfigError> {
        if self.bootstrap_addrs.is_empty() {
            return Err(ConfigError::MissingBootstrap);
        }
        for bootstrap_addr in &self.bootstrap_addrs {
            validate_bootstrap_addr(bootstrap_addr)?;
        }
        if let BrokerSecurityConfig::SaslSsl { server_name, .. } = &self.security
            && server_name.trim().is_empty()
        {
            return Err(ConfigError::MissingTlsServerName);
        }

        Ok(self)
    }
}

fn validate_bootstrap_addr(addr: &str) -> Result<(), ConfigError> {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return Err(ConfigError::InvalidBootstrapAddr(addr.to_string()));
    };
    if host.is_empty() || port.is_empty() {
        return Err(ConfigError::InvalidBootstrapAddr(addr.to_string()));
    }
    if port.parse::<u16>().is_err() {
        return Err(ConfigError::InvalidBootstrapAddr(addr.to_string()));
    }

    Ok(())
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
