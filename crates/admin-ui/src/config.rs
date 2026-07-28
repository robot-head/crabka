//! Runtime configuration for one admin UI instance.

use std::{
    fmt,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Parser;
use crabka_client_core::security::TlsConnectorConfig;
use crabka_security::ListenerProtocol;
use refined_type::rule::{GreaterI32, GreaterU64, GreaterUsize};
use thiserror::Error;

/// Default maximum size of an authenticated mutation JSON body.
pub const DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES: usize = 1_048_576;

/// A positive maximum size for an authenticated mutation JSON body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationJsonBodyLimitBytes(usize);

impl MutationJsonBodyLimitBytes {
    /// Validate a mutation JSON body limit.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated byte limit.
    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

impl Default for MutationJsonBodyLimitBytes {
    fn default() -> Self {
        Self::new(DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES)
            .expect("default mutation JSON body limit is positive")
    }
}

impl fmt::Display for MutationJsonBodyLimitBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for MutationJsonBodyLimitBytes {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Default server-side lifetime for an authenticated admin UI session.
pub const DEFAULT_SESSION_TTL_SECONDS: u64 = 28_800;

/// A positive session lifetime representable by the platform monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTtlSeconds(u64);

impl SessionTtlSeconds {
    /// Validate an admin UI session lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or cannot be added to the
    /// platform monotonic clock.
    pub fn new(value: u64) -> Result<Self, String> {
        let value = GreaterU64::<0>::new(value)
            .map(refined_type::Refined::into_value)
            .map_err(|error| error.to_string())?;

        if Instant::now()
            .checked_add(Duration::from_secs(value))
            .is_none()
        {
            return Err("session TTL exceeds the platform monotonic clock".to_string());
        }

        Ok(Self(value))
    }

    /// Return the validated session lifetime.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}

impl Default for SessionTtlSeconds {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_TTL_SECONDS)
            .expect("default session TTL is positive and representable")
    }
}

impl fmt::Display for SessionTtlSeconds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SessionTtlSeconds {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Default Kafka request timeout for admin UI topic mutations.
pub const DEFAULT_TOPIC_MUTATION_TIMEOUT_MS: i32 = 30_000;

/// A positive Kafka request timeout for admin UI topic mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicMutationTimeoutMs(i32);

impl TopicMutationTimeoutMs {
    /// Validate a topic-mutation request timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not positive.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated timeout in milliseconds.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl Default for TopicMutationTimeoutMs {
    fn default() -> Self {
        Self::new(DEFAULT_TOPIC_MUTATION_TIMEOUT_MS)
            .expect("default topic-mutation timeout is positive")
    }
}

impl fmt::Display for TopicMutationTimeoutMs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for TopicMutationTimeoutMs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Command-line and environment inputs owned by the admin UI runtime.
#[derive(Debug, Clone, Parser)]
#[command(name = "crabka-admin-ui")]
pub struct AdminUiRuntimeArgs {
    /// Maximum authenticated mutation JSON body size in bytes.
    #[arg(
        long,
        env = "CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES",
        default_value_t = MutationJsonBodyLimitBytes::default()
    )]
    pub mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes,

    /// Server-side lifetime for an authenticated session, in seconds.
    #[arg(
        long = "session-ttl-seconds",
        env = "CRABKA_ADMIN_UI_SESSION_TTL_SECONDS",
        default_value_t = SessionTtlSeconds::default()
    )]
    pub session_ttl: SessionTtlSeconds,

    /// Kafka request timeout for topic mutations, in milliseconds.
    #[arg(
        long = "topic-mutation-timeout-ms",
        env = "CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS",
        default_value_t = TopicMutationTimeoutMs::default()
    )]
    pub topic_mutation_timeout_ms: TopicMutationTimeoutMs,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUiConfig {
    pub listen_addr: SocketAddr,
    pub cluster_name: String,
    pub bootstrap_addrs: Vec<String>,
    pub security: BrokerSecurityConfig,
    pub session_ttl: SessionTtlSeconds,
    pub mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes,
    pub topic_mutation_timeout_ms: TopicMutationTimeoutMs,
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
            session_ttl: SessionTtlSeconds::default(),
            mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes::default(),
            topic_mutation_timeout_ms: TopicMutationTimeoutMs::default(),
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
