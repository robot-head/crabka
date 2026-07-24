//! Runtime configuration for the registry service.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

/// Resolved configuration for a running registry node.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// `host:port[,host:port...]` bootstrap addresses for the Crabka broker.
    pub bootstrap: String,
    /// Name of the backing compacted topic. Confluent default: `_schemas`.
    pub schemas_topic: String,
    /// Replication factor for `_schemas` when auto-created.
    pub schemas_topic_rf: i32,
    /// Client id used for the producer/reader connections.
    pub client_id: String,
    /// This node's externally reachable REST URL, advertised to peers for
    /// write-forwarding, e.g. `http://10.0.0.5:8081`.
    pub advertised_url: String,
    /// The primary-election group id (Confluent default: `schema-registry`).
    pub group_id: String,
    /// Whether this node may be elected primary.
    pub leader_eligibility: bool,
    /// Service-owned runtime policy.
    pub runtime: RegistryRuntimeConfig,
    /// Authentication / authorization / TLS / SR-to-broker client security.
    /// The [`Default`] is fully permissive (open HTTP, anonymous, plaintext
    /// broker client) — every field opts in independently.
    pub security: SecurityConfig,
}

/// Runtime policy for Schema Registry's broker interactions and defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRuntimeConfig {
    pub election_session_timeout_ms: i32,
    pub election_rebalance_timeout_ms: i32,
    pub election_heartbeat_interval_ms: u64,
    pub election_reconnect_backoff_ms: u64,
    pub store_reader_retry_backoff_ms: u64,
    pub store_reader_fetch_max_wait_ms: i32,
    pub store_reader_fetch_max_bytes: i32,
    pub schemas_topic_create_timeout_ms: i32,
    pub default_compatibility_level: String,
    pub default_mode: String,
}

impl RegistryRuntimeConfig {
    /// Validate relationships and string-valued runtime policy.
    ///
    /// # Errors
    ///
    /// Returns an error when timeouts conflict or a configured default is invalid.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.election_heartbeat_interval_ms
            >= u64::try_from(self.election_session_timeout_ms).unwrap_or(0)
        {
            anyhow::bail!("election heartbeat interval must be below session timeout");
        }
        if self.election_session_timeout_ms > self.election_rebalance_timeout_ms {
            anyhow::bail!("election session timeout exceeds rebalance timeout");
        }
        if crate::compat::CompatibilityLevel::try_parse(&self.default_compatibility_level).is_none()
        {
            anyhow::bail!("invalid default compatibility level");
        }
        if !matches!(
            self.default_mode.as_str(),
            "READWRITE" | "READONLY" | "IMPORT"
        ) {
            anyhow::bail!("invalid default mode");
        }
        Ok(())
    }
}

impl Default for RegistryRuntimeConfig {
    fn default() -> Self {
        Self {
            election_session_timeout_ms: 10_000,
            election_rebalance_timeout_ms: 30_000,
            election_heartbeat_interval_ms: 3_000,
            election_reconnect_backoff_ms: 500,
            store_reader_retry_backoff_ms: 250,
            store_reader_fetch_max_wait_ms: 500,
            store_reader_fetch_max_bytes: 1_048_576,
            schemas_topic_create_timeout_ms: 15_000,
            default_compatibility_level: "BACKWARD".into(),
            default_mode: "READWRITE".into(),
        }
    }
}

/// Opt-in security knobs. The [`Default`] (all `None`/`false`) reproduces the
/// pre-security behaviour exactly: open HTTP, anonymous requests, a plaintext
/// Kafka client to the broker.
#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    /// When true, an unauthenticated (Anonymous) request is rejected with 401.
    pub require_auth: bool,
    /// `WWW-Authenticate: basic realm="<realm>"`.
    pub realm: String,
    pub basic: Option<BasicAuthConfig>,
    pub bearer: Option<BearerAuthConfig>,
    /// Server TLS (HTTPS). None means plain HTTP.
    pub tls: Option<crabka_security::TlsConfig>,
    pub authz: Option<AuthzConfig>,
    /// SR-to-broker Kafka-client security. None means PLAINTEXT.
    pub client: Option<crabka_client_core::ClientSecurity>,
}

/// Inline `user -> credential` (plaintext per cp `PropertyFileLoginModule`, or a
/// `$2...` bcrypt hash). `file` is an htpasswd-style `user:cred` path.
#[derive(Debug, Clone, Default)]
pub struct BasicAuthConfig {
    pub users: HashMap<String, String>,
    pub file: Option<PathBuf>,
}

/// Reuse the broker OAuth validator; stored already-built.
#[derive(Clone)]
pub struct BearerAuthConfig {
    pub validator: std::sync::Arc<crabka_security::OAuthBearerValidator>,
}
impl std::fmt::Debug for BearerAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BearerAuthConfig")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzConfig {
    pub enabled: bool,
    pub super_users: HashSet<String>,
    pub acl_refresh: std::time::Duration,
}
impl Default for AuthzConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            super_users: HashSet::new(),
            acl_refresh: std::time::Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{RegistryRuntimeConfig, SecurityConfig};
    use crate::config_value::{PositiveI32, PositiveMillis};

    #[test]
    fn runtime_scalar_boundaries_and_defaults() {
        check!(PositiveMillis::new(0).is_err());
        check!(PositiveMillis::new(1).is_ok());
        check!(PositiveI32::new(0).is_err());
        check!(PositiveI32::new(1).is_ok());
        assert2::assert!(
            RegistryRuntimeConfig::default()
                == RegistryRuntimeConfig {
                    election_session_timeout_ms: 10_000,
                    election_rebalance_timeout_ms: 30_000,
                    election_heartbeat_interval_ms: 3_000,
                    election_reconnect_backoff_ms: 500,
                    store_reader_retry_backoff_ms: 250,
                    store_reader_fetch_max_wait_ms: 500,
                    store_reader_fetch_max_bytes: 1_048_576,
                    schemas_topic_create_timeout_ms: 15_000,
                    default_compatibility_level: "BACKWARD".into(),
                    default_mode: "READWRITE".into(),
                }
        );
    }

    #[test]
    fn runtime_relations_are_rejected() {
        let runtime = RegistryRuntimeConfig {
            election_heartbeat_interval_ms: 10_000,
            ..RegistryRuntimeConfig::default()
        };
        assert2::assert!(runtime.validate().is_err());
        let runtime = RegistryRuntimeConfig {
            election_rebalance_timeout_ms: 9_999,
            ..RegistryRuntimeConfig::default()
        };
        assert2::assert!(runtime.validate().is_err());
    }

    #[test]
    fn default_security_is_fully_open() {
        let s = SecurityConfig::default();
        check!(
            (
                s.require_auth,
                s.realm.is_empty(),
                s.basic.is_none(),
                s.bearer.is_none(),
                s.tls.is_none(),
                s.authz.is_none(),
                s.client.is_none(),
            ) == (false, true, true, true, true, true, true)
        );
    }

    #[test]
    fn authz_config_default_is_disabled_with_30s_refresh() {
        let a = super::AuthzConfig::default();
        assert2::assert!(
            a == super::AuthzConfig {
                enabled: false,
                super_users: std::collections::HashSet::new(),
                acl_refresh: std::time::Duration::from_secs(30),
            }
        );
    }

    #[test]
    fn bearer_auth_config_debug_does_not_leak_validator() {
        // The manual Debug impl exists so a validator (which may wrap JWKS
        // handles / secrets) never lands in logs — it must render as the opaque
        // tag regardless of its contents.
        let validator = std::sync::Arc::new(crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator::default(),
        ));
        let cfg = super::BearerAuthConfig { validator };
        assert2::assert!(format!("{cfg:?}") == "BearerAuthConfig");
    }
}
