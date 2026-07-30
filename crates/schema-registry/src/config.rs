//! Runtime configuration for the registry service.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use crabka_units::prelude::*;
use refined_type::{Refined, rule::GreaterI32};

fn protocol_millis_i32(name: &str, value: Time) -> anyhow::Result<i32> {
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        anyhow::bail!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX");
    }
    let millis = i32::try_from(millis).map_err(|_| {
        anyhow::anyhow!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        )
    })?;
    GreaterI32::<0>::new(millis)
        .map(Refined::into_value)
        .map_err(|_| {
            anyhow::anyhow!(
                "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
            )
        })
}

fn protocol_bytes_i32(name: &str, value: ByteSize) -> anyhow::Result<i32> {
    let bytes = value.bytes_u64();
    if !value.bytes_f64().is_finite() || ByteSize::from_bytes(bytes) != value {
        anyhow::bail!("{name} must be a positive whole number of bytes within 1..=i32::MAX");
    }
    let bytes = i32::try_from(bytes).map_err(|_| {
        anyhow::anyhow!("{name} must be a positive whole number of bytes within 1..=i32::MAX")
    })?;
    GreaterI32::<0>::new(bytes)
        .map(Refined::into_value)
        .map_err(|_| {
            anyhow::anyhow!("{name} must be a positive whole number of bytes within 1..=i32::MAX")
        })
}

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
///
/// The extents and sizes are quantities, so this is `PartialEq` but not `Eq`
/// (both store `f64`); nothing keys a map or set on a runtime config.
#[derive(Debug, Clone, PartialEq)]
pub struct RegistryRuntimeConfig {
    pub client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    pub client_frame_max: crabka_client_core::ClientFrameMax,
    pub election_session_timeout: Time,
    pub election_rebalance_timeout: Time,
    pub election_heartbeat_interval: Time,
    pub election_reconnect_backoff: Time,
    pub store_reader_retry_backoff: Time,
    pub store_reader_fetch_max_wait: Time,
    pub store_reader_fetch_max: ByteSize,
    pub schemas_topic_create_timeout: Time,
    /// Largest forwarded request body this node will buffer before replaying it
    /// to the primary.
    pub forward_max_body: ByteSize,
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
        protocol_millis_i32(
            "store reader fetch max wait",
            self.store_reader_fetch_max_wait,
        )?;
        protocol_bytes_i32("store reader fetch max", self.store_reader_fetch_max)?;
        protocol_millis_i32(
            "schemas topic create timeout",
            self.schemas_topic_create_timeout,
        )?;
        if self.election_heartbeat_interval >= self.election_session_timeout {
            anyhow::bail!("election heartbeat interval must be below session timeout");
        }
        if self.election_session_timeout > self.election_rebalance_timeout {
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
            client_dispatch_queue_capacity:
                crabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: crabka_client_core::ClientFrameMax::default(),
            election_session_timeout: secs(10),
            election_rebalance_timeout: secs(30),
            election_heartbeat_interval: secs(3),
            election_reconnect_backoff: millis(500),
            store_reader_retry_backoff: millis(250),
            store_reader_fetch_max_wait: millis(500),
            store_reader_fetch_max: mebibytes(1),
            schemas_topic_create_timeout: secs(15),
            forward_max_body: mebibytes(16),
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

/// `PartialEq` but not `Eq`: [`Time`] stores `f64`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthzConfig {
    pub enabled: bool,
    pub super_users: HashSet<String>,
    /// How often the ACL cache is rebuilt from the broker's `DescribeAcls`.
    pub acl_refresh: Time,
}
impl Default for AuthzConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            super_users: HashSet::new(),
            acl_refresh: DEFAULT_ACL_REFRESH,
        }
    }
}

/// Default ACL-cache refresh interval.
pub const DEFAULT_ACL_REFRESH: Time = secs(30);

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use super::{RegistryRuntimeConfig, SecurityConfig};
    use crate::config_value::PositiveI32;

    #[test]
    fn runtime_scalar_boundaries_and_defaults() {
        check!(PositiveI32::new(0).is_err());
        check!(PositiveI32::new(1).is_ok());
        check!(
            RegistryRuntimeConfig::default()
                == RegistryRuntimeConfig {
                    client_dispatch_queue_capacity:
                        crabka_client_core::ConnectionDispatchQueueCapacity::default(),
                    client_frame_max: crabka_client_core::ClientFrameMax::default(),
                    election_session_timeout: secs(10),
                    election_rebalance_timeout: secs(30),
                    election_heartbeat_interval: secs(3),
                    election_reconnect_backoff: millis(500),
                    store_reader_retry_backoff: millis(250),
                    store_reader_fetch_max_wait: millis(500),
                    store_reader_fetch_max: mebibytes(1),
                    schemas_topic_create_timeout: secs(15),
                    forward_max_body: mebibytes(16),
                    default_compatibility_level: "BACKWARD".into(),
                    default_mode: "READWRITE".into(),
                }
        );
    }

    #[test]
    fn runtime_relations_are_rejected() {
        let runtime = RegistryRuntimeConfig {
            election_heartbeat_interval: secs(10),
            ..RegistryRuntimeConfig::default()
        };
        check!(runtime.validate().is_err());
        let runtime = RegistryRuntimeConfig {
            election_rebalance_timeout: millis(9_999),
            ..RegistryRuntimeConfig::default()
        };
        check!(runtime.validate().is_err());
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
        check!(
            a == super::AuthzConfig {
                enabled: false,
                super_users: std::collections::HashSet::new(),
                acl_refresh: secs(30),
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
        check!(format!("{cfg:?}") == "BearerAuthConfig");
    }
}
