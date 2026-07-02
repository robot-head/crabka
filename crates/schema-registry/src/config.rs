//! Runtime configuration for the registry service.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

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
    /// Authentication / authorization / TLS / SR-to-broker client security.
    /// The [`Default`] is fully permissive (open HTTP, anonymous, plaintext
    /// broker client) — every field opts in independently.
    pub security: SecurityConfig,
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
    use super::SecurityConfig;
    use assert2::check;

    #[test]
    fn default_security_is_fully_open() {
        let s = SecurityConfig::default();
        check!(!s.require_auth);
        check!(s.realm.is_empty());
        check!(s.basic.is_none());
        check!(s.bearer.is_none());
        check!(s.tls.is_none());
        check!(s.authz.is_none());
        check!(s.client.is_none());
    }

    #[test]
    fn authz_config_default_is_disabled_with_30s_refresh() {
        let a = super::AuthzConfig::default();
        assert!(!a.enabled);
        assert!(a.super_users.is_empty());
        assert_eq!(a.acl_refresh, std::time::Duration::from_secs(30));
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
        assert_eq!(format!("{cfg:?}"), "BearerAuthConfig");
    }
}
