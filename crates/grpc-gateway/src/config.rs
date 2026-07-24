//! Gateway configuration, parsed from CLI flags / env in `bin/gateway.rs`.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

pub use crabka_security::ClientAuthMode;

use crate::webhook_config::CompiledWebhook;

/// TLS / mTLS settings for the gateway listener and the forward channel.
/// Present ⇒ the gateway serves over rustls; absent ⇒ plaintext.
#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// Server cert chain (PEM). Doubles as the gateway's client identity when
    /// forwarding (the cert is issued with server+client EKU).
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    /// CA(s) the forwarder trusts when verifying a peer gateway's server cert.
    pub trust_roots_path: Option<PathBuf>,
    /// CA(s) used to verify incoming client certs (mTLS). Required if
    /// `client_auth != Disabled`.
    pub client_ca_path: Option<PathBuf>,
    pub client_auth: ClientAuthMode,
    /// Cert hot-reload poll interval (seconds).
    pub reload_interval_secs: u64,
}

impl TlsSettings {
    /// Map to the `crabka-security` config used to build server/client configs.
    #[must_use]
    pub fn to_security(&self) -> crabka_security::TlsConfig {
        crabka_security::TlsConfig {
            cert_chain_path: self.cert_chain_path.clone(),
            private_key_path: self.private_key_path.clone(),
            trust_roots_path: self.trust_roots_path.clone(),
            client_ca_path: self.client_ca_path.clone(),
            client_auth: self.client_auth,
        }
    }
}

/// Bearer-token validation settings for per-request caller authentication.
///
/// Present ⇒ a [`BearerValidator`](crate::authz::BearerValidator) extension is
/// mounted on the router; absent ⇒ only the mTLS principal (if any) is used.
///
/// Currently supports only the **unsecured** (`alg:none`) JWS validator — the
/// Kafka `OAuthBearerUnsecuredValidatorCallbackHandler` equivalent, intended for
/// development / testing environments.
///
/// Signed JWKS-backed bearer validation is not part of this settings surface;
/// callers that need it should configure validation in the broker-facing
/// security layer instead.
#[derive(Debug, Clone)]
pub struct BearerSettings {
    /// The JWT claim whose string value becomes the principal name.
    /// Defaults to `"sub"`.
    pub principal_claim_name: String,
    /// Allowable clock-skew tolerance (milliseconds) for `exp`/`iat` checks.
    /// Defaults to `30_000` (30 seconds), mirroring the JVM default.
    pub allowable_clock_skew_ms: i64,
}

impl Default for BearerSettings {
    fn default() -> Self {
        Self {
            principal_claim_name: "sub".to_string(),
            allowable_clock_skew_ms: 30_000,
        }
    }
}

impl BearerSettings {
    /// Build an [`crabka_security::OAuthBearerValidator`] from these settings.
    ///
    /// Currently produces an `Unsecured(UnsecuredJwsValidator)` — the
    /// development validator; always succeeds construction.
    ///
    /// # Errors
    ///
    /// Currently infallible (returns `Ok`); the signature is `Result` to
    /// accommodate future `Signed` / `Introspection` variants that may fail
    /// at build time (e.g. bad JWKS URL, missing cert).
    pub fn build(&self) -> Result<crabka_security::OAuthBearerValidator, String> {
        Ok(crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator {
                principal_claim_name: self.principal_claim_name.clone(),
                allowable_clock_skew_ms: self.allowable_clock_skew_ms,
                ..Default::default()
            },
        ))
    }
}

/// Authorization settings. `None` ⇒ `AllowAll` (no enforcement; default).
#[derive(Debug, Clone)]
pub struct AuthzSettings {
    /// Principals (bare names) that bypass ACL checks.
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval (seconds).
    pub acl_refresh_secs: u64,
}

/// Deployment policy shared by gateway runtime components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRuntimeConfig {
    pub internal_topic_replication_factor: i16,
    pub internal_topic_allow_replication_fallback: bool,
    pub internal_topic_create_timeout_ms: i32,
    pub internal_topic_segment_ms: i64,
    pub internal_topic_min_cleanable_dirty_ratio_basis_points: u32,
    pub consumer_poll_timeout_ms: u64,
    pub ownership_warmup_empty_polls: u32,
    pub readiness_poll_interval_ms: u64,
    pub produce_max_body_bytes: usize,
    pub schema_registry_latest_cache_ttl_ms: u64,
    pub schema_registry_frame_raw: bool,
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            internal_topic_replication_factor: 3,
            internal_topic_allow_replication_fallback: true,
            internal_topic_create_timeout_ms: 10_000,
            internal_topic_segment_ms: 60_000,
            internal_topic_min_cleanable_dirty_ratio_basis_points: 100,
            consumer_poll_timeout_ms: 500,
            ownership_warmup_empty_polls: 2,
            readiness_poll_interval_ms: 250,
            produce_max_body_bytes: 2_097_152,
            schema_registry_latest_cache_ttl_ms: 5_000,
            schema_registry_frame_raw: false,
        }
    }
}

/// Runtime configuration for the gateway process.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// `host:port,host:port,...` of brokers for bootstrap.
    pub bootstrap: String,
    /// Connect-RPC + HTTP listen address.
    pub listen_addr: SocketAddr,
    /// Base `client.id` for the native clients this gateway opens.
    pub client_id: String,
    /// Internal compacted topic that stores dedup claims.
    pub dedup_topic: String,
    /// Partition count of the dedup topic (also the ownership shard count in P3).
    pub dedup_partitions: u32,
    /// Dedup window: claim-topic `retention.ms` and the dedup guarantee horizon.
    pub dedup_window_ms: i64,
    /// Consumer group used to divide dedup-topic ownership between replicas.
    pub dedup_ownership_group: String,
    /// `transactional.id` prefix; the per-partition id is `{prefix}-{p}`.
    pub dedup_txn_id_prefix: String,
    /// Address other replicas reach THIS gateway at (host:port of `listen_addr`,
    /// externally routable). Published to membership; used to forward.
    pub advertised_addr: String,
    /// Internal compacted topic carrying replica membership / owner routing.
    pub membership_topic: String,
    /// TLS/mTLS settings; `None` ⇒ plaintext (all current tests).
    pub tls: Option<TlsSettings>,
    /// Client security for connections FROM the gateway TO the broker.
    /// `None` ⇒ plaintext (default; all current tests).
    pub broker_security: Option<crabka_client_core::security::ClientSecurity>,
    /// Authorization settings; `None` ⇒ `AllowAll` (no enforcement; default).
    pub authz: Option<AuthzSettings>,
    /// Named webhook endpoints; compiled from a TOML config file at startup.
    /// Empty ⇒ `/v1/webhooks/{name}` returns 404 for every name.
    pub webhooks: HashMap<String, CompiledWebhook>,
    /// Outbound webhook subscriptions; compiled from a separate TOML config
    /// file at startup. Empty ⇒ no outbound delivery tasks are spawned.
    pub outbound: Vec<crate::outbound_config::CompiledSubscription>,
    /// Base URL of a Confluent-compatible Schema Registry (e.g.
    /// `http://schema-registry:8081`). When set, the gateway builds a
    /// [`crate::schema::codec::SchemaRegistryCodec`] and injects it into the
    /// produce and consume paths. When absent, `RawCodec` (the identity
    /// pass-through) is used — existing behaviour is unchanged.
    pub schema_registry_url: Option<String>,
    /// Common deployment policy for runtime components.
    pub runtime: GatewayRuntimeConfig,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::GatewayRuntimeConfig;
    use crate::config_value::{DirtyRatioBasisPoints, PositiveU64};

    #[test]
    fn runtime_defaults_and_boundaries() {
        assert!(
            GatewayRuntimeConfig::default()
                == GatewayRuntimeConfig {
                    internal_topic_replication_factor: 3,
                    internal_topic_allow_replication_fallback: true,
                    internal_topic_create_timeout_ms: 10_000,
                    internal_topic_segment_ms: 60_000,
                    internal_topic_min_cleanable_dirty_ratio_basis_points: 100,
                    consumer_poll_timeout_ms: 500,
                    ownership_warmup_empty_polls: 2,
                    readiness_poll_interval_ms: 250,
                    produce_max_body_bytes: 2_097_152,
                    schema_registry_latest_cache_ttl_ms: 5_000,
                    schema_registry_frame_raw: false,
                }
        );
        check!(PositiveU64::new(0).is_err());
        check!(PositiveU64::new(1).is_ok());
        check!(DirtyRatioBasisPoints::new(10_001).is_err());
        check!(DirtyRatioBasisPoints::new(10_000).is_ok());
    }
}
