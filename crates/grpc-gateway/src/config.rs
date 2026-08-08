//! Gateway configuration, parsed from CLI flags / env in `bin/gateway.rs`.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

pub use crabka_security::ClientAuthMode;
use crabka_units::prelude::*;
use refined_type::{
    Refined,
    rule::{GreaterI32, GreaterI64},
};

use crate::webhook_config::CompiledWebhook;

/// TLS / mTLS settings for the gateway listener and the forward channel.
/// Present ⇒ the gateway serves over rustls; absent ⇒ plaintext.
#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// Server cert chain (PEM). It is also the gateway's client identity on a
    /// forward, because the cert carries both the server and the client EKU.
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    /// CA(s) the forwarder trusts when it verifies a peer gateway's server cert.
    pub trust_roots_path: Option<PathBuf>,
    /// CA(s) that verify incoming client certs (mTLS). Required if
    /// `client_auth != Disabled`.
    pub client_ca_path: Option<PathBuf>,
    pub client_auth: ClientAuthMode,
    /// Cert hot-reload poll interval.
    pub reload_interval: Time,
}

impl TlsSettings {
    /// Map to the `crabka-security` config that builds server and client configs.
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
/// Currently supports only the unsecured (`alg:none`) JWS validator. That
/// validator is the equivalent of the Kafka
/// `OAuthBearerUnsecuredValidatorCallbackHandler`, and it is for development
/// and test environments.
///
/// Signed JWKS-backed bearer validation is not part of this settings surface.
/// Callers that need it should configure validation in the broker-facing
/// security layer instead.
#[derive(Debug, Clone)]
pub struct BearerSettings {
    /// The JWT claim whose string value becomes the principal name.
    /// Defaults to `"sub"`.
    pub principal_claim_name: String,
    /// Allowable clock-skew tolerance for `exp`/`iat` checks. Defaults to 30
    /// seconds, the same as the JVM default.
    pub allowable_clock_skew: Time,
}

impl Default for BearerSettings {
    fn default() -> Self {
        Self {
            principal_claim_name: "sub".to_string(),
            allowable_clock_skew: secs(30),
        }
    }
}

impl BearerSettings {
    /// Build an [`crabka_security::OAuthBearerValidator`] from these settings.
    ///
    /// Currently produces an `Unsecured(UnsecuredJwsValidator)`, the
    /// development validator. Construction always succeeds.
    ///
    /// # Errors
    ///
    /// Currently infallible; it always returns `Ok`. The signature is `Result`
    /// for future `Signed` and `Introspection` variants that can fail at build
    /// time, for example on a bad JWKS URL or a missing cert.
    pub fn build(&self) -> Result<crabka_security::OAuthBearerValidator, String> {
        Ok(crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator {
                principal_claim_name: self.principal_claim_name.clone(),
                allowable_clock_skew: self.allowable_clock_skew,
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
    /// ACL-cache refresh interval.
    pub acl_refresh: Time,
}

/// Deployment policy shared by gateway runtime components.
#[derive(Debug, Clone, PartialEq)]
pub struct GatewayRuntimeConfig {
    pub client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    pub client_frame_max: crabka_client_core::ClientFrameMax,
    pub internal_topic_replication_factor: i16,
    pub internal_topic_allow_replication_fallback: bool,
    pub internal_topic_create_timeout: Time,
    pub internal_topic_segment: Time,
    pub internal_topic_min_cleanable_dirty_ratio: Ratio,
    pub consumer_poll_timeout: Time,
    pub ownership_warmup_empty_polls: u32,
    pub readiness_poll_interval: Time,
    pub produce_max_body: ByteSize,
    pub forward_max_body: ByteSize,
    pub schema_registry_latest_cache_ttl: Time,
    pub schema_registry_frame_raw: bool,
}

fn protocol_millis_i32(name: &str, value: Time) -> Result<i32, String> {
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        ));
    }
    let millis = i32::try_from(millis).map_err(|_| {
        format!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX")
    })?;
    GreaterI32::<0>::new(millis)
        .map(Refined::into_value)
        .map_err(|_| {
            format!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX")
        })
}

// f64 represents every integer below 2^53; at this value adjacent inputs can
// collapse before validation sees the UOM quantity.
const FIRST_AMBIGUOUS_F64_MILLIS: i64 = 9_007_199_254_740_992;

fn protocol_millis_i64(name: &str, value: Time) -> Result<i64, String> {
    let millis = value.millis_i64();
    if millis >= FIRST_AMBIGUOUS_F64_MILLIS {
        return Err(format!(
            "{name} must be below {FIRST_AMBIGUOUS_F64_MILLIS}ms because UOM quantities use f64"
        ));
    }
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i64::MAX"
        ));
    }
    GreaterI64::<0>::new(millis)
        .map(Refined::into_value)
        .map_err(|_| {
            format!("{name} must be a positive whole number of milliseconds within 1..=i64::MAX")
        })
}

impl GatewayRuntimeConfig {
    /// Validate values lowered into Kafka protocol/config integer units.
    ///
    /// # Errors
    ///
    /// Returns an error when a value is fractional, non-positive, or outside
    /// the exact UOM range used by the downstream client.
    pub fn validate_protocol_units(&self) -> Result<(), String> {
        protocol_millis_i32(
            "internal topic create timeout",
            self.internal_topic_create_timeout,
        )?;
        protocol_millis_i64("internal topic segment", self.internal_topic_segment)?;
        Ok(())
    }
}

/// Validate the dedup retention duration before it is lowered to `retention.ms`.
///
/// # Errors
///
/// Returns an error unless `value` is an exact positive millisecond value below
/// the first integer boundary that an `f64` cannot distinguish safely.
pub fn validate_dedup_window(value: Time) -> Result<(), String> {
    protocol_millis_i64("dedup window", value).map(|_| ())
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            client_dispatch_queue_capacity:
                crabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: crabka_client_core::ClientFrameMax::default(),
            internal_topic_replication_factor: 3,
            internal_topic_allow_replication_fallback: true,
            internal_topic_create_timeout: secs(10),
            internal_topic_segment: minutes(1),
            internal_topic_min_cleanable_dirty_ratio: percent(1),
            consumer_poll_timeout: millis(500),
            ownership_warmup_empty_polls: 2,
            readiness_poll_interval: millis(250),
            produce_max_body: mebibytes(2),
            forward_max_body: mebibytes(2),
            schema_registry_latest_cache_ttl: secs(5),
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
    pub dedup_window: Time,
    /// Consumer group used to divide dedup-topic ownership between replicas.
    pub dedup_ownership_group: String,
    /// `transactional.id` prefix; the per-partition id is `{prefix}-{p}`.
    pub dedup_txn_id_prefix: String,
    /// Address other replicas reach THIS gateway at. It is the host:port of
    /// `listen_addr` and must be externally routable. The gateway publishes it
    /// to membership and uses it to forward.
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
    /// Named webhook endpoints, compiled from a TOML config file at startup.
    /// Empty ⇒ `/v1/webhooks/{name}` returns 404 for every name.
    pub webhooks: HashMap<String, CompiledWebhook>,
    /// Outbound webhook subscriptions, compiled from a separate TOML config
    /// file at startup. Empty ⇒ the gateway spawns no outbound delivery tasks.
    pub outbound: Vec<crate::outbound_config::CompiledSubscription>,
    /// Base URL of a Confluent-compatible Schema Registry (e.g.
    /// `http://schema-registry:8081`). When set, the gateway builds a
    /// [`crate::schema::codec::SchemaRegistryCodec`] and injects it into the
    /// produce and consume paths. When absent, the gateway uses `RawCodec`,
    /// the identity pass-through.
    pub schema_registry_url: Option<String>,
    /// Common deployment policy for runtime components.
    pub runtime: GatewayRuntimeConfig,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::prelude::*;

    use super::{BearerSettings, GatewayRuntimeConfig};
    use crate::config_value::{PartitionCount, PositiveU32};

    #[test]
    fn runtime_defaults_and_boundaries() {
        assert!(
            GatewayRuntimeConfig::default()
                == GatewayRuntimeConfig {
                    client_dispatch_queue_capacity:
                        crabka_client_core::ConnectionDispatchQueueCapacity::default(),
                    client_frame_max: crabka_client_core::ClientFrameMax::default(),
                    internal_topic_replication_factor: 3,
                    internal_topic_allow_replication_fallback: true,
                    internal_topic_create_timeout: secs(10),
                    internal_topic_segment: secs(60),
                    internal_topic_min_cleanable_dirty_ratio: fraction(0.01),
                    consumer_poll_timeout: millis(500),
                    ownership_warmup_empty_polls: 2,
                    readiness_poll_interval: millis(250),
                    produce_max_body: kibibytes(2048),
                    forward_max_body: kibibytes(2048),
                    schema_registry_latest_cache_ttl: secs(5),
                    schema_registry_frame_raw: false,
                }
        );
        check!(PositiveU32::new(0).is_err());
        check!(PositiveU32::new(1).is_ok());
        check!(PartitionCount::new(2_147_483_648).is_err());
        check!(PartitionCount::new(2_147_483_647).is_ok());
    }

    /// The bearer tolerance is a `Time`. The gateway hands it to
    /// `crabka-security` as the raw millisecond count that its validator expects.
    #[test]
    fn bearer_clock_skew_reaches_the_validator_in_milliseconds() {
        let validator = BearerSettings {
            principal_claim_name: "sub".to_string(),
            allowable_clock_skew: secs(45),
        }
        .build()
        .expect("unsecured validator builds");

        let crabka_security::OAuthBearerValidator::Unsecured(unsecured) = validator else {
            panic!("unsecured settings build an unsecured validator");
        };
        check!(unsecured.allowable_clock_skew == secs(45));
        check!(unsecured.principal_claim_name.as_str() == "sub");
    }
}
