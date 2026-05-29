//! TOML file-config surface for the `crabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and merged
//! into [`crate::BrokerConfig`]. Only `[[listeners]]`,
//! `inter_broker_listener_name`, and (passively) `[server_properties]`
//! are consumed; other top-level keys are accepted but ignored.

use std::net::SocketAddr;

use serde::Deserialize;

use crabka_security::ListenerProtocol;

use crate::config::ListenerSpec;

/// Failures surfaced by [`FileConfig::apply_to`]. Each variant
/// corresponds to a specific misconfiguration the broker can diagnose
/// at startup; the variants exist (rather than a single `String`
/// fallthrough) so the binary entry point can log structured context.
#[derive(Debug, thiserror::Error)]
pub enum FileConfigError {
    /// A `[section]` referenced by another field is missing — e.g.
    /// `[authorization] type = "opa"` without an `[authorization.opa]`
    /// table. The payload names the missing section.
    #[error("missing required TOML section: {0}")]
    MissingSection(String),
    /// `OpaAuthorizer::new` failed (see [`crate::authorizer::opa::OpaConfigError`]).
    /// The payload is the underlying error's `Debug` form — formatted
    /// here rather than at the call site so the binary entry point can
    /// log a single string.
    #[error("OPA authorizer configuration error: {0}")]
    OpaConfig(String),
    /// A TOML section's contents conflict in a way only the apply step
    /// can diagnose — e.g. `[remote_storage]` carrying both `storage_dir`
    /// (local backend) and `[remote_storage.s3]` (object-store backend).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — new fields may be added and old binaries should warn rather
/// than refuse to start.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileConfig {
    pub broker_id: Option<i32>,
    pub log_dir: Option<String>,
    /// Additional JBOD data directories (KIP-113). Maps to
    /// [`crate::BrokerConfig::extra_log_dirs`].
    #[serde(default)]
    pub extra_log_dirs: Vec<String>,
    /// KIP-392: this broker's rack id. Maps to `BrokerConfig::rack`.
    #[serde(default)]
    pub rack: Option<String>,

    /// KIP-392: replica selector name (`"leader"` | `"rack-aware"`).
    /// Maps to `BrokerConfig::replica_selector`.
    #[serde(default)]
    pub replica_selector: Option<String>,
    pub inter_broker_listener_name: Option<String>,
    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[serde(default)]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,

    /// SASL/OAUTHBEARER validator tuning. Only relevant when a
    /// listener enables the `OAUTHBEARER` mechanism.
    #[serde(default)]
    pub oauthbearer: Option<FileOAuthBearerConfig>,

    /// KIP-48: delegation-token master key + lifetime knobs.
    /// Env var `CRABKA_DELEGATION_TOKEN_SECRET_KEY` wins over `secret_key`
    /// here. When neither source provides a key, the broker disables
    /// delegation-token auth.
    #[serde(default)]
    pub delegation_token: Option<FileDelegationTokenConfig>,

    /// Principals that are unconditionally authorized for
    /// all operations, including KIP-48 delegation-token `act-as`. The
    /// operator emits `super_users = ["ANONYMOUS"]` when
    /// `Kafka.spec.delegationToken` is set so its PLAINTEXT
    /// inter-broker reconcile loop can mint per-`KafkaUser` tokens.
    /// `None` and `Some(empty)` are equivalent — both leave
    /// `BrokerConfig.super_users` empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub super_users: Option<Vec<String>>,

    /// KIP-405: tiered-storage enablement. Setting
    /// `storage_dir` turns tiered storage on broker-wide and roots the
    /// local reference `RemoteStorageManager` there.
    #[serde(default)]
    pub remote_storage: Option<FileRemoteStorageConfig>,

    /// Pluggable cluster authorizer + super-user list.
    /// `None` ⇒ [`crate::authorizer::AllowAllAuthorizer`] with empty
    /// super-users (default-on-no-config behavior). When `Some`, the
    /// `type` field selects the authorizer implementation; for
    /// `type = "opa"`, the `[authorization.opa]` subtable is required.
    #[serde(default)]
    pub authorization: Option<FileAuthorizationConfig>,

    /// `[process]` section — `KRaft` `process.roles`. Absent / empty leaves
    /// the `BrokerConfig` default `[Controller, Broker]`.
    #[serde(default)]
    pub process: Option<FileProcessConfig>,
}

/// TOML shape of `[remote_storage]`. Maps to
/// [`crate::BrokerConfig::remote_storage_backend`].
///
/// Exactly one of `storage_dir` (local filesystem) or `[remote_storage.s3]`
/// (S3-compatible object store) should be set. Setting both errors at
/// load time.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageConfig {
    /// Root directory for the local `LocalTieredStorage` backend.
    pub storage_dir: Option<String>,
    /// S3-compatible backend parameters. Omit to use `storage_dir`.
    pub s3: Option<FileRemoteStorageS3Config>,
    /// Opt-in to the topic-backed
    /// [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager).
    /// When absent, the broker uses the in-memory fixture.
    pub kafka_metadata: Option<FileKafkaRlmmConfig>,
}

/// TOML shape of `[remote_storage.kafka_metadata]`. Maps to
/// [`crate::config::KafkaRlmmConfig`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileKafkaRlmmConfig {
    /// `host:port` the manager dials to reach its own broker.
    pub bootstrap: String,
    /// Partition count for `__remote_log_metadata` on first creation.
    /// Defaults to 50 (Kafka's
    /// `remote.log.metadata.topic.num.partitions`).
    #[serde(default)]
    pub num_partitions: Option<i32>,
    /// Replication factor for `__remote_log_metadata` on first
    /// creation. Defaults to 3 (Kafka's
    /// `remote.log.metadata.topic.replication.factor`).
    #[serde(default)]
    pub replication: Option<i32>,
}

/// TOML shape of `[remote_storage.s3]`. Maps to
/// [`crabka_remote_storage::S3Config`].
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageS3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region. Required even for non-AWS endpoints (use any value).
    pub region: String,
    /// Optional key prefix inside the bucket (lets multiple clusters
    /// share a bucket).
    #[serde(default)]
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (e.g. `MinIO` or Cloudflare R2).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Explicit access key id. Falls back to the AWS credential chain
    /// (env vars, instance profile, …) when omitted.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Explicit secret access key. Falls back to the AWS credential chain
    /// when omitted.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP (off-by-default; required by `MinIO` running
    /// without TLS).
    #[serde(default)]
    pub allow_http: bool,
    /// Optional override of the multipart-upload threshold (bytes). When
    /// `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`]
    /// applies. Operators typically leave this alone; lower it to force
    /// multipart on smaller segments for testing.
    #[serde(default)]
    pub multipart_threshold: Option<u64>,
    /// Optional override of the per-part multipart chunk size (bytes).
    /// When `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`]
    /// applies. AWS requires parts ≥ 5 MiB except the last; `MinIO`
    /// tolerates smaller values.
    #[serde(default)]
    pub multipart_chunk_size: Option<usize>,
}

/// TOML shape of `[authorization]`. `type` (renamed to `authz_type` on
/// the Rust side to avoid shadowing the keyword) defaults to
/// `AllowAll`; `super_users` is the principal bypass list consulted by
/// every concrete authorizer impl.
///
/// `deny_unknown_fields` so a misspelled `super_user` typo at the top
/// of the `[authorization]` block is rejected at parse time rather
/// than silently producing the wrong authorizer.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuthorizationConfig {
    #[serde(rename = "type", default)]
    pub authz_type: AuthzType,
    #[serde(default)]
    pub super_users: Vec<String>,
    /// `Some` iff `authz_type == Opa`. Required in that case;
    /// `apply_to` returns [`FileConfigError::MissingSection`] when
    /// omitted.
    #[serde(default)]
    pub opa: Option<FileOpaConfig>,
}

/// Which [`crate::authorizer::Authorizer`] impl to instantiate.
/// `snake_case` to match the spec's `type = "allow_all" | "simple" |
/// "opa"` wire shape.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthzType {
    #[default]
    AllowAll,
    Simple,
    Opa,
}

/// TOML shape of `[authorization.opa]`. Mirrors the constructor
/// arguments of [`crate::authorizer::opa::OpaAuthorizer::new`]. Defaults
/// are picked to match Strimzi's `KafkaAuthorizationOpa` (`50_000` LRU
/// entries, 1 h TTL, fail-closed on OPA error).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileOpaConfig {
    /// OPA decision endpoint URL — must include the data-API path,
    /// e.g. `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// Permit the operation when the OPA call fails (timeout, 5xx,
    /// parse error). Default `false` (fail-closed).
    #[serde(default)]
    pub allow_on_error: bool,
    /// LRU cache capacity, in entries. Default `50_000`.
    #[serde(default = "default_opa_maximum_cache_size")]
    pub maximum_cache_size: usize,
    /// Decision TTL, in milliseconds. Default `3_600_000` (1 h).
    #[serde(default = "default_opa_expire_after_ms")]
    pub expire_after_ms: i64,
}

fn default_opa_maximum_cache_size() -> usize {
    50_000
}

fn default_opa_expire_after_ms() -> i64 {
    60 * 60 * 1_000
}

/// TOML shape of `[delegation_token]`. Maps to the three `delegation_token_*`
/// fields on [`crate::BrokerConfig`].
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileDelegationTokenConfig {
    /// HMAC master key. Overridden by `CRABKA_DELEGATION_TOKEN_SECRET_KEY`
    /// when set. Bytes are wrapped in
    /// [`crabka_security::SecretBytes`] before reaching `BrokerConfig`.
    pub secret_key: Option<String>,
    /// Hard upper bound on token lifetime, ms. Default 7 days.
    pub max_lifetime_ms: Option<i64>,
    /// Background sweep cadence, ms. Default 1 hour.
    pub expiry_check_interval_ms: Option<i64>,
    /// Default renew period — the initial `expiry_timestamp_ms` offset
    /// at create time and the implicit renew period when
    /// `RenewDelegationToken.renew_period_ms == -1`. Distinct from
    /// `max_lifetime_ms` (the absolute ceiling). Default 24 hours.
    pub default_renew_period_ms: Option<i64>,
}

/// `[process]` TOML section — `KRaft` `process.roles`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileProcessConfig {
    /// Role strings: `"controller"`, `"broker"` (case-insensitive). Empty
    /// or absent leaves the `BrokerConfig` default `[Controller, Broker]`.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// TOML shape of `[oauthbearer]`. Maps to
/// [`crabka_security::OAuthBearerValidator`]. Setting `jwks_endpoint_uri`
/// selects the signed-JWT validator; setting
/// `introspection_endpoint_uri` selects the RFC 7662 introspection
/// validator; the two endpoint URIs are mutually
/// exclusive. With neither set, the unsecured-JWS validator
/// (development only) is used.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileOAuthBearerConfig {
    /// Claim whose value becomes the principal name. Default `sub`.
    #[serde(default)]
    pub principal_claim_name: Option<String>,
    /// Optional `JsonPath` expression (RFC 9535, via
    /// jsonpath-rust) evaluated against the token claim set. Token is
    /// rejected when the expression yields empty/null/false. Compiled
    /// once at broker startup; malformed expressions panic with a
    /// descriptive error.
    #[serde(default)]
    pub custom_claim_check: Option<String>,
    /// Optional JWT `typ` header check. When set, JWT-mode
    /// validators (unsecured + signed JWS) require the JWT header's
    /// `typ` field to equal this string. Introspection-mode skips
    /// (no JWT header). Ignored when unset.
    #[serde(default)]
    pub valid_token_type: Option<String>,
    /// Clock-skew tolerance, in milliseconds, for `exp` / `iat` / `nbf`.
    /// Default 30000.
    #[serde(default)]
    pub allowable_clock_skew_ms: Option<i64>,

    /// JWKS endpoint URL. When set, tokens are validated as signed
    /// JWTs (RS256 / ES256) against the keys fetched from this URL, and the
    /// broker spawns a background refresher. When unset, the unsecured-JWS
    /// (`alg:none`) development validator is used.
    #[serde(default)]
    pub jwks_endpoint_uri: Option<String>,
    /// When set, the token `iss` claim must equal this. Signed
    /// validator only.
    #[serde(default)]
    pub valid_issuer_uri: Option<String>,
    /// When set, the token `aud` claim must contain this. Signed
    /// validator only.
    #[serde(default)]
    pub expected_audience: Option<String>,
    /// JWKS re-fetch interval, in milliseconds. Default 300000
    /// (5 minutes). Signed validator only.
    #[serde(default)]
    pub jwks_refresh_interval_ms: Option<u64>,

    /// PEM file containing the CA
    /// certificate(s) used to verify the `IdP`'s TLS certificate on ALL
    /// outbound HTTPS to the `IdP` — JWKS endpoint, introspection
    /// endpoint, and userinfo endpoint. When set, these are
    /// the *only* trust roots used for the outbound HTTPS (replaces the
    /// default webpki-roots — Strimzi-shaped). When unset, the broker
    /// uses reqwest's default rustls webpki-roots.
    #[serde(default)]
    pub idp_tls_trust: Option<std::path::PathBuf>,

    /// RFC 7662 introspection endpoint URL. When set,
    /// selects the introspection validator (mutually exclusive with
    /// `jwks_endpoint_uri`).
    #[serde(default)]
    pub introspection_endpoint_uri: Option<String>,

    /// Optional OIDC userinfo endpoint URL. When set, the
    /// introspection validator calls `GET userinfo` after a successful
    /// introspection and merges the profile claims over the
    /// introspection claims (introspection wins for `active`, `exp`,
    /// `iat`, `nbf`, `scope`, `client_id`, `sub`).
    #[serde(default)]
    pub userinfo_endpoint_uri: Option<String>,

    /// `client_id` the broker uses to authenticate (HTTP Basic
    /// Auth) against the introspection endpoint. Required when
    /// `introspection_endpoint_uri` is set.
    #[serde(default)]
    pub introspection_client_id: Option<String>,

    /// Filesystem path to a file containing the client
    /// secret the broker uses to authenticate against the introspection
    /// endpoint. Required when `introspection_endpoint_uri` is set.
    /// File-based (not literal) so secret material doesn't sit in the
    /// TOML; operator mounts a `Secret` and writes the mount path here.
    /// The file's trailing newline (if any) is stripped at config-load.
    #[serde(default)]
    pub introspection_client_secret_path: Option<std::path::PathBuf>,

    /// Timeout for the introspection (and userinfo) HTTP
    /// requests, in milliseconds. Default 10 000 (10 s).
    #[serde(default)]
    pub introspection_http_timeout_ms: Option<u64>,

    /// Optional ceiling on OAUTHBEARER session lifetime, in
    /// seconds. When set, the broker clamps `session_lifetime_ms` to
    /// `min(token_exp_ms - now_ms, cap * 1000)`. When unset, sessions
    /// last until the token's natural `exp`.
    #[serde(default)]
    pub max_session_lifetime_seconds: Option<u32>,

    /// Alternate claim name for principal-name fallback.
    #[serde(default)]
    pub fallback_user_name_claim: Option<String>,
    /// Prepended on fallback only.
    #[serde(default)]
    pub fallback_user_name_prefix: Option<String>,
    /// `JsonPath` expression (RFC 9535) extracting groups.
    /// Compiled once at broker startup; malformed expression panics
    /// with descriptive error.
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// When `groups_claim` resolves to a string, split on
    /// this delimiter.
    #[serde(default)]
    pub groups_claim_delimiter: Option<String>,

    /// Minimum pause (seconds) between on-demand JWKS refreshes
    /// triggered by validator signals (unknown-kid / bad-signature tokens).
    /// Defaults to 1 (Strimzi parity). Signed validator only.
    #[serde(default)]
    pub jwks_min_refresh_pause_seconds: Option<u32>,

    /// Maximum age (seconds) of the cached JWKS before validators
    /// reject tokens until the next successful refresh. Strimzi default 360
    /// (6 minutes). Unset = no expiry check. Fails
    /// closed on prolonged `IdP` outage. Signed validator only.
    #[serde(default)]
    pub jwks_expiry_seconds: Option<u32>,

    /// When true, the JWKS parser keeps keys regardless of `use`
    /// field. Default false (filter out `use=enc`). Some identity providers
    /// publish signing keys with `use="enc"` by mistake; operators set this
    /// to true to accept them. Signed validator only.
    #[serde(default)]
    pub jwks_ignore_key_use: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    #[serde(default)]
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileListenerSaslConfig {
    #[serde(default, deserialize_with = "deserialize_sasl_mechanisms")]
    pub enabled_mechanisms: Vec<crabka_security::SaslMechanism>,
}

fn deserialize_sasl_mechanisms<'de, D>(
    deserializer: D,
) -> Result<Vec<crabka_security::SaslMechanism>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let names: Vec<String> = Vec::deserialize(deserializer)?;
    names
        .into_iter()
        .map(|s| {
            crabka_security::SaslMechanism::from_wire(&s)
                .ok_or_else(|| D::Error::custom(format!("unknown SASL mechanism: {s}")))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FileListener {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,
    #[serde(default)]
    pub sasl_config: Option<FileListenerSaslConfig>,
}

impl FileConfig {
    /// Apply this file-config to a `BrokerConfig` that already holds
    /// CLI-derived values. The file fills in unset values and provides
    /// `listeners` + `inter_broker_listener_name` wholesale when those
    /// are at their respective "empty" defaults.
    ///
    /// CLI values always win — the binary's `main()` constructs the
    /// `BrokerConfig` from CLI args first, then calls `apply_to`. The
    /// file never overrides what was explicitly set on the CLI.
    ///
    /// **Caller contract:** when `--config-file` is used, the caller
    /// must NOT pass `--listen-addr` or `--advertised-listener`. The
    /// binary entrypoint enforces this (see `bin/broker.rs`); this
    /// method just merges what it's given.
    // Linear config-load pipeline; each arm is its own validator construction —
    // extraction obscures the dispatch shape.
    //
    // # Errors
    //
    // * [`FileConfigError::MissingSection`] when `[authorization] type = "opa"`
    //   is set without the required `[authorization.opa]` subtable.
    // * [`FileConfigError::OpaConfig`] when [`crate::authorizer::opa::OpaAuthorizer::new`]
    //   rejects the resolved knobs (zero cache size, no tokio runtime, etc.).
    #[allow(clippy::too_many_lines)]
    pub fn apply_to(self, cfg: &mut crate::config::BrokerConfig) -> Result<(), FileConfigError> {
        let defaults = crate::config::BrokerConfig::default();
        if let Some(id) = self.broker_id
            && cfg.broker_id == defaults.broker_id
        {
            cfg.broker_id = id;
        }
        if let Some(rack) = self.rack {
            cfg.rack = Some(rack);
        }
        if let Some(sel) = self.replica_selector {
            cfg.replica_selector = crate::replica_selector::ReplicaSelectorKind::from_config_str(
                &sel,
            )
            .map_err(|bad| {
                FileConfigError::InvalidConfig(format!("unknown replica_selector: {bad}"))
            })?;
        }
        if let Some(ld) = self.log_dir
            && cfg.log_dir == defaults.log_dir
        {
            cfg.log_dir = std::path::PathBuf::from(ld);
        }
        if !self.extra_log_dirs.is_empty() && cfg.extra_log_dirs.is_empty() {
            cfg.extra_log_dirs = self
                .extra_log_dirs
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect();
        }
        if !self.listeners.is_empty() {
            cfg.listeners = self
                .listeners
                .into_iter()
                .map(FileListener::into_spec)
                .collect();
        }
        if let Some(name) = self.inter_broker_listener_name {
            cfg.inter_broker_listener_name = name;
        }
        // `[server_properties]` is intentionally ignored.
        if let Some(proto) = self.controller_listener_protocol
            && cfg.controller_listener_protocol == defaults.controller_listener_protocol
        {
            cfg.controller_listener_protocol = proto;
        }
        if let Some(tls) = self.tls_config
            && cfg.tls_config.is_none()
        {
            use crabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
            cfg.tls_config = Some(BrokerTlsConfig {
                cert_chain_path: tls.cert_path,
                private_key_path: tls.key_path,
                trust_roots_path: None,
                client_ca_path: tls.client_ca_path,
                client_auth: match tls.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            });
        }
        if let Some(oauth) = self.oauthbearer {
            // Thread the IdP trust-store path
            // unconditionally. Inert when no HTTPS-bound endpoint is set,
            // and harmlessly carried for the unsecured validator.
            cfg.oauthbearer_idp_tls_trust
                .clone_from(&oauth.idp_tls_trust);
            // Optional session-lifetime cap. Carried unconditionally;
            // the auth handler interprets None as "no cap".
            cfg.oauthbearer_max_session_lifetime_seconds = oauth.max_session_lifetime_seconds;

            // Compile the JsonPath expression once at load time;
            // a malformed expression panics with a descriptive error.
            let custom_claim_check_compiled = oauth
                .custom_claim_check
                .as_deref()
                .map(|expr| {
                    jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
                        panic!(
                            "[oauthbearer]: invalid custom_claim_check JsonPath expression {expr:?}: {e}"
                        )
                    })
                });

            // Compile groups_claim JsonPath at load time.
            let groups_claim_compiled = oauth.groups_claim.as_deref().map(|expr| {
                jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
                    panic!("[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}")
                })
            });

            match (
                oauth.jwks_endpoint_uri.as_ref(),
                oauth.introspection_endpoint_uri.as_ref(),
            ) {
                (Some(_), Some(_)) => {
                    panic!(
                        "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
                    );
                }
                (Some(_), None) => {
                    // Signed-JWT validation. The empty key handle is
                    // populated by the refresher `Broker::start` spawns.
                    let jwks_uri = oauth.jwks_endpoint_uri.clone().unwrap();

                    // Create the signal channel + the shared
                    // timestamps here so the validator's `JwksHandle` and
                    // the refresher (constructed in `Broker::start`) point at
                    // the same Arc-shared state. Channel capacity 1 +
                    // `try_send` on the producer ⇒ signals coalesce.
                    let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(1);
                    let last_successful = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
                    let last_on_demand = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

                    let handle = crabka_security::JwksHandle::new_with_refresher_handles(
                        crabka_security::Jwks::empty(),
                        last_successful.clone(),
                        signal_tx,
                    );

                    let mut v = crabka_security::SignedJwsValidator::new(handle);
                    if let Some(name) = oauth.principal_claim_name {
                        v.principal_claim_name = name;
                    }
                    if let Some(skew) = oauth.allowable_clock_skew_ms {
                        v.allowable_clock_skew_ms = skew;
                    }
                    v.valid_issuer = oauth.valid_issuer_uri;
                    v.expected_audience = oauth.expected_audience;
                    // JsonPath custom_claim_check + JWT typ check.
                    v.custom_claim_check
                        .clone_from(&custom_claim_check_compiled);
                    v.valid_token_type.clone_from(&oauth.valid_token_type);
                    // Claims mapping.
                    v.fallback_user_name_claim
                        .clone_from(&oauth.fallback_user_name_claim);
                    v.fallback_user_name_prefix
                        .clone_from(&oauth.fallback_user_name_prefix);
                    v.groups_claim.clone_from(&groups_claim_compiled);
                    v.groups_claim_delimiter
                        .clone_from(&oauth.groups_claim_delimiter);
                    // Hard cache-expiry threshold.
                    v.expiry_ms = oauth.jwks_expiry_seconds.map(|s| i64::from(s) * 1000);
                    cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Signed(v);
                    cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
                    if let Some(ms) = oauth.jwks_refresh_interval_ms {
                        cfg.oauthbearer_jwks_refresh_interval =
                            std::time::Duration::from_millis(ms);
                    }

                    // Park signal_rx + shared state for Broker::start.
                    *cfg.oauthbearer_jwks_signal_rx.lock().unwrap() = Some(signal_rx);
                    cfg.oauthbearer_jwks_last_successful_fetch_ms = last_successful;
                    cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
                    cfg.oauthbearer_jwks_min_on_demand_pause = std::time::Duration::from_secs(
                        u64::from(oauth.jwks_min_refresh_pause_seconds.unwrap_or(1)),
                    );
                    cfg.oauthbearer_jwks_ignore_key_use =
                        oauth.jwks_ignore_key_use.unwrap_or(false);
                }
                (None, Some(introspect_uri)) => {
                    // RFC 7662 introspection validator. The
                    // client secret is read from disk at config-load.
                    let client_id =
                        oauth.introspection_client_id.clone().unwrap_or_else(|| {
                            panic!(
                                "[oauthbearer]: introspection_endpoint_uri set but introspection_client_id is missing"
                            )
                        });
                    let secret_path = oauth
                        .introspection_client_secret_path
                        .clone()
                        .unwrap_or_else(|| {
                            panic!(
                                "[oauthbearer]: introspection_endpoint_uri set but introspection_client_secret_path is missing"
                            )
                        });
                    let client_secret = std::fs::read_to_string(&secret_path)
                        .unwrap_or_else(|e| {
                            panic!(
                                "[oauthbearer]: failed to read introspection_client_secret_path {}: {}",
                                secret_path.display(),
                                e
                            )
                        })
                        .trim_end_matches(['\n', '\r'])
                        .to_string();
                    let timeout = std::time::Duration::from_millis(
                        oauth.introspection_http_timeout_ms.unwrap_or(10_000),
                    );
                    let client = crate::oauth_introspection::ReqwestIntrospectionClient::new(
                        introspect_uri.clone(),
                        oauth.userinfo_endpoint_uri.clone(),
                        client_id,
                        client_secret,
                        oauth.idp_tls_trust.as_deref(),
                        timeout,
                    )
                    .unwrap_or_else(|e| {
                        panic!("[oauthbearer]: failed to build introspection client: {e}")
                    });
                    let v = crabka_security::IntrospectionValidator {
                        client,
                        principal_claim_name: oauth
                            .principal_claim_name
                            .clone()
                            .unwrap_or_else(|| "sub".into()),
                        // JsonPath custom_claim_check. No typ
                        // check for introspection (no JWT header).
                        custom_claim_check: custom_claim_check_compiled.clone(),
                        call_userinfo: oauth.userinfo_endpoint_uri.is_some(),
                        allowable_clock_skew_ms: oauth.allowable_clock_skew_ms.unwrap_or(30_000),
                        // Claims mapping.
                        fallback_user_name_claim: oauth.fallback_user_name_claim.clone(),
                        fallback_user_name_prefix: oauth.fallback_user_name_prefix.clone(),
                        groups_claim: groups_claim_compiled.clone(),
                        groups_claim_delimiter: oauth.groups_claim_delimiter.clone(),
                    };
                    cfg.oauthbearer_validator =
                        crabka_security::OAuthBearerValidator::Introspection(v);
                }
                (None, None) => {
                    // Unsecured-JWS validation (development only).
                    let mut v = crabka_security::UnsecuredJwsValidator::default();
                    if let Some(name) = oauth.principal_claim_name {
                        v.principal_claim_name = name;
                    }
                    if let Some(skew) = oauth.allowable_clock_skew_ms {
                        v.allowable_clock_skew_ms = skew;
                    }
                    // JsonPath custom_claim_check + JWT typ check.
                    v.custom_claim_check = custom_claim_check_compiled;
                    v.valid_token_type.clone_from(&oauth.valid_token_type);
                    // Claims mapping.
                    v.fallback_user_name_claim = oauth.fallback_user_name_claim;
                    v.fallback_user_name_prefix = oauth.fallback_user_name_prefix;
                    v.groups_claim = groups_claim_compiled;
                    v.groups_claim_delimiter = oauth.groups_claim_delimiter;
                    cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Unsecured(v);
                }
            }
        }

        // KIP-48: delegation-token master key + lifetime knobs.
        // `CRABKA_DELEGATION_TOKEN_SECRET_KEY` env var wins over the TOML
        // `secret_key`; when neither source provides a key the broker
        // leaves the field as `None` and the four DT RPCs return
        // `DELEGATION_TOKEN_AUTH_DISABLED`.
        let env_key = std::env::var("CRABKA_DELEGATION_TOKEN_SECRET_KEY").ok();
        let toml_key = self
            .delegation_token
            .as_ref()
            .and_then(|d| d.secret_key.clone());
        if let Some(k) = env_key.or(toml_key) {
            cfg.delegation_token_secret_key =
                Some(crabka_security::SecretBytes::new(k.into_bytes()));
        }
        if let Some(d) = &self.delegation_token {
            if let Some(ms) = d.max_lifetime_ms {
                cfg.delegation_token_max_lifetime_ms = ms;
            }
            if let Some(ms) = d.expiry_check_interval_ms {
                cfg.delegation_token_expiry_check_interval_ms = ms;
            }
            if let Some(ms) = d.default_renew_period_ms {
                cfg.delegation_token_default_renew_period_ms = ms;
            }
        }

        // Merge the TOML super-user list into the broker's
        // set (initially empty). `extend` over `clone_from` because a
        // future CLI/programmatic source may pre-populate entries that
        // we should preserve. The `[authorization]` block
        // below may overwrite this with its own super-user list.
        if let Some(vec) = self.super_users {
            cfg.super_users.extend(vec.iter().cloned());
        }

        // `[remote_storage]` enables tiered storage broker-
        // wide. Either `storage_dir` (local filesystem) or
        // `[remote_storage.s3]` (S3-compatible object store) selects the
        // backend. Both set → error.
        if let Some(rs) = &self.remote_storage {
            match (&rs.storage_dir, &rs.s3) {
                (Some(_), Some(_)) => {
                    return Err(FileConfigError::InvalidConfig(
                        "[remote_storage] cannot set both `storage_dir` (local) \
                         and `[remote_storage.s3]` (object store)"
                            .into(),
                    ));
                }
                (Some(dir), None) => {
                    cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
                        dir: std::path::PathBuf::from(dir),
                    });
                }
                (None, Some(s3)) => {
                    cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::S3(
                        crabka_remote_storage::S3Config {
                            bucket: s3.bucket.clone(),
                            region: s3.region.clone(),
                            prefix: s3.prefix.clone(),
                            endpoint: s3.endpoint.clone(),
                            access_key_id: s3.access_key_id.clone(),
                            secret_access_key: s3.secret_access_key.clone(),
                            allow_http: s3.allow_http,
                            multipart_threshold: s3
                                .multipart_threshold
                                .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD),
                            multipart_chunk_size: s3
                                .multipart_chunk_size
                                .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE),
                        },
                    ));
                }
                (None, None) => {}
            }

            // `[remote_storage.kafka_metadata]` opts in to the
            // topic-backed RLMM. Defaults to in-memory when absent.
            if let Some(km) = &rs.kafka_metadata {
                cfg.remote_log_metadata_kafka = Some(crate::config::KafkaRlmmConfig {
                    bootstrap: km.bootstrap.clone(),
                    num_partitions: km.num_partitions.unwrap_or(50),
                    replication: km.replication.unwrap_or(3),
                    snapshot_interval: crate::config::DEFAULT_RLMM_SNAPSHOT_INTERVAL,
                    snapshot_dir: cfg.log_dir.join("remote-log-metadata"),
                });
            }
        }

        // Pluggable cluster authorizer. When `[authorization]`
        // is present, its `super_users` list becomes the broker's
        // authoritative super-user set (overwriting whatever the
        // top-level list contributed above — operator O2
        // emits exactly one of the two sources). When absent, fall
        // through to the default [`AllowAllAuthorizer`] and leave
        // `cfg.super_users` as whatever the earlier extend produced.
        if let Some(a) = self.authorization.as_ref() {
            let auth_super_users: std::collections::HashSet<String> =
                a.super_users.iter().cloned().collect();
            cfg.super_users.clone_from(&auth_super_users);
            cfg.authorizer = match a.authz_type {
                AuthzType::AllowAll => std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
                AuthzType::Simple => std::sync::Arc::new(
                    crate::authorizer::SimpleAclAuthorizer::new(auth_super_users),
                ),
                AuthzType::Opa => {
                    let opa = a.opa.as_ref().ok_or_else(|| {
                        FileConfigError::MissingSection("[authorization.opa]".into())
                    })?;
                    let built = crate::authorizer::opa::OpaAuthorizer::new(
                        auth_super_users,
                        opa.url.clone(),
                        opa.allow_on_error,
                        opa.maximum_cache_size,
                        opa.expire_after_ms,
                    )
                    .map_err(|e| FileConfigError::OpaConfig(format!("{e:?}")))?;
                    std::sync::Arc::new(built)
                }
            };
        }

        // KRaft `process.roles`. Absent / empty leaves the BrokerConfig
        // default (`[Controller, Broker]`).
        if let Some(p) = &self.process
            && !p.roles.is_empty()
        {
            let mut roles = Vec::with_capacity(p.roles.len());
            for r in &p.roles {
                let role = match r.to_ascii_lowercase().as_str() {
                    "controller" => crate::config::NodeRole::Controller,
                    "broker" => crate::config::NodeRole::Broker,
                    other => {
                        return Err(FileConfigError::InvalidConfig(format!(
                            "unknown process.role `{other}` (expected `controller` or `broker`)"
                        )));
                    }
                };
                roles.push(role);
            }
            cfg.roles = roles;
        }

        Ok(())
    }
}

impl FileListener {
    #[must_use]
    pub fn into_spec(self) -> ListenerSpec {
        use crabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol,
            tls_config: self.tls_config.map(|t| BrokerTlsConfig {
                cert_chain_path: t.cert_path,
                private_key_path: t.key_path,
                trust_roots_path: None,
                client_ca_path: t.client_ca_path,
                client_auth: match t.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            }),
            sasl_mechanisms: self.sasl_config.map(|s| s.enabled_mechanisms),
        }
    }
}

#[cfg(test)]
mod listener_auth_tests {
    use super::*;

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.listeners.len(), 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        assert_eq!(
            data_tls.cert_path,
            std::path::PathBuf::from("/tls/broker.crt")
        );
        assert_eq!(
            data_tls.key_path,
            std::path::PathBuf::from("/tls/broker.key")
        );
        assert_eq!(
            data_tls.client_ca_path.as_deref(),
            Some(std::path::Path::new("/tls/clients-ca.crt"))
        );
        assert_eq!(data_tls.client_auth, FileClientAuthMode::Required);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Disabled" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert_eq!(
            sasl.enabled_mechanisms,
            vec![crabka_security::SaslMechanism::ScramSha512]
        );
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Serializes any test that mutates process-wide env vars. Tests in
    /// the same `cargo test` process run on multiple threads by default,
    /// and `set_var`/`remove_var` are global side-effects.
    static ENV_LOCK_CELL: OnceLock<Mutex<()>> = OnceLock::new();
    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK_CELL.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn empty_toml_round_trips() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        assert_eq!(cfg, FileConfig::default());
    }

    #[test]
    fn full_toml_round_trips() {
        let src = r#"
broker_id = 0
log_dir = "/var/lib/crabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
        assert_eq!(cfg.log_dir.as_deref(), Some("/var/lib/crabka/data"));
        assert_eq!(cfg.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(cfg.listeners.len(), 2);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].protocol, ListenerProtocol::Plaintext);
        assert_eq!(
            cfg.server_properties
                .get("log.retention.hours")
                .map(String::as_str),
            Some("24")
        );
    }

    #[test]
    fn unknown_top_level_key_is_ignored() {
        // Forward-compat: a newer config file shouldn't break older brokers.
        let src = r#"
broker_id = 0
some_future_field = "from-a-later-slice"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
    }

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "SaslSsl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.listeners[0].protocol, ListenerProtocol::SaslSsl);
    }

    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "Plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_config: None,
        };
        let spec = fl.into_spec();
        assert_eq!(spec.name, "X");
        assert_eq!(spec.advertised, "h:9094");
        assert_eq!(spec.protocol, ListenerProtocol::Plaintext);
    }

    #[test]
    fn apply_to_populates_listeners() {
        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert_eq!(cfg.listeners.len(), 1);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].advertised, "demo-0:9092");
        assert_eq!(cfg.inter_broker_listener_name, "PLAIN");
    }

    #[test]
    fn apply_to_does_not_clobber_non_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        // simulate CLI --broker-id 7 already applied
        let mut cfg = BrokerConfig {
            broker_id: 7,
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        // CLI value wins because it differs from default.
        assert_eq!(cfg.broker_id, 7);
    }

    #[test]
    fn apply_to_fills_in_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default(); // broker_id == default (1)

        file.apply_to(&mut cfg).unwrap();

        assert_eq!(cfg.broker_id, 42);
    }

    #[test]
    fn tls_keys_round_trip() {
        let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/crabka/broker-tls/0.crt"
key_path  = "/etc/crabka/broker-tls/0.key"
client_ca_path = "/etc/crabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
        assert_eq!(
            cfg.controller_listener_protocol,
            Some(ListenerProtocol::Ssl)
        );
        let tls = cfg.tls_config.expect("tls_config present");
        assert_eq!(
            tls.cert_path,
            std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt")
        );
        assert_eq!(tls.client_auth, FileClientAuthMode::Required);
    }

    #[test]
    fn tls_keys_absent_round_trips() {
        let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
        assert_eq!(cfg.controller_listener_protocol, None);
        assert!(cfg.tls_config.is_none());
    }

    #[test]
    fn apply_to_propagates_tls_config() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert_eq!(
            cfg.controller_listener_protocol,
            crabka_security::ListenerProtocol::Ssl
        );
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert_eq!(tls.cert_chain_path, std::path::PathBuf::from("/c"));
    }

    #[test]
    fn apply_to_oauthbearer_jwks_selects_signed_validator() {
        let src = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
valid_issuer_uri = "https://idp.example"
expected_audience = "kafka"
principal_claim_name = "client_id"
jwks_refresh_interval_ms = 60000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert_eq!(
            cfg.oauthbearer_jwks_endpoint.as_deref(),
            Some("https://idp.example/jwks")
        );
        assert_eq!(
            cfg.oauthbearer_jwks_refresh_interval,
            std::time::Duration::from_mins(1)
        );
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Signed(v) => {
                assert_eq!(v.valid_issuer.as_deref(), Some("https://idp.example"));
                assert_eq!(v.expected_audience.as_deref(), Some("kafka"));
                assert_eq!(v.principal_claim_name, "client_id");
            }
            other => panic!("jwks_endpoint_uri must select the Signed validator; got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_jwks_stays_unsecured() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert_eq!(v.allowable_clock_skew_ms, 5000);
            }
            other => {
                panic!("no jwks_endpoint_uri must keep the unsecured validator; got {other:?}")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
idp_tls_trust = "/etc/crabka/oauth/idp-ca.pem"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert_eq!(
            cfg.oauthbearer_idp_tls_trust.as_deref(),
            Some(std::path::Path::new("/etc/crabka/oauth/idp-ca.pem")),
        );
    }

    #[test]
    fn apply_to_oauthbearer_without_idp_tls_trust_leaves_field_none() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_idp_tls_trust.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "the-secret").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(matches!(
            cfg.oauthbearer_validator,
            crabka_security::OAuthBearerValidator::Introspection(_)
        ));
    }

    #[test]
    #[should_panic(expected = "mutually exclusive")]
    fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_id")]
    fn apply_to_oauthbearer_introspection_requires_client_id() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_secret_path")]
    fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
        let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    fn apply_to_oauthbearer_introspection_with_userinfo_sets_call_userinfo_true() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
userinfo_endpoint_uri = "https://idp.example/userinfo"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Introspection(v) => assert!(v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_introspection_without_userinfo_sets_call_userinfo_false() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Introspection(v) => assert!(!v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_empty_listeners_does_not_clear_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = BrokerConfig {
            listeners: vec![crate::config::ListenerSpec {
                name: "X".into(),
                bind_addr: "0.0.0.0:9094".parse().unwrap(),
                advertised: "h:9094".into(),
                protocol: crabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
            }],
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        assert_eq!(cfg.listeners.len(), 1);
        assert_eq!(cfg.listeners[0].name, "X");
    }

    #[test]
    fn remote_storage_section_enables_and_sets_dir() {
        let toml = r#"
[remote_storage]
storage_dir = "/var/lib/crabka/tier"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Local { dir }) => {
                assert_eq!(dir, std::path::PathBuf::from("/var/lib/crabka/tier"));
            }
            other => panic!("expected Local backend, got {other:?}"),
        }
    }

    #[test]
    fn no_remote_storage_section_leaves_backend_none() {
        let file: FileConfig = toml::from_str("broker_id = 1").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.remote_storage_backend.is_none());
        assert!(cfg.remote_log_metadata_kafka.is_none());
    }

    #[test]
    fn kafka_metadata_section_parses_with_defaults() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "127.0.0.1:9092"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let km = cfg.remote_log_metadata_kafka.expect("kafka rlmm config");
        assert_eq!(km.bootstrap, "127.0.0.1:9092");
        assert_eq!(km.num_partitions, 50);
        assert_eq!(km.replication, 3);
    }

    #[test]
    fn kafka_metadata_section_honors_overrides() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "broker-0:9094"
num_partitions = 8
replication = 1
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let km = cfg.remote_log_metadata_kafka.expect("kafka rlmm config");
        assert_eq!(km.bootstrap, "broker-0:9094");
        assert_eq!(km.num_partitions, 8);
        assert_eq!(km.replication, 1);
    }

    #[test]
    fn remote_storage_s3_section_parses() {
        let toml = r#"
[remote_storage.s3]
bucket = "crabka-prod"
region = "us-east-1"
prefix = "cluster-a"
endpoint = "http://minio:9000"
allow_http = true
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => {
                assert_eq!(s3.bucket, "crabka-prod");
                assert_eq!(s3.region, "us-east-1");
                assert_eq!(s3.prefix.as_deref(), Some("cluster-a"));
                assert_eq!(s3.endpoint.as_deref(), Some("http://minio:9000"));
                assert!(s3.allow_http);
                assert!(s3.access_key_id.is_none());
                // Multipart knobs default when the TOML omits them.
                assert_eq!(
                    s3.multipart_threshold,
                    crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD
                );
                assert_eq!(
                    s3.multipart_chunk_size,
                    crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE
                );
            }
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_s3_section_round_trips_multipart_overrides() {
        let toml = r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"
multipart_threshold = 8192
multipart_chunk_size = 5242880
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => {
                assert_eq!(s3.multipart_threshold, 8192);
                assert_eq!(s3.multipart_chunk_size, 5_242_880);
            }
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_local_and_s3_together_rejected() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.s3]
bucket = "b"
region = "us-east-1"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("cannot set both"),
            "expected backend-conflict error, got: {rendered}"
        );
    }

    #[test]
    fn delegation_token_section_parses_secret_key_and_defaults() {
        // Hold the lock so a concurrently-running env-var test can't
        // leak CRABKA_DELEGATION_TOKEN_SECRET_KEY into this assertion.
        // `temp_env::with_var_unset` removes the var for the duration
        // of the closure and restores the prior value on return —
        // safe against the workspace `forbid(unsafe_code)` lint.
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            assert_eq!(
                cfg.delegation_token_secret_key
                    .as_ref()
                    .map(|s| s.as_bytes().to_vec()),
                Some(b"abcdef".to_vec()),
            );
            // KIP-48 defaults: 7 days max lifetime, 1 hour sweep cadence,
            // 24 hour default renew period.
            assert_eq!(
                cfg.delegation_token_max_lifetime_ms,
                7 * 24 * 60 * 60 * 1_000
            );
            assert_eq!(
                cfg.delegation_token_expiry_check_interval_ms,
                60 * 60 * 1_000
            );
            assert_eq!(
                cfg.delegation_token_default_renew_period_ms,
                24 * 60 * 60 * 1_000
            );
        });
    }

    #[test]
    fn delegation_token_default_renew_period_ms_default_and_override() {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            // (1) When the TOML omits `default_renew_period_ms`, the config
            //     stays at the 24h KIP-48 default.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            assert_eq!(
                cfg.delegation_token_default_renew_period_ms,
                24 * 60 * 60 * 1_000,
                "absent default_renew_period_ms should leave the 24h default in place",
            );

            // (2) When the TOML sets it, the override wins.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
default_renew_period_ms = 7200000
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            assert_eq!(
                cfg.delegation_token_default_renew_period_ms, 7_200_000,
                "TOML default_renew_period_ms must override the default",
            );
        });
    }

    #[test]
    fn delegation_token_env_var_overrides_toml() {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var(
            "CRABKA_DELEGATION_TOKEN_SECRET_KEY",
            Some("env-wins"),
            || {
                let toml = r#"
[delegation_token]
secret_key = "toml-loses"
"#;
                let file: FileConfig = toml::from_str(toml).unwrap();
                let mut cfg = crate::config::BrokerConfig::default();
                file.apply_to(&mut cfg).unwrap();

                assert_eq!(
                    cfg.delegation_token_secret_key
                        .as_ref()
                        .map(|s| s.as_bytes().to_vec()),
                    Some(b"env-wins".to_vec()),
                );
            },
        );
    }

    #[test]
    fn delegation_token_absent_when_unset_anywhere() {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            let file: FileConfig = toml::from_str("").unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            assert!(cfg.delegation_token_secret_key.is_none());
            // Lifetime knobs stay at their defaults when no section is present.
            assert_eq!(
                cfg.delegation_token_max_lifetime_ms,
                7 * 24 * 60 * 60 * 1_000
            );
            assert_eq!(
                cfg.delegation_token_expiry_check_interval_ms,
                60 * 60 * 1_000
            );
            assert_eq!(
                cfg.delegation_token_default_renew_period_ms,
                24 * 60 * 60 * 1_000
            );
        });
    }

    #[test]
    fn super_users_toml_populates_broker_config_set() {
        let toml = r#"
super_users = ["ANONYMOUS", "admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.super_users.contains("ANONYMOUS"));
        assert!(cfg.super_users.contains("admin"));
        assert_eq!(cfg.super_users.len(), 2);
    }

    // `[authorization]` TOML section → `Arc<dyn Authorizer>`.

    fn test_principal(name: &str) -> crabka_security::Principal {
        crabka_security::Principal {
            name: name.into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    #[test]
    fn authorization_section_simple_builds_simple_acl_authorizer() {
        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        let toml = r#"
[authorization]
type = "simple"
super_users = ["admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.super_users.contains("admin"),
            "[authorization].super_users must populate BrokerConfig.super_users for act-as parity"
        );
        // `admin` is a super-user → bypass returns Allow even with an
        // empty MetadataImage (no ACLs). This is the SimpleAclAuthorizer
        // contract; AllowAllAuthorizer would also Allow, but the
        // default-deny SimpleAcl behavior is exercised by the
        // explicit `type = "simple"` branch's own unit tests.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let admin = test_principal("admin");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &admin,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert_eq!(
            cfg.authorizer.authorize(&img, &req),
            AuthorizationResult::Allow
        );

        // Non-super-user with no matching ACL → Deny (proves we got
        // SimpleAcl, not AllowAll).
        let alice = test_principal("alice");
        let req_alice = AuthorizationRequest {
            principal: &alice,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert_eq!(
            cfg.authorizer.authorize(&img, &req_alice),
            AuthorizationResult::Deny,
            "type=simple must default-deny non-super-users with no matching ACL"
        );
    }

    #[test]
    fn authorization_section_opa_builds_opa_authorizer() {
        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        // `OpaAuthorizer::new` captures `Handle::try_current()` — needs
        // an active tokio runtime. `Runtime::new()` defaults to
        // multi-thread, which the OPA `block_in_place` bridge requires
        // for any actual HTTP call (super-user bypass below sidesteps
        // that).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let toml = r#"
[authorization]
type = "opa"
super_users = ["ANONYMOUS"]

[authorization.opa]
url = "http://opa.invalid:8181/v1/data/k/a"
allow_on_error = false
maximum_cache_size = 100
expire_after_ms = 60000
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            assert!(cfg.super_users.contains("ANONYMOUS"));

            // Smoke-check via the super-user bypass — no HTTP call is
            // made (and `opa.invalid` deliberately doesn't resolve).
            let img = MetadataImage::new(uuid::Uuid::nil());
            let anon = test_principal("ANONYMOUS");
            let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AuthorizationRequest {
                principal: &anon,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert_eq!(
                cfg.authorizer.authorize(&img, &req),
                AuthorizationResult::Allow,
                "OPA super-user bypass must short-circuit before any HTTP call"
            );
        });
    }

    #[test]
    fn authorization_section_absent_defaults_to_allow_all() {
        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        // Default authorizer is AllowAll — anyone gets Allow, including
        // a principal who isn't in any super-user set.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let anyone = test_principal("anyone");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &anyone,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert_eq!(
            cfg.authorizer.authorize(&img, &req),
            AuthorizationResult::Allow
        );
    }

    #[test]
    fn process_roles_controller_only_from_toml() {
        let toml = r#"
            [process]
            roles = ["controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(cfg.roles, vec![crate::config::NodeRole::Controller]);
    }

    #[test]
    fn process_roles_both_from_toml() {
        let toml = r#"
            [process]
            roles = ["broker", "controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(
            cfg.roles,
            vec![
                crate::config::NodeRole::Broker,
                crate::config::NodeRole::Controller
            ]
        );
    }

    #[test]
    fn process_roles_rejects_unknown_role() {
        let toml = r#"
            [process]
            roles = ["wizard"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        let err = fc.apply_to(&mut cfg).expect_err("unknown role rejected");
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }

    #[test]
    fn process_section_absent_leaves_default_roles() {
        let fc: FileConfig = toml::from_str("").expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(
            cfg.roles,
            vec![
                crate::config::NodeRole::Controller,
                crate::config::NodeRole::Broker
            ]
        );
    }

    #[test]
    fn apply_to_parses_rack_and_replica_selector() {
        use crate::replica_selector::ReplicaSelectorKind;
        let src = r#"
broker_id = 0
rack = "az-1"
replica_selector = "rack-aware"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = crate::config::BrokerConfig::default();
        cfg.apply_to(&mut broker).expect("apply");
        assert_eq!(broker.rack.as_deref(), Some("az-1"));
        assert_eq!(broker.replica_selector, ReplicaSelectorKind::RackAware);
    }

    #[test]
    fn apply_to_rejects_unknown_replica_selector() {
        let src = r#"
broker_id = 0
replica_selector = "nonsense"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = crate::config::BrokerConfig::default();
        assert!(cfg.apply_to(&mut broker).is_err());
    }
}
