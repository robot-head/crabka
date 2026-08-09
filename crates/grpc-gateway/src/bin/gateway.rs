//! `crabka-grpc-gateway` binary entry point.
//!
//! The binary parses CLI flags, builds the Connect-RPC router and a minimal
//! health router, then serves both on the configured listen address.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context as _;
use clap::Parser;
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_grpc_gateway::{
    codec::{RawCodec, RecordCodec},
    config::{
        AuthzSettings, BearerSettings, GatewayConfig, GatewayRuntimeConfig, TlsSettings,
        validate_dedup_window,
    },
    config_value::{PartitionCount, PositiveI16, PositiveU32},
    dedup::{
        DedupEngine,
        membership::{MembershipPublisher, MembershipStore},
        store::DedupStore,
        topic::{
            ensure_dedup_topic_with_policy, ensure_membership_topic_with_policy,
            internal_topic_policy,
        },
    },
    forward::{self, Forwarder},
    health::{self, Readiness},
    produce::ProduceCore,
    schema::{client::SchemaRegistryClient, codec::SchemaRegistryCodec},
    state::AppState,
};
use crabka_units::{parse, prelude::*};
use tokio_util::sync::CancellationToken;
use tracing::info;

// ── CLI ────────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "crabka-grpc-gateway",
    version,
    about = "gRPC / Connect-RPC + HTTP gateway into Crabka topics"
)]
struct Args {
    /// `host:port,host:port,...` bootstrap brokers.
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(
        long,
        env = "CRABKA_GRPC_GATEWAY_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_GRPC_GATEWAY_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,

    /// Bind address for the Connect-RPC + health server.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_LISTEN_ADDR",
        default_value = "0.0.0.0:9500"
    )]
    listen_addr: SocketAddr,

    /// `client.id` for the native clients this gateway opens.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_CLIENT_ID",
        default_value = "crabka-grpc-gateway"
    )]
    client_id: String,

    /// Internal dedup topic name.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_TOPIC",
        default_value = "__crabka_grpc_dedup"
    )]
    dedup_topic: String,

    /// Dedup topic partition count.
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_PARTITIONS", default_value = "8")]
    dedup_partitions: PartitionCount,

    /// Dedup window, e.g. `1h`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_WINDOW",
        default_value = "1h",
        value_parser = parse::positive_time
    )]
    dedup_window: Time,

    /// Consumer group used to divide dedup-topic ownership between replicas.
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_OWNERSHIP_GROUP")]
    dedup_ownership_group: Option<String>,

    /// Transactional id prefix for the dedup path.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_TXN_PREFIX",
        default_value = "crabka-gw-dedup"
    )]
    dedup_txn_id_prefix: String,

    /// Address peers reach this gateway at, for example `gw-0.gw:9500`.
    /// Required for active-active forwarding. It must be routable from other
    /// replicas.
    #[arg(long, env = "CRABKA_GATEWAY_ADVERTISED_ADDR")]
    advertised_addr: String,

    /// Internal membership / owner-routing topic.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_MEMBERSHIP_TOPIC",
        default_value = "__crabka_grpc_gateway_membership"
    )]
    membership_topic: String,

    /// Server cert chain (PEM). Enables TLS when set together with --tls-key.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,
    /// Server private key (PEM).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,
    /// CA(s) that verify incoming client certs (mTLS). Required if --tls-client-auth != disabled.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CLIENT_CA")]
    tls_client_ca: Option<std::path::PathBuf>,
    /// Client-cert mode: disabled | optional | required.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_TLS_CLIENT_AUTH",
        default_value = "disabled"
    )]
    tls_client_auth: String,
    /// CA(s) the forwarder trusts for peer gateway server certs. Defaults to --tls-client-ca.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_TRUST_ROOTS")]
    tls_trust_roots: Option<std::path::PathBuf>,
    /// Cert hot-reload poll interval, e.g. `30s`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_TLS_RELOAD_INTERVAL",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    tls_reload_interval: Time,

    /// Authorizer mode: off | simple.
    #[arg(long, env = "CRABKA_GATEWAY_AUTHZ", default_value = "off")]
    authz: String,
    /// Comma-separated super-user principal names that bypass ACL checks.
    #[arg(long, env = "CRABKA_GATEWAY_AUTHZ_SUPER_USERS", default_value = "")]
    authz_super_users: String,
    /// ACL-cache refresh interval, e.g. `30s`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_ACL_REFRESH_INTERVAL",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    acl_refresh_interval: Time,

    /// Bearer-token mode: off | unsecured.
    #[arg(long, env = "CRABKA_GATEWAY_BEARER", default_value = "off")]
    bearer: String,
    /// JWT claim name to use as the principal name.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_BEARER_PRINCIPAL_CLAIM",
        default_value = "sub"
    )]
    bearer_principal_claim: String,
    /// Allowable clock skew for bearer token timestamps, e.g. `30s`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_BEARER_ALLOWABLE_CLOCK_SKEW",
        default_value = "30s",
        value_parser = parse::non_negative_time
    )]
    bearer_allowable_clock_skew: Time,

    // ── Runtime policy ──────────────────────────────────────────────────────
    #[arg(long, env = "CRABKA_GATEWAY_INTERNAL_TOPIC_REPLICATION_FACTOR")]
    internal_topic_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "CRABKA_GATEWAY_INTERNAL_TOPIC_ALLOW_REPLICATION_FALLBACK")]
    internal_topic_allow_replication_fallback: Option<bool>,
    /// Internal-topic creation timeout, e.g. `10s`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_INTERNAL_TOPIC_CREATE_TIMEOUT",
        value_parser = parse::positive_time
    )]
    internal_topic_create_timeout: Option<Time>,
    /// Internal-topic `segment.ms` roll interval, e.g. `1m`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_INTERNAL_TOPIC_SEGMENT",
        value_parser = parse::positive_time
    )]
    internal_topic_segment: Option<Time>,
    /// Internal-topic `min.cleanable.dirty.ratio`, e.g. `1%`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_INTERNAL_TOPIC_MIN_CLEANABLE_DIRTY_RATIO",
        value_parser = parse::unit_ratio
    )]
    internal_topic_min_cleanable_dirty_ratio: Option<Ratio>,
    /// Consumer poll timeout, e.g. `500ms`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_CONSUMER_POLL_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_poll_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_GATEWAY_OWNERSHIP_WARMUP_EMPTY_POLLS")]
    ownership_warmup_empty_polls: Option<PositiveU32>,
    /// Readiness-watcher poll interval, e.g. `250ms`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_READINESS_POLL_INTERVAL",
        value_parser = parse::positive_time
    )]
    readiness_poll_interval: Option<Time>,
    /// Maximum accepted body size for generic HTTP produce requests, e.g. `2MiB`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_PRODUCE_MAX_BODY",
        value_parser = parse::positive_byte_size
    )]
    produce_max_body: Option<ByteSize>,
    /// Maximum accepted body size for internal forwarding requests, e.g. `2MiB`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_FORWARD_MAX_BODY",
        value_parser = parse::positive_byte_size
    )]
    forward_max_body: Option<ByteSize>,
    /// How long a resolved latest-schema id stays cached, e.g. `5s`.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_SCHEMA_REGISTRY_LATEST_CACHE_TTL",
        value_parser = parse::positive_time
    )]
    schema_registry_latest_cache_ttl: Option<Time>,
    #[arg(long, env = "CRABKA_GATEWAY_SCHEMA_REGISTRY_FRAME_RAW")]
    schema_registry_frame_raw: Option<bool>,

    /// CA cert (PEM) the gateway trusts when connecting to the broker over TLS.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_CA")]
    broker_tls_ca: Option<std::path::PathBuf>,
    /// Client cert chain (PEM) the gateway presents to the broker for mTLS.
    /// Must be set together with `--broker-tls-key`.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_CERT")]
    broker_tls_cert: Option<std::path::PathBuf>,
    /// Client private key (PEM) for mTLS to the broker.
    /// Must be set together with `--broker-tls-cert`.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_KEY")]
    broker_tls_key: Option<std::path::PathBuf>,
    /// SNI / server-name for the TLS handshake with the broker.
    /// Required when `--broker-tls-cert` and `--broker-tls-key` are set.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_SERVER_NAME")]
    broker_tls_server_name: Option<String>,

    /// Optional TOML file defining `[[webhooks.endpoints]]` for HTTP webhook inbound.
    #[arg(long, env = "CRABKA_GATEWAY_WEBHOOKS_CONFIG")]
    webhooks_config: Option<std::path::PathBuf>,

    /// Optional TOML file defining `[[subscriptions]]` for HTTP webhook outbound.
    #[arg(long, env = "CRABKA_GATEWAY_OUTBOUND_WEBHOOKS_CONFIG")]
    outbound_webhooks_config: Option<std::path::PathBuf>,

    /// Base URL of a Confluent-compatible Schema Registry, for example
    /// `http://schema-registry:8081`. When set, `SchemaRegistryCodec` encodes
    /// and decodes records. When absent, the gateway uses the identity
    /// `RawCodec`.
    #[arg(long, env = "CRABKA_GATEWAY_SCHEMA_REGISTRY_URL")]
    schema_registry_url: Option<String>,
}

// ── main ───────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Step 1: install the ring crypto provider before any TLS work.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let args = Args::parse();
    let runtime = args.validate_protocol_runtime()?;

    let otlp = crabka_telemetry::OtlpConfig::from_env(
        |k| std::env::var(k).ok(),
        &args.client_id,
        env!("CARGO_PKG_VERSION"),
        "crabka-grpc-gateway",
    )?;
    let telemetry = crabka_telemetry::init(
        otlp,
        "crabka_grpc_gateway=info,info",
        "info,gateway::audit=debug",
        "crabka-grpc-gateway",
    )
    .expect("telemetry init");
    info!(
        listen = %args.listen_addr,
        bootstrap = %args.bootstrap_servers,
        "crabka-grpc-gateway starting"
    );

    // Step 3: build TlsSettings from CLI args (requires both cert+key or neither).
    let tls = match (args.tls_cert.clone(), args.tls_key.clone()) {
        (Some(cert_chain_path), Some(private_key_path)) => {
            let client_auth = match args.tls_client_auth.as_str() {
                "disabled" => crabka_grpc_gateway::config::ClientAuthMode::Disabled,
                "optional" => crabka_grpc_gateway::config::ClientAuthMode::Optional,
                "required" => crabka_grpc_gateway::config::ClientAuthMode::Required,
                other => anyhow::bail!("invalid --tls-client-auth: {other}"),
            };
            Some(TlsSettings {
                cert_chain_path,
                private_key_path,
                trust_roots_path: args
                    .tls_trust_roots
                    .clone()
                    .or_else(|| args.tls_client_ca.clone()),
                client_ca_path: args.tls_client_ca.clone(),
                client_auth,
                reload_interval: args.tls_reload_interval,
            })
        }
        (None, None) => None,
        _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
    };

    let broker_security = build_broker_security(
        args.broker_tls_cert.as_ref(),
        args.broker_tls_key.as_ref(),
        args.broker_tls_ca.as_ref(),
        args.broker_tls_server_name.as_ref(),
    )?;
    let authz = build_authz_settings(&args)?;
    let webhooks = load_webhooks(&args)?;
    let outbound = load_outbound(&args)?;

    let dedup_ownership_group = args.resolved_dedup_ownership_group();
    let config = GatewayConfig {
        bootstrap: args.bootstrap_servers.clone(),
        listen_addr: args.listen_addr,
        client_id: args.client_id.clone(),
        dedup_topic: args.dedup_topic.clone(),
        dedup_partitions: args.dedup_partitions.into_value(),
        dedup_window: args.dedup_window,
        dedup_ownership_group,
        dedup_txn_id_prefix: args.dedup_txn_id_prefix.clone(),
        advertised_addr: args.advertised_addr.clone(),
        membership_topic: args.membership_topic.clone(),
        tls: tls.clone(),
        broker_security,
        authz,
        webhooks,
        outbound,
        schema_registry_url: args.schema_registry_url.clone(),
        runtime,
    };

    let bearer = build_bearer(&args)?;

    let result = Box::pin(run(config, bearer)).await;
    telemetry.shutdown();
    result
}

/// Build [`AuthzSettings`] from CLI args, or `None` when `--authz off`.
fn build_authz_settings(args: &Args) -> anyhow::Result<Option<AuthzSettings>> {
    match args.authz.as_str() {
        "off" => Ok(None),
        "simple" => {
            let super_users: Vec<String> = args
                .authz_super_users
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            Ok(Some(AuthzSettings {
                super_users,
                acl_refresh: args.acl_refresh_interval,
            }))
        }
        other => anyhow::bail!("invalid --authz: {other}"),
    }
}

/// Build [`ClientSecurity`] for outbound broker connections from the four
/// `--broker-tls-*` flags.
///
/// - Both cert and key present ⇒ mTLS. SNI is required in that case.
/// - Both absent ⇒ plaintext, which is `None`.
/// - Exactly one present ⇒ configuration error.
/// - CA only, with no cert and no key ⇒ one-way TLS with the given CA.
fn build_broker_security(
    cert: Option<&PathBuf>,
    key: Option<&PathBuf>,
    ca: Option<&PathBuf>,
    sni: Option<&String>,
) -> anyhow::Result<Option<crabka_client_core::security::ClientSecurity>> {
    use crabka_client_core::security::{ClientSecurity, TlsConnectorConfig};
    use crabka_security::ListenerProtocol;

    match (cert, key) {
        (Some(cert_path), Some(key_path)) => {
            let server_name = sni
                .cloned()
                .context("--broker-tls-server-name required with broker TLS")?;
            Ok(Some(ClientSecurity {
                protocol: ListenerProtocol::Ssl,
                tls: Some(TlsConnectorConfig {
                    trust_roots_pem: ca.cloned(),
                    server_name,
                    client_identity: Some((cert_path.clone(), key_path.clone())),
                }),
                sasl: None,
                sasl_host: None,
            }))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!("--broker-tls-cert and --broker-tls-key must be set together"),
    }
}

/// Load and compile the optional webhooks TOML config file.
fn load_webhooks(
    args: &Args,
) -> anyhow::Result<
    std::collections::HashMap<String, crabka_grpc_gateway::webhook_config::CompiledWebhook>,
> {
    match args.webhooks_config.as_ref() {
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("read webhooks config {}: {e}", path.display()))?;
            let file: crabka_grpc_gateway::webhook_config::WebhooksFile =
                toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parse webhooks config: {e}"))?;
            file.compile()
                .map_err(|e| anyhow::anyhow!("webhooks config: {e}"))
        }
        None => Ok(std::collections::HashMap::new()),
    }
}

/// Load and compile the optional outbound webhook subscriptions TOML config file.
fn load_outbound(
    args: &Args,
) -> anyhow::Result<Vec<crabka_grpc_gateway::outbound_config::CompiledSubscription>> {
    match args.outbound_webhooks_config.as_ref() {
        Some(path) => {
            let raw = std::fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!("read outbound webhooks config {}: {e}", path.display())
            })?;
            let file: crabka_grpc_gateway::outbound_config::OutboundFile = toml::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("parse outbound webhooks config: {e}"))?;
            file.compile()
                .map_err(|e| anyhow::anyhow!("outbound config: {e}"))
        }
        None => Ok(Vec::new()),
    }
}

/// Build an optional [`BearerValidator`] extension from CLI args.
fn build_bearer(
    args: &Args,
) -> anyhow::Result<Option<crabka_grpc_gateway::authz::auth_layer::BearerValidator>> {
    match args.bearer.as_str() {
        "off" => Ok(None),
        "unsecured" => {
            let v = BearerSettings {
                principal_claim_name: args.bearer_principal_claim.clone(),
                allowable_clock_skew: args.bearer_allowable_clock_skew,
            }
            .build()
            .map_err(|e| anyhow::anyhow!("bearer: {e}"))?;
            Ok(Some(
                crabka_grpc_gateway::authz::auth_layer::BearerValidator(Arc::new(v)),
            ))
        }
        other => anyhow::bail!("invalid --bearer: {other}"),
    }
}

impl Args {
    fn resolved_dedup_ownership_group(&self) -> String {
        self.dedup_ownership_group
            .clone()
            .unwrap_or_else(|| format!("{}-dedup-owners", self.client_id))
    }

    fn runtime_config(&self) -> GatewayRuntimeConfig {
        let mut runtime = GatewayRuntimeConfig {
            client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity::new(
                self.client_dispatch_queue_capacity,
            )
            .expect("validated by clap"),
            client_frame_max: ClientFrameMax::try_from(self.client_frame_max)
                .expect("validated by clap"),
            ..GatewayRuntimeConfig::default()
        };
        if let Some(value) = self.internal_topic_replication_factor {
            runtime.internal_topic_replication_factor = value.into_value();
        }
        if let Some(value) = self.internal_topic_allow_replication_fallback {
            runtime.internal_topic_allow_replication_fallback = value;
        }
        if let Some(value) = self.internal_topic_create_timeout {
            runtime.internal_topic_create_timeout = value;
        }
        if let Some(value) = self.internal_topic_segment {
            runtime.internal_topic_segment = value;
        }
        if let Some(value) = self.internal_topic_min_cleanable_dirty_ratio {
            runtime.internal_topic_min_cleanable_dirty_ratio = value;
        }
        if let Some(value) = self.consumer_poll_timeout {
            runtime.consumer_poll_timeout = value;
        }
        if let Some(value) = self.ownership_warmup_empty_polls {
            runtime.ownership_warmup_empty_polls = value.into_value();
        }
        if let Some(value) = self.readiness_poll_interval {
            runtime.readiness_poll_interval = value;
        }
        if let Some(value) = self.produce_max_body {
            runtime.produce_max_body = value;
        }
        if let Some(value) = self.forward_max_body {
            runtime.forward_max_body = value;
        }
        if let Some(value) = self.schema_registry_latest_cache_ttl {
            runtime.schema_registry_latest_cache_ttl = value;
        }
        if let Some(value) = self.schema_registry_frame_raw {
            runtime.schema_registry_frame_raw = value;
        }
        runtime
    }

    fn validate_protocol_runtime(&self) -> anyhow::Result<GatewayRuntimeConfig> {
        let runtime = self.runtime_config();
        runtime
            .validate_protocol_units()
            .map_err(anyhow::Error::msg)?;
        validate_dedup_window(self.dedup_window).map_err(anyhow::Error::msg)?;
        Ok(runtime)
    }
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

async fn run(
    config: GatewayConfig,
    bearer: Option<crabka_grpc_gateway::authz::auth_layer::BearerValidator>,
) -> anyhow::Result<()> {
    // Ensure internal topics exist before opening any producer/consumer.
    let topic_policy = internal_topic_policy(&config.runtime);
    ensure_dedup_topic_with_policy(
        &config.bootstrap,
        &config.dedup_topic,
        config.dedup_partitions,
        config.dedup_window,
        &topic_policy,
        config.broker_security.clone(),
        &config.runtime,
    )
    .await?;
    ensure_membership_topic_with_policy(
        &config.bootstrap,
        &config.membership_topic,
        &topic_policy,
        config.broker_security.clone(),
        &config.runtime,
    )
    .await?;

    let node_id = uuid::Uuid::new_v4().to_string();
    let store = Arc::new(DedupStore::new_with_policy(
        config.dedup_partitions,
        &config.runtime,
    ));
    let readiness = Readiness::new();
    let shutdown = CancellationToken::new();

    // Membership: tail the routing table, and install the publisher BEFORE the
    // ownership consumer starts so its first assignment is published.
    let membership = Arc::new(MembershipStore::new_with_policy(&config.runtime));
    spawn_membership_reader(&config, &membership, &node_id, &shutdown);
    let publisher = Arc::new(
        MembershipPublisher::new_with_policy(
            &config.bootstrap,
            &format!("{}-membership-pub", config.client_id),
            node_id.clone(),
            config.advertised_addr.clone(),
            config.membership_topic.clone(),
            config.broker_security.clone(),
            &config.runtime,
        )
        .await?,
    );
    store.set_membership(publisher);

    spawn_ownership_consumer(&config, &store, &shutdown);
    spawn_readiness_watcher(
        store.clone(),
        readiness.clone(),
        config.runtime.readiness_poll_interval,
    );

    let engine = Arc::new(DedupEngine::new_with_policy(
        &config.bootstrap,
        &config.client_id,
        &config.dedup_txn_id_prefix,
        config.dedup_topic.clone(),
        config.dedup_partitions,
        store,
        (config.broker_security.clone(), &config.runtime),
    ));

    // Step 4: build the forwarder — mTLS https when TLS is configured, plaintext http otherwise.
    let forwarder = match config.tls.as_ref() {
        Some(t) => {
            let client_cfg = t
                .to_security()
                .build_client_config_with_identity()
                .map_err(|e| anyhow::anyhow!("build forward client tls: {e}"))?;
            Arc::new(Forwarder::with_tls(client_cfg)?)
        }
        None => Arc::new(Forwarder::new()),
    };

    // Build the shared codec once: SchemaRegistryCodec when a URL is set,
    // RawCodec (identity pass-through) otherwise.
    let codec: Arc<dyn RecordCodec> = match &config.schema_registry_url {
        Some(url) => Arc::new(
            build_schema_registry_codec(url, &config.runtime)
                .map_err(|e| anyhow::anyhow!("schema registry client: {e}"))?,
        ),
        None => Arc::new(RawCodec),
    };

    // Spawn outbound delivery tasks with the shared codec (so `decode_to_json`
    // subscriptions can de-frame Confluent-framed values to JSON).
    spawn_outbound_subscriptions(&config, &shutdown, codec.clone()).await?;

    let produce = Box::pin(ProduceCore::new_with_policy(
        &config.bootstrap,
        &config.client_id,
        codec.clone(),
        config.broker_security.clone(),
        &config.runtime,
    ))
    .await?
    .with_dedup(engine)
    .with_forwarding(
        membership.clone(),
        forwarder,
        config.advertised_addr.clone(),
    );

    let gateway_authz = build_gateway_authz(&config, &shutdown, config.broker_security.clone());

    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(config.clone()),
        authz: gateway_authz,
        codec,
        queue: Arc::default(),
    });

    let app = crabka_grpc_gateway::router(state.clone())
        .merge(health::router(readiness))
        .merge(forward::forward_router(state.clone()))
        .merge(crabka_grpc_gateway::webhook::webhook_router(state.clone()))
        .merge(crabka_grpc_gateway::metrics::router())
        .layer(axum::middleware::from_fn(
            crabka_grpc_gateway::authz::auth_layer::resolve_principal,
        ));
    let app = match bearer {
        Some(bv) => app.layer(axum::Extension(bv)),
        None => app,
    };

    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.cancel();
    });

    let tls_dynamic = match config.tls.as_ref() {
        Some(t) => Some(
            crabka_grpc_gateway::serve::build_and_watch_tls(
                t.to_security(),
                t.reload_interval,
                shutdown.clone(),
            )
            .map_err(|e| anyhow::anyhow!("build tls server config: {e}"))?,
        ),
        None => None,
    };

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, tls = tls_dynamic.is_some(), "gateway listening");
    crabka_grpc_gateway::serve::serve(listener, app, tls_dynamic, shutdown).await?;
    Ok(())
}

fn build_schema_registry_codec(
    url: &str,
    runtime: &GatewayRuntimeConfig,
) -> Result<SchemaRegistryCodec, crabka_grpc_gateway::codec::CodecError> {
    let client =
        SchemaRegistryClient::new_with_policy(url, runtime.schema_registry_latest_cache_ttl)?;
    Ok(SchemaRegistryCodec::new(
        Arc::new(client),
        runtime.schema_registry_frame_raw,
    ))
}

/// Build the [`GatewayAuthz`] and, when `config.authz` is present, spawn the
/// ACL-cache refresh task.
fn build_gateway_authz(
    config: &GatewayConfig,
    shutdown: &CancellationToken,
    broker_security: Option<crabka_client_core::security::ClientSecurity>,
) -> Arc<crabka_grpc_gateway::authz::GatewayAuthz> {
    let authorizer: Arc<dyn crabka_authz::Authorizer> = match config.authz.as_ref() {
        Some(a) => Arc::new(crabka_authz::SimpleAclAuthorizer::new(
            a.super_users.iter().cloned().collect(),
        )),
        None => Arc::new(crabka_authz::AllowAllAuthorizer),
    };
    let gateway_authz = Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(authorizer));
    if let Some(a) = config.authz.as_ref() {
        tokio::spawn(gateway_authz.clone().run_acl_refresh_with_policy(
            config.bootstrap.clone(),
            a.acl_refresh,
            shutdown.clone(),
            broker_security,
            config.runtime.clone(),
        ));
    }
    gateway_authz
}

fn spawn_membership_reader(
    config: &GatewayConfig,
    membership: &Arc<MembershipStore>,
    node_id: &str,
    shutdown: &CancellationToken,
) {
    let membership = membership.clone();
    let bootstrap = config.bootstrap.clone();
    let client_id = format!("{}-membership", config.client_id);
    let topic = config.membership_topic.clone();
    let group = format!("__crabka_grpc_gateway_membership_reader-{node_id}");
    let shutdown = shutdown.clone();
    let security = config.broker_security.clone();
    let policy = config.runtime.clone();
    tokio::spawn(async move {
        if let Err(e) = membership
            .run_membership_with_policy(
                bootstrap,
                client_id,
                topic,
                group,
                shutdown,
                (security, policy),
            )
            .await
        {
            tracing::error!(error = %e, "membership reader exited with error");
        }
    });
}

fn spawn_ownership_consumer(
    config: &GatewayConfig,
    store: &Arc<DedupStore>,
    shutdown: &CancellationToken,
) {
    let store = store.clone();
    let (bootstrap, client_id, dedup_topic, ownership_group) = ownership_consumer_inputs(config);
    let shutdown = shutdown.clone();
    let security = config.broker_security.clone();
    let policy = config.runtime.clone();
    tokio::spawn(async move {
        if let Err(e) = store
            .run_ownership_with_policy(
                bootstrap,
                client_id,
                dedup_topic,
                ownership_group,
                shutdown,
                (security, policy),
            )
            .await
        {
            tracing::error!(error = %e, "dedup ownership task exited with error");
        }
    });
}

fn ownership_consumer_inputs(config: &GatewayConfig) -> (String, String, String, String) {
    (
        config.bootstrap.clone(),
        format!("{}-dedup-owner", config.client_id),
        config.dedup_topic.clone(),
        config.dedup_ownership_group.clone(),
    )
}

fn spawn_readiness_watcher(store: Arc<DedupStore>, readiness: Readiness, poll_interval: Time) {
    tokio::spawn(async move {
        loop {
            if store.has_warmed_once() {
                readiness.set_ready();
                break;
            }
            tokio::time::sleep(poll_interval.to_std()).await;
        }
    });
}

/// Build the DLQ producer and spawn one delivery task per outbound subscription.
///
/// This function does nothing when `config.outbound` is empty, so a default
/// deployment carries no overhead.
async fn spawn_outbound_subscriptions(
    config: &GatewayConfig,
    shutdown: &CancellationToken,
    codec: Arc<dyn RecordCodec>,
) -> anyhow::Result<()> {
    if config.outbound.is_empty() {
        return Ok(());
    }
    let dlq_producer = Arc::new(
        crabka_client_producer::Producer::builder()
            .bootstrap(config.bootstrap.clone())
            .client_id(format!("{}-outbound-dlq", config.client_id))
            .dispatch_queue_capacity(config.runtime.client_dispatch_queue_capacity.get())
            .frame_max(config.runtime.client_frame_max.size())
            .enable_idempotence(true)
            .acks(crabka_client_producer::Acks::All)
            .maybe_security(config.broker_security.clone())
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("build outbound dlq producer: {e}"))?,
    );
    for sub in &config.outbound {
        spawn_outbound_delivery(sub, config, dlq_producer.clone(), shutdown, codec.clone());
    }
    Ok(())
}

fn spawn_outbound_delivery(
    sub: &crabka_grpc_gateway::outbound_config::CompiledSubscription,
    config: &GatewayConfig,
    dlq_producer: Arc<crabka_client_producer::Producer>,
    shutdown: &CancellationToken,
    codec: Arc<dyn RecordCodec>,
) {
    let sub = sub.clone();
    let bootstrap = config.bootstrap.clone();
    let client_id = format!("{}-outbound-{}", config.client_id, sub.name);
    let shutdown = shutdown.clone();
    let security = config.broker_security.clone();
    let poll_timeout = config.runtime.consumer_poll_timeout;
    let policy = config.runtime.clone();
    tokio::spawn(async move {
        if let Err(e) = crabka_grpc_gateway::outbound::run_subscription_with_policy(
            sub,
            bootstrap,
            client_id,
            dlq_producer,
            shutdown,
            (security, poll_timeout, policy),
            codec,
        )
        .await
        {
            tracing::error!(error = %e, "outbound delivery task exited with error");
        }
    });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::{assert, check};
    use clap::Parser;
    use crabka_security::ListenerProtocol;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
        ])
        .unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
            "--client-dispatch-queue-capacity=7",
            "--client-frame-max=32KiB",
        ])
        .unwrap();
        assert!(custom.client_dispatch_queue_capacity == 7);
        assert!(custom.client_frame_max == kibibytes(32));
        assert!(custom.runtime_config().client_dispatch_queue_capacity.get() == 7);
        assert!(custom.runtime_config().client_frame_max.size() == kibibytes(32));

        for invalid in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
        ] {
            assert!(
                Args::try_parse_from([
                    "crabka-grpc-gateway",
                    "--bootstrap-servers=localhost:9092",
                    "--advertised-addr=localhost:9500",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_GRPC_GATEWAY_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_GRPC_GATEWAY_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_GRPC_GATEWAY_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
        ])
        .unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
            "--client-dispatch-queue-capacity=9",
            "--client-frame-max=64KiB",
        ])
        .unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == kibibytes(64));
    }

    #[test]
    fn build_broker_security_some_with_cert_and_key_and_sni() {
        let cert = PathBuf::from("/tmp/cert.pem");
        let key = PathBuf::from("/tmp/key.pem");
        let ca = PathBuf::from("/tmp/ca.pem");
        let sni = "broker.example.com".to_string();
        let sec = build_broker_security(Some(&cert), Some(&key), Some(&ca), Some(&sni))
            .expect("should succeed")
            .expect("should be Some");
        let tls = sec.tls.expect("tls should be set");
        assert2::assert!(sec.protocol == ListenerProtocol::Ssl);
        assert2::assert!(tls.server_name == "broker.example.com".to_string());
        assert2::assert!(tls.client_identity == Some((cert, key)));
        assert2::assert!(tls.trust_roots_pem == Some(ca));
    }

    #[test]
    fn build_broker_security_none_when_all_absent() {
        let sec = build_broker_security(None, None, None, None).expect("should succeed");
        assert2::assert!(sec.is_none());
    }

    #[test]
    fn build_broker_security_rejects_incomplete_tls_configuration() {
        let cert = PathBuf::from("/tmp/cert.pem");
        let key = PathBuf::from("/tmp/key.pem");
        for (_name, cert, key) in [
            ("certificate_without_key", Some(&cert), None),
            ("key_without_certificate", None, Some(&key)),
            ("certificate_and_key_without_sni", Some(&cert), Some(&key)),
        ] {
            assert2::assert!(build_broker_security(cert, key, None, None).is_err());
        }
    }

    #[test]
    fn runtime_cli_boundaries_precedence_and_defaults() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");

        for value in [
            "--dedup-partitions=0",
            "--dedup-partitions=2147483648",
            "--dedup-window=0s",
            // A dimensioned flag must carry its unit.
            "--dedup-window=3600000",
            "--tls-reload-interval=0s",
            "--tls-reload-interval=30",
            "--acl-refresh-interval=0s",
            "--internal-topic-replication-factor=0",
            "--internal-topic-create-timeout=0s",
            "--internal-topic-segment=0s",
            "--internal-topic-min-cleanable-dirty-ratio=101%",
            "--consumer-poll-timeout=0ms",
            "--ownership-warmup-empty-polls=0",
            "--readiness-poll-interval=0ms",
            "--produce-max-body=0B",
            "--produce-max-body=3145728",
            "--forward-max-body=0B",
            "--schema-registry-latest-cache-ttl=0s",
            "--bearer-allowable-clock-skew=-1ms",
        ] {
            assert!(
                Args::try_parse_from([
                    "crabka-grpc-gateway",
                    "--bootstrap-servers=localhost:9092",
                    "--advertised-addr=localhost:9500",
                    value,
                ])
                .is_err()
            );
        }

        let explicit = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
            "--internal-topic-replication-factor=2",
            "--internal-topic-allow-replication-fallback=false",
            "--internal-topic-create-timeout=10001ms",
            "--internal-topic-segment=60001ms",
            "--internal-topic-min-cleanable-dirty-ratio=1.01%",
            "--consumer-poll-timeout=501ms",
            "--ownership-warmup-empty-polls=3",
            "--readiness-poll-interval=251ms",
            "--produce-max-body=3MiB",
            "--forward-max-body=3145727B",
            "--schema-registry-latest-cache-ttl=5001ms",
            "--schema-registry-frame-raw=true",
            "--bearer-allowable-clock-skew=0s",
            "--dedup-ownership-group=custom-owners",
        ])
        .expect("parse explicit runtime values");
        assert!(
            explicit.runtime_config()
                == GatewayRuntimeConfig {
                    client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
                    client_frame_max: ClientFrameMax::default(),
                    internal_topic_replication_factor: 2,
                    internal_topic_allow_replication_fallback: false,
                    internal_topic_create_timeout: millis(10_001),
                    internal_topic_segment: millis(60_001),
                    internal_topic_min_cleanable_dirty_ratio: fraction(0.0101),
                    consumer_poll_timeout: millis(501),
                    ownership_warmup_empty_polls: 3,
                    readiness_poll_interval: millis(251),
                    produce_max_body: mebibytes(3),
                    forward_max_body: bytes(3_145_727),
                    schema_registry_latest_cache_ttl: millis(5_001),
                    schema_registry_frame_raw: true,
                }
        );
        assert!(explicit.bearer_allowable_clock_skew == secs(0));
        assert!(explicit.dedup_ownership_group.as_deref() == Some("custom-owners"));

        temp_env::with_vars(
            [
                ("CRABKA_GATEWAY_CONSUMER_POLL_TIMEOUT", None::<&str>),
                ("CRABKA_GATEWAY_PRODUCE_MAX_BODY", None::<&str>),
                ("CRABKA_GATEWAY_FORWARD_MAX_BODY", None::<&str>),
                ("CRABKA_GATEWAY_SCHEMA_REGISTRY_FRAME_RAW", None::<&str>),
                ("CRABKA_GATEWAY_DEDUP_WINDOW", None::<&str>),
                ("CRABKA_GATEWAY_INTERNAL_TOPIC_SEGMENT", None::<&str>),
                ("CRABKA_GATEWAY_INTERNAL_TOPIC_CREATE_TIMEOUT", None::<&str>),
            ],
            || {
                let defaults = Args::try_parse_from([
                    "crabka-grpc-gateway",
                    "--bootstrap-servers=localhost:9092",
                    "--advertised-addr=localhost:9500",
                ])
                .expect("parse defaults");
                assert!(defaults.runtime_config() == GatewayRuntimeConfig::default());
                assert!(defaults.bearer_allowable_clock_skew == secs(30));
                assert!(defaults.dedup_window == hours(1));
                assert!(defaults.tls_reload_interval == secs(30));
                assert!(defaults.acl_refresh_interval == secs(30));

                temp_env::with_vars(
                    [
                        ("CRABKA_GATEWAY_CONSUMER_POLL_TIMEOUT", Some("701ms")),
                        ("CRABKA_GATEWAY_PRODUCE_MAX_BODY", Some("3145729B")),
                        ("CRABKA_GATEWAY_FORWARD_MAX_BODY", Some("3145728B")),
                        ("CRABKA_GATEWAY_DEDUP_WINDOW", Some("3600001ms")),
                        ("CRABKA_GATEWAY_INTERNAL_TOPIC_SEGMENT", Some("60002ms")),
                        (
                            "CRABKA_GATEWAY_INTERNAL_TOPIC_CREATE_TIMEOUT",
                            Some("10002ms"),
                        ),
                    ],
                    || {
                        let from_env = Args::try_parse_from([
                            "crabka-grpc-gateway",
                            "--bootstrap-servers=localhost:9092",
                            "--advertised-addr=localhost:9500",
                        ])
                        .expect("parse environment");
                        let dedup_window = from_env.dedup_window;
                        let from_env = from_env
                            .validate_protocol_runtime()
                            .expect("validate protocol environment");
                        check!(from_env.consumer_poll_timeout == millis(701));
                        check!(from_env.produce_max_body == bytes(3_145_729));
                        check!(from_env.forward_max_body == bytes(3_145_728));
                        check!(dedup_window == millis(3_600_001));
                        check!(from_env.internal_topic_segment == millis(60_002));
                        check!(from_env.internal_topic_create_timeout == millis(10_002));

                        let from_cli = Args::try_parse_from([
                            "crabka-grpc-gateway",
                            "--bootstrap-servers=localhost:9092",
                            "--advertised-addr=localhost:9500",
                            "--consumer-poll-timeout=702ms",
                            "--produce-max-body=3145730B",
                            "--forward-max-body=3145729B",
                            "--dedup-window=3600002ms",
                            "--internal-topic-segment=60003ms",
                            "--internal-topic-create-timeout=10003ms",
                        ])
                        .expect("parse CLI over environment");
                        let dedup_window = from_cli.dedup_window;
                        let from_cli = from_cli
                            .validate_protocol_runtime()
                            .expect("validate protocol CLI");
                        check!(from_cli.consumer_poll_timeout == millis(702));
                        check!(from_cli.produce_max_body == bytes(3_145_730));
                        check!(from_cli.forward_max_body == bytes(3_145_729));
                        check!(dedup_window == millis(3_600_002));
                        check!(from_cli.internal_topic_segment == millis(60_003));
                        check!(from_cli.internal_topic_create_timeout == millis(10_003));
                    },
                );
            },
        );
    }

    #[test]
    fn protocol_runtime_values_require_exact_integer_milliseconds() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");

        for value in [
            "--dedup-window=0.5ms",
            "--internal-topic-segment=0.5ms",
            "--internal-topic-create-timeout=0.5ms",
            "--internal-topic-create-timeout=2147483648ms",
            "--dedup-window=9223372036854775808ms",
            "--internal-topic-segment=9223372036854775808ms",
            "--dedup-window=9007199254740992.5ms",
            "--internal-topic-segment=9007199254740992.5ms",
            "--dedup-window=9007199254740993ms",
            "--internal-topic-segment=9007199254740993ms",
            "--dedup-window=9007199254740992ms",
            "--internal-topic-segment=9007199254740992ms",
        ] {
            let args = Args::try_parse_from([
                "crabka-grpc-gateway",
                "--bootstrap-servers=localhost:9092",
                "--advertised-addr=localhost:9500",
                value,
            ])
            .expect("generic UOM parser accepts positive quantity");
            assert!(
                args.validate_protocol_runtime().is_err(),
                "accepted protocol value {value}"
            );
        }

        for values in [
            [
                "--dedup-window=2147483648ms",
                "--internal-topic-segment=2147483648ms",
                "--internal-topic-create-timeout=2147483647ms",
            ],
            [
                "--dedup-window=9007199254740991ms",
                "--internal-topic-segment=9007199254740991ms",
                "--internal-topic-create-timeout=10s",
            ],
        ] {
            let args = Args::try_parse_from([
                "crabka-grpc-gateway",
                "--bootstrap-servers=localhost:9092",
                "--advertised-addr=localhost:9500",
                values[0],
                values[1],
                values[2],
            ])
            .expect("parse exact protocol values");
            assert!(args.validate_protocol_runtime().is_ok());
        }

        let ambiguous = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
            "--dedup-window=9007199254740992ms",
        ])
        .expect("generic UOM parser accepts ambiguous quantity")
        .validate_protocol_runtime()
        .expect_err("ambiguous UOM quantity must be rejected");
        assert!(
            ambiguous
                .to_string()
                .contains("below 9007199254740992ms because UOM quantities use f64")
        );

        let defaults = Args::try_parse_from([
            "crabka-grpc-gateway",
            "--bootstrap-servers=localhost:9092",
            "--advertised-addr=localhost:9500",
        ])
        .expect("parse defaults");
        assert!(defaults.validate_protocol_runtime().is_ok());
    }

    #[test]
    fn schema_registry_codec_uses_configured_frame_raw() {
        let runtime = GatewayRuntimeConfig {
            schema_registry_frame_raw: true,
            ..GatewayRuntimeConfig::default()
        };

        let codec =
            build_schema_registry_codec("http://localhost:8081", &runtime).expect("valid URL");

        assert!(codec.frame_raw);
    }

    #[test]
    fn resolved_ownership_group_reaches_consumer_handoff() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");

        temp_env::with_var("CRABKA_GATEWAY_DEDUP_OWNERSHIP_GROUP", None::<&str>, || {
            let defaults = Args::try_parse_from([
                "crabka-grpc-gateway",
                "--bootstrap-servers=localhost:9092",
                "--advertised-addr=localhost:9500",
                "--client-id=gateway-a",
            ])
            .expect("parse defaults");
            let custom = Args::try_parse_from([
                "crabka-grpc-gateway",
                "--bootstrap-servers=localhost:9092",
                "--advertised-addr=localhost:9500",
                "--client-id=gateway-a",
                "--dedup-ownership-group=custom-owners",
            ])
            .expect("parse custom ownership group");

            for (args, expected) in [
                (defaults, "gateway-a-dedup-owners"),
                (custom, "custom-owners"),
            ] {
                let config = GatewayConfig {
                    bootstrap: args.bootstrap_servers.clone(),
                    listen_addr: args.listen_addr,
                    client_id: args.client_id.clone(),
                    dedup_topic: args.dedup_topic.clone(),
                    dedup_partitions: args.dedup_partitions.into_value(),
                    dedup_window: args.dedup_window,
                    dedup_ownership_group: args.resolved_dedup_ownership_group(),
                    dedup_txn_id_prefix: args.dedup_txn_id_prefix.clone(),
                    advertised_addr: args.advertised_addr.clone(),
                    membership_topic: args.membership_topic.clone(),
                    tls: None,
                    broker_security: None,
                    authz: None,
                    webhooks: std::collections::HashMap::new(),
                    outbound: Vec::new(),
                    schema_registry_url: None,
                    runtime: args.runtime_config(),
                };
                let (_, _, _, group) = ownership_consumer_inputs(&config);
                assert!(group == expected);
            }
        });
    }
}
