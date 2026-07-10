//! `crabka-grpc-gateway` binary entry point.
//!
//! Parses CLI flags, builds the Connect-RPC router and a minimal health
//! router, then serves both on the configured listen address.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context as _;
use clap::Parser;
use crabka_grpc_gateway::{
    codec::{RawCodec, RecordCodec},
    config::{AuthzSettings, BearerSettings, GatewayConfig, TlsSettings},
    dedup::{
        DedupEngine,
        membership::{MembershipPublisher, MembershipStore},
        store::DedupStore,
        topic::{ensure_dedup_topic, ensure_membership_topic},
    },
    forward::{self, Forwarder},
    health::{self, Readiness},
    produce::ProduceCore,
    schema::{client::SchemaRegistryClient, codec::SchemaRegistryCodec},
    state::AppState,
};
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
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_PARTITIONS", default_value_t = 8)]
    dedup_partitions: u32,

    /// Dedup window (ms).
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_WINDOW_MS",
        default_value_t = 3_600_000
    )]
    dedup_window_ms: i64,

    /// Transactional id prefix for the dedup path.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_TXN_PREFIX",
        default_value = "crabka-gw-dedup"
    )]
    dedup_txn_id_prefix: String,

    /// Address peers reach this gateway at (e.g. `gw-0.gw:9500`). Required for
    /// active-active forwarding; must be routable from other replicas.
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
    /// CA(s) used to verify incoming client certs (mTLS). Required if --tls-client-auth != disabled.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CLIENT_CA")]
    tls_client_ca: Option<std::path::PathBuf>,
    /// Client-cert mode: disabled | optional | required.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_TLS_CLIENT_AUTH",
        default_value = "disabled"
    )]
    tls_client_auth: String,
    /// CA(s) the forwarder trusts for peer gateway server certs (defaults to --tls-client-ca).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_TRUST_ROOTS")]
    tls_trust_roots: Option<std::path::PathBuf>,
    /// Cert hot-reload poll interval (seconds).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_RELOAD_SECS", default_value_t = 30)]
    tls_reload_secs: u64,

    /// Authorizer mode: off | simple.
    #[arg(long, env = "CRABKA_GATEWAY_AUTHZ", default_value = "off")]
    authz: String,
    /// Comma-separated super-user principal names that bypass ACL checks.
    #[arg(long, env = "CRABKA_GATEWAY_AUTHZ_SUPER_USERS", default_value = "")]
    authz_super_users: String,
    /// ACL-cache refresh interval (seconds).
    #[arg(long, env = "CRABKA_GATEWAY_ACL_REFRESH_SECS", default_value_t = 30)]
    acl_refresh_secs: u64,

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

    /// CA cert (PEM) the gateway trusts when connecting to the broker over TLS.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_CA")]
    broker_tls_ca: Option<std::path::PathBuf>,
    /// Client cert chain (PEM) presented to the broker for mTLS.
    /// Must be set together with `--broker-tls-key`.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_CERT")]
    broker_tls_cert: Option<std::path::PathBuf>,
    /// Client private key (PEM) for mTLS to the broker.
    /// Must be set together with `--broker-tls-cert`.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_KEY")]
    broker_tls_key: Option<std::path::PathBuf>,
    /// SNI / server-name used for the TLS handshake with the broker.
    /// Required when `--broker-tls-cert` + `--broker-tls-key` are set.
    #[arg(long, env = "CRABKA_GATEWAY_BROKER_TLS_SERVER_NAME")]
    broker_tls_server_name: Option<String>,

    /// Optional TOML file defining `[[webhooks.endpoints]]` for HTTP webhook inbound.
    #[arg(long, env = "CRABKA_GATEWAY_WEBHOOKS_CONFIG")]
    webhooks_config: Option<std::path::PathBuf>,

    /// Optional TOML file defining `[[subscriptions]]` for HTTP webhook outbound.
    #[arg(long, env = "CRABKA_GATEWAY_OUTBOUND_WEBHOOKS_CONFIG")]
    outbound_webhooks_config: Option<std::path::PathBuf>,

    /// Base URL of a Confluent-compatible Schema Registry (e.g.
    /// `http://schema-registry:8081`). When set, records are encoded/decoded
    /// via `SchemaRegistryCodec`; when absent, `RawCodec` (identity) is used.
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

    let otlp = crabka_telemetry::OtlpConfig::from_env(
        |k| std::env::var(k).ok(),
        &args.client_id,
        env!("CARGO_PKG_VERSION"),
        "crabka-grpc-gateway",
    );
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
                reload_interval_secs: args.tls_reload_secs,
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

    let config = GatewayConfig {
        bootstrap: args.bootstrap_servers.clone(),
        listen_addr: args.listen_addr,
        client_id: args.client_id.clone(),
        dedup_topic: args.dedup_topic.clone(),
        dedup_partitions: args.dedup_partitions,
        dedup_window_ms: args.dedup_window_ms,
        dedup_txn_id_prefix: args.dedup_txn_id_prefix.clone(),
        advertised_addr: args.advertised_addr.clone(),
        membership_topic: args.membership_topic.clone(),
        tls: tls.clone(),
        broker_security,
        authz,
        webhooks,
        outbound,
        schema_registry_url: args.schema_registry_url.clone(),
    };

    let bearer = build_bearer(&args)?;

    let result = run(config, bearer).await;
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
                acl_refresh_secs: args.acl_refresh_secs,
            }))
        }
        other => anyhow::bail!("invalid --authz: {other}"),
    }
}

/// Build [`ClientSecurity`] for outbound broker connections from the four
/// `--broker-tls-*` flags.
///
/// - Both cert+key present ⇒ mTLS; SNI is required in that case.
/// - Both absent ⇒ plaintext (`None`).
/// - Exactly one present ⇒ configuration error.
/// - CA only (no cert/key) ⇒ one-way TLS with the given CA.
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
                ..Default::default()
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

#[allow(clippy::too_many_lines)]
async fn run(
    config: GatewayConfig,
    bearer: Option<crabka_grpc_gateway::authz::auth_layer::BearerValidator>,
) -> anyhow::Result<()> {
    // Ensure internal topics exist before opening any producer/consumer.
    ensure_dedup_topic(
        &config.bootstrap,
        &config.dedup_topic,
        config.dedup_partitions,
        config.dedup_window_ms,
        GatewayConfig::DEDUP_TOPIC_REPLICATION,
        config.broker_security.clone(),
    )
    .await?;
    ensure_membership_topic(
        &config.bootstrap,
        &config.membership_topic,
        GatewayConfig::MEMBERSHIP_TOPIC_REPLICATION,
        config.broker_security.clone(),
    )
    .await?;

    let node_id = uuid::Uuid::new_v4().to_string();
    let store = Arc::new(DedupStore::new(config.dedup_partitions));
    let readiness = Readiness::new();
    let shutdown = CancellationToken::new();

    // Membership: tail the routing table, and install the publisher BEFORE the
    // ownership consumer starts so its first assignment is published.
    let membership = Arc::new(MembershipStore::new());
    spawn_membership_reader(&config, &membership, &node_id, &shutdown);
    let publisher = Arc::new(
        MembershipPublisher::new(
            &config.bootstrap,
            &format!("{}-membership-pub", config.client_id),
            node_id.clone(),
            config.advertised_addr.clone(),
            config.membership_topic.clone(),
            config.broker_security.clone(),
        )
        .await?,
    );
    store.set_membership(publisher);

    spawn_ownership_consumer(&config, &store, &shutdown);
    spawn_readiness_watcher(store.clone(), readiness.clone());

    let engine = Arc::new(DedupEngine::new(
        &config.bootstrap,
        &config.client_id,
        &config.dedup_txn_id_prefix,
        config.dedup_topic.clone(),
        config.dedup_partitions,
        store,
        config.broker_security.clone(),
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
        Some(url) => {
            let client = SchemaRegistryClient::new(url)
                .map_err(|e| anyhow::anyhow!("schema registry client: {e}"))?;
            Arc::new(SchemaRegistryCodec {
                client: Arc::new(client),
                frame_raw: false,
            })
        }
        None => Arc::new(RawCodec),
    };

    // Spawn outbound delivery tasks with the shared codec (so `decode_to_json`
    // subscriptions can de-frame Confluent-framed values to JSON).
    spawn_outbound_subscriptions(&config, &shutdown, codec.clone()).await?;

    let produce = ProduceCore::new(
        &config.bootstrap,
        &config.client_id,
        codec.clone(),
        config.broker_security.clone(),
    )
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
                t.reload_interval_secs,
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
        let refresh = std::time::Duration::from_secs(a.acl_refresh_secs);
        tokio::spawn(gateway_authz.clone().run_acl_refresh(
            config.bootstrap.clone(),
            refresh,
            shutdown.clone(),
            broker_security,
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
    tokio::spawn(async move {
        if let Err(e) = membership
            .run_membership(bootstrap, client_id, topic, group, shutdown, security)
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
    let bootstrap = config.bootstrap.clone();
    let client_id = format!("{}-dedup-owner", config.client_id);
    let dedup_topic = config.dedup_topic.clone();
    let shutdown = shutdown.clone();
    let security = config.broker_security.clone();
    tokio::spawn(async move {
        if let Err(e) = store
            .run_ownership(
                bootstrap,
                client_id,
                dedup_topic,
                "__crabka_grpc_gateway_dedup_owners".to_string(),
                shutdown,
                security,
            )
            .await
        {
            tracing::error!(error = %e, "dedup ownership task exited with error");
        }
    });
}

fn spawn_readiness_watcher(store: Arc<DedupStore>, readiness: Readiness) {
    tokio::spawn(async move {
        loop {
            if store.has_warmed_once() {
                readiness.set_ready();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });
}

/// Build the DLQ producer and spawn one delivery task per outbound subscription.
/// No-ops when `config.outbound` is empty (zero overhead for default deployments).
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
    tokio::spawn(async move {
        if let Err(e) = crabka_grpc_gateway::outbound::run_subscription(
            sub,
            bootstrap,
            client_id,
            dlq_producer,
            shutdown,
            security,
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
    use crabka_security::ListenerProtocol;

    use super::*;

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
        assert_eq!(
            (
                sec.protocol,
                tls.server_name,
                tls.client_identity,
                tls.trust_roots_pem,
            ),
            (
                ListenerProtocol::Ssl,
                "broker.example.com".to_string(),
                Some((cert, key)),
                Some(ca),
            )
        );
    }

    #[test]
    fn build_broker_security_none_when_all_absent() {
        let sec = build_broker_security(None, None, None, None).expect("should succeed");
        assert!(sec.is_none(), "all absent should return None");
    }

    #[test]
    fn build_broker_security_rejects_incomplete_tls_configuration() {
        let cert = PathBuf::from("/tmp/cert.pem");
        let key = PathBuf::from("/tmp/key.pem");
        for (name, cert, key) in [
            ("certificate_without_key", Some(&cert), None),
            ("key_without_certificate", None, Some(&key)),
            ("certificate_and_key_without_sni", Some(&cert), Some(&key)),
        ] {
            assert!(
                build_broker_security(cert, key, None, None).is_err(),
                "case {name}"
            );
        }
    }
}
