//! crabka-schema-registry: Confluent Schema Registry-compatible REST service.
//!
//! This binary is a thin `clap` → lib shim: it parses CLI flags into an
//! [`Args`], maps them into a [`SecurityCliInput`], and hands that to
//! [`crabka_schema_registry::cli::build_security`] for validation/assembly (kept
//! in the lib so it is unit-testable). The remaining glue (serve wiring,
//! election, ACL-refresh task) lives here.

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_client_admin::AdminClient;
use crabka_schema_registry::auth::AuthState;
use crabka_schema_registry::auth::basic::BasicAuthStore;
use crabka_schema_registry::authz::SchemaRegistryAuthz;
use crabka_schema_registry::cli::SecurityCliInput;
use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::serve::{serve_http, serve_https};
use crabka_schema_registry::rest::{self, AppState, SecurityLayers};

#[derive(Debug, Parser)]
#[command(
    name = "crabka-schema-registry",
    version,
    about = "Confluent Schema Registry-compatible service for Crabka"
)]
struct Args {
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_LISTEN_ADDR",
        default_value = "0.0.0.0:8081"
    )]
    listen_addr: SocketAddr,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC",
        default_value = "_schemas"
    )]
    schemas_topic: String,
    #[arg(long, env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC_RF", default_value_t = 3)]
    schemas_topic_rf: i32,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_CLIENT_ID",
        default_value = "crabka-schema-registry"
    )]
    client_id: String,
    #[arg(long, env = "SCHEMA_REGISTRY_ADVERTISED_URL")]
    advertised_url: Option<String>,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_GROUP_ID",
        default_value = "schema-registry"
    )]
    group_id: String,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_LEADER_ELIGIBILITY",
        default_value_t = true
    )]
    leader_eligibility: bool,

    // ── Authentication ──────────────────────────────────────────────────────
    /// Reject unauthenticated (anonymous) requests with 401.
    #[arg(long, env = "SCHEMA_REGISTRY_REQUIRE_AUTH", default_value_t = false)]
    require_auth: bool,
    /// `WWW-Authenticate: basic realm="<realm>"` realm advertised on 401. The
    /// default matches the realm `cp-schema-registry` emits under the standard
    /// `PropertyFileLoginModule` BASIC setup (the JAAS entry name).
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_AUTH_REALM",
        default_value = "SchemaRegistry-Props"
    )]
    realm: String,
    /// htpasswd-style `user:cred` file (one per line) for HTTP Basic. The cred
    /// is a plaintext password or a `$2…` bcrypt hash.
    #[arg(long, env = "SCHEMA_REGISTRY_BASIC_AUTH_FILE")]
    basic_auth_file: Option<PathBuf>,
    /// Inline Basic credential as `user:cred` (repeatable). Same cred format as
    /// `--basic-auth-file`. Enables Basic auth even without a file.
    #[arg(long = "basic-user", value_name = "USER:CRED")]
    basic_users: Vec<String>,

    // ── Bearer (OAuth) ──────────────────────────────────────────────────────
    /// Bearer-token mode: `off` | `unsecured`. `unsecured` accepts unsigned
    /// JWTs (dev only), mirroring the gateway's `--bearer`.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER", default_value = "off")]
    bearer: String,
    /// JWT claim whose value becomes the principal name (Bearer mode).
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_BEARER_PRINCIPAL_CLAIM",
        default_value = "sub"
    )]
    bearer_principal_claim: String,

    // ── JWKS Bearer ─────────────────────────────────────────────────────────
    /// JWKS endpoint URL (required when --bearer=jwks).
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_ENDPOINT_URI")]
    bearer_jwks_endpoint_uri: Option<String>,
    /// Expected token `iss` claim. Absent = no issuer check.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_VALID_ISSUER")]
    bearer_jwks_valid_issuer: Option<String>,
    /// Expected token `aud` value. Absent = no audience check.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_EXPECTED_AUDIENCE")]
    bearer_jwks_expected_audience: Option<String>,
    /// PEM CA bundle trusted for the JWKS HTTPS endpoint.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_CA")]
    bearer_jwks_ca: Option<PathBuf>,
    /// Override JWT principal-claim name for JWKS mode.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_PRINCIPAL_CLAIM")]
    bearer_jwks_principal_claim: Option<String>,
    /// JWKS refresh interval in milliseconds. Default 60 000.
    #[arg(long, env = "SCHEMA_REGISTRY_BEARER_JWKS_REFRESH_MS")]
    bearer_jwks_refresh_ms: Option<u64>,

    // ── Server TLS / mTLS ───────────────────────────────────────────────────
    /// Server cert chain (PEM). Enables HTTPS when set together with --tls-key.
    #[arg(long, env = "SCHEMA_REGISTRY_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// Server private key (PEM).
    #[arg(long, env = "SCHEMA_REGISTRY_TLS_KEY")]
    tls_key: Option<PathBuf>,
    /// CA(s) used to verify incoming client certs (mTLS). Required when
    /// --tls-client-auth != disabled.
    #[arg(long, env = "SCHEMA_REGISTRY_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,
    /// Client-cert mode: `disabled` | `optional` | `required`.
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_TLS_CLIENT_AUTH",
        default_value = "disabled"
    )]
    tls_client_auth: String,

    // ── Authorization (topic ACLs) ──────────────────────────────────────────
    /// Enable topic-ACL authorization (sourced from the broker's `DescribeAcls`).
    #[arg(long, env = "SCHEMA_REGISTRY_AUTHZ", default_value_t = false)]
    authz: bool,
    /// Super-user principal name that bypasses ACL checks (repeatable).
    #[arg(long = "super-user", value_name = "NAME")]
    super_users: Vec<String>,
    /// ACL-cache refresh interval (seconds).
    #[arg(long, env = "SCHEMA_REGISTRY_ACL_REFRESH_SECS", default_value_t = 30)]
    acl_refresh_secs: u64,

    // ── SR → broker client security ─────────────────────────────────────────
    /// Kafka client protocol to the broker: `PLAINTEXT` | `SSL` |
    /// `SASL_PLAINTEXT` | `SASL_SSL`.
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_KAFKA_SECURITY_PROTOCOL",
        default_value = "PLAINTEXT"
    )]
    kafka_security_protocol: String,
    /// SASL mechanism: `PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`.
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_KAFKA_SASL_MECHANISM",
        default_value = "PLAIN"
    )]
    kafka_sasl_mechanism: String,
    /// SASL username (PLAIN / SCRAM).
    #[arg(long, env = "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME")]
    kafka_sasl_username: Option<String>,
    /// SASL password (PLAIN / SCRAM).
    #[arg(long, env = "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD")]
    kafka_sasl_password: Option<String>,
    /// CA(s) (PEM) the client trusts for the broker's server cert (SSL /
    /// `SASL_SSL`).
    #[arg(long, env = "SCHEMA_REGISTRY_KAFKA_TLS_CA")]
    kafka_tls_ca: Option<PathBuf>,
    /// TLS SNI / server name for the broker connection (SSL / `SASL_SSL`).
    #[arg(long, env = "SCHEMA_REGISTRY_KAFKA_TLS_SERVER_NAME")]
    kafka_tls_server_name: Option<String>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    // Install the ring crypto provider before any TLS work (server or client).
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-schema-registry",
            env!("CARGO_PKG_VERSION"),
            "crabka-schema-registry",
        ),
        "crabka_schema_registry=info,info",
        "info",
        "crabka-schema-registry",
    )?;

    crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;

    let args = Args::parse();
    let crabka_schema_registry::cli::SecurityOutput {
        config: security,
        jwks_handle,
    } = crabka_schema_registry::cli::build_security(&args.security_input())?;
    let cfg = RegistryConfig {
        bootstrap: args.bootstrap_servers.clone(),
        schemas_topic: args.schemas_topic.clone(),
        schemas_topic_rf: args.schemas_topic_rf,
        client_id: args.client_id.clone(),
        advertised_url: args
            .advertised_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", args.listen_addr)),
        group_id: args.group_id.clone(),
        leader_eligibility: args.leader_eligibility,
        security,
    };
    info!(
        listen = %args.listen_addr,
        bootstrap = %cfg.bootstrap,
        topic = %cfg.schemas_topic,
        tls = cfg.security.tls.is_some(),
        require_auth = cfg.security.require_auth,
        authz = cfg.security.authz.as_ref().is_some_and(|a| a.enabled),
        "crabka-schema-registry starting"
    );

    let shutdown = CancellationToken::new();

    // ── JWKS refresh task (bearer=jwks only) ────────────────────────────────
    if let Some(jwks) = jwks_handle {
        let shutdown_for_jwks = shutdown.clone();
        tokio::spawn(async move {
            run_jwks_refresher(jwks, shutdown_for_jwks).await;
        });
    }

    let store = KafkaStore::start(&cfg, shutdown.clone()).await?;
    let primary = crabka_schema_registry::election::Election::start(&cfg, shutdown.clone()).await?;

    // ── Authentication state ────────────────────────────────────────────────
    let basic = match &cfg.security.basic {
        Some(b) => {
            Some(Arc::new(BasicAuthStore::load(b).map_err(|e| {
                anyhow::anyhow!("load basic-auth credentials: {e}")
            })?))
        }
        None => None,
    };
    let bearer = cfg.security.bearer.as_ref().map(|b| b.validator.clone());
    let auth = AuthState {
        basic,
        bearer,
        require_auth: cfg.security.require_auth,
        realm: cfg.security.realm.clone(),
    };

    // ── Authorization (+ ACL refresh task) ──────────────────────────────────
    let authz = match &cfg.security.authz {
        Some(a) if a.enabled => {
            let az = Arc::new(SchemaRegistryAuthz::new(a.super_users.clone(), true));
            let admin = AdminClient::connect_secured(
                &split_bootstrap(&cfg.bootstrap),
                cfg.security.client.clone(),
            )
            .await?;
            let az_for_task = az.clone();
            let refresh = a.acl_refresh;
            let shutdown_for_task = shutdown.clone();
            tokio::spawn(async move {
                az_for_task
                    .run_acl_refresh(admin, refresh, shutdown_for_task)
                    .await;
            });
            Some(az)
        }
        _ => None,
    };

    let fwd = rest::forward::ForwardState {
        primary,
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let layers = SecurityLayers {
        auth,
        authz,
        forward: fwd,
    };
    let app = rest::router_with_security(AppState { store }, layers);

    // ctrl-c → cancel shutdown.
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown.cancel();
        });
    }

    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(addr = %listener.local_addr()?, tls = cfg.security.tls.is_some(), "listening");
    if let Some(tls) = &cfg.security.tls {
        serve_https(listener, app, tls, shutdown).await?;
    } else {
        serve_http(listener, app, shutdown).await?;
    }
    telemetry.shutdown();
    Ok(())
}

impl Args {
    /// Map the parsed clap flags into the clap-free [`SecurityCliInput`] the lib
    /// validates/assembles. Pure field-shuffling — the security semantics live
    /// in [`crabka_schema_registry::cli::build_security`].
    fn security_input(&self) -> SecurityCliInput {
        SecurityCliInput {
            require_auth: self.require_auth,
            realm: self.realm.clone(),
            basic_auth_file: self.basic_auth_file.clone(),
            basic_users: self.basic_users.clone(),
            bearer: self.bearer.clone(),
            bearer_principal_claim: self.bearer_principal_claim.clone(),
            jwks_endpoint_uri: self.bearer_jwks_endpoint_uri.clone(),
            jwks_valid_issuer: self.bearer_jwks_valid_issuer.clone(),
            jwks_expected_audience: self.bearer_jwks_expected_audience.clone(),
            jwks_ca: self.bearer_jwks_ca.clone(),
            jwks_principal_claim: self.bearer_jwks_principal_claim.clone(),
            jwks_refresh_ms: self.bearer_jwks_refresh_ms,
            tls_cert: self.tls_cert.clone(),
            tls_key: self.tls_key.clone(),
            tls_client_ca: self.tls_client_ca.clone(),
            tls_client_auth: self.tls_client_auth.clone(),
            authz: self.authz,
            super_users: self.super_users.clone(),
            acl_refresh_secs: self.acl_refresh_secs,
            kafka_security_protocol: self.kafka_security_protocol.clone(),
            kafka_sasl_mechanism: self.kafka_sasl_mechanism.clone(),
            kafka_sasl_username: self.kafka_sasl_username.clone(),
            kafka_sasl_password: self.kafka_sasl_password.clone(),
            kafka_tls_ca: self.kafka_tls_ca.clone(),
            kafka_tls_server_name: self.kafka_tls_server_name.clone(),
        }
    }
}

/// Split a comma-separated `host:port,host:port` bootstrap string into the
/// `Vec<String>` the admin/connect APIs expect.
fn split_bootstrap(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Periodically fetch the JWKS endpoint and refresh the live key-set handle.
///
/// Fetches immediately on startup, then once per `jwks.refresh_ms`.
/// Cancelled by the shared `CancellationToken`.
async fn run_jwks_refresher(
    jwks: crabka_schema_registry::cli::JwksHandleForRefresh,
    cancel: CancellationToken,
) {
    use crabka_security::Jwks;
    use std::time::Duration;

    let client = build_jwks_client(jwks.ca_path.as_ref()).unwrap_or_else(|e| {
        tracing::error!(error = %e, "JWKS client build failed; using default TLS roots");
        reqwest::Client::new()
    });
    loop {
        match client.get(&jwks.endpoint_uri).send().await {
            Ok(resp) if resp.status().is_success() => match resp.text().await {
                Ok(text) => match Jwks::from_json(&text, true) {
                    Ok(new_keys) => {
                        jwks.handle.store(new_keys);
                        tracing::debug!(uri = %jwks.endpoint_uri, "JWKS refreshed");
                    }
                    Err(e) => tracing::warn!(
                        error = %e, uri = %jwks.endpoint_uri, "JWKS parse error"
                    ),
                },
                Err(e) => tracing::warn!(error = %e, "JWKS response body read error"),
            },
            Ok(resp) => tracing::warn!(
                status = %resp.status(), uri = %jwks.endpoint_uri, "JWKS endpoint error"
            ),
            Err(e) => {
                tracing::warn!(error = %e, uri = %jwks.endpoint_uri, "JWKS fetch failed");
            }
        }
        tokio::select! {
            () = cancel.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(jwks.refresh_ms)) => {}
        }
    }
}

fn build_jwks_client(ca_path: Option<&std::path::PathBuf>) -> anyhow::Result<reqwest::Client> {
    let Some(path) = ca_path else {
        return Ok(reqwest::Client::new());
    };
    let pem =
        std::fs::read(path).map_err(|e| anyhow::anyhow!("read JWKS CA {}: {e}", path.display()))?;
    let cert = reqwest::Certificate::from_pem(&pem)
        .map_err(|e| anyhow::anyhow!("parse JWKS CA PEM: {e}"))?;
    reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .map_err(|e| anyhow::anyhow!("build JWKS reqwest client: {e}"))
}
