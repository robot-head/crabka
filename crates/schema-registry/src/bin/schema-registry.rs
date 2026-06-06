//! crabka-schema-registry: Confluent Schema Registry-compatible REST service.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_client_admin::AdminClient;
use crabka_client_core::ClientSecurity;
use crabka_client_core::security::{SaslCredentials, TlsConnectorConfig};
use crabka_schema_registry::auth::AuthState;
use crabka_schema_registry::auth::basic::BasicAuthStore;
use crabka_schema_registry::authz::SchemaRegistryAuthz;
use crabka_schema_registry::config::{
    AuthzConfig, BasicAuthConfig, BearerAuthConfig, RegistryConfig, SecurityConfig,
};
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::serve::{serve_http, serve_https};
use crabka_schema_registry::rest::{self, AppState, SecurityLayers};
use crabka_security::{
    ClientAuthMode, ListenerProtocol, OAuthBearerValidator, SaslMechanism, TlsConfig,
};

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
    /// `WWW-Authenticate: Basic realm="<realm>"` value advertised on 401.
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_AUTH_REALM",
        default_value = "Schema Registry"
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
async fn main() -> anyhow::Result<()> {
    // Install the ring crypto provider before any TLS work (server or client).
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_schema_registry=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let security = build_security(&args)?;
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
    let store = KafkaStore::start(&cfg, shutdown.clone()).await?;
    let primary = crabka_schema_registry::election::Election::start(&cfg, shutdown.clone()).await?;

    // ── Authentication state ────────────────────────────────────────────────
    let basic = match &cfg.security.basic {
        Some(b) => Some(Arc::new(load_basic_store(b)?)),
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
    Ok(())
}

/// Assemble [`SecurityConfig`] from CLI args. The all-defaults case (no TLS, no
/// auth, no authz, plaintext broker client) yields the fully-open
/// [`SecurityConfig::default`] behaviour.
fn build_security(args: &Args) -> anyhow::Result<SecurityConfig> {
    Ok(SecurityConfig {
        require_auth: args.require_auth,
        realm: args.realm.clone(),
        basic: build_basic(args),
        bearer: build_bearer(args)?,
        tls: build_tls(args)?,
        authz: build_authz(args),
        client: build_client_security(args)?,
    })
}

/// Build [`BasicAuthConfig`] from `--basic-auth-file` / repeated `--basic-user`.
/// Returns `None` when neither is supplied (Basic disabled).
fn build_basic(args: &Args) -> Option<BasicAuthConfig> {
    if args.basic_auth_file.is_none() && args.basic_users.is_empty() {
        return None;
    }
    let mut users = HashMap::new();
    for entry in &args.basic_users {
        if let Some((u, c)) = entry.split_once(':') {
            users.insert(u.to_string(), c.to_string());
        } else {
            tracing::warn!(entry = %entry, "ignoring malformed --basic-user (want user:cred)");
        }
    }
    Some(BasicAuthConfig {
        users,
        file: args.basic_auth_file.clone(),
    })
}

/// Build [`BearerAuthConfig`] from `--bearer`. `off` ⇒ `None`; `unsecured` ⇒ a
/// dev `UnsecuredJwsValidator` (mirrors the gateway). Signed/`JWKS` validators
/// are supported by the config struct but not yet CLI-exposed.
fn build_bearer(args: &Args) -> anyhow::Result<Option<BearerAuthConfig>> {
    match args.bearer.as_str() {
        "off" => Ok(None),
        "unsecured" => {
            let validator =
                OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
                    principal_claim_name: args.bearer_principal_claim.clone(),
                    ..Default::default()
                });
            Ok(Some(BearerAuthConfig {
                validator: Arc::new(validator),
            }))
        }
        other => anyhow::bail!("invalid --bearer: {other} (want off|unsecured)"),
    }
}

/// Build server [`TlsConfig`] from the `--tls-*` flags. Requires both cert+key
/// or neither; `None` ⇒ plain HTTP.
fn build_tls(args: &Args) -> anyhow::Result<Option<TlsConfig>> {
    match (args.tls_cert.clone(), args.tls_key.clone()) {
        (Some(cert_chain_path), Some(private_key_path)) => {
            let client_auth = match args.tls_client_auth.as_str() {
                "disabled" => ClientAuthMode::Disabled,
                "optional" => ClientAuthMode::Optional,
                "required" => ClientAuthMode::Required,
                other => anyhow::bail!("invalid --tls-client-auth: {other}"),
            };
            Ok(Some(TlsConfig {
                cert_chain_path,
                private_key_path,
                trust_roots_path: args.tls_client_ca.clone(),
                client_ca_path: args.tls_client_ca.clone(),
                client_auth,
            }))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
    }
}

/// Build [`AuthzConfig`] from the `--authz` / `--super-user` flags.
fn build_authz(args: &Args) -> Option<AuthzConfig> {
    if !args.authz {
        return None;
    }
    let super_users: HashSet<String> = args.super_users.iter().cloned().collect();
    Some(AuthzConfig {
        enabled: true,
        super_users,
        acl_refresh: std::time::Duration::from_secs(args.acl_refresh_secs),
    })
}

/// Build SR → broker [`ClientSecurity`] from `--kafka-*`. `PLAINTEXT` ⇒ `None`
/// (plaintext, the pre-security default). PLAIN/SCRAM + TLS-CA are covered;
/// GSSAPI and client-cert (mTLS to the broker) are config-struct-supported but
/// not yet CLI-exposed.
fn build_client_security(args: &Args) -> anyhow::Result<Option<ClientSecurity>> {
    let protocol = match args.kafka_security_protocol.to_ascii_uppercase().as_str() {
        "PLAINTEXT" => return Ok(None),
        "SSL" => ListenerProtocol::Ssl,
        "SASL_PLAINTEXT" => ListenerProtocol::SaslPlaintext,
        "SASL_SSL" => ListenerProtocol::SaslSsl,
        other => anyhow::bail!(
            "invalid --kafka-security-protocol: {other} (want PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL)"
        ),
    };

    let tls = if protocol.requires_tls() {
        Some(TlsConnectorConfig {
            trust_roots_pem: args.kafka_tls_ca.clone(),
            server_name: args
                .kafka_tls_server_name
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
        })
    } else {
        None
    };

    let sasl = if protocol.requires_sasl() {
        Some(build_sasl(args)?)
    } else {
        None
    };

    Ok(Some(ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host: None,
    }))
}

/// Build the SASL credential set for a `SASL_*` broker protocol.
fn build_sasl(args: &Args) -> anyhow::Result<SaslCredentials> {
    let username = args
        .kafka_sasl_username
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--kafka-sasl-username required for SASL_* protocols"))?;
    let password = args
        .kafka_sasl_password
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--kafka-sasl-password required for SASL_* protocols"))?;
    match args.kafka_sasl_mechanism.to_ascii_uppercase().as_str() {
        "PLAIN" => Ok(SaslCredentials::Plain { username, password }),
        "SCRAM-SHA-256" => Ok(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username,
            password,
        }),
        "SCRAM-SHA-512" => Ok(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username,
            password,
        }),
        other => anyhow::bail!(
            "invalid --kafka-sasl-mechanism: {other} (want PLAIN|SCRAM-SHA-256|SCRAM-SHA-512); \
             GSSAPI is not yet CLI-exposed"
        ),
    }
}

/// Load a [`BasicAuthStore`] from inline users + an optional htpasswd-style
/// file. File entries layer over inline users (file wins on conflict).
fn load_basic_store(cfg: &BasicAuthConfig) -> anyhow::Result<BasicAuthStore> {
    let mut users = cfg.users.clone();
    if let Some(path) = &cfg.file {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read basic-auth file {}: {e}", path.display()))?;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((u, c)) = line.split_once(':') {
                users.insert(u.to_string(), c.to_string());
            }
        }
    }
    Ok(BasicAuthStore::from_users(users))
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
