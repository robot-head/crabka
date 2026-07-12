use std::{
    collections::{BTreeMap, HashMap},
    net::{SocketAddr, ToSocketAddrs},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_gres_control::{
    FinalCheckpoint, TenantName, TenantRecord, decode_tenant_config_record, tenant_config_topic,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{FjallKv, Kv, KvScan, MemKv, RestoreKv, SnapshotKv};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, Engine, ExecuteOutcome, PortalDescription, PreparedDescription,
        QueryResult, Session, TxStatus,
    },
    session::{AuthMode, SessionConfig},
};
use crabka_security::{
    ClientAuthMode, ListenerProtocol, SaslMechanism, TlsConfig, scram::PgScramVerifier,
};
use rand::RngExt as _;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

mod split_activation;
use split_activation::{PendingLiveTopology, PreparedLiveTopology, StagedLiveRangeSuccessor};

const DEFAULT_CHECKPOINT_FRAMES_THRESHOLD: u64 = 10_000;
const DEFAULT_CHECKPOINT_BYTES_THRESHOLD: u64 = 64 * 1024 * 1024;
const DEFAULT_CHECKPOINT_RETAIN_NEWEST: usize = 2;
const CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS: i32 = 30_000;
const TENANT_CONFIG_FETCH_MAX_WAIT_MS: i32 = 500;
const TENANT_CONFIG_FETCH_PARTITION_MAX_BYTES: i32 = 1 << 20;
const IDLE_MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(1);

trait SubstrateKv: SnapshotKv + RestoreKv {}

impl<T> SubstrateKv for T where T: SnapshotKv + RestoreKv {}

/// Command-line arguments for the `crabka-gres` binary.
#[derive(clap::Parser, Debug, Clone)]
#[command(version)]
pub struct Cli {
    /// Single-node service options.
    #[command(flatten)]
    pub serve: ServeArgs,
}

/// Arguments for the default serve mode (no subcommand).
#[derive(clap::Args, Debug, Clone)]
pub struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:5433")]
    pub listen: String,

    /// Path to the server certificate chain (PEM). Enables TLS with --tls-key.
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// Path to the server private key (PEM).
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// Authentication mode: "trust" or "scram". Substrate mode defaults to SCRAM from tenant config.
    #[arg(long)]
    pub auth: Option<String>,

    /// User credentials for --auth scram, as user=password (repeatable).
    #[arg(long = "user-cred", value_name = "USER=PASSWORD")]
    pub user_creds: Vec<String>,

    /// Directory for durable storage. Absent → ephemeral in-memory engine.
    #[arg(long, conflicts_with = "substrate_bootstrap")]
    pub data_dir: Option<PathBuf>,

    /// Substrate mode: Crabka bootstrap address for the tenant WAL topic.
    #[arg(long, requires = "tenant", conflicts_with = "data_dir")]
    pub substrate_bootstrap: Option<String>,

    /// Substrate mode: tenant name (owns `__gres_wal.<tenant>.r0` in single-range live mode).
    #[arg(long, requires = "substrate_bootstrap")]
    pub tenant: Option<String>,

    /// Substrate mode: local read-model cache directory (default: in-memory).
    #[arg(long, requires = "substrate_bootstrap")]
    pub cache_dir: Option<PathBuf>,

    /// In-process memory:// substrate dev/test mode: comma-separated table-start range boundaries, for example 0,100,200.
    #[arg(long, requires = "substrate_bootstrap")]
    pub ranges: Option<String>,

    /// Substrate mode: comma-separated hosted range ids, for example r0,r2.
    #[arg(long = "host-ranges", requires = "ranges")]
    pub host_ranges: Option<String>,

    /// Range-compute RPC address for hosted ranges. Required by deployments
    /// whose registry layout routes any range to this process.
    #[arg(long = "range-listen", requires = "ranges")]
    pub range_listen: Option<String>,

    /// PEM certificate chain used exclusively for range RPC mTLS.
    #[arg(long = "range-tls-cert")]
    pub range_tls_cert: Option<PathBuf>,
    /// PEM private key used exclusively for range RPC mTLS.
    #[arg(long = "range-tls-key")]
    pub range_tls_key: Option<PathBuf>,
    /// PEM CA trusted for range RPC peers and used to verify client certificates.
    #[arg(long = "range-tls-ca")]
    pub range_tls_ca: Option<PathBuf>,
    /// DNS identity verified for remote range RPC servers and sent as TLS SNI.
    #[arg(long = "range-tls-server-name")]
    pub range_tls_server_name: Option<String>,
    /// Client certificate subject DN authorized to execute range RPCs for this tenant.
    #[arg(long = "range-allowed-principal")]
    pub range_allowed_principals: Vec<String>,

    /// Substrate checkpoint object-store backend: s3, gcs, local, or in-memory.
    #[arg(long = "checkpoint-store", value_enum)]
    pub checkpoint_store: Option<CheckpointStoreKind>,

    /// Substrate checkpoint bucket name for S3 or GCS backends.
    #[arg(long = "checkpoint-bucket")]
    pub checkpoint_bucket: Option<String>,

    /// Optional object key prefix inside the checkpoint bucket.
    #[arg(long = "checkpoint-prefix")]
    pub checkpoint_prefix: Option<String>,

    /// Local filesystem root for checkpoint objects.
    #[arg(long = "checkpoint-local-root")]
    pub checkpoint_local_root: Option<PathBuf>,

    /// S3 checkpoint region.
    #[arg(long = "checkpoint-region")]
    pub checkpoint_region: Option<String>,

    /// S3/GCS-compatible checkpoint endpoint URL.
    #[arg(long = "checkpoint-endpoint")]
    pub checkpoint_endpoint: Option<String>,

    /// S3 checkpoint access key id (secret may come from the environment).
    #[arg(long = "checkpoint-access-key-id")]
    pub checkpoint_access_key_id: Option<String>,

    /// S3 checkpoint secret access key (id may come from the environment).
    #[arg(long = "checkpoint-secret-access-key")]
    pub checkpoint_secret_access_key: Option<String>,

    /// Allow plaintext HTTP for checkpoint object-store endpoints.
    #[arg(long = "checkpoint-allow-http")]
    pub checkpoint_allow_http: bool,

    /// GCS checkpoint service-account JSON key file.
    #[arg(long = "checkpoint-gcs-service-account-path")]
    pub checkpoint_gcs_service_account_path: Option<String>,

    /// GCS checkpoint inline service-account JSON key.
    #[arg(long = "checkpoint-gcs-service-account-key")]
    pub checkpoint_gcs_service_account_key: Option<String>,

    /// GCS checkpoint application-default-credentials JSON file.
    #[arg(long = "checkpoint-gcs-application-credentials-path")]
    pub checkpoint_gcs_application_credentials_path: Option<String>,

    /// Checkpoint after at least this many WAL frames since the previous manifest.
    #[arg(long = "checkpoint-frames")]
    pub checkpoint_frames: Option<NonZeroU64>,

    /// Checkpoint after at least this many WAL bytes since the previous manifest.
    #[arg(long = "checkpoint-bytes")]
    pub checkpoint_bytes: Option<NonZeroU64>,

    /// Target maximum bytes per checkpoint part object.
    #[arg(long = "checkpoint-part-bytes")]
    pub checkpoint_part_bytes: Option<NonZeroUsize>,

    /// Number of newest checkpoint directories to retain after pruning.
    #[arg(long = "checkpoint-retain")]
    pub checkpoint_retain: Option<NonZeroUsize>,
}

/// Object-store backend selected for substrate checkpoints.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStoreKind {
    /// S3-compatible object store.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Local filesystem object store.
    Local,
    /// In-process object store for tests and smoke runs.
    InMemory,
}

/// Parsed substrate runtime settings.
#[derive(Debug, Clone)]
pub struct SubstrateRuntimeConfig {
    /// Bootstrap address supplied by the CLI.
    pub bootstrap: String,
    /// Tenant that owns the WAL topic.
    pub tenant: String,
    /// Optional disposable local read-model cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Optional checkpointing configuration. Absent means full WAL replay.
    pub checkpoints: Option<CheckpointRuntimeConfig>,
    /// Optional Kafka SASL credentials for tenant-owned substrate resources.
    pub kafka_security: Option<ClientSecurity>,
    /// Optional in-process multi-range table-start boundaries.
    pub ranges: Option<String>,
    /// Optional range-compute placement for distributed mode. Range 0 is always hosted.
    pub host_ranges: Option<Vec<crabka_gres_ranges::RangeId>>,
    /// mTLS client configuration required for remote range routing.
    pub range_rpc: Option<RangeRpcRuntimeConfig>,
    /// Authenticated endpoint advertised for local range-control operations.
    pub advertised_endpoint: Option<String>,
}

/// Validated TLS-only range RPC configuration.
#[derive(Debug, Clone)]
pub struct RangeRpcRuntimeConfig {
    tls: TlsConfig,
    server_name: String,
    allowed_principals: std::collections::BTreeSet<String>,
}

/// Validated substrate checkpointing settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRuntimeConfig {
    /// Object-store connection settings for checkpoint objects.
    pub object_store: CheckpointObjectStoreConfig,
    /// WAL frames since the last manifest that trigger a checkpoint.
    pub frames_threshold: u64,
    /// WAL bytes since the last manifest that trigger a checkpoint.
    pub bytes_threshold: u64,
    /// Target checkpoint part object size in bytes.
    pub part_max_bytes: usize,
    /// Number of newest checkpoint directories retained after prune planning.
    pub retain_newest: usize,
}

/// Validated object-store settings used by checkpointing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointObjectStoreConfig {
    /// S3-compatible object store.
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// Optional key prefix.
        prefix: Option<String>,
        /// AWS region or compatible-region placeholder.
        region: String,
        /// Optional custom endpoint URL.
        endpoint: Option<String>,
        /// Optional access key id.
        access_key_id: Option<String>,
        /// Optional secret access key.
        secret_access_key: Option<String>,
        /// Whether HTTP endpoints are accepted.
        allow_http: bool,
    },
    /// Google Cloud Storage.
    Gcs {
        /// GCS bucket name.
        bucket: String,
        /// Optional key prefix.
        prefix: Option<String>,
        /// Optional service account key path.
        service_account_path: Option<String>,
        /// Optional inline service account key.
        service_account_key: Option<String>,
        /// Optional ADC file path.
        application_credentials_path: Option<String>,
        /// Optional custom endpoint URL.
        endpoint: Option<String>,
        /// Whether HTTP endpoints are accepted.
        allow_http: bool,
    },
    /// Local filesystem object store.
    Local { root: PathBuf },
    /// In-process object store.
    InMemory,
}

impl SubstrateRuntimeConfig {
    /// Parse substrate settings from serve arguments.
    pub fn from_args(args: &ServeArgs) -> std::io::Result<Option<Self>> {
        use std::io::{Error, ErrorKind};

        let Some(bootstrap) = args.substrate_bootstrap.as_deref() else {
            if checkpointing_was_requested(args) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "checkpoint options require --substrate-bootstrap",
                ));
            }
            if args.tenant.is_some() || args.cache_dir.is_some() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "--tenant and --cache-dir require --substrate-bootstrap",
                ));
            }
            return Ok(None);
        };
        if bootstrap.trim().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "--substrate-bootstrap must not be empty",
            ));
        }
        let tenant = args.tenant.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "--substrate-bootstrap requires --tenant",
            )
        })?;
        if tenant.trim().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "--tenant must not be empty",
            ));
        }
        let ranges = trimmed_optional(args.ranges.as_ref(), "--ranges")?;

        Ok(Some(Self {
            bootstrap: bootstrap.to_string(),
            tenant: tenant.to_string(),
            cache_dir: args.cache_dir.clone(),
            checkpoints: CheckpointRuntimeConfig::from_args(args)?,
            kafka_security: tenant_kafka_security_from_env(tenant),
            ranges,
            host_ranges: parse_host_ranges(args.host_ranges.as_deref())?,
            range_rpc: RangeRpcRuntimeConfig::from_args(args)?,
            advertised_endpoint: args.range_listen.clone(),
        }))
    }

    fn is_in_memory_bootstrap(&self) -> bool {
        matches!(self.bootstrap.as_str(), "memory://" | "in-memory://")
    }
}

impl RangeRpcRuntimeConfig {
    fn from_args(args: &ServeArgs) -> std::io::Result<Option<Self>> {
        let configured = args.range_tls_cert.is_some()
            || args.range_tls_key.is_some()
            || args.range_tls_ca.is_some()
            || args.range_tls_server_name.is_some()
            || !args.range_allowed_principals.is_empty();
        if !configured {
            return Ok(None);
        }
        let missing = |flag| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("range TLS requires {flag}"),
            )
        };
        let cert_chain_path = args
            .range_tls_cert
            .clone()
            .ok_or_else(|| missing("--range-tls-cert"))?;
        let private_key_path = args
            .range_tls_key
            .clone()
            .ok_or_else(|| missing("--range-tls-key"))?;
        let ca = args
            .range_tls_ca
            .clone()
            .ok_or_else(|| missing("--range-tls-ca"))?;
        let server_name = args
            .range_tls_server_name
            .as_deref()
            .ok_or_else(|| missing("--range-tls-server-name"))?
            .trim();
        if server_name.is_empty() {
            return invalid_input("--range-tls-server-name must not be empty");
        }
        let allowed_principals = args
            .range_allowed_principals
            .iter()
            .map(|principal| principal.trim())
            .filter(|principal| !principal.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        Ok(Some(Self {
            tls: TlsConfig {
                cert_chain_path,
                private_key_path,
                trust_roots_path: Some(ca.clone()),
                client_ca_path: Some(ca),
                client_auth: ClientAuthMode::Required,
            },
            server_name: server_name.to_owned(),
            allowed_principals,
        }))
    }

    fn client(&self) -> std::io::Result<crabka_gres_ranges::FramedTcpClient> {
        crabka_gres_ranges::FramedTcpClient::with_tls(crabka_gres_ranges::RangeTlsClientConfig {
            tls: self.tls.clone(),
            server_name: self.server_name.clone(),
        })
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }

    fn server(&self, tenant: String) -> crabka_gres_ranges::RangeTlsServerConfig {
        crabka_gres_ranges::RangeTlsServerConfig {
            tenant,
            tls: self.tls.clone(),
            allowed_principals: self.allowed_principals.clone(),
        }
    }
}

impl CheckpointRuntimeConfig {
    fn from_args(args: &ServeArgs) -> std::io::Result<Option<Self>> {
        if !checkpointing_was_requested(args) {
            return Ok(None);
        }
        if args.substrate_bootstrap.is_none() {
            return invalid_input("checkpoint options require --substrate-bootstrap");
        }

        let object_store = CheckpointObjectStoreConfig::from_args(args)?;
        let part_max_bytes = args.checkpoint_part_bytes.map_or(
            crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
            NonZeroUsize::get,
        );
        if part_max_bytes < 8 {
            return invalid_input("--checkpoint-part-bytes must be at least 8");
        }
        Ok(Some(Self {
            object_store,
            frames_threshold: args
                .checkpoint_frames
                .map_or(DEFAULT_CHECKPOINT_FRAMES_THRESHOLD, NonZeroU64::get),
            bytes_threshold: args
                .checkpoint_bytes
                .map_or(DEFAULT_CHECKPOINT_BYTES_THRESHOLD, NonZeroU64::get),
            part_max_bytes,
            retain_newest: args
                .checkpoint_retain
                .map_or(DEFAULT_CHECKPOINT_RETAIN_NEWEST, NonZeroUsize::get),
        }))
    }
}

impl CheckpointObjectStoreConfig {
    fn from_args(args: &ServeArgs) -> std::io::Result<Self> {
        let kind = infer_checkpoint_store_kind(args)?;
        match kind {
            CheckpointStoreKind::S3 => Self::s3_from_args(args),
            CheckpointStoreKind::Gcs => Self::gcs_from_args(args),
            CheckpointStoreKind::Local => Self::local_from_args(args),
            CheckpointStoreKind::InMemory => Self::in_memory_from_args(args),
        }
    }

    fn s3_from_args(args: &ServeArgs) -> std::io::Result<Self> {
        reject_local_only_args(args, "s3")?;
        reject_gcs_only_args(args, "s3")?;
        let bucket = required_trimmed(args.checkpoint_bucket.as_ref(), "--checkpoint-bucket")?;
        let region = required_trimmed(args.checkpoint_region.as_ref(), "--checkpoint-region")?;
        let env_access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let env_secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let (access_key_id, secret_access_key) = resolve_s3_credentials(
            args.checkpoint_access_key_id.as_ref(),
            args.checkpoint_secret_access_key.as_ref(),
            env_access_key.as_ref(),
            env_secret_key.as_ref(),
        )?;
        Ok(Self::S3 {
            bucket,
            prefix: parse_checkpoint_prefix(args)?,
            region,
            endpoint: trimmed_optional(args.checkpoint_endpoint.as_ref(), "--checkpoint-endpoint")?,
            access_key_id,
            secret_access_key,
            allow_http: args.checkpoint_allow_http,
        })
    }

    fn gcs_from_args(args: &ServeArgs) -> std::io::Result<Self> {
        reject_local_only_args(args, "gcs")?;
        reject_s3_only_args(args, "gcs")?;
        let bucket = required_trimmed(args.checkpoint_bucket.as_ref(), "--checkpoint-bucket")?;
        Ok(Self::Gcs {
            bucket,
            prefix: parse_checkpoint_prefix(args)?,
            service_account_path: trimmed_optional(
                args.checkpoint_gcs_service_account_path.as_ref(),
                "--checkpoint-gcs-service-account-path",
            )?,
            service_account_key: trimmed_optional(
                args.checkpoint_gcs_service_account_key.as_ref(),
                "--checkpoint-gcs-service-account-key",
            )?,
            application_credentials_path: trimmed_optional(
                args.checkpoint_gcs_application_credentials_path.as_ref(),
                "--checkpoint-gcs-application-credentials-path",
            )?,
            endpoint: trimmed_optional(args.checkpoint_endpoint.as_ref(), "--checkpoint-endpoint")?,
            allow_http: args.checkpoint_allow_http,
        })
    }

    fn local_from_args(args: &ServeArgs) -> std::io::Result<Self> {
        reject_bucket_args(args, "local")?;
        reject_s3_only_args(args, "local")?;
        reject_gcs_only_args(args, "local")?;
        reject_shared_remote_args(args, "local")?;
        let Some(root) = &args.checkpoint_local_root else {
            return invalid_input("--checkpoint-store local requires --checkpoint-local-root");
        };
        Ok(Self::Local { root: root.clone() })
    }

    fn in_memory_from_args(args: &ServeArgs) -> std::io::Result<Self> {
        reject_bucket_args(args, "in-memory")?;
        reject_local_only_args(args, "in-memory")?;
        reject_s3_only_args(args, "in-memory")?;
        reject_gcs_only_args(args, "in-memory")?;
        reject_shared_remote_args(args, "in-memory")?;
        Ok(Self::InMemory)
    }

    fn to_object_store_config(&self) -> crabka_object_store::ObjectStoreConfig {
        match self {
            Self::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
            } => crabka_object_store::ObjectStoreConfig::S3(crabka_object_store::S3Config {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                region: region.clone(),
                endpoint: endpoint.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                allow_http: *allow_http,
                ..Default::default()
            }),
            Self::Gcs {
                bucket,
                prefix,
                service_account_path,
                service_account_key,
                application_credentials_path,
                endpoint,
                allow_http,
            } => crabka_object_store::ObjectStoreConfig::Gcs(crabka_object_store::GcsConfig {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                service_account_path: service_account_path.clone(),
                service_account_key: service_account_key.clone(),
                application_credentials_path: application_credentials_path.clone(),
                endpoint: endpoint.clone(),
                allow_http: *allow_http,
                ..Default::default()
            }),
            Self::Local { root } => {
                crabka_object_store::ObjectStoreConfig::Local { root: root.clone() }
            }
            Self::InMemory => crabka_object_store::ObjectStoreConfig::InMemory,
        }
    }
}

fn resolve_s3_credentials(
    cli_access_key: Option<&String>,
    cli_secret_key: Option<&String>,
    env_access_key: Option<&String>,
    env_secret_key: Option<&String>,
) -> std::io::Result<(Option<String>, Option<String>)> {
    let access_key = trimmed_optional(
        cli_access_key.or(env_access_key),
        "--checkpoint-access-key-id/AWS_ACCESS_KEY_ID",
    )?;
    let secret_key = trimmed_optional(
        cli_secret_key.or(env_secret_key),
        "--checkpoint-secret-access-key/AWS_SECRET_ACCESS_KEY",
    )?;
    if access_key.is_some() != secret_key.is_some() {
        return invalid_input(
            "S3 checkpoint access key id and secret access key must be set together",
        );
    }
    Ok((access_key, secret_key))
}

/// Build the checkpoint object-store adapter selected by validated CLI settings.
pub fn build_checkpoint_store(
    config: &CheckpointRuntimeConfig,
) -> std::io::Result<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>> {
    let object_store_config = config.object_store.to_object_store_config();
    let object_store = crabka_object_store::build_object_store(&object_store_config)
        .map_err(|error| std::io::Error::other(format!("checkpoint object store: {error}")))?;
    let ops = Arc::new(crabka_object_store::ObjectStoreClient::new(object_store));
    Ok(Arc::new(
        crabka_gres_substrate::checkpoint::ObjectOpsCheckpointStore::new(ops),
    ))
}

/// Runtime resources that must live for as long as the pgwire server serves the engine.
pub struct GresRuntime {
    /// SQL execution engine used by pgwire sessions.
    pub engine: RuntimeEngine,
    checkpoint_runtime: Option<StartedCheckpointRuntime>,
    range_service: Option<Arc<dyn crabka_gres_ranges::RangeService>>,
    range_transfer: Option<Arc<dyn crabka_gres_ranges::RangeTransferCapability>>,
    staged_transfer: Option<Arc<LiveMultiRangeTransfer>>,
}

/// Test-only fault points for live-topology preparation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PrepareTopologyFault {
    None = 0,
    LockAcquisition = 1,
    HorizonLoad = 2,
    TsoConstruction = 3,
    ServiceAssembly = 4,
}

/// Test-only one-shot crash points in the durable topology-activation protocol.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TopologyActivationFault {
    None = 0,
    Prepared = 1,
    SourceCheckpoint = 2,
    FirstWriterActivated = 3,
    SecondWriterActivated = 4,
    FirstCheckpointDurable = 5,
    SecondCheckpointDurable = 6,
    CheckpointDurable = 7,
    TopologySwap = 8,
    TopologyCommitted = 9,
    BeforeMustActivate = 10,
    AfterMustActivate = 11,
    BeforeProducerInit = 12,
    AfterProducerInit = 13,
    BeforeDeferredBind = 14,
    AfterDeferredBind = 15,
}

impl GresRuntime {
    fn new(engine: SqlEngine) -> Self {
        Self {
            engine: RuntimeEngine::Single(Box::new(engine)),
            checkpoint_runtime: None,
            range_service: None,
            range_transfer: None,
            staged_transfer: None,
        }
    }

    fn multi(engine: crabka_gres_ranges::MultiRangeTenant) -> Self {
        let mut range_service =
            crabka_gres_ranges::HostedRangeService::new(engine.hosted_range_engines());
        if let Some((registry, client)) = engine.timestamp_primary_remote() {
            range_service = range_service.with_timestamp_primary_remote(registry, client);
        }
        let range_service = Arc::new(range_service);
        Self {
            engine: RuntimeEngine::Multi(Box::new(engine)),
            checkpoint_runtime: None,
            range_service: Some(range_service),
            range_transfer: None,
            staged_transfer: None,
        }
    }

    fn with_checkpoint_runtime(
        engine: SqlEngine,
        checkpoint_runtime: StartedCheckpointRuntime,
    ) -> Self {
        Self {
            engine: RuntimeEngine::Single(Box::new(engine)),
            checkpoint_runtime: Some(checkpoint_runtime),
            range_service: None,
            range_transfer: None,
            staged_transfer: None,
        }
    }

    /// Return whether a background checkpointer control loop was started.
    #[must_use]
    pub fn has_checkpoint_handle(&self) -> bool {
        self.checkpoint_runtime.is_some()
    }

    fn into_parts(self) -> (RuntimeEngine, Option<StartedCheckpointRuntime>) {
        (self.engine, self.checkpoint_runtime)
    }

    fn range_service(&self) -> Option<Arc<dyn crabka_gres_ranges::RangeService>> {
        self.range_service.clone()
    }

    /// Exercise the currently published authenticated range-service topology.
    #[doc(hidden)]
    pub async fn handle_range_request(
        &self,
        request: crabka_gres_ranges::RangeRequest,
    ) -> Option<crabka_gres_ranges::RangeResponse> {
        let service = self.range_service.as_ref()?;
        Some(service.handle(request).await)
    }

    /// Verify durable range-control receipts through the currently published r0 engine.
    #[doc(hidden)]
    pub async fn verify_current_range0_receipt_store(&self) -> Result<(), String> {
        let transfer = self
            .staged_transfer
            .as_ref()
            .ok_or_else(|| "live topology unavailable".to_owned())?;
        let engine = transfer
            .engines
            .read()
            .map_err(|_| "live topology lock poisoned".to_owned())?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .ok_or_else(|| "replacement r0 unavailable".to_owned())?
            .clone_handle();
        let request = crabka_gres_ranges::transport::RangeControlReq {
            tenant: transfer.config.tenant.clone(),
            range_id: crabka_gres_ranges::RangeId::COORDINATOR,
            generation: transfer
                .ranges
                .read()
                .map_err(|_| "live resources lock poisoned".to_owned())?
                .get(&crabka_gres_ranges::RangeId::COORDINATOR)
                .ok_or_else(|| "replacement r0 resources unavailable".to_owned())?
                .generation
                .0,
            operation_id: "post-r0-receipt".into(),
            operation: crabka_gres_ranges::transport::RangeControlOperation::Status,
        };
        let receipt = crabka_gres_ranges::control::RangeControlReceipt {
            request,
            request_digest: "post-r0-receipt-digest".into(),
            generation: 1,
            revision: 0,
            result: Some(crabka_gres_ranges::transport::RangeControlResp::Applied),
        };
        let first = crabka_gres_ranges::control::RangeZeroReceiptStore::new(
            transfer.config.tenant.clone(),
            engine.clone_handle(),
        );
        if !crabka_gres_ranges::control::RangeControlReceiptStore::compare_and_swap(
            &first,
            "post-r0-receipt",
            None,
            receipt.clone(),
        )
        .await?
        {
            return Err("replacement r0 receipt CAS failed".into());
        }
        let reopened = crabka_gres_ranges::control::RangeZeroReceiptStore::new(
            transfer.config.tenant.clone(),
            engine,
        );
        let loaded = crabka_gres_ranges::control::RangeControlReceiptStore::load(
            &reopened,
            "post-r0-receipt",
        )
        .await?;
        (loaded == Some(receipt))
            .then_some(())
            .ok_or_else(|| "replacement r0 receipt did not replay".into())
    }

    /// Inject one fail-before-mutation topology preparation fault.
    #[doc(hidden)]
    pub fn inject_prepare_topology_fault(&self, fault: PrepareTopologyFault) {
        if let Some(transfer) = &self.staged_transfer {
            transfer
                .prepare_fault
                .store(fault as u8, std::sync::atomic::Ordering::Release);
        }
    }

    /// Inject one fail-after-durable-transition activation crash.
    #[doc(hidden)]
    pub fn inject_topology_activation_fault(&self, fault: TopologyActivationFault) {
        if let Some(transfer) = &self.staged_transfer {
            transfer
                .activation_fault
                .store(fault as u8, std::sync::atomic::Ordering::Release);
        }
    }

    /// Return the hosted-range transfer foundation when this is a live multi-range runtime.
    #[must_use]
    pub fn range_transfer_capability(
        &self,
    ) -> Option<Arc<dyn crabka_gres_ranges::RangeTransferCapability>> {
        self.range_transfer.clone()
    }

    /// Return the currently published range map for a multi-range runtime.
    #[doc(hidden)]
    #[must_use]
    pub fn published_range_map(&self) -> Option<crabka_gres_ranges::RangeMap> {
        match &self.engine {
            RuntimeEngine::Multi(tenant) => Some(tenant.control_range_map()),
            RuntimeEngine::Single(_) => None,
        }
    }

    /// Physically move one populated ordinary table in a local live multi-range runtime.
    ///
    /// This only publishes to the in-process serving map. It deliberately does not
    /// mutate the control registry or coordinate a distributed operator workflow.
    pub async fn split_successors(
        &self,
        operation_id: impl Into<String>,
        command: crabka_gres_ranges::SplitCommand,
    ) -> Result<crabka_gres_ranges::SplitState, crabka_gres_ranges::LocalSqlSplitError> {
        let RuntimeEngine::Multi(tenant) = &self.engine else {
            return Err(crabka_gres_ranges::LocalSqlSplitError::Orchestration(
                crabka_gres_ranges::SplitError::Hook(
                    "populated table transfer requires a live multi-range runtime".to_owned(),
                ),
            ));
        };
        let transfer = self.staged_transfer.as_ref().ok_or_else(|| {
            crabka_gres_ranges::LocalSqlSplitError::Orchestration(
                crabka_gres_ranges::SplitError::Hook(
                    "populated table transfer requires live substrate staging".to_owned(),
                ),
            )
        })?;
        tenant
            .split_successors(operation_id, command, transfer.as_ref())
            .await
    }

    /// Return a raw snapshot of one hosted range for integration-test inspection.
    ///
    /// This does not expose a serving path and is deliberately limited to raw
    /// local KV state, so tests cannot mistake a catalog-backed SQL read for a
    /// physical transfer assertion.
    #[doc(hidden)]
    pub fn inspect_hosted_range_kv(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> std::io::Result<KvScan> {
        let RuntimeEngine::Multi(tenant) = &self.engine else {
            return invalid_input("hosted range inspection requires a multi-range runtime");
        };
        let engines = tenant.hosted_range_engines();
        let engine = engines.get(&range_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("hosted range r{range_id} is absent"),
            )
        })?;
        engine
            .kv_handle()
            .scan_range(&[], &[u8::MAX])
            .map_err(|error| std::io::Error::other(format!("inspect hosted range KV: {error}")))
    }

    /// Return a raw snapshot of an unserved staged successor for integration tests.
    #[doc(hidden)]
    pub fn inspect_staged_successor_kv(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> std::io::Result<Option<KvScan>> {
        let Some(transfer) = &self.staged_transfer else {
            return Ok(None);
        };
        transfer
            .staged_successor_kv(range_id)
            .map_err(|error| std::io::Error::other(format!("inspect staged successor KV: {error}")))
    }
}

/// Engine enum used by the binary so the single-range path stays unchanged while
/// substrate mode can host an in-process multi-range gateway.
pub enum RuntimeEngine {
    /// Single local/substrate SQL engine.
    Single(Box<SqlEngine>),
    /// In-process multi-range gateway.
    Multi(Box<crabka_gres_ranges::MultiRangeTenant>),
}

/// Per-connection session for [`RuntimeEngine`].
pub enum RuntimeSession {
    /// Single-engine session.
    Single(Box<crabka_pgexec::SqlSession>),
    /// Multi-range gateway session.
    Multi(crabka_gres_ranges::tenant::GatewaySession),
}

impl Engine for RuntimeEngine {
    type Session = RuntimeSession;

    fn connect(&self) -> Self::Session {
        match self {
            Self::Single(engine) => RuntimeSession::Single(Box::new(engine.connect())),
            Self::Multi(engine) => RuntimeSession::Multi(engine.connect()),
        }
    }
}

impl Session for RuntimeSession {
    async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<QueryResult>, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.simple_query(sql).await,
            Self::Multi(session) => session.simple_query(sql).await,
        }
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> Result<PreparedDescription, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.parse(name, sql, parameter_types).await,
            Self::Multi(session) => session.parse(name, sql, parameter_types).await,
        }
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => {
                session
                    .bind(portal, statement, params, result_formats)
                    .await
            }
            Self::Multi(session) => {
                session
                    .bind(portal, statement, params, result_formats)
                    .await
            }
        }
    }

    async fn describe_statement(
        &mut self,
        name: &str,
    ) -> Result<PreparedDescription, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.describe_statement(name).await,
            Self::Multi(session) => session.describe_statement(name).await,
        }
    }

    async fn describe_portal(
        &mut self,
        name: &str,
    ) -> Result<PortalDescription, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.describe_portal(name).await,
            Self::Multi(session) => session.describe_portal(name).await,
        }
    }

    async fn execute(
        &mut self,
        portal: &str,
        max_rows: u32,
    ) -> Result<ExecuteOutcome, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.execute(portal, max_rows).await,
            Self::Multi(session) => session.execute(portal, max_rows).await,
        }
    }

    async fn close(
        &mut self,
        target: CloseTarget<'_>,
    ) -> Result<(), crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.close(target).await,
            Self::Multi(session) => session.close(target).await,
        }
    }

    async fn sync(&mut self) -> Result<(), crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.sync().await,
            Self::Multi(session) => session.sync().await,
        }
    }

    fn tx_status(&self) -> TxStatus {
        match self {
            Self::Single(session) => session.tx_status(),
            Self::Multi(session) => session.tx_status(),
        }
    }
}

/// Result of one self-suspend monitor iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuspendMonitorOutcome {
    /// The tenant is not configured for idle suspension.
    Disabled,
    /// The tenant still has one or more open sessions.
    OpenSessions { count: usize },
    /// The tenant is not idle long enough yet.
    IdleWindowNotElapsed,
    /// Admission was already closed by another suspend attempt.
    AdmissionAlreadyClosed,
    /// A session raced with admission close, so this attempt was aborted.
    RacedSession { count: usize },
    /// Checkpoint size gate was exceeded, so the tenant remains warm.
    CheckpointTooLarge { bytes: u64, max_bytes: u64 },
    /// A final checkpoint was durable and the registry was marked suspended.
    Suspended,
}

/// Minimal registry seam used by the compute self-suspend path.
#[async_trait::async_trait]
pub trait SuspendRegistry: Send {
    /// Persist the tenant as suspended after a final checkpoint is durable.
    async fn mark_suspended(
        &mut self,
        tenant: &str,
        checkpoint: FinalCheckpoint,
    ) -> std::io::Result<()>;
}

/// Minimal final-checkpoint seam used by the compute self-suspend path.
#[async_trait::async_trait]
pub trait FinalCheckpointer: Send + Sync {
    /// Return the latest checkpoint size estimate used by the suspend size gate.
    async fn latest_checkpoint_bytes(&self) -> std::io::Result<u64>;

    /// Force and await a durable final checkpoint manifest.
    async fn force_final_checkpoint(&self) -> std::io::Result<FinalCheckpoint>;
}

struct LiveSuspendRegistry {
    registry: crabka_gres_control::Registry,
}

#[async_trait::async_trait]
impl SuspendRegistry for LiveSuspendRegistry {
    async fn mark_suspended(
        &mut self,
        tenant: &str,
        checkpoint: FinalCheckpoint,
    ) -> std::io::Result<()> {
        self.registry
            .mark_suspended_after_checkpoint(tenant, checkpoint)
            .await
            .map_err(|error| std::io::Error::other(format!("mark tenant suspended: {error}")))
    }
}

/// Configuration for substrate idle self-suspension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuspendPolicy {
    /// Tenant name written to the registry.
    pub tenant: String,
    /// Idle window before suspend. Zero disables self-suspend.
    pub idle_window: Duration,
    /// Optional checkpoint size gate.
    pub suspend_max_checkpoint_bytes: Option<u64>,
}

impl SuspendPolicy {
    fn from_tenant_record(record: Option<&TenantRecord>) -> Option<Self> {
        let record = record?;
        let idle_seconds = record.idle_seconds?;
        if idle_seconds == 0 {
            return None;
        }

        Some(Self {
            tenant: record.name.as_str().to_string(),
            idle_window: Duration::from_secs(idle_seconds),
            suspend_max_checkpoint_bytes: record.suspend_max_checkpoint_bytes,
        })
    }
}

/// Run one suspend attempt if the activity state is eligible.
pub async fn try_suspend_idle_tenant(
    policy: &SuspendPolicy,
    activity: &crabka_pgwire::server::ActivityTracker,
    checkpointer: &dyn FinalCheckpointer,
    registry: &mut dyn SuspendRegistry,
) -> std::io::Result<SuspendMonitorOutcome> {
    if policy.idle_window.is_zero() {
        return Ok(SuspendMonitorOutcome::Disabled);
    }

    let open_sessions = activity.open_sessions();
    if open_sessions != 0 {
        return Ok(SuspendMonitorOutcome::OpenSessions {
            count: open_sessions,
        });
    }

    if !idle_window_elapsed(activity.last_activity_unix_millis(), policy.idle_window) {
        return Ok(SuspendMonitorOutcome::IdleWindowNotElapsed);
    }

    if activity.close_for_suspend().is_err() {
        return Ok(SuspendMonitorOutcome::AdmissionAlreadyClosed);
    }

    let open_sessions = activity.open_sessions();
    if open_sessions != 0 {
        activity.reopen_after_suspend_abort();
        return Ok(SuspendMonitorOutcome::RacedSession {
            count: open_sessions,
        });
    }

    let checkpoint_bytes = checkpointer.latest_checkpoint_bytes().await?;
    if let Some(max_bytes) = policy.suspend_max_checkpoint_bytes
        && checkpoint_bytes > max_bytes
    {
        activity.reopen_after_suspend_abort();
        tracing::info!(
            tenant = %policy.tenant,
            checkpoint_bytes,
            max_bytes,
            "skip idle suspend because checkpoint exceeds configured size gate"
        );
        return Ok(SuspendMonitorOutcome::CheckpointTooLarge {
            bytes: checkpoint_bytes,
            max_bytes,
        });
    }

    let checkpoint = checkpointer.force_final_checkpoint().await?;
    registry.mark_suspended(&policy.tenant, checkpoint).await?;
    tracing::info!(tenant = %policy.tenant, "idle tenant suspended after final checkpoint");
    Ok(SuspendMonitorOutcome::Suspended)
}

fn idle_window_elapsed(last_activity_unix_millis: u64, idle_window: Duration) -> bool {
    let Some(now) = current_unix_millis() else {
        return false;
    };
    let idle_millis = now.saturating_sub(last_activity_unix_millis);
    idle_millis >= u64::try_from(idle_window.as_millis()).unwrap_or(u64::MAX)
}

fn current_unix_millis() -> Option<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    u64::try_from(millis).ok()
}

struct GresCheckpointWalPruner {
    bootstrap: CheckpointPruneBackend,
    security: Option<ClientSecurity>,
}

enum CheckpointPruneBackend {
    InMemory,
    Kafka { bootstrap_addrs: Vec<String> },
}

impl GresCheckpointWalPruner {
    fn in_memory() -> Self {
        Self {
            bootstrap: CheckpointPruneBackend::InMemory,
            security: None,
        }
    }

    fn kafka(bootstrap: &str, security: Option<ClientSecurity>) -> std::io::Result<Self> {
        let bootstrap_addrs: Vec<_> = bootstrap
            .split(',')
            .map(str::trim)
            .filter(|addr| !addr.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        if bootstrap_addrs.is_empty() {
            return invalid_input("substrate bootstrap address list is empty");
        }
        Ok(Self {
            bootstrap: CheckpointPruneBackend::Kafka { bootstrap_addrs },
            security,
        })
    }
}

#[async_trait::async_trait]
impl crabka_gres_substrate::CheckpointWalPruner for GresCheckpointWalPruner {
    async fn delete_records(
        &self,
        ops: &[crabka_client_admin::DeleteRecordsOp],
    ) -> Result<(), crabka_gres_substrate::SubstrateError> {
        if ops.is_empty() {
            return Ok(());
        }
        let CheckpointPruneBackend::Kafka { bootstrap_addrs } = &self.bootstrap else {
            return Ok(());
        };

        let mut admin = crabka_client_admin::AdminClient::connect_secured(
            bootstrap_addrs,
            self.security.clone(),
        )
        .await
        .map_err(|error| {
            crabka_gres_substrate::SubstrateError::Unavailable(format!(
                "checkpoint pruner admin connect: {error}"
            ))
        })?;
        let outcomes = admin
            .delete_records(ops, CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS)
            .await
            .map_err(|error| {
                crabka_gres_substrate::SubstrateError::Unavailable(format!(
                    "checkpoint delete records: {error}"
                ))
            })?;
        if let Some(outcome) = outcomes.iter().find(|outcome| outcome.error_code != 0) {
            return Err(crabka_gres_substrate::SubstrateError::Unavailable(format!(
                "checkpoint delete records for {} partition {} failed with error code {}",
                outcome.topic, outcome.partition, outcome.error_code
            )));
        }
        Ok(())
    }
}

fn checkpointing_was_requested(args: &ServeArgs) -> bool {
    args.checkpoint_store.is_some()
        || args.checkpoint_bucket.is_some()
        || args.checkpoint_prefix.is_some()
        || args.checkpoint_local_root.is_some()
        || args.checkpoint_region.is_some()
        || args.checkpoint_endpoint.is_some()
        || args.checkpoint_access_key_id.is_some()
        || args.checkpoint_secret_access_key.is_some()
        || args.checkpoint_allow_http
        || args.checkpoint_gcs_service_account_path.is_some()
        || args.checkpoint_gcs_service_account_key.is_some()
        || args.checkpoint_gcs_application_credentials_path.is_some()
        || args.checkpoint_frames.is_some()
        || args.checkpoint_bytes.is_some()
        || args.checkpoint_part_bytes.is_some()
        || args.checkpoint_retain.is_some()
}

fn infer_checkpoint_store_kind(args: &ServeArgs) -> std::io::Result<CheckpointStoreKind> {
    if let Some(kind) = args.checkpoint_store {
        return Ok(kind);
    }
    if args.checkpoint_local_root.is_some() {
        return Ok(CheckpointStoreKind::Local);
    }
    if args.checkpoint_bucket.is_some() {
        return Ok(CheckpointStoreKind::S3);
    }
    invalid_input(
        "checkpoint thresholds require --checkpoint-bucket, --checkpoint-local-root, or --checkpoint-store in-memory",
    )
}

fn parse_checkpoint_prefix(args: &ServeArgs) -> std::io::Result<Option<String>> {
    let Some(prefix) = trimmed_optional(args.checkpoint_prefix.as_ref(), "--checkpoint-prefix")?
    else {
        return Ok(None);
    };
    if prefix.starts_with('/') || prefix.ends_with('/') {
        return invalid_input("--checkpoint-prefix must not start or end with '/'");
    }
    Ok(Some(prefix))
}

fn required_trimmed(value: Option<&String>, flag: &str) -> std::io::Result<String> {
    let Some(trimmed) = trimmed_optional(value, flag)? else {
        return invalid_input(format!(
            "{flag} is required for the selected checkpoint store"
        ));
    };
    Ok(trimmed)
}

fn trimmed_optional(value: Option<&String>, flag: &str) -> std::io::Result<Option<String>> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return invalid_input(format!("{flag} must not be empty"));
    }
    Ok(Some(trimmed.to_owned()))
}

fn split_bootstrap(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn tenant_kafka_security_from_env(tenant: &str) -> Option<ClientSecurity> {
    let username =
        std::env::var("GRES_KAFKA_USERNAME").unwrap_or_else(|_| format!("gres-{tenant}"));
    let Ok(password) = std::env::var("GRES_KAFKA_PASSWORD") else {
        return None;
    };
    Some(ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username,
            password,
        }),
        sasl_host: None,
    })
}

fn resolve_bootstrap_addr(bootstrap: &str) -> Option<SocketAddr> {
    bootstrap
        .split(',')
        .filter_map(|entry| entry.trim().to_socket_addrs().ok())
        .find_map(|mut addrs| addrs.next())
}

fn reject_bucket_args(args: &ServeArgs, backend: &str) -> std::io::Result<()> {
    if args.checkpoint_bucket.is_some() || args.checkpoint_prefix.is_some() {
        return invalid_input(format!(
            "--checkpoint-bucket/--checkpoint-prefix are incompatible with --checkpoint-store {backend}",
        ));
    }
    Ok(())
}

fn reject_local_only_args(args: &ServeArgs, backend: &str) -> std::io::Result<()> {
    if args.checkpoint_local_root.is_some() {
        return invalid_input(format!(
            "--checkpoint-local-root is incompatible with --checkpoint-store {backend}",
        ));
    }
    Ok(())
}

fn reject_s3_only_args(args: &ServeArgs, backend: &str) -> std::io::Result<()> {
    if args.checkpoint_region.is_some()
        || args.checkpoint_access_key_id.is_some()
        || args.checkpoint_secret_access_key.is_some()
    {
        return invalid_input(format!(
            "S3 checkpoint flags are incompatible with --checkpoint-store {backend}",
        ));
    }
    Ok(())
}

fn reject_gcs_only_args(args: &ServeArgs, backend: &str) -> std::io::Result<()> {
    if args.checkpoint_gcs_service_account_path.is_some()
        || args.checkpoint_gcs_service_account_key.is_some()
        || args.checkpoint_gcs_application_credentials_path.is_some()
    {
        return invalid_input(format!(
            "GCS checkpoint flags are incompatible with --checkpoint-store {backend}",
        ));
    }
    Ok(())
}

fn reject_shared_remote_args(args: &ServeArgs, backend: &str) -> std::io::Result<()> {
    if args.checkpoint_endpoint.is_some() || args.checkpoint_allow_http {
        return invalid_input(format!(
            "remote checkpoint endpoint flags are incompatible with --checkpoint-store {backend}",
        ));
    }
    Ok(())
}

fn invalid_input<T>(message: impl Into<String>) -> std::io::Result<T> {
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

/// Build the TLS acceptor used for `PostgreSQL` `SSLRequest` upgrades.
pub fn tls_acceptor(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use std::io::{BufReader, Error, ErrorKind};

    let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(key_path)?))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no private key in file"))?;
    let provider = Arc::new(rustls_rustcrypto::provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Run the single-node pgwire service, binding the configured listener address.
pub async fn run_serve(args: ServeArgs) -> std::io::Result<()> {
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = %listener.local_addr()?, "crabka-gres listening");
    Box::pin(serve_listener(listener, args)).await
}

/// Run the single-node pgwire service on an already-bound listener.
pub async fn serve_listener(listener: TcpListener, args: ServeArgs) -> std::io::Result<()> {
    serve_listener_with_tenant_config_loader(listener, args, &LiveTenantConfigLoader).await
}

/// Run the pgwire service with an injected tenant-config loader for tests.
pub async fn serve_listener_with_tenant_config_loader(
    listener: TcpListener,
    args: ServeArgs,
    tenant_config_loader: &impl TenantConfigLoader,
) -> std::io::Result<()> {
    let sql_addr = listener.local_addr()?;
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(tls_acceptor(cert, key)?),
        _ => None,
    };

    let mut tenant_record = load_substrate_tenant_record(&args, tenant_config_loader).await?;
    let mut lifecycle_registry = None;
    if let (Some(record), Some(bootstrap)) =
        (tenant_record.as_ref(), args.substrate_bootstrap.as_deref())
        && !matches!(bootstrap, "memory://" | "in-memory://")
    {
        let mut registry = crabka_gres_control::Registry::connect(bootstrap)
            .await
            .map_err(|error| std::io::Error::other(format!("tenant registry connect: {error}")))?;
        registry
            .ensure_topic(1)
            .await
            .map_err(|error| std::io::Error::other(format!("tenant registry ensure: {error}")))?;
        tenant_record = registry
            .get(record.name.as_str())
            .await
            .map_err(|error| std::io::Error::other(format!("tenant registry read: {error}")))?;
        lifecycle_registry = Some(registry);
    }
    let effective_args = apply_tenant_runtime_defaults(args, tenant_record.as_ref())?;
    let mut runtime =
        open_runtime_with_tenant_record(&effective_args, tenant_record.as_ref()).await?;
    register_kafka_scanner_with_default_bootstrap(
        &mut runtime.engine,
        kafka_scanner_default_bootstrap(&effective_args),
    );
    let session_config = build_session_config_from_tenant(&effective_args, tenant_record.as_ref())?;

    let range_service = runtime.range_service();
    let (engine, checkpoint_runtime) = runtime.into_parts();
    let activity = Arc::new(crabka_pgwire::server::ActivityTracker::new());
    let shutdown = CancellationToken::new();
    let serve = crabka_pgwire::server::serve_tls_with_activity_until(
        listener,
        Arc::new(engine),
        Arc::new(session_config),
        tls,
        Arc::clone(&activity),
        shutdown.clone(),
    );

    let range_server = start_range_service(&effective_args, range_service).await?;
    let checkpointer = live_final_checkpointer(checkpoint_runtime);
    let registry = mark_active_after_recovery(tenant_record.as_ref(), lifecycle_registry).await?;
    let suspend = if let Some(policy) = SuspendPolicy::from_tenant_record(tenant_record.as_ref()) {
        match (registry, checkpointer) {
            (Some(registry), Some(checkpointer)) => Some((
                policy,
                Box::new(LiveSuspendRegistry { registry }) as Box<dyn SuspendRegistry>,
                checkpointer,
            )),
            (None, _) => {
                tracing::warn!(tenant = %policy.tenant, "substrate idle suspend disabled without live registry bootstrap");
                None
            }
            (_, None) => {
                tracing::warn!(tenant = %policy.tenant, "substrate idle suspend disabled: final checkpoint snapshot seam unavailable");
                None
            }
        }
    } else {
        None
    };

    // Publish Active only after every potentially blocking runtime component is
    // initialized. The activator treats Active as permission to connect, so an
    // earlier write can leave its held startup queued on a bound-but-unpolled
    // listener while initialization stalls.
    tracing::info!(listen = %sql_addr, "crabka-gres ready to accept sessions");
    let range_addr = range_server
        .as_ref()
        .map_or_else(|| "-".to_string(), |(_, address)| address.to_string());
    println!("CRABKA_GRES_READY {sql_addr} {range_addr}");
    let serve_result = if let Some((policy, registry, checkpointer)) = suspend {
        tokio::select! {
            result = serve => result,
            result = run_suspend_monitor(policy, activity, checkpointer, registry, shutdown) => result,
        }
    } else {
        serve.await
    };
    if let Some((server, _)) = range_server {
        server.abort();
        let _ = server.await;
    }
    serve_result
}

async fn start_range_service(
    args: &ServeArgs,
    service: Option<Arc<dyn crabka_gres_ranges::RangeService>>,
) -> std::io::Result<Option<(tokio::task::JoinHandle<()>, SocketAddr)>> {
    let Some(listen) = &args.range_listen else {
        return Ok(None);
    };
    let service = service.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--range-listen requires a multi-range runtime",
        )
    })?;
    let config = SubstrateRuntimeConfig::from_args(args)?
        .and_then(|config| config.range_rpc)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "--range-listen requires --range-tls-cert, --range-tls-key, --range-tls-ca, --range-tls-server-name, and --range-allowed-principal"))?;
    let tenant = args.tenant.clone().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--range-listen requires --tenant",
        )
    })?;
    let tls = config.server(tenant);
    tls.build_acceptor()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let listener = TcpListener::bind(listen).await?;
    let address = listener.local_addr()?;
    tracing::info!(range_listen = %address, "crabka-gres range compute listening");
    Ok(Some((
        tokio::spawn(async move {
            if let Err(error) = crabka_gres_ranges::serve_tls(listener, service, tls).await {
                tracing::warn!(%error, "range compute server stopped");
            }
        }),
        address,
    )))
}

async fn run_suspend_monitor(
    policy: SuspendPolicy,
    activity: Arc<crabka_pgwire::server::ActivityTracker>,
    checkpointer: Box<dyn FinalCheckpointer>,
    mut registry: Box<dyn SuspendRegistry>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    loop {
        tokio::time::sleep(IDLE_MONITOR_POLL_INTERVAL).await;
        match try_suspend_idle_tenant(
            &policy,
            activity.as_ref(),
            checkpointer.as_ref(),
            registry.as_mut(),
        )
        .await?
        {
            SuspendMonitorOutcome::Suspended => {
                shutdown.cancel();
                return Ok(());
            }
            SuspendMonitorOutcome::CheckpointTooLarge { bytes, max_bytes } => {
                tracing::info!(tenant = %policy.tenant, bytes, max_bytes, "tenant remains warm after suspend size-gate skip");
            }
            SuspendMonitorOutcome::Disabled
            | SuspendMonitorOutcome::OpenSessions { .. }
            | SuspendMonitorOutcome::IdleWindowNotElapsed
            | SuspendMonitorOutcome::AdmissionAlreadyClosed
            | SuspendMonitorOutcome::RacedSession { .. } => {}
        }
    }
}

async fn mark_active_after_recovery(
    tenant_record: Option<&crabka_gres_control::TenantRecord>,
    registry: Option<crabka_gres_control::Registry>,
) -> std::io::Result<Option<crabka_gres_control::Registry>> {
    let Some(record) = tenant_record else {
        return Ok(None);
    };
    let Some(mut registry) = registry else {
        return Ok(None);
    };
    let Some(current) = registry.get(record.name.as_str()).await.map_err(|error| {
        std::io::Error::other(format!("tenant registry read after recovery: {error}"))
    })?
    else {
        return Err(std::io::Error::other(
            "tenant registry record disappeared after recovery",
        ));
    };
    if current.state != crabka_gres_control::TenantState::ResumeRequested {
        return Ok(Some(registry));
    }
    let endpoint = current
        .endpoint
        .clone()
        .or_else(|| {
            current
                .ranges
                .iter()
                .find(|range| range.range_id == 0)
                .map(|range| range.endpoint.clone())
        })
        .or_else(|| record.endpoint.clone())
        .or_else(|| {
            record
                .ranges
                .iter()
                .find(|range| range.range_id == 0)
                .map(|range| range.endpoint.clone())
        })
        .ok_or_else(|| {
            std::io::Error::other("resume-requested tenant record has no compute endpoint")
        })?;
    registry
        .mark_active(record.name.as_str(), endpoint)
        .await
        .map_err(|error| std::io::Error::other(format!("tenant registry mark active: {error}")))?;
    let confirmed = registry.get(record.name.as_str()).await.map_err(|error| {
        std::io::Error::other(format!("tenant registry confirm active: {error}"))
    })?;
    if !confirmed.is_some_and(|record| record.state == crabka_gres_control::TenantState::Active) {
        return Err(std::io::Error::other(
            "tenant registry did not confirm Active after recovery",
        ));
    }
    tracing::info!(tenant = %record.name, "recovered tenant marked active");
    Ok(Some(registry))
}

fn live_final_checkpointer(
    checkpoint_runtime: Option<StartedCheckpointRuntime>,
) -> Option<Box<dyn FinalCheckpointer>> {
    checkpoint_runtime.map(|runtime| Box::new(runtime) as Box<dyn FinalCheckpointer>)
}

/// Loads the per-tenant compute config from `__gres_cfg.<tenant>`.
#[async_trait::async_trait]
pub trait TenantConfigLoader: Sync {
    /// Return the latest tenant record from the per-tenant config topic.
    async fn load_tenant_config(
        &self,
        bootstrap: &str,
        tenant: &TenantName,
        security: Option<ClientSecurity>,
    ) -> std::io::Result<Option<TenantRecord>>;
}

/// Kafka-backed tenant-config loader used by the binary.
pub struct LiveTenantConfigLoader;

#[async_trait::async_trait]
impl TenantConfigLoader for LiveTenantConfigLoader {
    async fn load_tenant_config(
        &self,
        bootstrap: &str,
        tenant: &TenantName,
        security: Option<ClientSecurity>,
    ) -> std::io::Result<Option<TenantRecord>> {
        load_live_tenant_config(bootstrap, tenant, security).await
    }
}

struct LiveRangeRegistrySource {
    bootstrap: String,
    tenant: TenantName,
    security: Option<ClientSecurity>,
}

#[async_trait::async_trait]
impl crabka_gres_ranges::registry::RangeRegistrySource for LiveRangeRegistrySource {
    async fn load_current(&self) -> Result<TenantRecord, crabka_gres_ranges::RegistryError> {
        load_live_tenant_config(&self.bootstrap, &self.tenant, self.security.clone())
            .await
            .map_err(|error| crabka_gres_ranges::RegistryError::Authoritative(error.to_string()))?
            .ok_or_else(|| {
                crabka_gres_ranges::RegistryError::Authoritative(format!(
                    "tenant {} is absent from the control registry",
                    self.tenant
                ))
            })
    }
}

async fn load_substrate_tenant_record(
    args: &ServeArgs,
    tenant_config_loader: &impl TenantConfigLoader,
) -> std::io::Result<Option<TenantRecord>> {
    let Some(bootstrap) = args.substrate_bootstrap.as_deref() else {
        return Ok(None);
    };
    let tenant = args.tenant.as_deref().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--substrate-bootstrap requires --tenant",
        )
    })?;
    let tenant = TenantName::try_from(tenant)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let security = tenant_kafka_security_from_env(tenant.as_str());
    let record = tenant_config_loader
        .load_tenant_config(bootstrap, &tenant, security)
        .await?;
    let Some(record) = record else {
        return invalid_input(format!(
            "missing tenant config in {}; create it with `crabka gres create-tenant --name {tenant}`",
            tenant_config_topic(&tenant)
        ));
    };
    if record.name != tenant {
        return invalid_input(format!(
            "tenant config in {} names {}, expected {tenant}",
            tenant_config_topic(&tenant),
            record.name
        ));
    }
    Ok(Some(record))
}

fn apply_tenant_runtime_defaults(
    mut args: ServeArgs,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<ServeArgs> {
    let Some(record) = tenant_record else {
        return Ok(args);
    };
    if args.checkpoint_prefix.is_none() {
        args.checkpoint_prefix.clone_from(&record.bucket_prefix);
    }
    if args.checkpoint_frames.is_none() {
        args.checkpoint_frames = nonzero_u64(record.checkpoint_frames, "checkpoint_frames")?;
    }
    if args.checkpoint_bytes.is_none() {
        args.checkpoint_bytes = nonzero_u64(record.checkpoint_bytes, "checkpoint_bytes")?;
    }
    Ok(args)
}

fn nonzero_u64(value: Option<u64>, field: &str) -> std::io::Result<Option<NonZeroU64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    NonZeroU64::new(value).map_or_else(
        || {
            invalid_input(format!(
                "tenant {field} must be greater than zero when present"
            ))
        },
        |value| Ok(Some(value)),
    )
}

async fn load_live_tenant_config(
    bootstrap: &str,
    tenant: &TenantName,
    security: Option<ClientSecurity>,
) -> std::io::Result<Option<TenantRecord>> {
    let topic = tenant_config_topic(tenant);
    let Some(addr) = resolve_bootstrap_addr(bootstrap) else {
        return invalid_input("substrate bootstrap address list is empty");
    };
    let mut admin = crabka_client_admin::AdminClient::connect_secured(
        &split_bootstrap(bootstrap),
        security.clone(),
    )
    .await
    .map_err(|error| std::io::Error::other(format!("tenant config metadata: {error}")))?;
    let metadata = admin
        .metadata(&[&topic])
        .await
        .map_err(|error| std::io::Error::other(format!("tenant config metadata: {error}")))?;
    let topic_entry = metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(|| std::io::Error::other(format!("tenant config metadata missing {topic}")))?;
    if let Some(error) = topic_entry.error {
        return Err(std::io::Error::other(format!(
            "tenant config metadata for {topic}: {} ({})",
            error.name, error.code
        )));
    }
    let Some(topic_id) = topic_entry.topic_id else {
        return Ok(None);
    };
    let options = crabka_client_core::ConnectionOptions {
        client_id: format!("crabka-gres-config-reader-{tenant}"),
        security: security.map(Box::new),
        ..Default::default()
    };
    let conn = crabka_client_core::Connection::connect_with_options(addr, options)
        .await
        .map_err(|error| std::io::Error::other(format!("tenant config connect: {error}")))?;
    let topic_id = crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());
    let mut next_offset = 0_i64;
    let mut latest = None;
    loop {
        let records = crabka_client_core::fetch_partition(
            &conn,
            &topic,
            topic_id,
            0,
            next_offset,
            TENANT_CONFIG_FETCH_MAX_WAIT_MS,
            TENANT_CONFIG_FETCH_PARTITION_MAX_BYTES,
        )
        .await
        .map_err(|error| std::io::Error::other(format!("tenant config fetch: {error}")))?;
        if records.is_empty() {
            conn.close();
            return Ok(latest);
        }
        for record in records {
            if record.offset < next_offset {
                continue;
            }
            next_offset = record.offset + 1;
            let Some(value) = record.value else {
                latest = None;
                continue;
            };
            latest =
                Some(decode_tenant_config_record(&value).map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                })?);
        }
    }
}

async fn open_runtime_with_tenant_record(
    args: &ServeArgs,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<GresRuntime> {
    if let Some(config) = SubstrateRuntimeConfig::from_args(args)? {
        return open_substrate_runtime_with_tenant_record(&config, tenant_record).await;
    }

    let engine = match args.data_dir.as_deref() {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            SqlEngine::open(dir)
                .map_err(|err| std::io::Error::other(format!("opening data dir: {err:?}")))
        }
        None => Ok(SqlEngine::new()),
    }?;
    Ok(GresRuntime::new(engine))
}

/// Construct a substrate-mode engine from a disposable cache store and WAL seam.
pub async fn open_substrate_engine(config: &SubstrateRuntimeConfig) -> std::io::Result<SqlEngine> {
    match open_substrate_runtime(config).await?.engine {
        RuntimeEngine::Single(engine) => Ok(*engine),
        RuntimeEngine::Multi(_) => {
            invalid_input("--ranges constructs a multi-range gateway, not a SqlEngine")
        }
    }
}

/// Construct substrate-mode runtime resources from a cache store and WAL seam.
pub async fn open_substrate_runtime(
    config: &SubstrateRuntimeConfig,
) -> std::io::Result<GresRuntime> {
    open_substrate_runtime_with_tenant_record(config, None).await
}

async fn open_substrate_runtime_with_tenant_record(
    config: &SubstrateRuntimeConfig,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<GresRuntime> {
    use std::io::Error;

    if let Some(boundaries) = config.ranges.as_deref() {
        return open_multirange_runtime(config, boundaries, tenant_record).await;
    }

    let store = open_substrate_cache(config.cache_dir.as_deref())?;
    if !config.is_in_memory_bootstrap() {
        return open_live_substrate_runtime(config, store, tenant_record).await;
    }

    let log = crabka_gres_substrate::InMemoryWalLog::shared();
    let (barrier, outcome) =
        crabka_gres_substrate::recover_after_barrier(store.as_ref(), log.as_ref(), log.as_ref())
            .await
            .map_err(|error| Error::other(format!("substrate recovery: {error}")))?;
    let snapshot_source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
        barrier.offset,
        outcome.next_journal_seq,
        barrier.generation,
    ));
    let checkpoint = build_checkpoint_runtime(
        config,
        Arc::clone(&store),
        Arc::clone(&snapshot_source),
        crabka_gres_substrate::wal_topic(&config.tenant),
        format!("{}/r0", config.tenant),
        None,
        || Ok(GresCheckpointWalPruner::in_memory()),
    )?;
    let engine = build_replicated_substrate_engine(
        &store,
        log,
        barrier.generation,
        outcome.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|checkpoint| Arc::clone(&checkpoint.stats)),
    )?;
    Ok(match checkpoint {
        Some(checkpoint) => GresRuntime::with_checkpoint_runtime(engine, checkpoint),
        None => GresRuntime::new(engine),
    })
}

async fn open_multirange_runtime(
    config: &SubstrateRuntimeConfig,
    boundaries: &str,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<GresRuntime> {
    let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("--tenant: {error}"),
        )
    })?;
    let mut tenant_config =
        crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(tenant, boundaries)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    if let Ok(fault) = std::env::var("CRABKA_GRES_TEST_COMMIT_FAULT") {
        let fault = match fault.as_str() {
            "before_decision_after_prepare" => {
                crabka_gres_ranges::GatewayCommitFault::BeforeDecisionAfterPrepare
            }
            "before_release_after_commit_decision" => {
                crabka_gres_ranges::GatewayCommitFault::BeforeReleaseAfterCommitDecision
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid CRABKA_GRES_TEST_COMMIT_FAULT",
                ));
            }
        };
        tenant_config = tenant_config.with_commit_fault_for_testing(fault);
    }
    if let Some(hosted_ranges) = &config.host_ranges {
        tenant_config = tenant_config
            .with_hosted_ranges(hosted_ranges.clone())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    }
    if let Some(record) = tenant_record {
        let mut registry = crabka_gres_ranges::RangeRegistry::from_tenant_record(record)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        if !matches!(config.bootstrap.as_str(), "memory://" | "in-memory://") {
            registry = registry.with_authoritative_source(Arc::new(LiveRangeRegistrySource {
                bootstrap: config.bootstrap.clone(),
                tenant: crabka_gres_control::TenantName::try_from(config.tenant.as_str()).map_err(
                    |error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error),
                )?,
                security: config.kafka_security.clone(),
            }));
        }
        if let Some(range_rpc) = &config.range_rpc {
            tenant_config = tenant_config
                .with_range_client(range_rpc.client()?)
                .with_range_registry(registry);
        } else if remote_ranges_are_configured(config, record) {
            return invalid_input(
                "remote range routing requires --range-tls-cert, --range-tls-key, --range-tls-ca, and --range-tls-server-name",
            );
        }
    }
    if config.is_in_memory_bootstrap() {
        if config.checkpoints.is_some() {
            return invalid_input(
                "multi-range checkpointing requires a live substrate broker; in-memory ranges have no durable transfer capability",
            );
        }
        let (gateway, _handles) = crabka_gres_ranges::MultiRangeTenant::start(tenant_config)
            .map_err(|error| std::io::Error::other(format!("multi-range tenant: {error}")))?;
        return Ok(GresRuntime::multi(gateway));
    }

    let checkpoint_store = config
        .checkpoints
        .as_ref()
        .map(build_checkpoint_store)
        .transpose()?;
    if tenant_config
        .hosted_ranges
        .as_ref()
        .is_some_and(|ranges| !ranges.contains(&crabka_gres_ranges::RangeId::COORDINATOR))
    {
        let follower_config = crabka_gres_substrate::LiveRecoveryConfig::new(
            config.bootstrap.clone(),
            tenant_config.tenant.clone(),
            crabka_gres_ranges::RangeId::COORDINATOR,
            config.kafka_security.clone(),
        );
        let follower_store: Arc<dyn RestoreKv> = match config.cache_dir.as_deref() {
            Some(parent) => {
                let dir = parent.join("r0-follower");
                std::fs::create_dir_all(&dir)?;
                Arc::new(FjallKv::open_cache(&dir).map_err(|error| {
                    std::io::Error::other(format!("range-0 follower cache: {error:?}"))
                })?)
            }
            None => Arc::new(MemKv::default()),
        };
        let follower = crabka_gres_substrate::bootstrap_live_range0_follower(
            &follower_config,
            follower_store,
            checkpoint_store.as_deref(),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("range-0 follower bootstrap: {error}")))?;
        let tail = follower.tail();
        let sampler = Arc::new(crabka_gres_substrate::BrokerRange0EndSampler(Arc::new(
            crabka_gres_substrate::LiveCommittedEndSampler::new(follower_config.clone()),
        )));
        tenant_config = tenant_config.with_read_only_range0_replica(
            crabka_gres_ranges::ReadOnlyRange0Replica::new(tail, sampler),
        );
        tokio::spawn(async move {
            loop {
                let applied = follower.tail().applied_offset();
                match crabka_gres_substrate::live_committed_end(&follower_config).await {
                    Ok(end) if end > applied => {
                        match crabka_gres_substrate::read_live_committed_tail(
                            &follower_config,
                            applied,
                            end,
                        )
                        .await
                        {
                            Ok(items) => {
                                for item in &items {
                                    if follower.apply_committed(item).is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "range-0 follower tail read failed")
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(%error, "range-0 follower end sample failed"),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
    let activation_receipt =
        split_activation::discover_activation_receipt(config, checkpoint_store.as_deref())
            .await
            .map_err(|error| std::io::Error::other(format!("substrate recovery: {error}")))?;
    let mut engines =
        recover_live_multirange_engines(config, &tenant_config, checkpoint_store.clone()).await?;
    if let Some(recovered_map) = split_activation::reconcile_before_readiness(
        config,
        &mut engines,
        checkpoint_store,
        activation_receipt,
    )
    .await?
    {
        tenant_config.range_map = recovered_map;
        if let Some(hosted) = &mut tenant_config.hosted_ranges {
            hosted.clear();
            hosted.extend(
                tenant_config
                    .range_map
                    .ranges()
                    .iter()
                    .map(|spec| spec.range_id),
            );
        }
    }
    open_live_multirange_tenant(tenant_config, engines, config).await
}

fn remote_ranges_are_configured(config: &SubstrateRuntimeConfig, record: &TenantRecord) -> bool {
    let Some(hosted_ranges) = &config.host_ranges else {
        return false;
    };
    record.ranges.iter().any(|range| {
        let range_id = crabka_gres_ranges::RangeId::new(range.range_id);
        !hosted_ranges.contains(&range_id) && range_id != crabka_gres_ranges::RangeId::COORDINATOR
    })
}

async fn recover_live_multirange_engines(
    config: &SubstrateRuntimeConfig,
    tenant_config: &crabka_gres_ranges::MultiRangeTenantConfig,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
) -> std::io::Result<LiveMultirangeEngines> {
    let recovery_configs = live_multirange_recovery_configs(config, tenant_config);
    let mut engines = BTreeMap::new();
    let mut range0_tso_horizon = None;
    for recovery_config in recovery_configs {
        let range_id = recovery_config.range;
        let store = open_substrate_range_cache(config.cache_dir.as_deref(), range_id)?;
        let recovered = open_live_range_substrate_engine(
            config,
            recovery_config,
            store,
            checkpoint_store.clone(),
        )
        .await?;
        if range_id == crabka_gres_ranges::RangeId::COORDINATOR {
            range0_tso_horizon.clone_from(&recovered.tso_horizon);
        }
        engines.insert(range_id, recovered);
    }
    Ok(LiveMultirangeEngines {
        engines,
        range0_tso_horizon,
    })
}

struct LiveMultirangeEngines {
    engines: BTreeMap<crabka_gres_ranges::RangeId, LiveRangeEngine>,
    range0_tso_horizon: Option<crabka_gres_substrate::SubstrateTsoHorizon>,
}

fn live_multirange_recovery_configs(
    config: &SubstrateRuntimeConfig,
    tenant_config: &crabka_gres_ranges::MultiRangeTenantConfig,
) -> Vec<crabka_gres_substrate::LiveRecoveryConfig> {
    tenant_config
        .range_map
        .ranges()
        .iter()
        .filter(|spec| {
            tenant_config
                .hosted_ranges
                .as_ref()
                .is_none_or(|ranges| ranges.contains(&spec.range_id))
        })
        .map(|spec| {
            crabka_gres_substrate::LiveRecoveryConfig::new(
                config.bootstrap.clone(),
                tenant_config.tenant.clone(),
                spec.range_id,
                config.kafka_security.clone(),
            )
            .with_optional_advertised_endpoint(config.advertised_endpoint.clone())
        })
        .collect()
}

struct SingleRangeLiveWalSelection {
    recovery_config: crabka_gres_substrate::LiveRecoveryConfig,
    writer_topic: String,
    checkpoint_topic: String,
    checkpoint_namespace: String,
}

fn single_range_live_wal_selection(
    config: &SubstrateRuntimeConfig,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<SingleRangeLiveWalSelection> {
    let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("--tenant: {error}"),
        )
    })?;
    let wal_generation = tenant_record.map_or(0, |record| {
        record
            .ranges
            .iter()
            .find(|range| range.range_id == crabka_gres_ranges::RangeId::COORDINATOR.as_u32())
            .map_or(record.wal_generation, |range| range.wal_generation)
    });
    let recovery_config = crabka_gres_substrate::LiveRecoveryConfig::new(
        config.bootstrap.clone(),
        tenant,
        crabka_gres_ranges::RangeId::COORDINATOR,
        config.kafka_security.clone(),
    )
    .with_wal_generation(wal_generation)
    .with_optional_advertised_endpoint(config.advertised_endpoint.clone());
    let topic = recovery_config.wal_topic();
    Ok(SingleRangeLiveWalSelection {
        checkpoint_namespace: recovery_config.checkpoint_namespace(),
        recovery_config,
        writer_topic: topic.clone(),
        checkpoint_topic: topic,
    })
}

async fn open_live_range_substrate_engine(
    config: &SubstrateRuntimeConfig,
    recovery_config: crabka_gres_substrate::LiveRecoveryConfig,
    store: Arc<dyn SubstrateKv>,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
) -> std::io::Result<LiveRangeEngine> {
    let range_id = recovery_config.range;
    let topic = recovery_config.wal_topic();
    let recovery_config =
        recovery_config_with_checkpoint_store(recovery_config, checkpoint_store.as_ref());
    let recovered = crabka_gres_substrate::recover_live_for_range_with_restore(
        recovery_config.clone(),
        store.as_ref(),
    )
    .await
    .map_err(|error| std::io::Error::other(format!("substrate recovery: {error}")))?;
    let producer_writer = Arc::new(crabka_gres_substrate::ProducerWalWriter::new(
        recovered.producer,
        topic.clone(),
    ));
    let writer = Arc::new(crabka_gres_substrate::DeferredWalWriter::staged());
    writer
        .activate(producer_writer)
        .map_err(|error| std::io::Error::other(format!("activate recovered writer: {error}")))?;
    let snapshot_source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
        recovered.barrier_offset,
        recovered.next_journal_seq,
        recovered.generation,
    ));
    let checkpoint = build_range_checkpoint_runtime(
        config,
        range_id,
        Arc::clone(&store),
        Arc::clone(&snapshot_source),
        topic.clone(),
        checkpoint_store,
    )?
    .map(Arc::new);
    let (engine, committer) = build_replicated_substrate_engine_with_committer(
        &store,
        Arc::clone(&writer),
        recovered.generation,
        recovered.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|runtime| Arc::clone(&runtime.stats)),
    )?;
    let tso_horizon = if range_id == crabka_gres_ranges::RangeId::COORDINATOR {
        let tso_store: Arc<dyn Kv> = store.clone();
        let tso_committer: Arc<dyn crabka_pgexec::Committer> = committer.clone();
        let tso_lease: Arc<dyn crabka_gres_substrate::FenceLease> = writer.clone();
        Some(crabka_gres_substrate::SubstrateTsoHorizon::new(
            tso_store,
            tso_committer,
            tso_lease,
            recovered.generation,
        ))
    } else {
        None
    };
    Ok(LiveRangeEngine {
        engine,
        tso_horizon: tso_horizon.clone(),
        resources: LiveRangeResources {
            store,
            writer,
            activation_committer: committer,
            snapshot_source,
            checkpoint,
            recovery_config,
            generation: recovered.generation,
            pause: Arc::new(std::sync::Mutex::new(RangePauseState::Idle)),
            tso_horizon: tso_horizon.clone(),
        },
    })
}

struct LiveRangeEngine {
    engine: SqlEngine,
    tso_horizon: Option<crabka_gres_substrate::SubstrateTsoHorizon>,
    resources: LiveRangeResources,
}

struct LiveRangeResources {
    store: Arc<dyn SubstrateKv>,
    writer: Arc<crabka_gres_substrate::DeferredWalWriter<crabka_gres_substrate::ProducerWalWriter>>,
    activation_committer: Arc<
        crabka_gres_substrate::SubstrateCommitter<
            crabka_gres_substrate::DeferredWalWriter<crabka_gres_substrate::ProducerWalWriter>,
        >,
    >,
    snapshot_source: Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    checkpoint: Option<Arc<StartedCheckpointRuntime>>,
    recovery_config: crabka_gres_substrate::LiveRecoveryConfig,
    generation: crabka_gres_substrate::WriterGeneration,
    pause: Arc<std::sync::Mutex<RangePauseState>>,
    tso_horizon: Option<crabka_gres_substrate::SubstrateTsoHorizon>,
}

struct DynamicLiveRangeService {
    current: std::sync::RwLock<Arc<crabka_gres_ranges::HostedRangeService>>,
    publishing: std::sync::atomic::AtomicBool,
}

impl DynamicLiveRangeService {
    fn new(service: crabka_gres_ranges::HostedRangeService) -> Self {
        Self {
            current: std::sync::RwLock::new(Arc::new(service)),
            publishing: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn replace(&self, service: crabka_gres_ranges::HostedRangeService) {
        *self.current.write().expect("live range service lock") = Arc::new(service);
    }

    fn begin_publication(&self) {
        self.publishing
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn finish_publication(&self) {
        self.publishing
            .store(false, std::sync::atomic::Ordering::Release);
    }

    fn load(&self) -> Arc<crabka_gres_ranges::HostedRangeService> {
        Arc::clone(&self.current.read().expect("live range service lock"))
    }
}

#[async_trait::async_trait]
impl crabka_gres_ranges::RangeService for DynamicLiveRangeService {
    async fn handle(
        &self,
        request: crabka_gres_ranges::RangeRequest,
    ) -> crabka_gres_ranges::RangeResponse {
        if self.publishing.load(std::sync::atomic::Ordering::Acquire) {
            return crabka_gres_ranges::RangeResponse::Error {
                error: crabka_gres_ranges::WireErrorKind::StaleEndpoint,
                message: "range topology publication is in progress; retry".into(),
            };
        }
        self.load().handle(request).await
    }

    async fn handle_connection(
        &self,
        request: crabka_gres_ranges::RangeRequest,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<Option<crabka_gres_ranges::RangeResponse>, crabka_gres_ranges::TransportError> {
        if self.publishing.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(Some(crabka_gres_ranges::RangeResponse::Error {
                error: crabka_gres_ranges::WireErrorKind::StaleEndpoint,
                message: "range topology publication is in progress; retry".into(),
            }));
        }
        self.load().handle_connection(request, writer).await
    }
}

enum RangePauseState {
    Idle,
    Pausing,
    Paused(crabka_gres_substrate::PausedWalWriter),
}

struct PauseReservation {
    pause: Arc<std::sync::Mutex<RangePauseState>>,
    active: bool,
}

impl PauseReservation {
    fn reserve(
        pause: Arc<std::sync::Mutex<RangePauseState>>,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<Self, crabka_gres_ranges::RangeTransferError> {
        let mut state = pause.lock().map_err(|_| range_pause_lock_error(range_id))?;
        if !matches!(&*state, RangePauseState::Idle) {
            return Err(crabka_gres_ranges::RangeTransferError::AlreadyPaused { range_id });
        }
        *state = RangePauseState::Pausing;
        drop(state);
        Ok(Self {
            pause,
            active: true,
        })
    }

    fn store(
        mut self,
        paused: crabka_gres_substrate::PausedWalWriter,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        let mut state = self
            .pause
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?;
        *state = RangePauseState::Paused(paused);
        self.active = false;
        Ok(())
    }
}

impl Drop for PauseReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.pause.lock()
            && matches!(&*state, RangePauseState::Pausing)
        {
            *state = RangePauseState::Idle;
        }
    }
}

fn range_pause_lock_error(
    range_id: crabka_gres_ranges::RangeId,
) -> crabka_gres_ranges::RangeTransferError {
    crabka_gres_ranges::RangeTransferError::Runtime {
        range_id,
        reason: "range pause state lock poisoned".to_owned(),
    }
}

async fn open_live_multirange_tenant(
    tenant_config: crabka_gres_ranges::MultiRangeTenantConfig,
    mut live_engines: LiveMultirangeEngines,
    config: &SubstrateRuntimeConfig,
) -> std::io::Result<GresRuntime> {
    let live_resources = live_engines
        .engines
        .iter()
        .map(|(range_id, engine)| (*range_id, engine.resources.clone()))
        .collect();
    let (gateway, _handles, tso_rpc) = if let Some(tso_horizon) =
        live_engines.range0_tso_horizon.take()
    {
        let persisted_max_ts = tso_horizon
            .load_max_ts()
            .map_err(|error| std::io::Error::other(format!("range-0 TSO horizon: {error}")))?;
        let tso_rpc = crabka_gres_ranges::tso_rpc_from_horizon(
            tso_horizon.clone(),
            tso_horizon.clone(),
            tso_horizon.epoch(),
            persisted_max_ts,
        )
        .map_err(|error| std::io::Error::other(format!("range-0 TSO oracle: {error}")))?;
        let timestamp_oracle =
            crabka_gres_ranges::pgexec_timestamp_oracle_from_rpc(Arc::clone(&tso_rpc));
        let (gateway, handles) =
            crabka_gres_ranges::MultiRangeTenant::start_with_engine_factory_and_timestamp_oracle(
                tenant_config,
                |_data_dir, range_id| {
                    live_engines
                        .engines
                        .remove(&range_id)
                        .map(|engine| engine.engine)
                        .ok_or_else(|| {
                            crabka_pgexec::ExecError::Unsupported(format!(
                                "recovered live substrate engine for range r{range_id} is missing"
                            ))
                        })
                },
                Some(timestamp_oracle),
            )
            .map_err(|error| std::io::Error::other(format!("multi-range tenant: {error}")))?;
        (gateway, handles, Some(tso_rpc))
    } else {
        let (gateway, handles) = crabka_gres_ranges::MultiRangeTenant::start_with_engine_factory(
            tenant_config,
            |_data_dir, range_id| {
                live_engines
                    .engines
                    .remove(&range_id)
                    .map(|engine| engine.engine)
                    .ok_or_else(|| {
                        crabka_pgexec::ExecError::Unsupported(format!(
                            "recovered live substrate engine for range r{range_id} is missing"
                        ))
                    })
            },
        )
        .map_err(|error| std::io::Error::other(format!("multi-range tenant: {error}")))?;
        (gateway, handles, None)
    };
    gateway
        .recover_ordinary_globals_before_serving()
        .await
        .map_err(|error| std::io::Error::other(format!("ordinary 2PC recovery: {error:?}")))?;
    let mut range_service =
        crabka_gres_ranges::HostedRangeService::new(gateway.hosted_range_engines());
    if let Some((registry, client)) = gateway.timestamp_primary_remote() {
        range_service = range_service.with_timestamp_primary_remote(registry, client);
    }
    if let Some(tso_rpc) = &tso_rpc {
        range_service = range_service.with_tso(Arc::clone(tso_rpc));
    }
    let dynamic_service = Arc::new(DynamicLiveRangeService::new(range_service));
    let transfer = Arc::new(LiveMultiRangeTransfer::new(
        live_resources,
        (*config).clone(),
        Arc::clone(&dynamic_service),
        gateway.hosted_range_engines(),
        tso_rpc,
    ));
    Ok(GresRuntime {
        engine: RuntimeEngine::Multi(Box::new(gateway)),
        checkpoint_runtime: None,
        range_service: Some(dynamic_service),
        range_transfer: Some(transfer.clone()),
        staged_transfer: Some(transfer),
    })
}

fn parse_host_ranges(
    value: Option<&str>,
) -> std::io::Result<Option<Vec<crabka_gres_ranges::RangeId>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mut ranges = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let numeric = token.strip_prefix('r').unwrap_or(token);
        let range_id = numeric.parse::<u32>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid --host-ranges token {token:?}: {error}"),
            )
        })?;
        ranges.push(crabka_gres_ranges::RangeId::new(range_id));
    }
    if ranges.is_empty() {
        return invalid_input("--host-ranges must contain at least one range id");
    }
    ranges.sort_unstable();
    ranges.dedup();
    Ok(Some(ranges))
}

struct StartedCheckpointRuntime {
    handle: crabka_gres_substrate::CheckpointHandle,
    stats: Arc<crabka_gres_substrate::CheckpointStats>,
    snapshot_source: Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    store: Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
    tenant: String,
    latest_checkpoint_bytes: std::sync::atomic::AtomicU64,
}

impl Clone for LiveRangeResources {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            writer: Arc::clone(&self.writer),
            activation_committer: Arc::clone(&self.activation_committer),
            snapshot_source: Arc::clone(&self.snapshot_source),
            checkpoint: self.checkpoint.clone(),
            recovery_config: self.recovery_config.clone(),
            generation: self.generation,
            pause: Arc::clone(&self.pause),
            tso_horizon: self.tso_horizon.clone(),
        }
    }
}

/// Live resources retained for foundation-only transfer operations.
struct LiveMultiRangeTransfer {
    ranges: std::sync::RwLock<BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>>,
    config: SubstrateRuntimeConfig,
    staged: std::sync::Mutex<BTreeMap<crabka_gres_ranges::RangeId, StagedLiveRangeSuccessor>>,
    engines: std::sync::RwLock<BTreeMap<crabka_gres_ranges::RangeId, SqlEngine>>,
    tso_rpc: std::sync::RwLock<Option<Arc<dyn crabka_gres_ranges::TsoRpc>>>,
    range_service: Arc<DynamicLiveRangeService>,
    retired: std::sync::Mutex<BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>>,
    pending: std::sync::Mutex<Option<PendingLiveTopology>>,
    prepared: std::sync::Mutex<Option<PreparedLiveTopology>>,
    committed_activation: std::sync::Mutex<Option<String>>,
    prepare_fault: std::sync::atomic::AtomicU8,
    activation_fault: std::sync::atomic::AtomicU8,
    activation_irreversible: std::sync::atomic::AtomicBool,
}

impl LiveMultiRangeTransfer {
    fn new(
        ranges: BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>,
        config: SubstrateRuntimeConfig,
        range_service: Arc<DynamicLiveRangeService>,
        engines: BTreeMap<crabka_gres_ranges::RangeId, SqlEngine>,
        tso_rpc: Option<Arc<dyn crabka_gres_ranges::TsoRpc>>,
    ) -> Self {
        Self {
            ranges: std::sync::RwLock::new(ranges),
            config,
            staged: std::sync::Mutex::new(BTreeMap::new()),
            engines: std::sync::RwLock::new(engines),
            tso_rpc: std::sync::RwLock::new(tso_rpc),
            range_service,
            retired: std::sync::Mutex::new(BTreeMap::new()),
            pending: std::sync::Mutex::new(None),
            prepared: std::sync::Mutex::new(None),
            committed_activation: std::sync::Mutex::new(None),
            prepare_fault: std::sync::atomic::AtomicU8::new(PrepareTopologyFault::None as u8),
            activation_fault: std::sync::atomic::AtomicU8::new(TopologyActivationFault::None as u8),
            activation_irreversible: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn range(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<LiveRangeResources, crabka_gres_ranges::RangeTransferError> {
        self.ranges
            .read()
            .map_err(|_| range_pause_lock_error(range_id))?
            .get(&range_id)
            .cloned()
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id,
                reason: "range is not hosted by this live runtime".to_owned(),
            })
    }

    fn staged_successor_kv(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<Option<KvScan>, crabka_gres_ranges::RangeTransferError> {
        let staged = self
            .staged
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?;
        let Some(successor) = staged.get(&range_id) else {
            return Ok(None);
        };
        successor
            .resources
            .store
            .scan_range(&[], &[u8::MAX])
            .map(Some)
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("scan staged successor KV: {error}"),
            })
    }

    fn release_pause(
        &self,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        let resources = self
            .retired
            .lock()
            .map_err(|_| range_pause_lock_error(barrier.range_id))?
            .get(&barrier.range_id)
            .cloned()
            .map_or_else(|| self.range(barrier.range_id), Ok)?;
        let mut state = resources
            .pause
            .lock()
            .map_err(|_| range_pause_lock_error(barrier.range_id))?;
        let RangePauseState::Paused(paused) = &*state else {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: barrier.range_id,
                reason: "range writer is not paused".to_owned(),
            });
        };
        if paused.barrier_offset != barrier.offset {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: barrier.range_id,
                reason: "barrier is not held by this paused range writer".to_owned(),
            });
        }
        let RangePauseState::Paused(paused) = std::mem::replace(&mut *state, RangePauseState::Idle)
        else {
            unreachable!("pause state was verified while locked");
        };
        drop(state);
        paused.resume();
        self.retired
            .lock()
            .map_err(|_| range_pause_lock_error(barrier.range_id))?
            .remove(&barrier.range_id);
        Ok(())
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "staging keeps every validation and durable ordering step visible at the boundary"
)]
#[async_trait::async_trait]
impl crabka_gres_ranges::RangeTransferCapability for LiveMultiRangeTransfer {
    async fn record_topology_activation_intent(
        &self,
        state: &crabka_gres_ranges::SplitState,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        use crabka_gres_ranges::control::{
            ActivationTargetProgress, RangeZeroTopologyActivationStore, TopologyActivationPhase,
            TopologyActivationReceipt, TopologyActivationReceiptStore,
        };

        let engine = self
            .engines
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(SqlEngine::clone_handle)
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "range-zero engine is unavailable for activation receipt".into(),
            })?;
        let tenant = self
            .ranges
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(|resources| resources.recovery_config.tenant.to_string())
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "range-zero resources are unavailable for activation receipt".into(),
            })?;
        let store = RangeZeroTopologyActivationStore::new(tenant.clone(), engine);
        if let Some(existing) = store.load(&state.operation_id).await.map_err(|reason| {
            crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("load topology activation receipt: {reason}"),
            }
        })? {
            if existing.split == *state {
                return Ok(());
            }
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: "operation id already names a different topology activation".into(),
            });
        }
        let mut targets = BTreeMap::new();
        for descriptor in std::iter::once(&state.left).chain(state.right.iter()) {
            targets.insert(
                descriptor.range_id,
                ActivationTargetProgress {
                    range_id: descriptor.range_id,
                    wal_generation: descriptor.wal_generation,
                    endpoint: descriptor.endpoint.clone(),
                    interval: descriptor.interval.clone(),
                    replay_journal_seq: None,
                    writer_activated: false,
                    bootstrap_checkpoint: None,
                },
            );
        }
        let receipt = TopologyActivationReceipt {
            tenant,
            operation_id: state.operation_id.clone(),
            revision: 0,
            phase: TopologyActivationPhase::Prepared,
            split: state.clone(),
            source_checkpoint: None,
            barrier_offset: None,
            tail_sha256: None,
            targets,
        };
        match store
            .compare_and_swap(&state.operation_id, None, receipt)
            .await
        {
            Ok(true) => self.activation_fault(
                TopologyActivationFault::Prepared,
                crabka_gres_ranges::RangeId::COORDINATOR,
            ),
            Ok(false) => Err(crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "topology activation receipt raced another writer".into(),
            }),
            Err(reason) => Err(crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("persist topology activation receipt: {reason}"),
            }),
        }
    }

    async fn record_topology_activation_checkpoint(
        &self,
        operation_id: &str,
        checkpoint: &crabka_gres_ranges::CheckpointManifest,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        use crabka_gres_ranges::control::{
            RangeZeroTopologyActivationStore, TopologyActivationReceiptStore,
        };
        let engine = self
            .engines
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(SqlEngine::clone_handle)
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "range-zero engine unavailable for checkpoint receipt".into(),
            })?;
        let tenant = self
            .ranges
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(|resources| resources.recovery_config.tenant.to_string())
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "range-zero resources unavailable for checkpoint receipt".into(),
            })?;
        let store = RangeZeroTopologyActivationStore::new(tenant, engine);
        let mut receipt = store
            .load(operation_id)
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("load checkpoint receipt: {reason}"),
            })?
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "activation intent missing before checkpoint".into(),
            })?;
        if let Some(existing) = &receipt.source_checkpoint {
            return if existing == checkpoint {
                Ok(())
            } else {
                Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: receipt.split.predecessor,
                    reason: "source checkpoint differs from activation receipt".into(),
                })
            };
        }
        if receipt.phase != crabka_gres_ranges::control::TopologyActivationPhase::Prepared {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: receipt.split.predecessor,
                reason: "source checkpoint requires the prepared activation phase".into(),
            });
        }
        let expected = receipt.revision;
        receipt.revision = receipt.revision.saturating_add(1);
        receipt.phase =
            crabka_gres_ranges::control::TopologyActivationPhase::SourceCheckpoint;
        receipt.source_checkpoint = Some(checkpoint.clone());
        if !store
            .compare_and_swap(operation_id, Some(expected), receipt)
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("persist checkpoint receipt: {reason}"),
            })?
        {
            return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "checkpoint receipt CAS raced".into(),
            });
        }
        self.activation_fault(
            TopologyActivationFault::SourceCheckpoint,
            crabka_gres_ranges::RangeId::COORDINATOR,
        )
    }

    fn publish_serving_topology(
        &self,
        engines: &BTreeMap<crabka_gres_ranges::RangeId, SqlEngine>,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        self.publish_topology(engines)
    }

    fn commit_serving_topology(&self) {
        self.commit_prepared_topology();
    }

    async fn finish_topology_activation(
        &self,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        use crabka_gres_ranges::control::{
            RangeZeroTopologyActivationStore, TopologyActivationPhase,
            TopologyActivationReceiptStore,
        };
        let operation_id = self
            .committed_activation
            .lock()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .clone()
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "committed topology is missing its activation id".into(),
            })?;
        self.activation_fault(
            TopologyActivationFault::TopologySwap,
            crabka_gres_ranges::RangeId::COORDINATOR,
        )?;
        let engine = self
            .engines
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(SqlEngine::clone_handle)
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "serving range-zero engine missing after topology commit".into(),
            })?;
        let tenant = self
            .ranges
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(|resources| resources.recovery_config.tenant.to_string())
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "serving range-zero resources missing after topology commit".into(),
            })?;
        let store = RangeZeroTopologyActivationStore::new(tenant, engine);
        let mut receipt = store
            .load(&operation_id)
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("load committed activation receipt: {reason}"),
            })?
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "committed activation receipt is missing".into(),
            })?;
        if receipt.phase == TopologyActivationPhase::TopologyCommitted {
            return Ok(());
        }
        let expected = receipt.revision;
        receipt.revision = receipt.revision.saturating_add(1);
        receipt.phase = TopologyActivationPhase::TopologyCommitted;
        if !store
            .compare_and_swap(&operation_id, Some(expected), receipt)
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: format!("persist committed topology receipt: {reason}"),
            })?
        {
            return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "committed topology receipt CAS raced".into(),
            });
        }
        self.activation_fault(
            TopologyActivationFault::TopologyCommitted,
            crabka_gres_ranges::RangeId::COORDINATOR,
        )?;
        *self
            .committed_activation
            .lock()
            .expect("committed activation lock") = None;
        Ok(())
    }

    fn begin_serving_topology_publication(&self) {
        self.range_service.begin_publication();
    }

    async fn mark_topology_must_activate(
        &self,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        split_activation::persist_must_activate(self).await
    }

    async fn activate_serving_topology(
        &self,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        use crabka_gres_ranges::control::{
            RangeZeroTopologyActivationStore, TopologyActivationPhase,
            TopologyActivationReceiptStore,
        };
        let pending = self
            .pending
            .lock()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .clone()
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "activate without pending topology".into(),
            })?;
        let activation_tenant = pending.left.recovery_config.tenant.to_string();
        for (index, (range_id, resources)) in [
            (pending.left_id, pending.left.clone()),
            (pending.right_id, pending.right.clone()),
        ]
        .into_iter()
        .enumerate()
        {
            if resources.writer.is_activated() {
                // A retry after the irreversible bind continues with durable checkpointing.
            } else {
                self.activation_fault(TopologyActivationFault::BeforeProducerInit, range_id)?;
                let recovered = crabka_gres_substrate::recover_live_for_range_with_restore(
                    resources.recovery_config.clone(),
                    resources.store.as_ref(),
                )
                .await
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("activate canonical successor writer: {error}"),
                    }
                })?;
                if recovered.generation != resources.generation {
                    return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                        range_id,
                        reason: format!(
                            "activated generation {} differs from staged generation {}",
                            recovered.generation.0, resources.generation.0
                        ),
                    });
                }
                self.activation_fault(TopologyActivationFault::AfterProducerInit, range_id)?;
                let canonical = Arc::new(crabka_gres_substrate::ProducerWalWriter::new(
                    recovered.producer,
                    resources.recovery_config.wal_topic(),
                ));
                if range_id == crabka_gres_ranges::RangeId::COORDINATOR {
                    split_activation::copy_must_activate_before_bind(
                        self,
                        &pending,
                        Arc::clone(&canonical),
                    )
                    .await?;
                }
                self.activation_fault(TopologyActivationFault::BeforeDeferredBind, range_id)?;
                resources
                    .writer
                    .activate(canonical)
                    .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("bind canonical successor writer: {error}"),
                    })?;
                self.activation_fault(TopologyActivationFault::AfterDeferredBind, range_id)?;
            }

            let receipt_engine = self
                .prepared
                .lock()
                .map_err(|_| range_pause_lock_error(range_id))?
                .as_ref()
                .and_then(|prepared| {
                    prepared
                        .engines
                        .get(&crabka_gres_ranges::RangeId::COORDINATOR)
                        .map(SqlEngine::clone_handle)
                })
                .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: "prepared range-zero engine missing during activation".into(),
                })?;
            use crabka_gres_ranges::control::{
                RangeZeroTopologyActivationStore, TopologyActivationPhase,
                TopologyActivationReceiptStore,
            };
            let store = RangeZeroTopologyActivationStore::new(
                resources.recovery_config.tenant.to_string(),
                receipt_engine,
            );
            let mut receipt = store
                .load(&pending.operation_id)
                .await
                .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: format!("load writer activation receipt: {reason}"),
                })?
                .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: "writer activation receipt missing from replacement range zero".into(),
                })?;
            if receipt.phase == TopologyActivationPhase::SourceCheckpoint {
                let expected = receipt.revision;
                receipt.revision = receipt.revision.saturating_add(1);
                receipt.phase = TopologyActivationPhase::MustActivate;
                receipt.source_checkpoint = Some(pending.source_checkpoint.clone());
                receipt.barrier_offset = Some(pending.barrier_offset);
                receipt.tail_sha256 = Some(pending.tail_sha256.clone());
                receipt
                    .targets
                    .get_mut(&pending.left_id)
                    .expect("validated left target")
                    .replay_journal_seq = Some(pending.left_replay_journal_seq);
                receipt
                    .targets
                    .get_mut(&pending.right_id)
                    .expect("validated right target")
                    .replay_journal_seq = Some(pending.right_replay_journal_seq);
                if !store
                    .compare_and_swap(&pending.operation_id, Some(expected), receipt.clone())
                    .await
                    .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("persist activation boundary: {reason}"),
                    })?
                {
                    return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: "activation boundary receipt CAS raced".into(),
                    });
                }
            } else if receipt.barrier_offset != Some(pending.barrier_offset)
                || receipt.tail_sha256.as_deref() != Some(pending.tail_sha256.as_str())
            {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id,
                    reason: "staged activation boundary differs from durable receipt".into(),
                });
            }
            if !receipt
                .targets
                .get(&range_id)
                .is_some_and(|target| target.writer_activated)
            {
                let expected = receipt.revision;
                receipt.revision = receipt.revision.saturating_add(1);
                receipt.phase = TopologyActivationPhase::WriterActivated;
                receipt
                    .targets
                    .get_mut(&range_id)
                    .expect("validated target receipt")
                    .writer_activated = true;
                if !store
                    .compare_and_swap(&pending.operation_id, Some(expected), receipt)
                    .await
                    .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("persist writer activation: {reason}"),
                    })?
                {
                    return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: "writer activation receipt CAS raced".into(),
                    });
                }
                self.activation_fault(
                    if index == 0 {
                        TopologyActivationFault::FirstWriterActivated
                    } else {
                        TopologyActivationFault::SecondWriterActivated
                    },
                    range_id,
                )?;
            }

            let mut receipt = store
                .load(&pending.operation_id)
                .await
                .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: format!("reload activation receipt: {reason}"),
                })?
                .expect("activation receipt persisted");
            if receipt
                .targets
                .get(&range_id)
                .and_then(|target| target.bootstrap_checkpoint.as_ref())
                .is_none()
            {
                let checkpoint = resources.checkpoint.as_ref().ok_or_else(|| {
                    crabka_gres_ranges::RangeTransferError::Unavailable {
                        range_id,
                        reason: "activated successor checkpoint runtime missing".into(),
                    }
                })?;
                let run = checkpoint
                    .handle
                    .checkpoint_from_source(
                        Arc::clone(&resources.snapshot_source),
                        crabka_gres_substrate::CheckpointTrigger::Manual,
                    )
                    .await
                    .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("write activated successor checkpoint: {error}"),
                    })?;
                let expected = receipt.revision;
                receipt.revision = receipt.revision.saturating_add(1);
                receipt
                    .targets
                    .get_mut(&range_id)
                    .expect("validated target receipt")
                    .bootstrap_checkpoint = Some(crabka_gres_ranges::CheckpointManifest {
                    range_id,
                    covered_offset: run.metadata.covered_offset,
                    manifest_key: run.metadata.manifest_key,
                });
                if !store
                    .compare_and_swap(&pending.operation_id, Some(expected), receipt)
                    .await
                    .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: format!("persist successor checkpoint: {reason}"),
                    })?
                {
                    return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id,
                        reason: "successor checkpoint receipt CAS raced".into(),
                    });
                }
                self.activation_fault(
                    if index == 0 {
                        TopologyActivationFault::FirstCheckpointDurable
                    } else {
                        TopologyActivationFault::SecondCheckpointDurable
                    },
                    range_id,
                )?;
            }
        }
        let receipt_engine = self
            .prepared
            .lock()
            .map_err(|_| range_pause_lock_error(pending.predecessor))?
            .as_ref()
            .and_then(|prepared| {
                prepared
                    .engines
                    .get(&crabka_gres_ranges::RangeId::COORDINATOR)
                    .map(SqlEngine::clone_handle)
            })
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: "prepared range-zero engine missing after activation".into(),
            })?;
        let store = RangeZeroTopologyActivationStore::new(activation_tenant, receipt_engine);
        let mut receipt = store
            .load(&pending.operation_id)
            .await
            .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: format!("load durable checkpoint phase: {reason}"),
            })?
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: pending.predecessor,
                reason: "activation receipt missing after successor checkpoints".into(),
            })?;
        if receipt.phase != TopologyActivationPhase::CheckpointDurable {
            if !receipt
                .targets
                .values()
                .all(|target| target.bootstrap_checkpoint.is_some())
            {
                return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: pending.predecessor,
                    reason: "successor checkpoint phase advanced before every target".into(),
                });
            }
            let expected = receipt.revision;
            receipt.revision = receipt.revision.saturating_add(1);
            receipt.phase = TopologyActivationPhase::CheckpointDurable;
            if !store
                .compare_and_swap(&pending.operation_id, Some(expected), receipt)
                .await
                .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: pending.predecessor,
                    reason: format!("persist durable checkpoint phase: {reason}"),
                })?
            {
                return Err(crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: pending.predecessor,
                    reason: "durable checkpoint phase CAS raced".into(),
                });
            }
            self.activation_fault(
                TopologyActivationFault::CheckpointDurable,
                pending.predecessor,
            )?;
        }
        Ok(())
    }

    fn finish_serving_topology_publication(&self) {
        self.range_service.finish_publication();
    }

    fn validate_successors(
        &self,
        plan: &crabka_gres_ranges::ValidatedSplitTransferPlan,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        let state = plan.state();
        let Some(expected) = self.config.advertised_endpoint.as_deref() else {
            return Err(crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: state.predecessor,
                reason: "live successor staging requires an authenticated advertised endpoint"
                    .to_owned(),
            });
        };
        let right = state.right.as_ref().ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: "split is missing its right successor".to_owned(),
            }
        })?;
        if state.left.endpoint != expected || right.endpoint != expected {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: format!(
                    "successor endpoint must match authenticated local endpoint {expected}"
                ),
            });
        }
        Ok(())
    }

    async fn force_checkpoint(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<crabka_gres_ranges::CheckpointManifest, crabka_gres_ranges::RangeTransferError>
    {
        let resources = self.range(range_id)?;
        let checkpoint = resources.checkpoint.as_ref().ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id,
                reason: "checkpoint flags were not configured for this runtime".to_owned(),
            }
        })?;
        let run = checkpoint
            .handle
            .checkpoint_from_source(
                Arc::clone(&checkpoint.snapshot_source),
                crabka_gres_substrate::CheckpointTrigger::Manual,
            )
            .await
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("force checkpoint: {error}"),
            })?;
        Ok(crabka_gres_ranges::CheckpointManifest {
            range_id,
            covered_offset: run.metadata.covered_offset,
            manifest_key: run.metadata.manifest_key,
        })
    }

    async fn pause_at_checkpoint(
        &self,
        checkpoint: &crabka_gres_ranges::CheckpointManifest,
    ) -> Result<crabka_gres_ranges::RangeTransferBarrier, crabka_gres_ranges::RangeTransferError>
    {
        let resources = self.range(checkpoint.range_id)?;
        if resources.snapshot_source.snapshot().covered_offset < checkpoint.covered_offset {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: checkpoint.range_id,
                reason: "writer has not reached the checkpoint covered offset".to_owned(),
            });
        }
        let reservation =
            PauseReservation::reserve(Arc::clone(&resources.pause), checkpoint.range_id)?;
        let paused = resources
            .writer
            .pause_and_barrier(resources.generation)
            .await
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: checkpoint.range_id,
                reason: format!("pause writer and commit barrier: {error}"),
            })?;
        let barrier = crabka_gres_ranges::RangeTransferBarrier {
            range_id: checkpoint.range_id,
            offset: paused.barrier_offset,
        };
        reservation.store(paused, checkpoint.range_id)?;
        Ok(barrier)
    }

    async fn read_committed_tail(
        &self,
        range_id: crabka_gres_ranges::RangeId,
        after_offset: i64,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<Vec<crabka_gres_ranges::CommittedTailRecord>, crabka_gres_ranges::RangeTransferError>
    {
        if barrier.range_id != range_id {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id,
                reason: "barrier belongs to another range".to_owned(),
            });
        }
        let resources = self.range(range_id)?;
        let is_current_barrier = {
            let state = resources
                .pause
                .lock()
                .map_err(|_| range_pause_lock_error(range_id))?;
            matches!(
                &*state,
                RangePauseState::Paused(paused) if paused.barrier_offset == barrier.offset
            )
        };
        if !is_current_barrier {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id,
                reason: "barrier is not held by this paused range writer".to_owned(),
            });
        }
        crabka_gres_substrate::read_live_committed_tail(
            &resources.recovery_config,
            after_offset,
            barrier.offset,
        )
        .await
        .map(|records| {
            records
                .into_iter()
                .map(|record| crabka_gres_ranges::CommittedTailRecord {
                    offset: record.offset,
                    bytes: record.bytes,
                })
                .collect()
        })
        .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
            range_id,
            reason: format!("read committed bounded tail: {error}"),
        })
    }

    async fn resume(
        &self,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        self.release_pause(barrier)
    }

    fn resume_after_drop(&self, barrier: crabka_gres_ranges::RangeTransferBarrier) {
        if self
            .activation_irreversible
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tracing::error!(
                range_id = barrier.range_id.as_u32(),
                "topology activation crossed writer binding; predecessor remains fail-closed for startup completion"
            );
            return;
        }
        if let Err(error) = self.release_pause(barrier) {
            tracing::error!(%error, range_id = barrier.range_id.as_u32(), "resume dropped range transfer pause");
        }
    }

    async fn stage_successors(
        &self,
        plan: &crabka_gres_ranges::ValidatedSplitTransferPlan,
        checkpoint: &crabka_gres_ranges::CheckpointManifest,
        tail: &[crabka_gres_ranges::CommittedTailRecord],
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<crabka_gres_ranges::StagedRangeSuccessors, crabka_gres_ranges::RangeTransferError>
    {
        self.validate_successors(plan)?;
        let state = plan.state();
        let requests = state.transfer_requests().map_err(|error| {
            crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: format!("invalid successor partition: {error}"),
            }
        })?;
        let stage_range = |request: crabka_gres_ranges::TableTransferRequest| async move {
            let source_manifest = checkpoint.clone();
            let source = self.range(checkpoint.range_id)?;
            validate_staged_transfer_boundary(&request, checkpoint, tail, barrier)?;
            if source.generation.0 != request.predecessor_generation {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: checkpoint.range_id,
                    reason: format!(
                        "source generation {} differs from requested predecessor generation {}",
                        source.generation.0, request.predecessor_generation
                    ),
                });
            }
            let source_holds_barrier = matches!(
                &*source
                    .pause
                    .lock()
                    .map_err(|_| range_pause_lock_error(checkpoint.range_id))?,
                RangePauseState::Paused(paused) if paused.barrier_offset == barrier.offset
            );
            if !source_holds_barrier {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: checkpoint.range_id,
                    reason: "source writer does not hold the requested transfer barrier".to_owned(),
                });
            }
            if self
                .ranges
                .read()
                .map_err(|_| range_pause_lock_error(request.target_range))?
                .contains_key(&request.target_range)
                && request.target_range != checkpoint.range_id
            {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: request.target_range,
                    reason: "successor range is already hosted".to_owned(),
                });
            }
            if self
                .staged
                .lock()
                .map_err(|_| range_pause_lock_error(request.target_range))?
                .contains_key(&request.target_range)
            {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: request.target_range,
                    reason: "successor range is already staged".to_owned(),
                });
            }
            let source_checkpoint = source.checkpoint.as_ref().ok_or_else(|| {
                crabka_gres_ranges::RangeTransferError::Unavailable {
                    range_id: checkpoint.range_id,
                    reason: "checkpoint flags were not configured for the source range".to_owned(),
                }
            })?;
            let expected_manifest_prefix = crabka_gres_substrate::ckpt_prefix_for_range(
                &source.recovery_config.tenant,
                checkpoint.range_id,
            );
            if !checkpoint
                .manifest_key
                .starts_with(&expected_manifest_prefix)
            {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: checkpoint.range_id,
                    reason: "source manifest key is outside the requested source range namespace"
                        .to_owned(),
                });
            }
            let staged_cache_dir = self.config.cache_dir.as_ref().map(|base| {
                base.join(format!(
                    "staged-{}-r{}-g{}",
                    state.operation_id,
                    request.target_range.as_u32(),
                    request.wal_generation
                ))
            });
            let target_store =
                open_substrate_range_cache(staged_cache_dir.as_deref(), request.target_range)
                    .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("open empty successor cache: {error}"),
                    })?;
            let target_recovery = crabka_gres_substrate::LiveRecoveryConfig::new(
                self.config.bootstrap.clone(),
                source.recovery_config.tenant.clone(),
                request.target_range,
                self.config.kafka_security.clone(),
            )
            .with_wal_generation(request.wal_generation)
            .with_optional_advertised_endpoint(self.config.advertised_endpoint.clone());
            crabka_gres_substrate::ensure_live_wal_topic(&target_recovery)
                .await
                .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id: request.target_range,
                    reason: format!("ensure staged successor WAL topic: {error}"),
                })?;
            let generation = crabka_gres_substrate::WriterGeneration(request.wal_generation);
            let filter = crabka_gres_substrate::CheckpointFilter::new(
                request.interval.start,
                request.interval.end,
            )
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: request.target_range,
                reason: format!("successor interval: {error}"),
            })?
            .with_physical_to_logical(plan.physical_to_logical().clone())
            .with_structural_ownership(request.target_range == state.left.range_id);
            let restore_plan =
                crabka_gres_substrate::restore_filtered_from_manifest_and_replay_tail(
                    source_checkpoint.store.as_ref(),
                    &checkpoint.manifest_key,
                    &source_checkpoint.tenant,
                    checkpoint.covered_offset,
                    target_store.as_ref(),
                    crabka_gres_substrate::RestoreTail {
                        current_generation: source.generation.0,
                        log_start: None,
                        committed_frames: tail
                            .iter()
                            .map(|record| crabka_gres_substrate::ReplayItem {
                                offset: record.offset,
                                bytes: record.bytes.clone(),
                            })
                            .collect(),
                        barrier_offset: barrier.offset,
                    },
                    filter,
                )
                .await
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("restore interval checkpoint and bounded tail: {error}"),
                    }
                })?;
            let writer = Arc::new(crabka_gres_substrate::DeferredWalWriter::staged());
            let snapshot_source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
                0,
                restore_plan.replay.next_journal_seq,
                generation,
            ));
            let checkpoint = build_range_checkpoint_runtime(
                &self.config,
                request.target_range,
                Arc::clone(&target_store),
                Arc::clone(&snapshot_source),
                target_recovery.wal_topic(),
                Some(Arc::clone(&source_checkpoint.store)),
            )
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: request.target_range,
                reason: format!("start successor checkpoint runtime: {error}"),
            })?
            .map(Arc::new)
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: request.target_range,
                reason: "successor staging requires checkpoint configuration".to_owned(),
            })?;
            let (mut engine, committer) = build_replicated_substrate_engine_with_committer(
                &target_store,
                Arc::clone(&writer),
                generation,
                restore_plan.replay.next_journal_seq,
                &snapshot_source,
                Some(Arc::clone(&checkpoint.stats)),
            )
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id: request.target_range,
                reason: format!("build successor SQL engine: {error}"),
            })?;
            let tso_horizon = request.target_range.is_coordinator().then(|| {
                let tso_store: Arc<dyn Kv> = target_store.clone();
                let tso_committer: Arc<dyn crabka_pgexec::Committer> = committer.clone();
                let tso_lease: Arc<dyn crabka_gres_substrate::FenceLease> = writer.clone();
                crabka_gres_substrate::SubstrateTsoHorizon::new(
                    tso_store,
                    tso_committer,
                    tso_lease,
                    generation,
                )
            });
            if let Some(horizon) = &tso_horizon {
                let persisted_max_ts = horizon.load_max_ts().map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("recover successor TSO horizon: {error}"),
                    }
                })?;
                let tso_rpc = crabka_gres_ranges::tso_rpc_from_horizon(
                    horizon.clone(),
                    horizon.clone(),
                    horizon.epoch(),
                    persisted_max_ts,
                )
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("recover successor TSO oracle: {error}"),
                    }
                })?;
                engine.set_timestamp_oracle(crabka_gres_ranges::pgexec_timestamp_oracle_from_rpc(
                    tso_rpc,
                ));
            }
            let resources = LiveRangeResources {
                store: target_store,
                writer,
                activation_committer: committer,
                snapshot_source,
                checkpoint: Some(checkpoint),
                recovery_config: target_recovery,
                generation,
                pause: Arc::new(std::sync::Mutex::new(RangePauseState::Idle)),
                tso_horizon,
            };
            self.staged
                .lock()
                .map_err(|_| range_pause_lock_error(request.target_range))?
                .insert(
                    request.target_range,
                    StagedLiveRangeSuccessor {
                        operation_id: state.operation_id.clone(),
                        source_checkpoint: source_manifest,
                        barrier_offset: barrier.offset,
                        tail_sha256: committed_tail_sha256(tail),
                        replay_journal_seq: restore_plan.replay.next_journal_seq,
                        engine,
                        resources,
                    },
                );
            Ok(crabka_gres_ranges::StagedRangeSuccessor {
                range_id: request.target_range,
                endpoint: request.endpoint,
                wal_generation: request.wal_generation,
            })
        };
        let [left_request, right_request] = requests;
        let left = stage_range(left_request).await?;
        let right = match stage_range(right_request).await {
            Ok(right) => right,
            Err(error) => {
                self.staged
                    .lock()
                    .map_err(|_| range_pause_lock_error(left.range_id))?
                    .remove(&left.range_id);
                return Err(error);
            }
        };
        Ok(crabka_gres_ranges::StagedRangeSuccessors { left, right })
    }

    async fn claim_successors(
        &self,
        staged: &crabka_gres_ranges::StagedRangeSuccessors,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<crabka_gres_ranges::ClaimedStagedSuccessors, crabka_gres_ranges::RangeTransferError>
    {
        let source = self.range(barrier.range_id)?;
        let source_holds_barrier = matches!(
            &*source
                .pause
                .lock()
                .map_err(|_| range_pause_lock_error(barrier.range_id))?,
            RangePauseState::Paused(paused) if paused.barrier_offset == barrier.offset
        );
        if !source_holds_barrier {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: barrier.range_id,
                reason: "source writer does not hold the requested transfer barrier".to_owned(),
            });
        }
        if staged.left.range_id == staged.right.range_id {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: staged.right.range_id,
                reason: "successor identities must be distinct".to_owned(),
            });
        }
        let mut successors = self
            .staged
            .lock()
            .map_err(|_| range_pause_lock_error(staged.right.range_id))?;
        if !successors.contains_key(&staged.left.range_id)
            || !successors.contains_key(&staged.right.range_id)
        {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: barrier.range_id,
                reason: "both successors must be staged before either is claimed".to_owned(),
            });
        }
        let left = successors.remove(&staged.left.range_id).ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: staged.left.range_id,
                reason: "left successor disappeared during atomic claim".to_owned(),
            }
        })?;
        let right = successors.remove(&staged.right.range_id).ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: staged.right.range_id,
                reason: "right successor disappeared during atomic claim".to_owned(),
            }
        })?;
        *self
            .pending
            .lock()
            .map_err(|_| range_pause_lock_error(barrier.range_id))? = Some(PendingLiveTopology {
            operation_id: left.operation_id.clone(),
            source_checkpoint: left.source_checkpoint.clone(),
            barrier_offset: left.barrier_offset,
            tail_sha256: left.tail_sha256.clone(),
            predecessor: barrier.range_id,
            left_id: staged.left.range_id,
            left_replay_journal_seq: left.replay_journal_seq,
            left: left.resources.clone(),
            right_id: staged.right.range_id,
            right_replay_journal_seq: right.replay_journal_seq,
            right: right.resources.clone(),
        });
        Ok(crabka_gres_ranges::ClaimedStagedSuccessors {
            left: crabka_gres_ranges::ClaimedStagedSuccessor {
                range_id: staged.left.range_id,
                endpoint: staged.left.endpoint.clone(),
                wal_generation: staged.left.wal_generation,
                engine: left.engine,
                keepalive: Arc::new(left.resources),
            },
            right: crabka_gres_ranges::ClaimedStagedSuccessor {
                range_id: staged.right.range_id,
                endpoint: staged.right.endpoint.clone(),
                wal_generation: staged.right.wal_generation,
                engine: right.engine,
                keepalive: Arc::new(right.resources),
            },
        })
    }
}

fn validate_staged_transfer_boundary(
    request: &crabka_gres_ranges::TableTransferRequest,
    checkpoint: &crabka_gres_ranges::CheckpointManifest,
    tail: &[crabka_gres_ranges::CommittedTailRecord],
    barrier: crabka_gres_ranges::RangeTransferBarrier,
) -> Result<(), crabka_gres_ranges::RangeTransferError> {
    if request.interval.range_id != request.target_range {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: request.target_range,
            reason: "successor interval range id differs from target range".to_owned(),
        });
    }
    if request.target_range == checkpoint.range_id
        && request.wal_generation <= request.predecessor_generation
    {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: request.target_range,
            reason: "same-id successor must advance the fenced WAL generation".to_owned(),
        });
    }
    if barrier.range_id != checkpoint.range_id {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: request.target_range,
            reason: "transfer barrier belongs to another source range".to_owned(),
        });
    }
    let Some(last) = tail.last() else {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: request.target_range,
            reason: "bounded transfer tail is empty".to_owned(),
        });
    };
    if last.offset != barrier.offset
        || tail.iter().any(|record| {
            record.offset <= checkpoint.covered_offset || record.offset > barrier.offset
        })
        || tail.windows(2).any(|pair| pair[0].offset >= pair[1].offset)
    {
        return Err(crabka_gres_ranges::RangeTransferError::Boundary {
            range_id: request.target_range,
            reason:
                "bounded transfer tail does not exactly cover the checkpoint-to-barrier interval"
                    .to_owned(),
        });
    }
    Ok(())
}

fn committed_tail_sha256(tail: &[crabka_gres_ranges::CommittedTailRecord]) -> String {
    use sha2::Digest;
    let mut digest = sha2::Sha256::new();
    for record in tail {
        digest.update(record.offset.to_be_bytes());
        digest.update(
            u64::try_from(record.bytes.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(&record.bytes);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[async_trait::async_trait]
impl FinalCheckpointer for StartedCheckpointRuntime {
    async fn latest_checkpoint_bytes(&self) -> std::io::Result<u64> {
        let snapshot = self.snapshot_source.snapshot();
        let metadata = crabka_gres_substrate::latest_checkpoint_metadata(
            self.store.as_ref(),
            &self.tenant,
            snapshot.wal_generation,
            None,
        )
        .await
        .map_err(|error| std::io::Error::other(format!("latest checkpoint metadata: {error}")))?;
        Ok(remember_latest_checkpoint_bytes(
            &self.latest_checkpoint_bytes,
            metadata.map(|metadata| metadata.total_bytes),
        ))
    }

    async fn force_final_checkpoint(&self) -> std::io::Result<FinalCheckpoint> {
        let run = self
            .handle
            .checkpoint_from_source(
                Arc::clone(&self.snapshot_source),
                crabka_gres_substrate::CheckpointTrigger::Manual,
            )
            .await
            .map_err(|error| std::io::Error::other(format!("final checkpoint: {error}")))?;
        Ok(FinalCheckpoint {
            wal_generation: run.metadata.wal_generation,
            covered_offset: run.metadata.covered_offset,
            manifest_key: run.metadata.manifest_key,
            total_bytes: run.metadata.total_bytes,
        })
    }
}

fn remember_latest_checkpoint_bytes(
    cached: &std::sync::atomic::AtomicU64,
    observed: Option<u64>,
) -> u64 {
    if let Some(bytes) = observed {
        cached.store(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    cached.load(std::sync::atomic::Ordering::Relaxed)
}

fn build_checkpoint_runtime(
    config: &SubstrateRuntimeConfig,
    store: Arc<dyn SubstrateKv>,
    snapshot_source: Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    wal_topic: String,
    checkpoint_namespace: String,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
    pruner: impl FnOnce() -> std::io::Result<GresCheckpointWalPruner>,
) -> std::io::Result<Option<StartedCheckpointRuntime>> {
    let Some(checkpoint_config) = &config.checkpoints else {
        return Ok(None);
    };
    let checkpoint_store = match checkpoint_store {
        Some(store) => store,
        None => build_checkpoint_store(checkpoint_config)?,
    };
    let service_config = crabka_gres_substrate::CheckpointConfig::new(
        checkpoint_namespace.clone(),
        wal_topic,
        checkpoint_config.frames_threshold,
        checkpoint_config.bytes_threshold,
        checkpoint_config.part_max_bytes,
        checkpoint_config.retain_newest,
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint config: {error}")))?;
    let stats = Arc::new(crabka_gres_substrate::CheckpointStats::default());
    let service = crabka_gres_substrate::CheckpointService::new(
        service_config,
        store,
        Arc::clone(&checkpoint_store),
        Arc::new(pruner()?),
        Arc::clone(&stats),
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint service: {error}")))?;
    Ok(Some(StartedCheckpointRuntime {
        handle: Arc::new(service).spawn(),
        stats,
        snapshot_source,
        store: checkpoint_store,
        tenant: checkpoint_namespace,
        latest_checkpoint_bytes: std::sync::atomic::AtomicU64::new(0),
    }))
}

fn build_range_checkpoint_runtime(
    config: &SubstrateRuntimeConfig,
    range_id: crabka_gres_ranges::RangeId,
    store: Arc<dyn SubstrateKv>,
    snapshot_source: Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    wal_topic: String,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
) -> std::io::Result<Option<StartedCheckpointRuntime>> {
    let Some(checkpoint_config) = &config.checkpoints else {
        return Ok(None);
    };
    let checkpoint_store = match checkpoint_store {
        Some(store) => store,
        None => build_checkpoint_store(checkpoint_config)?,
    };
    // CheckpointService derives object paths from this identity.  Encoding the range as a
    // path component yields `gres/<tenant>/r<range>/ckpt/`, never a shared tenant manifest set.
    let namespace = format!("{}/r{}", config.tenant, range_id.as_u32());
    let service_config = crabka_gres_substrate::CheckpointConfig::new(
        namespace.clone(),
        wal_topic,
        checkpoint_config.frames_threshold,
        checkpoint_config.bytes_threshold,
        checkpoint_config.part_max_bytes,
        checkpoint_config.retain_newest,
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint config: {error}")))?;
    let stats = Arc::new(crabka_gres_substrate::CheckpointStats::default());
    let service = crabka_gres_substrate::CheckpointService::new(
        service_config,
        store,
        Arc::clone(&checkpoint_store),
        Arc::new(GresCheckpointWalPruner::kafka(
            &config.bootstrap,
            config.kafka_security.clone(),
        )?),
        Arc::clone(&stats),
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint service: {error}")))?;
    Ok(Some(StartedCheckpointRuntime {
        handle: Arc::new(service).spawn(),
        stats,
        snapshot_source,
        store: checkpoint_store,
        tenant: namespace,
        latest_checkpoint_bytes: std::sync::atomic::AtomicU64::new(0),
    }))
}

async fn open_live_substrate_runtime(
    config: &SubstrateRuntimeConfig,
    store: Arc<dyn SubstrateKv>,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<GresRuntime> {
    let wal_selection = single_range_live_wal_selection(config, tenant_record)?;
    let checkpoint_store = config
        .checkpoints
        .as_ref()
        .map(build_checkpoint_store)
        .transpose()?;
    let recovery_config = recovery_config_with_checkpoint_store(
        wal_selection.recovery_config,
        checkpoint_store.as_ref(),
    );
    let recovered =
        crabka_gres_substrate::recover_live_for_range_with_restore(recovery_config, store.as_ref())
            .await
            .map_err(|error| std::io::Error::other(format!("substrate recovery: {error}")))?;
    let writer = Arc::new(crabka_gres_substrate::ProducerWalWriter::new(
        recovered.producer,
        wal_selection.writer_topic,
    ));
    crabka_gres_substrate::FenceLease::assert_current(writer.as_ref(), recovered.generation)
        .await
        .map_err(|error| std::io::Error::other(format!("WAL writer readiness fence: {error}")))?;
    let snapshot_source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
        recovered.barrier_offset,
        recovered.next_journal_seq,
        recovered.generation,
    ));
    let checkpoint = build_checkpoint_runtime(
        config,
        Arc::clone(&store),
        Arc::clone(&snapshot_source),
        wal_selection.checkpoint_topic,
        wal_selection.checkpoint_namespace,
        checkpoint_store,
        || GresCheckpointWalPruner::kafka(&config.bootstrap, config.kafka_security.clone()),
    )?;
    let engine = build_replicated_substrate_engine(
        &store,
        writer,
        recovered.generation,
        recovered.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|checkpoint| Arc::clone(&checkpoint.stats)),
    )?;
    Ok(match checkpoint {
        Some(checkpoint) => GresRuntime::with_checkpoint_runtime(engine, checkpoint),
        None => GresRuntime::new(engine),
    })
}

fn recovery_config_with_checkpoint_store(
    recovery_config: crabka_gres_substrate::LiveRecoveryConfig,
    checkpoint_store: Option<&Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
) -> crabka_gres_substrate::LiveRecoveryConfig {
    match checkpoint_store {
        Some(store) => recovery_config.with_checkpoints(Arc::clone(store)),
        None => recovery_config,
    }
}

fn build_replicated_substrate_engine<W>(
    store: &Arc<dyn SubstrateKv>,
    writer: Arc<W>,
    generation: crabka_gres_substrate::WriterGeneration,
    next_journal_seq: u64,
    snapshot_source: &Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    checkpoint_stats: Option<Arc<crabka_gres_substrate::CheckpointStats>>,
) -> std::io::Result<SqlEngine>
where
    W: crabka_gres_substrate::TransactionalWalWriter + crabka_gres_substrate::FenceLease + 'static,
{
    build_replicated_substrate_engine_with_committer(
        store,
        writer,
        generation,
        next_journal_seq,
        snapshot_source,
        checkpoint_stats,
    )
    .map(|(engine, _committer)| engine)
}

fn build_replicated_substrate_engine_with_committer<W>(
    store: &Arc<dyn SubstrateKv>,
    writer: Arc<W>,
    generation: crabka_gres_substrate::WriterGeneration,
    next_journal_seq: u64,
    snapshot_source: &Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    checkpoint_stats: Option<Arc<crabka_gres_substrate::CheckpointStats>>,
) -> std::io::Result<(SqlEngine, Arc<crabka_gres_substrate::SubstrateCommitter<W>>)>
where
    W: crabka_gres_substrate::TransactionalWalWriter + crabka_gres_substrate::FenceLease + 'static,
{
    snapshot_source.set_fence_lease(
        writer.clone() as Arc<dyn crabka_gres_substrate::FenceLease>,
        generation,
    );
    let committer_store: Arc<dyn Kv> = store.clone();
    let engine_read_store: Arc<dyn Kv> = store.clone();
    let engine_write_store: Arc<dyn Kv> = store.clone();
    let committer = crabka_gres_substrate::SubstrateCommitter::new(
        committer_store,
        Arc::clone(&writer),
        generation,
        next_journal_seq,
    )
    .with_checkpoint_snapshot_source(Arc::clone(snapshot_source));
    let committer = if let Some(checkpoint_stats) = checkpoint_stats {
        committer.with_checkpoint_stats(checkpoint_stats)
    } else {
        committer
    };
    let committer = Arc::new(committer);
    let linearizer = crabka_gres_substrate::SubstrateLinearizer::new(writer, generation);
    let engine = SqlEngine::replicated(
        engine_read_store,
        engine_write_store,
        committer.clone(),
        Arc::new(linearizer),
    )
    .map_err(|error| std::io::Error::other(format!("engine: {error:?}")))?;
    engine
        .reseed_counters()
        .map_err(|error| std::io::Error::other(format!("reseed counters: {error:?}")))?;
    let horizon = engine.checkpoint_horizon_provider();
    snapshot_source.set_garbage_horizon_provider(Arc::new(move || {
        horizon().map_err(|error| {
            crabka_gres_substrate::SubstrateError::Checkpoint(format!("garbage horizon: {error:?}"))
        })
    }));
    Ok((engine, committer))
}

fn open_substrate_cache(
    cache_dir: Option<&std::path::Path>,
) -> std::io::Result<Arc<dyn SubstrateKv>> {
    let Some(dir) = cache_dir else {
        return Ok(Arc::new(MemKv::default()));
    };
    std::fs::create_dir_all(dir)?;
    Ok(Arc::new(FjallKv::open_cache(dir).map_err(|error| {
        std::io::Error::other(format!("cache dir: {error:?}"))
    })?))
}

fn open_substrate_range_cache(
    cache_dir: Option<&std::path::Path>,
    range_id: crabka_gres_ranges::RangeId,
) -> std::io::Result<Arc<dyn SubstrateKv>> {
    let Some(dir) = cache_dir else {
        return Ok(Arc::new(MemKv::default()));
    };
    open_substrate_cache(Some(&dir.join(format!("r{}", range_id.as_u32()))))
}

/// Register Crabka's Kafka foreign-data scanner with the SQL engine.
pub fn register_kafka_scanner(engine: &mut SqlEngine) {
    engine.set_foreign_scanner(Arc::new(crabka_gres_fdw::KafkaFdw::with_defaults(None)));
}

/// Register Crabka's Kafka foreign-data scanner with an optional default bootstrap.
pub fn register_kafka_scanner_with_default_bootstrap(
    engine: &mut RuntimeEngine,
    default_bootstrap: Option<String>,
) {
    let scanner: Arc<dyn crabka_pgexec::foreign::ForeignScanner> =
        Arc::new(crabka_gres_fdw::KafkaFdw::with_defaults(default_bootstrap));
    match engine {
        RuntimeEngine::Single(engine) => engine.set_foreign_scanner(scanner),
        RuntimeEngine::Multi(tenant) => tenant.set_foreign_scanner(&scanner),
    }
}

fn kafka_scanner_default_bootstrap(args: &ServeArgs) -> Option<String> {
    args.substrate_bootstrap.clone()
}

/// Build pgwire session authentication and startup configuration.
pub fn build_session_config(args: &ServeArgs) -> std::io::Result<SessionConfig> {
    build_session_config_from_tenant(args, None)
}

/// Build pgwire session config after optional substrate tenant config has been loaded.
pub fn build_session_config_from_tenant(
    args: &ServeArgs,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<SessionConfig> {
    use std::io::{Error, ErrorKind};

    match (args.auth.as_deref(), tenant_record) {
        (Some("trust"), _) | (None, None) => Ok(SessionConfig::trust()),
        (Some("scram"), _) => build_scram_session_config(&args.user_creds),
        (None, Some(record)) => build_tenant_scram_session_config(record),
        (other, _) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unknown --auth {other:?}: expected \"trust\" or \"scram\""),
        )),
    }
}

fn build_tenant_scram_session_config(record: &TenantRecord) -> std::io::Result<SessionConfig> {
    let pg_verifier = PgScramVerifier::parse(&record.scram_verifier).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("tenant SCRAM verifier: {error}"),
        )
    })?;
    let verifier = crabka_pgwire::scram::ScramVerifier::from_parts(
        pg_verifier.salt,
        pg_verifier.iterations,
        sha256_key_array(pg_verifier.stored_key, "stored_key")?,
        sha256_key_array(pg_verifier.server_key, "server_key")?,
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let mut verifiers = HashMap::new();
    verifiers.insert(record.sql_user.as_str().to_string(), verifier);
    let mock_secret: [u8; 32] = rand::rng().random();
    Ok(SessionConfig {
        auth: AuthMode::ScramSha256 {
            verifiers,
            mock_secret,
        },
        ..SessionConfig::trust()
    })
}

fn sha256_key_array(bytes: Vec<u8>, field: &'static str) -> std::io::Result<[u8; 32]> {
    let actual = bytes.len();
    bytes.try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("tenant SCRAM verifier {field} must be 32 bytes, got {actual}"),
        )
    })
}

fn build_scram_session_config(user_creds: &[String]) -> std::io::Result<SessionConfig> {
    use std::io::{Error, ErrorKind};

    if user_creds.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "--auth scram requires --user-cred",
        ));
    }

    let mut verifiers = std::collections::HashMap::new();
    for credential in user_creds {
        let (user, password) = credential.split_once('=').ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "--user-cred must be USER=PASSWORD")
        })?;
        if user.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "--user-cred user name is empty",
            ));
        }
        let salt: [u8; crabka_pgwire::scram::SALT_LEN] = rand::rng().random();
        verifiers.insert(
            user.to_string(),
            crabka_pgwire::scram::ScramVerifier::from_password(password, salt.to_vec(), 4096),
        );
    }

    let mock_secret: [u8; 32] = rand::rng().random();
    Ok(SessionConfig {
        auth: AuthMode::ScramSha256 {
            verifiers,
            mock_secret,
        },
        ..SessionConfig::trust()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    use clap::{CommandFactory as _, Parser as _};

    use super::*;

    #[test]
    fn checkpoint_size_cache_survives_pruning_and_accepts_newer_smaller_manifest() {
        let cached = std::sync::atomic::AtomicU64::new(0);
        assert_eq!(
            remember_latest_checkpoint_bytes(&cached, Some(1_000)),
            1_000
        );
        assert_eq!(remember_latest_checkpoint_bytes(&cached, None), 1_000);
        assert_eq!(remember_latest_checkpoint_bytes(&cached, Some(100)), 100);
        assert_eq!(remember_latest_checkpoint_bytes(&cached, None), 100);
    }

    struct FakeCheckpointer {
        bytes: AtomicU64,
        checkpoints: AtomicUsize,
    }

    impl FakeCheckpointer {
        fn new(bytes: u64) -> Self {
            Self {
                bytes: AtomicU64::new(bytes),
                checkpoints: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl FinalCheckpointer for FakeCheckpointer {
        async fn latest_checkpoint_bytes(&self) -> std::io::Result<u64> {
            Ok(self.bytes.load(Ordering::SeqCst))
        }

        async fn force_final_checkpoint(&self) -> std::io::Result<FinalCheckpoint> {
            self.checkpoints.fetch_add(1, Ordering::SeqCst);
            Ok(FinalCheckpoint {
                wal_generation: 0,
                covered_offset: 1,
                manifest_key: "gres/tenant-a/ckpt/MANIFEST".into(),
                total_bytes: self.bytes.load(Ordering::SeqCst),
            })
        }
    }

    #[derive(Default)]
    struct FakeSuspendRegistry {
        marked: AtomicBool,
    }

    #[async_trait::async_trait]
    impl SuspendRegistry for FakeSuspendRegistry {
        async fn mark_suspended(
            &mut self,
            tenant: &str,
            checkpoint: FinalCheckpoint,
        ) -> std::io::Result<()> {
            assert_eq!(tenant, "tenant-a");
            assert_eq!(checkpoint.manifest_key, "gres/tenant-a/ckpt/MANIFEST");
            self.marked.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn serve_args(auth: Option<&str>, user_creds: Vec<String>) -> ServeArgs {
        ServeArgs {
            listen: "127.0.0.1:0".to_string(),
            tls_cert: None,
            tls_key: None,
            auth: auth.map(str::to_string),
            user_creds,
            data_dir: None,
            substrate_bootstrap: None,
            tenant: None,
            cache_dir: None,
            ranges: None,
            host_ranges: None,
            range_listen: None,
            range_tls_cert: None,
            range_tls_key: None,
            range_tls_ca: None,
            range_tls_server_name: None,
            range_allowed_principals: Vec::new(),
            checkpoint_store: None,
            checkpoint_bucket: None,
            checkpoint_prefix: None,
            checkpoint_local_root: None,
            checkpoint_region: None,
            checkpoint_endpoint: None,
            checkpoint_access_key_id: None,
            checkpoint_secret_access_key: None,
            checkpoint_allow_http: false,
            checkpoint_gcs_service_account_path: None,
            checkpoint_gcs_service_account_key: None,
            checkpoint_gcs_application_credentials_path: None,
            checkpoint_frames: None,
            checkpoint_bytes: None,
            checkpoint_part_bytes: None,
            checkpoint_retain: None,
        }
    }

    fn substrate_args() -> ServeArgs {
        ServeArgs {
            substrate_bootstrap: Some("memory://".to_string()),
            tenant: Some("tenant-a".to_string()),
            ..serve_args(Some("trust"), Vec::new())
        }
    }

    #[test]
    fn kafka_scanner_has_no_default_bootstrap_in_local_mode() {
        let args = serve_args(Some("trust"), Vec::new());

        assert_eq!(kafka_scanner_default_bootstrap(&args), None);
    }

    #[test]
    fn kafka_scanner_uses_substrate_bootstrap_as_default() {
        let args = substrate_args();

        assert_eq!(
            kafka_scanner_default_bootstrap(&args),
            Some("memory://".to_string())
        );
    }

    #[test]
    fn single_range_live_recovery_config_carries_checkpoint_store_when_configured() {
        let args = ServeArgs {
            substrate_bootstrap: Some("localhost:9092".to_string()),
            tenant: Some("tenant-a".to_string()),
            checkpoint_store: Some(CheckpointStoreKind::InMemory),
            ..serve_args(Some("trust"), Vec::new())
        };
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("config")
            .expect("substrate config");
        let wal_selection = single_range_live_wal_selection(&config, None).expect("wal selection");
        let checkpoint_store: Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore> =
            crabka_gres_substrate::checkpoint::InMemoryCheckpointStore::shared();

        let recovery_config = recovery_config_with_checkpoint_store(
            wal_selection.recovery_config,
            Some(&checkpoint_store),
        );

        assert!(recovery_config.checkpoints.is_some());
        assert_eq!(recovery_config.wal_topic(), "__gres_wal.tenant-a.r0");
    }

    fn tenant_record() -> TenantRecord {
        let verifier =
            PgScramVerifier::generate_with_salt("hunter2", 4096, vec![1; 16]).expect("verifier");
        TenantRecord::new(
            1,
            crabka_gres_control::TenantId::try_from("tenant-a").expect("tenant id"),
            TenantName::try_from("tenant-a").expect("tenant name"),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").expect("sql user"),
            verifier.to_string(),
            1,
        )
        .expect("tenant record")
    }

    fn suspend_policy() -> SuspendPolicy {
        SuspendPolicy {
            tenant: "tenant-a".to_string(),
            idle_window: Duration::from_millis(1),
            suspend_max_checkpoint_bytes: Some(100),
        }
    }

    fn idle_activity() -> Arc<crabka_pgwire::server::ActivityTracker> {
        let old = current_unix_millis().expect("clock").saturating_sub(60_000);
        Arc::new(crabka_pgwire::server::ActivityTracker::with_last_activity_unix_millis(old))
    }

    #[tokio::test]
    async fn idle_suspend_forces_final_checkpoint_then_marks_registry_suspended() {
        let activity = idle_activity();
        let checkpointer = FakeCheckpointer::new(64);
        let mut registry = FakeSuspendRegistry::default();

        let outcome = try_suspend_idle_tenant(
            &suspend_policy(),
            activity.as_ref(),
            &checkpointer,
            &mut registry,
        )
        .await
        .expect("suspend");

        assert_eq!(outcome, SuspendMonitorOutcome::Suspended);
        assert_eq!(checkpointer.checkpoints.load(Ordering::SeqCst), 1);
        assert!(registry.marked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn open_session_blocks_idle_suspend() {
        let activity = idle_activity();
        let _session = activity.try_open_session().expect("session");
        let checkpointer = FakeCheckpointer::new(64);
        let mut registry = FakeSuspendRegistry::default();

        let outcome = try_suspend_idle_tenant(
            &suspend_policy(),
            activity.as_ref(),
            &checkpointer,
            &mut registry,
        )
        .await
        .expect("check");

        assert_eq!(outcome, SuspendMonitorOutcome::OpenSessions { count: 1 });
        assert_eq!(checkpointer.checkpoints.load(Ordering::SeqCst), 0);
        assert!(!registry.marked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn closed_admission_rejects_racing_session_before_suspend_checkpoint() {
        let activity = idle_activity();
        activity.close_for_suspend().expect("close admission");
        let checkpointer = FakeCheckpointer::new(64);
        let mut registry = FakeSuspendRegistry::default();

        assert!(activity.try_open_session().is_none());
        let outcome = try_suspend_idle_tenant(
            &suspend_policy(),
            activity.as_ref(),
            &checkpointer,
            &mut registry,
        )
        .await
        .expect("check");

        assert_eq!(outcome, SuspendMonitorOutcome::AdmissionAlreadyClosed);
        assert_eq!(checkpointer.checkpoints.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn large_checkpoint_skips_suspend_and_remains_warm() {
        let activity = idle_activity();
        let checkpointer = FakeCheckpointer::new(101);
        let mut registry = FakeSuspendRegistry::default();

        let outcome = try_suspend_idle_tenant(
            &suspend_policy(),
            activity.as_ref(),
            &checkpointer,
            &mut registry,
        )
        .await
        .expect("check");

        assert_eq!(
            outcome,
            SuspendMonitorOutcome::CheckpointTooLarge {
                bytes: 101,
                max_bytes: 100,
            }
        );
        assert_eq!(checkpointer.checkpoints.load(Ordering::SeqCst), 0);
        assert!(!registry.marked.load(Ordering::SeqCst));
        assert!(activity.try_open_session().is_some());
    }

    struct EmptyTenantConfigLoader;

    #[async_trait::async_trait]
    impl TenantConfigLoader for EmptyTenantConfigLoader {
        async fn load_tenant_config(
            &self,
            _bootstrap: &str,
            _tenant: &TenantName,
            _security: Option<ClientSecurity>,
        ) -> std::io::Result<Option<TenantRecord>> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct RecordingTenantConfigLoader {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TenantConfigLoader for RecordingTenantConfigLoader {
        async fn load_tenant_config(
            &self,
            _bootstrap: &str,
            _tenant: &TenantName,
            _security: Option<ClientSecurity>,
        ) -> std::io::Result<Option<TenantRecord>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    #[test]
    fn trust_auth_builds_default_session_config() {
        let config = build_session_config(&serve_args(Some("trust"), Vec::new())).expect("config");
        assert!(matches!(config.auth, AuthMode::Trust));
    }

    #[test]
    fn scram_auth_requires_at_least_one_credential() {
        let error =
            build_session_config(&serve_args(Some("scram"), Vec::new())).expect_err("error");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "--auth scram requires --user-cred");
    }

    #[test]
    fn substrate_tenant_record_builds_default_scram_session_config() {
        let args = serve_args(None, Vec::new());
        let config =
            build_session_config_from_tenant(&args, Some(&tenant_record())).expect("config");

        assert!(matches!(config.auth, AuthMode::ScramSha256 { .. }));
    }

    #[test]
    fn explicit_trust_overrides_substrate_scram_for_dev() {
        let args = serve_args(Some("trust"), Vec::new());
        let config =
            build_session_config_from_tenant(&args, Some(&tenant_record())).expect("config");

        assert!(matches!(config.auth, AuthMode::Trust));
    }

    #[tokio::test]
    async fn missing_substrate_tenant_config_names_create_tenant_command() {
        let mut args = substrate_args();
        args.auth = None;

        let error = load_substrate_tenant_record(&args, &EmptyTenantConfigLoader)
            .await
            .expect_err("missing tenant config");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("__gres_cfg.tenant-a"));
        assert!(error.to_string().contains("crabka gres create-tenant"));
    }

    #[tokio::test]
    async fn live_substrate_ranges_reach_tenant_config_loader() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let mut args = substrate_args();
        args.substrate_bootstrap = Some("127.0.0.1:1".to_string());
        args.ranges = Some("0,100,200".to_string());
        let loader = RecordingTenantConfigLoader::default();

        let error = serve_listener_with_tenant_config_loader(listener, args, &loader)
            .await
            .expect_err("missing tenant config after loader is called");

        assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("missing tenant config"));
    }

    #[tokio::test]
    async fn memory_substrate_ranges_reach_tenant_config_loader() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let mut args = substrate_args();
        args.ranges = Some("0,100,200".to_string());
        let loader = RecordingTenantConfigLoader::default();

        let error = serve_listener_with_tenant_config_loader(listener, args, &loader)
            .await
            .expect_err("missing tenant config after loader is called");

        assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("missing tenant config"));
    }

    #[test]
    fn tenant_checkpoint_fields_default_and_cli_flags_override() {
        let mut record = tenant_record();
        record.bucket_prefix = Some("from-record".to_string());
        record.checkpoint_frames = Some(77);
        record.checkpoint_bytes = Some(88);
        let mut args = substrate_args();
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);

        let applied = apply_tenant_runtime_defaults(args.clone(), Some(&record)).expect("defaults");
        assert_eq!(applied.checkpoint_prefix.as_deref(), Some("from-record"));
        assert_eq!(applied.checkpoint_frames.map(NonZeroU64::get), Some(77));
        assert_eq!(applied.checkpoint_bytes.map(NonZeroU64::get), Some(88));

        args.checkpoint_prefix = Some("cli".to_string());
        args.checkpoint_frames = Some(NonZeroU64::new(7).expect("nonzero"));
        args.checkpoint_bytes = Some(NonZeroU64::new(8).expect("nonzero"));
        let applied = apply_tenant_runtime_defaults(args, Some(&record)).expect("overrides");
        assert_eq!(applied.checkpoint_prefix.as_deref(), Some("cli"));
        assert_eq!(applied.checkpoint_frames.map(NonZeroU64::get), Some(7));
        assert_eq!(applied.checkpoint_bytes.map(NonZeroU64::get), Some(8));
    }

    #[test]
    fn cli_help_exposes_only_single_node_serve_surface() {
        let mut help = Vec::new();
        Cli::command()
            .write_long_help(&mut help)
            .expect("write help");
        let help = String::from_utf8(help).expect("help is utf8");

        assert!(help.contains("--listen"));
        assert!(help.contains("--data-dir"));
        assert!(help.contains("--substrate-bootstrap"));
        assert!(help.contains("--tenant"));
        assert!(help.contains("--cache-dir"));
        assert!(help.contains("--ranges"));
        assert!(help.contains("--host-ranges"));
        assert!(help.contains("--checkpoint-bucket"));
        assert!(help.contains("--checkpoint-store"));
        assert!(help.contains("--checkpoint-frames"));
        assert!(help.contains("--checkpoint-bytes"));
        assert!(help.contains("--checkpoint-retain"));
        assert!(help.contains("--auth"));
        assert!(help.contains("--tls-cert"));
        assert!(!help.contains("node"));
        assert!(!help.contains("--node-addr"));
        assert!(!help.contains("--sql-addr"));
        assert!(!help.contains("--peer"));
    }

    #[test]
    fn cli_parse_accepts_serve_options_without_subcommand() {
        let data_dir = std::path::PathBuf::from("/tmp/crabka-gres-test");

        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--listen",
            "127.0.0.1:15433",
            "--data-dir",
            "/tmp/crabka-gres-test",
            "--auth",
            "scram",
            "--user-cred",
            "postgres=secret",
        ])
        .expect("serve options parse");

        assert_eq!(cli.serve.listen, "127.0.0.1:15433");
        assert_eq!(cli.serve.data_dir, Some(data_dir));
        assert_eq!(cli.serve.auth.as_deref(), Some("scram"));
        assert_eq!(cli.serve.user_creds, ["postgres=secret"]);
    }

    #[test]
    fn cli_parse_accepts_substrate_options() {
        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--listen",
            "127.0.0.1:15433",
            "--substrate-bootstrap",
            "memory://",
            "--tenant",
            "tenant-a",
            "--cache-dir",
            "/tmp/crabka-gres-cache",
        ])
        .expect("substrate options parse");

        assert_eq!(cli.serve.substrate_bootstrap.as_deref(), Some("memory://"));
        assert_eq!(cli.serve.tenant.as_deref(), Some("tenant-a"));
        assert_eq!(
            cli.serve.cache_dir,
            Some(std::path::PathBuf::from("/tmp/crabka-gres-cache"))
        );
    }

    #[test]
    fn cli_parse_accepts_checkpoint_s3_options() {
        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap",
            "127.0.0.1:9092",
            "--tenant",
            "tenant-a",
            "--checkpoint-bucket",
            "gres-checkpoints",
            "--checkpoint-region",
            "us-east-1",
            "--checkpoint-prefix",
            "dev/gres",
            "--checkpoint-frames",
            "100",
            "--checkpoint-bytes",
            "1048576",
            "--checkpoint-retain",
            "3",
        ])
        .expect("checkpoint options parse");

        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");

        assert_eq!(
            config.checkpoints.as_ref().map(|cfg| cfg.frames_threshold),
            Some(100)
        );
        assert_eq!(
            config.checkpoints.as_ref().map(|cfg| cfg.bytes_threshold),
            Some(1_048_576)
        );
        assert_eq!(
            config.checkpoints.as_ref().map(|cfg| cfg.retain_newest),
            Some(3)
        );
        assert!(matches!(
            config.checkpoints.expect("checkpoint config").object_store,
            CheckpointObjectStoreConfig::S3 { ref bucket, ref region, ref prefix, .. }
                if bucket == "gres-checkpoints" && region == "us-east-1" && prefix.as_deref() == Some("dev/gres")
        ));
    }

    #[test]
    fn checkpoint_options_without_object_store_are_rejected() {
        let mut args = substrate_args();
        args.checkpoint_frames = Some(NonZeroU64::new(10).expect("nonzero"));

        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("missing object store");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("checkpoint thresholds require"));
    }

    #[test]
    fn s3_checkpoint_credentials_fall_back_to_standard_environment_values() {
        let env_access = "minio".to_string();
        let env_secret = "minio-secret".to_string();
        let (access, secret) =
            resolve_s3_credentials(None, None, Some(&env_access), Some(&env_secret))
                .expect("paired environment credentials");

        assert_eq!(access.as_deref(), Some("minio"));
        assert_eq!(secret.as_deref(), Some("minio-secret"));
        assert!(resolve_s3_credentials(None, None, Some(&env_access), None).is_err());
    }

    #[test]
    fn checkpoint_options_require_substrate_mode() {
        let mut args = serve_args(Some("trust"), Vec::new());
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);

        let config = SubstrateRuntimeConfig::from_args(&args).expect_err("substrate required");

        assert_eq!(config.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            config.to_string(),
            "checkpoint options require --substrate-bootstrap"
        );
    }

    #[test]
    fn checkpoint_s3_requires_region() {
        let mut args = substrate_args();
        args.checkpoint_bucket = Some("gres-checkpoints".to_string());

        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("region required");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "--checkpoint-region is required for the selected checkpoint store"
        );
    }

    #[test]
    fn checkpoint_prefix_rejects_pathlike_slashes() {
        let mut args = substrate_args();
        args.checkpoint_bucket = Some("gres-checkpoints".to_string());
        args.checkpoint_region = Some("us-east-1".to_string());
        args.checkpoint_prefix = Some("/bad".to_string());

        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("bad prefix");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "--checkpoint-prefix must not start or end with '/'"
        );
    }

    #[test]
    fn checkpoint_local_store_builds_adapter() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut args = substrate_args();
        args.checkpoint_store = Some(CheckpointStoreKind::Local);
        args.checkpoint_local_root = Some(temp_dir.path().to_path_buf());
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config")
            .checkpoints
            .expect("checkpoint config");

        let store = build_checkpoint_store(&config).expect("checkpoint store");

        assert_eq!(Arc::strong_count(&store), 1);
    }

    #[tokio::test]
    async fn checkpoint_enabled_runtime_starts_control_loop() {
        let mut args = substrate_args();
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");

        let runtime = open_substrate_runtime(&config).await.expect("runtime");

        assert!(runtime.has_checkpoint_handle());
    }

    #[test]
    fn checkpoint_part_bytes_rejects_too_small_values() {
        let mut args = substrate_args();
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);
        args.checkpoint_part_bytes = Some(NonZeroUsize::new(7).expect("nonzero"));

        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("part too small");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "--checkpoint-part-bytes must be at least 8"
        );
    }

    #[test]
    fn cli_parse_requires_tenant_for_substrate_mode() {
        let error = Cli::try_parse_from(["crabka-gres", "--substrate-bootstrap", "memory://"])
            .expect_err("tenant is required");

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn cli_parse_rejects_substrate_with_data_dir() {
        let error = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap",
            "memory://",
            "--tenant",
            "tenant-a",
            "--data-dir",
            "/tmp/crabka-gres-data",
        ])
        .expect_err("data-dir conflicts with substrate mode");

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn substrate_config_rejects_empty_tenant() {
        let mut args = substrate_args();
        args.tenant = Some(String::new());

        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("empty tenant");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "--tenant must not be empty");
    }

    #[tokio::test]
    async fn substrate_runtime_constructs_in_process_engine() {
        let config = SubstrateRuntimeConfig::from_args(&substrate_args())
            .expect("valid config")
            .expect("substrate config");

        let mut engine = open_substrate_engine(&config).await.expect("engine");

        register_kafka_scanner(&mut engine);
    }

    #[tokio::test]
    async fn substrate_runtime_constructs_multirange_gateway() {
        let mut args = substrate_args();
        args.ranges = Some("0,100,200".to_string());
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");

        let runtime = open_substrate_runtime(&config).await.expect("runtime");

        assert!(matches!(runtime.engine, RuntimeEngine::Multi(_)));
    }

    #[tokio::test]
    async fn live_substrate_multirange_uses_broker_recovery_not_local_engines() {
        let config = SubstrateRuntimeConfig {
            bootstrap: "127.0.0.1:1".to_string(),
            tenant: "tenant-a".to_string(),
            cache_dir: None,
            checkpoints: None,
            kafka_security: None,
            ranges: Some("0,100,200".to_string()),
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
        };

        let Err(error) = open_substrate_runtime(&config).await else {
            panic!("unreachable broker should fail before serving live multirange runtime");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("substrate recovery"));
        assert!(!error.to_string().contains("multi-range tenant"));
    }

    #[tokio::test]
    async fn live_substrate_multirange_checkpoint_knobs_reach_range_recovery() {
        let mut args = substrate_args();
        args.substrate_bootstrap = Some("127.0.0.1:1".to_string());
        args.ranges = Some("0,100,200".to_string());
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");

        let Err(error) = open_substrate_runtime(&config).await else {
            panic!("unreachable broker should fail during range-specific recovery");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("substrate recovery"));
    }

    #[tokio::test]
    async fn memory_substrate_multirange_checkpoint_knobs_fail_clearly() {
        let mut args = substrate_args();
        args.ranges = Some("0,100,200".to_string());
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");

        let Err(error) = open_substrate_runtime(&config).await else {
            panic!("in-memory multi-range checkpoints lack a durable transfer capability");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error
                .to_string()
                .contains("requires a live substrate broker")
        );
    }

    #[tokio::test]
    async fn single_range_checkpoint_runtime_writes_to_the_range_zero_namespace() {
        let config = SubstrateRuntimeConfig {
            bootstrap: "broker-a:9092".to_string(),
            tenant: "tenant-a".to_string(),
            cache_dir: None,
            checkpoints: Some(CheckpointRuntimeConfig {
                object_store: CheckpointObjectStoreConfig::InMemory,
                frames_threshold: 1,
                bytes_threshold: 1,
                part_max_bytes: crabka_gres_substrate::DEFAULT_PART_MAX_BYTES,
                retain_newest: 2,
            }),
            kafka_security: None,
            ranges: None,
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
        };
        let wal_selection = single_range_live_wal_selection(&config, None).expect("wal selection");
        let kv = Arc::new(MemKv::default());
        kv.put(b"checkpointed".to_vec(), b"value".to_vec())
            .expect("seed checkpoint data");
        let store: Arc<dyn SubstrateKv> = kv;
        let checkpoint_store: Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore> =
            crabka_gres_substrate::checkpoint::InMemoryCheckpointStore::shared();
        let snapshot_source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
            7,
            8,
            crabka_gres_substrate::WriterGeneration(0),
        ));

        let checkpoint_runtime = build_checkpoint_runtime(
            &config,
            store,
            snapshot_source,
            wal_selection.checkpoint_topic,
            wal_selection.checkpoint_namespace,
            Some(Arc::clone(&checkpoint_store)),
            || Ok(GresCheckpointWalPruner::in_memory()),
        )
        .expect("checkpoint runtime")
        .expect("checkpoint runtime enabled");
        let checkpoint = checkpoint_runtime
            .force_final_checkpoint()
            .await
            .expect("checkpoint");

        assert!(
            checkpoint
                .manifest_key
                .starts_with("gres/tenant-a/r0/ckpt/")
        );
        assert!(
            checkpoint_store
                .list("gres/tenant-a/r0/ckpt/")
                .await
                .expect("range-zero checkpoint objects")
                .iter()
                .any(|object| object.key == checkpoint.manifest_key)
        );
        assert_eq!(
            checkpoint_runtime
                .latest_checkpoint_bytes()
                .await
                .expect("range-zero checkpoint metadata"),
            checkpoint.total_bytes
        );
        checkpoint_runtime
            .handle
            .shutdown()
            .await
            .expect("checkpoint shutdown");
    }

    #[tokio::test]
    async fn substrate_engine_wires_nonzero_safe_checkpoint_horizon() {
        let kv = Arc::new(MemKv::default());
        let store: Arc<dyn SubstrateKv> = kv.clone();
        let log = crabka_gres_substrate::InMemoryWalLog::shared();
        let source = Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
            -1,
            0,
            crabka_gres_substrate::WriterGeneration(0),
        ));
        let _engine = build_replicated_substrate_engine(
            &store,
            log,
            crabka_gres_substrate::WriterGeneration(0),
            0,
            &source,
            None,
        )
        .expect("substrate engine");

        let (snapshot, _pairs) = source.capture(kv.as_ref()).await.expect("capture");

        assert!(snapshot.garbage_horizon_xid >= crabka_pgmvcc::xid::FIRST_NORMAL_XID);
    }

    #[test]
    fn live_multirange_recovery_configs_are_range_specific() {
        let mut args = substrate_args();
        args.substrate_bootstrap = Some("broker-a:9092,broker-b:9092".to_string());
        args.ranges = Some("0,100,200".to_string());
        args.host_ranges = Some("r0,r2".to_string());
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");
        let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).expect("tenant");
        let tenant_config = crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(
            tenant,
            config.ranges.as_deref().expect("ranges"),
        )
        .expect("range config")
        .with_hosted_ranges(config.host_ranges.clone().expect("host ranges"))
        .expect("hosted ranges");

        let recovery_configs = live_multirange_recovery_configs(&config, &tenant_config);
        let topics = recovery_configs
            .iter()
            .map(crabka_gres_substrate::LiveRecoveryConfig::wal_topic)
            .collect::<Vec<_>>();
        let transactional_ids = recovery_configs
            .iter()
            .map(crabka_gres_substrate::LiveRecoveryConfig::transactional_id)
            .collect::<Vec<_>>();

        assert_eq!(topics, ["__gres_wal.tenant-a.r0", "__gres_wal.tenant-a.r2"]);
        assert_eq!(
            transactional_ids,
            ["__gres.tenant-a.r0", "__gres.tenant-a.r2"]
        );
        assert!(topics.iter().all(|topic| topic != "__gres_wal.tenant-a"));
    }

    #[test]
    fn live_single_range_wal_selection_matches_recovery_writer_and_checkpoint_topics() {
        let config = SubstrateRuntimeConfig {
            bootstrap: "broker-a:9092,broker-b:9092".to_string(),
            tenant: "tenant-a".to_string(),
            cache_dir: None,
            checkpoints: None,
            kafka_security: None,
            ranges: None,
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
        };

        let wal_selection = single_range_live_wal_selection(&config, None).expect("wal selection");

        assert_eq!(
            wal_selection.recovery_config.wal_topic(),
            "__gres_wal.tenant-a.r0"
        );
        assert_eq!(
            wal_selection.writer_topic,
            wal_selection.recovery_config.wal_topic()
        );
        assert_eq!(
            wal_selection.checkpoint_topic,
            wal_selection.recovery_config.wal_topic()
        );
        assert_eq!(
            wal_selection.checkpoint_namespace,
            wal_selection.recovery_config.checkpoint_namespace()
        );
        assert_eq!(wal_selection.checkpoint_namespace, "tenant-a/r0");
        assert_eq!(
            wal_selection.recovery_config.transactional_id(),
            "__gres.tenant-a.r0"
        );
        assert_ne!(
            wal_selection.writer_topic,
            crabka_gres_substrate::wal_topic("tenant-a")
        );

        let mut resumed = tenant_record();
        resumed.wal_generation = 3;
        let resumed_selection =
            single_range_live_wal_selection(&config, Some(&resumed)).expect("resumed selection");
        assert_eq!(
            resumed_selection.recovery_config.wal_topic(),
            "__gres_wal.tenant-a.r0.g0000000003"
        );
        assert_eq!(
            resumed_selection.writer_topic,
            resumed_selection.recovery_config.wal_topic()
        );
    }

    #[test]
    fn substrate_config_parses_host_ranges_and_rejects_empty_list() {
        let mut args = substrate_args();
        args.ranges = Some("0,100,200".to_string());
        args.host_ranges = Some("r2,r0".to_string());

        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");

        assert_eq!(
            config.host_ranges,
            Some(vec![
                crabka_gres_ranges::RangeId::new(0),
                crabka_gres_ranges::RangeId::new(2),
            ])
        );

        args.host_ranges = Some(",".to_string());
        let error = SubstrateRuntimeConfig::from_args(&args).expect_err("empty host ranges");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn substrate_runtime_selects_live_adapter_and_reports_broker_errors() {
        let config = SubstrateRuntimeConfig {
            bootstrap: "127.0.0.1:1".to_string(),
            tenant: "tenant-a".to_string(),
            cache_dir: None,
            checkpoints: None,
            kafka_security: None,
            ranges: None,
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
        };

        let Err(error) = open_substrate_engine(&config).await else {
            panic!("live adapter gap should reject runtime construction");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("substrate recovery"));
    }

    #[test]
    fn cli_parse_rejects_removed_node_mode() {
        let error = Cli::try_parse_from(["crabka-gres", "node", "--id", "1"])
            .expect_err("node mode is not part of the G-1 binary contract");

        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    async fn assert_runtime_session_v2(mut session: RuntimeSession) {
        session
            .simple_query("CREATE TABLE t150 (id int4); INSERT INTO t150 VALUES (1), (2)")
            .await
            .expect("seed");
        session
            .parse("statement", "SELECT id FROM t150 ORDER BY id", &[])
            .await
            .expect("parse");
        session
            .bind("portal", "statement", &[], &[])
            .await
            .expect("bind");
        let ExecuteOutcome::Rows { rows, completion } =
            session.execute("portal", 1).await.expect("page")
        else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert!(completion.is_none());
        let ExecuteOutcome::Rows { rows, completion } =
            session.execute("portal", 0).await.expect("resume")
        else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(completion.as_deref(), Some("SELECT 2"));
        session
            .close(CloseTarget::Portal("portal"))
            .await
            .expect("close");
        session
            .bind("sync_portal", "statement", &[], &[])
            .await
            .expect("bind portal for sync");
        session.sync().await.expect("sync");
        assert_eq!(
            session
                .execute("sync_portal", 0)
                .await
                .expect_err("sync clears portal")
                .code,
            "34000"
        );
        session
            .describe_statement("statement")
            .await
            .expect("prepared survives sync");
    }

    #[tokio::test]
    async fn runtime_session_forwards_v2_for_single_and_multi() {
        let single = RuntimeEngine::Single(Box::new(SqlEngine::new())).connect();
        assert_runtime_session_v2(single).await;

        let config = crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(
            crabka_gres_ranges::TenantName::parse("runtime_v2").expect("tenant"),
            "0,100,200",
        )
        .expect("config");
        let (multi, _handles) = crabka_gres_ranges::MultiRangeTenant::start(config).expect("multi");
        assert_runtime_session_v2(RuntimeEngine::Multi(Box::new(multi)).connect()).await;
    }

    #[tokio::test]
    async fn dynamic_range_service_fails_closed_during_topology_publication() {
        let dynamic = DynamicLiveRangeService::new(crabka_gres_ranges::HostedRangeService::new(
            BTreeMap::new(),
        ));
        dynamic.begin_publication();
        dynamic.replace(crabka_gres_ranges::HostedRangeService::new(BTreeMap::from(
            [(crabka_gres_ranges::RangeId::COORDINATOR, SqlEngine::new())],
        )));
        let request = crabka_gres_ranges::RangeRequest::Sql {
            range_id: crabka_gres_ranges::RangeId::COORDINATOR,
            sql: "SELECT 1".into(),
        };
        assert!(matches!(
            crabka_gres_ranges::RangeService::handle(&dynamic, request.clone()).await,
            crabka_gres_ranges::RangeResponse::Error {
                error: crabka_gres_ranges::WireErrorKind::StaleEndpoint,
                ..
            }
        ));
        dynamic.finish_publication();
        let after = crabka_gres_ranges::RangeService::handle(&dynamic, request).await;
        assert!(!matches!(
            after,
            crabka_gres_ranges::RangeResponse::Error {
                error: crabka_gres_ranges::WireErrorKind::StaleEndpoint,
                ..
            }
        ));
    }
}
