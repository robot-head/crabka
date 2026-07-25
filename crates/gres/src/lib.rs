use std::{
    collections::{BTreeMap, HashMap},
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_gres_control::{
    CheckpointPartBytes, DEFAULT_CHECKPOINT_BYTES, DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS,
    DEFAULT_CHECKPOINT_FRAMES, DEFAULT_CHECKPOINT_POLL_INTERVAL_MS,
    DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS, DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
    FinalCheckpoint, PositiveI32, PositiveMillis, PositiveUsize, RegistryPolicy,
    RegistryReplicationFactor, TenantName, TenantRecord, decode_tenant_config_record,
    tenant_config_topic,
};
use crabka_pgexec::SqlEngine;
use crabka_pgkv::{FjallKv, Kv, KvScan, MemKv, RestoreKv, SnapshotKv};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, CopyInResponse, Engine, ExecuteOutcome, Notification,
        PortalDescription, PreparedDescription, QueryResult, Session, TxStatus,
    },
    session::{AuthMode, SessionConfig},
};
use crabka_security::{
    ClientAuthMode, ListenerProtocol, SaslMechanism, TlsConfig, scram::PgScramVerifier,
};
use rand::RngExt as _;
use refined_type::rule::GreaterI32;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

mod live_range_control;
mod range0_follower;
mod split_activation;
use split_activation::{PendingLiveTopology, PreparedLiveTopology, StagedLiveRangeSuccessor};

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

#[cfg(test)]
impl Cli {
    fn try_parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let mut command = <Self as clap::CommandFactory>::command();
        for argument in [
            "wal_recovery_fetch_max_wait_ms",
            "wal_recovery_fetch_partition_max_bytes",
            "wal_recovery_fetch_response_max_bytes",
            "wal_recovery_empty_fetch_retries",
            "wal_recovery_dns_timeout_ms",
            "wal_recovery_connect_timeout_ms",
            "wal_recovery_request_timeout_ms",
            "wal_topic_replication_factor",
            "wal_topic_ensure_timeout_ms",
            "wal_admin_connect_timeout_ms",
            "wal_admin_request_timeout_ms",
            "wal_producer_flush_timeout_ms",
            "wal_producer_request_timeout_ms",
            "wal_producer_retries",
            "wal_producer_retry_backoff_ms",
            "wal_producer_routing_retry_budget_ms",
            "wal_producer_init_retry_timeout_ms",
            "wal_producer_init_max_backoff_ms",
            "wal_producer_transaction_timeout_ms",
            "wal_producer_compression",
            "wal_producer_linger_ms",
            "wal_producer_batch_bytes",
        ] {
            command = command.mut_arg(argument, |arg| arg.env(None::<&str>));
        }
        let matches = command.try_get_matches_from(itr)?;
        <Self as clap::FromArgMatches>::from_arg_matches(&matches)
    }
}

/// Arguments for the default serve mode (no subcommand).
#[derive(clap::Args, Debug, Clone)]
pub struct ServeArgs {
    /// Shared Gres registry policy.
    #[command(flatten)]
    pub registry: RegistryOptions,

    /// Local-engine vacuum pacing policy.
    #[command(flatten)]
    pub local_vacuum: LocalVacuumOptions,

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

    /// Periodic range-0 follower refresh cadence in multi-range substrate mode.
    #[arg(
        long = "range0-follower-poll-interval-ms",
        env = "CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS",
        requires = "ranges"
    )]
    pub range0_follower_poll_interval_ms: Option<PositiveMillis>,

    /// Broker long-poll wait for committed-WAL recovery fetches.
    #[arg(
        long = "wal-recovery-fetch-max-wait-ms",
        env = "CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_fetch_max_wait_ms: Option<PositiveI32>,

    /// Per-partition byte limit for committed-WAL recovery fetches.
    #[arg(
        long = "wal-recovery-fetch-partition-max-bytes",
        env = "CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_fetch_partition_max_bytes: Option<PositiveI32>,

    /// Whole-response byte limit for committed-WAL recovery fetches.
    #[arg(
        long = "wal-recovery-fetch-response-max-bytes",
        env = "CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_fetch_response_max_bytes: Option<PositiveI32>,

    /// Consecutive empty-fetch retries after the initial recovery fetch.
    #[arg(
        long = "wal-recovery-empty-fetch-retries",
        env = "CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_empty_fetch_retries: Option<PositiveUsize>,

    /// Timeout for resolving raw WAL recovery broker hostnames.
    #[arg(
        long = "wal-recovery-dns-timeout-ms",
        env = "CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_dns_timeout_ms: Option<PositiveMillis>,

    /// Timeout for establishing raw WAL recovery broker connections.
    #[arg(
        long = "wal-recovery-connect-timeout-ms",
        env = "CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_connect_timeout_ms: Option<PositiveMillis>,

    /// Timeout for raw WAL recovery broker requests.
    #[arg(
        long = "wal-recovery-request-timeout-ms",
        env = "CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_recovery_request_timeout_ms: Option<PositiveMillis>,

    /// Replication factor requested when creating range WAL topics.
    #[arg(
        long = "wal-topic-replication-factor",
        env = "CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR",
        requires = "substrate_bootstrap"
    )]
    pub wal_topic_replication_factor: Option<PositiveI32>,

    /// Timeout for ensuring range WAL topics.
    #[arg(
        long = "wal-topic-ensure-timeout-ms",
        env = "CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_topic_ensure_timeout_ms: Option<PositiveI32>,

    /// Timeout for establishing WAL admin broker connections.
    #[arg(
        long = "wal-admin-connect-timeout-ms",
        env = "CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_admin_connect_timeout_ms: Option<PositiveMillis>,

    /// Timeout for WAL admin broker requests.
    #[arg(
        long = "wal-admin-request-timeout-ms",
        env = "CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_admin_request_timeout_ms: Option<PositiveMillis>,

    /// Deadline for flushing all buffered and in-flight WAL records.
    #[arg(
        long = "wal-producer-flush-timeout-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_flush_timeout_ms: Option<PositiveMillis>,

    /// Timeout for WAL producer broker requests.
    #[arg(
        long = "wal-producer-request-timeout-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_request_timeout_ms: Option<PositiveMillis>,

    /// WAL producer retries after a batch's initial send.
    #[arg(
        long = "wal-producer-retries",
        env = "CRABKA_GRES_WAL_PRODUCER_RETRIES",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_retries: Option<NonNegativeI32>,

    /// WAL producer retry and producer-ID initial backoff.
    #[arg(
        long = "wal-producer-retry-backoff-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_retry_backoff_ms: Option<PositiveMillis>,

    /// Wall-clock routing retry budget for each WAL producer batch.
    #[arg(
        long = "wal-producer-routing-retry-budget-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_routing_retry_budget_ms: Option<PositiveMillis>,

    /// Producer-ID initialization retry timeout.
    #[arg(
        long = "wal-producer-init-retry-timeout-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_init_retry_timeout_ms: Option<PositiveMillis>,

    /// Producer-ID initialization retry backoff cap.
    #[arg(
        long = "wal-producer-init-max-backoff-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_init_max_backoff_ms: Option<PositiveMillis>,

    /// Transaction timeout sent by the WAL producer.
    #[arg(
        long = "wal-producer-transaction-timeout-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_transaction_timeout_ms: Option<PositiveMillis>,

    /// Compression used for WAL producer record batches.
    #[arg(
        long = "wal-producer-compression",
        env = "CRABKA_GRES_WAL_PRODUCER_COMPRESSION",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_compression: Option<crabka_client_producer::Compression>,

    /// WAL producer linger in whole milliseconds.
    #[arg(
        long = "wal-producer-linger-ms",
        env = "CRABKA_GRES_WAL_PRODUCER_LINGER_MS",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_linger_ms: Option<u64>,

    /// Maximum uncompressed WAL producer batch size in bytes.
    #[arg(
        long = "wal-producer-batch-bytes",
        env = "CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES",
        requires = "substrate_bootstrap"
    )]
    pub wal_producer_batch_bytes: Option<usize>,

    /// Substrate mode: comma-separated hosted range ids, for example r0,r2.
    #[arg(long = "host-ranges", requires = "ranges")]
    pub host_ranges: Option<String>,

    /// Timestamp-ordering source the multi-range tenant installs. Every
    /// process serving one tenant must select the same source.
    #[arg(long = "timestamp-source", value_enum, default_value = "logical-tso")]
    pub timestamp_source: TimestampSourceKind,

    /// Maximum tolerated clock offset in milliseconds for --timestamp-source
    /// hlc; sizes the read uncertainty window.
    #[arg(long = "hlc-max-offset-ms", default_value_t = 250)]
    pub hlc_max_offset_ms: u64,

    /// Fault-injection knob for load and chaos testing only, not for
    /// production use: skew this process's HLC wall-clock reads by a signed
    /// millisecond offset. Only meaningful with --timestamp-source hlc.
    #[arg(
        long = "hlc-wall-offset-ms",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    pub hlc_wall_offset_ms: i64,

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
    /// Client certificate subject DN authorized to execute destructive range control RPCs.
    #[arg(long = "operator-control-principal")]
    pub operator_control_principals: Vec<String>,

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
    #[arg(long = "checkpoint-frames", env = "CRABKA_GRES_CHECKPOINT_FRAMES")]
    pub checkpoint_frames: Option<NonZeroU64>,

    /// Checkpoint after at least this many WAL bytes since the previous manifest.
    #[arg(long = "checkpoint-bytes", env = "CRABKA_GRES_CHECKPOINT_BYTES")]
    pub checkpoint_bytes: Option<NonZeroU64>,

    /// Target maximum bytes per checkpoint part object.
    #[arg(
        long = "checkpoint-part-bytes",
        env = "CRABKA_GRES_CHECKPOINT_PART_BYTES"
    )]
    pub checkpoint_part_bytes: Option<CheckpointPartBytes>,

    /// Number of newest checkpoint directories to retain after pruning.
    #[arg(long = "checkpoint-retain", env = "CRABKA_GRES_CHECKPOINT_RETAIN")]
    pub checkpoint_retain: Option<PositiveUsize>,

    /// Kafka `DeleteRecords` timeout used after a durable checkpoint.
    #[arg(
        long = "checkpoint-delete-records-timeout-ms",
        env = "CRABKA_GRES_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS"
    )]
    pub checkpoint_delete_records_timeout_ms: Option<PositiveI32>,

    /// Background checkpoint threshold polling interval.
    #[arg(
        long = "checkpoint-poll-interval-ms",
        env = "CRABKA_GRES_CHECKPOINT_POLL_INTERVAL_MS"
    )]
    pub checkpoint_poll_interval_ms: Option<PositiveMillis>,

    /// Idle-tenant suspension polling interval.
    #[arg(
        long = "idle-suspend-poll-interval-ms",
        env = "CRABKA_GRES_IDLE_SUSPEND_POLL_INTERVAL_MS"
    )]
    pub idle_suspend_poll_interval_ms: Option<PositiveMillis>,
}

/// Optional local-engine vacuum pacing overrides.
#[derive(clap::Args, Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct LocalVacuumOptions {
    #[arg(
        long = "local-vacuum-idle-interval-ms",
        env = "CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS"
    )]
    idle_interval_ms: Option<PositiveMillis>,
    #[arg(
        long = "local-vacuum-backoff-floor-ms",
        env = "CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS"
    )]
    backoff_floor_ms: Option<PositiveMillis>,
    #[arg(
        long = "local-vacuum-hot-debt",
        env = "CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT"
    )]
    hot_debt: Option<NonZeroU64>,
    #[arg(
        long = "local-vacuum-key-budget",
        env = "CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET"
    )]
    key_budget: Option<PositiveUsize>,
    #[arg(
        long = "local-vacuum-max-key-budget",
        env = "CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET"
    )]
    max_key_budget: Option<PositiveUsize>,
    #[arg(
        long = "local-vacuum-step-fast-ms",
        env = "CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS"
    )]
    step_fast_ms: Option<PositiveMillis>,
    #[arg(
        long = "local-vacuum-step-slow-ms",
        env = "CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS"
    )]
    step_slow_ms: Option<PositiveMillis>,
    #[arg(
        long = "local-vacuum-idle-after-ms",
        env = "CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS"
    )]
    idle_after_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalVacuumPolicy {
    idle_interval: Duration,
    backoff_floor: Duration,
    hot_debt: u64,
    key_budget: usize,
    max_key_budget: usize,
    step_fast: Duration,
    step_slow: Duration,
    idle_after: Duration,
}

const DEFAULT_LOCAL_VACUUM_IDLE_INTERVAL_MS: u64 = 2_000;
const DEFAULT_LOCAL_VACUUM_BACKOFF_FLOOR_MS: u64 = 25;
const DEFAULT_LOCAL_VACUUM_STEP_FAST_MS: u64 = 3;
const DEFAULT_LOCAL_VACUUM_STEP_SLOW_MS: u64 = 12;
const DEFAULT_LOCAL_VACUUM_IDLE_AFTER_MS: u64 = 1_000;

fn local_vacuum_policy(args: &ServeArgs) -> std::io::Result<Option<LocalVacuumPolicy>> {
    let options = args.local_vacuum;
    let requested = options != LocalVacuumOptions::default();
    if args.substrate_bootstrap.is_some() {
        return if requested {
            invalid_input("local vacuum options are incompatible with --substrate-bootstrap")
        } else {
            Ok(None)
        };
    }

    let key_budget = options.key_budget.map_or(
        crabka_pgexec::VACUUM_STEP_KEY_BUDGET,
        PositiveUsize::into_value,
    );
    let max_key_budget = match options.max_key_budget {
        Some(value) => value.into_value(),
        None => key_budget.checked_mul(4).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "local vacuum default maximum key budget overflows usize",
            )
        })?,
    };
    let hot_debt = match options.hot_debt {
        Some(value) => value.get(),
        None => u64::try_from(key_budget).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "local vacuum key budget does not fit u64 debt accounting",
            )
        })?,
    };
    let idle_interval = Duration::from_millis(options.idle_interval_ms.map_or(
        DEFAULT_LOCAL_VACUUM_IDLE_INTERVAL_MS,
        PositiveMillis::into_value,
    ));
    let backoff_floor = Duration::from_millis(options.backoff_floor_ms.map_or(
        DEFAULT_LOCAL_VACUUM_BACKOFF_FLOOR_MS,
        PositiveMillis::into_value,
    ));
    let step_fast = Duration::from_millis(options.step_fast_ms.map_or(
        DEFAULT_LOCAL_VACUUM_STEP_FAST_MS,
        PositiveMillis::into_value,
    ));
    let step_slow = Duration::from_millis(options.step_slow_ms.map_or(
        DEFAULT_LOCAL_VACUUM_STEP_SLOW_MS,
        PositiveMillis::into_value,
    ));
    let idle_after = Duration::from_millis(options.idle_after_ms.map_or(
        DEFAULT_LOCAL_VACUUM_IDLE_AFTER_MS,
        PositiveMillis::into_value,
    ));
    if backoff_floor > idle_interval {
        return invalid_input("local vacuum backoff floor exceeds idle interval");
    }
    if key_budget > max_key_budget {
        return invalid_input("local vacuum key budget exceeds maximum key budget");
    }
    if step_fast >= step_slow {
        return invalid_input("local vacuum fast threshold must be below slow threshold");
    }
    Ok(Some(LocalVacuumPolicy {
        idle_interval,
        backoff_floor,
        hot_debt,
        key_budget,
        max_key_budget,
        step_fast,
        step_slow,
        idle_after,
    }))
}

fn validate_range0_follower_poll_interval(args: &ServeArgs) -> std::io::Result<()> {
    if args.range0_follower_poll_interval_ms.is_some() && args.ranges.is_none() {
        return invalid_input("--range0-follower-poll-interval-ms requires --ranges");
    }
    Ok(())
}

fn validate_wal_recovery_read_policy(args: &ServeArgs) -> std::io::Result<()> {
    if (args.wal_recovery_fetch_max_wait_ms.is_some()
        || args.wal_recovery_fetch_partition_max_bytes.is_some()
        || args.wal_recovery_fetch_response_max_bytes.is_some()
        || args.wal_recovery_empty_fetch_retries.is_some()
        || args.wal_recovery_dns_timeout_ms.is_some()
        || args.wal_recovery_connect_timeout_ms.is_some()
        || args.wal_recovery_request_timeout_ms.is_some()
        || args.wal_topic_replication_factor.is_some()
        || args.wal_topic_ensure_timeout_ms.is_some()
        || args.wal_admin_connect_timeout_ms.is_some()
        || args.wal_admin_request_timeout_ms.is_some()
        || args.wal_producer_flush_timeout_ms.is_some()
        || args.wal_producer_request_timeout_ms.is_some()
        || args.wal_producer_retries.is_some()
        || args.wal_producer_retry_backoff_ms.is_some()
        || args.wal_producer_routing_retry_budget_ms.is_some()
        || args.wal_producer_init_retry_timeout_ms.is_some()
        || args.wal_producer_init_max_backoff_ms.is_some()
        || args.wal_producer_transaction_timeout_ms.is_some()
        || args.wal_producer_compression.is_some()
        || args.wal_producer_linger_ms.is_some()
        || args.wal_producer_batch_bytes.is_some())
        && args.substrate_bootstrap.is_none()
    {
        return invalid_input("WAL recovery options require --substrate-bootstrap");
    }
    effective_wal_admin_policy(args)?;
    effective_wal_producer_flush_timeout(args)?;
    effective_wal_producer_retry_policy(args)?;
    effective_wal_producer_throughput_policy(args)?;
    Ok(())
}

fn effective_wal_admin_policy(
    args: &ServeArgs,
) -> std::io::Result<crabka_gres_substrate::WalAdminPolicy> {
    crabka_gres_substrate::WalAdminPolicy::new(
        args.wal_topic_replication_factor.map_or(
            crabka_gres_substrate::DEFAULT_WAL_TOPIC_REPLICATION_FACTOR,
            PositiveI32::into_value,
        ),
        args.wal_topic_ensure_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT_MS,
            PositiveI32::into_value,
        ),
        args.wal_admin_connect_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT_MS,
            PositiveMillis::into_value,
        ),
        args.wal_admin_request_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT_MS,
            PositiveMillis::into_value,
        ),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn effective_wal_producer_flush_timeout(
    args: &ServeArgs,
) -> std::io::Result<crabka_client_producer::ProducerFlushTimeout> {
    let default_ms = u64::try_from(
        crabka_client_producer::ProducerFlushTimeout::default()
            .duration()
            .as_millis(),
    )
    .expect("default producer flush timeout fits u64 milliseconds");
    crabka_client_producer::ProducerFlushTimeout::new(Duration::from_millis(
        args.wal_producer_flush_timeout_ms
            .map_or(default_ms, PositiveMillis::into_value),
    ))
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn effective_wal_producer_retry_policy(
    args: &ServeArgs,
) -> std::io::Result<crabka_client_producer::ProducerRetryPolicy> {
    let defaults = crabka_client_producer::ProducerRetryPolicy::default();
    let millis = |duration: Duration| {
        u64::try_from(duration.as_millis()).expect("producer policy duration fits u64 milliseconds")
    };
    crabka_client_producer::ProducerRetryPolicy::new(
        Duration::from_millis(args.wal_producer_request_timeout_ms.map_or_else(
            || millis(defaults.request_timeout()),
            PositiveMillis::into_value,
        )),
        args.wal_producer_retries
            .map_or(defaults.retries(), NonNegativeI32::into_value),
        Duration::from_millis(args.wal_producer_retry_backoff_ms.map_or_else(
            || millis(defaults.retry_backoff()),
            PositiveMillis::into_value,
        )),
        Duration::from_millis(args.wal_producer_routing_retry_budget_ms.map_or_else(
            || millis(defaults.routing_retry_budget()),
            PositiveMillis::into_value,
        )),
        Duration::from_millis(args.wal_producer_init_retry_timeout_ms.map_or_else(
            || millis(defaults.init_retry_timeout()),
            PositiveMillis::into_value,
        )),
        Duration::from_millis(args.wal_producer_init_max_backoff_ms.map_or_else(
            || millis(defaults.init_max_backoff()),
            PositiveMillis::into_value,
        )),
        Duration::from_millis(args.wal_producer_transaction_timeout_ms.map_or_else(
            || millis(defaults.transaction_timeout()),
            PositiveMillis::into_value,
        )),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn effective_wal_producer_throughput_policy(
    args: &ServeArgs,
) -> std::io::Result<crabka_client_producer::ProducerThroughputPolicy> {
    let defaults = crabka_client_producer::ProducerThroughputPolicy::default();
    let default_linger_ms = u64::try_from(defaults.linger().as_millis())
        .expect("default producer linger fits u64 milliseconds");
    crabka_client_producer::ProducerThroughputPolicy::new(
        args.wal_producer_compression
            .unwrap_or(defaults.compression()),
        Duration::from_millis(args.wal_producer_linger_ms.unwrap_or(default_linger_ms)),
        args.wal_producer_batch_bytes
            .unwrap_or(defaults.batch_bytes()),
        defaults.max_in_flight(),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

/// A nonnegative producer retry count representable on the protocol wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonNegativeI32(i32);

impl NonNegativeI32 {
    fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<-1>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    const fn into_value(self) -> i32 {
        self.0
    }
}

impl std::str::FromStr for NonNegativeI32 {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Validated Gres registry options shared by compute registry clients.
#[derive(clap::Args, Debug, Clone)]
pub struct RegistryOptions {
    #[arg(
        long = "registry-replication-factor",
        env = "CRABKA_GRES_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    replication_factor: RegistryReplicationFactor,
    #[arg(
        long = "registry-topic-create-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS",
        default_value = "15000"
    )]
    topic_create_timeout_ms: PositiveI32,
    #[arg(
        long = "registry-reader-retry-backoff-ms",
        env = "CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS",
        default_value = "250"
    )]
    reader_retry_backoff_ms: PositiveMillis,
    #[arg(
        long = "registry-fetch-max-wait-ms",
        env = "CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS",
        default_value = "500"
    )]
    fetch_max_wait_ms: PositiveI32,
    #[arg(
        long = "registry-fetch-partition-max-bytes",
        env = "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES",
        default_value = "1048576"
    )]
    fetch_partition_max_bytes: PositiveI32,
}

impl RegistryOptions {
    fn policy(&self) -> RegistryPolicy {
        RegistryPolicy::new(
            self.replication_factor.into_value(),
            self.topic_create_timeout_ms.into_value(),
            self.reader_retry_backoff_ms.into_value(),
            self.fetch_max_wait_ms.into_value(),
            self.fetch_partition_max_bytes.into_value(),
        )
        .expect("validated registry options")
    }
}

/// Timestamp-ordering source selected by `--timestamp-source`.
///
/// The kind mirrors [`crabka_gres_ranges::TimestampSourceMode`] without the
/// mode's parameters, which arrive through their own flags; [`Self::to_mode`]
/// reattaches them.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampSourceKind {
    /// Centralized range-0 logical oracle — the solo default.
    #[default]
    LogicalTso,
    /// Node-local Hybrid Logical Clock minting stamps without RPC.
    Hlc,
}

impl TimestampSourceKind {
    /// Attach the HLC uncertainty bound and produce the tenant-level mode.
    #[must_use]
    pub fn to_mode(self, hlc_max_offset_ms: u64) -> crabka_gres_ranges::TimestampSourceMode {
        match self {
            Self::LogicalTso => crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            Self::Hlc => crabka_gres_ranges::TimestampSourceMode::Hlc {
                max_offset_ms: hlc_max_offset_ms,
            },
        }
    }
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
    /// Periodic refresh cadence for a remote range-0 follower.
    pub range0_follower_poll_interval: Duration,
    /// Committed-WAL recovery read limits.
    pub recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy,
    /// WAL topic creation and admin connection settings.
    pub wal_admin_policy: crabka_gres_substrate::WalAdminPolicy,
    /// Deadline for flushing all buffered and in-flight WAL records.
    pub producer_flush_timeout: crabka_client_producer::ProducerFlushTimeout,
    /// WAL producer retry and transaction timing.
    pub producer_retry_policy: crabka_client_producer::ProducerRetryPolicy,
    /// WAL producer batching and compression settings.
    pub producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy,
    /// Optional range-compute placement for distributed mode. Range 0 is always hosted.
    pub host_ranges: Option<Vec<crabka_gres_ranges::RangeId>>,
    /// mTLS client configuration required for remote range routing.
    pub range_rpc: Option<RangeRpcRuntimeConfig>,
    /// Authenticated endpoint advertised for local range-control operations.
    pub advertised_endpoint: Option<String>,
    /// Timestamp-ordering source a multi-range tenant installs on this node.
    pub timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode,
    /// Testing-only signed HLC wall-clock skew in milliseconds for this node.
    pub hlc_wall_offset_ms: i64,
    /// Shared Gres registry policy.
    pub registry_policy: RegistryPolicy,
}

/// Validated TLS-only range RPC configuration.
#[derive(Debug, Clone)]
pub struct RangeRpcRuntimeConfig {
    tls: TlsConfig,
    server_name: String,
    range_rpc_principals: std::collections::BTreeSet<String>,
    operator_control_principals: std::collections::BTreeSet<String>,
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
    /// Kafka `DeleteRecords` timeout after a durable manifest.
    pub delete_records_timeout_ms: i32,
    /// Background checkpoint threshold polling interval.
    pub poll_interval: Duration,
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn from_args(args: &ServeArgs) -> std::io::Result<Option<Self>> {
        use std::io::{Error, ErrorKind};

        validate_range0_follower_poll_interval(args)?;
        validate_wal_recovery_read_policy(args)?;

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
            range0_follower_poll_interval: Duration::from_millis(
                args.range0_follower_poll_interval_ms.map_or(
                    DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
                    PositiveMillis::into_value,
                ),
            ),
            recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy::new(
                args.wal_recovery_fetch_max_wait_ms.map_or(
                    crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS,
                    PositiveI32::into_value,
                ),
                args.wal_recovery_fetch_partition_max_bytes.map_or(
                    crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES,
                    PositiveI32::into_value,
                ),
                args.wal_recovery_fetch_response_max_bytes.map_or(
                    crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES,
                    PositiveI32::into_value,
                ),
                args.wal_recovery_empty_fetch_retries.map_or(
                    crabka_gres_substrate::DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES,
                    PositiveUsize::into_value,
                ),
            )
            .and_then(|policy| {
                policy.with_dns_timeout(args.wal_recovery_dns_timeout_ms.map_or(
                    crabka_gres_substrate::DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS,
                    PositiveMillis::into_value,
                ))
            })
            .and_then(|policy| {
                policy.with_timeouts(
                    args.wal_recovery_connect_timeout_ms.map_or(
                        crabka_gres_substrate::DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS,
                        PositiveMillis::into_value,
                    ),
                    args.wal_recovery_request_timeout_ms.map_or(
                        crabka_gres_substrate::DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS,
                        PositiveMillis::into_value,
                    ),
                )
            })
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?,
            wal_admin_policy: effective_wal_admin_policy(args)?,
            producer_flush_timeout: effective_wal_producer_flush_timeout(args)?,
            producer_retry_policy: effective_wal_producer_retry_policy(args)?,
            producer_throughput_policy: effective_wal_producer_throughput_policy(args)?,
            host_ranges: parse_host_ranges(args.host_ranges.as_deref())?,
            range_rpc: RangeRpcRuntimeConfig::from_args(args)?,
            advertised_endpoint: args.range_listen.clone(),
            timestamp_source_mode: args.timestamp_source.to_mode(args.hlc_max_offset_ms),
            hlc_wall_offset_ms: args.hlc_wall_offset_ms,
            registry_policy: args.registry.policy(),
        }))
    }

    fn is_in_memory_bootstrap(&self) -> bool {
        is_in_memory_bootstrap(&self.bootstrap)
    }

    fn live_recovery_config(
        &self,
        tenant: crabka_gres_ranges::TenantName,
        range: crabka_gres_ranges::RangeId,
    ) -> crabka_gres_substrate::LiveRecoveryConfig {
        crabka_gres_substrate::LiveRecoveryConfig::new(
            self.bootstrap.clone(),
            tenant,
            range,
            self.kafka_security.clone(),
        )
        .with_read_policy(self.recovery_read_policy)
        .with_wal_admin_policy(self.wal_admin_policy)
        .with_producer_flush_timeout(self.producer_flush_timeout)
        .with_producer_retry_policy(self.producer_retry_policy)
        .with_producer_throughput_policy(self.producer_throughput_policy)
    }
}

/// Whether `bootstrap` names the in-process substrate seam instead of a
/// dialable broker address list.
fn is_in_memory_bootstrap(bootstrap: &str) -> bool {
    matches!(bootstrap, "memory://" | "in-memory://")
}

impl RangeRpcRuntimeConfig {
    fn from_args(args: &ServeArgs) -> std::io::Result<Option<Self>> {
        let configured = args.range_tls_cert.is_some()
            || args.range_tls_key.is_some()
            || args.range_tls_ca.is_some()
            || args.range_tls_server_name.is_some()
            || !args.range_allowed_principals.is_empty()
            || !args.operator_control_principals.is_empty();
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
        let range_rpc_principals = args
            .range_allowed_principals
            .iter()
            .map(|principal| principal.trim())
            .filter(|principal| !principal.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let operator_control_principals = args
            .operator_control_principals
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
            range_rpc_principals,
            operator_control_principals,
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
            range_rpc_principals: self.range_rpc_principals.clone(),
            operator_control_principals: self.operator_control_principals.clone(),
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
            CheckpointPartBytes::into_value,
        );
        Ok(Some(Self {
            object_store,
            frames_threshold: args
                .checkpoint_frames
                .map_or(DEFAULT_CHECKPOINT_FRAMES, NonZeroU64::get),
            bytes_threshold: args
                .checkpoint_bytes
                .map_or(DEFAULT_CHECKPOINT_BYTES, NonZeroU64::get),
            part_max_bytes,
            retain_newest: args.checkpoint_retain.map_or(
                crabka_gres_substrate::DEFAULT_CHECKPOINT_RETAIN,
                PositiveUsize::into_value,
            ),
            delete_records_timeout_ms: args.checkpoint_delete_records_timeout_ms.map_or(
                DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS,
                PositiveI32::into_value,
            ),
            poll_interval: Duration::from_millis(args.checkpoint_poll_interval_ms.map_or(
                DEFAULT_CHECKPOINT_POLL_INTERVAL_MS,
                PositiveMillis::into_value,
            )),
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
            crabka_gres_ranges::HostedRangeService::new(engine.hosted_range_engines())
                .with_ddl_gate(engine.schema_gate());
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

    fn into_parts(
        self,
    ) -> (
        RuntimeEngine,
        Option<StartedCheckpointRuntime>,
        Option<Arc<LiveMultiRangeTransfer>>,
    ) {
        (self.engine, self.checkpoint_runtime, self.staged_transfer)
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

    /// Inspect one currently hosted range's raw durable fold in system tests.
    #[doc(hidden)]
    pub fn hosted_range_kv_scan(
        &self,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<crabka_pgkv::KvScan, String> {
        let transfer = self
            .staged_transfer
            .as_ref()
            .ok_or_else(|| "live range transfer unavailable".to_owned())?;
        transfer
            .range(range_id)
            .map_err(|error| error.to_string())?
            .store
            .scan_range(&[], &[u8::MAX])
            .map_err(|error| error.to_string())
    }

    /// Physically move one populated ordinary table in a local live multi-range runtime.
    ///
    /// This only publishes to the in-process serving map. It deliberately does not
    /// mutate the control registry or coordinate a distributed operator workflow.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
    Multi(Box<crabka_gres_ranges::tenant::GatewaySession>),
}

impl Engine for RuntimeEngine {
    type Session = RuntimeSession;

    fn connect(&self) -> Self::Session {
        match self {
            Self::Single(engine) => RuntimeSession::Single(Box::new(engine.connect())),
            Self::Multi(engine) => RuntimeSession::Multi(Box::new(engine.connect())),
        }
    }

    fn connect_with_pid(&self, pid: i32) -> Self::Session {
        match self {
            Self::Single(engine) => RuntimeSession::Single(Box::new(engine.connect_with_pid(pid))),
            Self::Multi(engine) => RuntimeSession::Multi(Box::new(engine.connect_with_pid(pid))),
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

    async fn begin_copy_in(
        &mut self,
        sql: &str,
    ) -> Result<Option<CopyInResponse>, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.begin_copy_in(sql).await,
            Self::Multi(session) => session.begin_copy_in(sql).await,
        }
    }

    async fn copy_in(
        &mut self,
        sql: &str,
        data: Vec<bytes::Bytes>,
    ) -> Result<QueryResult, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.copy_in(sql, data).await,
            Self::Multi(session) => session.copy_in(sql, data).await,
        }
    }

    async fn copy_in_portal(
        &mut self,
        portal: &str,
        data: Vec<bytes::Bytes>,
    ) -> Result<QueryResult, crabka_pgwire::error::PgError> {
        match self {
            Self::Single(session) => session.copy_in_portal(portal, data).await,
            Self::Multi(session) => session.copy_in_portal(portal, data).await,
        }
    }

    fn take_notifications(&mut self) -> Option<tokio::sync::mpsc::Receiver<Notification>> {
        match self {
            Self::Single(session) => session.take_notifications(),
            Self::Multi(session) => session.take_notifications(),
        }
    }

    fn mark_statement_failed(&mut self) {
        match self {
            Self::Single(session) => session.mark_statement_failed(),
            Self::Multi(session) => session.mark_statement_failed(),
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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
    delete_records_timeout_ms: i32,
}

enum CheckpointPruneBackend {
    InMemory,
    Kafka { bootstrap_addrs: Vec<String> },
}

impl GresCheckpointWalPruner {
    fn in_memory(delete_records_timeout_ms: i32) -> Self {
        Self {
            bootstrap: CheckpointPruneBackend::InMemory,
            security: None,
            delete_records_timeout_ms,
        }
    }

    fn kafka(
        bootstrap: &str,
        security: Option<ClientSecurity>,
        delete_records_timeout_ms: i32,
    ) -> std::io::Result<Self> {
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
            delete_records_timeout_ms,
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
            .delete_records(ops, self.delete_records_timeout_ms)
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
        || args.checkpoint_delete_records_timeout_ms.is_some()
        || args.checkpoint_poll_interval_ms.is_some()
        || args.idle_suspend_poll_interval_ms.is_some()
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn tls_acceptor(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use std::io::{BufReader, Error, ErrorKind};

    use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let certs = CertificateDer::pem_reader_iter(BufReader::new(std::fs::File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
    let key = PrivateKeyDer::from_pem_reader(BufReader::new(std::fs::File::open(key_path)?))
        .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Run the single-node pgwire service, binding the configured listener address.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn run_serve(args: ServeArgs) -> std::io::Result<()> {
    validate_range0_follower_poll_interval(&args)?;
    validate_wal_recovery_read_policy(&args)?;
    local_vacuum_policy(&args)?;
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = %listener.local_addr()?, "crabka-gres listening");
    Box::pin(serve_listener(listener, args)).await
}

/// Run the single-node pgwire service on an already-bound listener.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn serve_listener(listener: TcpListener, args: ServeArgs) -> std::io::Result<()> {
    Box::pin(serve_listener_with_tenant_config_loader(
        listener,
        args,
        &LiveTenantConfigLoader,
    ))
    .await
}

/// Run the pgwire service with an injected tenant-config loader for tests.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn serve_listener_with_tenant_config_loader(
    listener: TcpListener,
    args: ServeArgs,
    tenant_config_loader: &impl TenantConfigLoader,
) -> std::io::Result<()> {
    validate_range0_follower_poll_interval(&args)?;
    validate_wal_recovery_read_policy(&args)?;
    let local_vacuum_policy = local_vacuum_policy(&args)?;
    let sql_addr = listener.local_addr()?;
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(tls_acceptor(cert, key)?),
        _ => None,
    };

    let mut tenant_record = load_substrate_tenant_record(&args, tenant_config_loader).await?;
    let tenant_security_enabled = tenant_record
        .as_ref()
        .is_some_and(|record| tenant_kafka_security_from_env(record.name.as_str()).is_some());
    let mut lifecycle_registry = None;
    if let (Some(record), Some(bootstrap)) = (
        tenant_record.as_ref(),
        lifecycle_registry_bootstrap(args.substrate_bootstrap.as_deref(), tenant_security_enabled),
    ) {
        let mut registry =
            crabka_gres_control::Registry::connect_with_policy(bootstrap, args.registry.policy())
                .await
                .map_err(|error| {
                    std::io::Error::other(format!("tenant registry connect: {error}"))
                })?;
        registry
            .ensure_topic()
            .await
            .map_err(|error| std::io::Error::other(format!("tenant registry ensure: {error}")))?;
        tenant_record = registry
            .get(record.name.as_str())
            .await
            .map_err(|error| std::io::Error::other(format!("tenant registry read: {error}")))?;
        lifecycle_registry = Some(registry);
    }
    let effective_args = apply_tenant_runtime_defaults(args, tenant_record.as_ref())?;
    let (early_range_service, early_range_server) =
        match bind_early_range_transport(&effective_args).await? {
            Some((service, server)) => (Some(service), Some(server)),
            None => (None, None),
        };
    let mut runtime = Box::pin(open_runtime_with_tenant_record(
        &effective_args,
        tenant_record.as_ref(),
        early_range_service,
    ))
    .await?;
    register_kafka_scanner_with_default_bootstrap(
        &mut runtime.engine,
        kafka_scanner_default_bootstrap(&effective_args),
    );
    let session_config = build_session_config_from_tenant(&effective_args, tenant_record.as_ref())?;

    let range_service = runtime.range_service();
    let (engine, checkpoint_runtime, _range_transfer_keepalive) = runtime.into_parts();
    let activity = Arc::new(crabka_pgwire::server::ActivityTracker::new());
    let shutdown = CancellationToken::new();
    // Periodic dead-version sweep for the single-range LOCAL engine (mem or
    // --data-dir). Substrate/replicated engines refuse local pruning
    // (`supports_local_vacuum` is false there) and rely on checkpoint-time GC.
    // The loop runs on a child token whose drop guard lives on THIS future's
    // stack: whether serving returns or is aborted, the sweep task stops and
    // releases its engine handle (and with it a --data-dir store lock).
    let _vacuum_guard = if let RuntimeEngine::Single(sql_engine) = &engine
        && let Some(policy) =
            local_vacuum_spawn_policy(local_vacuum_policy, sql_engine.supports_local_vacuum())
    {
        let vacuum_token = shutdown.child_token();
        tokio::spawn(run_local_vacuum_loop(
            sql_engine.clone_handle(),
            Arc::clone(&activity),
            vacuum_token.clone(),
            policy,
        ));
        Some(vacuum_token.drop_guard())
    } else {
        None
    };
    let serve = crabka_pgwire::server::serve_tls_with_activity_until(
        listener,
        Arc::new(engine),
        Arc::new(session_config),
        tls,
        Arc::clone(&activity),
        shutdown.clone(),
    );

    let range_server = if let Some(server) = early_range_server {
        if range_service.is_none() {
            // Dropping the guard aborts the warming serve task before the
            // startup error is reported.
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--range-listen requires a multi-range runtime",
            ));
        }
        Some(server.release())
    } else {
        start_range_service(&effective_args, range_service).await?
    };
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
            result = run_suspend_monitor(
                policy,
                activity,
                checkpointer,
                registry,
                shutdown,
                Duration::from_millis(
                    effective_args.idle_suspend_poll_interval_ms.map_or(
                        DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS,
                        PositiveMillis::into_value,
                    ),
                ),
            ) => result,
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

fn local_vacuum_spawn_policy(
    policy: Option<LocalVacuumPolicy>,
    supports_local_vacuum: bool,
) -> Option<LocalVacuumPolicy> {
    policy.filter(|_| supports_local_vacuum)
}

/// One pacing decision for the local vacuum loop: how long to sleep before
/// the next bounded step and how many version keys that step may examine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VacuumPace {
    interval: Duration,
    key_budget: usize,
}

impl VacuumPace {
    /// The relaxed cadence: one default-budget step every couple of seconds.
    const fn idle(policy: LocalVacuumPolicy) -> Self {
        Self {
            interval: policy.idle_interval,
            key_budget: policy.key_budget,
        }
    }
}

/// What the vacuum loop observed across one bounded step, feeding the pacer.
#[derive(Debug, Clone, Copy)]
struct VacuumStepObservation {
    /// Engine-wide committed primary-version Puts since the previous
    /// observation: the settle work foreground writes created meanwhile.
    writes_since_step: u64,
    /// Settle work this step retired: versions pruned + versions frozen +
    /// deleter stamps cleared. Same units as `writes_since_step` — every
    /// committed version Put eventually needs exactly one of the three.
    versions_settled: u64,
    /// Whether the step physically deleted or rewrote anything at all
    /// (including secondary-index and clog deletes).
    swept_anything: bool,
    /// Whether the step wrapped past the last table, completing a cycle.
    cycle_completed: bool,
    /// Whether the foreground looked idle across this step: no write
    /// committed since the previous step and no session ran any statement
    /// within [`LocalVacuumPolicy::idle_after`].
    foreground_idle: bool,
    /// Wall-clock duration of the step itself (excluding the sleep).
    step_elapsed: Duration,
}

/// Adaptive pacing for the local vacuum loop.
///
/// The controller keeps a debt ledger in settle-work units: committed
/// version Puts add debt, retired sweep work repays it. Debt above
/// [`LocalVacuumPolicy::hot_debt`] means the sweep is behind the write rate, so
/// steps run back-to-back (zero interval) until it catches up; once caught
/// up, the interval doubles from [`LocalVacuumPolicy::backoff_floor`] toward the
/// idle cadence. A completed cycle that swept nothing proves the whole
/// keyspace clean, which zeroes leftover debt — writes whose garbage the
/// write path already pruned, or pinned work a later cycle retires — so
/// insert-heavy load cannot wedge the loop at full speed forever. When the
/// foreground goes idle before the store is proven clean, steps run
/// back-to-back regardless of debt: reclaim capacity is spent while it is
/// free instead of after throughput has already decayed. Step budgets grow
/// toward [`LocalVacuumPolicy::max_key_budget`] only while hot steps stay fast and
/// shrink as soon as they slow, so an individual step never becomes a
/// foreground stall.
struct VacuumPacer {
    policy: LocalVacuumPolicy,
    /// Outstanding settle-work units (saturating at zero).
    debt: u64,
    /// Whether the in-progress sweep cycle has physically changed anything.
    cycle_dirty: bool,
    /// Whether the latest completed cycle proved the store clean (swept
    /// nothing, no concurrent writes). Any later write invalidates it.
    store_settled: bool,
    pace: VacuumPace,
}

impl VacuumPacer {
    /// Start at the relaxed cadence with the store not yet proven clean, so
    /// a store recovered with pre-existing garbage drains on the first idle
    /// window instead of waiting for a write to trip the debt ledger.
    const fn new(policy: LocalVacuumPolicy) -> Self {
        Self {
            policy,
            debt: 0,
            cycle_dirty: false,
            store_settled: false,
            pace: VacuumPace::idle(policy),
        }
    }

    const fn pace(&self) -> VacuumPace {
        self.pace
    }

    /// Fold one step's outcome into the ledger and decide the next pace.
    fn observe(&mut self, observation: &VacuumStepObservation) -> VacuumPace {
        if observation.writes_since_step > 0 {
            self.store_settled = false;
        }
        self.debt = self
            .debt
            .saturating_add(observation.writes_since_step)
            .saturating_sub(observation.versions_settled);
        self.cycle_dirty |= observation.swept_anything;
        if observation.cycle_completed {
            if !self.cycle_dirty {
                self.debt = 0;
                self.store_settled = observation.writes_since_step == 0;
            }
            self.cycle_dirty = false;
        }
        let hot = if observation.foreground_idle {
            !self.store_settled
        } else {
            self.debt > self.policy.hot_debt
        };
        let interval = if hot {
            Duration::ZERO
        } else if self.store_settled {
            // Proven clean: park at the idle cadence outright instead of
            // ramping — there is nothing left the ramp could discover.
            self.policy.idle_interval
        } else {
            (self.pace.interval * 2).clamp(self.policy.backoff_floor, self.policy.idle_interval)
        };
        let key_budget = if hot {
            if observation.step_elapsed <= self.policy.step_fast {
                self.pace
                    .key_budget
                    .saturating_mul(2)
                    .min(self.policy.max_key_budget)
            } else if observation.step_elapsed >= self.policy.step_slow {
                (self.pace.key_budget / 2).max(self.policy.key_budget)
            } else {
                self.pace.key_budget
            }
        } else {
            self.policy.key_budget
        };
        self.pace = VacuumPace {
            interval,
            key_budget,
        };
        self.pace
    }
}

fn local_vacuum_maintenance_due(
    swept_anything: bool,
    next_interval: Duration,
    elapsed_since_maintain: Duration,
    policy: LocalVacuumPolicy,
) -> bool {
    swept_anything
        && (next_interval > Duration::ZERO || elapsed_since_maintain >= policy.idle_interval)
}

/// Run bounded dead-MVCC-version sweep steps on the LOCAL serving engine
/// until `shutdown` fires, paced by a [`VacuumPacer`]. The write paths
/// already prune the rows they touch; the stepped sweep catches cold garbage
/// a write never revisits, spreading each full pass across many steps so
/// foreground statements never compete with a whole-store scan — while the
/// pacing keeps the sweep cursor lapping the keyspace at sustained load.
async fn run_local_vacuum_loop(
    engine: SqlEngine,
    activity: Arc<crabka_pgwire::server::ActivityTracker>,
    shutdown: CancellationToken,
    policy: LocalVacuumPolicy,
) {
    let mut pacer = VacuumPacer::new(policy);
    let mut last_version_puts = engine.committed_version_puts();
    let mut last_maintain = std::time::Instant::now();
    loop {
        let pace = pacer.pace();
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(pace.interval) => {}
        }
        let _maintenance = activity.begin_maintenance().await;
        let step_started = std::time::Instant::now();
        match engine.vacuum_step_budgeted(pace.key_budget).await {
            Ok(step) => {
                let step_elapsed = step_started.elapsed();
                let stats = step.stats;
                let swept_anything = stats.versions_pruned
                    + stats.index_entries_pruned
                    + stats.versions_frozen
                    + stats.clog_entries_pruned
                    + stats.stamps_cleared
                    > 0;
                let version_puts = engine.committed_version_puts();
                let writes_since_step = version_puts.saturating_sub(last_version_puts);
                last_version_puts = version_puts;
                let foreground_idle = writes_since_step == 0
                    && idle_window_elapsed(activity.last_activity_unix_millis(), policy.idle_after);
                let next = pacer.observe(&VacuumStepObservation {
                    writes_since_step,
                    versions_settled: stats.versions_pruned
                        + stats.versions_frozen
                        + stats.stamps_cleared,
                    swept_anything,
                    cycle_completed: step.cycle_completed,
                    foreground_idle,
                    step_elapsed,
                });
                // Rotate the LSM memtable only when the step physically
                // deleted or rewrote something, so its tombstones (and the
                // shadowed versions they retire) leave the scan path instead
                // of accumulating until a byte-size rotation — and, during a
                // back-to-back burst, at most once per idle interval (plus
                // the burst's final step) so fast consecutive steps do not
                // spray tiny sstables. Idle steps (settled tables skipped,
                // nothing found) never rotate.
                if local_vacuum_maintenance_due(
                    swept_anything,
                    next.interval,
                    last_maintain.elapsed(),
                    policy,
                ) {
                    last_maintain = std::time::Instant::now();
                    if let Err(error) = engine.kv_handle().maintain() {
                        tracing::warn!(?error, "post-vacuum store maintenance failed");
                    }
                }
                tracing::debug!(
                    versions_pruned = stats.versions_pruned,
                    index_entries_pruned = stats.index_entries_pruned,
                    versions_frozen = stats.versions_frozen,
                    keys_examined = step.keys_examined,
                    cycle_completed = step.cycle_completed,
                    writes_since_step,
                    debt = pacer.debt,
                    foreground_idle,
                    step_elapsed = ?step_elapsed,
                    next_interval = ?next.interval,
                    next_key_budget = next.key_budget,
                    "local vacuum step"
                );
            }
            Err(error) => tracing::warn!(?error, "local vacuum step failed"),
        }
    }
}

#[cfg(test)]
mod vacuum_pacing_tests {
    use assert2::assert;
    use crabka_pgexec::VACUUM_STEP_KEY_BUDGET;

    use super::*;

    const fn default_policy() -> LocalVacuumPolicy {
        LocalVacuumPolicy {
            idle_interval: Duration::from_secs(2),
            backoff_floor: Duration::from_millis(25),
            hot_debt: VACUUM_STEP_KEY_BUDGET as u64,
            key_budget: VACUUM_STEP_KEY_BUDGET,
            max_key_budget: VACUUM_STEP_KEY_BUDGET * 4,
            step_fast: Duration::from_millis(3),
            step_slow: Duration::from_millis(12),
            idle_after: Duration::from_secs(1),
        }
    }

    #[test]
    fn effective_defaults_pin_local_vacuum_policy() {
        const CHILD: &str = "CRABKA_TEST_GRES_LOCAL_VACUUM_DEFAULTS_CHILD";
        const VARIABLES: [&str; 8] = [
            "CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS",
            "CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS",
            "CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT",
            "CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET",
            "CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET",
            "CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS",
            "CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS",
            "CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS",
        ];
        if std::env::var_os(CHILD).is_none() {
            let mut child = std::process::Command::new(std::env::current_exe().expect("test exe"));
            child
                .args([
                    "--exact",
                    "vacuum_pacing_tests::effective_defaults_pin_local_vacuum_policy",
                ])
                .env(CHILD, "1");
            for variable in VARIABLES {
                child.env_remove(variable);
            }
            assert!(child.status().expect("defaults child test").success());
            return;
        }

        assert_eq!(
            local_vacuum_policy(
                &Cli::try_parse_from(["crabka-gres"])
                    .expect("defaults")
                    .serve,
            )
            .expect("valid defaults"),
            Some(default_policy())
        );
    }

    #[test]
    fn derived_maximum_key_budget_rejects_overflow() {
        let args = Cli::try_parse_from([
            "crabka-gres",
            "--local-vacuum-key-budget",
            &usize::MAX.to_string(),
        ])
        .expect("scalar-valid arguments")
        .serve;

        let error = local_vacuum_policy(&args).expect_err("derived maximum overflow");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "local vacuum default maximum key budget overflows usize"
        );
    }

    #[test]
    fn runtime_spawn_decision_requires_local_policy_and_engine_support() {
        let policy = default_policy();

        assert_eq!(local_vacuum_spawn_policy(Some(policy), true), Some(policy));
        assert_eq!(local_vacuum_spawn_policy(Some(policy), false), None);
        assert_eq!(local_vacuum_spawn_policy(None, true), None);
    }

    #[test]
    fn custom_idle_interval_controls_maintenance_rotation() {
        let policy = LocalVacuumPolicy {
            idle_interval: Duration::from_millis(40),
            ..default_policy()
        };

        assert!(!local_vacuum_maintenance_due(
            true,
            Duration::ZERO,
            Duration::from_millis(39),
            policy,
        ));
        assert!(local_vacuum_maintenance_due(
            true,
            Duration::ZERO,
            Duration::from_millis(40),
            policy,
        ));
    }

    /// Observation template: a busy, fast, mid-cycle step that swept nothing.
    const fn quiet_busy_step() -> VacuumStepObservation {
        VacuumStepObservation {
            writes_since_step: 1,
            versions_settled: 0,
            swept_anything: false,
            cycle_completed: false,
            foreground_idle: false,
            step_elapsed: Duration::from_millis(1),
        }
    }

    #[test]
    fn custom_policy_controls_every_local_vacuum_decision() {
        let policy = LocalVacuumPolicy {
            idle_interval: Duration::from_millis(90),
            backoff_floor: Duration::from_millis(7),
            hot_debt: 20,
            key_budget: 10,
            max_key_budget: 40,
            step_fast: Duration::from_millis(2),
            step_slow: Duration::from_millis(8),
            idle_after: Duration::from_millis(30),
        };
        let mut pacer = VacuumPacer::new(policy);
        assert_eq!(
            pacer.pace(),
            VacuumPace {
                interval: policy.idle_interval,
                key_budget: 10
            }
        );

        let hot_fast = VacuumStepObservation {
            writes_since_step: 21,
            step_elapsed: Duration::from_millis(2),
            ..quiet_busy_step()
        };
        assert_eq!(pacer.observe(&hot_fast).key_budget, 20);
        pacer.pace.key_budget = 40;
        assert_eq!(pacer.observe(&hot_fast).key_budget, 40);

        let hot_slow = VacuumStepObservation {
            step_elapsed: Duration::from_millis(8),
            ..hot_fast
        };
        assert_eq!(pacer.observe(&hot_slow).key_budget, 20);

        let caught_up = VacuumStepObservation {
            writes_since_step: 0,
            versions_settled: 100,
            step_elapsed: Duration::from_millis(4),
            ..quiet_busy_step()
        };
        assert_eq!(pacer.observe(&caught_up).interval, policy.backoff_floor);
        let mut previous = policy.backoff_floor;
        loop {
            let pace = pacer.observe(&quiet_busy_step());
            assert_eq!(pace.interval, (previous * 2).min(policy.idle_interval));
            previous = pace.interval;
            if pace.interval == policy.idle_interval {
                break;
            }
        }

        let now = current_unix_millis().expect("system clock");
        let last_activity = now.saturating_sub(20);
        assert!(idle_window_elapsed(
            last_activity,
            Duration::from_millis(10)
        ));
        assert!(!idle_window_elapsed(last_activity, policy.idle_after));
    }

    #[test]
    fn write_backlog_drives_the_interval_to_zero_and_repayment_backs_off() {
        let policy = default_policy();
        let mut pacer = VacuumPacer::new(policy);
        // Writes outpace sweeping: once outstanding work passes the hot
        // threshold the loop stops sleeping entirely.
        let behind = VacuumStepObservation {
            writes_since_step: 4_000,
            versions_settled: 500,
            swept_anything: true,
            ..quiet_busy_step()
        };
        assert!(pacer.observe(&behind).interval == policy.idle_interval); // debt 3 500
        assert!(pacer.observe(&behind).interval == policy.idle_interval); // debt 7 000
        assert!(pacer.observe(&behind).interval == Duration::ZERO); // debt 10 500
        // Sweeping catches up: the interval backs off multiplicatively toward
        // the idle cadence instead of snapping straight to it.
        let repaying = VacuumStepObservation {
            writes_since_step: 0,
            versions_settled: 20_000,
            swept_anything: true,
            ..quiet_busy_step()
        };
        let caught_up = pacer.observe(&repaying);
        assert!(caught_up.interval == policy.backoff_floor);
        assert!(caught_up.key_budget == VACUUM_STEP_KEY_BUDGET);
        let mut previous = caught_up.interval;
        loop {
            let pace = pacer.observe(&quiet_busy_step());
            assert!(pace.interval == (previous * 2).min(policy.idle_interval));
            previous = pace.interval;
            if pace.interval == policy.idle_interval {
                break;
            }
        }
    }

    #[test]
    fn hot_step_budgets_track_step_latency_within_bounds() {
        let policy = default_policy();
        // (previous budget, observed step latency) → next hot step's budget.
        let cases = [
            // Fast steps double the budget…
            (
                VACUUM_STEP_KEY_BUDGET,
                policy.step_fast,
                2 * VACUUM_STEP_KEY_BUDGET,
            ),
            // …but never past the cap…
            (
                policy.max_key_budget,
                Duration::from_millis(1),
                policy.max_key_budget,
            ),
            // …mid-range latency keeps the budget…
            (
                2 * VACUUM_STEP_KEY_BUDGET,
                Duration::from_millis(5),
                2 * VACUUM_STEP_KEY_BUDGET,
            ),
            // …slow steps halve it…
            (
                2 * VACUUM_STEP_KEY_BUDGET,
                policy.step_slow,
                VACUUM_STEP_KEY_BUDGET,
            ),
            // …but never below the pgexec default.
            (
                VACUUM_STEP_KEY_BUDGET,
                Duration::from_millis(50),
                VACUUM_STEP_KEY_BUDGET,
            ),
        ];
        for (previous_budget, step_elapsed, expected) in cases {
            let mut pacer = VacuumPacer::new(policy);
            pacer.debt = 2 * policy.hot_debt;
            pacer.pace.key_budget = previous_budget;
            let pace = pacer.observe(&VacuumStepObservation {
                step_elapsed,
                ..quiet_busy_step()
            });
            assert!(pace.interval == Duration::ZERO);
            assert!(
                pace.key_budget == expected,
                "previous {previous_budget}, elapsed {step_elapsed:?}"
            );
        }
    }

    #[test]
    fn idle_foreground_drains_until_a_clean_cycle_proves_the_store_settled() {
        let policy = default_policy();
        let mut pacer = VacuumPacer::new(policy);
        let idle_dirty = VacuumStepObservation {
            writes_since_step: 0,
            versions_settled: 300,
            swept_anything: true,
            cycle_completed: false,
            foreground_idle: true,
            step_elapsed: Duration::from_millis(1),
        };
        // Zero debt, but the store is not proven clean: drain back-to-back.
        assert!(pacer.observe(&idle_dirty).interval == Duration::ZERO);
        // A cycle that still swept something completes: keep draining (the
        // proving lap has not happened yet).
        let dirty_lap_end = VacuumStepObservation {
            cycle_completed: true,
            ..idle_dirty
        };
        assert!(pacer.observe(&dirty_lap_end).interval == Duration::ZERO);
        // A full lap that swept nothing parks the loop at the idle cadence
        // with the default budget, where it stays while nothing happens.
        let clean_lap = VacuumStepObservation {
            versions_settled: 0,
            swept_anything: false,
            cycle_completed: true,
            ..idle_dirty
        };
        assert!(pacer.observe(&clean_lap) == VacuumPace::idle(policy));
        assert!(pacer.observe(&clean_lap) == VacuumPace::idle(policy));
    }

    #[test]
    fn a_clean_cycle_zeroes_debt_the_write_path_already_repaid() {
        let policy = default_policy();
        let mut pacer = VacuumPacer::new(policy);
        // Load whose garbage the write path prunes itself: debt builds even
        // though sweeps find nothing.
        let phantom = VacuumStepObservation {
            writes_since_step: 20_000,
            ..quiet_busy_step()
        };
        assert!(pacer.observe(&phantom).interval == Duration::ZERO);
        // The hot lap completes without sweeping anything: the ledger resets
        // instead of pinning the loop at full speed forever.
        let clean_lap = VacuumStepObservation {
            writes_since_step: 100,
            cycle_completed: true,
            ..quiet_busy_step()
        };
        assert!(pacer.observe(&clean_lap).interval == policy.backoff_floor);
        assert!(pacer.debt == 0);
    }

    #[test]
    fn a_write_after_settling_reopens_idle_drain() {
        let policy = default_policy();
        let mut pacer = VacuumPacer::new(policy);
        let clean_idle_lap = VacuumStepObservation {
            writes_since_step: 0,
            versions_settled: 0,
            swept_anything: false,
            cycle_completed: true,
            foreground_idle: true,
            step_elapsed: Duration::from_millis(1),
        };
        assert!(pacer.observe(&clean_idle_lap).interval == policy.idle_interval);
        // A small write lands: the store is no longer proven clean, so the
        // next idle window drains again even though debt stays tiny.
        let small_write = VacuumStepObservation {
            writes_since_step: 5,
            ..quiet_busy_step()
        };
        assert!(pacer.observe(&small_write).interval == policy.idle_interval);
        let idle_unproven = VacuumStepObservation {
            cycle_completed: false,
            ..clean_idle_lap
        };
        assert!(pacer.observe(&idle_unproven).interval == Duration::ZERO);
    }
}

fn lifecycle_registry_bootstrap(
    bootstrap: Option<&str>,
    tenant_security_enabled: bool,
) -> Option<&str> {
    // Tenant-scoped Kafka principals are deliberately denied access to the
    // global registry. Their lifecycle remains owned by the control plane.
    if tenant_security_enabled {
        return None;
    }
    bootstrap.filter(|address| !is_in_memory_bootstrap(address))
}

/// Early-bound range transport whose serve task is aborted unless startup
/// hands it to the normal serving flow.
///
/// A startup error after the early bind must not leave a detached task
/// serving the warming transport — and possibly live timestamp grants — in a
/// process that reported failure to start.
struct EarlyRangeServer {
    server: Option<(tokio::task::JoinHandle<()>, SocketAddr)>,
}

impl EarlyRangeServer {
    /// Hand the serve task to the normal serving flow, disarming the abort.
    fn release(mut self) -> (tokio::task::JoinHandle<()>, SocketAddr) {
        self.server
            .take()
            .expect("early range server is released at most once")
    }
}

impl Drop for EarlyRangeServer {
    fn drop(&mut self) {
        if let Some((handle, _)) = self.server.take() {
            handle.abort();
        }
    }
}

/// Bind the range transport before recovery when the boot is identifiably a
/// live multirange runtime.
///
/// The listener serves a warming service that answers every request with a
/// re-resolvable error until recovery installs the range-0 timestamp oracle
/// and, later, tenant assembly swaps in the full topology. Every other
/// runtime mode returns `None` and keeps binding after the runtime opens.
async fn bind_early_range_transport(
    args: &ServeArgs,
) -> std::io::Result<Option<(Arc<DynamicLiveRangeService>, EarlyRangeServer)>> {
    if args.range_listen.is_none() {
        return Ok(None);
    }
    let Some(config) = SubstrateRuntimeConfig::from_args(args)? else {
        return Ok(None);
    };
    if config.ranges.is_none() || config.is_in_memory_bootstrap() {
        return Ok(None);
    }
    let dynamic = Arc::new(DynamicLiveRangeService::new(
        crabka_gres_ranges::HostedRangeService::new(BTreeMap::new()),
    ));
    let server = start_range_service(
        args,
        Some(Arc::clone(&dynamic) as Arc<dyn crabka_gres_ranges::RangeService>),
    )
    .await?
    .ok_or_else(|| std::io::Error::other("early range transport did not bind"))?;
    Ok(Some((
        dynamic,
        EarlyRangeServer {
            server: Some(server),
        },
    )))
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
    poll_interval: Duration,
) -> std::io::Result<()> {
    loop {
        tokio::time::sleep(poll_interval).await;
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
        policy: &RegistryPolicy,
    ) -> std::io::Result<Option<TenantRecord>>;
}

/// Kafka-backed tenant-config loader used by the binary.
///
/// In-memory bootstraps (`memory://` / `in-memory://`) have no broker to
/// dial, so the loader reports no tenant record instead of resolving the
/// bootstrap as a socket address.
pub struct LiveTenantConfigLoader;

#[async_trait::async_trait]
impl TenantConfigLoader for LiveTenantConfigLoader {
    async fn load_tenant_config(
        &self,
        bootstrap: &str,
        tenant: &TenantName,
        security: Option<ClientSecurity>,
        policy: &RegistryPolicy,
    ) -> std::io::Result<Option<TenantRecord>> {
        load_live_tenant_config(bootstrap, tenant, security, policy).await
    }
}

struct LiveRangeRegistrySource {
    bootstrap: String,
    tenant: TenantName,
    security: Option<ClientSecurity>,
    policy: RegistryPolicy,
}

struct MustActivateRangeRegistrySource {
    live: LiveRangeRegistrySource,
    current_layout: Vec<crabka_gres_control::RangeLayoutEntry>,
    source_record_version: u64,
    provisional_target: TenantRecord,
}

struct LiveSplitIntentAuthority {
    bootstrap: String,
    tenant: crabka_gres_control::TenantName,
    policy: RegistryPolicy,
}

/// Build the production registry-backed split authority for integration verification.
#[doc(hidden)]
#[must_use]
pub fn live_split_intent_authority(
    bootstrap: String,
    tenant: crabka_gres_control::TenantName,
    policy: RegistryPolicy,
) -> Arc<dyn crabka_gres_ranges::control::SplitIntentAuthority> {
    Arc::new(LiveSplitIntentAuthority {
        bootstrap,
        tenant,
        policy,
    })
}

#[cfg(test)]
struct AllowSplitIntentAuthority;

#[cfg(test)]
#[async_trait::async_trait]
impl crabka_gres_ranges::control::SplitIntentAuthority for AllowSplitIntentAuthority {
    async fn authorize_request(
        &self,
        _request: &crabka_gres_ranges::transport::RangeControlReq,
        _context: crabka_gres_ranges::control::IntentAuthorizationContext,
    ) -> Result<Option<crabka_gres_ranges::control::AuthorizedSplitIntent>, String> {
        Ok(Some(test_authorized_split_intent()?))
    }
}

#[cfg(test)]
fn test_authorized_split_intent()
-> Result<crabka_gres_ranges::control::AuthorizedSplitIntent, String> {
    use crabka_gres_control::{
        RangeBoundary, RangeLayoutEntry, RangeLayoutSplit, RangeLifecycle, SplitOperationPlan,
        SplitOperationRecord, TenantName,
    };
    let source = RangeLayoutEntry {
        range_id: 0,
        end_key: None,
        endpoint: "source:7443".into(),
        wal_generation: 0,
        lifecycle: RangeLifecycle::Serving,
        retirement: None,
    };
    let left = RangeLayoutEntry {
        range_id: 0,
        end_key: Some(RangeBoundary {
            table_id: 7,
            bucket: None,
            rowid: 10,
        }),
        endpoint: "left:7443".into(),
        wal_generation: 1,
        lifecycle: RangeLifecycle::Serving,
        retirement: None,
    };
    let right = RangeLayoutEntry {
        range_id: 1,
        end_key: None,
        endpoint: "right:7443".into(),
        wal_generation: 1,
        lifecycle: RangeLifecycle::Serving,
        retirement: None,
    };
    let record = SplitOperationRecord::new(
        TenantName::try_from("tenant-a").map_err(|error| error.to_string())?,
        "status-after-replace",
        RangeLayoutSplit {
            source_range_id: 0,
            predecessor_generation: 0,
            left: left.clone(),
            right: right.clone(),
        },
    )
    .map_err(|error| error.to_string())?
    .with_plan(SplitOperationPlan {
        source_record_version: 1,
        source_map_epoch: 0,
        routing_table_id: 7,
        current_layout: vec![source],
        target_layout: vec![left, right],
    })
    .map_err(|error| error.to_string())?;
    crabka_gres_ranges::control::AuthorizedSplitIntent::from_record(record)
}

#[async_trait::async_trait]
impl crabka_gres_ranges::control::SplitIntentAuthority for LiveSplitIntentAuthority {
    async fn authorize_request(
        &self,
        request: &crabka_gres_ranges::transport::RangeControlReq,
        context: crabka_gres_ranges::control::IntentAuthorizationContext,
    ) -> Result<Option<crabka_gres_ranges::control::AuthorizedSplitIntent>, String> {
        if request.tenant != self.tenant.as_str() {
            return Ok(None);
        }
        let mut registry = crabka_gres_control::Registry::connect_with_policy(
            &self.bootstrap,
            self.policy.clone(),
        )
        .await
        .map_err(|error| format!("connect split intent registry: {error}"))?;
        let operation = registry
            .load_split_operation(self.tenant.as_str(), &request.operation_id)
            .await
            .map_err(|error| format!("load split operation: {error}"))?;
        let Some(operation) = operation else {
            return Ok(None);
        };
        let Some(plan) = operation.plan.as_ref() else {
            return Ok(None);
        };
        let current = registry
            .get(self.tenant.as_str())
            .await
            .map_err(|error| format!("load current tenant layout: {error}"))?;
        let Some(current) = current else {
            return Ok(None);
        };
        let activated_pre_cutover_status = operation.phase
            == crabka_gres_control::SplitOperationPhase::Activated
            && matches!(
                request.operation,
                crabka_gres_ranges::transport::RangeControlOperation::Status
            );
        let target_phase = operation.phase.expects_target_registry_layout();
        let expected_layout = if target_phase {
            &plan.target_layout
        } else {
            &plan.current_layout
        };
        let mut layout_matches = current.ranges == *expected_layout
            && if target_phase {
                plan.source_record_version
                    .checked_add(1)
                    .is_some_and(|minimum| current.record_version >= minimum)
            } else {
                current.record_version == plan.source_record_version
            };
        if activated_pre_cutover_status {
            layout_matches |= current.ranges == plan.current_layout
                && current.record_version == plan.source_record_version;
            layout_matches |= current.ranges == plan.target_layout
                && Some(current.record_version) == plan.source_record_version.checked_add(1);
        }
        if !layout_matches {
            return Ok(None);
        }
        crabka_gres_ranges::control::RegistrySplitIntentView::new([operation])
            .authorize_request(request, context)
            .await
    }
}

#[async_trait::async_trait]
impl crabka_gres_ranges::registry::RangeRegistrySource for LiveRangeRegistrySource {
    async fn load_current(&self) -> Result<TenantRecord, crabka_gres_ranges::RegistryError> {
        load_live_tenant_config(
            &self.bootstrap,
            &self.tenant,
            self.security.clone(),
            &self.policy,
        )
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

#[async_trait::async_trait]
impl crabka_gres_ranges::registry::RangeRegistrySource for MustActivateRangeRegistrySource {
    async fn load_current(&self) -> Result<TenantRecord, crabka_gres_ranges::RegistryError> {
        let actual =
            crabka_gres_ranges::registry::RangeRegistrySource::load_current(&self.live).await?;
        select_must_activate_registry_record(
            actual,
            &self.current_layout,
            self.source_record_version,
            &self.provisional_target,
        )
    }
}

fn select_must_activate_registry_record(
    actual: TenantRecord,
    current_layout: &[crabka_gres_control::RangeLayoutEntry],
    source_record_version: u64,
    provisional_target: &TenantRecord,
) -> Result<TenantRecord, crabka_gres_ranges::RegistryError> {
    if actual.ranges == provisional_target.ranges {
        if actual.record_version >= provisional_target.record_version {
            return Ok(actual);
        }
        return Err(crabka_gres_ranges::RegistryError::Authoritative(
            "must-activate target tenant version predates sealed cutover".into(),
        ));
    }
    if actual.ranges == current_layout {
        if actual.record_version == source_record_version {
            return Ok(provisional_target.clone());
        }
        return Err(crabka_gres_ranges::RegistryError::Authoritative(
            "must-activate current tenant version differs from sealed source version".into(),
        ));
    }
    Err(crabka_gres_ranges::RegistryError::Authoritative(
        "must-activate tenant layout conflicts with both sealed current and target maps".into(),
    ))
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
    let policy = args.registry.policy();
    let record = tenant_config_loader
        .load_tenant_config(bootstrap, &tenant, security, &policy)
        .await?;
    let Some(record) = record else {
        // In-memory bootstraps carry no tenant record; the runtime serves
        // with CLI defaults, and `--auth`/`--user-cred` control SQL auth.
        if is_in_memory_bootstrap(bootstrap) {
            return Ok(None);
        }
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
    let checkpointing_requested = checkpointing_was_requested(&args);
    let Some(record) = tenant_record else {
        return Ok(args);
    };
    if !checkpointing_requested {
        return Ok(args);
    }
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
    policy: &RegistryPolicy,
) -> std::io::Result<Option<TenantRecord>> {
    // The in-process substrate seam has no broker to dial and no config
    // topic to read; startup falls back to CLI-provided defaults.
    if is_in_memory_bootstrap(bootstrap) {
        return Ok(None);
    }
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
            policy.fetch_max_wait_ms(),
            policy.fetch_partition_max_bytes(),
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

async fn load_live_split_operation(
    bootstrap: &str,
    tenant: &str,
    operation_id: &str,
    security: Option<ClientSecurity>,
    policy: &RegistryPolicy,
) -> std::io::Result<Option<crabka_gres_control::SplitOperationRecord>> {
    const TOPIC: &str = crabka_gres_control::TENANT_REGISTRY_TOPIC;
    const KEY_PREFIX: &[u8] = b"\0gres-split-operation\0";
    let Some(addr) = resolve_bootstrap_addr(bootstrap) else {
        return invalid_input("substrate bootstrap address list is empty");
    };
    let mut admin = crabka_client_admin::AdminClient::connect_secured(
        &split_bootstrap(bootstrap),
        security.clone(),
    )
    .await
    .map_err(|error| std::io::Error::other(format!("activation registry metadata: {error}")))?;
    let metadata = admin
        .metadata(&[TOPIC])
        .await
        .map_err(|error| std::io::Error::other(format!("activation registry metadata: {error}")))?;
    let topic = metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == TOPIC)
        .ok_or_else(|| std::io::Error::other("activation registry topic is absent"))?;
    if let Some(error) = topic.error {
        return Err(std::io::Error::other(format!(
            "activation registry metadata: {} ({})",
            error.name, error.code
        )));
    }
    let topic_id = topic
        .topic_id
        .ok_or_else(|| std::io::Error::other("activation registry topic id is absent"))?;
    let options = activation_registry_connection_options(tenant, security);
    let conn = crabka_client_core::Connection::connect_with_options(addr, options)
        .await
        .map_err(|error| std::io::Error::other(format!("activation registry connect: {error}")))?;
    let mut expected_key = KEY_PREFIX.to_vec();
    expected_key.extend(
        serde_json::to_vec(&(tenant, operation_id))
            .map_err(|error| std::io::Error::other(format!("activation registry key: {error}")))?,
    );
    let topic_id = crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());
    let mut next_offset = 0_i64;
    let mut latest = None;
    loop {
        let result = crabka_client_core::fetch_partition_with_isolation_progress(
            &conn,
            live_split_operation_fetch(policy, TOPIC, topic_id, next_offset),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("activation registry fetch: {error}")))?;
        for record in result.records {
            if record.key.as_deref() != Some(expected_key.as_slice()) {
                continue;
            }
            apply_live_split_operation_record(
                &mut latest,
                record.value.as_deref(),
                tenant,
                operation_id,
            )?;
        }
        let Some(progress) = result.next_offset else {
            conn.close();
            return Ok(latest);
        };
        next_offset = progress;
    }
}

fn live_split_operation_fetch<'a>(
    policy: &RegistryPolicy,
    topic: &'a str,
    topic_id: crabka_protocol::primitives::uuid::Uuid,
    fetch_offset: i64,
) -> crabka_client_core::IsolatedFetch<'a> {
    crabka_client_core::IsolatedFetch {
        topic,
        topic_id,
        partition: 0,
        fetch_offset,
        max_wait_ms: policy.fetch_max_wait_ms(),
        max_bytes: crabka_client_core::DEFAULT_FETCH_RESPONSE_MAX_BYTES,
        partition_max_bytes: policy.fetch_partition_max_bytes(),
        isolation_level: 1,
    }
}

fn apply_live_split_operation_record(
    latest: &mut Option<crabka_gres_control::SplitOperationRecord>,
    value: Option<&[u8]>,
    tenant: &str,
    operation_id: &str,
) -> std::io::Result<()> {
    let Some(value) = value else {
        *latest = None;
        return Ok(());
    };
    let operation = serde_json::from_slice::<crabka_gres_control::SplitOperationRecord>(value)
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("activation operation: {error}"),
            )
        })?;
    if operation.tenant.as_str() != tenant || operation.operation_id != operation_id {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "activation operation record conflicts with its registry key",
        ));
    }
    operation.ensure_valid().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid activation operation: {error}"),
        )
    })?;
    if let Some(prior) = latest.as_ref() {
        if operation.revision <= prior.revision {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "activation operation revision is not strictly monotone",
            ));
        }
        operation
            .ensure_monotone_extension(prior)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("non-monotone activation operation: {error}"),
                )
            })?;
    }
    *latest = Some(operation);
    Ok(())
}

fn activation_registry_connection_options(
    tenant: &str,
    security: Option<ClientSecurity>,
) -> crabka_client_core::ConnectionOptions {
    crabka_client_core::ConnectionOptions {
        client_id: format!("crabka-gres-activation-reader-{tenant}"),
        security: security.map(Box::new),
        ..Default::default()
    }
}

async fn open_runtime_with_tenant_record(
    args: &ServeArgs,
    tenant_record: Option<&TenantRecord>,
    early_service: Option<Arc<DynamicLiveRangeService>>,
) -> std::io::Result<GresRuntime> {
    if let Some(config) = SubstrateRuntimeConfig::from_args(args)? {
        return Box::pin(open_substrate_runtime_with_tenant_record(
            &config,
            tenant_record,
            early_service,
        ))
        .await;
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn open_substrate_engine(config: &SubstrateRuntimeConfig) -> std::io::Result<SqlEngine> {
    match Box::pin(open_substrate_runtime(config)).await?.engine {
        RuntimeEngine::Single(engine) => Ok(*engine),
        RuntimeEngine::Multi(_) => {
            invalid_input("--ranges constructs a multi-range gateway, not a SqlEngine")
        }
    }
}

/// Construct substrate-mode runtime resources from a cache store and WAL seam.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn open_substrate_runtime(
    config: &SubstrateRuntimeConfig,
) -> std::io::Result<GresRuntime> {
    Box::pin(open_substrate_runtime_with_tenant_record(
        config, None, None,
    ))
    .await
}

async fn open_substrate_runtime_with_tenant_record(
    config: &SubstrateRuntimeConfig,
    tenant_record: Option<&TenantRecord>,
    early_service: Option<Arc<DynamicLiveRangeService>>,
) -> std::io::Result<GresRuntime> {
    use std::io::Error;

    if let Some(boundaries) = config.ranges.as_deref() {
        return Box::pin(open_multirange_runtime(
            config,
            boundaries,
            tenant_record,
            early_service,
        ))
        .await;
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
        |timeout_ms| Ok(GresCheckpointWalPruner::in_memory(timeout_ms)),
    )?;
    if let Some(checkpoint) = &checkpoint {
        seed_checkpoint_planner_stats(checkpoint).await?;
    }
    let engine = build_replicated_substrate_engine(
        &store,
        log,
        barrier.generation,
        outcome.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|checkpoint| Arc::clone(&checkpoint.stats)),
        checkpoint.as_ref().map(|checkpoint| {
            Arc::clone(&checkpoint.planner_stats) as Arc<dyn crabka_pgexec::plan_dist::Stats>
        }),
    )?;
    Ok(match checkpoint {
        Some(checkpoint) => GresRuntime::with_checkpoint_runtime(engine, checkpoint),
        None => GresRuntime::new(engine),
    })
}

fn parse_test_commit_fault(fault: &str) -> std::io::Result<crabka_gres_ranges::GatewayCommitFault> {
    match fault {
        "before_decision_after_prepare" => {
            Ok(crabka_gres_ranges::GatewayCommitFault::BeforeDecisionAfterPrepare)
        }
        "before_release_after_commit_decision" => {
            Ok(crabka_gres_ranges::GatewayCommitFault::BeforeReleaseAfterCommitDecision)
        }
        "after_timestamp_prewrite_before_decision" => {
            Ok(crabka_gres_ranges::GatewayCommitFault::AfterTimestampPrewriteBeforeDecision)
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid CRABKA_GRES_TEST_COMMIT_FAULT",
        )),
    }
}

async fn try_join_stages_with_cleanup<F1, F2, T, E, C>(
    left: F1,
    right: F2,
    cleanup: C,
) -> Result<(T, T), E>
where
    F1: std::future::Future<Output = Result<T, E>>,
    F2: std::future::Future<Output = Result<T, E>>,
    C: FnOnce() -> Result<(), E>,
{
    match tokio::try_join!(left, right) {
        Ok(pair) => Ok(pair),
        Err(error) => {
            cleanup()?;
            Err(error)
        }
    }
}

/// This node's identity among the nodes of one tenant.
///
/// A name no other live process shares, used for exactly one thing: recognising
/// the notification records this node published itself.
///
/// It is always suffixed with a per-process random component, because
/// `--range-listen` is a *bind specification*, not a resolved address — every
/// pod of a tenant is given the same `0.0.0.0:7432` by the operator, and the
/// test harnesses pass the same `127.0.0.1:0` to every node. Deriving the
/// identity from that string alone made every node of a cluster share one
/// origin, so each discarded its peers' notifications as its own and
/// cross-gateway delivery was a silent no-op. The endpoint is kept as a prefix
/// only because it makes a record's origin readable in a log.
fn node_identity(config: &SubstrateRuntimeConfig) -> String {
    let unique: u64 = rand::rng().random();
    match &config.advertised_endpoint {
        Some(endpoint) => format!("{endpoint}#{unique:016x}"),
        None => format!("node-{unique:016x}"),
    }
}

fn multirange_tenant_config(
    config: &SubstrateRuntimeConfig,
    boundaries: &str,
    tenant_record: Option<&TenantRecord>,
) -> std::io::Result<crabka_gres_ranges::MultiRangeTenantConfig> {
    let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("--tenant: {error}"),
        )
    })?;
    let mut tenant_config =
        crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(tenant, boundaries)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
            .with_timestamp_source_mode(config.timestamp_source_mode)
            .with_hlc_wall_offset_ms(config.hlc_wall_offset_ms);
    if let Some(record) = tenant_record {
        tenant_config.range_map = range_map_from_tenant_layout(
            tenant_config.tenant.clone(),
            crabka_gres_ranges::MapEpoch::new(record.record_version),
            &record.ranges,
        )?;
    }
    if let Ok(fault) = std::env::var("CRABKA_GRES_TEST_COMMIT_FAULT") {
        tenant_config =
            tenant_config.with_commit_fault_for_testing(parse_test_commit_fault(&fault)?);
    }
    if let Some(hosted_ranges) = &config.host_ranges {
        if config.is_in_memory_bootstrap() {
            tenant_config = tenant_config
                .with_hosted_ranges(hosted_ranges.clone())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        } else {
            // Live activation recovery may replace the static CLI map before serving. Preserve
            // the syntactically parsed IDs for recovery selection and validate membership only
            // after the durable recovery map has been selected.
            tenant_config.hosted_ranges = Some(hosted_ranges.clone());
        }
    }
    if let Some(record) = tenant_record {
        let mut registry = crabka_gres_ranges::RangeRegistry::from_tenant_record(record)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        if !config.is_in_memory_bootstrap() {
            registry = registry.with_authoritative_source(Arc::new(LiveRangeRegistrySource {
                bootstrap: config.bootstrap.clone(),
                tenant: crabka_gres_control::TenantName::try_from(config.tenant.as_str()).map_err(
                    |error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error),
                )?,
                security: config.kafka_security.clone(),
                policy: config.registry_policy.clone(),
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
    Ok(tenant_config)
}

/// Bootstrap a read-only range-0 follower on a node that does not host the
/// coordinator range and install its read barrier plus continuous tailing.
async fn attach_range0_read_barrier(
    config: &SubstrateRuntimeConfig,
    tenant_config: crabka_gres_ranges::MultiRangeTenantConfig,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
) -> std::io::Result<crabka_gres_ranges::MultiRangeTenantConfig> {
    let follower_config = config
        .live_recovery_config(
            tenant_config.tenant.clone(),
            crabka_gres_ranges::RangeId::COORDINATOR,
        )
        .with_optional_advertised_endpoint(config.advertised_endpoint.clone());
    // Generation 0 of the follower cache; a rebuild after a WAL trim opens the
    // next generation beside it. Anything left over from a previous process is
    // stale by construction and is swept away first.
    range0_follower::remove_other_follower_stores(config.cache_dir.as_deref(), 0);
    let follower_store = range0_follower::open_follower_store(config.cache_dir.as_deref(), 0)?;
    let follower = crabka_gres_substrate::bootstrap_live_range0_follower(
        &follower_config,
        follower_store,
        checkpoint_store.as_deref(),
    )
    .await
    .map_err(|error| std::io::Error::other(format!("range-0 follower bootstrap: {error}")))?;
    let tail = follower.tail();
    // One persistent sampler serves both the per-statement read barrier
    // and the follower poll loop: it holds a live broker connection and
    // an incremental scan cursor, so neither path re-dials per call.
    let end_sampler = Arc::new(crabka_gres_substrate::LiveCommittedEndSampler::new(
        follower_config.clone(),
    ));
    let sampler = Arc::new(crabka_gres_substrate::BrokerRange0EndSampler(
        Arc::clone(&end_sampler) as Arc<dyn crabka_gres_substrate::CommittedEndSampler>,
    ));
    let catalog_refresh_poke = Arc::new(tokio::sync::Notify::new());
    let tenant_config = tenant_config.with_read_only_range0_replica(
        crabka_gres_ranges::ReadOnlyRange0Replica::new(tail, sampler)
            .with_catalog_refresh_poke(Arc::clone(&catalog_refresh_poke)),
    );
    tokio::spawn(
        range0_follower::Range0FollowerTail::new(
            follower,
            follower_config,
            end_sampler,
            checkpoint_store,
            config.cache_dir.clone(),
            config.range0_follower_poll_interval,
            catalog_refresh_poke,
        )
        .run(),
    );
    Ok(tenant_config)
}

async fn open_multirange_runtime(
    config: &SubstrateRuntimeConfig,
    boundaries: &str,
    tenant_record: Option<&TenantRecord>,
    early_service: Option<Arc<DynamicLiveRangeService>>,
) -> std::io::Result<GresRuntime> {
    let mut tenant_config = multirange_tenant_config(config, boundaries, tenant_record)?;
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

    // Past this point every range commits through the substrate WAL, whose
    // apply paths drop notify records before they can reach a store — the
    // precondition for replicating notifications at all.
    tenant_config = tenant_config.with_node_identity(node_identity(config));

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
        tenant_config =
            attach_range0_read_barrier(config, tenant_config, checkpoint_store.clone()).await?;
    }
    let mut activation_receipt =
        split_activation::discover_activation_receipt(config, checkpoint_store.as_deref())
            .await
            .map_err(|error| std::io::Error::other(format!("substrate recovery: {error}")))?;
    reconcile_startup_checkpoint_pins(
        config,
        &tenant_config,
        checkpoint_store.as_deref(),
        activation_receipt.as_ref(),
    )
    .await?;
    let timestamp_primary_aliases = activation_receipt
        .as_ref()
        .map(split_activation::ActivationDiscovery::timestamp_primary_aliases)
        .unwrap_or_default();
    let provisional_registry = if let Some((discovery, current)) =
        activation_receipt.as_mut().zip(tenant_record)
    {
        let operation = load_live_split_operation(
            &config.bootstrap,
            &discovery.receipt.tenant,
            &discovery.receipt.operation_id,
            config.kafka_security.clone(),
            &config.registry_policy,
        )
        .await?
        .ok_or_else(|| std::io::Error::other("activation operation is absent"))?;
        let plan = operation
            .plan
            .as_ref()
            .ok_or_else(|| std::io::Error::other("activation operation plan is absent"))?;
        if matches!(
            operation.phase,
            crabka_gres_control::SplitOperationPhase::LayoutPublished
                | crabka_gres_control::SplitOperationPhase::Retiring
                | crabka_gres_control::SplitOperationPhase::Resuming
                | crabka_gres_control::SplitOperationPhase::Completed
        ) {
            discovery.promote_authoritative_target_recovery()?;
        }
        let target = discovery.provisional_tenant_record(current, plan.source_record_version)?;
        Some((
            plan.current_layout.clone(),
            plan.source_record_version,
            target,
        ))
    } else {
        None
    };
    // Split-activation boots may rewrite the range map between recovery and
    // assembly; they keep the conservative sequencing and the warming
    // transport only receives the fully assembled service.
    let early_tso_service = if activation_receipt.is_none() {
        early_service.as_deref()
    } else {
        None
    };
    let mut engines = recover_live_multirange_engines(
        config,
        &tenant_config,
        checkpoint_store.clone(),
        activation_receipt.as_ref(),
        early_tso_service,
    )
    .await?;
    let (recovered_map, paused_control_recovery) =
        Box::pin(split_activation::reconcile_before_readiness(
            config,
            &mut engines,
            checkpoint_store,
            activation_receipt,
        ))
        .await?;
    if let Some(recovered_map) = recovered_map {
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
    if let Some(hosted_ranges) = &config.host_ranges {
        tenant_config = bind_recovered_hosted_ranges(tenant_config, hosted_ranges)?;
    }
    if paused_control_recovery {
        tenant_config = tenant_config.defer_timestamp_recovery();
    }
    if tenant_config.range_registry.is_some()
        && let Some(provisional) = provisional_registry
    {
        tenant_config.range_registry = Some(must_activate_range_registry(config, provisional)?);
    }
    tracing::info!(
        recovered_ranges = ?tenant_config
            .range_map
            .ranges()
            .iter()
            .map(|range| range.range_id.as_u32())
            .collect::<Vec<_>>(),
        hosted_ranges = ?tenant_config.hosted_ranges,
        engine_ranges = ?engines
            .engines
            .keys()
            .map(|range| range.as_u32())
            .collect::<Vec<_>>(),
        "activation recovery startup handoff"
    );
    open_live_multirange_tenant(
        tenant_config,
        engines,
        config,
        timestamp_primary_aliases,
        early_service,
    )
    .await
}

async fn reconcile_startup_checkpoint_pins(
    config: &SubstrateRuntimeConfig,
    tenant_config: &crabka_gres_ranges::MultiRangeTenantConfig,
    store: Option<&dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
    activation: Option<&split_activation::ActivationDiscovery>,
) -> std::io::Result<()> {
    let Some(store) = store else {
        return Ok(());
    };
    let mut ranges = tenant_config
        .range_map
        .ranges()
        .iter()
        .map(|range| range.range_id)
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(activation) = activation {
        ranges.extend(
            activation
                .receipt
                .split
                .current_map
                .ranges()
                .iter()
                .map(|range| range.range_id),
        );
        ranges.extend(
            activation
                .receipt
                .split
                .target_map
                .ranges()
                .iter()
                .map(|range| range.range_id),
        );
    }
    for range_id in ranges {
        let namespace = format!("{}/r{}", config.tenant, range_id.as_u32());
        let active = activation.and_then(|activation| {
            if !activation_requires_source_checkpoint_pin(activation.receipt.phase) {
                return None;
            }
            let checkpoint = activation.receipt.source_checkpoint.as_ref()?;
            (checkpoint.range_id == range_id).then_some((
                activation.receipt.operation_id.as_str(),
                checkpoint.manifest_key.as_str(),
                checkpoint.covered_offset,
            ))
        });
        crabka_gres_substrate::reconcile_checkpoint_pins(store, &namespace, active)
            .await
            .map_err(|error| {
                std::io::Error::other(format!(
                    "reconcile checkpoint pins for r{range_id}: {error}"
                ))
            })?;
    }
    Ok(())
}

const fn activation_requires_source_checkpoint_pin(
    phase: crabka_gres_ranges::control::TopologyActivationPhase,
) -> bool {
    matches!(
        phase,
        crabka_gres_ranges::control::TopologyActivationPhase::SourceCheckpoint
            | crabka_gres_ranges::control::TopologyActivationPhase::MustActivate
            | crabka_gres_ranges::control::TopologyActivationPhase::WriterActivated
            | crabka_gres_ranges::control::TopologyActivationPhase::CheckpointDurable
    )
}

fn range_map_from_tenant_layout(
    tenant: crabka_gres_ranges::TenantName,
    epoch: crabka_gres_ranges::MapEpoch,
    layout: &[crabka_gres_control::RangeLayoutEntry],
) -> std::io::Result<crabka_gres_ranges::RangeMap> {
    let mut start = crabka_gres_ranges::RangeKey::table_start(crabka_gres_ranges::TableId::new(0));
    let mut ranges = Vec::with_capacity(layout.len());
    for entry in layout {
        let end = entry.end_key.map(|boundary| match boundary.bucket {
            Some(bucket) => crabka_gres_ranges::RangeKey::hash(
                crabka_gres_ranges::TableId::new(boundary.table_id),
                bucket,
                boundary.rowid,
            ),
            None => crabka_gres_ranges::RangeKey::new(
                crabka_gres_ranges::TableId::new(boundary.table_id),
                boundary.rowid,
            ),
        });
        ranges.push(crabka_gres_ranges::RangeSpec::for_interval(
            crabka_gres_ranges::RangeId::new(entry.range_id),
            start,
            end,
        ));
        if let Some(end) = end {
            start = end;
        }
    }
    crabka_gres_ranges::RangeMap::new(tenant, epoch, ranges)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn bind_recovered_hosted_ranges(
    tenant_config: crabka_gres_ranges::MultiRangeTenantConfig,
    hosted_ranges: &[crabka_gres_ranges::RangeId],
) -> std::io::Result<crabka_gres_ranges::MultiRangeTenantConfig> {
    tenant_config
        .with_hosted_ranges(hosted_ranges.to_vec())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
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

/// Build the activation-gated provisional registry for a split-recovery boot.
fn must_activate_range_registry(
    config: &SubstrateRuntimeConfig,
    provisional: (
        Vec<crabka_gres_control::RangeLayoutEntry>,
        u64,
        TenantRecord,
    ),
) -> std::io::Result<crabka_gres_ranges::RangeRegistry> {
    let (current_layout, source_record_version, provisional_target) = provisional;
    Ok(
        crabka_gres_ranges::RangeRegistry::from_tenant_record(&provisional_target)
            .map_err(|error| std::io::Error::other(error.to_string()))?
            .with_authoritative_source(Arc::new(MustActivateRangeRegistrySource {
                live: LiveRangeRegistrySource {
                    bootstrap: config.bootstrap.clone(),
                    tenant: crabka_gres_control::TenantName::try_from(config.tenant.as_str())
                        .map_err(|error| {
                            std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
                        })?,
                    security: config.kafka_security.clone(),
                    policy: config.registry_policy.clone(),
                },
                current_layout,
                source_record_version,
                provisional_target,
            })),
    )
}

async fn recover_live_multirange_engines(
    config: &SubstrateRuntimeConfig,
    tenant_config: &crabka_gres_ranges::MultiRangeTenantConfig,
    checkpoint_store: Option<Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>>,
    activation: Option<&split_activation::ActivationDiscovery>,
    early_service: Option<&DynamicLiveRangeService>,
) -> std::io::Result<LiveMultirangeEngines> {
    let recovery_configs = live_multirange_recovery_configs(config, tenant_config, activation);
    let mut engines = BTreeMap::new();
    let mut range0_tso_horizon = None;
    let mut range0_tso = None;
    for recovery_config in recovery_configs {
        let range_id = recovery_config.range;
        reset_substrate_range_cache(
            config.cache_dir.as_deref(),
            range_id,
            local_checkpoint_root(config),
        )?;
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
            // Range 0 is fenced and replayed: activate the timestamp oracle
            // on the already-listening transport now, so fleet-wide grants
            // resume before this host's remaining ranges recover. The same
            // oracle instance is reused by the assembled tenant — a second
            // instance on the same epoch would let in-flight grants from the
            // first interleave non-monotonically with the second's. The
            // early oracle is already mode-appropriate, so an `Hlc` tenant
            // never serves logical grants, not even during recovery.
            if let (Some(early), Some(horizon)) = (early_service, recovered.tso_horizon.as_ref()) {
                let tso_rpc = build_range0_tso_rpc(
                    horizon,
                    tenant_config.timestamp_source_mode,
                    tenant_config.hlc_wall_offset_ms,
                )?;
                early.replace(
                    crabka_gres_ranges::HostedRangeService::new(BTreeMap::new())
                        .with_tso(Arc::clone(&tso_rpc)),
                );
                tracing::info!("range-0 timestamp oracle serving before full multirange recovery");
                range0_tso = Some(tso_rpc);
            }
        }
        engines.insert(range_id, recovered);
    }
    Ok(LiveMultirangeEngines {
        engines,
        range0_tso_horizon,
        range0_tso,
    })
}

/// Build the range-0 timestamp oracle RPC over a recovered durable horizon,
/// honoring the configured timestamp-source mode.
fn build_range0_tso_rpc(
    tso_horizon: &crabka_gres_substrate::SubstrateTsoHorizon,
    mode: crabka_gres_ranges::TimestampSourceMode,
    hlc_wall_offset_ms: i64,
) -> std::io::Result<Arc<dyn crabka_gres_ranges::TsoRpc>> {
    let persisted_max_ts = tso_horizon
        .load_max_ts()
        .map_err(|error| std::io::Error::other(format!("range-0 TSO horizon: {error}")))?;
    mode_tso_rpc_from_horizon(tso_horizon, persisted_max_ts, mode, hlc_wall_offset_ms)
        .map_err(|error| std::io::Error::other(format!("range-0 TSO oracle: {error}")))
}

/// Build the mode-appropriate range-0 grant oracle from an already-loaded
/// durable horizon.
///
/// Range 0 stays the single timestamp authority in both modes; only what
/// backs the authority differs. `LogicalTso` recovers the dense logical
/// oracle. `Hlc` recovers the wall-anchored oracle: its clock is seeded from
/// the same persisted horizon (so its first grant dominates everything any
/// predecessor granted, even across a wall-clock regression), it persists the
/// horizon in packed strides through the same epoch-gated committer, and the
/// node's fault-injection wall skew is applied exactly as on the in-memory
/// bootstrap path. The returned RPC serves both remote `RangeRequest::Tso`
/// grants and, wrapped by [`crabka_gres_ranges::pgexec_timestamp_oracle_from_rpc`],
/// the local tenant timestamp source.
fn mode_tso_rpc_from_horizon(
    tso_horizon: &crabka_gres_substrate::SubstrateTsoHorizon,
    persisted_max_ts: u64,
    mode: crabka_gres_ranges::TimestampSourceMode,
    hlc_wall_offset_ms: i64,
) -> Result<Arc<dyn crabka_gres_ranges::TsoRpc>, crabka_gres_ranges::TsoError> {
    match mode {
        crabka_gres_ranges::TimestampSourceMode::LogicalTso => {
            crabka_gres_ranges::tso_rpc_from_horizon(
                tso_horizon.clone(),
                tso_horizon.clone(),
                tso_horizon.epoch(),
                persisted_max_ts,
            )
        }
        crabka_gres_ranges::TimestampSourceMode::Hlc { .. } => {
            crabka_gres_ranges::hlc_tso_rpc_from_horizon(
                tso_horizon.clone(),
                tso_horizon.clone(),
                tso_horizon.epoch(),
                persisted_max_ts,
                crabka_gres_ranges::hlc_wall_clock(hlc_wall_offset_ms),
            )
        }
    }
}

struct LiveMultirangeEngines {
    engines: BTreeMap<crabka_gres_ranges::RangeId, LiveRangeEngine>,
    range0_tso_horizon: Option<crabka_gres_substrate::SubstrateTsoHorizon>,
    /// Oracle RPC already activated on the early-bound transport, reused by
    /// the assembled tenant so exactly one oracle lives per writer epoch.
    range0_tso: Option<Arc<dyn crabka_gres_ranges::TsoRpc>>,
}

fn live_multirange_recovery_configs(
    config: &SubstrateRuntimeConfig,
    tenant_config: &crabka_gres_ranges::MultiRangeTenantConfig,
    activation: Option<&split_activation::ActivationDiscovery>,
) -> Vec<crabka_gres_substrate::LiveRecoveryConfig> {
    let mut configs = activation
        .map_or(&tenant_config.range_map, |discovery| {
            &discovery.recovery_map
        })
        .ranges()
        .iter()
        .filter(|spec| {
            activation.is_some()
                || tenant_config
                    .hosted_ranges
                    .as_ref()
                    .is_none_or(|ranges| ranges.contains(&spec.range_id))
        })
        .map(|spec| {
            config
                .live_recovery_config(tenant_config.tenant.clone(), spec.range_id)
                .with_wal_generation(
                    activation
                        .and_then(|discovery| {
                            discovery.recovery_generations.get(&spec.range_id).copied()
                        })
                        .unwrap_or(0),
                )
                .with_optional_advertised_endpoint(config.advertised_endpoint.clone())
        })
        .collect::<Vec<_>>();
    // Recover range 0 ahead of its siblings so the timestamp oracle can start
    // serving grants before the rest of the host finishes recovering.
    configs.sort_by_key(|recovery| recovery.range != crabka_gres_ranges::RangeId::COORDINATOR);
    configs
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
    let recovery_config = config
        .live_recovery_config(tenant, crabka_gres_ranges::RangeId::COORDINATOR)
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
    if let Some(checkpoint) = &checkpoint {
        seed_checkpoint_planner_stats(checkpoint).await?;
    }
    let (engine, committer) = build_replicated_substrate_engine_with_committer(
        &store,
        Arc::clone(&writer),
        recovered.generation,
        recovered.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|runtime| Arc::clone(&runtime.stats)),
        checkpoint.as_ref().map(|runtime| {
            Arc::clone(&runtime.planner_stats) as Arc<dyn crabka_pgexec::plan_dist::Stats>
        }),
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
    range_control:
        std::sync::RwLock<Option<Arc<crabka_gres_ranges::control::GenerationFencedRangeControl>>>,
    publishing: std::sync::atomic::AtomicBool,
}

impl DynamicLiveRangeService {
    fn new(service: crabka_gres_ranges::HostedRangeService) -> Self {
        let range_control = service.range_control_dispatcher();
        Self {
            current: std::sync::RwLock::new(Arc::new(service)),
            range_control: std::sync::RwLock::new(range_control),
            publishing: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn attach_range_control(
        &self,
        control: Arc<crabka_gres_ranges::control::GenerationFencedRangeControl>,
    ) {
        *self.range_control.write().expect("live range control lock") = Some(control);
    }

    fn replace(&self, service: crabka_gres_ranges::HostedRangeService) {
        if let Some(control) = service.range_control_dispatcher() {
            self.attach_range_control(control);
        }
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

    fn load_range_control(
        &self,
    ) -> Option<Arc<crabka_gres_ranges::control::GenerationFencedRangeControl>> {
        self.range_control
            .read()
            .expect("live range control lock")
            .clone()
    }
}

#[async_trait::async_trait]
impl crabka_gres_ranges::RangeService for DynamicLiveRangeService {
    async fn handle(
        &self,
        request: crabka_gres_ranges::RangeRequest,
    ) -> crabka_gres_ranges::RangeResponse {
        if let crabka_gres_ranges::RangeRequest::Control(control_request) = request {
            if let Some(control) = self.load_range_control() {
                return crabka_gres_ranges::RangeResponse::Control(
                    control.handle(control_request).await,
                );
            }
            return self
                .load()
                .handle(crabka_gres_ranges::RangeRequest::Control(control_request))
                .await;
        }
        let activation_recovery = matches!(
            &request,
            crabka_gres_ranges::RangeRequest::TimestampRecover(_)
                | crabka_gres_ranges::RangeRequest::TimestampPrimaryRecover(_)
        );
        if self.publishing.load(std::sync::atomic::Ordering::Acquire) && !activation_recovery {
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
        if let crabka_gres_ranges::RangeRequest::Control(control_request) = request {
            if let Some(control) = self.load_range_control() {
                return Ok(Some(crabka_gres_ranges::RangeResponse::Control(
                    control.handle(control_request).await,
                )));
            }
            return self
                .load()
                .handle_connection(
                    crabka_gres_ranges::RangeRequest::Control(control_request),
                    writer,
                )
                .await;
        }
        let activation_recovery = matches!(
            &request,
            crabka_gres_ranges::RangeRequest::TimestampRecover(_)
                | crabka_gres_ranges::RangeRequest::TimestampPrimaryRecover(_)
        );
        if self.publishing.load(std::sync::atomic::Ordering::Acquire) && !activation_recovery {
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

struct StartedLiveMultirangeTenant {
    gateway: crabka_gres_ranges::MultiRangeTenant,
    _handles: crabka_gres_ranges::MultiRangeTenantHandles,
    tso_rpc: Option<Arc<dyn crabka_gres_ranges::TsoRpc>>,
}

async fn start_live_multirange_tenant(
    tenant_config: crabka_gres_ranges::MultiRangeTenantConfig,
    live_engines: &mut LiveMultirangeEngines,
) -> std::io::Result<StartedLiveMultirangeTenant> {
    // Reuse the oracle already activated during recovery when present: one
    // oracle instance per writer epoch, whether or not it served early. Both
    // arms honor the configured timestamp-source mode — the early oracle was
    // built with it, and the fresh oracle receives it here — so `Hlc` mode
    // genuinely serves wall-anchored grants on the live path.
    let tso_rpc = match (
        live_engines.range0_tso.take(),
        live_engines.range0_tso_horizon.take(),
    ) {
        (Some(tso_rpc), _) => Some(tso_rpc),
        (None, Some(tso_horizon)) => Some(build_range0_tso_rpc(
            &tso_horizon,
            tenant_config.timestamp_source_mode,
            tenant_config.hlc_wall_offset_ms,
        )?),
        (None, None) => None,
    };
    let (gateway, handles, tso_rpc) = if let Some(tso_rpc) = tso_rpc {
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
    Ok(StartedLiveMultirangeTenant {
        gateway,
        _handles: handles,
        tso_rpc,
    })
}

/// Publish the assembled topology on the node's dynamic range service.
///
/// An early-bound transport keeps its dynamic service: swapping the
/// assembled service in here is what upgrades the warming node to serving.
fn install_assembled_range_service(
    early_service: Option<Arc<DynamicLiveRangeService>>,
    range_service: crabka_gres_ranges::HostedRangeService,
) -> Arc<DynamicLiveRangeService> {
    match early_service {
        Some(dynamic) => {
            dynamic.replace(range_service);
            dynamic
        }
        None => Arc::new(DynamicLiveRangeService::new(range_service)),
    }
}

/// Assemble a hosted range service over the gateway's engines with the
/// node-level attachments every assembled topology shares: timestamp-primary
/// aliases and remote routing, plus the DDL gate and catalog-follower barrier.
///
/// Forwarded DDL must serialize behind the same mutex as local DDL and split
/// activation, and follower catalog barriers must observe the replica actually
/// installed on this gateway.
fn assembled_hosted_service(
    gateway: &crabka_gres_ranges::MultiRangeTenant,
    timestamp_primary_aliases: &BTreeMap<crabka_gres_ranges::RangeId, crabka_gres_ranges::RangeId>,
) -> crabka_gres_ranges::HostedRangeService {
    let mut service = crabka_gres_ranges::HostedRangeService::new(gateway.hosted_range_engines())
        .with_timestamp_primary_aliases(timestamp_primary_aliases.clone())
        .with_ddl_gate(gateway.schema_gate());
    if let Some(replica) = gateway.range0_replica() {
        service = service.with_catalog_follower(replica.barrier());
    }
    if let Some((registry, client)) = gateway.timestamp_primary_remote() {
        service = service.with_timestamp_primary_remote(registry, client);
    }
    service
}

async fn open_live_multirange_tenant(
    tenant_config: crabka_gres_ranges::MultiRangeTenantConfig,
    mut live_engines: LiveMultirangeEngines,
    config: &SubstrateRuntimeConfig,
    timestamp_primary_aliases: BTreeMap<crabka_gres_ranges::RangeId, crabka_gres_ranges::RangeId>,
    early_service: Option<Arc<DynamicLiveRangeService>>,
) -> std::io::Result<GresRuntime> {
    let live_resources = live_engines
        .engines
        .iter()
        .map(|(range_id, engine)| (*range_id, engine.resources.clone()))
        .collect();
    let StartedLiveMultirangeTenant {
        gateway,
        _handles,
        tso_rpc,
    } = start_live_multirange_tenant(tenant_config, &mut live_engines).await?;
    let mut range_service = assembled_hosted_service(&gateway, &timestamp_primary_aliases);
    if let Some(tso_rpc) = &tso_rpc {
        range_service = range_service.with_tso(Arc::clone(tso_rpc));
    }
    let dynamic_service = install_assembled_range_service(early_service, range_service);
    let transfer = Arc::new(LiveMultiRangeTransfer::new(
        live_resources,
        (*config).clone(),
        Arc::clone(&dynamic_service),
        gateway.hosted_range_engines(),
        tso_rpc,
        timestamp_primary_aliases.clone(),
    ));
    if transfer.current_range_zero_engine().is_err() {
        let mut hosted_service = assembled_hosted_service(&gateway, &timestamp_primary_aliases)
            .with_durable_inspector(transfer.clone());
        if let Some(tso_rpc) = transfer
            .tso_rpc
            .read()
            .map_err(|_| std::io::Error::other("live TSO lock poisoned"))?
            .clone()
        {
            hosted_service = hosted_service.with_tso(tso_rpc);
        }
        dynamic_service.replace(hosted_service);
        return Ok(GresRuntime {
            engine: RuntimeEngine::Multi(Box::new(gateway)),
            checkpoint_runtime: None,
            range_service: Some(dynamic_service),
            range_transfer: Some(transfer.clone()),
            staged_transfer: Some(transfer),
        });
    }
    let mut generations = transfer
        .ranges
        .read()
        .map_err(|_| std::io::Error::other("live range lock poisoned"))?
        .iter()
        .map(|(range_id, resources)| (*range_id, resources.generation.0))
        .collect::<Vec<_>>();
    generations.extend(
        transfer
            .retired
            .lock()
            .map_err(|_| std::io::Error::other("retired range lock poisoned"))?
            .iter()
            .map(|(range_id, resources)| (*range_id, resources.generation.0)),
    );
    generations.sort_unstable_by_key(|(range_id, _)| *range_id);
    generations.dedup();
    let receipt_store = Arc::new(live_range_control::LiveRangeControlReceiptStore::new(
        config.tenant.clone(),
        &transfer,
    ));
    let mut recovery_receipts =
        crabka_gres_ranges::control::RangeControlReceiptStore::list(receipt_store.as_ref())
            .await
            .map_err(|error| std::io::Error::other(format!("list control receipts: {error}")))?;
    let activation_store = crabka_gres_ranges::control::RangeZeroTopologyActivationStore::new(
        config.tenant.clone(),
        transfer
            .current_range_zero_engine()
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    let activation_receipts =
        crabka_gres_ranges::control::TopologyActivationReceiptStore::list(&activation_store)
            .await
            .map_err(|error| std::io::Error::other(format!("list topology receipts: {error}")))?;
    generations.extend(
        recovery_receipts
            .iter()
            .map(|receipt| (receipt.request.range_id, receipt.request.generation)),
    );
    generations.sort_unstable_by_key(|(range_id, _)| *range_id);
    generations.dedup();
    let Some((first_range, first_generation)) = generations.first().copied() else {
        return Err(std::io::Error::other(
            "range control requires a hosted range",
        ));
    };
    let executor = Box::new(live_range_control::LiveRangeControlExecutor::new(
        &transfer,
        gateway.clone(),
    ));
    let intent_authority = Arc::new(LiveSplitIntentAuthority {
        bootstrap: config.bootstrap.clone(),
        tenant: crabka_gres_control::TenantName::try_from(config.tenant.as_str())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?,
        policy: config.registry_policy.clone(),
    });
    let mut control = crabka_gres_ranges::control::GenerationFencedRangeControl::new(
        config.tenant.clone(),
        first_range,
        first_generation,
        executor,
        intent_authority.clone(),
    )
    .with_receipt_store(receipt_store.clone());
    for (range_id, generation) in generations.into_iter().skip(1) {
        control = control.with_range(range_id, generation);
    }
    let control = Arc::new(control);
    let mut activated_operations = activation_receipts
        .into_iter()
        .filter(|receipt| {
            matches!(
                receipt.phase,
                crabka_gres_ranges::control::TopologyActivationPhase::MustActivate
                    | crabka_gres_ranges::control::TopologyActivationPhase::WriterActivated
                    | crabka_gres_ranges::control::TopologyActivationPhase::CheckpointDurable
                    | crabka_gres_ranges::control::TopologyActivationPhase::TopologyCommitted
            )
        })
        .map(|receipt| receipt.operation_id)
        .collect::<std::collections::BTreeSet<_>>();
    for receipt in &recovery_receipts {
        if matches!(
            receipt.request.operation,
            crabka_gres_ranges::transport::RangeControlOperation::SuccessorFencePrologue { .. }
        ) && receipt.result.is_some()
        {
            activated_operations.insert(receipt.request.operation_id.clone());
        }
    }
    for operation_id in &activated_operations {
        transfer.note_activation_irreversible(operation_id);
    }
    recovery_receipts.retain(|receipt| {
        !activated_operations.contains(&receipt.request.operation_id)
            || matches!(
                receipt.request.operation,
                crabka_gres_ranges::transport::RangeControlOperation::RetirePredecessor
            )
    });
    recovery_receipts.sort_by_key(|receipt| {
        (
            receipt.request.operation_id.clone(),
            live_range_control::recovery_step_rank(&receipt.request.operation),
        )
    });
    for receipt in recovery_receipts {
        if !live_range_control::requires_startup_reconcile(&receipt.request.operation) {
            continue;
        }
        let response = control.handle(receipt.request).await;
        if matches!(
            response,
            crabka_gres_ranges::transport::RangeControlResp::Rejected { .. }
                | crabka_gres_ranges::transport::RangeControlResp::Ambiguous { .. }
        ) {
            return Err(std::io::Error::other(format!(
                "range-control startup reconciliation did not prove readiness: {response:?}"
            )));
        }
    }
    let mut controlled_service = assembled_hosted_service(&gateway, &timestamp_primary_aliases)
        .with_range_control(control)
        .with_durable_inspector(transfer.clone());
    if let Some(tso_rpc) = transfer
        .tso_rpc
        .read()
        .map_err(|_| std::io::Error::other("live TSO lock poisoned"))?
        .clone()
    {
        controlled_service = controlled_service.with_tso(tso_rpc);
    }
    dynamic_service.replace(controlled_service);
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
    planner_stats: Arc<crabka_gres_substrate::CheckpointPlannerStats>,
    snapshot_source: Arc<crabka_gres_substrate::CheckpointSnapshotSource>,
    store: Arc<dyn crabka_gres_substrate::checkpoint::CheckpointStore>,
    tenant: String,
    latest_checkpoint_bytes: std::sync::atomic::AtomicU64,
}

async fn seed_checkpoint_planner_stats(runtime: &StartedCheckpointRuntime) -> std::io::Result<()> {
    let snapshot = runtime.snapshot_source.snapshot();
    let metadata = crabka_gres_substrate::latest_checkpoint_metadata(
        runtime.store.as_ref(),
        &runtime.tenant,
        snapshot.wal_generation,
        None,
    )
    .await
    .map_err(|error| std::io::Error::other(format!("load checkpoint planner stats: {error}")))?;
    if let Some(metadata) = metadata {
        runtime.planner_stats.publish_verified(metadata);
    }
    Ok(())
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

/// Operation-scoped activation decisions reconstructed from durable receipts at startup.
#[derive(Default)]
struct IrreversibleActivations {
    operation_ids: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl IrreversibleActivations {
    fn contains(&self, operation_id: &str) -> Result<bool, String> {
        self.operation_ids
            .lock()
            .map(|operations| operations.contains(operation_id))
            .map_err(|_| "irreversible activation lock poisoned".to_string())
    }

    fn note(&self, operation_id: &str) -> Result<(), String> {
        if operation_id.is_empty() {
            return Err("irreversible activation operation id is empty".to_string());
        }
        self.operation_ids
            .lock()
            .map_err(|_| "irreversible activation lock poisoned".to_string())?
            .insert(operation_id.to_string());
        Ok(())
    }
}

/// Live resources retained for foundation-only transfer operations.
struct LiveMultiRangeTransfer {
    ranges: std::sync::RwLock<BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>>,
    config: SubstrateRuntimeConfig,
    staged: std::sync::Mutex<BTreeMap<crabka_gres_ranges::RangeId, StagedLiveRangeSuccessor>>,
    engines: std::sync::RwLock<BTreeMap<crabka_gres_ranges::RangeId, SqlEngine>>,
    tso_rpc: std::sync::RwLock<Option<Arc<dyn crabka_gres_ranges::TsoRpc>>>,
    timestamp_primary_aliases: BTreeMap<crabka_gres_ranges::RangeId, crabka_gres_ranges::RangeId>,
    range_service: Arc<DynamicLiveRangeService>,
    retired: std::sync::Mutex<BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>>,
    pending: std::sync::Mutex<Option<PendingLiveTopology>>,
    prepared: std::sync::Mutex<Option<PreparedLiveTopology>>,
    committed_activation: std::sync::Mutex<Option<String>>,
    prepare_fault: std::sync::atomic::AtomicU8,
    activation_fault: std::sync::atomic::AtomicU8,
    irreversible_activations: IrreversibleActivations,
}

struct LiveRangeGenerationWitness<'a> {
    transfer: &'a LiveMultiRangeTransfer,
    range_id: crabka_gres_ranges::RangeId,
}

#[async_trait::async_trait]
impl crabka_gres_substrate::GenerationWitness for LiveRangeGenerationWitness<'_> {
    async fn current_generation(&self) -> Result<u64, crabka_gres_substrate::SubstrateError> {
        self.transfer
            .range(self.range_id)
            .map(|resources| resources.generation.0)
            .map_err(|error| crabka_gres_substrate::SubstrateError::Unavailable(error.to_string()))
    }
}

#[async_trait::async_trait]
impl crabka_gres_ranges::DurableRecordInspector for LiveMultiRangeTransfer {
    async fn inspect(
        &self,
        request: crabka_gres_ranges::InspectDurableRecordsReq,
    ) -> Result<crabka_gres_ranges::InspectDurableRecordsResp, String> {
        use std::fmt::Write as _;

        use crabka_pgkv::key::KeyClass;
        use sha2::{Digest, Sha256};

        if request.tenant != self.config.tenant {
            return Err("durable inspection tenant does not match this compute".into());
        }
        let resources = self
            .range(request.range_id)
            .map_err(|error| error.to_string())?;
        if resources.generation.0 != request.generation {
            return Err("durable inspection generation is fenced".into());
        }
        let prefix = crabka_pgkv::key::table_prefix(request.table_id);
        let mut prefix_end = prefix.clone();
        let last = prefix_end.last_mut().expect("table prefix is non-empty");
        *last = last
            .checked_add(1)
            .ok_or_else(|| "table prefix has no successor".to_string())?;
        if request.start_key >= request.end_key
            || !request.start_key.starts_with(&prefix)
            || !(request.end_key.starts_with(&prefix) || request.end_key == prefix_end)
        {
            return Err(
                "durable inspection interval is outside the table primary namespace".into(),
            );
        }
        let digest_request = crabka_gres_ranges::InspectDurableRecordsReq {
            cursor: None,
            ..request.clone()
        };
        let digest = Sha256::digest(
            serde_json::to_vec(&digest_request)
                .map_err(|error| format!("encode inspection digest: {error}"))?,
        );
        let digest = digest
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                write!(&mut text, "{byte:02x}").expect("write to string");
                text
            });
        let cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_durable_cursor(cursor, &digest))
            .transpose()?;
        if let (Some(requested), Some((cursor_sample, _))) =
            (request.snapshot_offset, cursor.as_ref())
            && requested != *cursor_sample
        {
            return Err("durable inspection cursor snapshot does not match request".into());
        }
        let snapshot_offset = request
            .snapshot_offset
            .or_else(|| cursor.as_ref().map(|(sample, _)| *sample));
        let witness = LiveRangeGenerationWitness {
            transfer: self,
            range_id: request.range_id,
        };
        let fold = tokio::time::timeout(std::time::Duration::from_secs(4), async {
            let projection = crabka_gres_substrate::FoldProjection::All;
            let limits = crabka_gres_substrate::FoldLimits {
                max_records: 1_000_000,
                max_bytes: 256 * 1024 * 1024,
            };
            match snapshot_offset {
                Some(sample) => {
                    crabka_gres_substrate::committed_fold_snapshot_live_at(
                        &resources.recovery_config,
                        sample,
                        &witness,
                        projection,
                        limits,
                    )
                    .await
                }
                None => {
                    crabka_gres_substrate::committed_fold_snapshot_live(
                        &resources.recovery_config,
                        &witness,
                        projection,
                        limits,
                    )
                    .await
                }
            }
        })
        .await
        .map_err(|_| "durable inspection deadline exceeded".to_string())?
        .map_err(|error| format!("durable committed fold: {error}"))?;

        let mut selected = Vec::new();
        for ((key, value), source) in fold.records.into_iter().zip(fold.record_sources) {
            let include = match crabka_pgkv::key::classify_key(&key) {
                KeyClass::PrimaryRow { table_id, .. }
                | KeyClass::PrimaryVersion { table_id, .. }
                | KeyClass::HashPrimaryRow { table_id, .. }
                | KeyClass::HashPrimaryVersion { table_id, .. } => {
                    table_id == request.table_id
                        && request.start_key <= key
                        && key < request.end_key
                }
                KeyClass::System => timestamp_metadata_in_interval(
                    &key,
                    &value,
                    request.table_id,
                    &request.start_key,
                    &request.end_key,
                )?,
                KeyClass::SecondaryIndex { table_id, .. } if table_id == request.table_id => {
                    return Err("durable inspection encountered a non-primary table record".into());
                }
                KeyClass::Unknown if key.starts_with(&prefix) => {
                    return Err("durable inspection encountered a malformed table record".into());
                }
                _ => false,
            };
            if include && cursor.as_ref().is_none_or(|(_, after)| key > *after) {
                selected.push(crabka_gres_ranges::DurableRecord {
                    key,
                    value,
                    source_offset: Some(source.offset),
                    source_revision: Some(source.journal_seq),
                });
            }
        }
        selected.sort_by(|left, right| left.key.cmp(&right.key));
        let mut records = Vec::new();
        let mut bytes = 0_u32;
        let mut more = false;
        for record in selected {
            let record_bytes = u32::try_from(record.key.len() + record.value.len())
                .map_err(|_| "durable record size overflow".to_string())?;
            if records.len() >= request.max_records as usize
                || bytes
                    .checked_add(record_bytes)
                    .is_none_or(|next| next > request.max_bytes)
            {
                more = true;
                break;
            }
            bytes += record_bytes;
            records.push(record);
        }
        if records.is_empty() && more {
            return Err("one durable record exceeds the requested byte cap".into());
        }
        let next_cursor = more.then(|| {
            encode_durable_cursor(
                &digest,
                fold.sample_offset,
                &records.last().expect("non-empty limited page").key,
            )
        });
        let checkpoint = fold.checkpoint.as_ref();
        Ok(crabka_gres_ranges::InspectDurableRecordsResp {
            records,
            next_cursor,
            provenance: crabka_gres_ranges::DurableInspectProvenance {
                sample_offset: fold.sample_offset,
                wal_generation: fold.provenance.wal_generation,
                replay_start_offset: fold.provenance.replay_start_offset,
                replayed_records: fold.provenance.replayed_records,
                checkpoint_pairs: fold.provenance.checkpoint_pairs,
                checkpoint_manifest_key: checkpoint.map(|value| value.manifest_key.clone()),
                checkpoint_covered_offset: checkpoint.map(|value| value.covered_offset),
                checkpoint_journal_seq: checkpoint.map(|value| value.journal_seq),
            },
        })
    }
}

fn encode_durable_cursor(digest: &str, sample: i64, key: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut cursor = String::with_capacity(digest.len() + 1 + key.len() * 2);
    cursor.push_str(digest);
    cursor.push(':');
    write!(&mut cursor, "{sample}:").expect("write to string");
    for byte in key {
        write!(&mut cursor, "{byte:02x}").expect("write to string");
    }
    cursor
}

fn decode_durable_cursor(cursor: &str, digest: &str) -> Result<(i64, Vec<u8>), String> {
    let Some((found, rest)) = cursor.split_once(':') else {
        return Err("malformed durable inspection cursor".into());
    };
    let Some((sample, raw)) = rest.split_once(':') else {
        return Err("malformed durable inspection cursor".into());
    };
    if found != digest || raw.len() % 2 != 0 {
        return Err("durable inspection cursor does not match request".into());
    }
    let key = (0..raw.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&raw[offset..offset + 2], 16)
                .map_err(|_| "malformed durable inspection cursor".into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((
        sample
            .parse()
            .map_err(|_| "malformed durable inspection cursor".to_string())?,
        key,
    ))
}

fn timestamp_metadata_in_interval(
    key: &[u8],
    value: &[u8],
    table_id: u32,
    start: &[u8],
    end: &[u8],
) -> Result<bool, String> {
    const INTENT: &[u8] = b"\0\0\0\0meta/ts_intent/";
    const PREWRITE: &[u8] = b"\0\0\0\0meta/ts_prewrite/";
    const DESCRIPTOR: &[u8] = b"\0\0\0\0meta/ts_txn/";
    if let Some(tail) = key
        .strip_prefix(INTENT)
        .or_else(|| key.strip_prefix(PREWRITE))
    {
        let suffix = usize::from(key.starts_with(INTENT)) * 8;
        if !matches!(tail.len(), 12 | 17 | 20 | 25) || tail.len() < suffix + 12 {
            return Err("malformed timestamp metadata key".into());
        }
        let row = &tail[..tail.len() - suffix];
        let found_table = u32::from_be_bytes(row[..4].try_into().expect("4 bytes"));
        if found_table != table_id {
            return Ok(false);
        }
        let physical = match row.len() {
            12 => crabka_pgkv::key::row_key(
                table_id,
                u64::from_be_bytes(row[4..12].try_into().expect("8 bytes")),
            ),
            17 if row[4] == 1 => crabka_pgkv::key::hash_row_key(
                table_id,
                u32::from_be_bytes(row[5..9].try_into().expect("4 bytes")),
                u64::from_be_bytes(row[9..17].try_into().expect("8 bytes")),
            ),
            _ => return Err("malformed timestamp metadata bucket tag".into()),
        };
        return Ok(start <= physical.as_slice() && physical.as_slice() < end);
    }
    if let Some(raw) = key.strip_prefix(DESCRIPTOR) {
        if raw.len() != 8 {
            return Err("malformed timestamp descriptor key".into());
        }
        let start_ts = crabka_pgexec::TimestampTransactionId::new(u64::from_be_bytes(
            raw.try_into().expect("8 bytes"),
        ))
        .map_err(|error| format!("malformed timestamp descriptor timestamp: {error}"))?;
        let descriptor = crabka_pgexec::decode_timestamp_txn_descriptor_value(start_ts, value)
            .map_err(|error| format!("malformed timestamp descriptor: {error}"))?;
        let mut matches = false;
        let mut crosses_table = false;
        for operation in descriptor.operations {
            if operation.table_id != table_id {
                crosses_table = true;
                continue;
            }
            let physical = operation.bucket.map_or_else(
                || crabka_pgkv::key::row_key(table_id, operation.rowid),
                |bucket| crabka_pgkv::key::hash_row_key(table_id, bucket, operation.rowid),
            );
            matches |= start <= physical.as_slice() && physical.as_slice() < end;
        }
        if matches && crosses_table {
            return Err("timestamp descriptor crosses requested table namespace".into());
        }
        return Ok(matches);
    }
    Ok(false)
}

impl LiveMultiRangeTransfer {
    fn new(
        ranges: BTreeMap<crabka_gres_ranges::RangeId, LiveRangeResources>,
        config: SubstrateRuntimeConfig,
        range_service: Arc<DynamicLiveRangeService>,
        engines: BTreeMap<crabka_gres_ranges::RangeId, SqlEngine>,
        tso_rpc: Option<Arc<dyn crabka_gres_ranges::TsoRpc>>,
        timestamp_primary_aliases: BTreeMap<
            crabka_gres_ranges::RangeId,
            crabka_gres_ranges::RangeId,
        >,
    ) -> Self {
        Self {
            ranges: std::sync::RwLock::new(ranges),
            config,
            staged: std::sync::Mutex::new(BTreeMap::new()),
            engines: std::sync::RwLock::new(engines),
            tso_rpc: std::sync::RwLock::new(tso_rpc),
            timestamp_primary_aliases,
            range_service,
            retired: std::sync::Mutex::new(BTreeMap::new()),
            pending: std::sync::Mutex::new(None),
            prepared: std::sync::Mutex::new(None),
            committed_activation: std::sync::Mutex::new(None),
            prepare_fault: std::sync::atomic::AtomicU8::new(PrepareTopologyFault::None as u8),
            activation_fault: std::sync::atomic::AtomicU8::new(TopologyActivationFault::None as u8),
            irreversible_activations: IrreversibleActivations::default(),
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

    async fn compare_and_swap_paused_control_receipt(
        &self,
        tenant: &str,
        receipt: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool, crabka_gres_ranges::RangeTransferError> {
        let range_id = crabka_gres_ranges::RangeId::COORDINATOR;
        let resources = self
            .retired
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?
            .get(&range_id)
            .cloned()
            .map_or_else(|| self.range(range_id), Ok)?;
        let (authorization, barrier_offset) = {
            let state = resources
                .pause
                .lock()
                .map_err(|_| range_pause_lock_error(range_id))?;
            let RangePauseState::Paused(paused) = &*state else {
                return Err(crabka_gres_ranges::RangeTransferError::Unavailable {
                    range_id,
                    reason: "range zero is not paused for structural receipt append".into(),
                });
            };
            (paused.activation_authorization(), paused.barrier_offset)
        };
        resources
            .activation_committer
            .commit_range_control_receipt_cas(
                &authorization,
                barrier_offset,
                tenant,
                receipt,
                expected,
                value,
            )
            .await
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("commit paused range-control receipt: {error}"),
            })
    }

    fn current_range_zero_engine(
        &self,
    ) -> Result<SqlEngine, crabka_gres_ranges::RangeTransferError> {
        self.engines
            .read()
            .map_err(|_| range_pause_lock_error(crabka_gres_ranges::RangeId::COORDINATOR))?
            .get(&crabka_gres_ranges::RangeId::COORDINATOR)
            .map(SqlEngine::clone_handle)
            .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                reason: "current range-zero receipt engine is unavailable".into(),
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

    fn staged_successor_markers(
        &self,
        range_id: crabka_gres_ranges::RangeId,
        start: crabka_gres_ranges::RangeKey,
        end: Option<crabka_gres_ranges::RangeKey>,
    ) -> Result<Vec<crabka_gres_ranges::InDoubtMarker>, crabka_gres_ranges::RangeTransferError>
    {
        let staged = self
            .staged
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?;
        let successor = staged.get(&range_id).ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id,
                reason: "staged successor is unavailable for marker verification".into(),
            }
        })?;
        crabka_gres_ranges::tenant::in_doubt_markers_for_engine(&successor.engine, start, end)
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("inspect staged successor markers: {error}"),
            })
    }

    fn activation_is_irreversible(&self, operation_id: &str) -> bool {
        self.irreversible_activations
            .contains(operation_id)
            .unwrap_or(true)
    }

    fn note_activation_irreversible(&self, operation_id: &str) {
        if let Err(error) = self.irreversible_activations.note(operation_id) {
            tracing::error!(%error, %operation_id, "record irreversible activation");
        }
    }

    async fn release_checkpoint_pin(
        &self,
        operation_id: &str,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        let resources = self
            .retired
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?
            .get(&range_id)
            .cloned()
            .map_or_else(|| self.range(range_id), Ok)?;
        let checkpoint = resources.checkpoint.ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id,
                reason: "checkpoint runtime is unavailable for pin release".into(),
            }
        })?;
        checkpoint
            .handle
            .release_pin(operation_id.to_owned())
            .await
            .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                range_id,
                reason: format!("release checkpoint pin: {error}"),
            })
    }

    async fn retire_predecessor(
        &self,
        operation_id: &str,
        range_id: crabka_gres_ranges::RangeId,
        generation: u64,
        current_map: &crabka_gres_ranges::RangeMap,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        if !self.activation_is_irreversible(operation_id) {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id,
                reason: "predecessor retirement requires irreversible successor activation".into(),
            });
        }
        let resources = self
            .retired
            .lock()
            .map_err(|_| range_pause_lock_error(range_id))?
            .remove(&range_id);
        let Some(resources) = resources else {
            use crabka_gres_ranges::control::{
                RangeZeroTopologyActivationStore, TopologyActivationPhase,
                TopologyActivationReceiptStore,
            };
            let engine = self.current_range_zero_engine()?;
            let tenant = current_map.tenant().to_string();
            let receipt = RangeZeroTopologyActivationStore::new(tenant, engine)
                .load(operation_id)
                .await
                .map_err(|reason| crabka_gres_ranges::RangeTransferError::Runtime {
                    range_id,
                    reason: format!("load retirement activation proof: {reason}"),
                })?
                .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id,
                    reason: "retirement has no authoritative activation receipt".into(),
                })?;
            let ranges = self
                .ranges
                .read()
                .map_err(|_| range_pause_lock_error(range_id))?;
            let targets_ready = receipt.targets.iter().all(|(target_id, target)| {
                ranges
                    .get(target_id)
                    .is_some_and(|resources| resources.generation.0 == target.wal_generation)
                    && self
                        .engines
                        .read()
                        .is_ok_and(|engines| engines.contains_key(target_id))
            });
            let predecessor_replaced = ranges
                .get(&range_id)
                .is_none_or(|resources| resources.generation.0 > generation);
            if receipt.operation_id == operation_id
                && receipt.phase == TopologyActivationPhase::TopologyCommitted
                && receipt.split.predecessor == range_id
                && receipt.split.predecessor_generation == generation
                && receipt.split.target_map == *current_map
                && predecessor_replaced
                && targets_ready
            {
                return Ok(());
            }
            return Err(crabka_gres_ranges::RangeTransferError::Unavailable {
                range_id,
                reason: "retirement proof does not match the committed serving topology".into(),
            });
        };
        let paused = {
            let mut state = resources
                .pause
                .lock()
                .map_err(|_| range_pause_lock_error(range_id))?;
            let RangePauseState::Paused(_) = &*state else {
                return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id,
                    reason: "retired predecessor does not hold its pause fence".into(),
                });
            };
            let RangePauseState::Paused(paused) =
                std::mem::replace(&mut *state, RangePauseState::Pausing)
            else {
                unreachable!()
            };
            paused
        };
        paused.retire();
        Ok(())
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
        receipt.phase = crabka_gres_ranges::control::TopologyActivationPhase::SourceCheckpoint;
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
        split_activation::activate_serving_topology(self).await
    }
    fn finish_serving_topology_publication(&self) {
        self.range_service.finish_publication();
    }

    fn validate_successors(
        &self,
        plan: &crabka_gres_ranges::ValidatedSplitTransferPlan,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        let state = plan.state();
        let cardinality_matches = match state.operation {
            crabka_gres_ranges::SplitOperation::Split => state.right.is_some(),
            crabka_gres_ranges::SplitOperation::Move => state.right.is_none(),
            _ => false,
        };
        if !cardinality_matches {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: "transfer plan must contain exactly one Move or two Split successors"
                    .to_owned(),
            });
        }
        Ok(())
    }

    async fn force_checkpoint(
        &self,
        operation_id: &str,
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
            .checkpoint_from_source_pinned(
                Arc::clone(&checkpoint.snapshot_source),
                crabka_gres_substrate::CheckpointTrigger::Manual,
                operation_id.to_owned(),
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
        _operation_id: &str,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        self.release_pause(barrier)
    }

    async fn release_checkpoint_pin(
        &self,
        operation_id: &str,
        range_id: crabka_gres_ranges::RangeId,
    ) -> Result<(), crabka_gres_ranges::RangeTransferError> {
        LiveMultiRangeTransfer::release_checkpoint_pin(self, operation_id, range_id).await
    }

    fn resume_after_drop(
        &self,
        _operation_id: &str,
        barrier: crabka_gres_ranges::RangeTransferBarrier,
    ) {
        let irreversible_operation = self
            .pending
            .lock()
            .ok()
            .as_ref()
            .and_then(|pending| pending.as_ref())
            .filter(|pending| {
                pending.predecessor == barrier.range_id && pending.barrier_offset == barrier.offset
            })
            .map(|pending| pending.operation_id.clone())
            .is_some_and(|operation_id| self.activation_is_irreversible(&operation_id));
        if irreversible_operation {
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
        let coordinator_catalog: Arc<dyn Kv> = self
            .range(crabka_gres_ranges::RangeId::COORDINATOR)?
            .store
            .clone();
        let stage_range = |request: crabka_gres_ranges::TableTransferRequest| {
            let coordinator_catalog = Arc::clone(&coordinator_catalog);
            async move {
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
                        reason: "source writer does not hold the requested transfer barrier"
                            .to_owned(),
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
                        reason: "checkpoint flags were not configured for the source range"
                            .to_owned(),
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
                        reason:
                            "source manifest key is outside the requested source range namespace"
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
                reset_substrate_range_cache(
                    staged_cache_dir.as_deref(),
                    request.target_range,
                    local_checkpoint_root(&self.config),
                )
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("reset disposable successor cache: {error}"),
                    }
                })?;
                let target_store =
                    open_substrate_range_cache(staged_cache_dir.as_deref(), request.target_range)
                        .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("open empty successor cache: {error}"),
                    })?;
                let target_recovery = self
                    .config
                    .live_recovery_config(
                        source.recovery_config.tenant.clone(),
                        request.target_range,
                    )
                    .with_wal_generation(request.wal_generation)
                    .with_optional_advertised_endpoint(self.config.advertised_endpoint.clone())
                    .with_checkpoints(Arc::clone(&source_checkpoint.store));
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
                .with_structural_ownership(request.target_range == state.left.range_id)
                .with_target_range(request.target_range);
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
                            reason: format!(
                                "restore interval checkpoint and bounded tail: {error}"
                            ),
                        }
                    })?;
                let writer = Arc::new(crabka_gres_substrate::DeferredWalWriter::staged());
                let snapshot_source =
                    Arc::new(crabka_gres_substrate::CheckpointSnapshotSource::new(
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
                .ok_or_else(|| {
                    crabka_gres_ranges::RangeTransferError::Unavailable {
                        range_id: request.target_range,
                        reason: "successor staging requires checkpoint configuration".to_owned(),
                    }
                })?;
                seed_checkpoint_planner_stats(&checkpoint)
                    .await
                    .map_err(|error| crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("load successor checkpoint planner stats: {error}"),
                    })?;
                let (mut engine, committer) = build_replicated_substrate_engine_with_committer(
                    &target_store,
                    Arc::clone(&writer),
                    generation,
                    restore_plan.replay.next_journal_seq,
                    &snapshot_source,
                    Some(Arc::clone(&checkpoint.stats)),
                    Some(Arc::clone(&checkpoint.planner_stats)
                        as Arc<dyn crabka_pgexec::plan_dist::Stats>),
                )
                .map_err(|error| {
                    crabka_gres_ranges::RangeTransferError::Runtime {
                        range_id: request.target_range,
                        reason: format!("build successor SQL engine: {error}"),
                    }
                })?;
                engine.set_catalog_kv(coordinator_catalog);
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
                    let tso_rpc = mode_tso_rpc_from_horizon(
                        horizon,
                        persisted_max_ts,
                        self.config.timestamp_source_mode,
                        self.config.hlc_wall_offset_ms,
                    )
                    .map_err(|error| {
                        crabka_gres_ranges::RangeTransferError::Runtime {
                            range_id: request.target_range,
                            reason: format!("recover successor TSO oracle: {error}"),
                        }
                    })?;
                    engine.set_timestamp_oracle(
                        crabka_gres_ranges::pgexec_timestamp_oracle_from_rpc(tso_rpc),
                    );
                }
                let resources = LiveRangeResources {
                    store: target_store,
                    writer,
                    activation_committer: committer,
                    snapshot_source,
                    checkpoint: Some(checkpoint),
                    recovery_config: target_recovery
                        .with_replay_seed(0, restore_plan.replay.next_journal_seq),
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
            }
        };
        let mut requests = requests.into_iter();
        let left_request =
            requests
                .next()
                .ok_or_else(|| crabka_gres_ranges::RangeTransferError::Boundary {
                    range_id: state.predecessor,
                    reason: "mutation has no successor".into(),
                })?;
        let right_request = requests.next();
        if requests.next().is_some() {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: state.predecessor,
                reason: "mutation has more than two successors".into(),
            });
        }
        let (left, right) = if let Some(right_request) = right_request {
            let left_id = left_request.target_range;
            let right_id = right_request.target_range;
            match try_join_stages_with_cleanup(
                Box::pin(stage_range(left_request)),
                Box::pin(stage_range(right_request)),
                || {
                    let mut staged = self
                        .staged
                        .lock()
                        .map_err(|_| range_pause_lock_error(left_id))?;
                    staged.remove(&left_id);
                    staged.remove(&right_id);
                    Ok(())
                },
            )
            .await
            {
                Ok((left, right)) => (left, Some(right)),
                Err(error) => return Err(error),
            }
        } else {
            (stage_range(left_request).await?, None)
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
        if staged
            .right
            .as_ref()
            .is_some_and(|right| staged.left.range_id == right.range_id)
        {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: staged.left.range_id,
                reason: "successor identities must be distinct".to_owned(),
            });
        }
        let mut successors = self
            .staged
            .lock()
            .map_err(|_| range_pause_lock_error(staged.left.range_id))?;
        if !successors.contains_key(&staged.left.range_id)
            || staged
                .right
                .as_ref()
                .is_some_and(|right| !successors.contains_key(&right.range_id))
        {
            return Err(crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: barrier.range_id,
                reason: "every successor must be staged before any is claimed".to_owned(),
            });
        }
        let left = successors.remove(&staged.left.range_id).ok_or_else(|| {
            crabka_gres_ranges::RangeTransferError::Boundary {
                range_id: staged.left.range_id,
                reason: "left successor disappeared during atomic claim".to_owned(),
            }
        })?;
        let right = staged
            .right
            .as_ref()
            .map(|descriptor| {
                successors.remove(&descriptor.range_id).ok_or_else(|| {
                    crabka_gres_ranges::RangeTransferError::Boundary {
                        range_id: descriptor.range_id,
                        reason: "right successor disappeared during atomic claim".to_owned(),
                    }
                })
            })
            .transpose()?;
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
            right: staged
                .right
                .as_ref()
                .zip(right.as_ref())
                .map(|(descriptor, successor)| {
                    (
                        descriptor.range_id,
                        successor.replay_journal_seq,
                        successor.resources.clone(),
                    )
                }),
        });
        Ok(crabka_gres_ranges::ClaimedStagedSuccessors {
            left: crabka_gres_ranges::ClaimedStagedSuccessor {
                range_id: staged.left.range_id,
                endpoint: staged.left.endpoint.clone(),
                wal_generation: staged.left.wal_generation,
                engine: left.engine,
                keepalive: Arc::new(left.resources),
            },
            right: staged
                .right
                .as_ref()
                .zip(right)
                .map(
                    |(descriptor, successor)| crabka_gres_ranges::ClaimedStagedSuccessor {
                        range_id: descriptor.range_id,
                        endpoint: descriptor.endpoint.clone(),
                        wal_generation: descriptor.wal_generation,
                        engine: successor.engine,
                        keepalive: Arc::new(successor.resources),
                    },
                ),
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
    use std::fmt::Write as _;

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
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(&mut text, "{byte:02x}").expect("write to string");
            text
        })
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
    pruner: impl FnOnce(i32) -> std::io::Result<GresCheckpointWalPruner>,
) -> std::io::Result<Option<StartedCheckpointRuntime>> {
    let Some(checkpoint_config) = &config.checkpoints else {
        return Ok(None);
    };
    let checkpoint_store = match checkpoint_store {
        Some(store) => store,
        None => build_checkpoint_store(checkpoint_config)?,
    };
    let service_config =
        checkpoint_service_config(checkpoint_config, checkpoint_namespace.clone(), wal_topic)?;
    let stats = Arc::new(crabka_gres_substrate::CheckpointStats::default());
    let service = crabka_gres_substrate::CheckpointService::new(
        service_config,
        store,
        Arc::clone(&checkpoint_store),
        Arc::new(pruner(checkpoint_config.delete_records_timeout_ms)?),
        Arc::clone(&stats),
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint service: {error}")))?;
    let planner_stats = service.planner_stats();
    // Poll the configured thresholds off the commit path so a node that never
    // splits still trims its `retention.ms=-1` WAL topic.
    let handle = Arc::new(service).spawn_with_source(Arc::clone(&snapshot_source));
    Ok(Some(StartedCheckpointRuntime {
        handle,
        stats,
        planner_stats,
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
    let service_config =
        checkpoint_service_config(checkpoint_config, namespace.clone(), wal_topic)?;
    let stats = Arc::new(crabka_gres_substrate::CheckpointStats::default());
    let service = crabka_gres_substrate::CheckpointService::new(
        service_config,
        store,
        Arc::clone(&checkpoint_store),
        Arc::new(GresCheckpointWalPruner::kafka(
            &config.bootstrap,
            config.kafka_security.clone(),
            checkpoint_config.delete_records_timeout_ms,
        )?),
        Arc::clone(&stats),
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint service: {error}")))?;
    let planner_stats = service.planner_stats();
    // Poll the configured thresholds off the commit path so a node that never
    // splits still trims its `retention.ms=-1` WAL topic.
    let handle = Arc::new(service).spawn_with_source(Arc::clone(&snapshot_source));
    Ok(Some(StartedCheckpointRuntime {
        handle,
        stats,
        planner_stats,
        snapshot_source,
        store: checkpoint_store,
        tenant: namespace,
        latest_checkpoint_bytes: std::sync::atomic::AtomicU64::new(0),
    }))
}

fn checkpoint_service_config(
    config: &CheckpointRuntimeConfig,
    checkpoint_namespace: String,
    wal_topic: String,
) -> std::io::Result<crabka_gres_substrate::CheckpointConfig> {
    crabka_gres_substrate::CheckpointConfig::new(
        checkpoint_namespace,
        wal_topic,
        config.frames_threshold,
        config.bytes_threshold,
        config.part_max_bytes,
        config.retain_newest,
        config.poll_interval,
    )
    .map_err(|error| std::io::Error::other(format!("checkpoint config: {error}")))
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
        |timeout_ms| {
            GresCheckpointWalPruner::kafka(
                &config.bootstrap,
                config.kafka_security.clone(),
                timeout_ms,
            )
        },
    )?;
    if let Some(checkpoint) = &checkpoint {
        seed_checkpoint_planner_stats(checkpoint).await?;
    }
    let engine = build_replicated_substrate_engine(
        &store,
        writer,
        recovered.generation,
        recovered.next_journal_seq,
        &snapshot_source,
        checkpoint
            .as_ref()
            .map(|checkpoint| Arc::clone(&checkpoint.stats)),
        checkpoint.as_ref().map(|checkpoint| {
            Arc::clone(&checkpoint.planner_stats) as Arc<dyn crabka_pgexec::plan_dist::Stats>
        }),
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
    checkpoint_planner_stats: Option<Arc<dyn crabka_pgexec::plan_dist::Stats>>,
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
        checkpoint_planner_stats,
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
    checkpoint_planner_stats: Option<Arc<dyn crabka_pgexec::plan_dist::Stats>>,
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
    let mut engine = SqlEngine::replicated(
        engine_read_store,
        engine_write_store,
        committer.clone(),
        Arc::new(linearizer),
    )
    .map_err(|error| std::io::Error::other(format!("engine: {error:?}")))?;
    if let Some(checkpoint_stats) = checkpoint_planner_stats {
        engine.set_join_stats(Arc::new(crabka_pgexec::plan_dist::CombinedStats::new(
            engine.join_stats(),
            checkpoint_stats,
        )));
    }
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

fn reset_substrate_range_cache(
    cache_dir: Option<&std::path::Path>,
    range_id: crabka_gres_ranges::RangeId,
    protected_checkpoint_root: Option<&std::path::Path>,
) -> std::io::Result<()> {
    let Some(base) = cache_dir else {
        return Ok(());
    };
    let range_dir = base.join(format!("r{}", range_id.as_u32()));
    if let Some(protected) = protected_checkpoint_root {
        let resolved_range = resolve_path_through_existing_ancestor(&range_dir)?;
        let resolved_protected = resolve_path_through_existing_ancestor(protected)?;
        if resolved_range.starts_with(&resolved_protected)
            || resolved_protected.starts_with(&resolved_range)
        {
            return invalid_input(format!(
                "disposable range cache {} overlaps local checkpoint root {}",
                resolved_range.display(),
                resolved_protected.display()
            ));
        }
    }
    match std::fs::remove_dir_all(&range_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::create_dir_all(range_dir)
}

fn resolve_path_through_existing_ancestor(path: &std::path::Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut missing = Vec::new();
    while !ancestor.exists() {
        let name = ancestor.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cannot resolve protected path {}", absolute.display()),
            )
        })?;
        missing.push(name.to_os_string());
        ancestor = ancestor.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cannot resolve protected path {}", absolute.display()),
            )
        })?;
    }
    let mut resolved = std::fs::canonicalize(ancestor).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "cannot resolve protected path {}: {error}",
                absolute.display()
            ),
        )
    })?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn local_checkpoint_root(config: &SubstrateRuntimeConfig) -> Option<&std::path::Path> {
    match config
        .checkpoints
        .as_ref()
        .map(|checkpoint| &checkpoint.object_store)
    {
        Some(CheckpointObjectStoreConfig::Local { root }) => Some(root.as_path()),
        _ => None,
    }
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn build_session_config(args: &ServeArgs) -> std::io::Result<SessionConfig> {
    build_session_config_from_tenant(args, None)
}

/// Build pgwire session config after optional substrate tenant config has been loaded.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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

    use assert2::assert;
    use clap::CommandFactory as _;

    use super::*;

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    #[test]
    fn timestamp_prewrite_commit_fault_env_value_is_supported() {
        assert_eq!(
            parse_test_commit_fault("after_timestamp_prewrite_before_decision").unwrap(),
            crabka_gres_ranges::GatewayCommitFault::AfterTimestampPrewriteBeforeDecision
        );
        assert!(parse_test_commit_fault("unknown_timestamp_fault").is_err());
    }

    #[test]
    fn registry_policy_options_use_validated_defaults() {
        let defaults = Cli::try_parse_from(["crabka-gres"]).expect("defaults");
        assert!(defaults.serve.registry.policy() == crabka_gres_control::RegistryPolicy::default());
        for option in [
            "--registry-replication-factor=0",
            "--registry-replication-factor=32768",
            "--registry-topic-create-timeout-ms=0",
            "--registry-reader-retry-backoff-ms=0",
            "--registry-fetch-max-wait-ms=0",
            "--registry-fetch-partition-max-bytes=0",
        ] {
            assert!(Cli::try_parse_from(["crabka-gres", option]).is_err());
        }
    }

    #[test]
    fn registry_policy_options_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TEST_GRES_REGISTRY_ENV_CHILD";
        let vars = [
            ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", "2"),
            ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS", "15001"),
            ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS", "251"),
            ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS", "501"),
            ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES", "1048577"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "tests::registry_policy_options_read_environment_and_prefer_cli",
                ])
                .env(CHILD, "1")
                .envs(vars)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }
        let environment = Cli::try_parse_from(["crabka-gres"]).expect("environment policy");
        assert!(
            environment.serve.registry.policy()
                == crabka_gres_control::RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
                    .expect("policy")
        );
        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--registry-replication-factor=3",
            "--registry-topic-create-timeout-ms=15002",
            "--registry-reader-retry-backoff-ms=252",
            "--registry-fetch-max-wait-ms=502",
            "--registry-fetch-partition-max-bytes=1048578",
        ])
        .expect("CLI policy");
        assert!(
            cli.serve.registry.policy()
                == crabka_gres_control::RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578)
                    .expect("policy")
        );
    }

    #[test]
    fn local_vacuum_options_are_absent_by_default_and_cli_overrides_environment() {
        const CHILD: &str = "CRABKA_TEST_GRES_LOCAL_VACUUM_ENV_CHILD";
        let variables = [
            ("CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS", "11"),
            ("CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS", "12"),
            ("CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT", "13"),
            ("CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET", "14"),
            ("CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET", "15"),
            ("CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS", "16"),
            ("CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS", "17"),
            ("CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS", "18"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let mut defaults =
                std::process::Command::new(std::env::current_exe().expect("test exe"));
            defaults
                .args([
                    "--exact",
                    "tests::local_vacuum_options_are_absent_by_default_and_cli_overrides_environment",
                ])
                .env(CHILD, "absent");
            for (name, _) in variables {
                defaults.env_remove(name);
            }
            assert!(defaults.status().expect("defaults child test").success());

            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "tests::local_vacuum_options_are_absent_by_default_and_cli_overrides_environment",
                ])
                .env(CHILD, "configured")
                .envs(variables)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }

        if std::env::var(CHILD).as_deref() == Ok("absent") {
            let defaults = Cli::try_parse_from(["crabka-gres"])
                .expect("defaults")
                .serve;
            assert_eq!(defaults.local_vacuum, LocalVacuumOptions::default());
            return;
        }

        let environment = Cli::try_parse_from(["crabka-gres"])
            .expect("environment policy")
            .serve
            .local_vacuum;
        assert_eq!(
            environment.idle_interval_ms.map(PositiveMillis::into_value),
            Some(11)
        );
        assert_eq!(
            environment.backoff_floor_ms.map(PositiveMillis::into_value),
            Some(12)
        );
        assert_eq!(environment.hot_debt.map(NonZeroU64::get), Some(13));
        assert_eq!(
            environment.key_budget.map(PositiveUsize::into_value),
            Some(14)
        );
        assert_eq!(
            environment.max_key_budget.map(PositiveUsize::into_value),
            Some(15)
        );
        assert_eq!(
            environment.step_fast_ms.map(PositiveMillis::into_value),
            Some(16)
        );
        assert_eq!(
            environment.step_slow_ms.map(PositiveMillis::into_value),
            Some(17)
        );
        assert_eq!(
            environment.idle_after_ms.map(PositiveMillis::into_value),
            Some(18)
        );

        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--local-vacuum-idle-interval-ms",
            "21",
            "--local-vacuum-backoff-floor-ms",
            "22",
            "--local-vacuum-hot-debt",
            "23",
            "--local-vacuum-key-budget",
            "24",
            "--local-vacuum-max-key-budget",
            "25",
            "--local-vacuum-step-fast-ms",
            "26",
            "--local-vacuum-step-slow-ms",
            "27",
            "--local-vacuum-idle-after-ms",
            "28",
        ])
        .expect("CLI policy")
        .serve
        .local_vacuum;
        assert_eq!(
            cli.idle_interval_ms.map(PositiveMillis::into_value),
            Some(21)
        );
        assert_eq!(
            cli.backoff_floor_ms.map(PositiveMillis::into_value),
            Some(22)
        );
        assert_eq!(cli.hot_debt.map(NonZeroU64::get), Some(23));
        assert_eq!(cli.key_budget.map(PositiveUsize::into_value), Some(24));
        assert_eq!(cli.max_key_budget.map(PositiveUsize::into_value), Some(25));
        assert_eq!(cli.step_fast_ms.map(PositiveMillis::into_value), Some(26));
        assert_eq!(cli.step_slow_ms.map(PositiveMillis::into_value), Some(27));
        assert_eq!(cli.idle_after_ms.map(PositiveMillis::into_value), Some(28));
    }

    #[test]
    fn local_vacuum_policy_rejects_invalid_relationships_and_substrate_noops() {
        for option in [
            "--local-vacuum-idle-interval-ms=0",
            "--local-vacuum-backoff-floor-ms=0",
            "--local-vacuum-hot-debt=0",
            "--local-vacuum-key-budget=0",
            "--local-vacuum-max-key-budget=0",
            "--local-vacuum-step-fast-ms=0",
            "--local-vacuum-step-slow-ms=0",
            "--local-vacuum-idle-after-ms=0",
        ] {
            assert!(Cli::try_parse_from(["crabka-gres", option]).is_err());
        }

        for arguments in [
            [
                "--local-vacuum-idle-interval-ms",
                "10",
                "--local-vacuum-backoff-floor-ms",
                "11",
            ]
            .as_slice(),
            [
                "--local-vacuum-key-budget",
                "10",
                "--local-vacuum-max-key-budget",
                "9",
            ]
            .as_slice(),
            [
                "--local-vacuum-step-fast-ms",
                "10",
                "--local-vacuum-step-slow-ms",
                "10",
            ]
            .as_slice(),
        ] {
            let args = Cli::try_parse_from(
                std::iter::once("crabka-gres").chain(arguments.iter().copied()),
            )
            .expect("scalar-valid arguments")
            .serve;
            assert!(local_vacuum_policy(&args).is_err());
        }

        for option in [
            "--local-vacuum-idle-interval-ms=1",
            "--local-vacuum-backoff-floor-ms=1",
            "--local-vacuum-hot-debt=1",
            "--local-vacuum-key-budget=1",
            "--local-vacuum-max-key-budget=1",
            "--local-vacuum-step-fast-ms=1",
            "--local-vacuum-step-slow-ms=1",
            "--local-vacuum-idle-after-ms=1",
        ] {
            let args = Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                option,
            ])
            .expect("scalar-valid arguments")
            .serve;
            assert!(local_vacuum_policy(&args).is_err());
        }
    }

    #[tokio::test]
    async fn local_vacuum_validation_precedes_listener_bind() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let mut args = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--local-vacuum-hot-debt=1",
        ])
        .expect("scalar-valid arguments")
        .serve;
        args.listen = occupied.local_addr().expect("address").to_string();

        let error = run_serve(args).await.expect_err("invalid local policy");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "local vacuum options are incompatible with --substrate-bootstrap"
        );
    }

    #[tokio::test]
    async fn range0_follower_poll_validation_precedes_listener_bind() {
        const CHILD: &str = "CRABKA_TEST_GRES_RANGE0_FOLLOWER_POLL_BIND_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "tests::range0_follower_poll_validation_precedes_listener_bind",
                ])
                .env(CHILD, "1")
                .env_remove("CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS")
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }

        let occupied = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let mut args = Cli::try_parse_from(["crabka-gres"])
            .expect("defaults")
            .serve;
        args.listen = occupied.local_addr().expect("address").to_string();
        args.range0_follower_poll_interval_ms = Some(PositiveMillis::new(1).expect("positive"));

        let error = run_serve(args).await.expect_err("invalid range-0 policy");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "--range0-follower-poll-interval-ms requires --ranges"
        );
    }

    #[test]
    fn checkpoint_lifecycle_options_use_validated_defaults() {
        let args = Cli::try_parse_from(["crabka-gres"])
            .expect("defaults")
            .serve;

        assert!(args.checkpoint_frames.is_none());
        assert!(args.checkpoint_bytes.is_none());
        assert!(args.checkpoint_part_bytes.is_none());
        assert!(args.checkpoint_retain.is_none());
        assert!(args.checkpoint_delete_records_timeout_ms.is_none());
        assert!(args.checkpoint_poll_interval_ms.is_none());
        assert!(args.idle_suspend_poll_interval_ms.is_none());
        assert!(
            SubstrateRuntimeConfig::from_args(&args)
                .expect("standalone defaults")
                .is_none()
        );

        for option in [
            "--checkpoint-part-bytes=7",
            "--checkpoint-retain=0",
            "--checkpoint-delete-records-timeout-ms=0",
            "--checkpoint-delete-records-timeout-ms=2147483648",
            "--checkpoint-poll-interval-ms=0",
            "--idle-suspend-poll-interval-ms=0",
        ] {
            assert!(Cli::try_parse_from(["crabka-gres", option]).is_err());
        }
    }

    #[test]
    fn checkpoint_lifecycle_options_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TEST_GRES_CHECKPOINT_ENV_CHILD";
        let vars = [
            ("CRABKA_GRES_CHECKPOINT_FRAMES", "11"),
            ("CRABKA_GRES_CHECKPOINT_BYTES", "12"),
            ("CRABKA_GRES_CHECKPOINT_PART_BYTES", "13"),
            ("CRABKA_GRES_CHECKPOINT_RETAIN", "14"),
            ("CRABKA_GRES_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS", "15"),
            ("CRABKA_GRES_CHECKPOINT_POLL_INTERVAL_MS", "16"),
            ("CRABKA_GRES_IDLE_SUSPEND_POLL_INTERVAL_MS", "17"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "tests::checkpoint_lifecycle_options_read_environment_and_prefer_cli",
                ])
                .env(CHILD, "1")
                .envs(vars)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }

        let environment = Cli::try_parse_from(["crabka-gres"])
            .expect("environment checkpoint policy")
            .serve;
        assert!(environment.checkpoint_frames.map(NonZeroU64::get) == Some(11));
        assert!(environment.checkpoint_bytes.map(NonZeroU64::get) == Some(12));
        assert!(
            environment
                .checkpoint_part_bytes
                .map(CheckpointPartBytes::into_value)
                == Some(13)
        );
        assert!(environment.checkpoint_retain.map(PositiveUsize::into_value) == Some(14));
        assert!(
            environment
                .checkpoint_delete_records_timeout_ms
                .map(PositiveI32::into_value)
                == Some(15)
        );
        assert!(
            environment
                .checkpoint_poll_interval_ms
                .map(PositiveMillis::into_value)
                == Some(16)
        );
        assert!(
            environment
                .idle_suspend_poll_interval_ms
                .map(PositiveMillis::into_value)
                == Some(17)
        );

        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--checkpoint-frames=21",
            "--checkpoint-bytes=22",
            "--checkpoint-part-bytes=23",
            "--checkpoint-retain=24",
            "--checkpoint-delete-records-timeout-ms=25",
            "--checkpoint-poll-interval-ms=26",
            "--idle-suspend-poll-interval-ms=27",
        ])
        .expect("CLI checkpoint policy")
        .serve;
        assert!(cli.checkpoint_frames.map(NonZeroU64::get) == Some(21));
        assert!(cli.checkpoint_bytes.map(NonZeroU64::get) == Some(22));
        assert!(
            cli.checkpoint_part_bytes
                .map(CheckpointPartBytes::into_value)
                == Some(23)
        );
        assert!(cli.checkpoint_retain.map(PositiveUsize::into_value) == Some(24));
        assert!(
            cli.checkpoint_delete_records_timeout_ms
                .map(PositiveI32::into_value)
                == Some(25)
        );
        assert!(
            cli.checkpoint_poll_interval_ms
                .map(PositiveMillis::into_value)
                == Some(26)
        );
        assert!(
            cli.idle_suspend_poll_interval_ms
                .map(PositiveMillis::into_value)
                == Some(27)
        );
    }

    #[tokio::test]
    async fn two_successor_stages_start_together_and_cleanup_on_failure() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let both_started = Arc::new(tokio::sync::Notify::new());
        let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stage = |result: Result<u8, &'static str>| {
            let started = Arc::clone(&started);
            let both_started = Arc::clone(&both_started);
            async move {
                if started.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == 2 {
                    both_started.notify_waiters();
                }
                while started.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                    both_started.notified().await;
                }
                result
            }
        };
        let cleanup = Arc::clone(&cleaned);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            try_join_stages_with_cleanup(stage(Ok(2)), stage(Err("right failed")), move || {
                cleanup.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }),
        )
        .await
        .expect("parallel stages must not deadlock");
        assert_eq!(result, Err("right failed"));
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(cleaned.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn startup_reset_removes_nonempty_disposable_range_cache() {
        let root = tempfile::tempdir().expect("cache root");
        let range_dir = root.path().join("r1");
        std::fs::create_dir_all(&range_dir).expect("range cache");
        std::fs::write(range_dir.join("stale"), b"cache").expect("stale cache value");

        reset_substrate_range_cache(Some(root.path()), crabka_gres_ranges::RangeId::new(1), None)
            .expect("reset disposable cache");

        assert!(range_dir.is_dir());
        assert!(
            range_dir
                .read_dir()
                .expect("empty range cache")
                .next()
                .is_none()
        );
    }

    #[test]
    fn startup_reset_accepts_missing_cache_and_preserves_siblings() {
        let root = tempfile::tempdir().expect("cache root");
        let sibling = root.path().join("checkpoints");
        std::fs::create_dir_all(&sibling).expect("checkpoint sibling");
        std::fs::write(sibling.join("manifest"), b"durable").expect("checkpoint value");

        reset_substrate_range_cache(
            Some(root.path()),
            crabka_gres_ranges::RangeId::new(2),
            Some(&sibling),
        )
        .expect("reset absent disposable cache");

        assert_eq!(
            std::fs::read(sibling.join("manifest")).expect("checkpoint sibling remains"),
            b"durable"
        );
        assert!(root.path().join("r2").is_dir());
    }

    #[test]
    fn startup_reset_is_scoped_to_each_hosted_range() {
        let root = tempfile::tempdir().expect("cache root");
        for range in [0, 1, 2] {
            let dir = root.path().join(format!("r{range}"));
            std::fs::create_dir_all(&dir).expect("range cache");
            std::fs::write(dir.join("stale"), b"cache").expect("stale cache");
        }

        for range in [0, 2] {
            reset_substrate_range_cache(
                Some(root.path()),
                crabka_gres_ranges::RangeId::new(range),
                None,
            )
            .expect("reset hosted range");
        }

        assert!(root.path().join("r0").is_dir());
        assert!(root.path().join("r1/stale").exists());
        assert!(root.path().join("r2").is_dir());
    }

    #[test]
    fn startup_reset_rejects_checkpoint_overlap_in_both_directions_and_equality() {
        let root = tempfile::tempdir().expect("cache root");
        let range = root.path().join("cache/r1");
        std::fs::create_dir_all(&range).expect("range cache");
        for protected in [
            range.clone(),
            range.join("checkpoints"),
            root.path().join("cache"),
        ] {
            std::fs::create_dir_all(&protected).expect("protected path");
            let error = reset_substrate_range_cache(
                Some(&root.path().join("cache")),
                crabka_gres_ranges::RangeId::new(1),
                Some(&protected),
            )
            .expect_err("overlap must fail closed");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(range.exists(), "overlap rejection cannot delete cache");
        }
    }

    #[cfg(unix)]
    #[test]
    fn startup_reset_resolves_symlinked_checkpoint_overlap() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("cache root");
        let cache = root.path().join("cache");
        let range = cache.join("r1");
        let protected = range.join("durable");
        std::fs::create_dir_all(&protected).expect("protected path");
        let alias = root.path().join("checkpoint-alias");
        symlink(&protected, &alias).expect("checkpoint symlink");

        let error = reset_substrate_range_cache(
            Some(&cache),
            crabka_gres_ranges::RangeId::new(1),
            Some(&alias),
        )
        .expect_err("symlink overlap must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(protected.exists());
    }

    fn registry_test_record(
        version: u64,
        ranges: Vec<crabka_gres_control::RangeLayoutEntry>,
    ) -> TenantRecord {
        let mut record = TenantRecord::new(
            version,
            crabka_gres_control::TenantId::try_from("registry-overlay").expect("id"),
            TenantName::try_from("registry-overlay").expect("name"),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").expect("user"),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            3,
        )
        .expect("record")
        .with_range_layout(ranges)
        .expect("ranges");
        record.record_version = version;
        record
    }

    fn registry_test_range(range_id: u32, endpoint: &str) -> crabka_gres_control::RangeLayoutEntry {
        crabka_gres_control::RangeLayoutEntry {
            range_id,
            end_key: None,
            endpoint: endpoint.into(),
            wal_generation: u64::from(range_id),
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }
    }

    #[test]
    fn must_activate_registry_overlay_never_regresses_and_converges_exactly() {
        let current = registry_test_record(1, vec![registry_test_range(1, "source")]);
        let target = registry_test_record(2, vec![registry_test_range(2, "target")]);

        let overlaid =
            select_must_activate_registry_record(current.clone(), &current.ranges, 1, &target)
                .expect("current overlays target");
        assert_eq!(overlaid, target);

        let mut converged = target.clone();
        converged.record_version = 3;
        assert_eq!(
            select_must_activate_registry_record(converged.clone(), &current.ranges, 1, &target)
                .expect("target converges"),
            converged
        );

        let mut stale_target = target.clone();
        stale_target.record_version = 1;
        assert!(
            select_must_activate_registry_record(stale_target, &current.ranges, 1, &target)
                .is_err()
        );

        let mut newer_current = current.clone();
        newer_current.record_version = 2;
        assert!(
            select_must_activate_registry_record(newer_current, &current.ranges, 1, &target)
                .is_err()
        );

        let mut stale_current = current.clone();
        stale_current.record_version = 0;
        assert!(
            select_must_activate_registry_record(stale_current, &current.ranges, 1, &target)
                .is_err()
        );

        let conflict = registry_test_record(3, vec![registry_test_range(3, "conflict")]);
        assert!(
            select_must_activate_registry_record(conflict, &current.ranges, 1, &target).is_err()
        );
    }

    #[test]
    fn activation_registry_reader_preserves_secured_bootstrap_policy() {
        let options = activation_registry_connection_options(
            "secured-tenant",
            Some(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha512,
                    username: "reader".into(),
                    password: "secret".into(),
                }),
                sasl_host: Some("broker.internal".into()),
            }),
        );

        assert_eq!(
            options.client_id,
            "crabka-gres-activation-reader-secured-tenant"
        );
        let security = options.security.expect("security forwarded to reader");
        assert_eq!(security.protocol, ListenerProtocol::SaslPlaintext);
        assert!(matches!(security.sasl, Some(SaslCredentials::Scram { .. })));
        assert_eq!(security.sasl_host.as_deref(), Some("broker.internal"));
    }

    #[test]
    fn lifecycle_registry_respects_bootstrap_and_tenant_security() {
        struct Case {
            name: &'static str,
            bootstrap: Option<&'static str>,
            tenant_security_enabled: bool,
            expected: Option<&'static str>,
        }

        let cases = [
            Case {
                name: "no substrate bootstrap",
                bootstrap: None,
                tenant_security_enabled: false,
                expected: None,
            },
            Case {
                name: "memory substrate",
                bootstrap: Some("memory://"),
                tenant_security_enabled: false,
                expected: None,
            },
            Case {
                name: "in-memory substrate",
                bootstrap: Some("in-memory://"),
                tenant_security_enabled: false,
                expected: None,
            },
            Case {
                name: "unsecured live substrate",
                bootstrap: Some("broker:9092"),
                tenant_security_enabled: false,
                expected: Some("broker:9092"),
            },
            Case {
                name: "tenant-secured live substrate",
                bootstrap: Some("broker:9093"),
                tenant_security_enabled: true,
                expected: None,
            },
        ];

        for case in cases {
            assert_eq!(
                lifecycle_registry_bootstrap(case.bootstrap, case.tenant_security_enabled,),
                case.expected,
                "{}",
                case.name,
            );
        }
    }

    #[test]
    fn activation_registry_reader_fails_closed_on_noncanonical_history() {
        use crabka_gres_control::SplitOperationPhase;

        let initiated = test_authorized_split_intent()
            .expect("intent")
            .record()
            .clone();
        let running = initiated
            .advance(SplitOperationPhase::Running, 1, None)
            .expect("running");
        let encode = |record: &crabka_gres_control::SplitOperationRecord| {
            serde_json::to_vec(record).expect("encode operation")
        };

        let mut latest = None;
        apply_live_split_operation_record(
            &mut latest,
            Some(&encode(&initiated)),
            initiated.tenant.as_str(),
            &initiated.operation_id,
        )
        .expect("initial record");
        apply_live_split_operation_record(
            &mut latest,
            Some(&encode(&running)),
            initiated.tenant.as_str(),
            &initiated.operation_id,
        )
        .expect("monotone record");

        let mut malformed = initiated.clone();
        malformed.phase = SplitOperationPhase::Running;
        assert!(
            apply_live_split_operation_record(
                &mut None,
                Some(&encode(&malformed)),
                initiated.tenant.as_str(),
                &initiated.operation_id,
            )
            .is_err()
        );
        assert!(
            apply_live_split_operation_record(
                &mut latest.clone(),
                Some(&encode(&initiated)),
                initiated.tenant.as_str(),
                &initiated.operation_id,
            )
            .is_err()
        );

        let divergent = initiated
            .advance(SplitOperationPhase::Running, 2, None)
            .expect("independently valid equal revision");
        assert!(
            apply_live_split_operation_record(
                &mut Some(running.clone()),
                Some(&encode(&divergent)),
                initiated.tenant.as_str(),
                &initiated.operation_id,
            )
            .is_err()
        );

        let mut nonmonotone = divergent.clone();
        nonmonotone.revision += 1;
        nonmonotone.attempts = 1;
        assert!(nonmonotone.ensure_valid().is_ok());
        assert!(
            apply_live_split_operation_record(
                &mut Some(divergent),
                Some(&encode(&nonmonotone)),
                initiated.tenant.as_str(),
                &initiated.operation_id,
            )
            .is_err()
        );

        apply_live_split_operation_record(
            &mut latest,
            None,
            initiated.tenant.as_str(),
            &initiated.operation_id,
        )
        .expect("tombstone");
        assert!(latest.is_none());
    }

    #[test]
    fn irreversible_activation_is_scoped_to_the_operation() {
        let activations = IrreversibleActivations::default();
        activations.note("split-a").expect("record split-a");

        assert!(activations.contains("split-a").expect("load split-a"));
        assert!(!activations.contains("split-b").expect("load split-b"));
    }

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
            registry: RegistryOptions {
                replication_factor: RegistryReplicationFactor::new(1).expect("default"),
                topic_create_timeout_ms: PositiveI32::new(15_000).expect("default"),
                reader_retry_backoff_ms: PositiveMillis::new(250).expect("default"),
                fetch_max_wait_ms: PositiveI32::new(500).expect("default"),
                fetch_partition_max_bytes: PositiveI32::new(1_048_576).expect("default"),
            },
            local_vacuum: LocalVacuumOptions::default(),
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
            range0_follower_poll_interval_ms: None,
            wal_recovery_fetch_max_wait_ms: None,
            wal_recovery_fetch_partition_max_bytes: None,
            wal_recovery_fetch_response_max_bytes: None,
            wal_recovery_empty_fetch_retries: None,
            wal_recovery_dns_timeout_ms: None,
            wal_recovery_connect_timeout_ms: None,
            wal_recovery_request_timeout_ms: None,
            wal_topic_replication_factor: None,
            wal_topic_ensure_timeout_ms: None,
            wal_admin_connect_timeout_ms: None,
            wal_admin_request_timeout_ms: None,
            wal_producer_flush_timeout_ms: None,
            wal_producer_request_timeout_ms: None,
            wal_producer_retries: None,
            wal_producer_retry_backoff_ms: None,
            wal_producer_routing_retry_budget_ms: None,
            wal_producer_init_retry_timeout_ms: None,
            wal_producer_init_max_backoff_ms: None,
            wal_producer_transaction_timeout_ms: None,
            wal_producer_compression: None,
            wal_producer_linger_ms: None,
            wal_producer_batch_bytes: None,
            host_ranges: None,
            timestamp_source: TimestampSourceKind::LogicalTso,
            hlc_max_offset_ms: 250,
            hlc_wall_offset_ms: 0,
            range_listen: None,
            range_tls_cert: None,
            range_tls_key: None,
            range_tls_ca: None,
            range_tls_server_name: None,
            range_allowed_principals: Vec::new(),
            operator_control_principals: Vec::new(),
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
            checkpoint_delete_records_timeout_ms: None,
            checkpoint_poll_interval_ms: None,
            idle_suspend_poll_interval_ms: None,
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
    fn source_checkpoint_pin_is_kept_only_while_activation_needs_it() {
        use crabka_gres_ranges::control::TopologyActivationPhase;

        for phase in [
            TopologyActivationPhase::Prepared,
            TopologyActivationPhase::TopologyCommitted,
            TopologyActivationPhase::Aborted,
        ] {
            assert!(!activation_requires_source_checkpoint_pin(phase));
        }
        for phase in [
            TopologyActivationPhase::SourceCheckpoint,
            TopologyActivationPhase::MustActivate,
            TopologyActivationPhase::WriterActivated,
            TopologyActivationPhase::CheckpointDurable,
        ] {
            assert!(activation_requires_source_checkpoint_pin(phase));
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
        let verifier = PgScramVerifier::generate_with_salt(&fixture_password(), 4096, vec![1; 16])
            .expect("verifier");
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
            _policy: &RegistryPolicy,
        ) -> std::io::Result<Option<TenantRecord>> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct RecordingTenantConfigLoader {
        calls: AtomicUsize,
        policy: std::sync::Mutex<Option<RegistryPolicy>>,
    }

    #[async_trait::async_trait]
    impl TenantConfigLoader for RecordingTenantConfigLoader {
        async fn load_tenant_config(
            &self,
            _bootstrap: &str,
            _tenant: &TenantName,
            _security: Option<ClientSecurity>,
            policy: &RegistryPolicy,
        ) -> std::io::Result<Option<TenantRecord>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.policy.lock().expect("policy lock") = Some(policy.clone());
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
        args.substrate_bootstrap = Some("127.0.0.1:9092".to_string());
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
    async fn memory_substrate_missing_tenant_config_is_tolerated() {
        use assert2::assert;

        let mut args = substrate_args();
        args.registry = RegistryOptions {
            replication_factor: RegistryReplicationFactor::new(2).expect("replication factor"),
            topic_create_timeout_ms: PositiveI32::new(15_001).expect("create timeout"),
            reader_retry_backoff_ms: PositiveMillis::new(251).expect("retry backoff"),
            fetch_max_wait_ms: PositiveI32::new(777).expect("fetch wait"),
            fetch_partition_max_bytes: PositiveI32::new(2_000_000).expect("fetch bytes"),
        };
        let expected_policy = args.registry.policy();
        let loader = RecordingTenantConfigLoader::default();

        let record = load_substrate_tenant_record(&args, &loader)
            .await
            .expect("in-memory bootstrap tolerates a missing tenant record");

        assert!(loader.calls.load(Ordering::SeqCst) == 1);
        assert!(
            loader.policy.lock().expect("policy lock").as_ref() == Some(&expected_policy),
            "serve registry policy must reach the tenant config loader"
        );
        assert!(record.is_none());
    }

    #[test]
    fn split_operation_fetch_uses_registry_policy_limits() {
        let policy = RegistryPolicy::new(2, 15_001, 251, 777, 2_000_000).expect("registry policy");
        let fetch = live_split_operation_fetch(
            &policy,
            "topic",
            crabka_protocol::primitives::uuid::Uuid([0; 16]),
            42,
        );

        assert!(fetch.max_wait_ms == 777);
        assert!(fetch.partition_max_bytes == 2_000_000);
    }

    #[tokio::test]
    async fn live_tenant_config_loader_short_circuits_in_memory_bootstraps() {
        use assert2::assert;

        let tenant = TenantName::try_from("tenant-a").expect("tenant name");
        for bootstrap in ["memory://", "in-memory://"] {
            let record = LiveTenantConfigLoader
                .load_tenant_config(bootstrap, &tenant, None, &RegistryPolicy::default())
                .await
                .expect("in-memory bootstrap must not dial a broker");
            assert!(
                record.is_none(),
                "bootstrap {bootstrap} must yield no tenant record"
            );
        }
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
    fn tenant_checkpoint_fields_do_not_activate_checkpointing() {
        let mut record = tenant_record();
        record.bucket_prefix = Some("from-record".to_string());
        record.checkpoint_frames = Some(77);
        record.checkpoint_bytes = Some(88);
        let applied =
            apply_tenant_runtime_defaults(substrate_args(), Some(&record)).expect("defaults");

        assert!(applied.checkpoint_prefix.is_none());
        assert!(applied.checkpoint_frames.is_none());
        assert!(applied.checkpoint_bytes.is_none());
        assert!(
            SubstrateRuntimeConfig::from_args(&applied)
                .expect("substrate config")
                .expect("substrate config")
                .checkpoints
                .is_none()
        );
    }

    #[test]
    fn checkpoint_threshold_defaults_apply_only_after_tenant_hydration() {
        let mut args = substrate_args();
        args.checkpoint_store = Some(CheckpointStoreKind::InMemory);
        let defaults = CheckpointRuntimeConfig::from_args(&args)
            .expect("checkpoint defaults")
            .expect("checkpoint config");
        assert!(defaults.frames_threshold == DEFAULT_CHECKPOINT_FRAMES);
        assert!(defaults.bytes_threshold == DEFAULT_CHECKPOINT_BYTES);
        assert!(defaults.delete_records_timeout_ms == DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS);
        assert!(
            defaults.poll_interval == Duration::from_millis(DEFAULT_CHECKPOINT_POLL_INTERVAL_MS)
        );

        let mut record = tenant_record();
        record.checkpoint_frames = Some(77);
        record.checkpoint_bytes = Some(88);
        let hydrated = apply_tenant_runtime_defaults(args.clone(), Some(&record))
            .expect("tenant checkpoint policy");
        let from_record = CheckpointRuntimeConfig::from_args(&hydrated)
            .expect("record checkpoint policy")
            .expect("checkpoint config");
        assert!(from_record.frames_threshold == 77);
        assert!(from_record.bytes_threshold == 88);

        args.checkpoint_frames = Some(NonZeroU64::new(7).expect("nonzero"));
        args.checkpoint_bytes = Some(NonZeroU64::new(8).expect("nonzero"));
        let explicit =
            apply_tenant_runtime_defaults(args, Some(&record)).expect("explicit checkpoint policy");
        let from_explicit = CheckpointRuntimeConfig::from_args(&explicit)
            .expect("explicit checkpoint policy")
            .expect("checkpoint config");
        assert!(from_explicit.frames_threshold == 7);
        assert!(from_explicit.bytes_threshold == 8);
    }

    #[test]
    fn checkpoint_runtime_policy_reaches_service_and_pruner_consumers() {
        let args = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--checkpoint-store=in-memory",
            "--checkpoint-delete-records-timeout-ms=1234",
            "--checkpoint-poll-interval-ms=5678",
        ])
        .expect("checkpoint policy")
        .serve;
        let runtime = SubstrateRuntimeConfig::from_args(&args)
            .expect("substrate config")
            .expect("substrate config");
        let checkpoint = runtime.checkpoints.expect("checkpoint config");
        let service = checkpoint_service_config(
            &checkpoint,
            "tenant-a/r0".to_owned(),
            "__gres_wal.tenant-a.r0".to_owned(),
        )
        .expect("service config");
        let pruner = GresCheckpointWalPruner::in_memory(checkpoint.delete_records_timeout_ms);

        assert!(service.poll_interval == Duration::from_millis(5_678));
        assert!(pruner.delete_records_timeout_ms == 1_234);
    }

    #[test]
    fn cli_help_exposes_only_single_node_serve_surface() {
        use assert2::assert;

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
        assert!(help.contains("--timestamp-source"));
        assert!(help.contains("--hlc-max-offset-ms"));
        assert!(help.contains("--hlc-wall-offset-ms"));
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
    fn timestamp_source_hlc_flags_reach_the_tenant_config() {
        use assert2::assert;

        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap",
            "memory://",
            "--tenant",
            "tenant-a",
            "--ranges",
            "0,100",
            "--timestamp-source",
            "hlc",
            "--hlc-max-offset-ms",
            "500",
            "--hlc-wall-offset-ms",
            "-200",
        ])
        .expect("hlc timestamp-source flags parse");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");

        let tenant_config =
            multirange_tenant_config(&config, "0,100", None).expect("tenant config");

        assert!(
            tenant_config.timestamp_source_mode
                == crabka_gres_ranges::TimestampSourceMode::Hlc { max_offset_ms: 500 }
        );
        assert!(tenant_config.hlc_wall_offset_ms == -200);
    }

    #[test]
    fn timestamp_source_defaults_to_the_logical_tso_mode() {
        use assert2::assert;

        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap",
            "memory://",
            "--tenant",
            "tenant-a",
            "--ranges",
            "0,100",
        ])
        .expect("substrate options parse");
        assert!(cli.serve.timestamp_source == TimestampSourceKind::LogicalTso);
        assert!(cli.serve.hlc_max_offset_ms == 250);
        assert!(cli.serve.hlc_wall_offset_ms == 0);
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");

        let tenant_config =
            multirange_tenant_config(&config, "0,100", None).expect("tenant config");

        assert!(
            tenant_config.timestamp_source_mode
                == crabka_gres_ranges::TimestampSourceMode::LogicalTso
        );
        assert!(tenant_config.hlc_wall_offset_ms == 0);
    }

    #[test]
    fn range0_follower_poll_interval_uses_default_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_TEST_GRES_RANGE0_FOLLOWER_POLL_CHILD";
        const ENV: &str = "CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS";
        let base = [
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--ranges=0,10",
        ];
        if std::env::var_os(CHILD).is_none() {
            for (mode, value) in [("default", None), ("environment", Some("17"))] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::range0_follower_poll_interval_uses_default_environment_and_cli_precedence",
                    ])
                    .env(CHILD, mode);
                match value {
                    Some(value) => {
                        child.env(ENV, value);
                    }
                    None => {
                        child.env_remove(ENV);
                    }
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        let expected = if std::env::var(CHILD).as_deref() == Ok("environment") {
            17
        } else {
            DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS
        };
        let parsed = Cli::try_parse_from(base).expect("policy").serve;
        assert_eq!(
            SubstrateRuntimeConfig::from_args(&parsed)
                .expect("valid config")
                .expect("substrate config")
                .range0_follower_poll_interval,
            Duration::from_millis(expected)
        );

        let cli = Cli::try_parse_from(
            base.into_iter()
                .chain(["--range0-follower-poll-interval-ms=19"]),
        )
        .expect("CLI policy")
        .serve;
        assert_eq!(
            SubstrateRuntimeConfig::from_args(&cli)
                .expect("valid config")
                .expect("substrate config")
                .range0_follower_poll_interval,
            Duration::from_millis(19)
        );
    }

    #[test]
    fn range0_follower_poll_interval_rejects_zero_and_non_multirange_use() {
        const CHILD: &str = "CRABKA_TEST_GRES_RANGE0_FOLLOWER_POLL_REJECTION_CHILD";
        const ENV: &str = "CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS";
        if std::env::var_os(CHILD).is_none() {
            for (mode, value) in [
                ("scrubbed", None),
                ("environment_without_ranges", Some("1")),
            ] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::range0_follower_poll_interval_rejects_zero_and_non_multirange_use",
                    ])
                    .env(CHILD, mode)
                    .env_remove(ENV);
                if let Some(value) = value {
                    child.env(ENV, value);
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        if std::env::var(CHILD).as_deref() == Ok("environment_without_ranges") {
            assert!(
                Cli::try_parse_from([
                    "crabka-gres",
                    "--substrate-bootstrap=memory://",
                    "--tenant=tenant-a",
                ])
                .is_err()
            );
            return;
        }

        assert!(
            Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                "--ranges=0,10",
                "--range0-follower-poll-interval-ms=0",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                "--range0-follower-poll-interval-ms=1",
            ])
            .is_err()
        );

        let mut programmatic = Cli::try_parse_from(["crabka-gres"])
            .expect("defaults")
            .serve;
        programmatic.range0_follower_poll_interval_ms =
            Some(PositiveMillis::new(1).expect("positive"));
        let error = SubstrateRuntimeConfig::from_args(&programmatic)
            .expect_err("programmatic non-multirange configuration");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn wal_recovery_read_policy_uses_defaults_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_TEST_GRES_WAL_RECOVERY_READ_POLICY_CHILD";
        const VARS: [&str; 22] = [
            "CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES",
            "CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR",
            "CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_RETRIES",
            "CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_COMPRESSION",
            "CRABKA_GRES_WAL_PRODUCER_LINGER_MS",
            "CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES",
        ];
        if std::env::var_os(CHILD).is_none() {
            for mode in ["defaults", "environment"] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::wal_recovery_read_policy_uses_defaults_environment_and_cli_precedence",
                    ])
                    .env(CHILD, mode);
                for variable in VARS {
                    child.env_remove(variable);
                }
                if mode == "environment" {
                    for (variable, value) in VARS.into_iter().zip([
                        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
                        "29", "30", "31", "32", "33", "34", "35", "none", "36", "37",
                    ]) {
                        child.env(variable, value);
                    }
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        let base = [
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
        ];
        let expected = if std::env::var(CHILD).as_deref() == Ok("environment") {
            (17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27)
        } else {
            (
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS,
                crabka_gres_substrate::DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS,
                crabka_gres_substrate::DEFAULT_WAL_TOPIC_REPLICATION_FACTOR,
                crabka_gres_substrate::DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT_MS,
                crabka_gres_substrate::DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT_MS,
                crabka_gres_substrate::DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT_MS,
            )
        };
        let config = SubstrateRuntimeConfig::from_args(
            &<Cli as clap::Parser>::try_parse_from(base)
                .expect("policy")
                .serve,
        )
        .expect("valid config")
        .expect("substrate config");
        let policy = config.recovery_read_policy;
        assert_eq!(policy.fetch_max_wait_ms(), expected.0);
        assert_eq!(policy.fetch_partition_max_bytes(), expected.1);
        assert_eq!(policy.fetch_response_max_bytes(), expected.2);
        assert_eq!(policy.empty_fetch_retries(), expected.3);
        assert!(policy.dns_timeout() == Duration::from_millis(expected.4));
        assert_eq!(policy.connect_timeout(), Duration::from_millis(expected.5));
        assert_eq!(policy.request_timeout(), Duration::from_millis(expected.6));
        let admin = config.wal_admin_policy;
        assert_eq!(admin.replication_factor(), expected.7);
        assert_eq!(admin.topic_ensure_timeout_ms(), expected.8);
        assert_eq!(admin.connect_timeout(), Duration::from_millis(expected.9));
        assert_eq!(admin.request_timeout(), Duration::from_millis(expected.10));

        let cli = <Cli as clap::Parser>::try_parse_from(base.into_iter().chain([
            "--wal-recovery-fetch-max-wait-ms=27",
            "--wal-recovery-fetch-partition-max-bytes=28",
            "--wal-recovery-fetch-response-max-bytes=29",
            "--wal-recovery-empty-fetch-retries=30",
            "--wal-recovery-dns-timeout-ms=30",
            "--wal-recovery-connect-timeout-ms=31",
            "--wal-recovery-request-timeout-ms=32",
            "--wal-topic-replication-factor=33",
            "--wal-topic-ensure-timeout-ms=34",
            "--wal-admin-connect-timeout-ms=35",
            "--wal-admin-request-timeout-ms=36",
        ]))
        .expect("CLI policy");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        let policy = config.recovery_read_policy;
        assert_eq!(policy.fetch_max_wait_ms(), 27);
        assert_eq!(policy.fetch_partition_max_bytes(), 28);
        assert_eq!(policy.fetch_response_max_bytes(), 29);
        assert_eq!(policy.empty_fetch_retries(), 30);
        assert!(policy.dns_timeout() == Duration::from_millis(30));
        assert_eq!(policy.connect_timeout(), Duration::from_millis(31));
        assert_eq!(policy.request_timeout(), Duration::from_millis(32));
        let admin = config.wal_admin_policy;
        assert_eq!(admin.replication_factor(), 33);
        assert_eq!(admin.topic_ensure_timeout_ms(), 34);
        assert_eq!(admin.connect_timeout(), Duration::from_millis(35));
        assert_eq!(admin.request_timeout(), Duration::from_millis(36));
    }

    #[test]
    fn wal_recovery_hostile_environment_does_not_leak_into_parser_tests() {
        let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
            .args([
                "--exact",
                "tests::registry_policy_options_use_validated_defaults",
            ])
            .env("CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS", "17")
            .env("CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES", "18")
            .env("CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES", "19")
            .env("CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES", "20")
            .env("CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS", "21")
            .env("CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS", "21")
            .env("CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS", "22")
            .env("CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR", "23")
            .env("CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS", "24")
            .env("CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS", "25")
            .env("CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS", "26")
            .env("CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS", "27")
            .env("CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS", "27")
            .env("CRABKA_GRES_WAL_PRODUCER_RETRIES", "28")
            .env("CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS", "29")
            .env("CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS", "30")
            .env("CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS", "31")
            .env("CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS", "32")
            .env("CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS", "33")
            .env("CRABKA_GRES_WAL_PRODUCER_COMPRESSION", "gzip")
            .env("CRABKA_GRES_WAL_PRODUCER_LINGER_MS", "34")
            .env("CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES", "35")
            .status()
            .expect("child test");

        assert!(status.success());
    }

    #[test]
    fn wal_recovery_read_policy_rejects_zero_and_inert_use() {
        for option in [
            "--wal-recovery-fetch-max-wait-ms=0",
            "--wal-recovery-fetch-partition-max-bytes=0",
            "--wal-recovery-fetch-response-max-bytes=0",
            "--wal-recovery-empty-fetch-retries=0",
            "--wal-recovery-dns-timeout-ms=0",
            "--wal-recovery-connect-timeout-ms=0",
            "--wal-recovery-request-timeout-ms=0",
            "--wal-topic-replication-factor=0",
            "--wal-topic-ensure-timeout-ms=0",
            "--wal-admin-connect-timeout-ms=0",
            "--wal-admin-request-timeout-ms=0",
            "--wal-producer-flush-timeout-ms=0",
            "--wal-producer-request-timeout-ms=0",
            "--wal-producer-retry-backoff-ms=0",
            "--wal-producer-routing-retry-budget-ms=0",
            "--wal-producer-init-retry-timeout-ms=0",
            "--wal-producer-init-max-backoff-ms=0",
            "--wal-producer-transaction-timeout-ms=0",
        ] {
            assert!(
                Cli::try_parse_from([
                    "crabka-gres",
                    "--substrate-bootstrap=memory://",
                    "--tenant=tenant-a",
                    option,
                ])
                .is_err()
            );
        }
        for option in [
            "--wal-recovery-dns-timeout-ms=1",
            "--wal-recovery-connect-timeout-ms=1",
            "--wal-recovery-request-timeout-ms=1",
            "--wal-topic-replication-factor=1",
            "--wal-topic-ensure-timeout-ms=1",
            "--wal-admin-connect-timeout-ms=1",
            "--wal-admin-request-timeout-ms=1",
            "--wal-producer-flush-timeout-ms=1",
            "--wal-producer-request-timeout-ms=1",
            "--wal-producer-retries=0",
            "--wal-producer-retry-backoff-ms=1",
            "--wal-producer-routing-retry-budget-ms=1",
            "--wal-producer-init-retry-timeout-ms=1",
            "--wal-producer-init-max-backoff-ms=1",
            "--wal-producer-transaction-timeout-ms=1",
            "--wal-producer-compression=gzip",
            "--wal-producer-linger-ms=0",
            "--wal-producer-batch-bytes=1",
        ] {
            assert!(Cli::try_parse_from(["crabka-gres", option]).is_err());
        }
        let oversized_replication = <Cli as clap::Parser>::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-topic-replication-factor=32768",
        ])
        .expect("positive parser value");
        let error = SubstrateRuntimeConfig::from_args(&oversized_replication.serve)
            .expect_err("replication factor exceeds protocol maximum");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        for (option, field) in [
            (
                "--wal-producer-flush-timeout-ms=2147483648",
                "producer flush timeout",
            ),
            (
                "--wal-producer-request-timeout-ms=2147483648",
                "request timeout",
            ),
            (
                "--wal-producer-retry-backoff-ms=2147483648",
                "producer retry backoff",
            ),
            (
                "--wal-producer-routing-retry-budget-ms=2147483648",
                "routing retry budget",
            ),
            (
                "--wal-producer-init-retry-timeout-ms=2147483648",
                "producer-ID initialization retry timeout",
            ),
            (
                "--wal-producer-init-max-backoff-ms=2147483648",
                "producer-ID initialization maximum backoff",
            ),
            (
                "--wal-producer-transaction-timeout-ms=2147483648",
                "transaction timeout",
            ),
        ] {
            let args = Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                option,
            ])
            .expect("positive parser value")
            .serve;
            let error = SubstrateRuntimeConfig::from_args(&args)
                .expect_err("producer duration exceeds supported maximum");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains(field), "{error}");
        }
        let args = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-retry-backoff-ms=2",
            "--wal-producer-init-max-backoff-ms=1",
        ])
        .expect("positive parser values")
        .serve;
        assert!(
            SubstrateRuntimeConfig::from_args(&args)
                .expect_err("initial backoff exceeds cap")
                .to_string()
                .contains("backoff")
        );
        for (option, field) in [
            ("--wal-producer-linger-ms=2147483648", "linger"),
            ("--wal-producer-batch-bytes=0", "batch bytes"),
            ("--wal-producer-batch-bytes=2147483648", "batch bytes"),
        ] {
            let args = Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                option,
            ])
            .expect("parser value")
            .serve;
            let error = SubstrateRuntimeConfig::from_args(&args)
                .expect_err("throughput value exceeds supported range");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains(field), "{error}");
        }
        assert!(
            Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                "--wal-producer-compression=brotli",
            ])
            .is_err()
        );
        let args = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-retries=0",
        ])
        .expect("zero retries")
        .serve;
        assert_eq!(
            SubstrateRuntimeConfig::from_args(&args)
                .expect("zero retries are valid")
                .expect("substrate config")
                .producer_retry_policy
                .retries(),
            0
        );

        assert!(
            Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                "--wal-producer-retries=-1",
            ])
            .is_err()
        );
        for option in 0..22 {
            let mut programmatic = serve_args(Some("trust"), Vec::new());
            set_wal_policy_option(&mut programmatic, option);
            let error = SubstrateRuntimeConfig::from_args(&programmatic)
                .expect_err("inert recovery policy");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }

    #[tokio::test]
    async fn wal_recovery_read_policy_validation_precedes_listener_bind() {
        const CHILD: &str = "CRABKA_TEST_GRES_WAL_RECOVERY_BIND_CHILD";
        const VARS: [&str; 22] = [
            "CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES",
            "CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR",
            "CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_RETRIES",
            "CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_COMPRESSION",
            "CRABKA_GRES_WAL_PRODUCER_LINGER_MS",
            "CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES",
        ];
        if std::env::var_os(CHILD).is_none() {
            let mut child = std::process::Command::new(std::env::current_exe().expect("test exe"));
            child
                .args([
                    "--exact",
                    "tests::wal_recovery_read_policy_validation_precedes_listener_bind",
                ])
                .env(CHILD, "1");
            for variable in VARS {
                child.env_remove(variable);
            }
            assert!(child.status().expect("child test").success());
            return;
        }

        let occupied = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        for option in 0..21 {
            let mut args = serve_args(Some("trust"), Vec::new());
            args.listen = occupied.local_addr().expect("address").to_string();
            set_wal_policy_option(&mut args, option);
            let error = run_serve(args).await.expect_err("invalid recovery policy");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }

        let mut args = substrate_args();
        args.listen = occupied.local_addr().expect("address").to_string();
        args.wal_topic_replication_factor =
            Some(PositiveI32::new(32_768).expect("positive parser value"));
        let error = run_serve(args)
            .await
            .expect_err("invalid replication factor before bind");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        let mut args = substrate_args();
        args.listen = occupied.local_addr().expect("address").to_string();
        args.wal_producer_batch_bytes = Some(0);
        let error = run_serve(args)
            .await
            .expect_err("invalid producer throughput before bind");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    fn set_wal_policy_option(args: &mut ServeArgs, option: usize) {
        match option {
            0 => args.wal_recovery_fetch_max_wait_ms = PositiveI32::new(1).ok(),
            1 => args.wal_recovery_fetch_partition_max_bytes = PositiveI32::new(1).ok(),
            2 => args.wal_recovery_fetch_response_max_bytes = PositiveI32::new(1).ok(),
            3 => args.wal_recovery_empty_fetch_retries = PositiveUsize::new(1).ok(),
            4 => args.wal_recovery_dns_timeout_ms = PositiveMillis::new(1).ok(),
            5 => args.wal_recovery_connect_timeout_ms = PositiveMillis::new(1).ok(),
            6 => args.wal_recovery_request_timeout_ms = PositiveMillis::new(1).ok(),
            7 => args.wal_topic_replication_factor = PositiveI32::new(1).ok(),
            8 => args.wal_topic_ensure_timeout_ms = PositiveI32::new(1).ok(),
            9 => args.wal_admin_connect_timeout_ms = PositiveMillis::new(1).ok(),
            10 => args.wal_admin_request_timeout_ms = PositiveMillis::new(1).ok(),
            11 => args.wal_producer_flush_timeout_ms = PositiveMillis::new(1).ok(),
            12 => args.wal_producer_request_timeout_ms = PositiveMillis::new(1).ok(),
            13 => args.wal_producer_retries = NonNegativeI32::new(0).ok(),
            14 => args.wal_producer_retry_backoff_ms = PositiveMillis::new(1).ok(),
            15 => args.wal_producer_routing_retry_budget_ms = PositiveMillis::new(1).ok(),
            16 => args.wal_producer_init_retry_timeout_ms = PositiveMillis::new(1).ok(),
            17 => args.wal_producer_init_max_backoff_ms = PositiveMillis::new(1).ok(),
            18 => args.wal_producer_transaction_timeout_ms = PositiveMillis::new(1).ok(),
            19 => args.wal_producer_compression = Some(crabka_client_producer::Compression::Gzip),
            20 => args.wal_producer_linger_ms = Some(0),
            21 => args.wal_producer_batch_bytes = Some(1),
            _ => unreachable!("test policy option"),
        }
    }

    #[test]
    fn wal_recovery_read_policy_reaches_shared_recovery_config_helper() {
        let policy = crabka_gres_substrate::RecoveryReadPolicy::new(31, 32, 33, 34)
            .expect("distinctive policy")
            .with_dns_timeout(37)
            .expect("distinctive DNS timeout")
            .with_timeouts(35, 36)
            .expect("distinctive timeouts");
        let mut config = SubstrateRuntimeConfig::from_args(&substrate_args())
            .expect("config")
            .expect("substrate config");
        config.recovery_read_policy = policy;
        let tenant = crabka_gres_ranges::TenantName::parse("tenant-a".to_string()).expect("tenant");

        let recovery =
            config.live_recovery_config(tenant.clone(), crabka_gres_ranges::RangeId::new(7));

        assert_eq!(recovery.read_policy(), policy);
        let admin =
            crabka_gres_substrate::WalAdminPolicy::new(41, 42, 43, 44).expect("distinctive policy");
        config.wal_admin_policy = admin;
        let recovery = config.live_recovery_config(tenant, crabka_gres_ranges::RangeId::new(7));
        assert_eq!(recovery.wal_admin_policy(), admin);
        assert_eq!(
            include_str!("lib.rs")
                .split_once("\n#[cfg(test)]\nmod vacuum_pacing_tests {")
                .expect("test module boundary")
                .0
                .matches("LiveRecoveryConfig::new(")
                .count(),
            1
        );
        assert_eq!(
            include_str!("split_activation.rs")
                .matches("LiveRecoveryConfig::new(")
                .count(),
            0
        );
    }

    #[test]
    fn wal_producer_retry_policy_accepts_distinctive_values_and_reaches_recovery() {
        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-request-timeout-ms=31",
            "--wal-producer-retries=32",
            "--wal-producer-retry-backoff-ms=33",
            "--wal-producer-routing-retry-budget-ms=34",
            "--wal-producer-init-retry-timeout-ms=35",
            "--wal-producer-init-max-backoff-ms=36",
            "--wal-producer-transaction-timeout-ms=37",
        ])
        .expect("WAL producer policy");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        let policy = config.producer_retry_policy;

        assert_eq!(policy.request_timeout(), Duration::from_millis(31));
        assert_eq!(policy.retries(), 32);
        assert_eq!(policy.retry_backoff(), Duration::from_millis(33));
        assert_eq!(policy.routing_retry_budget(), Duration::from_millis(34));
        assert_eq!(policy.init_retry_timeout(), Duration::from_millis(35));
        assert_eq!(policy.init_max_backoff(), Duration::from_millis(36));
        assert_eq!(policy.transaction_timeout(), Duration::from_millis(37));

        let tenant = crabka_gres_ranges::TenantName::parse("tenant-a").expect("tenant");
        assert_eq!(
            config
                .live_recovery_config(tenant, crabka_gres_ranges::RangeId::new(7))
                .producer_retry_policy(),
            policy
        );
    }

    #[test]
    fn wal_producer_flush_timeout_uses_defaults_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_TEST_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_CHILD";
        const ENV: &str = "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS";
        if std::env::var_os(CHILD).is_none() {
            for mode in ["defaults", "environment"] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::wal_producer_flush_timeout_uses_defaults_environment_and_cli_precedence",
                    ])
                    .env(CHILD, mode)
                    .env_remove(ENV);
                if mode == "environment" {
                    child.env(ENV, "41");
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        let base = [
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
        ];
        let config = SubstrateRuntimeConfig::from_args(
            &<Cli as clap::Parser>::try_parse_from(base)
                .expect("flush timeout")
                .serve,
        )
        .expect("valid config")
        .expect("substrate config");
        let expected_ms = if std::env::var(CHILD).as_deref() == Ok("environment") {
            41
        } else {
            50_000
        };
        assert_eq!(
            config.producer_flush_timeout.duration(),
            Duration::from_millis(expected_ms)
        );

        let cli = <Cli as clap::Parser>::try_parse_from(
            base.into_iter()
                .chain(["--wal-producer-flush-timeout-ms=51"]),
        )
        .expect("CLI flush timeout");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        assert_eq!(
            config.producer_flush_timeout.duration(),
            Duration::from_millis(51)
        );

        let tenant = crabka_gres_ranges::TenantName::parse("tenant-a").expect("tenant");
        assert_eq!(
            config
                .live_recovery_config(tenant, crabka_gres_ranges::RangeId::new(7))
                .producer_flush_timeout(),
            config.producer_flush_timeout
        );
    }

    #[test]
    fn wal_producer_flush_timeout_rejects_invalid_and_local_only_use() {
        assert!(
            <Cli as clap::Parser>::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                "--wal-producer-flush-timeout-ms=0",
            ])
            .is_err()
        );
        let oversized = <Cli as clap::Parser>::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-flush-timeout-ms=2147483648",
        ])
        .expect("positive parser value");
        assert!(SubstrateRuntimeConfig::from_args(&oversized.serve).is_err());

        let maximum = <Cli as clap::Parser>::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-flush-timeout-ms=2147483647",
        ])
        .expect("maximum protocol timeout");
        assert_eq!(
            SubstrateRuntimeConfig::from_args(&maximum.serve)
                .expect("valid config")
                .expect("substrate config")
                .producer_flush_timeout
                .milliseconds(),
            2_147_483_647
        );

        let fractional = <Cli as clap::Parser>::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-flush-timeout-ms=1.5",
        ])
        .expect_err("fractional milliseconds");
        assert_eq!(fractional.kind(), clap::error::ErrorKind::ValueValidation);

        assert!(
            <Cli as clap::Parser>::try_parse_from([
                "crabka-gres",
                "--wal-producer-flush-timeout-ms=1",
            ])
            .is_err()
        );
    }

    #[test]
    fn wal_producer_throughput_policy_accepts_distinctive_values_and_reaches_recovery() {
        let cli = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--wal-producer-compression=zstd",
            "--wal-producer-linger-ms=38",
            "--wal-producer-batch-bytes=39",
        ])
        .expect("WAL producer throughput policy");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        let policy = config.producer_throughput_policy;

        assert_eq!(
            policy.compression(),
            crabka_client_producer::Compression::Zstd
        );
        assert_eq!(policy.linger(), Duration::from_millis(38));
        assert_eq!(policy.batch_bytes(), 39);
        assert_eq!(
            policy.max_in_flight(),
            crabka_client_producer::DEFAULT_PRODUCER_MAX_IN_FLIGHT
        );

        let tenant = crabka_gres_ranges::TenantName::parse("tenant-a").expect("tenant");
        assert_eq!(
            config
                .live_recovery_config(tenant, crabka_gres_ranges::RangeId::new(7))
                .producer_throughput_policy(),
            policy
        );
    }

    #[test]
    fn wal_producer_throughput_policy_uses_defaults_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_TEST_GRES_WAL_PRODUCER_THROUGHPUT_POLICY_CHILD";
        const VARS: [&str; 21] = [
            "CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES",
            "CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR",
            "CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_RETRIES",
            "CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_COMPRESSION",
            "CRABKA_GRES_WAL_PRODUCER_LINGER_MS",
            "CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES",
        ];
        if std::env::var_os(CHILD).is_none() {
            for mode in ["defaults", "environment"] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::wal_producer_throughput_policy_uses_defaults_environment_and_cli_precedence",
                    ])
                    .env(CHILD, mode);
                for variable in VARS {
                    child.env_remove(variable);
                }
                if mode == "environment" {
                    for (variable, value) in VARS[18..].iter().copied().zip(["gzip", "41", "42"]) {
                        child.env(variable, value);
                    }
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        let base = [
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
        ];
        let config = SubstrateRuntimeConfig::from_args(
            &<Cli as clap::Parser>::try_parse_from(base)
                .expect("policy")
                .serve,
        )
        .expect("valid config")
        .expect("substrate config");
        let expected = if std::env::var(CHILD).as_deref() == Ok("environment") {
            crabka_client_producer::ProducerThroughputPolicy::new(
                crabka_client_producer::Compression::Gzip,
                Duration::from_millis(41),
                42,
                crabka_client_producer::DEFAULT_PRODUCER_MAX_IN_FLIGHT,
            )
            .expect("environment policy")
        } else {
            crabka_client_producer::ProducerThroughputPolicy::default()
        };
        assert_eq!(config.producer_throughput_policy, expected);

        let cli = <Cli as clap::Parser>::try_parse_from(base.into_iter().chain([
            "--wal-producer-compression=lz4",
            "--wal-producer-linger-ms=51",
            "--wal-producer-batch-bytes=52",
        ]))
        .expect("CLI policy");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        let expected = crabka_client_producer::ProducerThroughputPolicy::new(
            crabka_client_producer::Compression::Lz4,
            Duration::from_millis(51),
            52,
            crabka_client_producer::DEFAULT_PRODUCER_MAX_IN_FLIGHT,
        )
        .expect("CLI policy");
        assert_eq!(config.producer_throughput_policy, expected);
    }

    #[test]
    fn wal_producer_retry_policy_uses_defaults_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_TEST_GRES_WAL_PRODUCER_POLICY_CHILD";
        const VARS: [&str; 21] = [
            "CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES",
            "CRABKA_GRES_WAL_RECOVERY_EMPTY_FETCH_RETRIES",
            "CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR",
            "CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS",
            "CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_REQUEST_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_RETRIES",
            "CRABKA_GRES_WAL_PRODUCER_RETRY_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_ROUTING_RETRY_BUDGET_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_RETRY_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_INIT_MAX_BACKOFF_MS",
            "CRABKA_GRES_WAL_PRODUCER_TRANSACTION_TIMEOUT_MS",
            "CRABKA_GRES_WAL_PRODUCER_COMPRESSION",
            "CRABKA_GRES_WAL_PRODUCER_LINGER_MS",
            "CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES",
        ];
        if std::env::var_os(CHILD).is_none() {
            for mode in ["defaults", "environment"] {
                let mut child =
                    std::process::Command::new(std::env::current_exe().expect("test exe"));
                child
                    .args([
                        "--exact",
                        "tests::wal_producer_retry_policy_uses_defaults_environment_and_cli_precedence",
                    ])
                    .env(CHILD, mode)
                    .env("CRABKA_GRES_WAL_RECOVERY_FETCH_MAX_WAIT_MS", "0");
                for variable in VARS {
                    child.env_remove(variable);
                }
                if mode == "environment" {
                    for (variable, value) in VARS[11..18]
                        .iter()
                        .copied()
                        .zip(["41", "42", "43", "44", "45", "46", "47"])
                    {
                        child.env(variable, value);
                    }
                }
                assert!(child.status().expect("child test").success());
            }
            return;
        }

        let base = [
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
        ];
        let config = SubstrateRuntimeConfig::from_args(
            &<Cli as clap::Parser>::try_parse_from(base)
                .expect("policy")
                .serve,
        )
        .expect("valid config")
        .expect("substrate config");
        let expected = if std::env::var(CHILD).as_deref() == Ok("environment") {
            crabka_client_producer::ProducerRetryPolicy::new(
                Duration::from_millis(41),
                42,
                Duration::from_millis(43),
                Duration::from_millis(44),
                Duration::from_millis(45),
                Duration::from_millis(46),
                Duration::from_millis(47),
            )
            .expect("environment policy")
        } else {
            crabka_client_producer::ProducerRetryPolicy::default()
        };
        assert_eq!(config.producer_retry_policy, expected);

        let cli = <Cli as clap::Parser>::try_parse_from(base.into_iter().chain([
            "--wal-producer-request-timeout-ms=51",
            "--wal-producer-retries=52",
            "--wal-producer-retry-backoff-ms=53",
            "--wal-producer-routing-retry-budget-ms=54",
            "--wal-producer-init-retry-timeout-ms=55",
            "--wal-producer-init-max-backoff-ms=56",
            "--wal-producer-transaction-timeout-ms=57",
        ]))
        .expect("CLI policy");
        let config = SubstrateRuntimeConfig::from_args(&cli.serve)
            .expect("valid config")
            .expect("substrate config");
        let expected = crabka_client_producer::ProducerRetryPolicy::new(
            Duration::from_millis(51),
            52,
            Duration::from_millis(53),
            Duration::from_millis(54),
            Duration::from_millis(55),
            Duration::from_millis(56),
            Duration::from_millis(57),
        )
        .expect("CLI policy");
        assert_eq!(config.producer_retry_policy, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_range0_follower_poll_and_poke_control_refresh() {
        let poke = Arc::new(tokio::sync::Notify::new());
        let periodic_poke = Arc::clone(&poke);
        let periodic = tokio::spawn(async move {
            range0_follower::wait_for_refresh(&periodic_poke, Duration::from_millis(7)).await;
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(6)).await;
        assert!(!periodic.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        periodic.await.expect("periodic wake");

        let notified_poke = Arc::clone(&poke);
        let notified = tokio::spawn(async move {
            range0_follower::wait_for_refresh(&notified_poke, Duration::from_mins(1)).await;
        });
        tokio::task::yield_now().await;
        poke.notify_one();
        notified.await.expect("notification wake");
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
        for option in [
            "--checkpoint-frames=10",
            "--checkpoint-delete-records-timeout-ms=25",
            "--checkpoint-poll-interval-ms=26",
            "--idle-suspend-poll-interval-ms=27",
        ] {
            let args = Cli::try_parse_from([
                "crabka-gres",
                "--substrate-bootstrap=memory://",
                "--tenant=tenant-a",
                option,
            ])
            .expect("checkpoint option")
            .serve;
            let error = SubstrateRuntimeConfig::from_args(&args).expect_err("missing object store");

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("checkpoint thresholds require"));
        }
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
    fn checkpoint_lifecycle_cli_options_require_substrate_mode() {
        for option in [
            "--checkpoint-delete-records-timeout-ms=25",
            "--checkpoint-poll-interval-ms=26",
            "--idle-suspend-poll-interval-ms=27",
        ] {
            let args = Cli::try_parse_from(["crabka-gres", option])
                .expect("checkpoint lifecycle option")
                .serve;
            let error = SubstrateRuntimeConfig::from_args(&args).expect_err("substrate required");

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(
                error.to_string(),
                "checkpoint options require --substrate-bootstrap"
            );
        }
    }

    #[test]
    fn checkpoint_lifecycle_environment_options_require_substrate_mode() {
        const CHILD: &str = "CRABKA_TEST_GRES_CHECKPOINT_REQUIRED_ENV_CHILD";
        const VARIABLES: [&str; 3] = [
            "CRABKA_GRES_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS",
            "CRABKA_GRES_CHECKPOINT_POLL_INTERVAL_MS",
            "CRABKA_GRES_IDLE_SUSPEND_POLL_INTERVAL_MS",
        ];

        if let Ok(variable) = std::env::var(CHILD) {
            let args = Cli::try_parse_from(["crabka-gres"])
                .expect("checkpoint lifecycle environment option")
                .serve;
            let error = SubstrateRuntimeConfig::from_args(&args).expect_err("substrate required");

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{variable}");
            assert_eq!(
                error.to_string(),
                "checkpoint options require --substrate-bootstrap",
                "{variable}"
            );
            return;
        }

        for variable in VARIABLES {
            let mut command =
                std::process::Command::new(std::env::current_exe().expect("test executable"));
            command
                .args([
                    "--exact",
                    "tests::checkpoint_lifecycle_environment_options_require_substrate_mode",
                ])
                .env(CHILD, variable);
            for other in VARIABLES {
                command.env_remove(other);
            }
            command.env(variable, "25");
            assert!(
                command.status().expect("child test").success(),
                "{variable}"
            );
        }
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
            range0_follower_poll_interval: Duration::from_millis(
                DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
            ),
            recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy::default(),
            wal_admin_policy: crabka_gres_substrate::WalAdminPolicy::default(),
            producer_flush_timeout: crabka_client_producer::ProducerFlushTimeout::default(),
            producer_retry_policy: crabka_client_producer::ProducerRetryPolicy::default(),
            producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy::default(),
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
            timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            hlc_wall_offset_ms: 0,
            registry_policy: RegistryPolicy::default(),
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
                delete_records_timeout_ms: 30_000,
                poll_interval: Duration::from_secs(1),
            }),
            kafka_security: None,
            ranges: None,
            range0_follower_poll_interval: Duration::from_millis(
                DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
            ),
            recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy::default(),
            wal_admin_policy: crabka_gres_substrate::WalAdminPolicy::default(),
            producer_flush_timeout: crabka_client_producer::ProducerFlushTimeout::default(),
            producer_retry_policy: crabka_client_producer::ProducerRetryPolicy::default(),
            producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy::default(),
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
            timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            hlc_wall_offset_ms: 0,
            registry_policy: RegistryPolicy::default(),
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
            |timeout_ms| Ok(GresCheckpointWalPruner::in_memory(timeout_ms)),
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

        let recovery_configs = live_multirange_recovery_configs(&config, &tenant_config, None);
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
    fn live_multirange_recovery_recovers_range_zero_first() {
        use assert2::assert;

        let mut args = substrate_args();
        args.substrate_bootstrap = Some("broker-a:9092".to_string());
        args.ranges = Some("0,100".to_string());
        let config = SubstrateRuntimeConfig::from_args(&args)
            .expect("valid config")
            .expect("substrate config");
        let tenant = crabka_gres_ranges::TenantName::parse(config.tenant.clone()).expect("tenant");
        let mut tenant_config = crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(
            tenant,
            config.ranges.as_deref().expect("ranges"),
        )
        .expect("range config");
        // Post-split maps order specs by key span, not range id: put the
        // coordinator behind a sibling to prove recovery still runs it first.
        let mut specs = tenant_config.range_map.ranges().to_vec();
        specs[0].range_id = crabka_gres_ranges::RangeId::new(7);
        specs[1].range_id = crabka_gres_ranges::RangeId::COORDINATOR;
        tenant_config.range_map = crabka_gres_ranges::RangeMap::new(
            tenant_config.tenant.clone(),
            crabka_gres_ranges::MapEpoch::new(1),
            specs,
        )
        .expect("reordered map");

        let recovery_ranges = live_multirange_recovery_configs(&config, &tenant_config, None)
            .iter()
            .map(|recovery| recovery.range)
            .collect::<Vec<_>>();

        assert!(
            recovery_ranges
                == [
                    crabka_gres_ranges::RangeId::COORDINATOR,
                    crabka_gres_ranges::RangeId::new(7),
                ]
        );
    }

    #[tokio::test]
    async fn early_range_transport_serves_grants_before_topology_swap() {
        use assert2::assert;
        use crabka_gres_ranges::{
            RangeId, RangeRequest, RangeResponse, RangeService, TsoReq, TsoResp, WireErrorKind,
        };

        let dynamic = DynamicLiveRangeService::new(crabka_gres_ranges::HostedRangeService::new(
            BTreeMap::new(),
        ));

        // Warming: every request answers a re-resolvable error.
        let warming_grant = dynamic
            .handle(RangeRequest::Tso(TsoReq::Grant { count: 1 }))
            .await;
        assert!(
            warming_grant
                == RangeResponse::Error {
                    error: WireErrorKind::StaleEndpoint,
                    message: "range r0 timestamp oracle is not hosted here".to_string(),
                }
        );

        // Range-0 recovery installs the oracle: grants serve, SQL stays gated.
        let horizon =
            crabka_gres_ranges::MemoryTsoHorizon::new(Arc::new(crabka_pgkv::MemKv::default()), 1);
        let tso_rpc = crabka_gres_ranges::tso_rpc_from_horizon(horizon.clone(), horizon, 1, 0)
            .expect("warming tso rpc");
        dynamic.replace(
            crabka_gres_ranges::HostedRangeService::new(BTreeMap::new()).with_tso(tso_rpc),
        );

        let granted = dynamic
            .handle(RangeRequest::Tso(TsoReq::Grant { count: 3 }))
            .await;
        assert!(
            granted
                == RangeResponse::Tso(TsoResp::Granted {
                    first_ts: 1,
                    count: 3
                })
        );

        let gated_sql = dynamic
            .handle(RangeRequest::Sql {
                range_id: RangeId::new(1),
                sql: "SELECT 1".to_string(),
            })
            .await;
        assert!(
            gated_sql
                == RangeResponse::Error {
                    error: WireErrorKind::StaleEndpoint,
                    message: "range r1 is not hosted here".to_string(),
                }
        );
    }

    fn spawn_guarded_listener(listener: TcpListener, address: SocketAddr) -> EarlyRangeServer {
        let handle = tokio::spawn(async move {
            loop {
                let _connection = listener.accept().await;
            }
        });
        EarlyRangeServer {
            server: Some((handle, address)),
        }
    }

    #[tokio::test]
    async fn early_range_server_guard_aborts_the_listener_on_drop() {
        use assert2::assert;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let guard = spawn_guarded_listener(listener, address);

        drop(guard);

        // The abort releases the listener: the port becomes bindable again.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match TcpListener::bind(address).await {
                Ok(_rebound) => break,
                Err(error) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "early listener still bound after guard drop: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn early_range_server_release_keeps_the_listener_serving() {
        use assert2::assert;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let guard = spawn_guarded_listener(listener, address);

        let (handle, released_address) = guard.release();

        assert!(released_address == address);
        assert!(!handle.is_finished());
        // The released task still owns the listener, so the port stays bound.
        assert!(TcpListener::bind(address).await.is_err());
        handle.abort();
    }

    #[test]
    fn deferred_hosted_ranges_validate_only_against_selected_recovery_map() {
        let tenant = crabka_gres_ranges::TenantName::parse("deferred-hosts").expect("tenant");
        let current =
            crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(tenant, "0:0,50:10")
                .expect("current map");
        let requested = [
            crabka_gres_ranges::RangeId::COORDINATOR,
            crabka_gres_ranges::RangeId::new(2),
        ];
        assert!(bind_recovered_hosted_ranges(current.clone(), &requested).is_err());

        let mut target = current;
        let mut specs = target.range_map.ranges().to_vec();
        specs[1].range_id = crabka_gres_ranges::RangeId::new(2);
        target.range_map = crabka_gres_ranges::RangeMap::new(
            target.tenant.clone(),
            crabka_gres_ranges::MapEpoch::new(1),
            specs,
        )
        .expect("target map");
        let bound = bind_recovered_hosted_ranges(target, &requested).expect("target hosts");
        assert_eq!(bound.hosted_ranges.as_deref(), Some(requested.as_slice()));
        assert!(
            bind_recovered_hosted_ranges(bound, &[crabka_gres_ranges::RangeId::new(9)]).is_err()
        );
    }

    #[test]
    fn registry_layout_preserves_hash_bucket_boundary_in_initial_serving_map() {
        let tenant = crabka_gres_ranges::TenantName::parse("hash-layout").expect("tenant");
        let layout = vec![
            crabka_gres_control::RangeLayoutEntry {
                range_id: 0,
                end_key: Some(crabka_gres_control::RangeBoundary::hash(50, 4, 0)),
                endpoint: "127.0.0.1:1".into(),
                wal_generation: 0,
                lifecycle: crabka_gres_control::RangeLifecycle::default(),
                retirement: None,
            },
            crabka_gres_control::RangeLayoutEntry {
                range_id: 1,
                end_key: None,
                endpoint: "127.0.0.1:2".into(),
                wal_generation: 0,
                lifecycle: crabka_gres_control::RangeLifecycle::default(),
                retirement: None,
            },
        ];

        let map =
            range_map_from_tenant_layout(tenant, crabka_gres_ranges::MapEpoch::new(7), &layout)
                .expect("registry map");

        assert_eq!(map.epoch().as_u64(), 7);
        assert_eq!(
            map.ranges()[0].end,
            Some(crabka_gres_ranges::RangeKey::hash(
                crabka_gres_ranges::TableId::new(50),
                4,
                0,
            ))
        );
        assert_eq!(map.ranges()[1].start, map.ranges()[0].end.unwrap());
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
            range0_follower_poll_interval: Duration::from_millis(
                DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
            ),
            recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy::default(),
            wal_admin_policy: crabka_gres_substrate::WalAdminPolicy::default(),
            producer_flush_timeout: crabka_client_producer::ProducerFlushTimeout::default(),
            producer_retry_policy: crabka_client_producer::ProducerRetryPolicy::default(),
            producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy::default(),
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
            timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            hlc_wall_offset_ms: 0,
            registry_policy: RegistryPolicy::default(),
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
            range0_follower_poll_interval: Duration::from_millis(
                DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
            ),
            recovery_read_policy: crabka_gres_substrate::RecoveryReadPolicy::default(),
            wal_admin_policy: crabka_gres_substrate::WalAdminPolicy::default(),
            producer_flush_timeout: crabka_client_producer::ProducerFlushTimeout::default(),
            producer_retry_policy: crabka_client_producer::ProducerRetryPolicy::default(),
            producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy::default(),
            host_ranges: None,
            range_rpc: None,
            advertised_endpoint: None,
            timestamp_source_mode: crabka_gres_ranges::TimestampSourceMode::LogicalTso,
            hlc_wall_offset_ms: 0,
            registry_policy: RegistryPolicy::default(),
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
    async fn runtime_session_forwards_the_notification_stream_with_its_pid() {
        use assert2::assert;

        let mut session = RuntimeEngine::Single(Box::new(SqlEngine::new())).connect_with_pid(4242);
        let mut notifications = session
            .take_notifications()
            .expect("the single-engine session hands over its notification stream");

        session.simple_query("LISTEN news").await.expect("listen");
        session
            .simple_query("NOTIFY news, 'hello'")
            .await
            .expect("notify");

        assert!(
            notifications.try_recv()
                == Ok(Notification {
                    process_id: 4242,
                    channel: "news".to_string(),
                    payload: "hello".to_string(),
                })
        );
        // The receiver is handed over once: the wire loop owns it from then on.
        assert!(session.take_notifications().is_none());
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
        let recovery = crabka_gres_ranges::RangeRequest::TimestampPrimaryRecover(
            crabka_gres_ranges::transport::TimestampPrimaryRecoverReq {
                primary_range: crabka_gres_ranges::RangeId::COORDINATOR,
                identity: crabka_gres_ranges::transport::WireTimestampIdentity {
                    start_ts: 1,
                    global_xid: 1,
                    primary_range: 0,
                },
            },
        );
        assert!(!matches!(
            crabka_gres_ranges::RangeService::handle(&dynamic, recovery).await,
            crabka_gres_ranges::RangeResponse::Error { message, .. }
                if message == "range topology publication is in progress; retry"
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

    #[tokio::test]
    async fn dynamic_range_service_preserves_control_across_inner_replacement() {
        struct StatusExecutor;

        #[async_trait::async_trait]
        impl crabka_gres_ranges::control::RangeControlExecutor for StatusExecutor {
            async fn execute(
                &self,
                _request: &crabka_gres_ranges::transport::RangeControlReq,
                _intent: &crabka_gres_ranges::control::AuthorizedSplitIntent,
            ) -> crabka_gres_ranges::transport::RangeControlResp {
                crabka_gres_ranges::transport::RangeControlResp::Status {
                    paused: false,
                    serving: true,
                    barrier_offset: None,
                }
            }
        }

        let control = Arc::new(
            crabka_gres_ranges::control::GenerationFencedRangeControl::new(
                "tenant-a",
                crabka_gres_ranges::RangeId::COORDINATOR,
                0,
                Box::new(StatusExecutor),
                Arc::new(AllowSplitIntentAuthority),
            ),
        );
        let dynamic = DynamicLiveRangeService::new(
            crabka_gres_ranges::HostedRangeService::new(BTreeMap::new())
                .with_range_control(control),
        );
        let request = crabka_gres_ranges::RangeRequest::Control(
            crabka_gres_ranges::transport::RangeControlReq {
                tenant: "tenant-a".into(),
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                generation: 0,
                operation_id: "status-after-replace".into(),
                operation: crabka_gres_ranges::transport::RangeControlOperation::Status,
            },
        );
        assert!(matches!(
            crabka_gres_ranges::RangeService::handle(&dynamic, request.clone()).await,
            crabka_gres_ranges::RangeResponse::Control(
                crabka_gres_ranges::transport::RangeControlResp::Status { serving: true, .. }
            )
        ));

        dynamic.begin_publication();
        assert!(matches!(
            crabka_gres_ranges::RangeService::handle(&dynamic, request.clone()).await,
            crabka_gres_ranges::RangeResponse::Control(
                crabka_gres_ranges::transport::RangeControlResp::Status { serving: true, .. }
            )
        ));

        dynamic.replace(crabka_gres_ranges::HostedRangeService::new(BTreeMap::from(
            [(crabka_gres_ranges::RangeId::COORDINATOR, SqlEngine::new())],
        )));

        assert!(matches!(
            crabka_gres_ranges::RangeService::handle(&dynamic, request).await,
            crabka_gres_ranges::RangeResponse::Control(
                crabka_gres_ranges::transport::RangeControlResp::Status { serving: true, .. }
            )
        ));
        dynamic.finish_publication();
    }

    #[test]
    fn durable_inspection_cursor_binds_digest_sample_and_exact_key() {
        let key = crabka_pgkv::key::hash_row_key(50, 15, u64::MAX);
        let cursor = encode_durable_cursor("digest-a", 42, &key);
        assert_eq!(
            decode_durable_cursor(&cursor, "digest-a").expect("cursor"),
            (42, key)
        );
        assert!(decode_durable_cursor(&cursor, "digest-b").is_err());
        assert!(decode_durable_cursor("digest-a:42:0", "digest-a").is_err());
    }

    #[test]
    fn durable_inspection_rejects_malformed_timestamp_metadata() {
        let start = crabka_pgkv::key::row_key(50, 0);
        let end = crabka_pgkv::key::row_key(50, 10);
        assert!(
            timestamp_metadata_in_interval(b"\0\0\0\0meta/ts_intent/bad", b"", 50, &start, &end,)
                .is_err()
        );
    }
}
