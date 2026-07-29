#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use clap::{Parser, ValueEnum};
use crabka_blockstore::{IndexSnapshotRetain, ProfileIndex};
use crabka_client_consumer::ConsumerFetchMaxBytes;
use crabka_client_producer::Producer;
use crabka_pprof::UnionProfileStore;
use crabka_profiles::{
    blockbuilder::BlockBuilderConfig,
    cold_store::ColdProfileStore,
    compactor::{DownsamplePolicy, compact_once_with_policy},
    distributor::{DistributorState, KafkaSink, serve},
    hot_store::{WalTailProfileStore, run_wal_tail},
    ingest::{RelabelConfig, TenantLimitConfig},
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query::{QuerierState, serve as serve_querier},
    query_frontend::FrontendConfig,
};
use crabka_telemetry::OtlpConfig;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, parse, secs,
};
use object_store::{ObjectStore, path::Path as ObjectPath};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:4040")]
    listen: SocketAddr,
    #[arg(long, env = "CRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    admin_listen_addr: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_WAL_FETCH_MAX",
        default_value = "2MiB",
        value_parser = parse_consumer_fetch_size
    )]
    wal_fetch_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX",
        default_value = "256KiB",
        value_parser = parse_consumer_fetch_size
    )]
    wal_fetch_partition_max: ByteSize,
    #[arg(long, default_value = "file://./.crabka-profiles-blocks")]
    object_store_url: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_INDEX_SNAPSHOT_MAX",
        default_value = "256MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    index_snapshot_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN",
        default_value_t = IndexSnapshotRetain::default()
    )]
    index_snapshot_retain: IndexSnapshotRetain,
    #[arg(
        long,
        env = "CRABKA_PROFILES_WAL_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    wal_poll_timeout: Time,
    // Same deployment contract as the flush-max-age flag: the name and the
    // millisecond integer stay, and the value is lifted into a `Time` where it
    // enters the frontend config.
    #[arg(long, default_value_t = FrontendConfig::default().shard_width.millis_i64())]
    query_frontend_shard_ms: i64,
    #[arg(long)]
    tenant_limits_config: Option<std::path::PathBuf>,
    #[arg(long)]
    profiles_limits_overrides_config: Option<std::path::PathBuf>,
    #[arg(long, default_value = "crabka-profiles-query-wal-tail")]
    query_wal_tail_group_id: String,
    #[arg(long, default_value_t = 8)]
    compactor_max_blocks_per_job: usize,
    #[arg(long)]
    compactor_downsample_resolution_ns: Option<i64>,
    #[arg(long, default_value_t = crabka_profiles::blockbuilder::DEFAULT_FLUSH_RECORDS)]
    block_builder_flush_records: usize,
    // The flag name and its millisecond integer encoding are the deployment
    // contract (`demo/observability/docker-compose.yml` passes it), so the raw
    // value is held here and lifted into a `Time` where it enters the config.
    #[arg(long, default_value_t = crabka_profiles::blockbuilder::DEFAULT_FLUSH_MAX_AGE.millis_i64())]
    block_builder_flush_max_age_ms: i64,
    /// debuginfod base URLs (comma-separated) to fetch DWARF for unsymbolized
    /// native frames. Empty by default: the symbolizer makes NO outbound
    /// requests unless an operator explicitly opts in by supplying URLs.
    #[arg(long = "debuginfod-url", value_delimiter = ',')]
    debuginfod_urls: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Target {
    Distributor,
    BlockBuilder,
    Querier,
    QueryFrontend,
    Compactor,
    Symbolizer,
}

struct ConfiguredObjectStore {
    store: std::sync::Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

fn parse_consumer_fetch_size(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ConsumerFetchMaxBytes::try_from(size)?;
    Ok(size)
}

fn parse_positive_whole_byte_size(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}

impl ConfiguredObjectStore {
    fn object_key(&self, key: &str) -> String {
        let prefix = self.prefix.as_ref().trim_matches('/');
        let key = key.trim_start_matches('/');
        if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{prefix}/{key}")
        }
    }
}

fn build_object_store(
    url: &str,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(ConfiguredObjectStore {
        store: std::sync::Arc::from(store),
        prefix,
    })
}

/// How often the querier reloads the profile block index from object storage.
const INDEX_REFRESH_INTERVAL: Time = secs(15);

/// How long each hot WAL-tail poll waits for records.
/// Periodically reload the profile block index from object storage and swap it
/// into the cold store. The block-builder writes new blocks continuously; without
/// this the querier only ever sees the index snapshot it loaded at boot, so blocks
/// created afterwards are invisible (manifests as recent profiles — especially
/// sparse ones like memory that age out of the hot tier — returning empty). Mirrors
/// the traces querier's `TraceIndex` refresh loop.
fn spawn_profile_index_refresh(
    cold: Arc<ColdProfileStore>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    max_bytes: ByteSize,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(INDEX_REFRESH_INTERVAL.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if let Ok(index) =
                ProfileIndex::load_latest_snapshot_with_max_bytes(&store, &index_key, max_bytes)
                    .await
            {
                cold.replace_index(Arc::new(index));
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-profiles",
            env!("CARGO_PKG_VERSION"),
            "crabka-profiles",
        )?,
        "crabka_profiles=info,info",
        "info",
        "crabka-profiles",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin(
        cli.admin_listen_addr,
        crabka_profiles::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    match cli.target {
        Target::Distributor => {
            let limits = load_tenant_limits_config(cli.tenant_limits_config.as_deref())?;
            let profile_overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let producer = Producer::builder()
                .bootstrap(&cli.bootstrap)
                .build()
                .await?;
            let state = Arc::new(DistributorState {
                sink: Arc::new(KafkaSink::new(Arc::new(producer))),
                limits,
                profile_overrides,
                active_series: Mutex::default(),
                ingestion_buckets: Mutex::default(),
                relabel: Vec::<RelabelConfig>::new(),
                max_decompressed: mebibytes(16),
                metrics: metrics.clone(),
            });
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve(cli.listen, state, shutdown).await?;
            tracing::info!(%bound, "profiles distributor listening");
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::BlockBuilder => {
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let mut config =
                BlockBuilderConfig::new(cli.bootstrap, configured.store).with_metrics(metrics);
            config.wal_fetch_max = cli.wal_fetch_max;
            config.wal_fetch_partition_max = cli.wal_fetch_partition_max;
            config.flush_records = cli.block_builder_flush_records;
            config.flush_max_age = Time::from_millis(cli.block_builder_flush_max_age_ms);
            config.poll_timeout = cli.wal_poll_timeout;
            config.index_snapshot_max = cli.index_snapshot_max;
            config.index_snapshot_retain = cli.index_snapshot_retain;
            crabka_profiles::blockbuilder::run_with_config(config).await?;
        }
        Target::Querier => {
            let overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let index = ProfileIndex::load_latest_snapshot_with_max_bytes(
                &configured.store,
                &index_key,
                cli.index_snapshot_max,
            )
            .await
            .unwrap_or_else(|_| ProfileIndex::new());
            let refresh_store = Arc::clone(&configured.store);
            let cold = Arc::new(ColdProfileStore::new_with_debuginfod_urls(
                configured.store,
                Arc::new(index),
                cli.debuginfod_urls.clone(),
            )?);
            spawn_profile_index_refresh(
                Arc::clone(&cold),
                refresh_store,
                index_key.clone(),
                cli.index_snapshot_max,
            );
            let hot = WalTailProfileStore::new();
            spawn_wal_tail(&cli, hot.clone());
            let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
            let state = Arc::new(
                QuerierState::new_with_overrides(union, overrides).with_metrics(metrics.clone()),
            );
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve_querier(cli.listen, state, shutdown).await?;
            tracing::info!(%bound, "profiles querier listening");
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::QueryFrontend => {
            let overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let index = ProfileIndex::load_latest_snapshot_with_max_bytes(
                &configured.store,
                &index_key,
                cli.index_snapshot_max,
            )
            .await
            .unwrap_or_else(|_| ProfileIndex::new());
            let refresh_store = Arc::clone(&configured.store);
            let cold = Arc::new(ColdProfileStore::new_with_debuginfod_urls(
                configured.store,
                Arc::new(index),
                cli.debuginfod_urls.clone(),
            )?);
            spawn_profile_index_refresh(
                Arc::clone(&cold),
                refresh_store,
                index_key.clone(),
                cli.index_snapshot_max,
            );
            let hot = WalTailProfileStore::new();
            spawn_wal_tail(&cli, hot.clone());
            let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
            let state = Arc::new(
                QuerierState::new_frontend_with_overrides(
                    union,
                    FrontendConfig {
                        shard_width: Time::from_millis(cli.query_frontend_shard_ms),
                    },
                    overrides,
                )
                .with_metrics(metrics.clone()),
            );
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve_querier(cli.listen, state, shutdown).await?;
            tracing::info!(
                %bound,
                shard_width_ms = cli.query_frontend_shard_ms,
                "profiles query-frontend listening"
            );
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::Symbolizer => {
            crabka_profiles::symbolizer::run(cli.debuginfod_urls).await?;
        }
        Target::Compactor => {
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let mut index = ProfileIndex::load_latest_snapshot_with_max_bytes(
                &configured.store,
                &index_key,
                cli.index_snapshot_max,
            )
            .await
            .unwrap_or_else(|_| ProfileIndex::new());
            let downsample = cli
                .compactor_downsample_resolution_ns
                .map(|resolution_ns| DownsamplePolicy { resolution_ns });
            let metas = compact_once_with_policy(
                &configured.store,
                &mut index,
                cli.compactor_max_blocks_per_job,
                downsample,
            )
            .await?;
            index
                .save_latest_snapshot_with_retain(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_retain,
                )
                .await?;
            tracing::info!(
                compacted_blocks = metas.len(),
                downsample_resolution_ns = ?cli.compactor_downsample_resolution_ns,
                "profiles compactor finished one pass"
            );
        }
    }

    Ok(())
}

fn load_tenant_limits_config(
    path: Option<&Path>,
) -> Result<TenantLimitConfig, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(TenantLimitConfig::default());
    };
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn load_profiles_limits_overrides_config(
    path: Option<&Path>,
) -> Result<OverridesProvider, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(OverridesProvider::new(Limits::default()));
    };
    let text = std::fs::read_to_string(path)?;
    Ok(OverridesProvider::from_yaml(&text)?)
}

fn spawn_wal_tail(cli: &Cli, hot: WalTailProfileStore) {
    let bootstrap = cli.bootstrap.clone();
    let group_id = cli.query_wal_tail_group_id.clone();
    let poll_timeout = cli.wal_poll_timeout;
    tokio::spawn(async move {
        if let Err(err) = run_wal_tail(hot, bootstrap, group_id, poll_timeout).await {
            tracing::warn!(%err, "profiles hot WAL-tail stopped");
        }
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser;
    use crabka_units::{bytes, per_sec};

    use super::*;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();

        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn parses_block_builder_flush_options() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "block-builder",
            "--block-builder-flush-records",
            "4096",
            "--block-builder-flush-max-age-ms",
            "60000",
        ])
        .unwrap();

        assert!(cli.block_builder_flush_records == 4096);
        assert!(cli.block_builder_flush_max_age_ms == 60_000);
    }

    #[test]
    fn index_snapshot_policy_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(
            cli.index_snapshot_max,
            crabka_blockstore::DEFAULT_INDEX_SNAPSHOT_MAX
        );
        assert_eq!(
            cli.index_snapshot_retain.into_value(),
            crabka_blockstore::DEFAULT_INDEX_SNAPSHOT_RETAIN
        );

        for flag in ["--index-snapshot-max", "--index-snapshot-retain"] {
            for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
                assert!(
                    Cli::try_parse_from([
                        "crabka-profiles",
                        "--target",
                        "block-builder",
                        flag,
                        invalid,
                    ])
                    .is_err(),
                    "{flag} should reject {invalid:?}"
                );
            }
        }
        for invalid in ["1.5B", "18446744073709551616B"] {
            assert!(
                Cli::try_parse_from([
                    "crabka-profiles",
                    "--target",
                    "block-builder",
                    "--index-snapshot-max",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn index_snapshot_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_PROFILES_INDEX_SNAPSHOT_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::index_snapshot_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_PROFILES_INDEX_SNAPSHOT_MAX", "1KiB")
                    .env("CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN", "3")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.index_snapshot_max.bytes_u64(), 1024);
        assert_eq!(from_env.index_snapshot_retain.into_value(), 3);

        let from_cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "block-builder",
            "--index-snapshot-max",
            "2KiB",
            "--index-snapshot-retain",
            "4",
        ])
        .unwrap();
        assert_eq!(from_cli.index_snapshot_max.bytes_u64(), 2048);
        assert_eq!(from_cli.index_snapshot_retain.into_value(), 4);
    }

    #[test]
    fn wal_fetch_limits_preserve_defaults_and_reject_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(cli.wal_fetch_max.bytes_i32(), 2_097_152);
        assert_eq!(cli.wal_fetch_partition_max.bytes_i32(), 262_144);

        for (flag, invalid) in [
            ("--wal-fetch-max", "0"),
            ("--wal-fetch-max", "not-a-number"),
            ("--wal-fetch-max", "-1B"),
            ("--wal-fetch-max", "1.5B"),
            ("--wal-fetch-max", "2147483648B"),
            ("--wal-fetch-partition-max", "0"),
            ("--wal-fetch-partition-max", "not-a-number"),
            ("--wal-fetch-partition-max", "-1B"),
            ("--wal-fetch-partition-max", "1.5B"),
            ("--wal-fetch-partition-max", "2147483648B"),
        ] {
            assert!(
                Cli::try_parse_from([
                    "crabka-profiles",
                    "--target",
                    "block-builder",
                    flag,
                    invalid,
                ])
                .is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn wal_fetch_limits_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_PROFILES_WAL_FETCH_LIMITS_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_fetch_limits_read_environment_and_prefer_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_PROFILES_WAL_FETCH_MAX", "1KiB")
                    .env("CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX", "256B")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_fetch_max.bytes_i32(), 1024);
        assert_eq!(from_env.wal_fetch_partition_max.bytes_i32(), 256);

        let from_cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "block-builder",
            "--wal-fetch-max",
            "2KiB",
            "--wal-fetch-partition-max",
            "512B",
        ])
        .unwrap();
        assert_eq!(from_cli.wal_fetch_max.bytes_i32(), 2048);
        assert_eq!(from_cli.wal_fetch_partition_max.bytes_i32(), 512);
    }

    #[test]
    fn wal_poll_timeout_preserves_default_and_accepts_units() {
        let defaults =
            Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(defaults.wal_poll_timeout, crabka_units::millis(500));

        let overridden = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--wal-poll-timeout",
            "2s",
        ])
        .unwrap();
        assert_eq!(overridden.wal_poll_timeout, crabka_units::secs(2));

        for invalid in ["0", "1", "1KiB"] {
            assert!(
                Cli::try_parse_from([
                    "crabka-profiles",
                    "--target",
                    "query-frontend",
                    "--wal-poll-timeout",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn wal_poll_timeout_reads_environment_and_cli_wins() {
        const CHILD: &str = "CRABKA_PROFILES_WAL_POLL_TIMEOUT_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_poll_timeout_reads_environment_and_cli_wins",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_PROFILES_WAL_POLL_TIMEOUT", "750ms")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_poll_timeout, crabka_units::millis(750));

        let from_cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "query-frontend",
            "--wal-poll-timeout",
            "1s",
        ])
        .unwrap();
        assert_eq!(from_cli.wal_poll_timeout, crabka_units::secs(1));
    }

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();

        assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_shard_width() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "query-frontend",
            "--query-frontend-shard-ms",
            "30000",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
        assert!(cli.query_frontend_shard_ms == 30_000);
    }

    #[test]
    fn parses_query_wal_tail_group_id() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--query-wal-tail-group-id",
            "profiles-tail-a",
        ])
        .unwrap();

        assert!(cli.query_wal_tail_group_id == "profiles-tail-a");
    }

    #[test]
    fn parses_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "query-frontend",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_distributor_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "distributor",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_compactor_max_blocks_per_job() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "compactor",
            "--compactor-max-blocks-per-job",
            "3",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        assert!(cli.compactor_max_blocks_per_job == 3);
    }

    #[test]
    fn parses_compactor_downsample_resolution() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "compactor",
            "--compactor-downsample-resolution-ns",
            "60000000000",
        ])
        .unwrap();

        assert!(cli.compactor_downsample_resolution_ns == Some(60_000_000_000));
    }

    #[test]
    fn debuginfod_urls_default_is_empty() {
        // Security default: no outbound debuginfod egress unless the operator
        // explicitly opts in. The list must be empty when the flag is absent.
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();

        assert!(cli.debuginfod_urls.is_empty());
    }

    #[test]
    fn parses_debuginfod_urls() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--debuginfod-url",
            "http://one.example,http://two.example",
        ])
        .unwrap();

        assert!(
            cli.debuginfod_urls
                == vec![
                    "http://one.example".to_string(),
                    "http://two.example".to_string()
                ]
        );
    }

    #[test]
    fn loads_tenant_limits_config_from_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("limits.json");
        std::fs::write(
            &path,
            r#"{
              "default": {
                "max_label_names_per_series": 10,
                "max_label_value": "100B",
                "session_id_buckets": 32
              },
              "tenants": {
                "tenant-a": {
                  "max_label_names_per_series": 2,
                  "max_label_value": "3B",
                  "session_id_buckets": 4
                }
              }
            }"#,
        )
        .unwrap();

        let config = load_tenant_limits_config(Some(&path)).unwrap();

        assert!(config.default.max_label_names_per_series == 10);
        assert!(config.for_tenant("tenant-a").max_label_value == bytes(3));
    }

    #[test]
    fn loads_profiles_limits_overrides_config_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overrides.yaml");
        std::fs::write(
            &path,
            r"
overrides:
  tenant-a:
    max_query_length_secs: 30
    max_flamegraph_nodes_max: 512
",
        )
        .unwrap();

        let overrides = load_profiles_limits_overrides_config(Some(&path)).unwrap();

        assert!(
            *overrides.for_tenant("tenant-a")
                == crabka_profiles::limits::Limits {
                    ingestion_rate: per_sec(10_000),
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 512,
                    max_query_length: secs(30),
                    max_session_id_cardinality: 0,
                }
        );
        // An unlisted tenant inherits the process default query-length cap.
        check!(
            overrides.for_tenant("tenant-b").max_query_length
                == crabka_profiles::limits::DEFAULT_MAX_QUERY_LENGTH
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-profiles", "--target", "bogus"]).is_err());
    }
}
