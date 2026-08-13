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
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_client_producer::Producer;
use crabka_pprof::{DebuginfodConfig, UnionProfileStore};
use crabka_profiles::{
    blockbuilder::BlockBuilderConfig,
    cold_store::ColdProfileStore,
    compactor::{DownsamplePolicy, compact_once_with_policy},
    distributor::{DistributorState, KafkaSink, serve_supervised},
    hot_store::{RetentionConfig, WalTailProfileStore},
    ingest::{RelabelConfig, TenantLimitConfig},
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query::{QuerierState, serve_supervised as serve_querier},
    query_frontend::FrontendConfig,
};
use crabka_telemetry::OtlpConfig;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    fmt::Human as _,
    parse,
};
#[cfg(test)]
use crabka_units::{mebibytes, secs};
use object_store::{ObjectStore, path::Path as ObjectPath};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "CRABKA_PROFILES_TARGET")]
    target: Target,
    #[arg(
        long,
        env = "CRABKA_PROFILES_LISTEN_ADDR",
        default_value = "127.0.0.1:4040"
    )]
    listen: SocketAddr,
    #[arg(long, env = "CRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "CRABKA_PROFILES_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    bootstrap: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_WAL_TOPIC",
        default_value = crabka_profiles::PROFILES_WAL_TOPIC,
        value_parser = parse_non_empty_string
    )]
    wal_topic: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_BLOCK_BUILDER_GROUP_ID",
        default_value = "crabka-profiles-block-builder",
        value_parser = parse_non_empty_string
    )]
    block_builder_group_id: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_DISTRIBUTOR_REQUEST_MAX",
        default_value = "16MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    distributor_request_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_DISTRIBUTOR_MAX_TRACKED_TENANTS",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    distributor_max_tracked_tenants: usize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_LEGACY_MAX_NODES",
        default_value_t = 500_000,
        value_parser = parse_positive_usize
    )]
    legacy_max_nodes: usize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_LEGACY_MAX_PATH_BYTES",
        default_value = "64MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    legacy_max_path_bytes: ByteSize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_LEGACY_MAX_TRIE_DEPTH",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    legacy_max_trie_depth: usize,
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
    #[arg(
        long,
        env = "CRABKA_PROFILES_OBJECT_STORE_URL",
        default_value = "file://./.crabka-profiles-blocks"
    )]
    object_store_url: String,
    #[arg(
        long,
        env = "CRABKA_PROFILES_INDEX_OBJECT_KEY",
        default_value = "index/profiles.json",
        value_parser = parse_non_empty_string
    )]
    index_object_key: String,
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
        env = "CRABKA_PROFILES_INDEX_REFRESH_INTERVAL",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    index_refresh_interval: Time,
    #[arg(
        long,
        env = "CRABKA_PROFILES_WAL_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    wal_poll_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_PROFILES_HOT_STORE_MAX_AGE",
        default_value = "6h",
        value_parser = parse::positive_time
    )]
    hot_store_max_age: Time,
    #[arg(
        long,
        env = "CRABKA_PROFILES_HOT_STORE_MAX_RECORDS",
        default_value_t = 1_000_000,
        value_parser = parse_positive_usize
    )]
    hot_store_max_records: usize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_HEATMAP_VALUE_BUCKETS",
        default_value_t = 32,
        value_parser = parse_positive_usize
    )]
    heatmap_value_buckets: usize,
    #[arg(
        long,
        env = "CRABKA_PROFILES_HEATMAP_TIME_BUCKETS_MAX",
        default_value_t = 4096,
        value_parser = parse_positive_usize
    )]
    heatmap_time_buckets_max: usize,
    #[arg(
        long = "query-frontend-shard-width",
        visible_alias = "query-frontend-shard-ms",
        env = "CRABKA_PROFILES_QUERY_FRONTEND_SHARD_WIDTH",
        default_value = "15m",
        value_parser = parse_positive_time_or_legacy_millis
    )]
    query_frontend_shard_width: Time,
    #[arg(long, env = "CRABKA_PROFILES_TENANT_LIMITS_CONFIG")]
    tenant_limits_config: Option<std::path::PathBuf>,
    #[arg(long, env = "CRABKA_PROFILES_LIMITS_OVERRIDES_CONFIG")]
    profiles_limits_overrides_config: Option<std::path::PathBuf>,
    #[arg(
        long,
        env = "CRABKA_PROFILES_QUERY_WAL_TAIL_GROUP_ID",
        default_value = "crabka-profiles-query-wal-tail",
        value_parser = parse_non_empty_string
    )]
    query_wal_tail_group_id: String,
    #[arg(long, env = "CRABKA_PROFILES_COMPACTOR_MAX_BLOCKS_PER_JOB", default_value_t = 8, value_parser = parse_min_two_usize)]
    compactor_max_blocks_per_job: usize,
    #[arg(
        long = "compactor-downsample-resolution",
        visible_alias = "compactor-downsample-resolution-ns",
        env = "CRABKA_PROFILES_COMPACTOR_DOWNSAMPLE_RESOLUTION",
        value_parser = parse_positive_time_or_legacy_nanos
    )]
    compactor_downsample_resolution: Option<Time>,
    #[arg(long, env = "CRABKA_PROFILES_BLOCK_BUILDER_FLUSH_RECORDS", default_value_t = crabka_profiles::blockbuilder::DEFAULT_FLUSH_RECORDS, value_parser = parse_positive_usize)]
    block_builder_flush_records: usize,
    #[arg(
        long = "block-builder-flush-max-age",
        visible_alias = "block-builder-flush-max-age-ms",
        env = "CRABKA_PROFILES_BLOCK_BUILDER_FLUSH_MAX_AGE",
        default_value = "10s",
        value_parser = parse_positive_time_or_legacy_millis
    )]
    block_builder_flush_max_age: Time,
    /// debuginfod base URLs, comma-separated, to fetch DWARF for unsymbolized
    /// native frames. Empty by default: the symbolizer makes NO outbound
    /// requests until an operator supplies URLs.
    #[arg(
        long = "debuginfod-url",
        env = "CRABKA_PROFILES_DEBUGINFOD_URLS",
        value_delimiter = ','
    )]
    debuginfod_urls: Vec<String>,
    #[arg(
        long,
        env = "CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE",
        value_parser = parse_positive_whole_byte_size
    )]
    debuginfod_max_artifact_size: Option<ByteSize>,
    #[arg(
        long,
        env = "CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT",
        value_parser = parse::positive_time
    )]
    debuginfod_connect_timeout: Option<Time>,
    #[arg(
        long,
        env = "CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT",
        value_parser = parse::positive_time
    )]
    debuginfod_request_timeout: Option<Time>,
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

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    GreaterUsize::<0>::new(value.parse::<usize>().map_err(|error| error.to_string())?)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

fn parse_non_empty_string(value: &str) -> Result<String, String> {
    refined_type::rule::NonEmptyString::new(value.to_owned())
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

fn parse_min_two_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    GreaterUsize::<1>::new(value.parse::<usize>().map_err(|error| error.to_string())?)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

fn parse_positive_time_or_legacy(value: &str, legacy: fn(i64) -> Time) -> Result<Time, String> {
    if let Ok(raw) = value.parse::<i64>() {
        if raw <= 0 {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy(raw));
    }
    parse::positive_time(value).map_err(|error| error.to_string())
}

fn parse_positive_time_or_legacy_millis(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_millis)
}

fn parse_positive_time_or_legacy_nanos(value: &str) -> Result<Time, String> {
    parse_positive_time_or_legacy(value, Time::from_nanos)
}

fn client_resource_policy(
    cli: &Cli,
) -> (
    crabka_client_core::ConnectionDispatchQueueCapacity,
    crabka_client_core::ClientFrameMax,
) {
    (
        crabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated profiles client dispatch queue capacity"),
        crabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated profiles client frame maximum"),
    )
}

fn debuginfod_config(cli: &Cli) -> Result<DebuginfodConfig, String> {
    let defaults = DebuginfodConfig::default();
    DebuginfodConfig::new(
        cli.debuginfod_max_artifact_size
            .unwrap_or(defaults.max_artifact_size()),
        cli.debuginfod_connect_timeout
            .unwrap_or(defaults.connect_timeout()),
        cli.debuginfod_request_timeout
            .unwrap_or(defaults.request_timeout()),
    )
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

/// How long each hot WAL-tail poll waits for records.
/// Periodically reload the profile block index from object storage and swap it
/// into the cold store. The block-builder writes new blocks continuously.
/// Without this reload the querier only ever sees the index snapshot that it
/// loaded at boot, so blocks created after boot stay invisible. The symptom is
/// that recent profiles return empty, above all sparse ones such as memory that
/// age out of the hot tier. This loop mirrors the `TraceIndex` refresh loop of
/// the traces querier.
fn spawn_profile_index_refresh(
    cold: Arc<ColdProfileStore>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    max_bytes: ByteSize,
    interval: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = tick.tick() => {}
            }
            match ProfileIndex::load_latest_snapshot_with_max_bytes(&store, &index_key, max_bytes)
                .await
            {
                Ok(index) => cold.replace_index(Arc::new(index)),
                Err(error) => {
                    tracing::warn!(%error, %index_key, "profile index refresh failed; retaining last good index");
                }
            }
        }
    });
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let telemetry = crabka_telemetry::init(
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
    let result = run(cli).await;
    telemetry.shutdown();
    result
}

#[allow(clippy::too_many_lines)]
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(&cli);
    let debuginfod_config = debuginfod_config(&cli)?;
    let metrics = ServiceMetrics::new();
    let admin = crabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        crabka_profiles::metrics::metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    let role = async move {
        match cli.target {
            Target::Distributor => {
                let limits = load_tenant_limits_config(cli.tenant_limits_config.as_deref())?;
                let profile_overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let producer = Producer::builder()
                    .bootstrap(&cli.bootstrap)
                    .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
                    .frame_max(client_frame_max.size())
                    .build()
                    .await?;
                let state = Arc::new(DistributorState {
                    sink: Arc::new(KafkaSink::with_topic(Arc::new(producer), cli.wal_topic)),
                    limits,
                    profile_overrides,
                    active_series: Mutex::default(),
                    ingestion_buckets: Mutex::default(),
                    relabel: Vec::<RelabelConfig>::new(),
                    max_decompressed: cli.distributor_request_max,
                    max_tracked_tenants: cli.distributor_max_tracked_tenants,
                    legacy_decode_limits: crabka_profiles::ingest::LegacyDecodeLimits {
                        max_nodes: cli.legacy_max_nodes,
                        max_path_bytes: cli.legacy_max_path_bytes,
                        max_trie_depth: cli.legacy_max_trie_depth,
                    },
                    metrics: metrics.clone(),
                });
                let shutdown = role_shutdown_token();
                let bound = serve_supervised(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(%bound, "profiles distributor listening");
                shutdown.cancelled().await;
            }
            Target::BlockBuilder => {
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let mut config =
                    BlockBuilderConfig::new(cli.bootstrap, configured.store).with_metrics(metrics);
                config.client_dispatch_queue_capacity = client_dispatch_queue_capacity;
                config.client_frame_max = client_frame_max;
                config.wal_topic = cli.wal_topic;
                config.group_id = cli.block_builder_group_id;
                config.index_key = index_key;
                config.wal_fetch_max = cli.wal_fetch_max;
                config.wal_fetch_partition_max = cli.wal_fetch_partition_max;
                config.flush_records = cli.block_builder_flush_records;
                config.flush_max_age = cli.block_builder_flush_max_age;
                config.poll_timeout = cli.wal_poll_timeout;
                config.index_snapshot_max = cli.index_snapshot_max;
                config.index_snapshot_retain = cli.index_snapshot_retain;
                crabka_profiles::blockbuilder::run_with_config(config).await?;
            }
            Target::Querier => {
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                spawn_profile_index_refresh(
                    Arc::clone(&cold),
                    refresh_store,
                    index_key.clone(),
                    cli.index_snapshot_max,
                    cli.index_refresh_interval,
                    shutdown.clone(),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                let wal_tail = spawn_wal_tail(
                    &cli,
                    hot.clone(),
                    client_dispatch_queue_capacity,
                    client_frame_max,
                );
                let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
                let state = Arc::new(
                    QuerierState::new_with_overrides(union, overrides)
                        .with_heatmap_policy(
                            cli.heatmap_value_buckets,
                            cli.heatmap_time_buckets_max,
                        )
                        .with_metrics(metrics.clone()),
                );
                let bound = serve_querier(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(%bound, "profiles querier listening");
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    result = wal_tail => {
                        shutdown.cancel();
                        result??;
                    }
                }
            }
            Target::QueryFrontend => {
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                spawn_profile_index_refresh(
                    Arc::clone(&cold),
                    refresh_store,
                    index_key.clone(),
                    cli.index_snapshot_max,
                    cli.index_refresh_interval,
                    shutdown.clone(),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                let wal_tail = spawn_wal_tail(
                    &cli,
                    hot.clone(),
                    client_dispatch_queue_capacity,
                    client_frame_max,
                );
                let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
                let state = Arc::new(
                    QuerierState::new_frontend_with_overrides(
                        union,
                        FrontendConfig {
                            shard_width: cli.query_frontend_shard_width,
                        },
                        overrides,
                    )
                    .with_heatmap_policy(cli.heatmap_value_buckets, cli.heatmap_time_buckets_max)
                    .with_metrics(metrics.clone()),
                );
                let bound = serve_querier(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(
                    %bound,
                    shard_width = %cli.query_frontend_shard_width.human(),
                    "profiles query-frontend listening"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    result = wal_tail => {
                        shutdown.cancel();
                        result??;
                    }
                }
            }
            Target::Symbolizer => {
                crabka_profiles::symbolizer::run_with_config(
                    cli.debuginfod_urls,
                    debuginfod_config,
                )
                .await?;
            }
            Target::Compactor => {
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let downsample =
                    cli.compactor_downsample_resolution
                        .map(|resolution| DownsamplePolicy {
                            resolution_ns: resolution.nanos_i64(),
                        });
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
                    downsample_resolution = ?cli.compactor_downsample_resolution,
                    "profiles compactor finished one pass"
                );
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tokio::select! {
        result = role => result,
        result = crabka_telemetry::profiling::await_admin_exit(admin) => Ok(result?),
    }
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

fn spawn_wal_tail(
    cli: &Cli,
    hot: WalTailProfileStore,
    client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: crabka_client_core::ClientFrameMax,
) -> tokio::task::JoinHandle<Result<(), crabka_profiles::ProfilesError>> {
    let bootstrap = cli.bootstrap.clone();
    let group_id = cli.query_wal_tail_group_id.clone();
    let wal_topic = cli.wal_topic.clone();
    let poll_timeout = cli.wal_poll_timeout;
    tokio::spawn(async move {
        crabka_profiles::hot_store::run_wal_tail_with_topic(
            hot,
            bootstrap,
            group_id,
            wal_topic,
            poll_timeout,
            client_dispatch_queue_capacity,
            client_frame_max,
        )
        .await
    })
}

fn role_shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let signal = token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    token
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex as StdMutex, OnceLock};

    use assert2::{assert, check};
    use clap::{CommandFactory, Parser};
    use crabka_units::{bytes, per_sec};

    use super::*;

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .unwrap();
        assert!(custom.client_dispatch_queue_capacity == 7);
        assert!(custom.client_frame_max == crabka_units::kibibytes(32));

        for args in [
            vec![
                "crabka-profiles",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "crabka-profiles",
                "--target",
                "querier",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_PROFILES_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_PROFILES_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_PROFILES_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == crabka_units::kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == crabka_units::kibibytes(64));
    }

    static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    const DEBUGINFOD_ENV: [(&str, Option<&str>); 4] = [
        ("CRABKA_PROFILES_DEBUGINFOD_URLS", None),
        ("CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE", None),
        ("CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT", None),
        ("CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT", None),
    ];

    #[test]
    fn debuginfod_config_preserves_defaults_and_accepts_cli_units() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(DEBUGINFOD_ENV, || {
            let defaults = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
            assert!(debuginfod_config(&defaults).unwrap() == DebuginfodConfig::default());

            let custom = Cli::try_parse_from([
                "crabka-profiles",
                "--target",
                "querier",
                "--debuginfod-max-artifact-size",
                "64MiB",
                "--debuginfod-connect-timeout",
                "250ms",
                "--debuginfod-request-timeout",
                "3s",
            ])
            .unwrap();
            let config = debuginfod_config(&custom).unwrap();
            assert!(config.max_artifact_size() == mebibytes(64));
            assert!(config.connect_timeout() == crabka_units::millis(250));
            assert!(config.request_timeout() == secs(3));
        });
    }

    #[test]
    fn debuginfod_config_reads_environment() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(
            [
                (
                    "CRABKA_PROFILES_DEBUGINFOD_URLS",
                    Some("http://one.example,http://two.example"),
                ),
                (
                    "CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE",
                    Some("32MiB"),
                ),
                ("CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT", Some("500ms")),
                ("CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT", Some("4s")),
            ],
            || {
                let cli =
                    Cli::try_parse_from(["crabka-profiles", "--target", "symbolizer"]).unwrap();
                let config = debuginfod_config(&cli).unwrap();
                assert!(
                    cli.debuginfod_urls
                        == vec![
                            "http://one.example".to_string(),
                            "http://two.example".to_string()
                        ]
                );
                assert!(config.max_artifact_size() == mebibytes(32));
                assert!(config.connect_timeout() == crabka_units::millis(500));
                assert!(config.request_timeout() == secs(4));
            },
        );
    }

    #[test]
    fn debuginfod_config_rejects_connect_timeout_beyond_request_timeout() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "symbolizer",
            "--debuginfod-connect-timeout",
            "5s",
            "--debuginfod-request-timeout",
            "4s",
        ])
        .unwrap();

        assert!(debuginfod_config(&cli).is_err());
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn runtime_policy_preserves_defaults_and_accepts_units() {
        let defaults = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.distributor_request_max == mebibytes(16));
        assert!(defaults.distributor_max_tracked_tenants == 4096);
        assert!(defaults.legacy_max_nodes == 500_000);
        assert!(defaults.legacy_max_path_bytes == mebibytes(64));
        assert!(defaults.legacy_max_trie_depth == 4096);
        assert!(defaults.index_refresh_interval == secs(15));
        assert!(defaults.hot_store_max_age == crabka_units::hours(6));
        assert!(defaults.hot_store_max_records == 1_000_000);
        assert!(defaults.heatmap_value_buckets == 32);
        assert!(defaults.heatmap_time_buckets_max == 4096);
        assert!(defaults.query_frontend_shard_width == crabka_units::minutes(15));

        let custom = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--distributor-request-max",
            "2MiB",
            "--distributor-max-tracked-tenants",
            "32",
            "--legacy-max-nodes",
            "100",
            "--legacy-max-path-bytes",
            "1MiB",
            "--legacy-max-trie-depth",
            "64",
            "--index-refresh-interval",
            "2s",
            "--hot-store-max-age",
            "30m",
            "--hot-store-max-records",
            "500",
            "--heatmap-value-buckets",
            "16",
            "--heatmap-time-buckets-max",
            "256",
            "--query-frontend-shard-width",
            "1m",
            "--block-builder-flush-max-age",
            "3s",
            "--compactor-downsample-resolution",
            "5m",
        ])
        .unwrap();
        assert!(custom.distributor_request_max == mebibytes(2));
        assert!(custom.distributor_max_tracked_tenants == 32);
        assert!(custom.legacy_max_nodes == 100);
        assert!(custom.legacy_max_path_bytes == mebibytes(1));
        assert!(custom.legacy_max_trie_depth == 64);
        assert!(custom.index_refresh_interval == secs(2));
        assert!(custom.hot_store_max_age == crabka_units::minutes(30));
        assert!(custom.hot_store_max_records == 500);
        assert!(custom.heatmap_value_buckets == 16);
        assert!(custom.heatmap_time_buckets_max == 256);
        assert!(custom.query_frontend_shard_width == crabka_units::minutes(1));
        assert!(custom.block_builder_flush_max_age == secs(3));
        assert!(custom.compactor_downsample_resolution == Some(crabka_units::minutes(5)));
    }

    #[test]
    fn runtime_policy_rejects_zero_and_invalid_counts() {
        for (flag, invalid) in [
            ("--distributor-request-max", "0B"),
            ("--distributor-max-tracked-tenants", "0"),
            ("--legacy-max-nodes", "0"),
            ("--legacy-max-path-bytes", "0B"),
            ("--legacy-max-trie-depth", "0"),
            ("--index-refresh-interval", "0s"),
            ("--hot-store-max-age", "0s"),
            ("--hot-store-max-records", "0"),
            ("--heatmap-value-buckets", "0"),
            ("--heatmap-time-buckets-max", "0"),
            ("--query-frontend-shard-width", "0"),
            ("--block-builder-flush-records", "0"),
            ("--block-builder-flush-max-age", "0"),
            ("--compactor-max-blocks-per-job", "1"),
            ("--compactor-downsample-resolution", "0"),
        ] {
            assert!(
                Cli::try_parse_from(["crabka-profiles", "--target", "querier", flag, invalid])
                    .is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn deployment_identity_preserves_defaults_and_rejects_empty_values() {
        let defaults = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.wal_topic == crabka_profiles::PROFILES_WAL_TOPIC);
        assert!(defaults.block_builder_group_id == "crabka-profiles-block-builder");
        assert!(defaults.index_object_key == "index/profiles.json");

        let custom = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--wal-topic",
            "profiles-a",
            "--block-builder-group-id",
            "builders-a",
            "--index-object-key",
            "indexes/a.json",
        ])
        .unwrap();
        assert!(custom.wal_topic == "profiles-a");
        assert!(custom.block_builder_group_id == "builders-a");
        assert!(custom.index_object_key == "indexes/a.json");

        for flag in [
            "--wal-topic",
            "--block-builder-group-id",
            "--index-object-key",
            "--query-wal-tail-group-id",
        ] {
            assert!(
                Cli::try_parse_from(["crabka-profiles", "--target", "querier", flag, ""]).is_err(),
                "{flag} should reject an empty value"
            );
        }
    }

    #[test]
    fn runtime_policy_reads_environment_and_cli_wins() {
        const CHILD: &str = "CRABKA_PROFILES_RUNTIME_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::runtime_policy_reads_environment_and_cli_wins",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_PROFILES_DISTRIBUTOR_REQUEST_MAX", "2MiB")
                    .env("CRABKA_PROFILES_DISTRIBUTOR_MAX_TRACKED_TENANTS", "32")
                    .env("CRABKA_PROFILES_INDEX_REFRESH_INTERVAL", "2s")
                    .env("CRABKA_PROFILES_HOT_STORE_MAX_AGE", "30m")
                    .env("CRABKA_PROFILES_HOT_STORE_MAX_RECORDS", "500")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();
        assert!(from_env.distributor_request_max == mebibytes(2));
        assert!(from_env.distributor_max_tracked_tenants == 32);
        assert!(from_env.index_refresh_interval == secs(2));
        assert!(from_env.hot_store_max_age == crabka_units::minutes(30));
        assert!(from_env.hot_store_max_records == 500);

        let from_cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--distributor-request-max",
            "3MiB",
            "--distributor-max-tracked-tenants",
            "64",
        ])
        .unwrap();
        assert!(from_cli.distributor_request_max == mebibytes(3));
        assert!(from_cli.distributor_max_tracked_tenants == 64);
    }

    #[test]
    fn every_process_argument_has_an_environment_binding() {
        let command = Cli::command();
        let missing = command
            .get_arguments()
            .filter(|argument| argument.get_long().is_some() && argument.get_env().is_none())
            .filter_map(|argument| argument.get_long().map(str::to_owned))
            .collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "arguments without env bindings: {missing:?}"
        );
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
        assert!(cli.block_builder_flush_max_age == crabka_units::minutes(1));
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
        assert!(cli.query_frontend_shard_width == crabka_units::secs(30));
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

        assert!(cli.compactor_downsample_resolution == Some(crabka_units::minutes(1)));
    }

    #[test]
    fn debuginfod_urls_default_is_empty() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var("CRABKA_PROFILES_DEBUGINFOD_URLS", None::<&str>, || {
            // Security default: no outbound debuginfod egress unless the operator
            // explicitly opts in. The list must be empty when the flag is absent.
            let cli = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();

            assert!(cli.debuginfod_urls.is_empty());
        });
    }

    #[test]
    fn parses_debuginfod_urls() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var("CRABKA_PROFILES_DEBUGINFOD_URLS", None::<&str>, || {
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
        });
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
