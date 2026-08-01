#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{net::SocketAddr, process::ExitCode, sync::Arc};

use arc_swap::ArcSwap;
use clap::{ArgAction, Args, Parser, ValueEnum};
use crabka_blockstore::{
    BlockStore, BlockWriter, IndexSnapshotRetain, PromotedSpanAttr, TraceIndex,
};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerFetchMaxBytes};
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_client_producer::Producer;
use crabka_telemetry::OtlpConfig;
use crabka_traceql::{EngineOpts, TraceqlEngine};
use crabka_traces::{
    LiveStore, TRACES_WAL_TOPIC, blockbuilder,
    compactor::compact_index_window_with_max_bytes,
    distributor::{self, DistributorState, KafkaSink},
    frontend::{self, FrontendConfig, TraceIndexCatalog},
    ids::UnixNano,
    livestore,
    metrics::ServiceMetrics,
    metricsgen::{
        KafkaSpanSource, MetricsGenConfig, MetricsGenService, PrometheusRemoteWriteSink,
        SystemClock,
    },
    querier::{
        self as trace_querier,
        http::HttpConfig,
        live::{LiveSource, LiveTier, RemoteLiveSource},
        store::{CrabkaSpanStore, DEFAULT_SCAN_CONCAT_MAX, SharedTraceIndex},
    },
    span::batch::RESOURCE_ATTR_PREFIX,
};
use crabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt, TimeExt as _},
    kibibytes, parse, secs,
};
use num_traits::ToPrimitive as _;
use object_store::{ObjectStore, path::Path};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use url::Url;

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

fn parse_non_negative_whole_byte_size_or_bytes(value: &str) -> Result<ByteSize, String> {
    let size = value.parse::<u64>().map_or_else(
        |_| parse::non_negative_byte_size(value).map_err(|error| error.to_string()),
        |bytes| Ok(ByteSize::from_bytes(bytes)),
    )?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a non-negative whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}

fn parse_scan_concat_max(value: &str) -> Result<ByteSize, String> {
    let size = parse_positive_whole_byte_size(value)?;
    if size > DEFAULT_SCAN_CONCAT_MAX {
        return Err("scan concatenation maximum must not exceed 1.5GB".to_owned());
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

    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    GreaterUsize::<0>::new(value)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

fn parse_time_or_legacy_i64(
    value: &str,
    legacy_unit: fn(i64) -> Time,
    positive: bool,
) -> Result<Time, String> {
    if let Ok(value) = value.parse::<i64>() {
        if value < 0 || (positive && value == 0) {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy_unit(value));
    }
    if positive {
        parse::positive_time(value)
    } else {
        parse::non_negative_time(value)
    }
    .map_err(|error| error.to_string())
}

fn parse_positive_time_or_nanos(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_nanos, true)
}

fn parse_positive_time_or_millis(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_millis, true)
}

fn parse_positive_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, true)
}

fn parse_non_negative_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, false)
}

fn parse_positive_time_or_nanos_f64(value: &str) -> Result<Time, String> {
    value.parse::<f64>().map_or_else(
        |_| parse::positive_time(value).map_err(|error| error.to_string()),
        |value| {
            if value.is_finite() && value > 0.0 {
                Ok(Time::from_secs_f64(value / 1_000_000_000.0))
            } else {
                Err("time must be finite and positive".to_owned())
            }
        },
    )
}

fn parse_unix_nano(value: &str) -> Result<UnixNano, String> {
    if value == "max" {
        return Ok(UnixNano(i64::MAX));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(UnixNano(value));
    }
    parse::time(value)
        .map(|value| UnixNano(value.nanos_i64()))
        .map_err(|error| error.to_string())
}

#[derive(Debug, Parser)]
#[command(name = "crabka-traces")]
#[command(about = "Tempo-compatible traces service for Crabka")]
struct Cli {
    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "CRABKA_TRACES_TARGET")]
    target: Target,
    #[arg(long, env = "CRABKA_TRACES_LISTEN", default_value = "127.0.0.1:3200")]
    listen: String,
    #[arg(long, env = "CRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "CRABKA_TRACES_GRPC_LISTEN",
        default_value = "127.0.0.1:4317"
    )]
    grpc_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_OTLP_HTTP_LISTEN",
        default_value = "127.0.0.1:4318"
    )]
    otlp_http_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_JAEGER_GRPC_LISTEN",
        default_value = "127.0.0.1:14250"
    )]
    jaeger_grpc_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_JAEGER_COMPACT_LISTEN",
        default_value = "127.0.0.1:6831"
    )]
    jaeger_compact_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_JAEGER_HTTP_LISTEN",
        default_value = "127.0.0.1:14268"
    )]
    jaeger_http_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_ZIPKIN_LISTEN",
        default_value = "127.0.0.1:9411"
    )]
    zipkin_listen: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    bootstrap: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_TRACES_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_WAL_FETCH_MAX",
        default_value = "2MiB",
        value_parser = parse_consumer_fetch_size
    )]
    wal_fetch_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_WAL_FETCH_PARTITION_MAX",
        default_value = "256KiB",
        value_parser = parse_consumer_fetch_size
    )]
    wal_fetch_partition_max: ByteSize,
    #[arg(
        long = "retention",
        visible_alias = "retention-ns",
        env = "CRABKA_TRACES_RETENTION",
        default_value = "30m",
        value_parser = parse_positive_time_or_nanos
    )]
    retention: Time,
    #[arg(
        long = "block-builder-window",
        visible_alias = "block-builder-window-secs",
        env = "CRABKA_TRACES_BLOCK_BUILDER_WINDOW",
        default_value = "5s",
        value_parser = parse_positive_time_or_secs
    )]
    block_builder_window: Time,
    #[arg(
        long,
        env = "CRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF",
        default_value = "100ms",
        value_parser = parse::positive_time
    )]
    block_builder_empty_poll_backoff: Time,
    #[arg(long, default_value_t = crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS)]
    block_builder_flush_max_records: usize,
    #[arg(
        long = "block-builder-flush-max-age",
        visible_alias = "block-builder-flush-max-age-ms",
        env = "CRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_AGE",
        default_value = "10s",
        value_parser = parse_positive_time_or_millis
    )]
    block_builder_flush_max_age: Time,
    #[arg(long, env = "CRABKA_TRACES_QUERIER_LIVE_STORE", action = ArgAction::SetTrue)]
    querier_live_store: bool,
    #[arg(long, env = "CRABKA_TRACES_QUERIER_LIVE_STORE_URL")]
    querier_live_store_url: Option<String>,
    #[arg(
        long,
        env = "CRABKA_TRACES_TRACE_INDEX_KEY",
        default_value = "index/traces.json"
    )]
    trace_index_key: String,
    #[arg(
        long,
        env = "CRABKA_TRACES_INDEX_SNAPSHOT_MAX",
        default_value = "256MiB",
        value_parser = parse_positive_whole_byte_size
    )]
    index_snapshot_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN",
        default_value_t = IndexSnapshotRetain::default()
    )]
    index_snapshot_retain: IndexSnapshotRetain,
    #[arg(
        long,
        env = "CRABKA_TRACES_BLOCK_READ_MAX",
        default_value = "1GiB",
        value_parser = parse_positive_whole_byte_size
    )]
    block_read_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_SCAN_CONCAT_MAX",
        default_value = "1.5GB",
        value_parser = parse_scan_concat_max
    )]
    scan_concat_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_OBJECT_STORE_URL",
        default_value = "memory:///"
    )]
    object_store_url: String,
    #[arg(long, env = "CRABKA_TRACES_REMOTE_WRITE_URL")]
    remote_write_url: Option<String>,
    #[arg(
        long = "collection-interval",
        visible_alias = "collection-interval-secs",
        env = "CRABKA_TRACES_COLLECTION_INTERVAL",
        value_parser = parse_non_negative_time_or_secs
    )]
    collection_interval: Option<Time>,
    #[arg(long, env = "CRABKA_TRACES_MAX_EXEMPLARS_PER_SERIES")]
    max_exemplars_per_series: Option<usize>,
    #[arg(
        long = "edge-ttl",
        visible_alias = "edge-ttl-secs",
        env = "CRABKA_TRACES_EDGE_TTL",
        value_parser = parse_non_negative_time_or_secs
    )]
    edge_ttl: Option<Time>,
    #[arg(long, env = "CRABKA_TRACES_EDGE_STORE_MAX_ITEMS")]
    edge_store_max_items: Option<usize>,
    #[arg(
        long = "histogram-buckets",
        visible_alias = "histogram-buckets-ns",
        env = "CRABKA_TRACES_HISTOGRAM_BUCKETS",
        value_delimiter = ',',
        value_parser = parse_positive_time_or_nanos_f64
    )]
    histogram_buckets: Option<Vec<Time>>,
    #[command(flatten)]
    metrics: MetricsFlags,
    #[arg(
        long = "compaction-start",
        visible_alias = "compaction-start-ns",
        env = "CRABKA_TRACES_COMPACTION_START",
        default_value = "0ns",
        value_parser = parse_unix_nano
    )]
    compaction_start: UnixNano,
    #[arg(
        long = "compaction-end",
        visible_alias = "compaction-end-ns",
        env = "CRABKA_TRACES_COMPACTION_END",
        default_value = "max",
        value_parser = parse_unix_nano
    )]
    compaction_end: UnixNano,
    #[arg(
        long,
        env = "CRABKA_TRACES_QUERIER_URL",
        default_value = "http://127.0.0.1:3200"
    )]
    querier_url: String,
    #[arg(
        long = "live-frontier",
        visible_alias = "live-frontier-ns",
        env = "CRABKA_TRACES_LIVE_FRONTIER",
        value_parser = parse_unix_nano
    )]
    live_frontier: Option<UnixNano>,
    #[arg(long, env = "CRABKA_TRACES_QUERY_QUEUE_DEPTH", default_value_t = 128)]
    query_queue_depth: usize,
    #[arg(
        long,
        env = "CRABKA_TRACES_TARGET_BYTES_PER_JOB",
        default_value = "0B",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    target_bytes_per_job: ByteSize,
    #[arg(long, env = "CRABKA_TRACES_MAX_TRACE_SPANS", default_value_t = usize::MAX)]
    max_trace_spans: usize,
    #[arg(
        long,
        env = "CRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT",
        default_value_t = 25,
        value_parser = parse_positive_usize
    )]
    tag_query_filter_autocomplete_limit: usize,
    #[arg(
        long = "traceql-default-limit",
        env = "CRABKA_TRACES_TRACEQL_DEFAULT_LIMIT",
        default_value_t = 20,
        value_parser = parse_positive_usize
    )]
    traceql_default_limit: usize,
    #[arg(
        long = "traceql-default-spans-per-span-set",
        env = "CRABKA_TRACES_TRACEQL_DEFAULT_SPANS_PER_SPAN_SET",
        default_value_t = 3,
        value_parser = parse_positive_usize
    )]
    traceql_default_spss: usize,
    #[arg(
        long,
        env = "CRABKA_TRACES_TRACEQL_MAX_TRACES",
        default_value_t = 1000,
        value_parser = parse_positive_usize
    )]
    max_search_traces: usize,
    #[arg(long, env = "CRABKA_TRACES_TRACEQL_MAX_EXEMPLARS", default_value_t = 0)]
    max_metric_exemplars: usize,
    #[arg(
        long = "traceql-compare-max-values-per-attr",
        env = "CRABKA_TRACES_TRACEQL_COMPARE_MAX_VALUES_PER_ATTR",
        default_value_t = 256,
        value_parser = parse_positive_usize
    )]
    traceql_compare_max_values_per_attr: usize,
    #[arg(
        long = "traceql-histogram-buckets",
        env = "CRABKA_TRACES_TRACEQL_HISTOGRAM_BUCKETS",
        default_value = "2ms,4ms,8ms,16ms,32ms,64ms,128ms,256ms,512ms,1024ms,2048ms,4096ms,8192ms,16384ms",
        value_delimiter = ',',
        value_parser = parse::positive_time
    )]
    traceql_histogram_buckets: Vec<Time>,
    #[arg(
        long,
        env = "CRABKA_TRACES_MAX_SPANS_PER_REQUEST",
        default_value_t = 10_000
    )]
    max_spans_per_request: usize,
    #[arg(long, env = "CRABKA_TRACES_MAX_SPANS_PER_TRACE", default_value_t = usize::MAX)]
    max_spans_per_trace: usize,
    #[arg(long, env = "CRABKA_TRACES_MAX_INGEST_SPANS_PER_SECOND", default_value_t = usize::MAX)]
    max_ingest_spans_per_second: usize,
    #[arg(long, env = "CRABKA_TRACES_INGEST_RATE_BURST", default_value_t = usize::MAX)]
    ingest_rate_burst: usize,
    #[arg(
        long = "promote-span-attr",
        env = "CRABKA_TRACES_PROMOTE_SPAN_ATTR",
        value_delimiter = ','
    )]
    promote_span_attrs: Vec<String>,
    #[arg(
        long = "promote-resource-attr",
        env = "CRABKA_TRACES_PROMOTE_RESOURCE_ATTR",
        value_delimiter = ','
    )]
    promote_resource_attrs: Vec<String>,
    #[arg(
        long,
        env = "CRABKA_TRACES_MAX_ATTR_VALUE_LEN",
        default_value = "64KiB",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    max_attr_value_len: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_MAX_DECOMPRESSED_BYTES",
        default_value = "10MiB",
        value_parser = parse_non_negative_whole_byte_size_or_bytes
    )]
    max_decompressed_bytes: ByteSize,
    #[arg(
        long,
        env = "CRABKA_TRACES_METRICS_GENERATOR_POLL_BATCH_SIZE",
        default_value_t = 1_000,
        value_parser = parse_positive_usize
    )]
    metrics_generator_poll_batch_size: usize,
    #[arg(
        long,
        env = "CRABKA_TRACES_METRICS_GENERATOR_POLL_ERROR_BACKOFF",
        default_value = "200ms",
        value_parser = parse::positive_time
    )]
    metrics_generator_poll_error_backoff: Time,
    #[arg(long, env = "CRABKA_TRACES_CONFIG")]
    config: Option<String>,
}

#[derive(Debug, Args)]
struct MetricsFlags {
    #[arg(long, env = "CRABKA_TRACES_ENABLE_TARGET_INFO")]
    enable_target_info: bool,
    #[arg(long, env = "CRABKA_TRACES_ENABLE_STATUS_MESSAGE")]
    enable_status_message: bool,
    #[arg(long, env = "CRABKA_TRACES_ENABLE_MESSAGING_SYSTEM_LATENCY")]
    enable_messaging_system_latency: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Target {
    Distributor,
    BlockBuilder,
    LiveStore,
    Querier,
    QueryFrontend,
    Compactor,
    MetricsGenerator,
}

struct ConfiguredObjectStore {
    store: Arc<dyn ObjectStore>,
    root: Url,
    prefix: Path,
}

impl ConfiguredObjectStore {
    fn object_key(&self, key: &str) -> String {
        blockbuilder::prefixed_object_key(self.prefix.as_ref(), key)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // `run` fans out over every role, so its state machine is large; boxing keeps
    // it off the startup task's stack.
    match Box::pin(run(cli)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(2)
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-traces",
            env!("CARGO_PKG_VERSION"),
            "crabka-traces",
        )?,
        "crabka_traces=info,info",
        "info",
        "crabka-traces",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin_with_config(
        cli.admin_listen_addr,
        crabka_traces::metrics::metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    let shutdown = CancellationToken::new();
    let shutdown_task = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown_task.cancel();
        }
    });

    match cli.target {
        Target::Distributor => run_distributor(cli, metrics, shutdown).await?,
        Target::BlockBuilder => run_block_builder(cli, metrics, shutdown).await?,
        Target::LiveStore => run_live_store(cli, shutdown).await?,
        Target::Querier => run_querier(cli, metrics, shutdown).await?,
        Target::QueryFrontend => run_query_frontend(cli, shutdown).await?,
        Target::Compactor => run_compactor(cli).await?,
        Target::MetricsGenerator => run_metrics_generator(cli, shutdown).await?,
    }
    Ok(())
}

async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Boxed: the producer-startup future is several KB and would otherwise be
    // inlined into this role's future (and from there into `run`'s). One
    // allocation at startup keeps the role futures small.
    let producer = Box::pin(
        Producer::builder()
            .bootstrap(cli.bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build(),
    )
    .await?;
    let mut state =
        DistributorState::with_metrics(Arc::new(KafkaSink::new(Arc::new(producer))), metrics);
    state.limits.max_spans_per_request = cli.max_spans_per_request;
    state.limits.max_spans_per_trace = cli.max_spans_per_trace;
    state.limits.max_ingest_rate = ingest_rate_from_cli(cli.max_ingest_spans_per_second);
    state.limits.ingest_rate_burst = cli.ingest_rate_burst;
    state.limits.max_attr_value = cli.max_attr_value_len;
    state.shared_limits = state.limits.to_shared_limits();
    state.max_decompressed = cli.max_decompressed_bytes;
    let state = Arc::new(state);
    let addr: SocketAddr = cli.listen.parse()?;
    let grpc_addr: SocketAddr = cli.grpc_listen.parse()?;
    let otlp_http_addr: SocketAddr = cli.otlp_http_listen.parse()?;
    let jaeger_grpc_addr: SocketAddr = cli.jaeger_grpc_listen.parse()?;
    let jaeger_compact_addr: SocketAddr = cli.jaeger_compact_listen.parse()?;
    let jaeger_http_addr: SocketAddr = cli.jaeger_http_listen.parse()?;
    let zipkin_addr: SocketAddr = cli.zipkin_listen.parse()?;
    let grpc_shutdown = shutdown.clone();
    let grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(err) = distributor::serve_otlp_grpc(grpc_addr, grpc_state, grpc_shutdown).await {
            tracing::warn!(error = %err, "traces distributor OTLP/gRPC server error");
        }
    });
    let jaeger_grpc_shutdown = shutdown.clone();
    let jaeger_grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(err) = distributor::serve_jaeger_grpc(
            jaeger_grpc_addr,
            jaeger_grpc_state,
            jaeger_grpc_shutdown,
        )
        .await
        {
            tracing::warn!(error = %err, "traces distributor Jaeger gRPC server error");
        }
    });
    let jaeger_compact_bound = distributor::serve_jaeger_compact_udp(
        jaeger_compact_addr,
        Arc::clone(&state),
        shutdown.clone(),
    )
    .await?;
    tracing::info!(%jaeger_compact_bound, "traces distributor Jaeger compact UDP listening");
    let otlp_http_bound =
        distributor::serve(otlp_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%otlp_http_bound, "traces distributor OTLP/HTTP listening");
    let jaeger_http_bound =
        distributor::serve(jaeger_http_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%jaeger_http_bound, "traces distributor Jaeger thrift HTTP listening");
    let zipkin_bound =
        distributor::serve(zipkin_addr, Arc::clone(&state), shutdown.clone()).await?;
    tracing::info!(%zipkin_bound, "traces distributor Zipkin HTTP listening");
    let bound = distributor::serve(addr, state, shutdown.clone()).await?;
    tracing::info!(%bound, "traces distributor listening");
    shutdown.cancelled().await;
    Ok(())
}

async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let promoted_attrs = promoted_attrs_from_cli(&cli)?;
    let consumer = wal_consumer(
        cli.bootstrap.clone(),
        "crabka-traces-block-builder",
        Some("crabka-traces-block-builder"),
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial_index = TraceIndex::load_latest_snapshot_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await
    .unwrap_or_else(|_| TraceIndex::new());
    let index = Arc::new(Mutex::new(initial_index));
    blockbuilder::run(
        consumer,
        writer,
        index,
        configured.store,
        blockbuilder::BlockBuilderConfig {
            object_key_prefix,
            index_key: trace_index_key,
            window: cli.block_builder_window,
            empty_poll_backoff: cli.block_builder_empty_poll_backoff,
            promoted_attrs,
            flush_max_records: cli.block_builder_flush_max_records,
            flush_max_age: cli.block_builder_flush_max_age,
            index_snapshot_retain: cli.index_snapshot_retain,
        },
        metrics,
        shutdown,
    )
    .await?;
    Ok(())
}

async fn run_live_store(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let consumer = wal_consumer(
        cli.bootstrap.clone(),
        "crabka-traces-live-store",
        None,
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let store = Arc::new(RwLock::new(LiveStore::new(cli.retention.nanos_i64())));
    let router = build_live_store_router(&cli, Arc::clone(&store))?;
    let live_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(err) = livestore::run(consumer, store, live_shutdown).await {
            tracing::warn!(error = %err, "traces live-store consumer error");
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces live-store listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn run_querier(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let live_store = cli
        .querier_live_store
        .then(|| Arc::new(RwLock::new(LiveStore::new(cli.retention.nanos_i64()))));
    let (router, store, trace_index_key, trace_index) =
        build_querier_router_with_live(&cli, metrics, live_store.clone()).await?;
    if let Some(live_store) = live_store {
        let consumer = wal_consumer(
            cli.bootstrap.clone(),
            "crabka-traces-querier-live-store",
            None,
            cli.wal_fetch_max,
            cli.wal_fetch_partition_max,
            cli.client_dispatch_queue_capacity,
            cli.client_frame_max,
        )
        .await?;
        let live_shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(err) = livestore::run(consumer, live_store, live_shutdown).await {
                tracing::warn!(error = %err, "traces querier embedded live-store error");
            }
        });
    }
    // Periodically reload the TraceIndex so newly-compacted blocks become visible
    // without restarting the querier.
    let refresh_shutdown = shutdown.clone();
    let refresh_store = Arc::clone(&store);
    let refresh_index = Arc::clone(&trace_index);
    let refresh_interval = cli.block_builder_window.max(secs(1));
    let index_snapshot_max = cli.index_snapshot_max;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(refresh_interval.to_std());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = refresh_shutdown.cancelled() => break,
                _ = tick.tick() => {
                    if let Ok(idx) = TraceIndex::load_latest_snapshot_with_max_bytes(
                        &refresh_store,
                        &trace_index_key,
                        index_snapshot_max,
                    ).await {
                        refresh_index.store(Arc::new(idx));
                    }
                }
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces querier listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
async fn build_querier_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let (router, ..) = build_querier_router_with_live(cli, ServiceMetrics::new(), None).await?;
    Ok(router)
}

async fn build_querier_router_with_live(
    cli: &Cli,
    metrics: ServiceMetrics,
    live_store: Option<Arc<RwLock<LiveStore>>>,
) -> Result<
    (axum::Router, Arc<dyn ObjectStore>, String, SharedTraceIndex),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let configured = build_object_store(cli)?;
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial = TraceIndex::load_latest_snapshot_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await
    .unwrap_or_else(|_| TraceIndex::new());
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(initial));
    let blocks = Arc::new(BlockStore::new_with_block_read_max(
        Arc::clone(&configured.store),
        configured.root,
        cli.block_read_max,
    ));
    let live = if let Some(store) = live_store {
        Some(LiveTier::new(Arc::new(IndexedLiveSource::new(
            store,
            Arc::clone(&trace_index),
        ))))
    } else if let Some(url) = &cli.querier_live_store_url {
        Some(LiveTier::new(Arc::new(RemoteLiveSource::new(
            Url::parse(url)?,
            Arc::clone(&trace_index),
        ))))
    } else {
        None
    };
    let store = Arc::new(CrabkaSpanStore::new_with_scan_concat_max(
        blocks,
        Arc::clone(&trace_index),
        live,
        cli.scan_concat_max,
    ));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)?));
    let router = trace_querier::http::router_with_config_and_metrics(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
            tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
            ..HttpConfig::default()
        },
        metrics,
    );
    Ok((router, configured.store, trace_index_key, trace_index))
}

fn build_live_store_router(
    cli: &Cli,
    live_store: Arc<RwLock<LiveStore>>,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(TraceIndex::new()));
    let blocks = Arc::new(BlockStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Url::parse("memory:///")?,
    ));
    let live = LiveTier::new(Arc::new(IndexedLiveSource::new(
        Arc::clone(&live_store),
        Arc::clone(&trace_index),
    )));
    let store = Arc::new(CrabkaSpanStore::new_with_scan_concat_max(
        blocks,
        trace_index,
        Some(live),
        cli.scan_concat_max,
    ));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)?));
    let tempo_router = trace_querier::http::router_with_config(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
            tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
            ..HttpConfig::default()
        },
    );
    let internal_router = axum::Router::new()
        .route(
            "/api/crabka/live/span-batches",
            axum::routing::get(live_span_batches),
        )
        .with_state(live_store);
    Ok(tempo_router.merge(internal_router))
}

async fn live_span_batches(
    axum::extract::State(live_store): axum::extract::State<Arc<RwLock<LiveStore>>>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let start = match live_i64_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (axum::http::StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end = match live_i64_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (axum::http::StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end < start {
        return (axum::http::StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let tenant = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous");
    let guard = live_store.read().await;
    let batches = match guard.span_batches(tenant, start, end).await {
        Ok(batches) => batches,
        Err(err) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
                .into_response();
        }
    };
    match trace_querier::live::encode_span_batches(&batches) {
        Ok(bytes) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream",
            )],
            bytes,
        )
            .into_response(),
        Err(err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )
            .into_response(),
    }
}

fn live_i64_param(uri: &axum::http::Uri, name: &str) -> Result<i64, String> {
    uri.query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
        })
        .ok_or_else(|| format!("missing query parameter {name}"))?
        .parse::<i64>()
        .map_err(|_| format!("invalid query parameter {name}"))
}

fn engine_opts_from_cli(cli: &Cli) -> std::io::Result<EngineOpts> {
    if !cli
        .traceql_histogram_buckets
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "TraceQL histogram buckets must be strictly increasing",
        ));
    }
    Ok(EngineOpts {
        default_limit: cli.traceql_default_limit,
        default_spss: cli.traceql_default_spss,
        max_traces: cli.max_search_traces,
        max_exemplars: cli.max_metric_exemplars,
        compare_max_values_per_attr: cli.traceql_compare_max_values_per_attr,
        histogram_buckets: cli.traceql_histogram_buckets.clone(),
    })
}

struct IndexedLiveSource {
    store: Arc<RwLock<LiveStore>>,
    trace_index: SharedTraceIndex,
}

impl IndexedLiveSource {
    fn new(store: Arc<RwLock<LiveStore>>, trace_index: SharedTraceIndex) -> Self {
        Self { store, trace_index }
    }
}

#[async_trait::async_trait]
impl LiveSource for IndexedLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> crabka_traces::querier::live::Result<Vec<arrow::record_batch::RecordBatch>> {
        let guard = self.store.read().await;
        guard.span_batches(tenant, start_ns, end_ns).await
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> crabka_traces::querier::live::Result<Option<crabka_traceql::TraceSpans>> {
        let guard = self.store.read().await;
        guard.trace_spans(tenant, trace_id).await
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<crabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> crabka_traces::querier::live::Result<Vec<crabka_traceql::ScopedTag>> {
        let guard = self.store.read().await;
        guard.tag_names(tenant, scope, start_ns, end_ns).await
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> crabka_traces::querier::live::Result<Vec<crabka_traceql::TypedValue>> {
        let guard = self.store.read().await;
        guard.tag_values(tenant, tag, start_ns, end_ns).await
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        let trace_index = self.trace_index.load();
        trace_index
            .trace_blocks(tenant)
            .iter()
            .map(|block| block.max_ts.saturating_add(1))
            .max()
            .unwrap_or_default()
    }
}

async fn run_query_frontend(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(&cli, addr)?;
    let catalog = build_trace_index_catalog(&cli).await?;
    tracing::info!(%addr, "traces query-frontend listening");
    frontend::run_query_frontend(cfg, catalog, shutdown).await?;
    Ok(())
}

/// Map the role CLI onto the new query-frontend [`FrontendConfig`].
///
/// `--querier-url` is a comma-separated list of querier URLs (with scheme); the
/// new [`HttpQuerier`] pool takes bare `host:port`, so the scheme/path are
/// stripped here. `--live-frontier` (and its legacy `--live-frontier-ns`
/// alias) maps to `hot_frontier_ns` (`None` => `0`, i.e. the live tier is
/// always probed).
fn frontend_config_from_cli(
    cli: &Cli,
    listen_addr: SocketAddr,
) -> Result<FrontendConfig, Box<dyn std::error::Error + Send + Sync>> {
    let querier_addrs = parse_querier_addrs(&cli.querier_url)?;
    Ok(FrontendConfig {
        querier_addrs,
        target_per_job: cli.target_bytes_per_job,
        max_concurrency: cli.query_queue_depth.max(1),
        hot_frontier_ns: cli.live_frontier.unwrap_or(UnixNano(0)).0,
        max_trace: max_trace_size(cli.max_trace_spans),
        listen_addr,
        ..FrontendConfig::default()
    })
}

/// Build the production block catalog from the trace index when row-group
/// sharding is enabled (`--target-bytes-per-job > 0`); otherwise an empty
/// catalog (whole-tier search, no per-block fan-out).
async fn build_trace_index_catalog(
    cli: &Cli,
) -> Result<TraceIndexCatalog, Box<dyn std::error::Error + Send + Sync>> {
    if cli.target_bytes_per_job == ByteSize::from_bytes(0) {
        return Ok(TraceIndexCatalog::new(std::collections::BTreeMap::new()));
    }
    let configured = build_object_store(cli)?;
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let trace_index = TraceIndex::load_latest_snapshot_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await
    .unwrap_or_else(|_| TraceIndex::new());
    let blocks =
        BlockStore::new_with_block_read_max(configured.store, configured.root, cli.block_read_max);
    Ok(TraceIndexCatalog::from_trace_index(&blocks, &trace_index)
        .await
        .unwrap_or_else(|_| TraceIndexCatalog::new(std::collections::BTreeMap::new())))
}

/// Parse `--querier-url` (comma-separated querier URLs, scheme allowed) into the
/// bare `host:port` addresses the [`HttpQuerier`] pool dials.
fn parse_querier_addrs(
    value: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut addrs = Vec::new();
    for raw in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let url = Url::parse(raw)?;
        let host = url
            .host_str()
            .ok_or_else(|| format!("querier url missing host: {raw}"))?;
        let addr = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        addrs.push(addr);
    }
    if addrs.is_empty() {
        return Err(format!("no querier addresses parsed from {value:?}").into());
    }
    Ok(addrs)
}

#[cfg(test)]
async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    use crabka_traces::frontend::{HttpQuerier, QueryFrontend};

    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(cli, addr)?;
    let catalog = build_trace_index_catalog(cli).await?;
    let backend = HttpQuerier::new(cfg.querier_addrs.clone(), cfg.request_timeout.to_std())?;
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    Ok(frontend::server::router_with_backend(qf))
}

async fn run_compactor(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let mut index = TraceIndex::load_latest_snapshot_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await
    .unwrap_or_else(|_| TraceIndex::new());
    compact_index_window_with_max_bytes(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        cli.compaction_start.0,
        cli.compaction_end.0,
        cli.block_read_max,
    )
    .await?;
    index
        .save_latest_snapshot_with_retain(
            &configured.store,
            &trace_index_key,
            cli.index_snapshot_retain,
        )
        .await?;
    Ok(())
}

fn build_object_store(
    cli: &Cli,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let root = Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url_opts(&root, std::env::vars())?;
    let configured = ConfiguredObjectStore {
        store: Arc::from(store),
        root,
        prefix,
    };
    tracing::debug!(
        object_store_url = %configured.root,
        object_store_prefix = %configured.prefix,
        "configured traces object store"
    );
    Ok(configured)
}

fn promoted_attrs_from_cli(cli: &Cli) -> Result<Vec<PromotedSpanAttr>, String> {
    let mut attrs = Vec::new();
    for spec in &cli.promote_resource_attrs {
        attrs.push(parse_promoted_attr(spec, Some(RESOURCE_ATTR_PREFIX))?);
    }
    for spec in &cli.promote_span_attrs {
        attrs.push(parse_promoted_attr(spec, None)?);
    }
    Ok(attrs)
}

fn parse_promoted_attr(spec: &str, key_prefix: Option<&str>) -> Result<PromotedSpanAttr, String> {
    let (key, value_type) = spec.split_once(':').unwrap_or((spec, "string"));
    if key.is_empty() {
        return Err("promoted attribute key cannot be empty".into());
    }

    let key = format!("{}{}", key_prefix.unwrap_or_default(), key);
    match value_type {
        "string" | "str" => Ok(PromotedSpanAttr::string(key)),
        "int" | "i64" => Ok(PromotedSpanAttr::int(key)),
        "double" | "float" | "f64" => Ok(PromotedSpanAttr::double(key)),
        "bool" | "boolean" => Ok(PromotedSpanAttr::bool(key)),
        other => Err(format!(
            "unsupported promoted attribute type {other:?}; expected string, int, double, or bool"
        )),
    }
}

async fn run_metrics_generator(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = if let Some(path) = &cli.config {
        let bytes = std::fs::read_to_string(path)?;
        serde_yaml::from_str::<MetricsGenConfig>(&bytes)?
    } else {
        MetricsGenConfig::default()
    };
    apply_metrics_generator_cli_overrides(&mut cfg, &cli);

    let consumer = wal_consumer(
        cli.bootstrap,
        "crabka-traces-metrics-generator",
        None,
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let source = Arc::new(KafkaSpanSource::new(consumer));
    let sink = Arc::new(PrometheusRemoteWriteSink::new(cfg.remote_write_url.clone()));
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink)
        .with_poll_policy(
            cli.metrics_generator_poll_batch_size,
            cli.metrics_generator_poll_error_backoff,
        );
    service.run(shutdown).await;
    Ok(())
}

/// `usize::MAX` is the CLI's "no limit" spelling; a zero rate is how the shared
/// limits express unlimited.
fn ingest_rate_from_cli(spans_per_sec: usize) -> Frequency {
    if spans_per_sec == usize::MAX {
        <Frequency as FrequencyExt>::ZERO
    } else {
        Frequency::from_per_sec(f64_from_usize(spans_per_sec))
    }
}

fn f64_from_usize(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

/// The assembled-trace ceiling, budgeting ~64KiB of OTLP JSON per span.
fn max_trace_size(spans: usize) -> ByteSize {
    kibibytes(64) * f64_from_usize(spans)
}

fn apply_metrics_generator_cli_overrides(cfg: &mut MetricsGenConfig, cli: &Cli) {
    if let Some(interval) = cli.collection_interval {
        cfg.collection_interval = interval;
    }
    if let Some(url) = &cli.remote_write_url {
        cfg.remote_write_url.clone_from(url);
    }
    if let Some(max) = cli.max_exemplars_per_series {
        cfg.max_exemplars_per_series = max;
    }
    if let Some(ttl) = cli.edge_ttl {
        cfg.edge_ttl = ttl;
    }
    if let Some(max) = cli.edge_store_max_items {
        cfg.edge_store_max_items = max;
    }
    if let Some(buckets) = &cli.histogram_buckets {
        cfg.histogram_buckets_ns = buckets
            .iter()
            .map(|bucket| bucket.secs_f64() * 1_000_000_000.0)
            .collect();
    }
    cfg.enable_target_info |= cli.metrics.enable_target_info;
    cfg.enable_status_message |= cli.metrics.enable_status_message;
    cfg.enable_messaging_system_latency |= cli.metrics.enable_messaging_system_latency;
}

async fn wal_consumer(
    bootstrap: String,
    group_id: &str,
    group_instance_id: Option<&str>,
    fetch_max: ByteSize,
    fetch_partition_max: ByteSize,
    client_dispatch_queue_capacity: usize,
    client_frame_max: ByteSize,
) -> Result<Consumer, crabka_client_consumer::ConsumerError> {
    // Boxed: consumer startup (bootstrap resolve, double `JoinGroup`,
    // `SyncGroup`, offset priming) builds a ~13 KB future. Every role that
    // reads the WAL awaits this, so leaving it inline pushes each role future
    // — and the `run` dispatcher that unions them — past `clippy::large_futures`.
    // The consumer is built once per process, so the allocation is free.
    Box::pin(
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id.to_string())
            .maybe_group_instance_id(group_instance_id)
            .fetch_max(fetch_max)
            .fetch_partition_max(fetch_partition_max)
            .dispatch_queue_capacity(client_dispatch_queue_capacity)
            .frame_max(client_frame_max)
            .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::{
        body::Body,
        http::{Request, StatusCode as HttpStatusCode},
    };
    use clap::{CommandFactory as _, Parser};
    use crabka_units::minutes;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn non_dimensioned_cli_arguments_have_environment_backing() {
        let command = Cli::command();
        for (id, env) in [
            ("target", "CRABKA_TRACES_TARGET"),
            ("listen", "CRABKA_TRACES_LISTEN"),
            ("grpc_listen", "CRABKA_TRACES_GRPC_LISTEN"),
            ("otlp_http_listen", "CRABKA_TRACES_OTLP_HTTP_LISTEN"),
            ("jaeger_grpc_listen", "CRABKA_TRACES_JAEGER_GRPC_LISTEN"),
            (
                "jaeger_compact_listen",
                "CRABKA_TRACES_JAEGER_COMPACT_LISTEN",
            ),
            ("jaeger_http_listen", "CRABKA_TRACES_JAEGER_HTTP_LISTEN"),
            ("zipkin_listen", "CRABKA_TRACES_ZIPKIN_LISTEN"),
            ("bootstrap", "CRABKA_TRACES_BOOTSTRAP"),
            ("querier_live_store", "CRABKA_TRACES_QUERIER_LIVE_STORE"),
            (
                "querier_live_store_url",
                "CRABKA_TRACES_QUERIER_LIVE_STORE_URL",
            ),
            ("trace_index_key", "CRABKA_TRACES_TRACE_INDEX_KEY"),
            ("object_store_url", "CRABKA_TRACES_OBJECT_STORE_URL"),
            ("remote_write_url", "CRABKA_TRACES_REMOTE_WRITE_URL"),
            (
                "max_exemplars_per_series",
                "CRABKA_TRACES_MAX_EXEMPLARS_PER_SERIES",
            ),
            ("edge_store_max_items", "CRABKA_TRACES_EDGE_STORE_MAX_ITEMS"),
            ("querier_url", "CRABKA_TRACES_QUERIER_URL"),
            ("query_queue_depth", "CRABKA_TRACES_QUERY_QUEUE_DEPTH"),
            ("max_trace_spans", "CRABKA_TRACES_MAX_TRACE_SPANS"),
            (
                "max_spans_per_request",
                "CRABKA_TRACES_MAX_SPANS_PER_REQUEST",
            ),
            ("max_spans_per_trace", "CRABKA_TRACES_MAX_SPANS_PER_TRACE"),
            (
                "max_ingest_spans_per_second",
                "CRABKA_TRACES_MAX_INGEST_SPANS_PER_SECOND",
            ),
            ("ingest_rate_burst", "CRABKA_TRACES_INGEST_RATE_BURST"),
            ("promote_span_attrs", "CRABKA_TRACES_PROMOTE_SPAN_ATTR"),
            (
                "promote_resource_attrs",
                "CRABKA_TRACES_PROMOTE_RESOURCE_ATTR",
            ),
            ("config", "CRABKA_TRACES_CONFIG"),
            ("enable_target_info", "CRABKA_TRACES_ENABLE_TARGET_INFO"),
            (
                "enable_status_message",
                "CRABKA_TRACES_ENABLE_STATUS_MESSAGE",
            ),
            (
                "enable_messaging_system_latency",
                "CRABKA_TRACES_ENABLE_MESSAGING_SYSTEM_LATENCY",
            ),
        ] {
            let configured = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .and_then(|value| value.to_str());
            check!(configured == Some(env), "missing {env} on {id}");
        }
    }

    #[test]
    fn process_environment_supplies_cli_and_explicit_flags_win() {
        const CHILD: &str = "CRABKA_TRACES_PROCESS_ENVIRONMENT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::process_environment_supplies_cli_and_explicit_flags_win",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_TARGET", "querier")
                    .env("CRABKA_TRACES_LISTEN", "127.0.0.1:3210")
                    .env("CRABKA_TRACES_ENABLE_TARGET_INFO", "true")
                    .env(
                        "CRABKA_TRACES_PROMOTE_SPAN_ATTR",
                        "http.method:string,http.status:int",
                    )
                    .env("CRABKA_TRACES_QUERY_QUEUE_DEPTH", "7")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces"]).unwrap();
        check!(
            (
                from_env.target,
                from_env.listen.as_str(),
                from_env.metrics.enable_target_info,
                from_env.promote_span_attrs.as_slice(),
                from_env.query_queue_depth,
            ) == (
                Target::Querier,
                "127.0.0.1:3210",
                true,
                &[
                    "http.method:string".to_string(),
                    "http.status:int".to_string()
                ][..],
                7,
            )
        );

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=query-frontend",
            "--listen=127.0.0.1:3220",
            "--query-queue-depth=11",
        ])
        .unwrap();
        check!(
            (
                from_cli.target,
                from_cli.listen.as_str(),
                from_cli.query_queue_depth
            ) == (Target::QueryFrontend, "127.0.0.1:3220", 11)
        );
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == crabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .unwrap();
        assert2::assert!(custom.client_dispatch_queue_capacity == 7);
        assert2::assert!(custom.client_frame_max == kibibytes(32));

        for args in [
            vec![
                "crabka-traces",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "crabka-traces",
                "--target",
                "querier",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert2::assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_TRACES_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert2::assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert2::assert!(from_cli.client_frame_max == kibibytes(64));
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "distributor"]).unwrap();
        assert2::assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_distributor_grpc_listener() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--grpc-listen",
            "127.0.0.1:4317",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.grpc_listen.as_str() == "127.0.0.1:4317");
    }

    #[test]
    fn parses_distributor_jaeger_compact_listener() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--jaeger-compact-listen",
            "127.0.0.1:6831",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.jaeger_compact_listen.as_str() == "127.0.0.1:6831");
    }

    #[test]
    fn parses_distributor_jaeger_grpc_listener() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--jaeger-grpc-listen",
            "127.0.0.1:14250",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.jaeger_grpc_listen.as_str() == "127.0.0.1:14250");
    }

    #[test]
    fn distributor_defaults_include_tempo_push_ports() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "distributor"]).unwrap();

        assert2::assert!(cli.otlp_http_listen.as_str() == "127.0.0.1:4318");
        assert2::assert!(cli.jaeger_grpc_listen.as_str() == "127.0.0.1:14250");
        assert2::assert!(cli.jaeger_http_listen.as_str() == "127.0.0.1:14268");
        assert2::assert!(cli.zipkin_listen.as_str() == "127.0.0.1:9411");
    }

    #[test]
    fn parses_distributor_ingest_limits() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--max-spans-per-request",
            "123",
            "--max-attr-value-len",
            "456",
            "--max-decompressed-bytes",
            "789",
        ])
        .unwrap();

        assert2::assert!(cli.max_spans_per_request == 123);
        assert2::assert!(cli.max_attr_value_len == ByteSize::from_bytes(456));
        assert2::assert!(cli.max_decompressed_bytes == ByteSize::from_bytes(789));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
        assert2::assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn parses_block_builder_flush_window() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "block-builder",
            "--block-builder-window-secs",
            "30",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::BlockBuilder));
        assert2::assert!(cli.block_builder_window == secs(30));
    }

    #[test]
    fn block_builder_flush_knobs_default() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();

        check!(cli.block_builder_empty_poll_backoff == crabka_units::millis(100));
        assert2::assert!(
            cli.block_builder_flush_max_records
                == crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS
        );
        assert2::assert!(
            cli.block_builder_flush_max_age == crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE
        );
    }

    #[test]
    fn block_builder_empty_poll_backoff_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::block_builder_empty_poll_backoff_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF", "7ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target=block-builder"]).unwrap();
        check!(from_env.block_builder_empty_poll_backoff == crabka_units::millis(7));
        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=block-builder",
            "--block-builder-empty-poll-backoff=11ms",
        ])
        .unwrap();
        check!(from_cli.block_builder_empty_poll_backoff == crabka_units::millis(11));
        check!(
            Cli::try_parse_from([
                "crabka-traces",
                "--target=block-builder",
                "--block-builder-empty-poll-backoff=0ms",
            ])
            .is_err()
        );
    }

    #[test]
    fn index_snapshot_policy_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
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
                        "crabka-traces",
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
                    "crabka-traces",
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
        const CHILD: &str = "CRABKA_TRACES_INDEX_SNAPSHOT_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::index_snapshot_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_INDEX_SNAPSHOT_MAX", "1KiB")
                    .env("CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN", "3")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.index_snapshot_max.bytes_u64(), 1024);
        assert_eq!(from_env.index_snapshot_retain.into_value(), 3);

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
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
    fn block_read_max_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(
            cli.block_read_max,
            crabka_blockstore::DEFAULT_BLOCK_READ_MAX
        );

        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert!(
                Cli::try_parse_from([
                    "crabka-traces",
                    "--target",
                    "querier",
                    "--block-read-max",
                    invalid,
                ])
                .is_err(),
                "--block-read-max should reject {invalid:?}"
            );
        }
        for invalid in ["1.5B", "18446744073709551616B"] {
            assert!(
                Cli::try_parse_from([
                    "crabka-traces",
                    "--target",
                    "querier",
                    "--block-read-max",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn block_read_max_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_BLOCK_READ_MAX_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::block_read_max_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_BLOCK_READ_MAX", "1KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(from_env.block_read_max.bytes_u64(), 1024);

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--block-read-max",
            "2KiB",
        ])
        .unwrap();
        assert_eq!(from_cli.block_read_max.bytes_u64(), 2048);
    }

    #[test]
    fn scan_concat_max_preserves_default_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(cli.scan_concat_max.bytes_u64(), 1_500_000_000);

        for invalid in [
            "0",
            "not-a-number",
            "-1B",
            "1500000001B",
            "18446744073709551616B",
        ] {
            assert!(
                Cli::try_parse_from([
                    "crabka-traces",
                    "--target",
                    "querier",
                    "--scan-concat-max",
                    invalid,
                ])
                .is_err(),
                "--scan-concat-max should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn scan_concat_max_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_SCAN_CONCAT_MAX_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::scan_concat_max_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_SCAN_CONCAT_MAX", "1KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(from_env.scan_concat_max.bytes_u64(), 1024);

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--scan-concat-max",
            "2KiB",
        ])
        .unwrap();
        assert_eq!(from_cli.scan_concat_max.bytes_u64(), 2048);
    }

    #[test]
    fn wal_fetch_limits_preserve_defaults_and_reject_invalid_values() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
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
                Cli::try_parse_from(["crabka-traces", "--target", "block-builder", flag, invalid,])
                    .is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn wal_fetch_limits_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TRACES_WAL_FETCH_LIMITS_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_fetch_limits_read_environment_and_prefer_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_WAL_FETCH_MAX", "1KiB")
                    .env("CRABKA_TRACES_WAL_FETCH_PARTITION_MAX", "256B")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_fetch_max.bytes_i32(), 1024);
        assert_eq!(from_env.wal_fetch_partition_max.bytes_i32(), 256);

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
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
    fn parses_block_builder_flush_knobs() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "block-builder",
            "--block-builder-flush-max-records",
            "1000",
            "--block-builder-flush-max-age-ms",
            "30000",
        ])
        .unwrap();

        check!(
            (
                cli.target,
                cli.block_builder_flush_max_records,
                cli.block_builder_flush_max_age,
            ) == (Target::BlockBuilder, 1000, secs(30))
        );
    }

    #[test]
    fn parses_block_builder_promoted_attrs() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "block-builder",
            "--promote-resource-attr",
            "service.name:string",
            "--promote-span-attr",
            "http.status_code:int",
            "--promote-span-attr",
            "http.method",
        ])
        .unwrap();

        let promoted = promoted_attrs_from_cli(&cli).unwrap();
        check!(
            promoted
                == vec![
                    crabka_blockstore::PromotedSpanAttr::string("__resource.service.name"),
                    crabka_blockstore::PromotedSpanAttr::int("http.status_code"),
                    crabka_blockstore::PromotedSpanAttr::string("http.method"),
                ]
        );
    }

    #[test]
    fn rejects_unknown_promoted_attr_type() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "block-builder",
            "--promote-span-attr",
            "http.method:bytes",
        ])
        .unwrap();

        assert2::assert!(promoted_attrs_from_cli(&cli).is_err());
    }

    #[test]
    fn rejects_unknown_target() {
        assert2::assert!(Cli::try_parse_from(["crabka-traces", "--target", "bogus"]).is_err());
    }

    #[test]
    fn parses_live_store_retention() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "live-store",
            "--retention-ns",
            "42",
        ])
        .unwrap();
        assert2::assert!(matches!(cli.target, Target::LiveStore));
        assert2::assert!(cli.retention == Time::from_nanos(42));
    }

    #[test]
    fn parses_querier_live_store_option() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--querier-live-store",
            "--retention-ns",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.querier_live_store);
        assert2::assert!(cli.retention == Time::from_nanos(42));
    }

    #[test]
    fn parses_querier_remote_live_store_url() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--querier-live-store-url",
            "http://127.0.0.1:3201",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.querier_live_store_url.as_deref() == Some("http://127.0.0.1:3201"));
    }

    #[tokio::test]
    async fn live_store_router_serves_recent_trace_by_id() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(crabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([7; 16], [3; 8]),
        });
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/07070707070707070707070707070707")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == HttpStatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "COMPLETE");
        check!(
            json["trace"]["resourceSpans"][0]["resource"]["attributes"][0]["key"] == "service.name"
        );
        check!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] == "GET /live"
        );
    }

    #[tokio::test]
    async fn remote_live_source_reads_batches_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(crabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([8; 16], [4; 8]),
        });
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            crabka_blockstore::TraceBlockStats {
                object_key: "blocks/cold.parquet".into(),
                min_ts: 0,
                max_ts: 999,
                bloom: crabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(index)),
        );

        let batches = source.span_batches("tenant-a", 1_000, 2_000).await.unwrap();

        assert2::assert!(source.block_builder_frontier_ns("tenant-a") == 1_000);
        assert2::assert!(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>()
                == 1
        );
        server.abort();
    }

    #[tokio::test]
    async fn remote_live_source_reads_trace_by_id_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(crabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([9; 16], [5; 8]),
        });
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
        );

        let trace = source
            .trace_spans("tenant-a", &[9; 16])
            .await
            .unwrap()
            .unwrap();

        check!(trace.trace_id == [9; 16]);
        check!(trace.root_service_name == "live-api");
        check!(trace.spans[0].span_id == [5; 8]);
        check!(trace.spans[0].name == "GET /live");
        server.abort();
    }

    #[tokio::test]
    async fn remote_live_source_reads_tags_and_values_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(crabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([11; 16], [7; 8]),
        });
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
        );

        let tags = source
            .tag_names(
                "tenant-a",
                Some(crabka_traceql::TagScope::Resource),
                0,
                2_000,
            )
            .await
            .unwrap();
        let values = source
            .tag_values("tenant-a", "resource.service.name", 0, 2_000)
            .await
            .unwrap();

        assert2::assert!(
            tags.iter()
                .any(|scope| scope.tags.iter().any(|tag| tag == "service.name"))
        );
        assert2::assert!(values.iter().any(|value| value.value == "live-api"));
        server.abort();
    }

    #[tokio::test]
    async fn querier_router_federates_remote_live_store_by_id() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(crabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([10; 16], [6; 8]),
        });
        let live_cli = Cli::try_parse_from(["crabka-traces", "--target", "live-store"]).unwrap();
        let live_router = build_live_store_router(&live_cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, live_router).await.unwrap();
        });
        let querier_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--querier-live-store-url",
            &format!("http://{addr}"),
        ])
        .unwrap();
        let router = build_querier_router(&querier_cli).await.unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status() == HttpStatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["spanId"]
                == "BgYGBgYGBgY="
        );
        server.abort();
    }

    #[tokio::test]
    async fn indexed_live_source_uses_trace_index_max_timestamp_as_frontier() {
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            crabka_blockstore::TraceBlockStats {
                object_key: "blocks/a.parquet".into(),
                min_ts: 100,
                max_ts: 499,
                bloom: crabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        index.add_trace_block(
            "tenant-a",
            crabka_blockstore::TraceBlockStats {
                object_key: "blocks/b.parquet".into(),
                min_ts: 500,
                max_ts: 750,
                bloom: crabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        let source = IndexedLiveSource::new(
            Arc::new(RwLock::new(LiveStore::new(i64::MAX))),
            Arc::new(ArcSwap::from_pointee(index)),
        );

        assert2::assert!(source.block_builder_frontier_ns("tenant-a") == 751);
        assert2::assert!(source.block_builder_frontier_ns("tenant-b") == 0);
    }

    fn test_span(trace_id: [u8; 16], span_id: [u8; 8]) -> crabka_traces::Span {
        crabka_traces::Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "GET /live".into(),
            kind: crabka_traces::SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: crabka_traces::StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![crabka_traces::KeyValue {
                key: "service.name".into(),
                value: crabka_traces::AttrValue::Str("live-api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[test]
    fn parses_metrics_generator_options() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "metrics-generator",
            "--remote-write-url",
            "http://mimir.example/api/v1/push",
            "--collection-interval-secs",
            "30",
            "--max-exemplars-per-series",
            "3",
            "--edge-ttl-secs",
            "20",
            "--edge-store-max-items",
            "1234",
            "--histogram-buckets-ns",
            "1000,2000,5000",
            "--config",
            "metricsgen.yaml",
        ])
        .unwrap();

        check!(
            (
                cli.target,
                cli.remote_write_url.as_deref(),
                cli.collection_interval,
                cli.max_exemplars_per_series,
                cli.edge_ttl,
                cli.edge_store_max_items,
                cli.histogram_buckets,
                cli.config.as_deref(),
            ) == (
                Target::MetricsGenerator,
                Some("http://mimir.example/api/v1/push"),
                Some(secs(30)),
                Some(3),
                Some(secs(20)),
                Some(1234),
                Some(vec![
                    Time::from_nanos(1000),
                    Time::from_nanos(2000),
                    Time::from_nanos(5000)
                ]),
                Some("metricsgen.yaml"),
            )
        );
    }

    #[test]
    fn duration_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_DURATION_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::duration_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_RETENTION", "42s")
                    .env("CRABKA_TRACES_BLOCK_BUILDER_WINDOW", "7s")
                    .env("CRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_AGE", "8s")
                    .env("CRABKA_TRACES_COLLECTION_INTERVAL", "9s")
                    .env("CRABKA_TRACES_EDGE_TTL", "10s")
                    .env("CRABKA_TRACES_HISTOGRAM_BUCKETS", "1ms,2ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-traces", "--target=metrics-generator"]).unwrap();
        check!(
            (
                from_env.retention,
                from_env.block_builder_window,
                from_env.block_builder_flush_max_age,
                from_env.collection_interval,
                from_env.edge_ttl,
                from_env.histogram_buckets,
            ) == (
                secs(42),
                secs(7),
                secs(8),
                Some(secs(9)),
                Some(secs(10)),
                Some(vec![crabka_units::millis(1), crabka_units::millis(2)]),
            )
        );

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=metrics-generator",
            "--retention=11s",
            "--block-builder-window=12s",
            "--block-builder-flush-max-age=13s",
            "--collection-interval=14s",
            "--edge-ttl=15s",
            "--histogram-buckets=3ms,4ms",
        ])
        .unwrap();
        check!(
            (
                from_cli.retention,
                from_cli.block_builder_window,
                from_cli.block_builder_flush_max_age,
                from_cli.collection_interval,
                from_cli.edge_ttl,
                from_cli.histogram_buckets,
            ) == (
                secs(11),
                secs(12),
                secs(13),
                Some(secs(14)),
                Some(secs(15)),
                Some(vec![crabka_units::millis(3), crabka_units::millis(4)]),
            )
        );
    }

    #[test]
    fn byte_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_BYTE_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::byte_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_TARGET_BYTES_PER_JOB", "1KiB")
                    .env("CRABKA_TRACES_MAX_ATTR_VALUE_LEN", "2KiB")
                    .env("CRABKA_TRACES_MAX_DECOMPRESSED_BYTES", "3KiB")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target=query-frontend"]).unwrap();
        check!(
            (
                from_env.target_bytes_per_job,
                from_env.max_attr_value_len,
                from_env.max_decompressed_bytes,
            ) == (kibibytes(1), kibibytes(2), kibibytes(3))
        );
        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=query-frontend",
            "--target-bytes-per-job=4KiB",
            "--max-attr-value-len=5KiB",
            "--max-decompressed-bytes=6KiB",
        ])
        .unwrap();
        check!(
            (
                from_cli.target_bytes_per_job,
                from_cli.max_attr_value_len,
                from_cli.max_decompressed_bytes,
            ) == (kibibytes(4), kibibytes(5), kibibytes(6))
        );
    }

    #[test]
    fn parses_metrics_generator_optional_spanmetrics_switches() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "metrics-generator",
            "--enable-target-info",
            "--enable-status-message",
            "--enable-messaging-system-latency",
        ])
        .unwrap();

        check!(
            (
                cli.metrics.enable_target_info,
                cli.metrics.enable_status_message,
                cli.metrics.enable_messaging_system_latency,
            ) == (true, true, true)
        );
    }

    #[test]
    fn metrics_generator_poll_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_METRICS_GENERATOR_POLL_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::metrics_generator_poll_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_METRICS_GENERATOR_POLL_BATCH_SIZE", "7")
                    .env("CRABKA_TRACES_METRICS_GENERATOR_POLL_ERROR_BACKOFF", "11ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-traces", "--target=metrics-generator"]).unwrap();
        check!(
            (
                from_env.metrics_generator_poll_batch_size,
                from_env.metrics_generator_poll_error_backoff
            ) == (7, crabka_units::millis(11))
        );
        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=metrics-generator",
            "--metrics-generator-poll-batch-size=13",
            "--metrics-generator-poll-error-backoff=17ms",
        ])
        .unwrap();
        check!(
            (
                from_cli.metrics_generator_poll_batch_size,
                from_cli.metrics_generator_poll_error_backoff
            ) == (13, crabka_units::millis(17))
        );
        for flag in [
            "--metrics-generator-poll-batch-size=0",
            "--metrics-generator-poll-error-backoff=0ms",
        ] {
            check!(
                Cli::try_parse_from(["crabka-traces", "--target=metrics-generator", flag]).is_err()
            );
        }
    }

    #[test]
    fn metrics_generator_config_preserves_file_values_without_cli_overrides() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "metrics-generator"]).unwrap();
        let mut cfg = MetricsGenConfig {
            collection_interval: secs(30),
            max_exemplars_per_series: 5,
            edge_ttl: minutes(1),
            edge_store_max_items: 2_000,
            histogram_buckets_ns: vec![1_000.0, 2_000.0],
            remote_write_url: "http://metrics.example/api/v1/push".into(),
            ..MetricsGenConfig::default()
        };

        apply_metrics_generator_cli_overrides(&mut cfg, &cli);

        check!(
            (
                cfg.collection_interval,
                cfg.max_exemplars_per_series,
                cfg.edge_ttl,
                cfg.edge_store_max_items,
                cfg.histogram_buckets_ns.as_slice(),
                cfg.remote_write_url.as_str(),
            ) == (
                secs(30),
                5,
                minutes(1),
                2_000,
                &[1_000.0, 2_000.0][..],
                "http://metrics.example/api/v1/push",
            )
        );

        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "metrics-generator",
            "--collection-interval-secs",
            "45",
            "--max-exemplars-per-series",
            "2",
            "--edge-ttl-secs",
            "9",
            "--edge-store-max-items",
            "77",
            "--histogram-buckets-ns",
            "500,1000,2500",
            "--remote-write-url",
            "http://override.example/api/v1/push",
        ])
        .unwrap();

        apply_metrics_generator_cli_overrides(&mut cfg, &cli);

        check!(
            (
                cfg.collection_interval,
                cfg.max_exemplars_per_series,
                cfg.edge_ttl,
                cfg.edge_store_max_items,
                cfg.histogram_buckets_ns.as_slice(),
                cfg.remote_write_url.as_str(),
            ) == (
                secs(45),
                2,
                secs(9),
                77,
                &[500.0, 1_000.0, 2_500.0][..],
                "http://override.example/api/v1/push",
            )
        );
    }

    #[tokio::test]
    async fn builds_querier_router_from_defaults() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();

        assert2::assert!(build_querier_router(&cli).await.is_ok());
    }

    #[tokio::test]
    async fn parses_querier_trace_span_limit() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--max-trace-spans",
            "100",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_trace_spans == 100);
        check!(build_querier_router(&cli).await.is_ok());
    }

    #[test]
    fn tag_query_filter_autocomplete_limit_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(
                std::env::current_exe().expect("test executable"),
            )
            .args([
                "--exact",
                "tests::tag_query_filter_autocomplete_limit_reads_environment_and_prefers_cli",
            ])
            .env(CHILD, "1")
            .env("CRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT", "7")
            .status()
            .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target=querier"]).unwrap();
        check!(from_env.tag_query_filter_autocomplete_limit == 7);
        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=querier",
            "--tag-query-filter-autocomplete-limit=11",
        ])
        .unwrap();
        check!(from_cli.tag_query_filter_autocomplete_limit == 11);
        check!(
            Cli::try_parse_from([
                "crabka-traces",
                "--target=querier",
                "--tag-query-filter-autocomplete-limit=0",
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn parses_querier_search_trace_limit() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--max-search-traces",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_search_traces == 42);
        check!(build_querier_router(&cli).await.is_ok());
    }

    #[test]
    fn parses_querier_traceql_metric_exemplar_limit() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--max-metric-exemplars",
            "7",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_metric_exemplars == 7);
        check!(engine_opts_from_cli(&cli).unwrap().max_exemplars == 7);
    }

    #[test]
    fn traceql_policy_parses_defaults_overrides_and_boundaries() {
        let defaults = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();
        check!(engine_opts_from_cli(&defaults).unwrap() == EngineOpts::default());

        let configured = Cli::try_parse_from([
            "crabka-traces",
            "--target=querier",
            "--traceql-default-limit=5",
            "--traceql-default-spans-per-span-set=7",
            "--max-search-traces=11",
            "--max-metric-exemplars=13",
            "--traceql-compare-max-values-per-attr=17",
            "--traceql-histogram-buckets=19ms,23ms",
        ])
        .unwrap();
        check!(
            engine_opts_from_cli(&configured).unwrap()
                == EngineOpts {
                    default_limit: 5,
                    default_spss: 7,
                    max_traces: 11,
                    max_exemplars: 13,
                    compare_max_values_per_attr: 17,
                    histogram_buckets: vec![crabka_units::millis(19), crabka_units::millis(23)],
                }
        );

        for flag in [
            "--traceql-default-limit=0",
            "--traceql-default-spans-per-span-set=0",
            "--max-search-traces=0",
            "--traceql-compare-max-values-per-attr=0",
            "--traceql-histogram-buckets=0ms",
        ] {
            check!(
                Cli::try_parse_from(["crabka-traces", "--target=querier", flag]).is_err(),
                "accepted {flag}"
            );
        }
        let unordered = Cli::try_parse_from([
            "crabka-traces",
            "--target=querier",
            "--traceql-histogram-buckets=23ms,19ms",
        ])
        .unwrap();
        check!(engine_opts_from_cli(&unordered).is_err());
    }

    #[test]
    fn traceql_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_TRACEQL_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::traceql_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_TRACEQL_DEFAULT_LIMIT", "5")
                    .env("CRABKA_TRACES_TRACEQL_DEFAULT_SPANS_PER_SPAN_SET", "7")
                    .env("CRABKA_TRACES_TRACEQL_MAX_TRACES", "11")
                    .env("CRABKA_TRACES_TRACEQL_MAX_EXEMPLARS", "13")
                    .env("CRABKA_TRACES_TRACEQL_COMPARE_MAX_VALUES_PER_ATTR", "17")
                    .env("CRABKA_TRACES_TRACEQL_HISTOGRAM_BUCKETS", "19ms,23ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target=querier"]).unwrap();
        check!(engine_opts_from_cli(&from_env).unwrap().default_limit == 5);
        check!(
            engine_opts_from_cli(&from_env).unwrap().histogram_buckets
                == vec![crabka_units::millis(19), crabka_units::millis(23)]
        );

        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=querier",
            "--traceql-default-limit=29",
            "--traceql-histogram-buckets=31ms,37ms",
        ])
        .unwrap();
        check!(engine_opts_from_cli(&from_cli).unwrap().default_limit == 29);
        check!(
            engine_opts_from_cli(&from_cli).unwrap().histogram_buckets
                == vec![crabka_units::millis(31), crabka_units::millis(37)]
        );
    }

    #[test]
    fn parses_distributor_trace_span_limit() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--max-spans-per-trace",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.max_spans_per_trace == 42);
    }

    #[test]
    fn parses_distributor_ingest_rate_limit() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "distributor",
            "--max-ingest-spans-per-second",
            "42",
            "--ingest-rate-burst",
            "7",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.max_ingest_spans_per_second == 42);
        assert2::assert!(cli.ingest_rate_burst == 7);
    }

    #[test]
    fn parses_compactor_window() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "compactor",
            "--compaction-start-ns",
            "100",
            "--compaction-end-ns",
            "200",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Compactor));
        assert2::assert!(cli.compaction_start == UnixNano(100));
        assert2::assert!(cli.compaction_end == UnixNano(200));
    }

    #[test]
    fn unix_time_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TRACES_UNIX_TIME_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::unix_time_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_TRACES_COMPACTION_START", "1s")
                    .env("CRABKA_TRACES_COMPACTION_END", "2s")
                    .env("CRABKA_TRACES_LIVE_FRONTIER", "3s")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-traces", "--target=compactor"]).unwrap();
        check!(
            (
                from_env.compaction_start,
                from_env.compaction_end,
                from_env.live_frontier,
            ) == (
                UnixNano(1_000_000_000),
                UnixNano(2_000_000_000),
                Some(UnixNano(3_000_000_000)),
            )
        );
        let from_cli = Cli::try_parse_from([
            "crabka-traces",
            "--target=compactor",
            "--compaction-start=4s",
            "--compaction-end=5s",
            "--live-frontier=6s",
        ])
        .unwrap();
        check!(
            (
                from_cli.compaction_start,
                from_cli.compaction_end,
                from_cli.live_frontier,
            ) == (
                UnixNano(4_000_000_000),
                UnixNano(5_000_000_000),
                Some(UnixNano(6_000_000_000)),
            )
        );
    }

    #[test]
    fn parses_object_store_url_and_builds_store() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "querier",
            "--object-store-url",
            "memory:///tempo/traces",
        ])
        .unwrap();

        check!(cli.object_store_url == "memory:///tempo/traces");
        let configured = build_object_store(&cli).unwrap();
        assert2::assert!(&configured.root == &Url::parse("memory:///tempo/traces").unwrap());
        assert2::assert!(configured.prefix.to_string() == "tempo/traces".to_string());
        assert2::assert!(
            configured.object_key("index/traces.json")
                == "tempo/traces/index/traces.json".to_string()
        );
        assert2::assert!(
            configured.object_key("traces/tenant-a/block.parquet")
                == "tempo/traces/traces/tenant-a/block.parquet".to_string()
        );
    }

    #[tokio::test]
    async fn parses_query_frontend_options_and_builds_router() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "query-frontend",
            "--querier-url",
            "http://querier-a.example:3200,http://querier-b.example:3200",
            "--live-frontier-ns",
            "60000000000",
            "--query-queue-depth",
            "4",
            "--target-bytes-per-job",
            "4096",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::QueryFrontend));
        assert2::assert!(
            cli.querier_url.as_str()
                == "http://querier-a.example:3200,http://querier-b.example:3200"
        );
        assert2::assert!(cli.live_frontier == Some(UnixNano(60_000_000_000)));
        assert2::assert!(cli.query_queue_depth == 4);
        assert2::assert!(cli.target_bytes_per_job == ByteSize::from_bytes(4096));
        check!(build_query_frontend_router(&cli).await.is_ok());
    }
}
