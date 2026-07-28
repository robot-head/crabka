#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{net::SocketAddr, process::ExitCode, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use clap::{ArgAction, Args, Parser, ValueEnum};
use crabka_blockstore::{BlockStore, BlockWriter, PromotedSpanAttr, TraceIndex};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::Producer;
use crabka_telemetry::OtlpConfig;
use crabka_traceql::{EngineOpts, TraceqlEngine};
use crabka_traces::{
    LiveStore, TRACES_WAL_TOPIC, blockbuilder,
    compactor::compact_index_window,
    distributor::{self, DistributorState, KafkaSink},
    frontend::{self, FrontendConfig, TraceIndexCatalog},
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
        store::{CrabkaSpanStore, SharedTraceIndex},
    },
    span::batch::RESOURCE_ATTR_PREFIX,
};
use object_store::{ObjectStore, path::Path};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use url::Url;

const WAL_FETCH_MAX_BYTES: i32 = 2 * 1024 * 1024;
const WAL_FETCH_PARTITION_MAX_BYTES: i32 = 256 * 1024;

#[derive(Debug, Parser)]
#[command(name = "crabka-traces")]
#[command(about = "Tempo-compatible traces service for Crabka")]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:3200")]
    listen: String,
    #[arg(long, env = "CRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    admin_listen_addr: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:4317")]
    grpc_listen: String,
    #[arg(long, default_value = "127.0.0.1:4318")]
    otlp_http_listen: String,
    #[arg(long, default_value = "127.0.0.1:14250")]
    jaeger_grpc_listen: String,
    #[arg(long, default_value = "127.0.0.1:6831")]
    jaeger_compact_listen: String,
    #[arg(long, default_value = "127.0.0.1:14268")]
    jaeger_http_listen: String,
    #[arg(long, default_value = "127.0.0.1:9411")]
    zipkin_listen: String,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, default_value_t = 30 * 60 * 1_000_000_000_i64)]
    retention_ns: i64,
    #[arg(long, default_value_t = 5)]
    block_builder_window_secs: u64,
    #[arg(long, default_value_t = crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS)]
    block_builder_flush_max_records: usize,
    #[arg(long, default_value_t = crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE.as_millis() as u64)]
    block_builder_flush_max_age_ms: u64,
    #[arg(long, action = ArgAction::SetTrue)]
    querier_live_store: bool,
    #[arg(long)]
    querier_live_store_url: Option<String>,
    #[arg(long, default_value = "index/traces.json")]
    trace_index_key: String,
    #[arg(long, default_value = "memory:///")]
    object_store_url: String,
    #[arg(long)]
    remote_write_url: Option<String>,
    #[arg(long)]
    collection_interval_secs: Option<u64>,
    #[arg(long)]
    max_exemplars_per_series: Option<usize>,
    #[arg(long)]
    edge_ttl_secs: Option<u64>,
    #[arg(long)]
    edge_store_max_items: Option<usize>,
    #[arg(long)]
    #[arg(value_delimiter = ',')]
    histogram_buckets_ns: Option<Vec<f64>>,
    #[command(flatten)]
    metrics: MetricsFlags,
    #[arg(long, default_value_t = 0)]
    compaction_start_ns: i64,
    #[arg(long, default_value_t = i64::MAX)]
    compaction_end_ns: i64,
    #[arg(long, default_value = "http://127.0.0.1:3200")]
    querier_url: String,
    #[arg(long)]
    live_frontier_ns: Option<i64>,
    #[arg(long, default_value_t = 128)]
    query_queue_depth: usize,
    #[arg(long, default_value_t = 0)]
    target_bytes_per_job: u64,
    #[arg(long, default_value_t = usize::MAX)]
    max_trace_spans: usize,
    #[arg(long, default_value_t = 1000)]
    max_search_traces: usize,
    #[arg(long, default_value_t = 0)]
    max_metric_exemplars: usize,
    #[arg(long, default_value_t = 10_000)]
    max_spans_per_request: usize,
    #[arg(long, default_value_t = usize::MAX)]
    max_spans_per_trace: usize,
    #[arg(long, default_value_t = usize::MAX)]
    max_ingest_spans_per_second: usize,
    #[arg(long, default_value_t = usize::MAX)]
    ingest_rate_burst: usize,
    #[arg(long = "promote-span-attr")]
    promote_span_attrs: Vec<String>,
    #[arg(long = "promote-resource-attr")]
    promote_resource_attrs: Vec<String>,
    #[arg(long, default_value_t = 64 * 1024)]
    max_attr_value_len: usize,
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    max_decompressed_bytes: usize,
    #[arg(long)]
    config: Option<String>,
}

#[derive(Debug, Args)]
struct MetricsFlags {
    #[arg(long)]
    enable_target_info: bool,
    #[arg(long)]
    enable_status_message: bool,
    #[arg(long)]
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
    match run(cli).await {
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
        ),
        "crabka_traces=info,info",
        "info",
        "crabka-traces",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin(
        cli.admin_listen_addr,
        crabka_traces::metrics::metrics_router(metrics.registry.clone()),
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
    let producer = Box::pin(Producer::builder().bootstrap(cli.bootstrap).build()).await?;
    let mut state =
        DistributorState::with_metrics(Arc::new(KafkaSink::new(Arc::new(producer))), metrics);
    state.limits.max_spans_per_request = cli.max_spans_per_request;
    state.limits.max_spans_per_trace = cli.max_spans_per_trace;
    state.limits.max_ingest_spans_per_second = cli.max_ingest_spans_per_second;
    state.limits.ingest_rate_burst = cli.ingest_rate_burst;
    state.limits.max_attr_value_len = cli.max_attr_value_len;
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
    )
    .await?;
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial_index = TraceIndex::load_latest_snapshot(&configured.store, &trace_index_key)
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
            window: Duration::from_secs(cli.block_builder_window_secs),
            promoted_attrs,
            flush_max_records: cli.block_builder_flush_max_records,
            flush_max_age: Duration::from_millis(cli.block_builder_flush_max_age_ms),
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
    let consumer = wal_consumer(cli.bootstrap.clone(), "crabka-traces-live-store", None).await?;
    let store = Arc::new(RwLock::new(LiveStore::new(cli.retention_ns)));
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
        .then(|| Arc::new(RwLock::new(LiveStore::new(cli.retention_ns))));
    let (router, store, trace_index_key, trace_index) =
        build_querier_router_with_live(&cli, metrics, live_store.clone()).await?;
    if let Some(live_store) = live_store {
        let consumer = wal_consumer(
            cli.bootstrap.clone(),
            "crabka-traces-querier-live-store",
            None,
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
    let refresh_interval = cli.block_builder_window_secs.max(1);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(refresh_interval));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = refresh_shutdown.cancelled() => break,
                _ = tick.tick() => {
                    if let Ok(idx) = TraceIndex::load_latest_snapshot(&refresh_store, &trace_index_key).await {
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
    let initial = TraceIndex::load_latest_snapshot(&configured.store, &trace_index_key)
        .await
        .unwrap_or_else(|_| TraceIndex::new());
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(initial));
    let blocks = Arc::new(BlockStore::new(
        Arc::clone(&configured.store),
        configured.root,
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
    let store = Arc::new(CrabkaSpanStore::new(blocks, Arc::clone(&trace_index), live));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)));
    let router = trace_querier::http::router_with_config_and_metrics(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
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
    let store = Arc::new(CrabkaSpanStore::new(blocks, trace_index, Some(live)));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)));
    let tempo_router = trace_querier::http::router_with_config(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
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

fn engine_opts_from_cli(cli: &Cli) -> EngineOpts {
    EngineOpts {
        max_traces: cli.max_search_traces,
        max_exemplars: cli.max_metric_exemplars,
        ..EngineOpts::default()
    }
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
/// stripped here. `--live-frontier-ns` maps to `hot_frontier_ns` (`None` => `0`,
/// i.e. the live tier is always probed).
fn frontend_config_from_cli(
    cli: &Cli,
    listen_addr: SocketAddr,
) -> Result<FrontendConfig, Box<dyn std::error::Error + Send + Sync>> {
    let querier_addrs = parse_querier_addrs(&cli.querier_url)?;
    Ok(FrontendConfig {
        querier_addrs,
        target_bytes_per_job: cli.target_bytes_per_job,
        max_concurrency: cli.query_queue_depth.max(1),
        hot_frontier_ns: cli.live_frontier_ns.unwrap_or(0),
        max_trace_bytes: u64::try_from(cli.max_trace_spans)
            .unwrap_or(u64::MAX)
            .saturating_mul(64 * 1024),
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
    if cli.target_bytes_per_job == 0 {
        return Ok(TraceIndexCatalog::new(std::collections::BTreeMap::new()));
    }
    let configured = build_object_store(cli)?;
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let trace_index = TraceIndex::load_latest_snapshot(&configured.store, &trace_index_key)
        .await
        .unwrap_or_else(|_| TraceIndex::new());
    let blocks = BlockStore::new(configured.store, configured.root);
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
    let backend = HttpQuerier::new(cfg.querier_addrs.clone(), cfg.request_timeout)?;
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
    let mut index = TraceIndex::load_latest_snapshot(&configured.store, &trace_index_key)
        .await
        .unwrap_or_else(|_| TraceIndex::new());
    compact_index_window(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        cli.compaction_start_ns,
        cli.compaction_end_ns,
    )
    .await?;
    index
        .save_latest_snapshot(&configured.store, &trace_index_key)
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

    let consumer = wal_consumer(cli.bootstrap, "crabka-traces-metrics-generator", None).await?;
    let source = Arc::new(KafkaSpanSource::new(consumer));
    let sink = Arc::new(PrometheusRemoteWriteSink::new(cfg.remote_write_url.clone()));
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink);
    service.run(shutdown).await;
    Ok(())
}

fn apply_metrics_generator_cli_overrides(cfg: &mut MetricsGenConfig, cli: &Cli) {
    if let Some(secs) = cli.collection_interval_secs {
        cfg.collection_interval = Duration::from_secs(secs);
    }
    if let Some(url) = &cli.remote_write_url {
        cfg.remote_write_url.clone_from(url);
    }
    if let Some(max) = cli.max_exemplars_per_series {
        cfg.max_exemplars_per_series = max;
    }
    if let Some(secs) = cli.edge_ttl_secs {
        cfg.edge_ttl = Duration::from_secs(secs);
    }
    if let Some(max) = cli.edge_store_max_items {
        cfg.edge_store_max_items = max;
    }
    if let Some(buckets) = &cli.histogram_buckets_ns {
        cfg.histogram_buckets_ns.clone_from(buckets);
    }
    cfg.enable_target_info |= cli.metrics.enable_target_info;
    cfg.enable_status_message |= cli.metrics.enable_status_message;
    cfg.enable_messaging_system_latency |= cli.metrics.enable_messaging_system_latency;
}

async fn wal_consumer(
    bootstrap: String,
    group_id: &str,
    group_instance_id: Option<&str>,
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
            .fetch_max_bytes(WAL_FETCH_MAX_BYTES)
            .fetch_partition_max_bytes(WAL_FETCH_PARTITION_MAX_BYTES)
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
    use clap::Parser;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

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
        assert2::assert!(cli.max_attr_value_len == 456);
        assert2::assert!(cli.max_decompressed_bytes == 789);
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
        assert2::assert!(cli.block_builder_window_secs == 30);
    }

    #[test]
    fn block_builder_flush_knobs_default() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();

        assert2::assert!(
            cli.block_builder_flush_max_records
                == crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS
        );
        assert2::assert!(
            cli.block_builder_flush_max_age_ms
                == u64::try_from(crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE.as_millis())
                    .unwrap()
        );
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
                cli.block_builder_flush_max_age_ms,
            ) == (Target::BlockBuilder, 1000, 30_000)
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
        assert2::assert!(cli.retention_ns == 42);
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
        assert2::assert!(cli.retention_ns == 42);
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
                cli.collection_interval_secs,
                cli.max_exemplars_per_series,
                cli.edge_ttl_secs,
                cli.edge_store_max_items,
                cli.histogram_buckets_ns,
                cli.config.as_deref(),
            ) == (
                Target::MetricsGenerator,
                Some("http://mimir.example/api/v1/push"),
                Some(30),
                Some(3),
                Some(20),
                Some(1234),
                Some(vec![1000.0, 2000.0, 5000.0]),
                Some("metricsgen.yaml"),
            )
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
    fn metrics_generator_config_preserves_file_values_without_cli_overrides() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "metrics-generator"]).unwrap();
        let mut cfg = MetricsGenConfig {
            collection_interval: Duration::from_secs(30),
            max_exemplars_per_series: 5,
            edge_ttl: Duration::from_mins(1),
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
                Duration::from_secs(30),
                5,
                Duration::from_mins(1),
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
                Duration::from_secs(45),
                2,
                Duration::from_secs(9),
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
        check!(engine_opts_from_cli(&cli).max_exemplars == 7);
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
        assert2::assert!(cli.compaction_start_ns == 100);
        assert2::assert!(cli.compaction_end_ns == 200);
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
        assert2::assert!(cli.live_frontier_ns == Some(60_000_000_000));
        assert2::assert!(cli.query_queue_depth == 4);
        assert2::assert!(cli.target_bytes_per_job == 4096);
        check!(build_query_frontend_router(&cli).await.is_ok());
    }
}
