use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use crabka_blockstore::{BlockStore, BlockWriter, PromotedSpanAttr, TraceIndex};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::Producer;
use crabka_traceql::{EngineOpts, TraceqlEngine};
use crabka_traces::{
    LiveStore, TRACES_WAL_TOPIC, blockbuilder,
    compactor::compact_index_window,
    distributor::{self, DistributorState, KafkaSink},
    livestore,
    metricsgen::{
        KafkaSpanSource, MetricsGenConfig, MetricsGenService, PrometheusRemoteWriteSink,
        SystemClock,
    },
    querier::{self as trace_querier, http::HttpConfig, store::CrabkaSpanStore},
    query_frontend::{self, QueryFrontendConfig, backend_blocks_by_tenant_from_trace_index},
    span::batch::RESOURCE_ATTR_PREFIX,
};
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "crabka-traces")]
#[command(about = "Tempo-compatible traces service for Crabka")]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:3200")]
    listen: String,
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
    #[arg(long, default_value = "index/traces.json")]
    trace_index_key: String,
    #[arg(long, default_value = "memory:///")]
    object_store_url: String,
    #[arg(long, default_value = "http://localhost:9009/api/v1/push")]
    remote_write_url: String,
    #[arg(long, default_value_t = 15)]
    collection_interval_secs: u64,
    #[arg(long)]
    enable_target_info: bool,
    #[arg(long)]
    enable_status_message: bool,
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
    let shutdown = CancellationToken::new();
    let shutdown_task = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown_task.cancel();
        }
    });

    match cli.target {
        Target::Distributor => run_distributor(cli, shutdown).await?,
        Target::BlockBuilder => run_block_builder(cli, shutdown).await?,
        Target::LiveStore => run_live_store(cli, shutdown).await?,
        Target::Querier => run_querier(cli, shutdown).await?,
        Target::QueryFrontend => run_query_frontend(cli, shutdown).await?,
        Target::Compactor => run_compactor(cli).await?,
        Target::MetricsGenerator => run_metrics_generator(cli, shutdown).await?,
    }
    Ok(())
}

async fn run_distributor(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let producer = Producer::builder().bootstrap(cli.bootstrap).build().await?;
    let mut state = DistributorState::new(Arc::new(KafkaSink::new(Arc::new(producer))));
    state.limits.max_spans_per_request = cli.max_spans_per_request;
    state.limits.max_spans_per_trace = cli.max_spans_per_trace;
    state.limits.max_ingest_spans_per_second = cli.max_ingest_spans_per_second;
    state.limits.ingest_rate_burst = cli.ingest_rate_burst;
    state.limits.max_attr_value_len = cli.max_attr_value_len;
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
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let promoted_attrs = promoted_attrs_from_cli(&cli)?;
    let consumer = wal_consumer(cli.bootstrap.clone(), "crabka-traces-block-builder").await?;
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    blockbuilder::run(
        consumer,
        writer,
        index,
        configured.store,
        blockbuilder::BlockBuilderConfig {
            object_key_prefix,
            index_key: trace_index_key,
            window: Duration::from_secs(5),
            promoted_attrs,
        },
        shutdown,
    )
    .await?;
    Ok(())
}

async fn run_live_store(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let consumer = wal_consumer(cli.bootstrap, "crabka-traces-live-store").await?;
    let store = Arc::new(RwLock::new(LiveStore::new(cli.retention_ns)));
    livestore::run(consumer, store, shutdown).await?;
    Ok(())
}

async fn run_querier(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let router = build_querier_router(&cli).await?;
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

async fn build_querier_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let configured = build_object_store(cli)?;
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let trace_index = Arc::new(
        TraceIndex::load(&configured.store, &trace_index_key)
            .await
            .unwrap_or_else(|_| TraceIndex::new()),
    );
    let blocks = Arc::new(BlockStore::new(configured.store, configured.root));
    let store = Arc::new(CrabkaSpanStore::new(blocks, trace_index, None));
    let engine = Arc::new(TraceqlEngine::new(
        store,
        EngineOpts {
            max_traces: cli.max_search_traces,
            ..EngineOpts::default()
        },
    ));
    Ok(trace_querier::http::router_with_config(
        engine,
        HttpConfig {
            max_trace_spans: cli.max_trace_spans,
        },
    ))
}

async fn run_query_frontend(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let router = build_query_frontend_router(&cli).await?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "traces query-frontend listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = QueryFrontendConfig::new(&cli.querier_url)?;
    cfg.live_frontier_ns = cli.live_frontier_ns;
    cfg.max_queue_depth = cli.query_queue_depth;
    cfg.target_bytes_per_job = cli.target_bytes_per_job;
    if cli.target_bytes_per_job > 0 {
        let configured = build_object_store(cli)?;
        let trace_index_key = configured.object_key(&cli.trace_index_key);
        let trace_index = TraceIndex::load(&configured.store, &trace_index_key)
            .await
            .unwrap_or_else(|_| TraceIndex::new());
        let blocks = BlockStore::new(configured.store, configured.root);
        cfg.backend_blocks_by_tenant =
            backend_blocks_by_tenant_from_trace_index(&blocks, &trace_index)
                .await
                .unwrap_or_default();
    }
    Ok(query_frontend::router(cfg))
}

async fn run_compactor(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configured = build_object_store(&cli)?;
    let writer = BlockWriter::new(configured.store.clone());
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let mut index = TraceIndex::load(&configured.store, &trace_index_key)
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
    index.save(&configured.store, &trace_index_key).await?;
    Ok(())
}

fn build_object_store(
    cli: &Cli,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let root = Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url(&root)?;
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
    cfg.collection_interval = Duration::from_secs(cli.collection_interval_secs);
    cfg.remote_write_url = cli.remote_write_url.clone();
    cfg.enable_target_info |= cli.enable_target_info;
    cfg.enable_status_message |= cli.enable_status_message;

    let consumer = wal_consumer(cli.bootstrap, "crabka-traces-metrics-generator").await?;
    let source = Arc::new(KafkaSpanSource::new(consumer));
    let sink = Arc::new(PrometheusRemoteWriteSink::new(cli.remote_write_url));
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink);
    service.run(shutdown).await;
    Ok(())
}

async fn wal_consumer(
    bootstrap: String,
    group_id: &str,
) -> Result<Consumer, crabka_client_consumer::ConsumerError> {
    Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group_id.to_string())
        .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "distributor"]).unwrap();
        assert!(matches!(cli.target, Target::Distributor));
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

        assert!(matches!(cli.target, Target::Distributor));
        assert!(cli.grpc_listen == "127.0.0.1:4317");
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

        assert!(matches!(cli.target, Target::Distributor));
        assert!(cli.jaeger_compact_listen == "127.0.0.1:6831");
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

        assert!(matches!(cli.target, Target::Distributor));
        assert!(cli.jaeger_grpc_listen == "127.0.0.1:14250");
    }

    #[test]
    fn distributor_defaults_include_tempo_push_ports() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "distributor"]).unwrap();

        assert!(cli.otlp_http_listen == "127.0.0.1:4318");
        assert!(cli.jaeger_grpc_listen == "127.0.0.1:14250");
        assert!(cli.jaeger_http_listen == "127.0.0.1:14268");
        assert!(cli.zipkin_listen == "127.0.0.1:9411");
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

        assert!(cli.max_spans_per_request == 123);
        assert!(cli.max_attr_value_len == 456);
        assert!(cli.max_decompressed_bytes == 789);
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
        assert!(matches!(cli.target, Target::BlockBuilder));
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
        assert!(
            promoted[0] == crabka_blockstore::PromotedSpanAttr::string("__resource.service.name")
        );
        assert!(promoted[1] == crabka_blockstore::PromotedSpanAttr::int("http.status_code"));
        assert!(promoted[2] == crabka_blockstore::PromotedSpanAttr::string("http.method"));
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

        assert!(promoted_attrs_from_cli(&cli).is_err());
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-traces", "--target", "bogus"]).is_err());
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
        assert!(matches!(cli.target, Target::LiveStore));
        assert!(cli.retention_ns == 42);
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
            "--config",
            "metricsgen.yaml",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::MetricsGenerator));
        assert!(cli.remote_write_url == "http://mimir.example/api/v1/push");
        assert!(cli.collection_interval_secs == 30);
        assert!(cli.config.as_deref() == Some("metricsgen.yaml"));
    }

    #[test]
    fn parses_metrics_generator_optional_spanmetrics_switches() {
        let cli = Cli::try_parse_from([
            "crabka-traces",
            "--target",
            "metrics-generator",
            "--enable-target-info",
            "--enable-status-message",
        ])
        .unwrap();

        assert!(cli.enable_target_info);
        assert!(cli.enable_status_message);
    }

    #[tokio::test]
    async fn builds_querier_router_from_defaults() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "querier"]).unwrap();

        assert!(build_querier_router(&cli).await.is_ok());
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

        assert!(matches!(cli.target, Target::Querier));
        assert!(cli.max_trace_spans == 100);
        assert!(build_querier_router(&cli).await.is_ok());
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

        assert!(matches!(cli.target, Target::Querier));
        assert!(cli.max_search_traces == 42);
        assert!(build_querier_router(&cli).await.is_ok());
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

        assert!(matches!(cli.target, Target::Distributor));
        assert!(cli.max_spans_per_trace == 42);
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

        assert!(matches!(cli.target, Target::Distributor));
        assert!(cli.max_ingest_spans_per_second == 42);
        assert!(cli.ingest_rate_burst == 7);
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

        assert!(matches!(cli.target, Target::Compactor));
        assert!(cli.compaction_start_ns == 100);
        assert!(cli.compaction_end_ns == 200);
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

        assert!(cli.object_store_url == "memory:///tempo/traces");
        let configured = build_object_store(&cli).unwrap();
        assert!(configured.root == Url::parse("memory:///tempo/traces").unwrap());
        assert!(configured.prefix.to_string() == "tempo/traces");
        assert!(configured.object_key("index/traces.json") == "tempo/traces/index/traces.json");
        assert!(
            configured.object_key("traces/tenant-a/block.parquet")
                == "tempo/traces/traces/tenant-a/block.parquet"
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

        assert!(matches!(cli.target, Target::QueryFrontend));
        assert!(cli.querier_url == "http://querier-a.example:3200,http://querier-b.example:3200");
        assert!(cli.live_frontier_ns == Some(60_000_000_000));
        assert!(cli.query_queue_depth == 4);
        assert!(cli.target_bytes_per_job == 4096);
        assert!(build_query_frontend_router(&cli).await.is_ok());
    }
}
