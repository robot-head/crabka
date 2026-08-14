// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, ValueEnum};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_client_producer::Producer;
use crabka_metrics::{OverridesProvider, WAL_TOPIC};
use crabka_metrics_service::{
    KafkaRecordingRuleWalSink, KafkaRulerStateSink, PrometheusRulerStateSink, RULER_STATE_TOPIC,
    RulerAlertmanagerSink, RulerStateFanoutSink, WalHeadConsumerCommit, WalHeadConsumerPoll,
    run_ruler_evaluation_loop, run_ruler_state_consumer_loop, run_wal_head_consumer_loop,
    serve_prometheus_router_joinable,
};
use crabka_promql::{
    EngineOpts, PrometheusApiState, QueryFrontendOptions, RulerShard, WalHead, prometheus_router,
};
use crabka_telemetry::OtlpConfig;
use crabka_units::{parse, prelude::*};
use object_store::ObjectStore;

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "CRABKA_METRICS_SERVICE_TARGET")]
    target: Target,
    #[arg(
        long,
        env = "CRABKA_METRICS_SERVICE_LISTEN",
        default_value = "127.0.0.1:4041"
    )]
    listen: SocketAddr,
    #[arg(
        long,
        env = "CRABKA_METRICS_SERVICE_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_METRICS_SERVICE_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_METRICS_OBJECT_STORE_URL",
        default_value = "file://./.crabka-metrics-blocks"
    )]
    object_store_url: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_MANIFEST_PREFIX",
        default_value = "metrics"
    )]
    manifest_prefix: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_COLD_CACHE_TTL",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    cold_cache_ttl: Time,
    #[arg(
        long,
        env = "CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK",
        default_value = "1h",
        value_parser = parse::positive_time
    )]
    unbounded_compatibility_lookback: Time,
    #[arg(long, env = "CRABKA_METRICS_RUNTIME_OVERRIDES")]
    runtime_overrides: Option<PathBuf>,
    #[arg(
        long,
        env = "CRABKA_METRICS_QUERY_FRONTEND_SPLIT",
        default_value = "60s",
        value_parser = parse::positive_time
    )]
    query_frontend_split: Time,
    #[arg(
        long,
        env = "CRABKA_METRICS_QUERY_FRONTEND_SHARDS",
        default_value_t = 1
    )]
    query_frontend_shards: usize,
    #[arg(
        long,
        env = "CRABKA_METRICS_MAX_CONCURRENT_QUERIES",
        default_value_t = 2
    )]
    max_concurrent_queries: usize,
    #[arg(
        long = "query-lookback-delta",
        env = "CRABKA_METRICS_QUERY_LOOKBACK_DELTA",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    query_lookback_delta: Time,
    #[arg(
        long = "query-eval-interval",
        env = "CRABKA_METRICS_QUERY_EVAL_INTERVAL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    query_eval_interval: Time,
    #[arg(
        long = "query-max-samples",
        env = "CRABKA_METRICS_QUERY_MAX_SAMPLES",
        default_value_t = 50_000_000,
        value_parser = parse_positive_usize
    )]
    query_max_samples: usize,
    #[arg(
        long = "remote-read-max-body",
        env = "CRABKA_METRICS_REMOTE_READ_MAX_BODY",
        default_value = "64MiB",
        value_parser = parse_remote_read_max_body
    )]
    remote_read_max_body: ByteSize,
    #[arg(
        long,
        env = "CRABKA_METRICS_QUERY_FRONTEND_CACHE_PREFIX",
        default_value = "metrics-query-cache"
    )]
    query_frontend_cache_prefix: String,
    #[arg(long, env = "CRABKA_METRICS_RULER_TENANT", default_value = "anonymous")]
    ruler_tenant: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_RULER_EVAL_INTERVAL",
        default_value = "60s",
        value_parser = parse::positive_time
    )]
    ruler_eval_interval: Time,
    #[arg(long, env = "CRABKA_METRICS_RULER_SHARD_INDEX", default_value_t = 1)]
    ruler_shard_index: usize,
    #[arg(long, env = "CRABKA_METRICS_RULER_SHARD_TOTAL", default_value_t = 1)]
    ruler_shard_total: usize,
    #[arg(long, env = "CRABKA_METRICS_RULER_ALERTMANAGER_URL")]
    ruler_alertmanager_url: Option<String>,
    #[arg(
        long,
        env = "CRABKA_METRICS_RULER_STATE_TOPIC",
        default_value = RULER_STATE_TOPIC
    )]
    ruler_state_topic: String,
    #[arg(long, env = "CRABKA_METRICS_WAL_BOOTSTRAP")]
    wal_bootstrap: Option<String>,
    #[arg(
        long,
        env = "CRABKA_METRICS_WAL_GROUP_ID",
        default_value = "crabka-metrics-querier"
    )]
    wal_group_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_WAL_CLIENT_ID",
        default_value = "crabka-metrics-querier"
    )]
    wal_client_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_WAL_TOPIC",
        default_value = WAL_TOPIC
    )]
    wal_topic: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_WAL_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    wal_poll_timeout: Time,
    /// How far back the in-memory WAL head keeps samples.
    #[arg(
        long,
        env = "CRABKA_METRICS_QUERIER_WAL_HEAD_RETENTION",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    wal_head_retention: Time,
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

fn parse_remote_read_max_body(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = value.bytes_u64();
    if ByteSize::from_bytes(bytes) == value {
        Ok(value)
    } else {
        Err("remote-read maximum body must be a whole-byte value".to_owned())
    }
}

fn query_engine_opts(cli: &Cli) -> EngineOpts {
    EngineOpts {
        lookback_delta: cli.query_lookback_delta,
        eval_interval: cli.query_eval_interval,
        max_samples: cli.query_max_samples,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Target {
    Querier,
    QueryFrontend,
    Ruler,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "metrics-service",
            env!("CARGO_PKG_VERSION"),
            "crabka-metrics-service",
        )?,
        "crabka_metrics_service=info,info",
        "info",
        "crabka-metrics-service",
    )?;
    let result = async {
        let metrics = crabka_promql::metrics::ServiceMetrics::new();
        let admin = crabka_telemetry::profiling::spawn_admin_from_env_with_config(
            "0.0.0.0:9404",
            crabka_promql::metrics::metrics_router(metrics.registry.clone()),
            cli.profiling.clone(),
        )
        .await?;

        let role = async {
            match cli.target {
                Target::Querier => run_querier(cli, metrics).await?,
                Target::QueryFrontend => run_query_frontend(cli, metrics).await?,
                Target::Ruler => run_ruler(cli, metrics).await?,
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        tokio::select! {
            result = role => result?,
            result = crabka_telemetry::profiling::await_admin_exit(admin) => result?,
        }
        Ok(())
    }
    .await;
    telemetry.shutdown();
    result
}

#[tracing::instrument(
    level = "info",
    name = "metrics.run_query_frontend",
    skip_all,
    fields(listen = %cli.listen, object_store = %cli.object_store_url, manifest_prefix = %cli.manifest_prefix),
    err
)]
async fn run_query_frontend(
    cli: Cli,
    metrics: crabka_promql::metrics::ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
        Arc::clone(&store),
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let mut state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_metrics(metrics)
        .with_query_frontend_cache(
            QueryFrontendOptions {
                split_interval: cli.query_frontend_split,
                shard_count: cli.query_frontend_shards,
            },
            Arc::new(crabka_promql::ObjectStoreQueryFrontendCache::new(
                store,
                cli.query_frontend_cache_prefix.clone(),
            )),
        );
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_query_limits(overrides);
    }
    let router = prometheus_router(Arc::new(state));
    let shutdown = Shutdown::new();
    spawn_ctrl_c_listener(shutdown.clone());
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, shutdown.signalled()).await?;
    tracing::info!(%bound, "metrics-service query-frontend listening");
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}

#[tracing::instrument(
    level = "info",
    name = "metrics.run_ruler",
    skip_all,
    fields(listen = %cli.listen, tenant = %cli.ruler_tenant, shard_index = cli.ruler_shard_index, shard_total = cli.ruler_shard_total),
    err
)]
async fn run_ruler(
    cli: Cli,
    metrics: crabka_promql::metrics::ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let mut state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_metrics(metrics);
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_query_limits(overrides);
    }
    let state = Arc::new(state);
    let router = prometheus_router(Arc::clone(&state));
    let shard = RulerShard::new(cli.ruler_shard_index, cli.ruler_shard_total)?;

    let bootstrap = cli.wal_bootstrap.clone().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--wal-bootstrap is required for --target ruler",
        )
    })?;
    let mut state_consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(format!("{}-ruler-state", cli.wal_group_id))
        .client_id(format!("{}-ruler-state", cli.wal_client_id))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ruler_state_topic.clone()])
        .build()
        .await?;
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let wal_sink = KafkaRecordingRuleWalSink::new(Arc::clone(&producer), cli.wal_topic.clone());
    let state_sink = RulerStateFanoutSink::new(
        PrometheusRulerStateSink::new(Arc::clone(&state)),
        KafkaRulerStateSink::new(producer, cli.ruler_state_topic.clone()),
    );
    let tenant = cli.ruler_tenant.clone();
    let interval = cli.ruler_eval_interval;
    let alertmanager_url = cli.ruler_alertmanager_url.clone();
    let state_for_replay = Arc::clone(&state);
    let state_topic = cli.ruler_state_topic.clone();
    let poll_timeout = cli.wal_poll_timeout;

    let shutdown = Shutdown::new();
    spawn_ctrl_c_listener(shutdown.clone());

    // The ruler state consumer and evaluation loop are critical: both feed
    // ruler correctness. Their stop predicate observes the shared shutdown, and
    // if either returns (the loops only return on error, never voluntarily) we
    // surface it with `error!` and trigger shutdown so the process winds down
    // loudly rather than silently running headless.
    let consumer_shutdown = shutdown.clone();
    let consumer_stop = consumer_shutdown.rx.clone();
    tokio::spawn(async move {
        let result = run_ruler_state_consumer_loop(
            &mut state_consumer,
            &state_for_replay,
            &state_topic,
            poll_timeout,
            move |_| *consumer_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler state consumer stopped; shutting down");
        }
        consumer_shutdown.trigger();
    });
    let eval_shutdown = shutdown.clone();
    let eval_stop = eval_shutdown.rx.clone();
    tokio::spawn(async move {
        let result = run_ruler_evaluation_loop(
            state,
            (
                wal_sink,
                RulerAlertmanagerSink::from_endpoint(alertmanager_url),
                state_sink,
            ),
            tenant,
            shard,
            interval,
            move || *eval_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler evaluation loop stopped; shutting down");
        }
        eval_shutdown.trigger();
    });

    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, shutdown.signalled()).await?;
    tracing::info!(%bound, "metrics-service ruler listening");
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}

#[tracing::instrument(
    level = "info",
    name = "metrics.run_querier",
    skip_all,
    fields(listen = %cli.listen, object_store = %cli.object_store_url, manifest_prefix = %cli.manifest_prefix, wal_topic = %cli.wal_topic),
    err
)]
async fn run_querier(
    cli: Cli,
    metrics: crabka_promql::metrics::ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let head = WalHead::with_retention(cli.wal_head_retention);
    let shutdown = Shutdown::new();
    spawn_ctrl_c_listener(shutdown.clone());
    if let Some(bootstrap) = cli.wal_bootstrap.clone() {
        let wal_head = head.clone();
        let wal_topic = cli.wal_topic.clone();
        let poll_timeout = cli.wal_poll_timeout;
        let group_id = cli.wal_group_id.clone();
        let client_id = cli.wal_client_id.clone();
        let subscribe_topic = cli.wal_topic.clone();
        spawn_wal_head_consumer_task(
            move || async move {
                Consumer::builder()
                    .bootstrap(bootstrap)
                    .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
                    .frame_max(cli.client_frame_max)
                    .group_id(group_id)
                    .client_id(client_id)
                    .auto_offset_reset(AutoOffsetReset::Earliest)
                    .subscribe([subscribe_topic])
                    .build()
                    .await
                    .map_err(|error| error.to_string())
            },
            wal_head,
            wal_topic,
            poll_timeout,
            shutdown.clone(),
        );
    }
    let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        head,
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let mut state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_metrics(metrics);
    if let Some(overrides) = load_runtime_overrides(cli.runtime_overrides.as_deref())? {
        state = state.with_query_limits(overrides);
    }
    let router = prometheus_router(Arc::new(state));
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, shutdown.signalled()).await?;
    tracing::info!(%bound, "metrics-service querier listening");
    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    server.await?;
    Ok(())
}

fn spawn_wal_head_consumer_task<C, Build, BuildFuture>(
    build_consumer: Build,
    wal_head: WalHead,
    wal_topic: String,
    poll_timeout: Time,
    shutdown: Shutdown,
) -> tokio::task::JoinHandle<()>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + Send + 'static,
    Build: FnOnce() -> BuildFuture + Send + 'static,
    BuildFuture: Future<Output = Result<C, String>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut consumer = match build_consumer().await {
            Ok(consumer) => consumer,
            Err(error) => {
                tracing::error!(%error, "metrics WAL head consumer failed to start; shutting down");
                shutdown.trigger();
                return;
            }
        };
        let consumer_stop = shutdown.rx.clone();
        let result = run_wal_head_consumer_loop(
            &mut consumer,
            &wal_head,
            &wal_topic,
            poll_timeout,
            move |_| *consumer_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics WAL head consumer stopped; shutting down");
        }
        shutdown.trigger();
    })
}

/// A single process-wide shutdown signal shared by the HTTP server and every
/// background task.
///
/// A `true` value in the watch asks the axum server to start its graceful
/// drain, and tells the consumer and eval loops to stop. A critical background
/// task that exits also sets the watch, whether it exits cleanly or with an
/// error. The whole process then stops, instead of a continued run with a dead
/// loop.
#[derive(Clone)]
struct Shutdown {
    tx: tokio::sync::watch::Sender<bool>,
    rx: tokio::sync::watch::Receiver<bool>,
}

impl Shutdown {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self { tx, rx }
    }

    /// Request shutdown.
    ///
    /// This method is idempotent. Repeated triggers do nothing.
    fn trigger(&self) {
        let _ = self.tx.send(true);
    }

    /// Return a future that resolves after a caller requests shutdown.
    ///
    /// Each consumer gets its own clone: the server's graceful-shutdown hook
    /// and each background task.
    fn signalled(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut rx = self.rx.clone();
        async move {
            // `borrow()` covers the already-triggered case; otherwise wait for the
            // next change. `changed()` only errors once every sender is dropped, by
            // which point we also want to stop, so treat that as "shut down".
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Spawn a task that sets the shared shutdown on the first SIGINT, that is
/// Ctrl-C.
///
/// One signal stops the server and all background tasks together.
fn spawn_ctrl_c_listener(shutdown: Shutdown) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::error!("failed to listen for ctrl-c; triggering shutdown");
        }
        shutdown.trigger();
    });
}

fn load_runtime_overrides(
    path: Option<&Path>,
) -> Result<Option<OverridesProvider>, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let yaml = std::fs::read_to_string(path)?;
    Ok(Some(OverridesProvider::from_yaml(&yaml)?))
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use clap::Parser;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "query-frontend",
            "--query-frontend-split",
            "30s",
            "--query-frontend-shards",
            "4",
            "--query-frontend-cache-prefix",
            "tenant-a-query-cache",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::QueryFrontend));
        assert2::assert!(cli.query_frontend_split == secs(30));
        assert2::assert!(cli.query_frontend_shards == 4);
        assert2::assert!(cli.query_frontend_cache_prefix.as_str() == "tenant-a-query-cache");
    }

    #[test]
    fn parses_ruler_target_and_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "ruler",
            "--ruler-tenant",
            "tenant-a",
            "--ruler-eval-interval",
            "15s",
            "--ruler-shard-index",
            "2",
            "--ruler-shard-total",
            "4",
            "--ruler-alertmanager-url",
            "http://alertmanager.example/api/v2/alerts",
            "--ruler-state-topic",
            "__tenant_a_ruler_state",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Ruler));
        assert2::assert!(cli.ruler_tenant.as_str() == "tenant-a");
        assert2::assert!(cli.ruler_eval_interval == secs(15));
        assert2::assert!(cli.ruler_shard_index == 2);
        assert2::assert!(cli.ruler_shard_total == 4);
        assert2::assert!(
            cli.ruler_alertmanager_url.as_deref()
                == Some("http://alertmanager.example/api/v2/alerts")
        );
        assert2::assert!(cli.ruler_state_topic.as_str() == "__tenant_a_ruler_state");
    }

    #[test]
    fn parses_listen_address() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--listen",
            "127.0.0.1:0",
        ])
        .unwrap();

        assert2::assert!(cli.listen.port() == 0);
    }

    #[test]
    fn parses_blockstore_querier_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--object-store-url",
            "file:///tmp/crabka-metrics",
            "--manifest-prefix",
            "metrics/tenant-a",
        ])
        .unwrap();

        assert2::assert!(&cli.object_store_url == &"file:///tmp/crabka-metrics".to_string());
        assert2::assert!(&cli.manifest_prefix == &"metrics/tenant-a".to_string());
    }

    #[test]
    fn cold_store_policy_parses_defaults_overrides_and_boundaries() {
        let defaults =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.cold_cache_ttl == crabka_metrics_service::DEFAULT_COLD_CACHE_TTL);
        assert2::assert!(
            defaults.unbounded_compatibility_lookback
                == crabka_metrics_service::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK
        );

        let configured = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--cold-cache-ttl",
            "5s",
            "--unbounded-compatibility-lookback",
            "10m",
        ])
        .unwrap();
        assert2::assert!(configured.cold_cache_ttl == secs(5));
        assert2::assert!(configured.unbounded_compatibility_lookback == minutes(10));

        for args in [
            ["--cold-cache-ttl", "0s"],
            ["--cold-cache-ttl", "-1s"],
            ["--unbounded-compatibility-lookback", "0s"],
            ["--unbounded-compatibility-lookback", "-1s"],
        ] {
            assert2::assert!(
                Cli::try_parse_from([
                    "crabka-metrics-service",
                    "--target",
                    "querier",
                    args[0],
                    args[1],
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn cold_store_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_METRICS_SERVICE_COLD_STORE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::cold_store_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_METRICS_COLD_CACHE_TTL", "5s")
                    .env("CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK", "10m")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.cold_cache_ttl == secs(5));
        assert2::assert!(from_env.unbounded_compatibility_lookback == minutes(10));

        let from_cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--cold-cache-ttl",
            "7s",
            "--unbounded-compatibility-lookback",
            "20m",
        ])
        .unwrap();
        assert2::assert!(from_cli.cold_cache_ttl == secs(7));
        assert2::assert!(from_cli.unbounded_compatibility_lookback == minutes(20));
    }

    #[test]
    fn query_policy_parses_defaults_overrides_and_boundaries() {
        let defaults =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.query_lookback_delta == minutes(5));
        assert2::assert!(defaults.query_eval_interval == minutes(1));
        assert2::assert!(defaults.query_max_samples == 50_000_000);
        assert2::assert!(defaults.remote_read_max_body == mebibytes(64));

        let configured = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--query-lookback-delta=7m",
            "--query-eval-interval=11s",
            "--query-max-samples=13",
            "--remote-read-max-body=17MiB",
        ])
        .unwrap();
        assert2::assert!(query_engine_opts(&configured).lookback_delta == minutes(7));
        assert2::assert!(query_engine_opts(&configured).eval_interval == secs(11));
        assert2::assert!(query_engine_opts(&configured).max_samples == 13);
        assert2::assert!(configured.remote_read_max_body == mebibytes(17));

        for flag in [
            "--query-lookback-delta=0s",
            "--query-eval-interval=0s",
            "--query-max-samples=0",
            "--remote-read-max-body=0B",
            "--remote-read-max-body=1.5B",
        ] {
            assert2::assert!(
                Cli::try_parse_from(["crabka-metrics-service", "--target", "querier", flag,])
                    .is_err(),
                "accepted {flag}"
            );
        }
    }

    #[test]
    fn query_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_METRICS_QUERY_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::query_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_METRICS_QUERY_LOOKBACK_DELTA", "7m")
                    .env("CRABKA_METRICS_QUERY_EVAL_INTERVAL", "11s")
                    .env("CRABKA_METRICS_QUERY_MAX_SAMPLES", "13")
                    .env("CRABKA_METRICS_REMOTE_READ_MAX_BODY", "17MiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(query_engine_opts(&from_env).lookback_delta == minutes(7));
        assert2::assert!(query_engine_opts(&from_env).eval_interval == secs(11));
        assert2::assert!(query_engine_opts(&from_env).max_samples == 13);
        assert2::assert!(from_env.remote_read_max_body == mebibytes(17));

        let from_cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--query-lookback-delta=19m",
            "--query-eval-interval=23s",
            "--query-max-samples=29",
            "--remote-read-max-body=31MiB",
        ])
        .unwrap();
        assert2::assert!(query_engine_opts(&from_cli).lookback_delta == minutes(19));
        assert2::assert!(query_engine_opts(&from_cli).eval_interval == secs(23));
        assert2::assert!(query_engine_opts(&from_cli).max_samples == 29);
        assert2::assert!(from_cli.remote_read_max_body == mebibytes(31));
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-metrics-service",
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
                "crabka-metrics-service",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "crabka-metrics-service",
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
        const CHILD: &str = "CRABKA_METRICS_SERVICE_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_METRICS_SERVICE_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_METRICS_SERVICE_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "crabka-metrics-service",
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
    fn parses_runtime_overrides_path() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "query-frontend",
            "--runtime-overrides",
            "/etc/crabka/runtime.yaml",
        ])
        .unwrap();

        assert2::assert!(
            cli.runtime_overrides == Some(std::path::PathBuf::from("/etc/crabka/runtime.yaml"))
        );
    }

    #[test]
    fn parses_querier_wal_head_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "querier",
            "--wal-bootstrap",
            "127.0.0.1:9092",
            "--wal-group-id",
            "metrics-querier",
            "--wal-client-id",
            "querier-a",
            "--wal-topic",
            "__crabka_metrics_wal",
            "--wal-head-retention",
            "10m",
        ])
        .unwrap();

        assert2::assert!(cli.wal_bootstrap.as_deref() == Some("127.0.0.1:9092"));
        assert2::assert!(cli.wal_group_id.as_str() == "metrics-querier");
        assert2::assert!(cli.wal_client_id.as_str() == "querier-a");
        assert2::assert!(cli.wal_topic.as_str() == "__crabka_metrics_wal");
        assert2::assert!(cli.wal_head_retention == minutes(10));
    }

    #[test]
    fn runtime_options_read_unit_bearing_environment_values() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("CRABKA_METRICS_SERVICE_TARGET", Some("querier")),
                ("CRABKA_METRICS_WAL_POLL_TIMEOUT", Some("250ms")),
                ("CRABKA_METRICS_QUERIER_WAL_HEAD_RETENTION", Some("10m")),
            ],
            || {
                let cli =
                    Cli::try_parse_from(["crabka-metrics-service"]).expect("parse environment");
                assert2::assert!(matches!(cli.target, Target::Querier));
                assert2::assert!(
                    (cli.wal_poll_timeout, cli.wal_head_retention) == (millis(250), minutes(10))
                );
            },
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert2::assert!(
            Cli::try_parse_from(["crabka-metrics-service", "--target", "bogus"]).is_err()
        );
    }

    #[tokio::test]
    async fn shutdown_signalled_resolves_after_trigger() {
        let shutdown = Shutdown::new();
        let signalled = shutdown.signalled();
        // Trigger from another task; the server's graceful-shutdown hook (this
        // `signalled` future) must then resolve so the join can drain.
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            trigger.trigger();
        });
        signalled.await;
    }

    #[tokio::test]
    async fn shutdown_signalled_resolves_immediately_when_already_triggered() {
        // A background task that triggers shutdown before the server begins its
        // drain (e.g. a critical consumer dies during startup) must not leave the
        // graceful-shutdown future hanging: `watch` retains the latest value, so a
        // receiver cloned after the trigger observes it on first poll.
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.signalled().await;
    }

    #[tokio::test]
    async fn shutdown_stop_predicate_observes_trigger() {
        // The background loops' stop predicate borrows a cloned receiver; flipping
        // the shared shutdown must make that borrow read `true`.
        let shutdown = Shutdown::new();
        let stop = shutdown.rx.clone();
        assert2::assert!(!*stop.borrow());
        shutdown.trigger();
        assert2::assert!(*stop.borrow());
    }

    #[tokio::test]
    async fn wal_head_consumer_startup_runs_in_background() {
        let shutdown = Shutdown::new();
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        let task = spawn_wal_head_consumer_task(
            || async move {
                let _ = rx.await;
                Ok(PendingWalHeadConsumer)
            },
            crabka_promql::WalHead::new(),
            "__crabka_metrics_wal".to_string(),
            millis(1),
            shutdown.clone(),
        );

        let signalled = tokio::time::timeout(millis(25).to_std(), shutdown.signalled()).await;
        task.abort();

        assert2::assert!(signalled.is_err());
    }

    struct PendingWalHeadConsumer;

    #[async_trait::async_trait]
    impl crabka_metrics_service::WalHeadConsumerPoll for PendingWalHeadConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<
            Vec<crabka_client_consumer::ConsumerRecord>,
            crabka_metrics_service::WalHeadConsumerError,
        > {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl crabka_metrics_service::WalHeadConsumerCommit for PendingWalHeadConsumer {
        async fn commit_sync(
            &mut self,
        ) -> Result<(), crabka_metrics_service::WalHeadConsumerError> {
            Ok(())
        }
    }
}
