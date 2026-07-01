// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
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
use object_store::ObjectStore;

const DEFAULT_WAL_HEAD_RETENTION_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:4041")]
    listen: SocketAddr,
    #[arg(long, default_value = "file://./.crabka-metrics-blocks")]
    object_store_url: String,
    #[arg(long, default_value = "metrics")]
    manifest_prefix: String,
    #[arg(long)]
    runtime_overrides: Option<PathBuf>,
    #[arg(long, default_value_t = 60_000)]
    query_frontend_split_ms: i64,
    #[arg(long, default_value_t = 1)]
    query_frontend_shards: usize,
    #[arg(long, default_value_t = 2)]
    max_concurrent_queries: usize,
    #[arg(long, default_value = "metrics-query-cache")]
    query_frontend_cache_prefix: String,
    #[arg(long, default_value = "anonymous")]
    ruler_tenant: String,
    #[arg(long, default_value_t = 60_000)]
    ruler_eval_interval_ms: u64,
    #[arg(long, default_value_t = 1)]
    ruler_shard_index: usize,
    #[arg(long, default_value_t = 1)]
    ruler_shard_total: usize,
    #[arg(long)]
    ruler_alertmanager_url: Option<String>,
    #[arg(long, default_value = RULER_STATE_TOPIC)]
    ruler_state_topic: String,
    #[arg(long)]
    wal_bootstrap: Option<String>,
    #[arg(long, default_value = "crabka-metrics-querier")]
    wal_group_id: String,
    #[arg(long, default_value = "crabka-metrics-querier")]
    wal_client_id: String,
    #[arg(long, default_value = WAL_TOPIC)]
    wal_topic: String,
    #[arg(long, default_value_t = 500)]
    wal_poll_ms: u64,
    #[arg(long, default_value_t = DEFAULT_WAL_HEAD_RETENTION_MS)]
    wal_head_retention_ms: i64,
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
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "metrics-service",
            env!("CARGO_PKG_VERSION"),
            "crabka-metrics-service",
        ),
        "crabka_metrics_service=info,info",
        "info",
        "crabka-metrics-service",
    )?;
    let metrics = crabka_promql::metrics::ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin_from_env_with(
        "0.0.0.0:9404",
        crabka_promql::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    let cli = Cli::parse();
    match cli.target {
        Target::Querier => run_querier(cli, metrics).await?,
        Target::QueryFrontend => run_query_frontend(cli, metrics).await?,
        Target::Ruler => run_ruler(cli, metrics).await?,
    }

    Ok(())
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
    );
    let mut state = PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default())
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_metrics(metrics)
        .with_query_frontend_cache(
            QueryFrontendOptions {
                split_interval_ms: cli.query_frontend_split_ms,
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
    );
    let mut state = PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default())
        .with_max_concurrent_queries(cli.max_concurrent_queries)
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
        .group_id(format!("{}-ruler-state", cli.wal_group_id))
        .client_id(format!("{}-ruler-state", cli.wal_client_id))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ruler_state_topic.clone()])
        .build()
        .await?;
    let producer = Arc::new(Producer::builder().bootstrap(bootstrap).build().await?);
    let wal_sink = KafkaRecordingRuleWalSink::new(Arc::clone(&producer), cli.wal_topic.clone());
    let state_sink = RulerStateFanoutSink::new(
        PrometheusRulerStateSink::new(Arc::clone(&state)),
        KafkaRulerStateSink::new(producer, cli.ruler_state_topic.clone()),
    );
    let tenant = cli.ruler_tenant.clone();
    let interval = Duration::from_millis(cli.ruler_eval_interval_ms);
    let alertmanager_url = cli.ruler_alertmanager_url.clone();
    let state_for_replay = Arc::clone(&state);
    let state_topic = cli.ruler_state_topic.clone();
    let poll_timeout = Duration::from_millis(cli.wal_poll_ms);

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
            wal_sink,
            RulerAlertmanagerSink::from_endpoint(alertmanager_url),
            state_sink,
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
    let head = WalHead::with_retention_ms(cli.wal_head_retention_ms);
    let shutdown = Shutdown::new();
    spawn_ctrl_c_listener(shutdown.clone());
    if let Some(bootstrap) = cli.wal_bootstrap.clone() {
        let wal_head = head.clone();
        let wal_topic = cli.wal_topic.clone();
        let poll_timeout = Duration::from_millis(cli.wal_poll_ms);
        let group_id = cli.wal_group_id.clone();
        let client_id = cli.wal_client_id.clone();
        let subscribe_topic = cli.wal_topic.clone();
        spawn_wal_head_consumer_task(
            move || async move {
                Consumer::builder()
                    .bootstrap(bootstrap)
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
    );
    let mut state = PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default())
        .with_max_concurrent_queries(cli.max_concurrent_queries)
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
    poll_timeout: Duration,
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
/// background task. Flipping the watch to `true` asks the axum server to begin
/// its graceful drain and tells the consumer/eval loops to stop; a critical
/// background task that exits (cleanly or with an error) also flips it so the
/// whole process winds down loudly instead of limping on with a dead loop.
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

    /// Request shutdown. Idempotent — repeated triggers are no-ops.
    fn trigger(&self) {
        let _ = self.tx.send(true);
    }

    /// A future that resolves once shutdown has been requested. Cloned per
    /// consumer (the server's graceful-shutdown hook, each background task).
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

/// Spawn a task that flips the shared shutdown on the first SIGINT (Ctrl-C), so
/// a single signal tears down the server and all background tasks together.
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
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();

        assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "query-frontend",
            "--query-frontend-split-ms",
            "30000",
            "--query-frontend-shards",
            "4",
            "--query-frontend-cache-prefix",
            "tenant-a-query-cache",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
        assert!(cli.query_frontend_split_ms == 30_000);
        assert!(cli.query_frontend_shards == 4);
        assert!(cli.query_frontend_cache_prefix == "tenant-a-query-cache");
    }

    #[test]
    fn parses_ruler_target_and_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics-service",
            "--target",
            "ruler",
            "--ruler-tenant",
            "tenant-a",
            "--ruler-eval-interval-ms",
            "15000",
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

        assert!(matches!(cli.target, Target::Ruler));
        assert!(cli.ruler_tenant == "tenant-a");
        assert!(cli.ruler_eval_interval_ms == 15_000);
        assert!(cli.ruler_shard_index == 2);
        assert!(cli.ruler_shard_total == 4);
        assert!(
            cli.ruler_alertmanager_url.as_deref()
                == Some("http://alertmanager.example/api/v2/alerts")
        );
        assert!(cli.ruler_state_topic == "__tenant_a_ruler_state");
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

        assert!(cli.listen.port() == 0);
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

        assert!(cli.object_store_url == "file:///tmp/crabka-metrics");
        assert!(cli.manifest_prefix == "metrics/tenant-a");
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

        assert!(
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
            "--wal-head-retention-ms",
            "600000",
        ])
        .unwrap();

        assert!(cli.wal_bootstrap.as_deref() == Some("127.0.0.1:9092"));
        assert!(cli.wal_group_id == "metrics-querier");
        assert!(cli.wal_client_id == "querier-a");
        assert!(cli.wal_topic == "__crabka_metrics_wal");
        assert!(cli.wal_head_retention_ms == 600_000);
    }

    #[test]
    fn querier_wal_head_retention_default_is_bounded_for_demo_load() {
        let cli = Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();

        assert!(cli.wal_head_retention_ms == 300_000);
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-metrics-service", "--target", "bogus"]).is_err());
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
        assert!(!*stop.borrow());
        shutdown.trigger();
        assert!(*stop.borrow());
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
            std::time::Duration::from_millis(1),
            shutdown.clone(),
        );

        let signalled =
            tokio::time::timeout(std::time::Duration::from_millis(25), shutdown.signalled()).await;
        task.abort();

        assert!(
            signalled.is_err(),
            "pending WAL startup should not block caller or trigger shutdown"
        );
    }

    struct PendingWalHeadConsumer;

    #[async_trait::async_trait]
    impl crabka_metrics_service::WalHeadConsumerPoll for PendingWalHeadConsumer {
        async fn poll(
            &mut self,
            _timeout: std::time::Duration,
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
