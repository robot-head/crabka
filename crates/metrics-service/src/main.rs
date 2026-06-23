// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

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
    RulerAlertmanagerSink, RulerStateFanoutSink, run_ruler_evaluation_loop,
    run_ruler_state_consumer_loop, run_wal_head_consumer_loop, serve_prometheus_router_joinable,
};
use crabka_promql::{
    EngineOpts, PrometheusApiState, QueryFrontendOptions, RulerShard, WalHead, prometheus_router,
};
use object_store::ObjectStore;

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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cli = Cli::parse();
    match cli.target {
        Target::Querier => run_querier(cli).await?,
        Target::QueryFrontend => run_query_frontend(cli).await?,
        Target::Ruler => run_ruler(cli).await?,
    }

    Ok(())
}

async fn run_query_frontend(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
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

async fn run_ruler(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
    );
    let mut state = PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default());
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

async fn run_querier(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let head = WalHead::new();
    let shutdown = Shutdown::new();
    spawn_ctrl_c_listener(shutdown.clone());
    if let Some(bootstrap) = cli.wal_bootstrap.clone() {
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(cli.wal_group_id.clone())
            .client_id(cli.wal_client_id.clone())
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe([cli.wal_topic.clone()])
            .build()
            .await?;
        let wal_head = head.clone();
        let wal_topic = cli.wal_topic.clone();
        let poll_timeout = Duration::from_millis(cli.wal_poll_ms);
        // The WAL head consumer is critical: without it the querier serves a
        // frozen head. Observe the shared shutdown, and if the loop returns
        // (only on error) surface it and trigger shutdown so the process winds
        // down loudly instead of serving stale data headless.
        let consumer_shutdown = shutdown.clone();
        let consumer_stop = consumer_shutdown.rx.clone();
        tokio::spawn(async move {
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
            consumer_shutdown.trigger();
        });
    }
    let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        head,
    );
    let mut state = PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default());
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
        ])
        .unwrap();

        assert!(cli.wal_bootstrap.as_deref() == Some("127.0.0.1:9092"));
        assert!(cli.wal_group_id == "metrics-querier");
        assert!(cli.wal_client_id == "querier-a");
        assert!(cli.wal_topic == "__crabka_metrics_wal");
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
}
