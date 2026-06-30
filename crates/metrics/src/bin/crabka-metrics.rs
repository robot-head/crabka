#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use clap::{Parser, ValueEnum};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::Producer;
use crabka_metrics::distributor::{
    DistributorState, HA_TRACKER_TOPIC, KafkaHaElectionSink, KafkaSink,
    run_ha_election_consumer_loop, serve,
};
use crabka_metrics::metrics::ServiceMetrics;
use crabka_metrics::{MetricsCompactorConfig, run_compactor_consumer_loop};
use crabka_telemetry::OtlpConfig;
use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:4041")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, default_value = "file://./.crabka-metrics-blocks")]
    object_store_url: String,
    #[arg(long, default_value = "crabka-metrics-compactor")]
    compactor_group_id: String,
    #[arg(long, default_value = "crabka-metrics-compactor")]
    compactor_client_id: String,
    #[arg(long, default_value_t = 1000)]
    compactor_poll_timeout_ms: u64,
    /// Flush the accumulated compaction buffer once this many WAL records are buffered.
    #[arg(long, default_value_t = crabka_metrics::DEFAULT_FLUSH_MAX_ROWS)]
    compactor_flush_max_rows: usize,
    /// Flush the accumulated compaction buffer once its oldest record reaches this age.
    #[arg(long, default_value_t = crabka_metrics::DEFAULT_FLUSH_MAX_AGE.as_millis() as u64)]
    compactor_flush_max_age_ms: u64,
    /// Delete compacted metric blocks older than this window. Zero disables retention.
    #[arg(long, default_value_t = 0)]
    compactor_retention_ms: u64,
    /// How often the compactor sweeps object-store blocks/indexes for retention.
    #[arg(long, default_value_t = 60_000)]
    compactor_retention_sweep_ms: u64,
    #[arg(long, default_value = HA_TRACKER_TOPIC)]
    ha_tracker_topic: String,
    #[arg(long, default_value = "crabka-metrics-ha-tracker")]
    ha_tracker_group_id: String,
    #[arg(long, default_value = "crabka-metrics-ha-tracker")]
    ha_tracker_client_id: String,
    #[arg(long, default_value_t = 500)]
    ha_tracker_poll_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Target {
    Distributor,
    Compactor,
    Querier,
    QueryFrontend,
    Ruler,
}

fn runnable_targets() -> &'static [Target] {
    &[
        Target::Distributor,
        Target::Compactor,
        Target::Querier,
        Target::QueryFrontend,
        Target::Ruler,
    ]
}

fn build_object_store(url: &str) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    let parsed = url::Url::parse(url)?;
    let (store, _prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(Arc::from(store))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-metrics",
            env!("CARGO_PKG_VERSION"),
            "crabka-metrics",
        ),
        "crabka_metrics=info,info",
        "info",
        "crabka-metrics",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin_from_env_with(
        "0.0.0.0:9404",
        crabka_metrics::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    let cli = Cli::parse();
    if !runnable_targets().contains(&cli.target) {
        eprintln!("metrics target {:?} is not implemented yet", cli.target);
        std::process::exit(2);
    }
    match cli.target {
        Target::Distributor => run_distributor(cli, metrics).await?,
        Target::Compactor => run_compactor(cli).await?,
        Target::Querier => run_querier(cli).await?,
        Target::QueryFrontend => run_query_frontend(cli).await?,
        Target::Ruler => run_ruler(cli).await?,
    }

    Ok(())
}

async fn run_querier(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let bound = serve_querier(cli.listen, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    tracing::info!(%bound, "metrics querier listening");
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

async fn run_query_frontend(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let bound = serve_query_frontend(cli.listen, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    tracing::info!(%bound, "metrics query-frontend listening");
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

async fn run_ruler(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let bound = serve_ruler(cli.listen, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    tracing::info!(%bound, "metrics ruler listening");
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

async fn serve_querier(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(addr, querier_router(), "metrics querier", shutdown).await
}

async fn serve_query_frontend(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(
        addr,
        query_frontend_router(),
        "metrics query-frontend",
        shutdown,
    )
    .await
}

async fn serve_ruler(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(addr, ruler_router(), "metrics ruler", shutdown).await
}

async fn serve_role_http(
    addr: SocketAddr,
    router: Router,
    role_name: &'static str,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, %role_name, "metrics role server stopped with error");
        }
    });
    Ok(bound)
}

fn querier_router() -> Router {
    Router::new()
        .route("/api/v1/status/buildinfo", get(querier_build_info))
        .route(
            "/prometheus/api/v1/status/buildinfo",
            get(querier_build_info),
        )
}

fn query_frontend_router() -> Router {
    role_status_router("query-frontend")
}

fn ruler_router() -> Router {
    role_status_router("ruler")
}

fn role_status_router(role: &'static str) -> Router {
    Router::new()
        .route(
            "/api/v1/status/buildinfo",
            get(move || async move { role_build_info(role) }),
        )
        .route(
            "/prometheus/api/v1/status/buildinfo",
            get(move || async move { role_build_info(role) }),
        )
}

async fn querier_build_info() -> impl IntoResponse {
    role_build_info("querier")
}

fn role_build_info(role: &'static str) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": {
                "role": role,
                "version": env!("CARGO_PKG_VERSION"),
                "revision": "unknown",
                "branch": "unknown",
                "buildUser": "crabka",
                "buildDate": "unknown",
                "goVersion": "n/a"
            }
        })),
    )
}

async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&cli.bootstrap)
            .build()
            .await?,
    );
    let mut ha_consumer = Consumer::builder()
        .bootstrap(&cli.bootstrap)
        .group_id(cli.ha_tracker_group_id.clone())
        .client_id(cli.ha_tracker_client_id.clone())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ha_tracker_topic.clone()])
        .build()
        .await?;
    let state = Arc::new(
        DistributorState::new(Arc::new(KafkaSink::new(Arc::clone(&producer))))
            .with_ha_election_sink(Arc::new(KafkaHaElectionSink::new(
                Arc::clone(&producer),
                cli.ha_tracker_topic.clone(),
            )))
            .with_metrics(metrics),
    );
    let ha_state = Arc::clone(&state);
    let ha_topic = cli.ha_tracker_topic.clone();
    let ha_poll_timeout = Duration::from_millis(cli.ha_tracker_poll_timeout_ms);
    tokio::spawn(async move {
        let result = run_ha_election_consumer_loop(
            &mut ha_consumer,
            ha_state.tracker(),
            &ha_topic,
            ha_poll_timeout,
            |_| false,
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "metrics HA tracker consumer stopped");
        }
    });
    let bound = serve(cli.listen, state, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    tracing::info!(%bound, "metrics distributor listening");
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

async fn run_compactor(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let store = build_object_store(&cli.object_store_url)?;
    let mut config = MetricsCompactorConfig::new(cli.bootstrap);
    config.group_id = cli.compactor_group_id;
    config.client_id = cli.compactor_client_id;
    config.poll_timeout = Duration::from_millis(cli.compactor_poll_timeout_ms);
    config.flush_max_rows = cli.compactor_flush_max_rows;
    config.flush_max_age = Duration::from_millis(cli.compactor_flush_max_age_ms);
    let runtime = config.build_runtime(store.clone())?;
    let mut consumer = config.build_consumer().await?;
    let stopping = Arc::new(AtomicBool::new(false));
    if cli.compactor_retention_ms > 0 {
        spawn_retention_sweeper(
            store,
            Duration::from_millis(cli.compactor_retention_ms),
            Duration::from_millis(cli.compactor_retention_sweep_ms.max(1)),
            Arc::clone(&stopping),
        );
    }
    let signal = Arc::clone(&stopping);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.store(true, Ordering::SeqCst);
    });
    let result = run_compactor_consumer_loop(
        &mut consumer,
        &runtime.block_writer,
        &runtime.index_sink,
        runtime.loop_config,
        |_| stopping.load(Ordering::SeqCst),
    )
    .await?;
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics compactor stopped"
    );
    Ok(())
}

fn spawn_retention_sweeper(
    store: Arc<dyn ObjectStore>,
    retention: Duration,
    sweep_interval: Duration,
    stopping: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        loop {
            match crabka_metrics::enforce_compaction_retention(
                store.clone(),
                unix_time_ms(),
                retention,
            )
            .await
            {
                Ok(stats) => {
                    if stats.manifests_deleted > 0 || stats.blocks_deleted > 0 {
                        tracing::info!(
                            manifests_scanned = stats.manifests_scanned,
                            manifests_deleted = stats.manifests_deleted,
                            blocks_deleted = stats.blocks_deleted,
                            "metrics compactor retention deleted old blocks"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "metrics compactor retention sweep failed");
                }
            }
            if stopping.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(sweep_interval).await;
            if stopping.load(Ordering::SeqCst) {
                break;
            }
        }
    });
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use axum::body::Body;
    use axum::http::Request;
    use clap::Parser;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_distributor_ha_tracker_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "distributor",
            "--ha-tracker-topic",
            "__tenant_a_ha",
            "--ha-tracker-group-id",
            "metrics-ha",
            "--ha-tracker-client-id",
            "metrics-ha-1",
            "--ha-tracker-poll-timeout-ms",
            "250",
        ])
        .unwrap();

        assert!(cli.ha_tracker_topic == "__tenant_a_ha");
        assert!(cli.ha_tracker_group_id == "metrics-ha");
        assert!(cli.ha_tracker_client_id == "metrics-ha-1");
        assert!(cli.ha_tracker_poll_timeout_ms == 250);
    }

    #[test]
    fn parses_query_frontend_target() {
        let cli = Cli::try_parse_from(["crabka-metrics", "--target", "query-frontend"]).unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
    }

    #[test]
    fn querier_target_is_runnable() {
        assert!(runnable_targets().contains(&Target::Querier));
    }

    #[test]
    fn query_frontend_target_is_runnable() {
        assert!(runnable_targets().contains(&Target::QueryFrontend));
    }

    #[test]
    fn ruler_target_is_runnable() {
        assert!(runnable_targets().contains(&Target::Ruler));
    }

    #[tokio::test]
    async fn querier_router_serves_prometheus_build_info() {
        let response = querier_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn querier_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_querier("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn query_frontend_router_serves_prometheus_build_info() {
        let response = query_frontend_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn query_frontend_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_query_frontend("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn ruler_router_serves_prometheus_build_info() {
        let response = ruler_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn ruler_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_ruler("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[test]
    fn parses_compactor_runtime_options() {
        let cli = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "compactor",
            "--bootstrap",
            "broker:9092",
            "--compactor-group-id",
            "metrics-c",
            "--compactor-poll-timeout-ms",
            "250",
            "--compactor-retention-ms",
            "3600000",
            "--compactor-retention-sweep-ms",
            "30000",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        assert!(cli.bootstrap == "broker:9092");
        assert!(cli.compactor_group_id == "metrics-c");
        assert!(cli.compactor_poll_timeout_ms == 250);
        assert!(cli.compactor_retention_ms == 3_600_000);
        assert!(cli.compactor_retention_sweep_ms == 30_000);
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-metrics", "--target", "bogus"]).is_err());
    }
}
