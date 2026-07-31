#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use clap::{Parser, ValueEnum};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_client_producer::Producer;
use crabka_metrics::{
    DEFAULT_MAX_RATE_BUCKETS, MetricsCompactorConfig,
    distributor::{
        DistributorState, HA_TRACKER_TOPIC, KafkaHaElectionSink, KafkaSink,
        run_ha_election_consumer_loop, serve,
    },
    metrics::ServiceMetrics,
    run_compactor_consumer_loop,
};
use crabka_telemetry::OtlpConfig;
use crabka_units::{parse, prelude::*};
use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, env = "CRABKA_METRICS_TARGET")]
    target: Target,
    #[arg(long, env = "CRABKA_METRICS_LISTEN", default_value = "127.0.0.1:4041")]
    listen: SocketAddr,
    #[arg(long, env = "CRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    admin_listen_addr: SocketAddr,
    #[arg(
        long,
        env = "CRABKA_METRICS_BOOTSTRAP",
        default_value = "127.0.0.1:9092"
    )]
    bootstrap: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_METRICS_CLIENT_FRAME_MAX",
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
        env = "CRABKA_METRICS_COMPACTOR_GROUP_ID",
        default_value = "crabka-metrics-compactor"
    )]
    compactor_group_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_CLIENT_ID",
        default_value = "crabka-metrics-compactor"
    )]
    compactor_client_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_POLL_TIMEOUT",
        default_value = "1s",
        value_parser = parse::positive_time
    )]
    compactor_poll_timeout: Time,
    /// Flush the accumulated compaction buffer once this many WAL records are buffered.
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_FLUSH_MAX_ROWS",
        default_value_t = crabka_metrics::DEFAULT_FLUSH_MAX_ROWS
    )]
    compactor_flush_max_rows: usize,
    /// Flush the accumulated compaction buffer once its oldest record reaches this age.
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_FLUSH_MAX_AGE",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    compactor_flush_max_age: Time,
    /// Delete compacted metric blocks older than this window. Zero disables retention.
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_RETENTION",
        default_value = "0s",
        value_parser = parse::non_negative_time
    )]
    compactor_retention: Time,
    /// How often the compactor sweeps object-store blocks/indexes for retention.
    #[arg(
        long,
        env = "CRABKA_METRICS_COMPACTOR_RETENTION_SWEEP_INTERVAL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    compactor_retention_sweep_interval: Time,
    #[arg(
        long,
        env = "CRABKA_METRICS_HA_TRACKER_TOPIC",
        default_value = HA_TRACKER_TOPIC
    )]
    ha_tracker_topic: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_HA_TRACKER_GROUP_ID",
        default_value = "crabka-metrics-ha-tracker"
    )]
    ha_tracker_group_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_HA_TRACKER_CLIENT_ID",
        default_value = "crabka-metrics-ha-tracker"
    )]
    ha_tracker_client_id: String,
    #[arg(
        long,
        env = "CRABKA_METRICS_HA_TRACKER_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    ha_tracker_poll_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_METRICS_HA_FAILOVER_TIMEOUT",
        default_value = "30s",
        value_parser = parse::time,
        allow_hyphen_values = true
    )]
    ha_failover_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_METRICS_INGEST_RATE_BUCKET_CAP",
        default_value_t = DEFAULT_MAX_RATE_BUCKETS,
        value_parser = parse_ingest_rate_bucket_cap
    )]
    ingest_rate_bucket_cap: usize,
    #[arg(
        long,
        env = "CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED",
        default_value = "32MiB",
        value_parser = parse_distributor_max_decompressed
    )]
    distributor_max_decompressed: ByteSize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IngestRateBucketCap(usize);

impl IngestRateBucketCap {
    fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("ingest rate bucket cap: {error}"))
    }

    #[must_use]
    const fn get(self) -> usize {
        self.0
    }
}

fn parse_ingest_rate_bucket_cap(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(IngestRateBucketCap::new)
        .map(IngestRateBucketCap::get)
}

fn parse_distributor_max_decompressed(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    usize::try_from(size.bytes_u64())
        .map_err(|_| "size must fit the platform request boundary".to_owned())?;
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
    let cli = Cli::parse();
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-metrics",
            env!("CARGO_PKG_VERSION"),
            "crabka-metrics",
        )?,
        "crabka_metrics=info,info",
        "info",
        "crabka-metrics",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin(
        cli.admin_listen_addr,
        crabka_metrics::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    if !runnable_targets().contains(&cli.target) {
        eprintln!("metrics target {:?} is not implemented yet", cli.target);
        std::process::exit(2);
    }
    match cli.target {
        Target::Distributor => run_distributor(cli, metrics).await?,
        Target::Compactor => run_compactor(cli, metrics).await?,
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
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .build()
            .await?,
    );
    let mut ha_consumer = Consumer::builder()
        .bootstrap(&cli.bootstrap)
        .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
        .frame_max(cli.client_frame_max)
        .group_id(cli.ha_tracker_group_id.clone())
        .client_id(cli.ha_tracker_client_id.clone())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([cli.ha_tracker_topic.clone()])
        .build()
        .await?;
    let state = Arc::new(
        DistributorState::new(Arc::new(KafkaSink::new(Arc::clone(&producer))))
            .with_ha_failover_timeout(cli.ha_failover_timeout)
            .with_max_rate_buckets(cli.ingest_rate_bucket_cap)
            .with_max_decompressed(cli.distributor_max_decompressed)
            .with_ha_election_sink(Arc::new(KafkaHaElectionSink::new(
                Arc::clone(&producer),
                cli.ha_tracker_topic.clone(),
            )))
            .with_metrics(metrics),
    );
    let ha_state = Arc::clone(&state);
    let ha_topic = cli.ha_tracker_topic.clone();
    let ha_poll_timeout = cli.ha_tracker_poll_timeout;
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

// cargo-mutants: live compactor I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = build_object_store(&cli.object_store_url)?;
    let retention = cli.compactor_retention;
    let sweep_interval = cli.compactor_retention_sweep_interval;
    let mut config = MetricsCompactorConfig::new(cli.bootstrap);
    config.client_dispatch_queue_capacity =
        ConnectionDispatchQueueCapacity::new(cli.client_dispatch_queue_capacity)
            .expect("validated metrics client dispatch queue capacity");
    config.client_frame_max =
        ClientFrameMax::try_from(cli.client_frame_max).expect("validated metrics frame maximum");
    config.group_id = cli.compactor_group_id;
    config.client_id = cli.compactor_client_id;
    config.poll_timeout = cli.compactor_poll_timeout;
    config.flush_max_rows = cli.compactor_flush_max_rows;
    config.flush_max_age = cli.compactor_flush_max_age;
    let runtime = config.build_runtime(store.clone())?;
    let mut consumer = config.build_consumer().await?;
    let stopping = Arc::new(AtomicBool::new(false));
    if retention > Time::ZERO {
        spawn_retention_sweeper(store, retention, sweep_interval, Arc::clone(&stopping));
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
    // Record the cumulative metric blocks the compactor wrote to object storage.
    metrics.record_blocks_compacted(result.writes as u64);
    tracing::info!(
        polls = result.polls,
        polled_records = result.polled_records,
        compacted_records = result.compacted_records,
        writes = result.writes,
        "metrics compactor stopped"
    );
    Ok(())
}

// cargo-mutants: background wall-clock loop is exercised through compactor integration.
#[cfg_attr(test, mutants::skip)]
fn spawn_retention_sweeper(
    store: Arc<dyn ObjectStore>,
    retention: Time,
    sweep_interval: Time,
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
            tokio::time::sleep(sweep_interval.to_std()).await;
            if stopping.load(Ordering::SeqCst) {
                break;
            }
        }
    });
}

// cargo-mutants: wall-clock read; no deterministic assertion.
#[cfg_attr(test, mutants::skip)]
fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis().min(i64::MAX as u128)).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use clap::Parser;
    use tower::ServiceExt;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == crabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "distributor",
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
                "crabka-metrics",
                "--target",
                "distributor",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "crabka-metrics",
                "--target",
                "distributor",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_METRICS_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_METRICS_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_METRICS_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == crabka_units::kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "distributor",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == crabka_units::kibibytes(64));
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn distributor_policy_parses_defaults_overrides_and_boundaries() {
        let defaults = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
        check!(
            defaults.ha_failover_timeout
                == crabka_metrics::distributor::DEFAULT_HA_FAILOVER_TIMEOUT
        );
        check!(defaults.ingest_rate_bucket_cap == DEFAULT_MAX_RATE_BUCKETS);
        check!(
            defaults.distributor_max_decompressed
                == crabka_metrics::distributor::DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED
        );

        let configured = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "distributor",
            "--ha-failover-timeout",
            "-1s",
            "--ingest-rate-bucket-cap",
            "7",
            "--distributor-max-decompressed",
            "64KiB",
        ])
        .unwrap();
        check!(configured.ha_failover_timeout == Time::from_millis(-1_000));
        check!(configured.ingest_rate_bucket_cap == 7);
        check!(configured.distributor_max_decompressed == kibibytes(64));

        for args in [
            ["--ingest-rate-bucket-cap", "0"],
            ["--distributor-max-decompressed", "0B"],
            ["--distributor-max-decompressed", "1.5B"],
        ] {
            let input = [
                "crabka-metrics",
                "--target",
                "distributor",
                args[0],
                args[1],
            ];
            assert!(Cli::try_parse_from(input).is_err());
        }
    }

    #[test]
    fn distributor_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_METRICS_DISTRIBUTOR_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::distributor_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_METRICS_HA_FAILOVER_TIMEOUT", "-1s")
                    .env("CRABKA_METRICS_INGEST_RATE_BUCKET_CAP", "7")
                    .env("CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED", "64KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
        check!(from_env.ha_failover_timeout == Time::from_millis(-1_000));
        check!(from_env.ingest_rate_bucket_cap == 7);
        check!(from_env.distributor_max_decompressed == kibibytes(64));

        let from_cli = Cli::try_parse_from([
            "crabka-metrics",
            "--target",
            "distributor",
            "--ha-failover-timeout",
            "5s",
            "--ingest-rate-bucket-cap",
            "9",
            "--distributor-max-decompressed",
            "128KiB",
        ])
        .unwrap();
        check!(from_cli.ha_failover_timeout == secs(5));
        check!(from_cli.ingest_rate_bucket_cap == 9);
        check!(from_cli.distributor_max_decompressed == kibibytes(128));
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
            "--ha-tracker-poll-timeout",
            "250ms",
        ])
        .unwrap();

        check!(cli.ha_tracker_topic == "__tenant_a_ha");
        check!(cli.ha_tracker_group_id == "metrics-ha");
        check!(cli.ha_tracker_client_id == "metrics-ha-1");
        check!(cli.ha_tracker_poll_timeout == millis(250));
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
            "--compactor-poll-timeout",
            "250ms",
            "--compactor-retention",
            "1h",
            "--compactor-retention-sweep-interval",
            "30s",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        check!(cli.bootstrap == "broker:9092");
        check!(cli.compactor_group_id == "metrics-c");
        check!(cli.compactor_poll_timeout == millis(250));
        check!(cli.compactor_retention == hours(1));
        check!(cli.compactor_retention_sweep_interval == secs(30));
    }

    #[test]
    fn runtime_options_read_unit_bearing_environment_values() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("CRABKA_METRICS_TARGET", Some("compactor")),
                ("CRABKA_METRICS_COMPACTOR_POLL_TIMEOUT", Some("250ms")),
                ("CRABKA_METRICS_COMPACTOR_FLUSH_MAX_AGE", Some("2m")),
                ("CRABKA_METRICS_COMPACTOR_RETENTION", Some("1h")),
                (
                    "CRABKA_METRICS_COMPACTOR_RETENTION_SWEEP_INTERVAL",
                    Some("30s"),
                ),
            ],
            || {
                let cli = Cli::try_parse_from(["crabka-metrics"]).expect("parse environment");
                assert!(matches!(cli.target, Target::Compactor));
                assert!(
                    (
                        cli.compactor_poll_timeout,
                        cli.compactor_flush_max_age,
                        cli.compactor_retention,
                        cli.compactor_retention_sweep_interval,
                    ) == (millis(250), minutes(2), hours(1), secs(30))
                );
            },
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-metrics", "--target", "bogus"]).is_err());
    }
}
