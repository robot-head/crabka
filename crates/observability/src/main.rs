//! `crabka-observability` — role-selectable Loki-compatible logs service,
//! self-instrumented (OTLP traces + JSON logs + CPU/heap pprof).

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use crabka_observability::metrics::ServiceMetrics;
use crabka_observability::{ServiceConfig, build_service_dependencies, serve_service};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-logs",
            env!("CARGO_PKG_VERSION"),
            "crabka-logs",
        ),
        "crabka_observability=info,info",
        "info",
        "crabka-logs",
    )?;
    let metrics = ServiceMetrics::new();
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target) plus the
    // Prometheus RED-metrics exporter on the same :9404 admin port.
    crabka_telemetry::profiling::serve_admin_from_env_with(
        "0.0.0.0:9404",
        crabka_observability::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    let config = ServiceConfig::parse();
    let dependencies = build_service_dependencies(&config)
        .await?
        .with_metrics(metrics);
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}
