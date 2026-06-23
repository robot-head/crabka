//! `crabka-observability` — role-selectable Loki-compatible logs service,
//! self-instrumented (OTLP traces + JSON logs + CPU/heap pprof).

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(unix, feature = "heap-profiling"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use clap::Parser;
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
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target).
    crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;

    let config = ServiceConfig::parse();
    let dependencies = build_service_dependencies(&config).await?;
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}
