//! `crabka-observability` is a role-selectable Loki-compatible logs service.
//! It self-instruments with OTLP traces, JSON logs, and CPU and heap pprof.

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use crabka_observability::{
    ClientResourcePolicy, ServiceConfig, build_service_dependencies_with_client_resource_policy,
    metrics::ServiceMetrics, serve_service,
};
use crabka_units::{ByteSize, parse};

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    #[command(flatten)]
    service: ServiceConfig,
    #[arg(
        long,
        env = "CRABKA_OBSERVABILITY_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_OBSERVABILITY_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_frame_max
    )]
    client_frame_max: ByteSize,
}

fn parse_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    crabka_client_core::ConnectionDispatchQueueCapacity::new(value)
        .map(crabka_client_core::ConnectionDispatchQueueCapacity::get)
}

fn parse_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    crabka_client_core::ClientFrameMax::try_from(value)
        .map(crabka_client_core::ClientFrameMax::size)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client_resource_policy = ClientResourcePolicy {
        dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated client dispatch queue capacity"),
        frame_max: crabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated client frame maximum"),
    };
    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-logs",
            env!("CARGO_PKG_VERSION"),
            "crabka-logs",
        )?,
        "crabka_observability=info,info",
        "info",
        "crabka-logs",
    )?;
    let metrics = ServiceMetrics::new();
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target) plus the
    // Prometheus RED-metrics exporter on the same :9404 admin port.
    crabka_telemetry::profiling::serve_admin_from_env_with_config(
        "0.0.0.0:9404",
        crabka_observability::metrics::metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    let config = cli.service;
    let dependencies =
        build_service_dependencies_with_client_resource_policy(&config, client_resource_policy)
            .await?
            .with_metrics(metrics);
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_resource_policy_parses_defaults_overrides_and_invalid_values() {
        let defaults =
            Cli::try_parse_from(["crabka-observability", "--target", "querier"]).expect("defaults");
        assert_eq!(defaults.client_dispatch_queue_capacity, 64);
        assert_eq!(defaults.client_frame_max, crabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-observability",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .expect("custom policy");
        assert_eq!(custom.client_dispatch_queue_capacity, 7);
        assert_eq!(custom.client_frame_max, crabka_units::kibibytes(32));

        for option in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
            "--client-frame-max=1.5B",
        ] {
            Cli::try_parse_from(["crabka-observability", "--target", "querier", option])
                .expect_err(option);
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TEST_OBSERVABILITY_CLIENT_POLICY_ENV_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let environment = Cli::try_parse_from(["crabka-observability", "--target", "querier"])
                .expect("environment policy");
            assert_eq!(environment.client_dispatch_queue_capacity, 7);
            assert_eq!(environment.client_frame_max, crabka_units::kibibytes(32));

            let cli = Cli::try_parse_from([
                "crabka-observability",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "9",
                "--client-frame-max",
                "64KiB",
            ])
            .expect("CLI policy");
            assert_eq!(cli.client_dispatch_queue_capacity, 9);
            assert_eq!(cli.client_frame_max, crabka_units::kibibytes(64));
            return;
        }

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("CRABKA_OBSERVABILITY_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                .env("CRABKA_OBSERVABILITY_CLIENT_FRAME_MAX", "32KiB")
                .status()
                .expect("run isolated environment parser test");
        assert!(status.success());
    }

    #[test]
    fn profiling_policy_flattens_cli_and_environment() {
        const CHILD: &str = "CRABKA_TEST_OBSERVABILITY_PROFILING_ENV_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let environment = Cli::try_parse_from(["crabka-observability", "--target", "querier"])
                .expect("environment profiling policy");
            assert_eq!(
                environment.profiling.profiling_cpu_default_duration,
                crabka_units::secs(2)
            );
            assert_eq!(
                environment
                    .profiling
                    .profiling_cpu_sample_frequency
                    .frequency(),
                crabka_units::per_sec(101)
            );

            let cli = Cli::try_parse_from([
                "crabka-observability",
                "--target",
                "querier",
                "--profiling-cpu-default-duration=3s",
                "--profiling-cpu-sample-frequency=103Hz",
            ])
            .expect("CLI profiling policy");
            assert_eq!(
                cli.profiling.profiling_cpu_default_duration,
                crabka_units::secs(3)
            );
            assert_eq!(
                cli.profiling.profiling_cpu_sample_frequency.frequency(),
                crabka_units::per_sec(103)
            );
            return;
        }

        let defaults = Cli::try_parse_from(["crabka-observability", "--target", "querier"])
            .expect("default profiling policy");
        assert_eq!(
            defaults.profiling,
            crabka_telemetry::profiling::ProfilingConfig::default()
        );

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "tests::profiling_policy_flattens_cli_and_environment",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("CRABKA_PROFILING_CPU_DEFAULT_DURATION", "2s")
                .env("CRABKA_PROFILING_CPU_SAMPLE_FREQUENCY", "101Hz")
                .status()
                .expect("run isolated profiling environment parser test");
        assert!(status.success());
    }
}
