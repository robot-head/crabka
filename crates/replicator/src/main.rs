// `#[tracing::instrument]` on the deeply-nested replication futures pushes the
// type-layout query past the default depth limit; raise it (mirrors lib.rs).
#![recursion_limit = "256"]

use anyhow::Context as _;
use clap::Parser;
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_replicator::{
    config::{ClientResourcePolicy, ReplicatorConfig},
    supervisor::FlowSupervisor,
};
use crabka_units::{ByteSize, parse};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Debug, Parser)]
#[command(name = "crabka-replicator", version, about)]
struct Cli {
    /// Path to the replicator YAML config.
    #[arg(long, env = "CRABKA_REPLICATOR_CONFIG")]
    config: std::path::PathBuf,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "crabka_replicator=info,info".into());
    tracing_subscriber::registry()
        .with(crabka_logfmt::layer(filter, std::io::stdout))
        .init();

    let cli = Cli::parse();
    let yaml = std::fs::read_to_string(&cli.config)
        .with_context(|| format!("reading config {}", cli.config.display()))?;
    let config = ReplicatorConfig::from_yaml(&yaml)?;
    let client_resource_policy = ClientResourcePolicy {
        dispatch_queue_capacity: ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated by clap"),
        frame_max: ClientFrameMax::try_from(cli.client_frame_max).expect("validated by clap"),
    };
    config.validate()?;

    let supervisor = FlowSupervisor::run_with_policy(config, client_resource_policy).await?;
    tracing::info!("crabka-replicator running; send SIGINT/ctrl-c to stop");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested; draining flows");
    supervisor.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use crabka_units::{kibibytes, mebibytes};

    use super::Cli;

    #[test]
    fn client_resource_policy_parses_defaults_overrides_and_rejects_invalid() {
        let defaults = Cli::try_parse_from(["crabka-replicator", "--config=config.yaml"]).unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Cli::try_parse_from([
            "crabka-replicator",
            "--config=config.yaml",
            "--client-dispatch-queue-capacity=7",
            "--client-frame-max=32KiB",
        ])
        .unwrap();
        assert2::assert!(custom.client_dispatch_queue_capacity == 7);
        assert2::assert!(custom.client_frame_max == kibibytes(32));

        for invalid in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
        ] {
            assert2::assert!(
                Cli::try_parse_from(["crabka-replicator", "--config=config.yaml", invalid])
                    .is_err()
            );
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_REPLICATOR_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_REPLICATOR_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_REPLICATOR_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["crabka-replicator", "--config=config.yaml"]).unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "crabka-replicator",
            "--config=config.yaml",
            "--client-dispatch-queue-capacity=9",
            "--client-frame-max=64KiB",
        ])
        .unwrap();
        assert2::assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert2::assert!(from_cli.client_frame_max == kibibytes(64));
    }
}
