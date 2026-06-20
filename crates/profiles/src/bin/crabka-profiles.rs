use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use crabka_client_producer::Producer;
use crabka_profiles::distributor::{DistributorState, KafkaSink, serve};
use crabka_profiles::ingest::{RelabelConfig, TenantLimits};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:4040")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Target {
    Distributor,
    BlockBuilder,
    Querier,
    QueryFrontend,
    Compactor,
    Symbolizer,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cli = Cli::parse();
    match cli.target {
        Target::Distributor => {
            let producer = Producer::builder()
                .bootstrap(&cli.bootstrap)
                .build()
                .await?;
            let state = Arc::new(DistributorState {
                sink: Arc::new(KafkaSink::new(Arc::new(producer))),
                limits: TenantLimits::default(),
                relabel: Vec::<RelabelConfig>::new(),
                max_decompressed: 1 << 24,
            });
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve(cli.listen, state, shutdown).await?;
            tracing::info!(%bound, "profiles distributor listening");
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::BlockBuilder => {
            crabka_profiles::blockbuilder::run().await?;
        }
        Target::Querier | Target::QueryFrontend | Target::Compactor | Target::Symbolizer => {
            eprintln!("target {:?} is not implemented in this slice", cli.target);
            std::process::exit(2);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();

        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-profiles", "--target", "bogus"]).is_err());
    }
}
