use clap::{Parser, Subcommand};
use crabka_operator::{config::OperatorConfig, gen_crds, run};

#[derive(Debug, Parser)]
#[command(name = "crabka-operator", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the operator: watch CRDs and reconcile.
    Run(RunArgs),
    /// Emit CRD YAML manifests to a directory.
    GenCrds { out_dir: std::path::PathBuf },
    /// Scan all (or one namespace's) Kafka CRs and reissue any
    /// broker leaf certs within renewalDays of expiry. Designed to be
    /// driven by the `CronJob` shipped in the operator Helm chart.
    CaRenewalCheck(CaRenewalCheckArgs),
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    #[command(flatten)]
    config: OperatorConfig,
}

#[derive(Debug, clap::Args)]
struct CaRenewalCheckArgs {
    /// Run scoped to a single namespace. Default: cluster-scoped.
    #[arg(long, env = "WATCH_NAMESPACE")]
    namespace: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // rustls 0.23 refuses to auto-pick a CryptoProvider when multiple
    // are linkable (or none is enabled at the binary level). kube's
    // rustls-tls feature pulls rustls transitively without selecting
    // one, so install ring explicitly before any TLS use.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install default rustls CryptoProvider");

    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run::run(args.config).await,
        Command::GenCrds { out_dir } => gen_crds::write_all(&out_dir),
        Command::CaRenewalCheck(args) => {
            tracing_subscriber::fmt::init();
            let client = kube::Client::try_default().await?;
            crabka_operator::controller::cluster_ca::run_renewal_check(
                client,
                args.namespace.as_deref(),
            )
            .await
            .map_err(anyhow::Error::from)
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn ca_renewal_check_parse_cases() {
        for (name, argv, expected_namespace) in [
            (
                "explicit namespace",
                vec!["bin", "ca-renewal-check", "--namespace", "demo"],
                Some("demo"),
            ),
            ("no namespace", vec!["bin", "ca-renewal-check"], None),
        ] {
            let cli = Cli::parse_from(argv);
            match cli.command {
                Command::CaRenewalCheck(args) => {
                    assert_eq!(args.namespace.as_deref(), expected_namespace, "case {name}");
                }
                _ => panic!("case {name}: expected CaRenewalCheck variant"),
            }
        }
    }
}
