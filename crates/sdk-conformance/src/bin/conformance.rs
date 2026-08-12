//! CLI entry point for the SDK conformance harness.

use std::path::PathBuf;

use clap::Parser;
use crabka_sdk_conformance::{
    Harness, HarnessConfig, HarnessSubstrate,
    mock_adapter::{FaultMode, run_stdio},
    protocol::CONTRACT_MINOR_V1_0,
};
use tokio::io::{BufReader, BufWriter};

#[derive(Debug, Parser)]
struct Cli {
    /// Adapter executable to run.
    #[arg(long)]
    adapter: Option<PathBuf>,
    /// Vector directory.
    #[arg(long, default_value = "crates/sdk-conformance/vectors/v1")]
    vectors: PathBuf,
    /// Run only one vector id.
    #[arg(long)]
    filter: Option<String>,
    /// Gateway endpoint sent to adapters.
    #[arg(long, default_value = "mock://gateway")]
    endpoint: String,
    /// Boot an in-process broker plus plaintext h2c gateway and pass its endpoint to adapters.
    #[arg(long)]
    live_substrate: bool,
    /// In live mode, run explicit live-only vectors supported by the current live SDK client.
    #[arg(long)]
    live_compatible_only: bool,
    /// Run this binary as a mock adapter over stdio.
    #[arg(long, hide = true)]
    mock_adapter: bool,
    /// Fault mode for mock adapter negative tests.
    #[arg(long, hide = true, default_value = "none")]
    mock_fault: String,
    /// Contract minor reported by the hidden mock adapter.
    #[arg(long, hide = true, default_value_t = CONTRACT_MINOR_V1_0)]
    mock_contract_minor: u16,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if cli.mock_adapter {
        let fault_mode = match cli.mock_fault.as_str() {
            "none" => FaultMode::None,
            "wrong-publish" => FaultMode::WrongPublish,
            "wrong-configure" => FaultMode::WrongConfigure,
            other => return Err(format!("unknown mock fault mode: {other}").into()),
        };
        let stdin = BufReader::new(tokio::io::stdin());
        let stdout = BufWriter::new(tokio::io::stdout());
        run_stdio(stdin, stdout, fault_mode, cli.mock_contract_minor).await?;
        return Ok(());
    }

    let adapter = cli.adapter.ok_or("--adapter is required")?;
    let substrate = if cli.live_substrate {
        HarnessSubstrate::Live
    } else {
        HarnessSubstrate::External
    };
    let harness = Harness::new(HarnessConfig {
        adapter,
        adapter_args: vec![],
        vectors_dir: cli.vectors,
        filter: cli.filter,
        endpoint: cli.endpoint,
        substrate,
        live_compatible_only: cli.live_compatible_only,
    });
    let summary = harness.run().await?;
    for skipped in &summary.skipped {
        eprintln!("{} skipped: {}", skipped.vector_id, skipped.reason);
    }
    if summary.is_success() {
        if summary.skipped.is_empty() {
            println!("{} vectors passed", summary.passed);
        } else {
            println!(
                "{} vectors passed, {} skipped",
                summary.passed,
                summary.skipped.len()
            );
        }
        Ok(())
    } else {
        for failure in &summary.failed {
            eprintln!(
                "{} / {} failed\nexpected: {:?}\nactual: {:?}",
                failure.vector_id, failure.step, failure.expected, failure.actual
            );
        }
        Err("conformance failed".into())
    }
}
