//! `crabka-bench-driver` — runs one scenario × one stack and writes a
//! `RunOutput` JSON file.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use crabka_bench_driver::{
    scenario::{Scenario, Stack},
    workload::{self, DriverConfig},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "crabka-bench-driver", version, about)]
struct Cli {
    /// Path to the scenario YAML.
    #[arg(long, env = "BENCH_SCENARIO_PATH")]
    scenario: PathBuf,
    /// Kafka bootstrap servers (host:port).
    #[arg(long, env = "BENCH_BOOTSTRAP_SERVERS")]
    bootstrap: String,
    /// Which Kafka stack this is — pure metadata, doesn't change behaviour.
    #[arg(long, env = "BENCH_STACK", value_enum)]
    stack: StackArg,
    /// Topic name (must already exist; orchestrator creates it via `KafkaTopic` CR).
    #[arg(long, env = "BENCH_TOPIC", default_value = "bench-topic")]
    topic: String,
    /// Namespace the brokers live in (used for Prometheus pod regex).
    #[arg(long, env = "BENCH_NAMESPACE", default_value = "default")]
    namespace: String,
    /// Prometheus base URL. If absent, resource fields are zero and
    /// `notes` reflects the skip.
    #[arg(long, env = "BENCH_PROMETHEUS_URL")]
    prometheus: Option<String>,
    /// Configured broker count. The driver uses this to gate RF=3-only
    /// scenarios.
    #[arg(long, env = "BENCH_BROKER_COUNT", default_value_t = 1)]
    broker_count: u32,
    /// Output path for the `RunOutput` JSON.
    #[arg(long, env = "BENCH_OUTPUT_PATH", default_value = "/results/run.json")]
    out: PathBuf,
    /// Enable a TLS-encrypted data path: producers and consumers dial the
    /// broker's `Ssl` listener instead of plaintext. Unset (the default) keeps
    /// the existing plaintext benchmark path unchanged.
    #[arg(long, env = "BENCH_TLS_ENABLED", default_value_t = false)]
    tls_enabled: bool,
    /// PEM CA bundle the client trusts to verify the broker serving cert.
    /// Required when `--tls-enabled`; mounted from the per-stack cluster-CA
    /// Secret (e.g. `/etc/bench-ca/ca.crt`).
    #[arg(long, env = "BENCH_TLS_CA_PATH")]
    tls_ca_path: Option<PathBuf>,
    /// SNI / server-name presented in the TLS handshake, matched against a SAN
    /// on the broker serving cert. LOAD-BEARING: the bootstrap is resolved to a
    /// pod IP and dialed by IP, so the SNI must be set explicitly to a SAN name
    /// (crabka: `demo-broker-headless.<ns>.svc.cluster.local`;
    /// Strimzi: `demo-kafka-bootstrap`). Required when `--tls-enabled`.
    #[arg(long, env = "BENCH_TLS_SERVER_NAME")]
    tls_server_name: Option<String>,
    /// Optional mTLS client certificate (PEM). One-way TLS is sufficient for
    /// the benchmark; set this (with `--tls-client-key`) only when the listener
    /// requires client auth.
    #[arg(long, env = "BENCH_TLS_CLIENT_CERT")]
    tls_client_cert: Option<PathBuf>,
    /// Optional mTLS client private key (PEM). Pairs with `--tls-client-cert`.
    #[arg(long, env = "BENCH_TLS_CLIENT_KEY")]
    tls_client_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum StackArg {
    Crabka,
    Kafka,
}

impl StackArg {
    fn into_stack(self) -> Stack {
        match self {
            StackArg::Crabka => Stack::Crabka,
            StackArg::Kafka => Stack::Kafka,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();

    // rustls's process-global crypto provider needs to be installed
    // exactly once before any TLS code runs. The `kube` and `reqwest`
    // rustls features both rely on this. Failing to install means TLS
    // operations panic with "no process-level CryptoProvider available".
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();

    let yaml = tokio::fs::read_to_string(&cli.scenario)
        .await
        .with_context(|| format!("read scenario {}", cli.scenario.display()))?;
    let scenario: Scenario = serde_yaml::from_str(&yaml).context("parse scenario yaml")?;

    let scenario_id = hash_str(&scenario.name);

    // Assemble the TLS data-path config from the CLI/env knobs. When
    // `--tls-enabled` is set, a CA path and a server-name (SNI) are mandatory:
    // without the explicit SNI the handshake validates against a pod IP that is
    // not on the cert and fails. mTLS is optional (one-way TLS by default).
    let tls = if cli.tls_enabled {
        let ca_path = cli
            .tls_ca_path
            .context("BENCH_TLS_ENABLED set but BENCH_TLS_CA_PATH missing")?;
        let server_name = cli
            .tls_server_name
            .context("BENCH_TLS_ENABLED set but BENCH_TLS_SERVER_NAME missing")?;
        let client_identity = match (cli.tls_client_cert, cli.tls_client_key) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => anyhow::bail!(
                "BENCH_TLS_CLIENT_CERT and BENCH_TLS_CLIENT_KEY must be set together (mTLS) or both unset (one-way TLS)"
            ),
        };
        Some(workload::TlsParams {
            ca_path,
            server_name,
            client_identity,
        })
    } else {
        None
    };

    let cfg = DriverConfig {
        bootstrap: cli.bootstrap,
        topic: cli.topic,
        stack: cli.stack.into_stack(),
        namespace: cli.namespace,
        prometheus_url: cli.prometheus,
        broker_count: cli.broker_count,
        scenario_id,
        tls,
    };

    let out = workload::run(scenario, cfg).await?;

    if let Some(parent) = cli.out.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let json = serde_json::to_string_pretty(&out).context("encode run output")?;
    tokio::fs::write(&cli.out, json)
        .await
        .with_context(|| format!("write run output to {}", cli.out.display()))?;

    // Brief stdout summary so `kubectl logs` shows progress.
    println!(
        "stack={:?} scenario={} produced={} consumed={} mb_in={:.2} p99_ms={:.2}",
        out.stack,
        out.scenario.name,
        out.throughput.msgs_produced,
        out.throughput.msgs_consumed,
        out.throughput.mb_in,
        out.producer_latency_ms.p99_ms,
    );
    Ok(())
}

/// Stable, simple hash so producer-stamped magic IDs match across pods.
fn hash_str(s: &str) -> u64 {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}
