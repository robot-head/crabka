//! `crabka-bench-driver` — runs one scenario × one stack and writes a
//! `RunOutput` JSON file.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use crabka_bench_driver::{
    prom::PrometheusRequestTimeoutSeconds,
    scenario::{Scenario, Stack},
    workload::{self, ClientRequestTimeoutSeconds, DriverConfig},
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
    /// HTTP request timeout for Prometheus queries, in seconds.
    #[arg(
        long,
        env = "BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS",
        default_value_t = PrometheusRequestTimeoutSeconds::default()
    )]
    prometheus_request_timeout_seconds: PrometheusRequestTimeoutSeconds,
    /// Producer request timeout, in seconds.
    #[arg(
        long,
        env = "BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS",
        default_value_t = workload::default_producer_request_timeout()
    )]
    producer_request_timeout_seconds: ClientRequestTimeoutSeconds,
    /// Consumer request timeout, in seconds. Defaults to 5 for Crabka and 30
    /// for Kafka.
    #[arg(long, env = "BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS")]
    consumer_request_timeout_seconds: Option<ClientRequestTimeoutSeconds>,
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

fn resolve_consumer_request_timeout(
    stack: Stack,
    configured: Option<ClientRequestTimeoutSeconds>,
) -> ClientRequestTimeoutSeconds {
    configured.unwrap_or_else(|| workload::default_consumer_request_timeout(stack))
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

    let stack = cli.stack.into_stack();
    let consumer_request_timeout_seconds =
        resolve_consumer_request_timeout(stack, cli.consumer_request_timeout_seconds);
    let cfg = DriverConfig {
        bootstrap: cli.bootstrap,
        topic: cli.topic,
        stack,
        namespace: cli.namespace,
        prometheus_url: cli.prometheus,
        prometheus_request_timeout_seconds: cli.prometheus_request_timeout_seconds,
        producer_request_timeout_seconds: cli.producer_request_timeout_seconds,
        consumer_request_timeout_seconds,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use super::*;

    fn required_args(stack: &'static str) -> Vec<&'static str> {
        vec![
            "crabka-bench-driver",
            "--scenario",
            "scenario.yaml",
            "--bootstrap",
            "broker:9092",
            "--stack",
            stack,
        ]
    }

    #[test]
    fn client_request_timeout_defaults_follow_active_stack() {
        let crabka = Cli::try_parse_from(required_args("crabka")).expect("Crabka timeout defaults");
        let kafka = Cli::try_parse_from(required_args("kafka")).expect("Kafka timeout defaults");

        assert_eq!(
            crabka.producer_request_timeout_seconds.duration(),
            Duration::from_secs(2)
        );
        assert_eq!(
            resolve_consumer_request_timeout(
                Stack::Crabka,
                crabka.consumer_request_timeout_seconds,
            )
            .duration(),
            Duration::from_secs(5)
        );
        assert_eq!(
            resolve_consumer_request_timeout(Stack::Kafka, kafka.consumer_request_timeout_seconds,)
                .duration(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn client_request_timeout_rejects_invalid_cli_values() {
        for option in [
            "--producer-request-timeout-seconds",
            "--consumer-request-timeout-seconds",
        ] {
            for invalid in ["0", "not-a-number", "-1", "2147484"] {
                let mut args = required_args("crabka");
                args.extend([option, invalid]);
                assert!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
            }
        }
    }

    #[test]
    fn client_request_timeouts_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_BENCH_CLIENT_TIMEOUTS_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_request_timeouts_read_environment_and_prefer_cli",
                    ])
                    .env(CHILD, "1")
                    .env("BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS", "11")
                    .env("BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS", "12")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(required_args("crabka")).expect("environment");
        assert_eq!(
            from_env.producer_request_timeout_seconds.duration(),
            Duration::from_secs(11)
        );
        assert_eq!(
            resolve_consumer_request_timeout(
                Stack::Crabka,
                from_env.consumer_request_timeout_seconds,
            )
            .duration(),
            Duration::from_secs(12)
        );

        let mut args = required_args("crabka");
        args.extend([
            "--producer-request-timeout-seconds",
            "21",
            "--consumer-request-timeout-seconds",
            "22",
        ]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        assert_eq!(
            from_cli.producer_request_timeout_seconds.duration(),
            Duration::from_secs(21)
        );
        assert_eq!(
            resolve_consumer_request_timeout(
                Stack::Crabka,
                from_cli.consumer_request_timeout_seconds,
            )
            .duration(),
            Duration::from_secs(22)
        );
    }

    #[test]
    fn prometheus_request_timeout_environment_and_cli_precedence() {
        const CHILD: &str = "CRABKA_BENCH_PROMETHEUS_TIMEOUT_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::prometheus_request_timeout_environment_and_cli_precedence",
                    ])
                    .env(CHILD, "1")
                    .env("BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS", "32")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(required_args("crabka")).expect("environment");
        assert_eq!(
            from_env.prometheus_request_timeout_seconds.duration(),
            Duration::from_secs(32)
        );

        let mut args = required_args("crabka");
        args.extend(["--prometheus-request-timeout-seconds", "64"]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        assert_eq!(
            from_cli.prometheus_request_timeout_seconds.duration(),
            Duration::from_secs(64)
        );
    }
}
