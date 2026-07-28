use std::{net::SocketAddr, sync::Arc};

use clap::Parser;
use crabka_gres_activator::{
    ActivatorConfig, ControlRegistryWakeRegistry, NonEmptyValue, PositiveMillis, WakeCoordinator,
    serve_conn,
};
use crabka_gres_control::{
    PositiveI32, PositiveMillis as RegistryPositiveMillis, Registry, RegistryPolicy,
    RegistryReplicationFactor,
};
use crabka_units::{Time, convert::TimeExt as _};
use tokio::net::TcpListener;

/// A validated positive millisecond count as a time extent.
///
/// [`PositiveMillis`] is the activator's parse-level validator over the raw
/// `u64` a CLI flag carries; this is the seam where it becomes a quantity.
/// [`TimeExt::from_millis`] takes an `i64`, so a value past `i64::MAX`
/// milliseconds saturates rather than wrapping negative.
fn positive_millis(value: PositiveMillis) -> Time {
    Time::from_millis(i64::try_from(value.into_value()).unwrap_or(i64::MAX))
}

#[derive(Debug, Parser)]
#[command(name = "crabka-gres-activator")]
struct Args {
    #[arg(long, env = "CRABKA_GRES_ACTIVATOR_LISTEN")]
    listen: SocketAddr,
    #[arg(long, env = "CRABKA_GRES_ACTIVATOR_BOOTSTRAP")]
    bootstrap: NonEmptyValue,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS",
        default_value = "250"
    )]
    registry_poll_ms: PositiveMillis,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS",
        default_value = "30000"
    )]
    cold_start_timeout_ms: PositiveMillis,
    #[command(flatten)]
    registry: RegistryOptions,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE",
        default_value = "{tenant}:5432"
    )]
    backend_endpoint_template: NonEmptyValue,
}

#[derive(Debug, clap::Args)]
struct RegistryOptions {
    #[arg(
        long = "registry-replication-factor",
        env = "CRABKA_GRES_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    replication_factor: RegistryReplicationFactor,
    #[arg(
        long = "registry-topic-create-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS",
        default_value = "15000"
    )]
    topic_create_timeout_ms: PositiveI32,
    #[arg(
        long = "registry-reader-retry-backoff-ms",
        env = "CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS",
        default_value = "250"
    )]
    reader_retry_backoff_ms: RegistryPositiveMillis,
    #[arg(
        long = "registry-fetch-max-wait-ms",
        env = "CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS",
        default_value = "500"
    )]
    fetch_max_wait_ms: PositiveI32,
    #[arg(
        long = "registry-fetch-partition-max-bytes",
        env = "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES",
        default_value = "1048576"
    )]
    fetch_partition_max_bytes: PositiveI32,
    #[arg(
        long = "registry-producer-dns-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS"
    )]
    producer_dns_timeout_ms: Option<RegistryPositiveMillis>,
    #[arg(
        long = "registry-reader-admin-dns-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS"
    )]
    reader_admin_dns_timeout_ms: Option<RegistryPositiveMillis>,
}

impl RegistryOptions {
    fn policy(&self) -> RegistryPolicy {
        let producer_dns_timeout_ms = self.producer_dns_timeout_ms.map_or_else(
            || {
                RegistryPolicy::default()
                    .producer_dns_timeout()
                    .milliseconds()
            },
            RegistryPositiveMillis::into_value,
        );
        let reader_admin_dns_timeout_ms = self.reader_admin_dns_timeout_ms.map_or_else(
            || {
                RegistryPolicy::default()
                    .reader_admin_dns_timeout()
                    .milliseconds()
            },
            RegistryPositiveMillis::into_value,
        );

        RegistryPolicy::new(
            self.replication_factor.into_value(),
            self.topic_create_timeout_ms.into_value(),
            self.reader_retry_backoff_ms.into_value(),
            self.fetch_max_wait_ms.into_value(),
            self.fetch_partition_max_bytes.into_value(),
        )
        .expect("validated registry options")
        .with_producer_dns_timeout_ms(producer_dns_timeout_ms)
        .expect("validated registry producer DNS timeout")
        .with_reader_admin_dns_timeout_ms(reader_admin_dns_timeout_ms)
        .expect("validated registry reader/admin DNS timeout")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let cfg = ActivatorConfig {
        listen: args.listen,
        bootstrap: args.bootstrap.into_value(),
        registry_poll: positive_millis(args.registry_poll_ms),
        cold_start_timeout: positive_millis(args.cold_start_timeout_ms),
        backend_endpoint_template: args.backend_endpoint_template.into_value(),
    };
    let mut registry =
        Registry::connect_with_policy(&cfg.bootstrap, args.registry.policy()).await?;
    registry.ensure_topic().await?;
    let coordinator = Arc::new(WakeCoordinator::new(ControlRegistryWakeRegistry::new(
        registry,
        cfg.clone(),
    )));
    let listener = TcpListener::bind(cfg.listen).await?;
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let coordinator = Arc::clone(&coordinator);
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_conn(stream, &coordinator, &cfg).await {
                tracing::warn!(%peer_addr, %error, "gres activator connection failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::assert;
    use clap::Parser;
    use crabka_gres_activator::{NonEmptyValue, PositiveMillis};
    use crabka_gres_control::RegistryPolicy;

    use super::Args;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const CLEAN_CONFIG_ENV: [(&str, Option<&str>); 12] = [
        ("CRABKA_GRES_ACTIVATOR_LISTEN", None),
        ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", None),
        ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS", None),
        ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS", None),
        ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", None),
        ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS", None),
        ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS", None),
        ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS", None),
        ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES", None),
        ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS", None),
        ("CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS", None),
        ("CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE", None),
    ];

    #[test]
    fn validated_input_boundaries() {
        assert!(PositiveMillis::new(0).is_err());
        assert!(PositiveMillis::new(1).is_ok());
        assert!("0".parse::<PositiveMillis>().is_err());
        assert!("1".parse::<PositiveMillis>().is_ok());
        assert!(NonEmptyValue::new(String::new()).is_err());
        assert!("broker:9092".parse::<NonEmptyValue>().is_ok());
    }

    #[test]
    fn validated_input_rejects_invalid_cli_values() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(CLEAN_CONFIG_ENV, || {
            assert!(
                Args::try_parse_from([
                    "crabka-gres-activator",
                    "--listen=127.0.0.1:6433",
                    "--bootstrap=",
                ])
                .is_err()
            );
            for value in [
                "--registry-poll-ms=0",
                "--cold-start-timeout-ms=0",
                "--registry-replication-factor=0",
                "--registry-replication-factor=32768",
                "--registry-topic-create-timeout-ms=0",
                "--registry-reader-retry-backoff-ms=0",
                "--registry-fetch-max-wait-ms=0",
                "--registry-fetch-partition-max-bytes=0",
                "--registry-producer-dns-timeout-ms=0",
                "--registry-reader-admin-dns-timeout-ms=0",
                "--backend-endpoint-template=",
            ] {
                assert!(
                    Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:6433",
                        "--bootstrap=broker:9092",
                        value,
                    ])
                    .is_err()
                );
            }
        });
    }

    #[test]
    fn validated_input_defaults_environment_and_precedence() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(CLEAN_CONFIG_ENV, || {
            let defaults = Args::try_parse_from([
                "crabka-gres-activator",
                "--listen=127.0.0.1:6433",
                "--bootstrap=broker:9092",
            ])
            .expect("parse defaults");
            assert!(defaults.listen.to_string() == "127.0.0.1:6433");
            assert!(defaults.bootstrap.into_value() == "broker:9092");
            assert!(defaults.registry_poll_ms.into_value() == 250);
            assert!(defaults.cold_start_timeout_ms.into_value() == 30_000);
            assert!(defaults.registry.policy() == RegistryPolicy::default());
            assert!(defaults.backend_endpoint_template.into_value() == "{tenant}:5432");

            temp_env::with_vars(
                [
                    ("CRABKA_GRES_ACTIVATOR_LISTEN", Some("127.0.0.1:7433")),
                    ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", Some("env-broker:9092")),
                    ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS", Some("251")),
                    ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS", Some("30001")),
                    ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", Some("2")),
                    (
                        "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS",
                        Some("15001"),
                    ),
                    ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS", Some("251")),
                    ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS", Some("501")),
                    (
                        "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES",
                        Some("1048577"),
                    ),
                    ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS", Some("37")),
                    (
                        "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS",
                        Some("37"),
                    ),
                    (
                        "CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE",
                        Some("env-backend:5432"),
                    ),
                ],
                || {
                    let from_env =
                        Args::try_parse_from(["crabka-gres-activator"]).expect("parse environment");
                    assert!(from_env.listen.to_string() == "127.0.0.1:7433");
                    assert!(from_env.bootstrap.into_value() == "env-broker:9092");
                    assert!(from_env.registry_poll_ms.into_value() == 251);
                    assert!(from_env.cold_start_timeout_ms.into_value() == 30_001);
                    let environment_policy = RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
                        .expect("policy")
                        .with_producer_dns_timeout_ms(37)
                        .expect("environment DNS timeout")
                        .with_reader_admin_dns_timeout_ms(37)
                        .expect("environment reader/admin DNS timeout");
                    assert!(from_env.registry.policy() == environment_policy);
                    assert!(from_env.backend_endpoint_template.into_value() == "env-backend:5432");

                    let from_cli = Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:8433",
                        "--bootstrap=cli-broker:9092",
                        "--registry-poll-ms=252",
                        "--cold-start-timeout-ms=30002",
                        "--registry-replication-factor=3",
                        "--registry-topic-create-timeout-ms=15002",
                        "--registry-reader-retry-backoff-ms=252",
                        "--registry-fetch-max-wait-ms=502",
                        "--registry-fetch-partition-max-bytes=1048578",
                        "--registry-producer-dns-timeout-ms=47",
                        "--registry-reader-admin-dns-timeout-ms=47",
                        "--backend-endpoint-template=cli-backend:5432",
                    ])
                    .expect("parse CLI over environment");
                    assert!(from_cli.listen.to_string() == "127.0.0.1:8433");
                    assert!(from_cli.bootstrap.into_value() == "cli-broker:9092");
                    assert!(from_cli.registry_poll_ms.into_value() == 252);
                    assert!(from_cli.cold_start_timeout_ms.into_value() == 30_002);
                    let cli_policy = RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578)
                        .expect("policy")
                        .with_producer_dns_timeout_ms(47)
                        .expect("CLI DNS timeout")
                        .with_reader_admin_dns_timeout_ms(47)
                        .expect("CLI reader/admin DNS timeout");
                    assert!(from_cli.registry.policy() == cli_policy);
                    assert!(from_cli.backend_endpoint_template.into_value() == "cli-backend:5432");
                },
            );
        });
    }
}
