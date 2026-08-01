use std::{net::SocketAddr, sync::Arc};

use clap::Parser;
use crabka_gres_activator::{
    ActivatorConfig, ControlRegistryWakeRegistry, NonEmptyValue, WakeCoordinator, serve_conn,
};
use crabka_gres_control::{Registry, RegistryPolicy, RegistryReplicationFactor};
use crabka_units::{ByteSize, Time};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "crabka-gres-activator")]
struct Args {
    #[arg(long, env = "CRABKA_GRES_ACTIVATOR_LISTEN")]
    listen: SocketAddr,
    #[arg(long, env = "CRABKA_GRES_ACTIVATOR_BOOTSTRAP")]
    bootstrap: NonEmptyValue,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_REGISTRY_POLL",
        default_value = "250ms",
        value_parser = crabka_units::parse::positive_time
    )]
    registry_poll: Time,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT",
        default_value = "30s",
        value_parser = crabka_units::parse::positive_time
    )]
    cold_start_timeout: Time,
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
        long = "client-dispatch-queue-capacity",
        env = "CRABKA_GRES_ACTIVATOR_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long = "client-frame-max",
        env = "CRABKA_GRES_ACTIVATOR_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long = "registry-reader-fetch-min",
        env = "CRABKA_GRES_REGISTRY_READER_FETCH_MIN",
        default_value = "1B",
        value_parser = parse_fetch_min
    )]
    reader_fetch_min: ByteSize,
    #[arg(
        long = "registry-replication-factor",
        env = "CRABKA_GRES_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    replication_factor: RegistryReplicationFactor,
    #[arg(
        long = "registry-topic-create-timeout",
        env = "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT",
        default_value = "15s",
        value_parser = crabka_units::parse::positive_time
    )]
    topic_create_timeout: Time,
    #[arg(
        long = "registry-reader-retry-backoff",
        env = "CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF",
        default_value = "250ms",
        value_parser = crabka_units::parse::positive_time
    )]
    reader_retry_backoff: Time,
    #[arg(
        long = "registry-fetch-max-wait",
        env = "CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT",
        default_value = "500ms",
        value_parser = crabka_units::parse::positive_time
    )]
    fetch_max_wait: Time,
    #[arg(
        long = "registry-fetch-partition-max",
        env = "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX",
        default_value = "1MiB",
        value_parser = crabka_units::parse::positive_byte_size
    )]
    fetch_partition_max: ByteSize,
    #[arg(
        long = "registry-producer-dns-timeout",
        env = "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    producer_dns_timeout: Option<Time>,
    #[arg(
        long = "registry-reader-admin-dns-timeout",
        env = "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    reader_admin_dns_timeout: Option<Time>,
}

impl RegistryOptions {
    fn dispatch_queue_capacity(&self) -> crabka_client_core::ConnectionDispatchQueueCapacity {
        crabka_client_core::ConnectionDispatchQueueCapacity::new(
            self.client_dispatch_queue_capacity,
        )
        .expect("validated activator client dispatch queue capacity")
    }

    fn frame_max(&self) -> crabka_client_core::ClientFrameMax {
        crabka_client_core::ClientFrameMax::try_from(self.client_frame_max)
            .expect("validated activator client frame maximum")
    }

    fn reader_fetch_min(&self) -> crabka_client_core::FetchMinBytes {
        crabka_client_core::FetchMinBytes::try_from(self.reader_fetch_min)
            .expect("validated registry reader fetch minimum")
    }

    fn policy(&self) -> RegistryPolicy {
        let defaults = RegistryPolicy::default();

        RegistryPolicy::new(
            self.replication_factor.into_value(),
            self.topic_create_timeout,
            self.reader_retry_backoff,
            self.fetch_max_wait,
            self.fetch_partition_max,
        )
        .expect("validated registry options")
        .with_producer_dns_timeout(
            self.producer_dns_timeout
                .unwrap_or_else(|| defaults.producer_dns_timeout().time()),
        )
        .expect("validated registry producer DNS timeout")
        .with_reader_admin_dns_timeout(
            self.reader_admin_dns_timeout
                .unwrap_or_else(|| defaults.reader_admin_dns_timeout().time()),
        )
        .expect("validated registry reader/admin DNS timeout")
        .with_client_resource_policy(
            self.dispatch_queue_capacity(),
            self.frame_max(),
            self.reader_fetch_min(),
        )
    }
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    crabka_client_core::ConnectionDispatchQueueCapacity::new(value)
        .map(crabka_client_core::ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value =
        crabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    crabka_client_core::ClientFrameMax::try_from(value)
        .map(crabka_client_core::ClientFrameMax::size)
}

fn parse_fetch_min(value: &str) -> Result<ByteSize, String> {
    let value =
        crabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    crabka_client_core::FetchMinBytes::try_from(value).map(crabka_client_core::FetchMinBytes::size)
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
        registry_poll: args.registry_poll,
        cold_start_timeout: args.cold_start_timeout,
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
    use crabka_units::convert::TimeExt as _;

    use super::Args;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const CLEAN_CONFIG_ENV: [(&str, Option<&str>); 15] = [
        ("CRABKA_GRES_ACTIVATOR_LISTEN", None),
        ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", None),
        ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL", None),
        ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT", None),
        ("CRABKA_GRES_ACTIVATOR_CLIENT_DISPATCH_QUEUE_CAPACITY", None),
        ("CRABKA_GRES_ACTIVATOR_CLIENT_FRAME_MAX", None),
        ("CRABKA_GRES_REGISTRY_READER_FETCH_MIN", None),
        ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", None),
        ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT", None),
        ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF", None),
        ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT", None),
        ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX", None),
        ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT", None),
        ("CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT", None),
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
                "--client-dispatch-queue-capacity=0",
                "--client-frame-max=0B",
                "--client-frame-max=1.5B",
                "--client-frame-max=101MiB",
                "--registry-reader-fetch-min=0B",
                "--registry-reader-fetch-min=1.5B",
                "--registry-poll=0ms",
                "--cold-start-timeout=0ms",
                "--registry-replication-factor=0",
                "--registry-replication-factor=32768",
                "--registry-topic-create-timeout=0ms",
                "--registry-reader-retry-backoff=0ms",
                "--registry-fetch-max-wait=0ms",
                "--registry-fetch-partition-max=0B",
                "--registry-producer-dns-timeout=0ms",
                "--registry-reader-admin-dns-timeout=0ms",
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
    fn client_policy_reads_environment_and_prefers_cli() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(CLEAN_CONFIG_ENV, || {
            temp_env::with_vars(
                [
                    (
                        "CRABKA_GRES_ACTIVATOR_CLIENT_DISPATCH_QUEUE_CAPACITY",
                        Some("7"),
                    ),
                    ("CRABKA_GRES_ACTIVATOR_CLIENT_FRAME_MAX", Some("32KiB")),
                    ("CRABKA_GRES_REGISTRY_READER_FETCH_MIN", Some("3B")),
                ],
                || {
                    let environment = Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:6433",
                        "--bootstrap=broker:9092",
                    ])
                    .expect("parse environment client policy");
                    let environment_policy = environment.registry.policy();
                    assert!(environment_policy.dispatch_queue_capacity().get() == 7);
                    assert!(environment_policy.frame_max().size() == crabka_units::kibibytes(32));
                    assert!(environment_policy.reader_fetch_min().size() == crabka_units::bytes(3));

                    let cli = Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:6433",
                        "--bootstrap=broker:9092",
                        "--client-dispatch-queue-capacity=9",
                        "--client-frame-max=64KiB",
                        "--registry-reader-fetch-min=5B",
                    ])
                    .expect("parse CLI client policy");
                    let cli_policy = cli.registry.policy();
                    assert!(cli_policy.dispatch_queue_capacity().get() == 9);
                    assert!(cli_policy.frame_max().size() == crabka_units::kibibytes(64));
                    assert!(cli_policy.reader_fetch_min().size() == crabka_units::bytes(5));
                },
            );
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
            assert!(defaults.registry_poll.millis_i64() == 250);
            assert!(defaults.cold_start_timeout.millis_i64() == 30_000);
            assert!(defaults.registry.policy() == RegistryPolicy::default());
            assert!(defaults.backend_endpoint_template.into_value() == "{tenant}:5432");

            temp_env::with_vars(
                [
                    ("CRABKA_GRES_ACTIVATOR_LISTEN", Some("127.0.0.1:7433")),
                    ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", Some("env-broker:9092")),
                    ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL", Some("251ms")),
                    ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT", Some("30001ms")),
                    ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", Some("2")),
                    ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT", Some("15001ms")),
                    ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF", Some("251ms")),
                    ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT", Some("501ms")),
                    ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX", Some("1048577B")),
                    ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT", Some("37ms")),
                    (
                        "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT",
                        Some("37ms"),
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
                    assert!(from_env.registry_poll.millis_i64() == 251);
                    assert!(from_env.cold_start_timeout.millis_i64() == 30_001);
                    let environment_policy = RegistryPolicy::new(
                        2,
                        crabka_units::millis(15_001),
                        crabka_units::millis(251),
                        crabka_units::millis(501),
                        crabka_units::bytes(1_048_577),
                    )
                    .expect("policy")
                    .with_producer_dns_timeout(crabka_units::millis(37))
                    .expect("environment DNS timeout")
                    .with_reader_admin_dns_timeout(crabka_units::millis(37))
                    .expect("environment reader/admin DNS timeout");
                    assert!(from_env.registry.policy() == environment_policy);
                    assert!(from_env.backend_endpoint_template.into_value() == "env-backend:5432");

                    let from_cli = Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:8433",
                        "--bootstrap=cli-broker:9092",
                        "--registry-poll=252ms",
                        "--cold-start-timeout=30002ms",
                        "--registry-replication-factor=3",
                        "--registry-topic-create-timeout=15002ms",
                        "--registry-reader-retry-backoff=252ms",
                        "--registry-fetch-max-wait=502ms",
                        "--registry-fetch-partition-max=1048578B",
                        "--registry-producer-dns-timeout=47ms",
                        "--registry-reader-admin-dns-timeout=47ms",
                        "--backend-endpoint-template=cli-backend:5432",
                    ])
                    .expect("parse CLI over environment");
                    assert!(from_cli.listen.to_string() == "127.0.0.1:8433");
                    assert!(from_cli.bootstrap.into_value() == "cli-broker:9092");
                    assert!(from_cli.registry_poll.millis_i64() == 252);
                    assert!(from_cli.cold_start_timeout.millis_i64() == 30_002);
                    let cli_policy = RegistryPolicy::new(
                        3,
                        crabka_units::millis(15_002),
                        crabka_units::millis(252),
                        crabka_units::millis(502),
                        crabka_units::bytes(1_048_578),
                    )
                    .expect("policy")
                    .with_producer_dns_timeout(crabka_units::millis(47))
                    .expect("CLI DNS timeout")
                    .with_reader_admin_dns_timeout(crabka_units::millis(47))
                    .expect("CLI reader/admin DNS timeout");
                    assert!(from_cli.registry.policy() == cli_policy);
                    assert!(from_cli.backend_endpoint_template.into_value() == "cli-backend:5432");
                },
            );
        });
    }
}
