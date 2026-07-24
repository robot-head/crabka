use std::{net::SocketAddr, sync::Arc, time::Duration};

use clap::Parser;
use crabka_gres_activator::{
    ActivatorConfig, ControlRegistryWakeRegistry, NonEmptyValue, PositiveMillis, ReplicationFactor,
    WakeCoordinator, serve_conn,
};
use crabka_gres_control::Registry;
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
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    registry_replication_factor: ReplicationFactor,
    #[arg(
        long,
        env = "CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE",
        default_value = "{tenant}:5432"
    )]
    backend_endpoint_template: NonEmptyValue,
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
        registry_poll: Duration::from_millis(args.registry_poll_ms.into_value()),
        cold_start_timeout: Duration::from_millis(args.cold_start_timeout_ms.into_value()),
        backend_endpoint_template: args.backend_endpoint_template.into_value(),
    };
    let mut registry = Registry::connect(&cfg.bootstrap).await?;
    registry
        .ensure_topic(args.registry_replication_factor.into_value())
        .await?;
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
    use crabka_gres_activator::{NonEmptyValue, PositiveMillis, ReplicationFactor};

    use super::Args;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const CLEAN_CONFIG_ENV: [(&str, Option<&str>); 6] = [
        ("CRABKA_GRES_ACTIVATOR_LISTEN", None),
        ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", None),
        ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS", None),
        ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS", None),
        ("CRABKA_GRES_ACTIVATOR_REGISTRY_REPLICATION_FACTOR", None),
        ("CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE", None),
    ];

    #[test]
    fn validated_input_boundaries() {
        assert!(PositiveMillis::new(0).is_err());
        assert!(PositiveMillis::new(1).is_ok());
        assert!("0".parse::<PositiveMillis>().is_err());
        assert!("1".parse::<PositiveMillis>().is_ok());
        assert!(ReplicationFactor::new(0).is_err());
        assert!(ReplicationFactor::new(32_767).is_ok());
        assert!(ReplicationFactor::new(32_768).is_err());
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
            assert!(defaults.registry_replication_factor.into_value() == 1);
            assert!(defaults.backend_endpoint_template.into_value() == "{tenant}:5432");

            temp_env::with_vars(
                [
                    ("CRABKA_GRES_ACTIVATOR_LISTEN", Some("127.0.0.1:7433")),
                    ("CRABKA_GRES_ACTIVATOR_BOOTSTRAP", Some("env-broker:9092")),
                    ("CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS", Some("251")),
                    ("CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS", Some("30001")),
                    (
                        "CRABKA_GRES_ACTIVATOR_REGISTRY_REPLICATION_FACTOR",
                        Some("2"),
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
                    assert!(from_env.registry_replication_factor.into_value() == 2);
                    assert!(from_env.backend_endpoint_template.into_value() == "env-backend:5432");

                    let from_cli = Args::try_parse_from([
                        "crabka-gres-activator",
                        "--listen=127.0.0.1:8433",
                        "--bootstrap=cli-broker:9092",
                        "--registry-poll-ms=252",
                        "--cold-start-timeout-ms=30002",
                        "--registry-replication-factor=3",
                        "--backend-endpoint-template=cli-backend:5432",
                    ])
                    .expect("parse CLI over environment");
                    assert!(from_cli.listen.to_string() == "127.0.0.1:8433");
                    assert!(from_cli.bootstrap.into_value() == "cli-broker:9092");
                    assert!(from_cli.registry_poll_ms.into_value() == 252);
                    assert!(from_cli.cold_start_timeout_ms.into_value() == 30_002);
                    assert!(from_cli.registry_replication_factor.into_value() == 3);
                    assert!(from_cli.backend_endpoint_template.into_value() == "cli-backend:5432");
                },
            );
        });
    }
}
