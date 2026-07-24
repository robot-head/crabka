use std::{net::SocketAddr, str::FromStr, time::Duration};

use clap::{Args, ValueEnum};
use refined_type::rule::GreaterU64;
use serde::{Deserialize, Serialize};

/// A validated positive operator configuration value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct PositiveU64(u64);

impl PositiveU64 {
    /// Return the validated value.
    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
    }

    /// Return the value as a duration in milliseconds.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}

impl TryFrom<u64> for PositiveU64 {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }
}

impl From<PositiveU64> for u64 {
    fn from(value: PositiveU64) -> Self {
        value.into_value()
    }
}

impl FromStr for PositiveU64 {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<u64>()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::try_from)
    }
}

/// Operator runtime configuration.
///
/// All fields can be set via CLI (`--watch-namespaces`, `--health-addr`, …)
/// or via env (`WATCH_NAMESPACES`, `HEALTH_ADDR`, …). CLI wins on conflict.
#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct OperatorConfig {
    /// Comma-separated namespaces to watch. Empty = cluster-scoped.
    #[arg(long, env = "WATCH_NAMESPACES", value_delimiter = ',', num_args = 0..)]
    pub watch_namespaces: Vec<String>,

    /// Namespace the operator runs in (used for the leader-election Lease).
    #[arg(long, env = "OPERATOR_NAMESPACE", default_value = "crabka-operator")]
    pub operator_namespace: String,

    /// Lease name for leader election.
    #[arg(long, env = "LEASE_NAME", default_value = "crabka-operator-leader")]
    pub lease_name: String,

    /// Identity advertised in the Lease (typically the pod name).
    #[arg(long, env = "POD_NAME", default_value = "crabka-operator-local")]
    pub pod_name: String,

    /// Address for `/healthz`, `/readyz`, `/metrics`.
    #[arg(long, env = "HEALTH_ADDR", default_value = "0.0.0.0:8080")]
    pub health_addr: SocketAddr,

    /// Tracing filter (e.g. `info,kube=warn`).
    #[arg(
        long,
        env = "RUST_LOG",
        default_value = "info,kube_client::client::builder=warn"
    )]
    pub log_filter: String,

    /// Default broker image used when `Kafka.spec.image` is unset.
    #[arg(long, env = "DEFAULT_BROKER_IMAGE")]
    pub default_broker_image: Option<String>,

    /// Default gateway image used when `KafkaGrpcGateway.spec.image` is unset.
    #[arg(long, env = "DEFAULT_GATEWAY_IMAGE")]
    pub default_gateway_image: Option<String>,
    /// Default schema-registry image used when `SchemaRegistry.spec.image` is unset.
    #[arg(long, env = "DEFAULT_SCHEMA_REGISTRY_IMAGE")]
    pub default_schema_registry_image: Option<String>,

    /// Default Gres compute image used when `GresTenant` compute image support is added.
    #[arg(long, env = "DEFAULT_GRES_IMAGE")]
    pub default_gres_image: Option<String>,

    /// Default `PgDog` image used when `Gres.spec.pgdog.image` is unset.
    #[arg(long, env = "DEFAULT_PGDOG_IMAGE")]
    pub default_pgdog_image: Option<String>,

    /// Default Gres activator image used by `Gres` fleets.
    #[arg(long, env = "DEFAULT_GRES_ACTIVATOR_IMAGE")]
    pub default_gres_activator_image: Option<String>,

    /// Number of attempts to reload and verify a `PgDog` configuration.
    #[arg(long, env = "PGDOG_RELOAD_ATTEMPTS", default_value = "3")]
    pub pgdog_reload_attempts: PositiveU64,
    /// Delay between `PgDog` reload verification attempts.
    #[arg(long, env = "PGDOG_RELOAD_BACKOFF_MS", default_value = "100")]
    pub pgdog_reload_backoff_ms: PositiveU64,
    /// Requeue delay after `PgDog` remains stale.
    #[arg(long, env = "PGDOG_RELOAD_REQUEUE_MS", default_value = "15000")]
    pub pgdog_reload_requeue_ms: PositiveU64,
    /// Timeout for one `PgDog` admin reload operation.
    #[arg(long, env = "PGDOG_ADMIN_TIMEOUT_MS", default_value = "20000")]
    pub pgdog_admin_timeout_ms: PositiveU64,
    /// Fallback poll interval when no earlier `PgDog` transition is pending.
    #[arg(long, env = "PGDOG_TRANSITION_POLL_MS", default_value = "60000")]
    pub pgdog_transition_poll_ms: PositiveU64,
    /// Requeue delay after a controller reconcile error.
    #[arg(long, env = "CONTROLLER_ERROR_REQUEUE_MS", default_value = "15000")]
    pub controller_error_requeue_ms: PositiveU64,

    /// Durable checkpoint object store used to verify a suspended tenant before
    /// its WAL topics are deleted. Parking is disabled when this is unset.
    #[arg(long, env = "GRES_CHECKPOINT_STORE", value_enum)]
    pub gres_checkpoint_store: Option<GresCheckpointStoreKind>,
    /// Bucket containing Gres checkpoint manifests.
    #[arg(long, env = "GRES_CHECKPOINT_BUCKET")]
    pub gres_checkpoint_bucket: Option<String>,
    /// S3 region. Required when `GRES_CHECKPOINT_STORE=s3`.
    #[arg(long, env = "GRES_CHECKPOINT_REGION")]
    pub gres_checkpoint_region: Option<String>,
    /// Optional S3-compatible or GCS endpoint.
    #[arg(long, env = "GRES_CHECKPOINT_ENDPOINT")]
    pub gres_checkpoint_endpoint: Option<String>,
    /// Permit an HTTP object-store endpoint, intended for explicitly configured development stores.
    #[arg(long, env = "GRES_CHECKPOINT_ALLOW_HTTP", default_value_t = false)]
    pub gres_checkpoint_allow_http: bool,
    /// Optional explicit S3 access key id; otherwise the provider chain is used.
    #[arg(long, env = "GRES_CHECKPOINT_ACCESS_KEY_ID")]
    pub gres_checkpoint_access_key_id: Option<String>,
    /// Optional explicit S3 secret access key; otherwise the provider chain is used.
    #[arg(long, env = "GRES_CHECKPOINT_SECRET_ACCESS_KEY")]
    pub gres_checkpoint_secret_access_key: Option<String>,
    /// Optional GCS service-account JSON path.
    #[arg(long, env = "GRES_CHECKPOINT_GCS_SERVICE_ACCOUNT_PATH")]
    pub gres_checkpoint_gcs_service_account_path: Option<String>,
    /// Optional GCS ADC JSON path.
    #[arg(long, env = "GRES_CHECKPOINT_GCS_APPLICATION_CREDENTIALS_PATH")]
    pub gres_checkpoint_gcs_application_credentials_path: Option<String>,
}

/// Supported durable checkpoint object-store providers for tenant WAL parking.
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GresCheckpointStoreKind {
    /// S3 or an S3-compatible endpoint.
    S3,
    /// Google Cloud Storage.
    Gcs,
}

impl OperatorConfig {
    /// Iterator over watched namespaces, or `None` for cluster-scoped.
    #[must_use]
    pub fn watched(&self) -> Option<&[String]> {
        if self.watch_namespaces.is_empty() {
            None
        } else {
            Some(&self.watch_namespaces)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        cfg: OperatorConfig,
    }

    #[test]
    fn cli_defaults_compute_cluster_scope() {
        let parsed = Wrap::parse_from(["bin"]);
        assert!(parsed.cfg.watched().is_none());
        assert!(parsed.cfg.operator_namespace == "crabka-operator");
        assert!(parsed.cfg.pgdog_reload_attempts.into_value() == 3);
        assert!(parsed.cfg.pgdog_reload_backoff_ms.into_value() == 100);
        assert!(parsed.cfg.pgdog_reload_requeue_ms.into_value() == 15_000);
        assert!(parsed.cfg.pgdog_admin_timeout_ms.into_value() == 20_000);
        assert!(parsed.cfg.pgdog_transition_poll_ms.into_value() == 60_000);
        assert!(parsed.cfg.controller_error_requeue_ms.into_value() == 15_000);
    }

    #[test]
    fn comma_separated_namespaces_parse() {
        let parsed = Wrap::parse_from(["bin", "--watch-namespaces=a,b,c"]);
        assert!(parsed.cfg.watch_namespaces == vec!["a", "b", "c"]);
        assert!(parsed.cfg.watched().is_some());
    }

    #[test]
    fn environment_values_parse_and_cli_wins() {
        const CHILD: &str = "CRABKA_OPERATOR_CONFIG_ENV_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "config::tests::environment_values_parse_and_cli_wins",
                ])
                .env(CHILD, "1")
                .env("PGDOG_RELOAD_ATTEMPTS", "4")
                .env("PGDOG_RELOAD_BACKOFF_MS", "5")
                .env("PGDOG_RELOAD_REQUEUE_MS", "6")
                .env("PGDOG_ADMIN_TIMEOUT_MS", "7")
                .env("PGDOG_TRANSITION_POLL_MS", "8")
                .env("CONTROLLER_ERROR_REQUEUE_MS", "9")
                .output()
                .expect("spawn isolated environment test");
            assert!(
                output.status.success(),
                "child stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let parsed = Wrap::parse_from(["bin", "--pgdog-reload-attempts", "10"]);
        assert!(parsed.cfg.pgdog_reload_attempts.into_value() == 10);
        assert!(parsed.cfg.pgdog_reload_backoff_ms.into_value() == 5);
        assert!(parsed.cfg.pgdog_reload_requeue_ms.into_value() == 6);
        assert!(parsed.cfg.pgdog_admin_timeout_ms.into_value() == 7);
        assert!(parsed.cfg.pgdog_transition_poll_ms.into_value() == 8);
        assert!(parsed.cfg.controller_error_requeue_ms.into_value() == 9);
    }

    #[test]
    fn controller_timing_values_reject_zero_and_overflow() {
        for option in [
            "--pgdog-reload-attempts",
            "--pgdog-reload-backoff-ms",
            "--pgdog-reload-requeue-ms",
            "--pgdog-admin-timeout-ms",
            "--pgdog-transition-poll-ms",
            "--controller-error-requeue-ms",
        ] {
            assert!(Wrap::try_parse_from(["bin", option, "0"]).is_err());
            assert!(Wrap::try_parse_from(["bin", option, "18446744073709551616"]).is_err());
        }
    }

    #[test]
    fn positive_config_value_serde_rejects_zero() {
        let value: PositiveU64 = "123".parse().expect("positive value");
        assert!(serde_json::to_value(value).unwrap() == 123);
        assert!(serde_json::from_str::<PositiveU64>("0").is_err());
    }
}
