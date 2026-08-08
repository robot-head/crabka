use std::{net::SocketAddr, str::FromStr};

use clap::{Args, ValueEnum};
use crabka_units::{ByteSize, Time, convert::TimeExt as _, parse};
use refined_type::rule::GreaterU64;
use serde::{Deserialize, Serialize};

/// A validated positive operator configuration value.
///
/// The `refined_type` rule rejects zero, so an instance is proof of a usable
/// dimensionless count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct PositiveU64(u64);

impl PositiveU64 {
    /// Return the validated value.
    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
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
/// You can set all fields with a CLI flag such as `--watch-namespaces` or
/// `--health-addr`, or with an env variable such as `WATCH_NAMESPACES` or
/// `HEALTH_ADDR`. The CLI value wins on a conflict.
#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct OperatorConfig {
    /// Comma-separated namespaces to watch. An empty value means
    /// cluster-scoped.
    #[arg(long, env = "WATCH_NAMESPACES", value_delimiter = ',', num_args = 0..)]
    pub watch_namespaces: Vec<String>,

    /// Namespace the operator runs in. The leader-election Lease uses it.
    #[arg(long, env = "OPERATOR_NAMESPACE", default_value = "crabka-operator")]
    pub operator_namespace: String,

    /// Lease name for leader election.
    #[arg(long, env = "LEASE_NAME", default_value = "crabka-operator-leader")]
    pub lease_name: String,

    /// Identity advertised in the Lease. This is usually the pod name.
    #[arg(long, env = "POD_NAME", default_value = "crabka-operator-local")]
    pub pod_name: String,

    /// Address for `/healthz`, `/readyz`, `/metrics`.
    #[arg(long, env = "HEALTH_ADDR", default_value = "0.0.0.0:8080")]
    pub health_addr: SocketAddr,

    /// Capacity shared by every outbound Kafka admin connection.
    #[arg(
        long,
        env = "OPERATOR_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    pub client_dispatch_queue_capacity: usize,

    /// Maximum frame size shared by every outbound Kafka admin connection.
    #[arg(
        long,
        env = "OPERATOR_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    pub client_frame_max: ByteSize,

    /// Tracing filter, for example `info,kube=warn`.
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
    /// Default connector worker image used when `KafkaConnector.spec.image` is unset.
    #[arg(long, env = "DEFAULT_CONNECTOR_IMAGE")]
    pub default_connector_image: Option<String>,
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
    #[arg(
        long,
        env = "PGDOG_RELOAD_BACKOFF",
        default_value = "100ms",
        value_parser = parse::positive_time
    )]
    pub pgdog_reload_backoff: Time,
    /// Requeue delay after `PgDog` remains stale.
    #[arg(
        long,
        env = "PGDOG_RELOAD_REQUEUE",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    pub pgdog_reload_requeue: Time,
    /// Timeout for one `PgDog` admin reload operation.
    #[arg(
        long,
        env = "PGDOG_ADMIN_TIMEOUT",
        default_value = "20s",
        value_parser = parse::positive_time
    )]
    pub pgdog_admin_timeout: Time,
    /// Fallback poll interval when no earlier `PgDog` transition is pending.
    #[arg(
        long,
        env = "PGDOG_TRANSITION_POLL",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub pgdog_transition_poll: Time,
    /// Requeue delay after a controller reconcile error.
    #[arg(
        long,
        env = "CONTROLLER_ERROR_REQUEUE",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    pub controller_error_requeue: Time,
    /// Requeue delay while a referenced Kubernetes resource is not ready.
    #[arg(
        long,
        env = "CONTROLLER_DEPENDENCY_REQUEUE",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    pub controller_dependency_requeue: Time,
    /// Periodic cadence for detecting external state drift.
    #[arg(
        long,
        env = "CONTROLLER_DRIFT_REQUEUE",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub controller_drift_requeue: Time,
    /// Requeue delay for invalid resources that require user correction.
    #[arg(
        long,
        env = "CONTROLLER_INVALID_REQUEUE",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub controller_invalid_requeue: Time,
    /// Requeue delay while a certificate dependency is being provisioned.
    #[arg(
        long,
        env = "CONTROLLER_CERTIFICATE_REQUEUE",
        default_value = "10s",
        value_parser = parse::positive_time
    )]
    pub controller_certificate_requeue: Time,
    /// Drift-check cadence for operator-managed TLS users.
    #[arg(
        long,
        env = "USER_TLS_DRIFT_REQUEUE",
        default_value = "6h",
        value_parser = parse::positive_time
    )]
    pub user_tls_drift_requeue: Time,
    /// Duration for which an unrenewed leader-election lease remains valid.
    #[arg(
        long,
        env = "LEADER_LEASE_DURATION",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    pub leader_lease_duration: Time,
    /// Poll cadence while another operator replica holds the lease.
    #[arg(
        long,
        env = "LEADER_RETRY_INTERVAL",
        default_value = "2s",
        value_parser = parse::positive_time
    )]
    pub leader_retry_interval: Time,
    /// Timeout for Kafka topic create and delete operations.
    #[arg(
        long,
        env = "TOPIC_MUTATION_TIMEOUT",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    pub topic_mutation_timeout: Time,
    /// Timeout for one operator-to-rebalancer request.
    #[arg(
        long,
        env = "REBALANCER_REQUEST_TIMEOUT",
        default_value = "30s",
        value_parser = parse::positive_time
    )]
    pub rebalancer_request_timeout: Time,
    /// Requeue cadence while a rebalance is executing.
    #[arg(
        long,
        env = "REBALANCER_POLL_INTERVAL",
        default_value = "10s",
        value_parser = parse::positive_time
    )]
    pub rebalancer_poll_interval: Time,
    /// Requeue cadence while no rebalance action is active.
    #[arg(
        long,
        env = "REBALANCER_IDLE_INTERVAL",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub rebalancer_idle_interval: Time,
    /// Requeue delay after an invalid delegation-token broker request.
    #[arg(
        long,
        env = "DELEGATION_TOKEN_INVALID_REQUEUE",
        default_value = "1h",
        value_parser = parse::positive_time
    )]
    pub delegation_token_invalid_requeue: Time,
    /// Backoff after a transient delegation-token broker failure.
    #[arg(
        long,
        env = "DELEGATION_TOKEN_TRANSIENT_BACKOFF",
        default_value = "5m",
        value_parser = parse::positive_time
    )]
    pub delegation_token_transient_backoff: Time,
    /// Shortest delegation-token renewal requeue.
    #[arg(
        long,
        env = "DELEGATION_TOKEN_MIN_REQUEUE",
        default_value = "1m",
        value_parser = parse::positive_time
    )]
    pub delegation_token_min_requeue: Time,
    /// Longest delegation-token renewal requeue.
    #[arg(
        long,
        env = "DELEGATION_TOKEN_MAX_REQUEUE",
        default_value = "24h",
        value_parser = parse::positive_time
    )]
    pub delegation_token_max_requeue: Time,

    /// Durable checkpoint object store. The operator uses it to verify a
    /// suspended tenant before it deletes the tenant's WAL topics. Parking is
    /// disabled when this is unset.
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
    /// Optional explicit S3 access key id. If it is unset, the operator uses
    /// the provider chain.
    #[arg(long, env = "GRES_CHECKPOINT_ACCESS_KEY_ID")]
    pub gres_checkpoint_access_key_id: Option<String>,
    /// Optional explicit S3 secret access key. If it is unset, the operator
    /// uses the provider chain.
    #[arg(long, env = "GRES_CHECKPOINT_SECRET_ACCESS_KEY")]
    pub gres_checkpoint_secret_access_key: Option<String>,
    /// Optional GCS service-account JSON path.
    #[arg(long, env = "GRES_CHECKPOINT_GCS_SERVICE_ACCOUNT_PATH")]
    pub gres_checkpoint_gcs_service_account_path: Option<String>,
    /// Optional GCS ADC JSON path.
    #[arg(long, env = "GRES_CHECKPOINT_GCS_APPLICATION_CREDENTIALS_PATH")]
    pub gres_checkpoint_gcs_application_credentials_path: Option<String>,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    crabka_client_core::ConnectionDispatchQueueCapacity::new(value)
        .map(crabka_client_core::ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    crabka_client_core::ClientFrameMax::try_from(value)
        .map(crabka_client_core::ClientFrameMax::size)
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

    /// Validate relationships between independently parsed runtime settings.
    ///
    /// # Errors
    ///
    /// Returns the invalid relationship before any external I/O begins.
    pub fn validate(&self) -> Result<(), String> {
        crabka_client_core::ConnectionDispatchQueueCapacity::new(
            self.client_dispatch_queue_capacity,
        )?;
        crabka_client_core::ClientFrameMax::try_from(self.client_frame_max)?;
        let lease_seconds_f64 = self.leader_lease_duration.secs_f64();
        if !lease_seconds_f64.is_finite() || lease_seconds_f64.fract() != 0.0 {
            return Err("leader lease duration must be a whole number of seconds".to_owned());
        }
        let lease_seconds = self.leader_lease_duration.secs_i64();
        if i32::try_from(lease_seconds).is_err() {
            return Err("leader lease duration exceeds Kubernetes i32 seconds".to_owned());
        }
        if self.leader_retry_interval > self.leader_lease_duration {
            return Err("leader retry interval exceeds lease duration".to_owned());
        }
        if self.rebalancer_poll_interval > self.rebalancer_idle_interval {
            return Err("rebalancer poll interval exceeds idle interval".to_owned());
        }
        if self.delegation_token_min_requeue > self.delegation_token_max_requeue {
            return Err("delegation-token minimum requeue exceeds maximum requeue".to_owned());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use assert2::assert;
    use clap::{CommandFactory as _, Parser};
    use crabka_units::{hours, millis, minutes, secs};

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
        assert!(parsed.cfg.client_dispatch_queue_capacity == 64);
        assert!(parsed.cfg.client_frame_max == crabka_units::mebibytes(100));
        assert!(parsed.cfg.pgdog_reload_attempts.into_value() == 3);
        assert!(parsed.cfg.pgdog_reload_backoff == millis(100));
        assert!(parsed.cfg.pgdog_reload_requeue == secs(15));
        assert!(parsed.cfg.pgdog_admin_timeout == secs(20));
        assert!(parsed.cfg.pgdog_transition_poll == secs(60));
        assert!(parsed.cfg.controller_error_requeue == secs(15));
    }

    #[test]
    fn client_resource_policy_accepts_uom_overrides_and_rejects_invalid_values() {
        let configured = Wrap::parse_from([
            "bin",
            "--client-dispatch-queue-capacity=7",
            "--client-frame-max=32KiB",
        ])
        .cfg;
        assert!(configured.client_dispatch_queue_capacity == 7);
        assert!(configured.client_frame_max == crabka_units::kibibytes(32));

        for option in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
            "--client-frame-max=1.5B",
        ] {
            assert!(Wrap::try_parse_from(["bin", option]).is_err());
        }
    }

    #[test]
    fn runtime_timing_policy_uses_uom_defaults_cli_env_and_ordering() {
        let defaults = Wrap::parse_from(["bin"]).cfg;
        assert!(defaults.controller_dependency_requeue == secs(30));
        assert!(defaults.controller_drift_requeue == minutes(1));
        assert!(defaults.controller_invalid_requeue == minutes(5));
        assert!(defaults.controller_certificate_requeue == secs(10));
        assert!(defaults.user_tls_drift_requeue == hours(6));
        assert!(defaults.leader_lease_duration == secs(15));
        assert!(defaults.leader_retry_interval == secs(2));
        assert!(defaults.topic_mutation_timeout == secs(30));
        assert!(defaults.rebalancer_request_timeout == secs(30));
        assert!(defaults.rebalancer_poll_interval == secs(10));
        assert!(defaults.rebalancer_idle_interval == minutes(5));
        assert!(defaults.delegation_token_invalid_requeue == hours(1));
        assert!(defaults.delegation_token_transient_backoff == minutes(5));
        assert!(defaults.delegation_token_min_requeue == minutes(1));
        assert!(defaults.delegation_token_max_requeue == hours(24));
        defaults.validate().expect("default timing policy");

        let command = Wrap::command();
        for (id, environment) in [
            (
                "controller_dependency_requeue",
                "CONTROLLER_DEPENDENCY_REQUEUE",
            ),
            ("controller_drift_requeue", "CONTROLLER_DRIFT_REQUEUE"),
            ("controller_invalid_requeue", "CONTROLLER_INVALID_REQUEUE"),
            (
                "controller_certificate_requeue",
                "CONTROLLER_CERTIFICATE_REQUEUE",
            ),
            ("user_tls_drift_requeue", "USER_TLS_DRIFT_REQUEUE"),
            ("leader_lease_duration", "LEADER_LEASE_DURATION"),
            ("leader_retry_interval", "LEADER_RETRY_INTERVAL"),
            ("topic_mutation_timeout", "TOPIC_MUTATION_TIMEOUT"),
            ("rebalancer_request_timeout", "REBALANCER_REQUEST_TIMEOUT"),
            ("rebalancer_poll_interval", "REBALANCER_POLL_INTERVAL"),
            ("rebalancer_idle_interval", "REBALANCER_IDLE_INTERVAL"),
            (
                "delegation_token_invalid_requeue",
                "DELEGATION_TOKEN_INVALID_REQUEUE",
            ),
            (
                "delegation_token_transient_backoff",
                "DELEGATION_TOKEN_TRANSIENT_BACKOFF",
            ),
            (
                "delegation_token_min_requeue",
                "DELEGATION_TOKEN_MIN_REQUEUE",
            ),
            (
                "delegation_token_max_requeue",
                "DELEGATION_TOKEN_MAX_REQUEUE",
            ),
        ] {
            let argument = command
                .get_arguments()
                .find(|argument| argument.get_id().as_str() == id)
                .expect("timing argument");
            assert_eq!(argument.get_env(), Some(std::ffi::OsStr::new(environment)));
        }

        let configured = Wrap::parse_from([
            "bin",
            "--controller-dependency-requeue=31ms",
            "--controller-drift-requeue=32ms",
            "--controller-invalid-requeue=33ms",
            "--controller-certificate-requeue=34ms",
            "--user-tls-drift-requeue=35ms",
            "--leader-lease-duration=36ms",
            "--leader-retry-interval=37ms",
            "--topic-mutation-timeout=38ms",
            "--rebalancer-request-timeout=39ms",
            "--rebalancer-poll-interval=40ms",
            "--rebalancer-idle-interval=41ms",
            "--delegation-token-invalid-requeue=42ms",
            "--delegation-token-transient-backoff=43ms",
            "--delegation-token-min-requeue=44ms",
            "--delegation-token-max-requeue=45ms",
        ])
        .cfg;
        assert!(configured.controller_dependency_requeue == millis(31));
        assert!(configured.controller_drift_requeue == millis(32));
        assert!(configured.controller_invalid_requeue == millis(33));
        assert!(configured.controller_certificate_requeue == millis(34));
        assert!(configured.user_tls_drift_requeue == millis(35));
        assert!(configured.leader_lease_duration == millis(36));
        assert!(configured.leader_retry_interval == millis(37));
        assert!(configured.topic_mutation_timeout == millis(38));
        assert!(configured.rebalancer_request_timeout == millis(39));
        assert!(configured.rebalancer_poll_interval == millis(40));
        assert!(configured.rebalancer_idle_interval == millis(41));
        assert!(configured.delegation_token_invalid_requeue == millis(42));
        assert!(configured.delegation_token_transient_backoff == millis(43));
        assert!(configured.delegation_token_min_requeue == millis(44));
        assert!(configured.delegation_token_max_requeue == millis(45));

        let mut inverted = defaults;
        inverted.leader_retry_interval = inverted.leader_lease_duration + millis(1);
        assert!(inverted.validate().unwrap_err().contains("leader retry"));
        inverted = Wrap::parse_from(["bin"]).cfg;
        inverted.rebalancer_poll_interval = inverted.rebalancer_idle_interval + millis(1);
        assert!(inverted.validate().unwrap_err().contains("rebalancer poll"));
        inverted = Wrap::parse_from(["bin"]).cfg;
        inverted.delegation_token_min_requeue = inverted.delegation_token_max_requeue + millis(1);
        assert!(
            inverted
                .validate()
                .unwrap_err()
                .contains("delegation-token minimum")
        );
        inverted = Wrap::parse_from(["bin"]).cfg;
        inverted.leader_lease_duration = millis(1_500);
        let error = inverted.validate().unwrap_err();
        assert!(error.contains("whole number of seconds"), "got: {error}");
        inverted = Wrap::parse_from(["bin"]).cfg;
        inverted.leader_lease_duration = crabka_units::days(365 * 100);
        assert!(inverted.validate().unwrap_err().contains("i32 seconds"));
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
                .env("PGDOG_RELOAD_BACKOFF", "5ms")
                .env("PGDOG_RELOAD_REQUEUE", "6ms")
                .env("PGDOG_ADMIN_TIMEOUT", "7ms")
                .env("PGDOG_TRANSITION_POLL", "8ms")
                .env("CONTROLLER_ERROR_REQUEUE", "9ms")
                .env("OPERATOR_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                .env("OPERATOR_CLIENT_FRAME_MAX", "32KiB")
                .output()
                .expect("spawn isolated environment test");
            assert!(
                output.status.success(),
                "child stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let environment = Wrap::parse_from(["bin"]);
        assert!(environment.cfg.pgdog_reload_attempts.into_value() == 4);
        assert!(environment.cfg.pgdog_reload_backoff == millis(5));
        assert!(environment.cfg.pgdog_reload_requeue == millis(6));
        assert!(environment.cfg.pgdog_admin_timeout == millis(7));
        assert!(environment.cfg.pgdog_transition_poll == millis(8));
        assert!(environment.cfg.controller_error_requeue == millis(9));
        assert!(environment.cfg.client_dispatch_queue_capacity == 7);
        assert!(environment.cfg.client_frame_max == crabka_units::kibibytes(32));

        let parsed = Wrap::parse_from([
            "bin",
            "--pgdog-reload-attempts",
            "10",
            "--pgdog-reload-backoff",
            "11ms",
            "--pgdog-reload-requeue",
            "12ms",
            "--pgdog-admin-timeout",
            "13ms",
            "--pgdog-transition-poll",
            "14ms",
            "--controller-error-requeue",
            "15ms",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ]);
        assert!(parsed.cfg.pgdog_reload_attempts.into_value() == 10);
        assert!(parsed.cfg.pgdog_reload_backoff == millis(11));
        assert!(parsed.cfg.pgdog_reload_requeue == millis(12));
        assert!(parsed.cfg.pgdog_admin_timeout == millis(13));
        assert!(parsed.cfg.pgdog_transition_poll == millis(14));
        assert!(parsed.cfg.controller_error_requeue == millis(15));
        assert!(parsed.cfg.client_dispatch_queue_capacity == 9);
        assert!(parsed.cfg.client_frame_max == crabka_units::kibibytes(64));
    }

    #[test]
    fn controller_timing_values_reject_zero_and_overflow() {
        assert!(Wrap::try_parse_from(["bin", "--pgdog-reload-attempts", "0"]).is_err());
        assert!(
            Wrap::try_parse_from(["bin", "--pgdog-reload-attempts", "18446744073709551616",])
                .is_err()
        );
        for option in [
            "--pgdog-reload-backoff",
            "--pgdog-reload-requeue",
            "--pgdog-admin-timeout",
            "--pgdog-transition-poll",
            "--controller-error-requeue",
        ] {
            assert!(Wrap::try_parse_from(["bin", option, "0ms"]).is_err());
            assert!(Wrap::try_parse_from(["bin", option, "1"]).is_err());
        }
    }

    #[test]
    fn positive_config_value_serde_rejects_zero() {
        let value: PositiveU64 = "123".parse().expect("positive value");
        assert!(serde_json::to_value(value).unwrap() == 123);
        assert!(serde_json::from_str::<PositiveU64>("0").is_err());
    }
}
