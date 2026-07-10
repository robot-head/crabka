use std::net::SocketAddr;

use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

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
    }

    #[test]
    fn comma_separated_namespaces_parse() {
        let parsed = Wrap::parse_from(["bin", "--watch-namespaces=a,b,c"]);
        assert!(parsed.cfg.watch_namespaces == vec!["a", "b", "c"]);
        assert!(parsed.cfg.watched().is_some());
    }
}
