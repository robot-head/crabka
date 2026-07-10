use std::net::SocketAddr;

use clap::Args;
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
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        cfg: OperatorConfig,
    }

    #[test]
    fn cli_namespace_cases() {
        for (name, args, expected) in [
            (
                "defaults compute cluster scope",
                vec!["bin"],
                (Vec::<String>::new(), None, "crabka-operator"),
            ),
            (
                "comma-separated namespaces",
                vec!["bin", "--watch-namespaces=a,b,c"],
                (
                    vec!["a".to_string(), "b".to_string(), "c".to_string()],
                    Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                    "crabka-operator",
                ),
            ),
        ] {
            let parsed = Wrap::parse_from(args);
            assert_eq!(
                (
                    parsed.cfg.watch_namespaces.clone(),
                    parsed.cfg.watched().map(<[String]>::to_vec),
                    parsed.cfg.operator_namespace.as_str(),
                ),
                expected,
                "case {name}"
            );
        }
    }
}
