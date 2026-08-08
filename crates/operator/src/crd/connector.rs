//! `KafkaConnector` CRD for one operator-managed `PostgreSQL` `CDC` worker.

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Supported connector implementation.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectorType {
    /// `PostgreSQL` logical decoding source that writes `CDC` records to `Kafka`.
    PostgresSource,
}

/// Reference to one key in a `Secret` in the connector's namespace.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorSecretKeyRef {
    /// Secret name.
    pub name: String,
    /// Key containing the `PostgreSQL` connection URL.
    pub key: String,
}

/// Worker batching and checkpoint cadence.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorRuntime {
    /// Maximum records read in one source poll. Worker default: 500.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_batch: Option<usize>,
    /// Maximum interval between durable checkpoint commits, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub commit_interval_ms: Option<u64>,
    /// Delay after an empty source poll, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub poll_backoff_ms: Option<u64>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaConnector",
    plural = "kafkaconnectors",
    singular = "kafkaconnector",
    shortname = "kc",
    namespaced,
    status = "KafkaConnectorStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KafkaConnectorSpec {
    /// Connector implementation. The initial release supports `postgresSource`.
    #[serde(rename = "type")]
    pub type_: ConnectorType,
    /// Gracefully stop the worker while retaining its durable checkpoint.
    #[serde(default)]
    pub paused: bool,
    /// Container image override. When absent, the operator default is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Worker `CPU` and memory requests/limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// `Secret` key containing the `PostgreSQL` connection URL.
    pub database_url: ConnectorSecretKeyRef,
    /// `PostgreSQL` logical replication slot.
    #[schemars(length(min = 1))]
    pub slot: String,
    /// `PostgreSQL` publication read by the connector.
    #[schemars(length(min = 1))]
    pub publication: String,
    /// Default `PostgreSQL` schema for unqualified table names. Default `public`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Unqualified tables included in CDC. All belong to `schema`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(min = 1))]
    pub tables: Vec<String>,
    /// Prefix prepended to destination topic names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_prefix: Option<String>,
    /// Optional worker runtime overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<ConnectorRuntime>,
}

/// Observed state for an operator-managed connector.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorStatus {
    /// Standard `Ready`, `Paused`, and `Failed` conditions.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,
    /// Generation represented by the rendered Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Desired worker replicas (zero while paused, otherwise one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Ready worker replicas reported by the Deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_exposes_namespaced_postgres_source_contract() {
        let crd = serde_json::to_value(KafkaConnector::crd()).unwrap();
        check!(crd["spec"]["scope"] == "Namespaced");
        check!(crd["spec"]["names"]["shortNames"][0] == "kc");
        let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let required = spec["required"].as_array().unwrap();
        for field in ["type", "databaseUrl", "slot", "publication"] {
            assert!(
                required.iter().any(|value| value == field),
                "missing {field}"
            );
        }
        check!(
            spec["properties"]["runtime"]["properties"]["maxBatch"]["minimum"].as_f64()
                == Some(1.0)
        );
        check!(spec["properties"]["tables"]["minItems"].as_u64() == Some(1));
    }

    #[test]
    fn spec_serializes_with_worker_facing_names() {
        let spec = KafkaConnectorSpec {
            type_: ConnectorType::PostgresSource,
            paused: true,
            image: None,
            resources: None,
            database_url: ConnectorSecretKeyRef {
                name: "database".into(),
                key: "url".into(),
            },
            slot: "orders_crabka".into(),
            publication: "crabka_connect".into(),
            schema: Some("public".into()),
            tables: vec!["orders".into()],
            topic_prefix: Some("db".into()),
            runtime: Some(ConnectorRuntime {
                max_batch: Some(100),
                commit_interval_ms: Some(1_000),
                poll_backoff_ms: Some(100),
            }),
        };
        let value = serde_json::to_value(spec).unwrap();
        check!(value["type"] == "postgresSource");
        check!(value["databaseUrl"]["name"] == "database");
        check!(value["runtime"]["commitIntervalMs"] == 1_000);
    }
}
