//! `SchemaRegistry` CRD.
//!
//! This CRD deploys the standalone `crabka-schema-registry` service. The
//! service is a Kafka client of the broker, and its state lives in
//! `_schemas`. The service is stateless. N replicas join the `"sr"`
//! election group. The group elects one primary, and the other replicas
//! forward the writes to it. The `crabka.io/cluster` label links the CR to
//! a managed `Kafka`, as it does for `KafkaTopic`.

use crabka_units::{ByteSize, Time};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "SchemaRegistry",
    plural = "schemaregistries",
    singular = "schemaregistry",
    shortname = "sr",
    namespaced,
    status = "SchemaRegistryStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistrySpec {
    /// Number of stateless replicas. All of them join the election
    /// group. Default 1.
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Container image. Defaults to the operator's
    /// `--default-schema-registry-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Bootstrap override for an external Kafka that the operator does
    /// not manage. When unset, the operator derives the bootstrap from the
    /// internal listener of the Kafka with the `crabka.io/cluster` label.
    /// Secured external brokers are future work. The managed path with the
    /// label is the secured one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_servers: Option<String>,

    /// Backing compacted topic. Default `_schemas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schemas_topic: Option<String>,

    /// Replication factor for `_schemas` when auto-created. Default 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schemas_topic_replication_factor: Option<i32>,

    /// Election group id. Default `schema-registry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    /// Schema Registry runtime policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<SchemaRegistryRuntime>,

    /// Kafka client id used by the registry. Default `crabka-schema-registry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub client_id: Option<String>,

    /// Kubernetes probe timing overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_checks: Option<SchemaRegistryHealthChecks>,

    /// Client security from the SR to the broker, with SASL and TLS. It
    /// maps to the `--kafka-*` flags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_client: Option<SchemaRegistryKafkaClient>,

    /// Server TLS for the HTTPS REST surface. `None` means plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<SchemaRegistryTls>,

    /// REST authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SchemaRegistryAuthn>,

    /// REST authorization, based on the Kafka ACLs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<SchemaRegistryAuthz>,

    /// Pod resource requirements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
}

/// Schema Registry broker interaction and store-default policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistryRuntime {
    /// Kafka client request-dispatch queue capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub client_dispatch_queue_capacity: Option<usize>,
    /// Maximum accepted Kafka client frame size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub client_frame_max: Option<ByteSize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub election_session_timeout: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub election_rebalance_timeout: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub election_heartbeat_interval: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub election_reconnect_backoff: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub store_reader_retry_backoff: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub store_reader_fetch_max_wait: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub store_reader_fetch_max: Option<ByteSize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub schemas_topic_create_timeout: Option<Time>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub forward_max_body: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_compatibility_level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<String>,
}

/// Kubernetes readiness and liveness probe timing.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistryHealthChecks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub readiness_initial_delay_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_period_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub liveness_initial_delay_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub liveness_period_seconds: Option<i32>,
}

/// Reference to a cert-manager `Issuer` or `ClusterIssuer`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertManagerIssuerRef {
    pub name: String,
    /// The default is `Issuer`. Set `ClusterIssuer` for an issuer with
    /// cluster scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// API group. Default `cert-manager.io`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// Server TLS.
///
/// The cert and the key come from a Secret or from a cert-manager issuer.
/// Client-cert verification is optional.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Secret of type kubernetes.io/tls with `tls.crt` and `tls.key`. Do
    /// not set it together with `issuerRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// cert-manager issuer reference. Do not set it together with
    /// `secretName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_ref: Option<CertManagerIssuerRef>,
    /// Client-cert mode. Default `Disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth: Option<TlsClientAuth>,
    /// Secret with `ca.crt` that verifies the client certs. It is
    /// necessary when `clientAuth` is not Disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca_secret_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TlsClientAuth {
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthn {
    /// Reject anonymous requests with 401.
    #[serde(default)]
    pub require_auth: bool,
    /// `WWW-Authenticate: basic realm="<realm>"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic: Option<BasicAuthn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer: Option<BearerAuthn>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasicAuthn {
    /// Secret with one key that holds `user:cred` entries, one on each
    /// line. The cred is plaintext or a `$2…` bcrypt hash. The operator
    /// mounts the key as a file and gives it in `--basic-auth-file`.
    pub users_secret_name: String,
    /// Secret key holding the htpasswd-style file. Default `users`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub users_secret_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BearerAuthn {
    pub mode: BearerMode,
    /// JWT claim used as the principal name. Default `sub`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,
    /// JWKS endpoint URI. It is necessary when `mode` is `Jwks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,
    /// Expected `iss` claim value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_valid_issuer: Option<String>,
    /// Expected `aud` claim value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expected_audience: Option<String>,
    /// Name of the Secret whose `ca.crt` key the operator mounts and
    /// gives in `--bearer-jwks-ca`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_tls_secret_name: Option<String>,
    /// JWT claim to use as the principal when the mode is `Jwks`. It
    /// overrides `principalClaim` on the JWKS paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_principal_claim: Option<String>,
    /// JWKS key-set refresh interval. Default `1m`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub jwks_refresh: Option<Time>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    /// For development only. Accept unsigned JWTs and verify no
    /// signature.
    Unsecured,
    /// For production. Verify the JWT signatures against a remote JWKS
    /// endpoint.
    Jwks,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthz {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval. Default `30s`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub acl_refresh: Option<Time>,
}

/// Client security from the SR to the broker.
///
/// It maps to the `--kafka-*` flags of the binary.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryKafkaClient {
    /// For example `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, or `SASL_SSL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_protocol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl: Option<KafkaClientSasl>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<KafkaClientTls>,
}

/// SASL credentials for the connection from the SR to the broker.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientSasl {
    /// For example `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`.
    pub mechanism: String,
    /// Name of the Secret that holds the `username` and `password` keys.
    pub secret_ref: String,
}

/// TLS settings for the connection from the SR to the broker.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientTls {
    /// Secret with a `ca.crt` key that gives the broker CA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_secret_name: Option<String>,
    /// Override the server name for TLS SNI and hostname verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name_override: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryStatus {
    /// Kubernetes-style conditions: `KafkaReady`, `Available`, and
    /// `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,
    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// In-cluster REST URL that the clients use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn client_policy_round_trips_and_has_schema() {
        let runtime = SchemaRegistryRuntime {
            client_dispatch_queue_capacity: Some(7),
            client_frame_max: Some(crabka_units::kibibytes(32)),
            ..SchemaRegistryRuntime::default()
        };
        let json = serde_json::to_value(&runtime).unwrap();
        check!(json["clientDispatchQueueCapacity"] == 7);
        check!(json["clientFrameMax"] == "32KiB");
        assert!(serde_json::from_value::<SchemaRegistryRuntime>(json).unwrap() == runtime);

        let crd = serde_json::to_value(SchemaRegistry::crd()).unwrap();
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["runtime"]["properties"];
        check!(properties["clientDispatchQueueCapacity"]["minimum"].as_f64() == Some(1.0));
        check!(properties["clientFrameMax"]["type"] == "string");
    }
}
