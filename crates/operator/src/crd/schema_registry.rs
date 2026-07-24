//! `SchemaRegistry` CRD. Deploys the standalone `crabka-schema-registry`
//! service (a Kafka client of the broker; state lives in `_schemas`).
//! Stateless — N replicas join the `"sr"` election group, one is elected
//! primary, the rest forward writes. Associated with a managed `Kafka` via
//! the `crabka.io/cluster` label (mirrors `KafkaTopic`).

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
    /// Stateless replicas; all join the election group. Default 1.
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Container image. Defaults to the operator's
    /// `--default-schema-registry-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Override bootstrap for an external/unmanaged Kafka. When unset,
    /// bootstrap is derived from the `crabka.io/cluster`-labeled Kafka's
    /// internal listener. (Secured external brokers are a future
    /// enhancement; the managed/label path is the secured one.)
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

    /// SR → broker client security (SASL / TLS). Maps to `--kafka-*` flags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_client: Option<SchemaRegistryKafkaClient>,

    /// Server TLS (HTTPS REST). None = plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<SchemaRegistryTls>,

    /// REST authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SchemaRegistryAuthn>,

    /// REST authorization (Kafka-ACL based).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<SchemaRegistryAuthz>,

    /// Pod resource requirements.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
}

/// Schema Registry broker interaction and store-default policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistryRuntime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub election_session_timeout_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub election_rebalance_timeout_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub election_heartbeat_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub election_reconnect_backoff_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub store_reader_retry_backoff_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub store_reader_fetch_max_wait_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub store_reader_fetch_max_bytes: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub schemas_topic_create_timeout_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub forward_max_body_bytes: Option<i32>,
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
    /// Defaults to `Issuer`; set `ClusterIssuer` for cluster-scoped issuers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// API group. Default `cert-manager.io`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// Server TLS: cert/key from a Secret or a cert-manager issuer, plus optional
/// client-cert verification.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Secret (type kubernetes.io/tls) with `tls.crt` + `tls.key`.
    /// Mutually exclusive with `issuerRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// cert-manager issuer reference. Mutually exclusive with `secretName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_ref: Option<CertManagerIssuerRef>,
    /// Client-cert mode. Default `Disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth: Option<TlsClientAuth>,
    /// Secret with `ca.crt` to verify client certs (required when
    /// `clientAuth` != Disabled).
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
    /// Secret with a single key holding newline-separated `user:cred`
    /// entries (cred = plaintext or `$2…` bcrypt). The key is mounted as
    /// a file and passed via `--basic-auth-file`.
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
    /// JWKS endpoint URI (required when `mode` = `Jwks`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,
    /// Expected `iss` claim value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_valid_issuer: Option<String>,
    /// Expected `aud` claim value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expected_audience: Option<String>,
    /// Secret name whose `ca.crt` key is mounted and passed as
    /// `--bearer-jwks-ca`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_tls_secret_name: Option<String>,
    /// JWT claim to use as the principal when mode is `Jwks`. Overrides
    /// `principalClaim` for JWKS paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_principal_claim: Option<String>,
    /// JWKS key-set refresh interval in milliseconds. Default 60 000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_refresh_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    /// Dev-only: accept unsigned JWTs (no signature verification).
    Unsecured,
    /// Production: verify JWT signatures against a remote JWKS endpoint.
    Jwks,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthz {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval (seconds). Default 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl_refresh_seconds: Option<i64>,
}

/// SR → broker client security. Maps to the binary's `--kafka-*` flags.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryKafkaClient {
    /// e.g. `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_protocol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl: Option<KafkaClientSasl>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<KafkaClientTls>,
}

/// SASL credentials for the SR → broker connection.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientSasl {
    /// e.g. `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
    pub mechanism: String,
    /// Name of the Secret holding `username` and `password` keys.
    pub secret_ref: String,
}

/// TLS settings for the SR → broker connection.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClientTls {
    /// Secret with a `ca.crt` key used as the broker CA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_secret_name: Option<String>,
    /// Override the server name used for TLS SNI / hostname verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name_override: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryStatus {
    /// Kubernetes-style conditions: `KafkaReady`, `Available`, `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,
    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// In-cluster REST URL clients use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}
