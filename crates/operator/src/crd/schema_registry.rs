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

/// Server TLS: cert/key (and optional client-cert verification) from Secrets.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTls {
    /// Secret (type kubernetes.io/tls) with `tls.crt` + `tls.key`.
    pub secret_name: String,
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
    /// Bearer mode. Only `Unsecured` (dev) is supported today; JWKS is a
    /// future SR enhancement.
    pub mode: BearerMode,
    /// JWT claim used as the principal name. Default `sub`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BearerMode {
    Unsecured,
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
