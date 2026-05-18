//! Slice 36: `KafkaUser` CRD. Strimzi-shaped — SCRAM-SHA-512
//! authentication + simple ACL authorization in this slice. mTLS auth
//! and quotas land in slices 37 and 38.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaUser",
    plural = "kafkausers",
    singular = "kafkauser",
    shortname = "ku",
    namespaced,
    status = "KafkaUserStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserSpec {
    pub authentication: Authentication,

    /// Authorization is optional — a user with no ACLs can still
    /// authenticate. When absent, ACL reconciliation is skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
}

/// Tagged enum on `type`, mirroring Strimzi.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Authentication {
    #[serde(rename = "scram-sha-512")]
    ScramSha512(ScramSha512Auth),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScramSha512Auth {
    /// PBKDF2 iteration count. Defaults to 8192 on the controller side
    /// (matches `crabka_client_admin::DEFAULT_SCRAM_ITERATIONS`); the
    /// broker rejects values < 4096.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 4096, max = 1_000_000))]
    pub iterations: Option<i32>,

    /// Raw-password length (bytes) for the operator-generated secret.
    /// Defaults to 32 bytes (44 base64 chars). Ignored on reconcile if
    /// a Secret with key `password` already exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 16, max = 256))]
    pub password_length: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Authorization {
    /// Mirrors Strimzi's `KafkaUserAuthorizationSimple` — drives the
    /// `SimpleAclAuthorizer` (which Crabka implements as the only
    /// authorizer today).
    Simple(SimpleAuthorization),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    #[serde(default)]
    pub acls: Vec<AclRule>,
}

/// One rule from `spec.authorization.acls`. Expanded into one
/// `(resource, operation, host, type)` tuple per `operations` entry
/// at reconcile time.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AclRule {
    pub resource: AclResource,
    /// Non-empty list — the reconciler refuses an empty `operations`
    /// with `Ready=False reason=InvalidSpec`.
    pub operations: Vec<AclOp>,
    /// Source-address pattern; `"*"` is the wildcard. Defaults to
    /// `"*"` when omitted.
    #[serde(default = "default_host", skip_serializing_if = "is_default_host")]
    pub host: String,
    /// `allow` or `deny`; defaults to `allow`.
    #[serde(default, rename = "type")]
    pub permission: AclPermission,
}

fn default_host() -> String {
    "*".into()
}
fn is_default_host(h: &String) -> bool {
    h == "*"
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AclResource {
    #[serde(rename = "type")]
    pub kind: AclResourceKind,
    pub name: String,
    /// `literal` (exact match) or `prefixed` (resources whose name
    /// starts with `name`). Defaults to `literal`.
    #[serde(default)]
    pub pattern_type: AclPatternType,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AclResourceKind {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AclPatternType {
    #[default]
    Literal,
    Prefixed,
}

/// All 11 Kafka ACL operations. Matches the discriminants in
/// `org.apache.kafka.common.acl.AclOperation`.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub enum AclOp {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AclPermission {
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserStatus {
    /// Standard Kubernetes-style condition list. Surfaces `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective Kafka principal name (matches `metadata.name`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Name of the Kubernetes Secret holding the user's password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,

    /// True once SCRAM-SHA-512 credentials are provisioned.
    #[serde(default)]
    pub scram_sha512: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaUser::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "KafkaUser");
        assert_eq!(crd.spec.names.plural, "kafkausers");
        assert!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"ku".to_string())),
            "expected shortname `ku`",
        );
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn full_spec_round_trips_through_json() {
        let ku = KafkaUser::new(
            "alice",
            KafkaUserSpec {
                authentication: Authentication::ScramSha512(ScramSha512Auth {
                    iterations: Some(16384),
                    password_length: Some(48),
                }),
                authorization: Some(Authorization::Simple(SimpleAuthorization {
                    acls: vec![AclRule {
                        resource: AclResource {
                            kind: AclResourceKind::Topic,
                            name: "orders".into(),
                            pattern_type: AclPatternType::Literal,
                        },
                        operations: vec![AclOp::Read, AclOp::Describe],
                        host: "*".into(),
                        permission: AclPermission::Allow,
                    }],
                })),
            },
        );
        let json = serde_json::to_string(&ku).unwrap();
        assert!(json.contains("\"type\":\"scram-sha-512\""), "got: {json}");
        assert!(json.contains("\"iterations\":16384"), "got: {json}");
        assert!(json.contains("\"type\":\"simple\""), "got: {json}");
        assert!(json.contains("\"name\":\"orders\""), "got: {json}");
        let back: KafkaUser = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, ku.spec);
    }

    #[test]
    fn minimum_spec_parses() {
        let json = r#"{"authentication":{"type":"scram-sha-512"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert!(matches!(
            spec.authentication,
            Authentication::ScramSha512(ScramSha512Auth {
                iterations: None,
                password_length: None,
            })
        ));
        assert!(spec.authorization.is_none());
    }

    #[test]
    fn acl_rule_defaults_host_and_permission() {
        let json = r#"{
            "resource": {"type":"topic","name":"orders"},
            "operations":["Read"]
        }"#;
        let rule: AclRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.host, "*");
        assert_eq!(rule.permission, AclPermission::Allow);
        assert_eq!(rule.resource.pattern_type, AclPatternType::Literal);
    }

    #[test]
    fn acl_rule_omits_default_host_on_serialize() {
        let rule = AclRule {
            resource: AclResource {
                kind: AclResourceKind::Topic,
                name: "orders".into(),
                pattern_type: AclPatternType::Literal,
            },
            operations: vec![AclOp::Read],
            host: "*".into(),
            permission: AclPermission::Allow,
        };
        let j = serde_json::to_string(&rule).unwrap();
        assert!(!j.contains("host"), "default host should be omitted: {j}");
    }

    #[test]
    fn acl_rule_emits_non_default_host() {
        let rule = AclRule {
            resource: AclResource {
                kind: AclResourceKind::Topic,
                name: "orders".into(),
                pattern_type: AclPatternType::Literal,
            },
            operations: vec![AclOp::Read],
            host: "10.0.0.0".into(),
            permission: AclPermission::Allow,
        };
        let j = serde_json::to_string(&rule).unwrap();
        assert!(j.contains("\"host\":\"10.0.0.0\""), "got: {j}");
    }

    #[test]
    fn status_omits_optional_fields_when_unset() {
        let status = KafkaUserStatus::default();
        let j = serde_json::to_string(&status).unwrap();
        assert!(!j.contains("observedGeneration"), "got: {j}");
        assert!(!j.contains("username"), "got: {j}");
        assert!(!j.contains("secret"), "got: {j}");
        // `scramSha512` is a plain bool — serde emits it.
        assert!(j.contains("\"scramSha512\":false"), "got: {j}");
    }
}
