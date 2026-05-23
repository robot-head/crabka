//! `KafkaUser` CRD. Strimzi-shaped — SCRAM-SHA-512 + mTLS
//! authentication (slices 36, 37), simple ACL authorization, and
//! optional per-user quotas (slice 38).

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

    /// Slice 38: optional per-user client quotas (KIP-13/124/257).
    /// Maps onto Kafka's `(user)` quota entity via `AlterClientQuotas`
    /// (`api_key` 49). Absent → operator does not touch the broker's
    /// quota state; present → the operator drives the broker's quota
    /// keys for `User:<name>` toward the spec (sets configured fields,
    /// tombstones omitted ones).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quotas: Option<KafkaUserQuotas>,
}

/// Strimzi-shaped `KafkaUserQuotas`. Field names + JSON types match
/// `kafka.strimzi.io/v1beta2`; values flow to the broker as `f64`
/// (Kafka's wire type for client-quota values).
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserQuotas {
    /// Maximum produce-side bytes/sec. Backed by `producer_byte_rate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub producer_byte_rate: Option<i32>,

    /// Maximum consume-side bytes/sec. Backed by `consumer_byte_rate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub consumer_byte_rate: Option<i32>,

    /// Maximum percentage of a request-handler thread's time the user
    /// may consume (0..=100). Backed by `request_percentage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 100))]
    pub request_percentage: Option<i32>,

    /// KIP-599 controller-mutation rate (creates/deletes/sec).
    /// Backed by `controller_mutation_rate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_mutation_rate: Option<f64>,
}

/// Tagged enum on `type`, mirroring Strimzi.
///
/// The wire shape is flat (Strimzi-compatible): `type` is the
/// discriminator and per-variant config fields are siblings of `type`.
/// The custom `schema_with` hand-rolls a structural schema because
/// kube-rs 3.x's `StructuralSchemaRewriter` panics when `oneOf`
/// branches share a `type` property with differing `enum` values (the
/// default schemars output for tagged-union enums). Same workaround as
/// `Storage` in `kafka_node_pool.rs`. Cross-variant constraints (e.g.
/// "`iterations` only valid when `type=scram-sha-512`") are enforced
/// by the operator at reconcile time, not by the apiserver.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[schemars(schema_with = "authentication_schema")]
pub enum Authentication {
    #[serde(rename = "scram-sha-512")]
    ScramSha512(ScramSha512Auth),
    #[serde(rename = "tls")]
    Tls(TlsAuth),
    /// Slice 50: credential-less user. The operator provisions ACLs +
    /// quotas under `User:<metadata.name>` but does not create a Secret
    /// or issue a cert — credentials are managed out-of-band (e.g. an
    /// OIDC provider for SASL/OAUTHBEARER, or a CA outside Crabka for
    /// mTLS). Mirrors Strimzi's `tls-external`.
    #[serde(rename = "tls-external")]
    TlsExternal,
}

fn authentication_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["scram-sha-512", "tls", "tls-external"],
            },
            "iterations": { "type": "integer", "minimum": 4096, "maximum": 1_000_000 },
            "passwordLength": { "type": "integer", "minimum": 16, "maximum": 256 },
            "validityDays": { "type": "integer", "minimum": 1, "maximum": 36500 },
            "renewalDays": { "type": "integer", "minimum": 1, "maximum": 3650 },
        },
    })
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

/// mTLS authentication config. The operator generates an X.509 client
/// cert signed by the per-cluster clients CA, stored in the
/// per-user Secret under keys `user.crt`, `user.key`, `ca.crt`. The
/// cert's Subject is the bare RDN `CN=<KafkaUser name>`; that DN is
/// the ACL / quota principal.
///
/// **Validity & renewal.** The cert lives for `validity_days` (default
/// 365). The reconciler reissues iff `not_after - now <= renewal_days`
/// (default 30). Reissue replaces `user.crt` and `user.key`; consumers
/// must reload their TLS client config to pick the new material up.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TlsAuth {
    /// Cert lifetime in days. Default 365.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 36500))]
    pub validity_days: Option<u32>,

    /// Days before `notAfter` at which the operator reissues. Default 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 3650))]
    pub renewal_days: Option<u32>,
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

// The bools are independent wire-level status axes (each is a distinct
// reconcile outcome that surfaces in `kubectl describe ku`); refactoring
// to an enum would hurt the printed-status ergonomics.
#[allow(clippy::struct_excessive_bools)]
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

    /// True once the spec's `quotas` (if any) have been reflected in the
    /// broker's `(user)` client-quota state. False when `spec.quotas`
    /// is `None` (the operator does not touch broker quotas).
    #[serde(default)]
    pub quotas_in_sync: bool,

    /// `true` once a TLS user has a current cert Secret. Mirrors
    /// `scram_sha512`.
    #[serde(default)]
    pub tls: bool,

    /// Slice 50: `true` once a credential-less user
    /// (`type: tls-external`) has been reconciled. Surfaces in
    /// `kubectl describe ku` so operators can tell at a glance that
    /// the operator does not own this user's credentials.
    #[serde(default)]
    pub external: bool,

    /// RFC3339 timestamp of the user cert's `notAfter`. Present when
    /// `tls == true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert_not_after: Option<String>,

    /// The principal string the operator pinned in ACLs (e.g.
    /// `User:CN=alice` for TLS users, `User:alice` for SCRAM). Always
    /// populated when the user is provisioned. Load-bearing for
    /// debugging "why isn't my ACL matching" issues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_principal: Option<String>,
}

impl KafkaUserQuotas {
    /// Project the typed spec onto the wire key→value map the admin
    /// client consumes. `producerByteRate=null` etc. are skipped — the
    /// reconciler's diff then tombstones any broker key not present
    /// here.
    #[must_use]
    pub fn to_quota_map(&self) -> std::collections::BTreeMap<String, f64> {
        let mut out = std::collections::BTreeMap::new();
        if let Some(v) = self.producer_byte_rate {
            out.insert("producer_byte_rate".into(), f64::from(v));
        }
        if let Some(v) = self.consumer_byte_rate {
            out.insert("consumer_byte_rate".into(), f64::from(v));
        }
        if let Some(v) = self.request_percentage {
            out.insert("request_percentage".into(), f64::from(v));
        }
        if let Some(v) = self.controller_mutation_rate {
            out.insert("controller_mutation_rate".into(), v);
        }
        out
    }
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
                quotas: Some(KafkaUserQuotas {
                    producer_byte_rate: Some(1_048_576),
                    consumer_byte_rate: Some(2_097_152),
                    request_percentage: Some(55),
                    controller_mutation_rate: Some(10.0),
                }),
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
        // `scramSha512` + `quotasInSync` are plain bools — serde emits them.
        assert!(j.contains("\"scramSha512\":false"), "got: {j}");
        assert!(j.contains("\"quotasInSync\":false"), "got: {j}");
    }

    #[test]
    fn quotas_empty_serializes_as_empty_object() {
        let q = KafkaUserQuotas::default();
        let j = serde_json::to_string(&q).unwrap();
        assert_eq!(j, "{}");
        assert!(q.to_quota_map().is_empty());
    }

    #[test]
    fn quotas_to_map_only_emits_set_fields() {
        let q = KafkaUserQuotas {
            producer_byte_rate: Some(1_048_576),
            consumer_byte_rate: None,
            request_percentage: Some(25),
            controller_mutation_rate: None,
        };
        let m = q.to_quota_map();
        assert_eq!(m.len(), 2);
        assert!((m["producer_byte_rate"] - 1_048_576.0).abs() < f64::EPSILON);
        assert!((m["request_percentage"] - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn quotas_to_map_carries_controller_mutation_rate_as_double() {
        let q = KafkaUserQuotas {
            controller_mutation_rate: Some(2.5),
            ..Default::default()
        };
        let m = q.to_quota_map();
        assert_eq!(m.len(), 1);
        assert!((m["controller_mutation_rate"] - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn quotas_parse_from_strimzi_shape() {
        let json = r#"{
            "producerByteRate": 1048576,
            "consumerByteRate": 2097152,
            "requestPercentage": 55,
            "controllerMutationRate": 10.5
        }"#;
        let q: KafkaUserQuotas = serde_json::from_str(json).unwrap();
        assert_eq!(q.producer_byte_rate, Some(1_048_576));
        assert_eq!(q.consumer_byte_rate, Some(2_097_152));
        assert_eq!(q.request_percentage, Some(55));
        assert_eq!(q.controller_mutation_rate, Some(10.5));
    }

    #[test]
    fn empty_quotas_object_parses_and_is_a_clear_signal() {
        // `spec.quotas: {}` is the "wipe broker quotas" signal — the
        // reconciler diffs against an empty desired map and tombstones
        // every key the broker has for this user.
        let json = r#"{"authentication":{"type":"scram-sha-512"},"quotas":{}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        let q = spec.quotas.expect("quotas section present");
        assert!(q.to_quota_map().is_empty());
    }

    #[test]
    fn omitted_quotas_means_operator_does_not_manage() {
        // `spec.quotas` absent => `spec.quotas == None` => the
        // reconciler skips quota reconciliation entirely.
        let json = r#"{"authentication":{"type":"scram-sha-512"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert!(spec.quotas.is_none());
    }

    #[test]
    fn tls_auth_round_trips() {
        let auth = Authentication::Tls(TlsAuth::default());
        let v = serde_json::to_value(&auth).unwrap();
        assert_eq!(v, serde_json::json!({"type": "tls"}));
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert_eq!(back, auth);
    }

    #[test]
    fn tls_auth_with_validity_days_round_trips() {
        let auth = Authentication::Tls(TlsAuth {
            validity_days: Some(180),
            renewal_days: Some(14),
        });
        let v = serde_json::to_value(&auth).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "type": "tls",
                "validityDays": 180,
                "renewalDays": 14,
            })
        );
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert_eq!(back, auth);
    }

    #[test]
    fn authentication_scram_round_trips_unchanged() {
        let auth = Authentication::ScramSha512(ScramSha512Auth::default());
        let v = serde_json::to_value(&auth).unwrap();
        assert_eq!(v, serde_json::json!({"type": "scram-sha-512"}));
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert_eq!(back, auth);
    }

    #[test]
    fn status_tls_fields_omit_when_unset() {
        let status = KafkaUserStatus {
            tls: false,
            tls_cert_not_after: None,
            tls_principal: None,
            ..Default::default()
        };
        let j = serde_json::to_string(&status).unwrap();
        assert!(!j.contains("tlsCertNotAfter"), "got: {j}");
        assert!(!j.contains("tlsPrincipal"), "got: {j}");
        assert!(j.contains("\"tls\":false"), "got: {j}");
    }

    #[test]
    fn status_tls_fields_emit_when_set() {
        let status = KafkaUserStatus {
            tls: true,
            tls_cert_not_after: Some("2027-05-19T00:00:00Z".into()),
            tls_principal: Some("User:CN=alice".into()),
            ..Default::default()
        };
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(v.get("tls"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(
            v.get("tlsCertNotAfter").and_then(|x| x.as_str()),
            Some("2027-05-19T00:00:00Z")
        );
        assert_eq!(
            v.get("tlsPrincipal").and_then(|x| x.as_str()),
            Some("User:CN=alice")
        );
    }

    #[test]
    fn tls_external_round_trips() {
        let auth = Authentication::TlsExternal;
        let j = serde_json::to_string(&auth).unwrap();
        assert_eq!(j, r#"{"type":"tls-external"}"#);
        let back: Authentication = serde_json::from_str(&j).unwrap();
        assert_eq!(back, auth);
    }

    #[test]
    fn tls_external_with_quotas_and_acls_round_trips() {
        let spec = KafkaUserSpec {
            authentication: Authentication::TlsExternal,
            authorization: Some(Authorization::Simple(SimpleAuthorization {
                acls: vec![AclRule {
                    resource: AclResource {
                        kind: AclResourceKind::Topic,
                        name: "orders".into(),
                        pattern_type: AclPatternType::Literal,
                    },
                    operations: vec![AclOp::Read],
                    host: "*".into(),
                    permission: AclPermission::Allow,
                }],
            })),
            quotas: Some(KafkaUserQuotas {
                producer_byte_rate: Some(1_048_576),
                ..Default::default()
            }),
        };
        let j = serde_json::to_string(&spec).unwrap();
        assert!(j.contains("\"type\":\"tls-external\""), "got: {j}");
        assert!(j.contains("\"name\":\"orders\""), "got: {j}");
        assert!(j.contains("\"producerByteRate\":1048576"), "got: {j}");
        let back: KafkaUserSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn tls_external_minimum_spec_parses() {
        let json = r#"{"authentication":{"type":"tls-external"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.authentication, Authentication::TlsExternal);
        assert!(spec.authorization.is_none());
        assert!(spec.quotas.is_none());
    }

    #[test]
    fn status_external_field_emits_when_true() {
        let status = KafkaUserStatus {
            external: true,
            ..Default::default()
        };
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(v.get("external"), Some(&serde_json::Value::Bool(true)));
    }

    #[test]
    fn status_external_field_emits_default_false() {
        let status = KafkaUserStatus::default();
        let j = serde_json::to_string(&status).unwrap();
        assert!(j.contains("\"external\":false"), "got: {j}");
    }
}
