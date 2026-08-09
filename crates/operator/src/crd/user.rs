//! `KafkaUser` CRD.
//!
//! The CRD is Strimzi-shaped. It supports SCRAM-SHA-512 and mTLS
//! authentication, simple ACL authorization, and optional per-user quotas.

use crabka_units::Time;
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

    /// Authorization is optional. A user with no ACLs can still
    /// authenticate. When this field is absent, the operator skips ACL
    /// reconciliation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,

    /// Optional per-user client quotas (KIP-13/124/257). This field maps
    /// onto Kafka's `(user)` quota entity through `AlterClientQuotas`, which
    /// is `api_key` 49. When the field is absent, the operator does not touch
    /// the broker's quota state. When the field is present, the operator
    /// drives the broker's quota keys for `User:<name>` toward the spec. It
    /// sets the configured fields and tombstones the omitted ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quotas: Option<KafkaUserQuotas>,
}

/// Strimzi-shaped `KafkaUserQuotas`. The field names and the JSON types match
/// `kafka.strimzi.io/v1beta2`. The values flow to the broker as `f64`, which
/// is Kafka's wire type for client-quota values.
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

    /// Maximum percentage of a request-handler thread's time that the user
    /// can consume, in the range `0..=100`. Backed by `request_percentage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 100))]
    pub request_percentage: Option<i32>,

    /// KIP-599 controller-mutation rate in creates and deletes per second.
    /// Backed by `controller_mutation_rate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_mutation_rate: Option<f64>,
}

/// Tagged enum on `type`, in the same shape as Strimzi.
///
/// The wire shape is flat and Strimzi-compatible. `type` is the discriminator,
/// and the per-variant config fields are siblings of `type`. The custom
/// `schema_with` writes a structural schema by hand, because the
/// `StructuralSchemaRewriter` of kube-rs 3.x panics when `oneOf` branches
/// share a `type` property with different `enum` values. That is the default
/// schemars output for tagged-union enums. This is the same workaround as
/// `Storage` in `kafka_node_pool.rs`.
///
/// The operator enforces the cross-variant constraints at reconcile time, and
/// the apiserver does not. One such constraint is that `iterations` is valid
/// only when `type=scram-sha-512`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[schemars(schema_with = "authentication_schema")]
pub enum Authentication {
    #[serde(rename = "scram-sha-512")]
    ScramSha512(ScramSha512Auth),
    /// SCRAM-SHA-256 sibling of `ScramSha512`. The operator provisions the
    /// password Secret, the ACLs, and the quotas exactly as for SHA-512. The
    /// only differences on the wire are the mechanism byte, 1 against 2, and
    /// the HMAC algorithm. Pair this variant with a broker-side
    /// `enabled_sasl_mechanisms` that covers `ScramSha256`.
    #[serde(rename = "scram-sha-256")]
    ScramSha256(ScramSha256Auth),
    #[serde(rename = "tls")]
    Tls(TlsAuth),
    /// Credential-less user. The operator provisions ACLs and quotas under
    /// `User:<metadata.name>`, but it does not create a Secret and does not
    /// issue a cert. Something out-of-band manages the credentials, for
    /// example an OIDC provider for SASL/OAUTHBEARER, or a CA outside Crabka
    /// for mTLS. This is the same as Strimzi's `tls-external`.
    #[serde(rename = "tls-external")]
    TlsExternal,
    /// KIP-48 delegation-token authentication. The operator
    /// acts-as a super-user to mint a token owned by this user, persists
    /// `(token-id, hmac)` into a Secret, and periodically renews ahead
    /// of expiry.
    #[serde(rename = "delegation-token")]
    DelegationToken(DelegationTokenAuth),
}

/// Per-user knobs that flow into `CreateDelegationToken` and into the
/// operator's renew loop. The reconciler mints the token with the admin
/// client, and the operator acts-as a super-user. The reconciler persists
/// `(token-id, hmac)` into the user's Secret and renews ahead of
/// `expiry_timestamp_ms`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenAuth {
    /// Principal strings, for example `"User:bob"`, that can renew or expire
    /// this token in addition to the owner. Default: empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renewers: Vec<String>,

    /// Hard upper bound on token lifetime. `None` gives the broker's
    /// `delegation_token_max_lifetime`, which defaults to 7d. The broker caps
    /// this value even when you set it explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub max_lifetime: Option<Time>,

    /// Renew when `expiry_timestamp_ms - now <= this`. Default 24h.
    /// Minimum 60s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub renew_before_expiry: Option<Time>,
}

fn authentication_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["scram-sha-512", "scram-sha-256", "tls", "tls-external", "delegation-token"],
            },
            // SCRAM
            "iterations": { "type": "integer", "minimum": 4096, "maximum": 1_000_000 },
            "passwordLength": { "type": "integer", "minimum": 16, "maximum": 256 },
            // TLS
            "validityDays": { "type": "integer", "minimum": 1, "maximum": 36500 },
            "renewalDays": { "type": "integer", "minimum": 1, "maximum": 3650 },
            // Delegation token
            "renewers": {
                "type": "array",
                "items": { "type": "string", "pattern": "^User:.+$" },
            },
            "maxLifetime": { "type": "string" },
            "renewBeforeExpiry": { "type": "string" },
        },
    })
}

macro_rules! scram_auth {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            /// PBKDF2 iteration count. Defaults to 8192 on the controller
            /// side, which matches
            /// `crabka_client_admin::DEFAULT_SCRAM_ITERATIONS`. The broker
            /// rejects values < 4096.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            #[schemars(range(min = 4096, max = 1_000_000))]
            pub iterations: Option<i32>,

            /// Raw-password length in bytes for the operator-generated
            /// secret. Defaults to 32 bytes, which is 44 base64 chars. The
            /// reconcile ignores it if a Secret with the key `password`
            /// already exists.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            #[schemars(range(min = 16, max = 256))]
            pub password_length: Option<u16>,
        }
    };
}

scram_auth!(ScramSha512Auth);

scram_auth!(
    /// SCRAM-SHA-256 sibling of [`ScramSha512Auth`]. The field shape is the
    /// same. The only semantic difference is the wire mechanism and the HMAC
    /// algorithm that the reconciler's match arm selects.
    ScramSha256Auth
);

/// mTLS authentication config. The operator generates an X.509 client cert
/// that the per-cluster clients CA signs. The operator stores the cert in the
/// per-user Secret under the keys `user.crt`, `user.key`, and `ca.crt`. The
/// cert's Subject is the bare RDN `CN=<KafkaUser name>`, and that DN is the
/// ACL and quota principal.
///
/// **Validity & renewal.** The cert lives for `validity_days`, which defaults
/// to 365. The reconciler reissues the cert if and only if
/// `not_after - now <= renewal_days`, which defaults to 30. A reissue replaces
/// `user.crt` and `user.key`. Consumers must reload their TLS client config to
/// get the new material.
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
    /// The same shape as Strimzi's `KafkaUserAuthorizationSimple`. It drives
    /// the `SimpleAclAuthorizer`, which is the only authorizer that Crabka
    /// implements today.
    Simple(SimpleAuthorization),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    #[serde(default)]
    pub acls: Vec<AclRule>,
}

/// One rule from `spec.authorization.acls`. The operator expands it into one
/// `(resource, operation, host, type)` tuple per `operations` entry at
/// reconcile time.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AclRule {
    pub resource: AclResource,
    /// Non-empty list. The reconciler refuses an empty `operations` with
    /// `Ready=False reason=InvalidSpec`.
    pub operations: Vec<AclOp>,
    /// Source-address pattern. `"*"` is the wildcard. Defaults to `"*"` when
    /// omitted.
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
    /// `literal` for an exact match, or `prefixed` for resources whose name
    /// starts with `name`. Defaults to `literal`.
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

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(transparent)]
pub struct StatusFlag(pub bool);

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserStatus {
    /// Standard Kubernetes-style condition list. It shows `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective Kafka principal name. It matches `metadata.name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Name of the Kubernetes Secret holding the user's password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,

    /// True once SCRAM-SHA-512 credentials are provisioned.
    #[serde(default)]
    pub scram_sha512: StatusFlag,

    /// True once SCRAM-SHA-256 credentials are provisioned.
    #[serde(default)]
    pub scram_sha256: StatusFlag,

    /// True once the operator has reflected the spec's `quotas`, if any, in
    /// the broker's `(user)` client-quota state. False when `spec.quotas` is
    /// `None`, because the operator then does not touch broker quotas.
    #[serde(default)]
    pub quotas_in_sync: bool,

    /// `true` once a TLS user has a current cert Secret. It has the same
    /// shape as `scram_sha512`.
    #[serde(default)]
    pub tls: bool,

    /// `true` once the operator has reconciled a credential-less user with
    /// `type: tls-external`. It appears in `kubectl describe ku`, so that
    /// operators can see that the operator does not own this user's
    /// credentials.
    #[serde(default)]
    pub external: StatusFlag,

    /// RFC3339 timestamp of the user cert's `notAfter`. Present when
    /// `tls == true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert_not_after: Option<String>,

    /// The principal string that the operator pinned in ACLs. It is
    /// `User:CN=alice` for TLS users, and `User:alice` for SCRAM and
    /// `tls-external` users. It is always filled in when the user is
    /// provisioned. It is load-bearing for the debug of ACL-match problems.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_principal: Option<String>,

    /// Persisted `token_id`, a UUID, of the operator-managed delegation
    /// token for this user. The operator uses it across reconciles to find the
    /// same token with `DescribeDelegationToken`. It is present once the
    /// operator has issued a token with `CreateDelegationToken`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_id: Option<String>,

    /// Current `expiry_timestamp_ms` of the operator-managed delegation
    /// token. Each successful renew extends it. The operator compares it
    /// against `now` to decide when to renew, as
    /// `spec.authentication.renewBeforeExpiry` specifies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_expiry_timestamp_ms: Option<i64>,

    /// Token's absolute upper bound, which is `max_timestamp_ms`. A renew can
    /// never push `expiry_timestamp_ms` past this bound. The operator stops
    /// the renews and surfaces `TokenExpiring` once no more extension is
    /// possible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_max_timestamp_ms: Option<i64>,
}

impl KafkaUserQuotas {
    /// Project the typed spec onto the wire key-to-value map that the admin
    /// client uses. This function skips null values such as
    /// `producerByteRate=null`. The reconciler's diff then tombstones any
    /// broker key that is not present here.
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
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaUser::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "KafkaUser");
        check!(crd.spec.names.plural == "kafkausers");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"ku".to_string())),
            "expected shortname `ku`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
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
        for want in [
            "\"type\":\"scram-sha-512\"",
            "\"iterations\":16384",
            "\"type\":\"simple\"",
            "\"name\":\"orders\"",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaUser = serde_json::from_str(&json).unwrap();
        assert!(back.spec == ku.spec);
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
        assert!(
            rule == AclRule {
                resource: AclResource {
                    kind: AclResourceKind::Topic,
                    name: "orders".to_string(),
                    pattern_type: AclPatternType::Literal,
                },
                operations: vec![AclOp::Read],
                host: "*".to_string(),
                permission: AclPermission::Allow,
            }
        );
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
        for absent in ["observedGeneration", "username", "secret"] {
            assert!(!j.contains(absent), "case {absent:?}; got: {j}");
        }
        // `scramSha512` + `quotasInSync` are plain bools — serde emits them.
        assert!(j.contains("\"scramSha512\":false"), "got: {j}");
        assert!(j.contains("\"quotasInSync\":false"), "got: {j}");
    }

    #[test]
    fn quotas_empty_serializes_as_empty_object() {
        let q = KafkaUserQuotas::default();
        let j = serde_json::to_string(&q).unwrap();
        assert!(j == "{}");
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
        check!(m.len() == 2);
        check!((m["producer_byte_rate"] - 1_048_576.0).abs() < f64::EPSILON);
        check!((m["request_percentage"] - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn quotas_to_map_carries_controller_mutation_rate_as_double() {
        let q = KafkaUserQuotas {
            controller_mutation_rate: Some(2.5),
            ..Default::default()
        };
        let m = q.to_quota_map();
        assert!(m.len() == 1);
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
        assert!(
            q == KafkaUserQuotas {
                producer_byte_rate: Some(1_048_576),
                consumer_byte_rate: Some(2_097_152),
                request_percentage: Some(55),
                controller_mutation_rate: Some(10.5),
            }
        );
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
        assert!(v == serde_json::json!({"type": "tls"}));
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert!(back == auth);
    }

    #[test]
    fn tls_auth_with_validity_days_round_trips() {
        let auth = Authentication::Tls(TlsAuth {
            validity_days: Some(180),
            renewal_days: Some(14),
        });
        let v = serde_json::to_value(&auth).unwrap();
        assert!(
            v == serde_json::json!({
                "type": "tls",
                "validityDays": 180,
                "renewalDays": 14,
            })
        );
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert!(back == auth);
    }

    #[test]
    fn authentication_scram_round_trips_unchanged() {
        let auth = Authentication::ScramSha512(ScramSha512Auth::default());
        let v = serde_json::to_value(&auth).unwrap();
        assert!(v == serde_json::json!({"type": "scram-sha-512"}));
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert!(back == auth);
    }

    #[test]
    fn authentication_scram_sha256_round_trips_with_overrides() {
        // `scram-sha-256` sibling of the existing SHA-512
        // round-trip test. Cover both the empty-defaults shape AND the
        // explicit-overrides shape to lock the schema (a `passwordLength`
        // change between releases would silently roll every user's
        // Secret).
        let auth_default = Authentication::ScramSha256(ScramSha256Auth::default());
        let v = serde_json::to_value(&auth_default).unwrap();
        assert!(v == serde_json::json!({"type": "scram-sha-256"}));
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert!(back == auth_default);

        let auth_overrides = Authentication::ScramSha256(ScramSha256Auth {
            iterations: Some(16_384),
            password_length: Some(64),
        });
        let v = serde_json::to_value(&auth_overrides).unwrap();
        assert!(
            v == serde_json::json!({
                "type": "scram-sha-256",
                "iterations": 16_384,
                "passwordLength": 64,
            })
        );
        let back: Authentication = serde_json::from_value(v).unwrap();
        assert!(back == auth_overrides);
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
        check!(!j.contains("tlsCertNotAfter"), "got: {j}");
        check!(!j.contains("tlsPrincipal"), "got: {j}");
        check!(j.contains("\"tls\":false"), "got: {j}");
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
        check!(v.get("tls") == Some(&serde_json::Value::Bool(true)));
        check!(v.get("tlsCertNotAfter").and_then(|x| x.as_str()) == Some("2027-05-19T00:00:00Z"));
        check!(v.get("tlsPrincipal").and_then(|x| x.as_str()) == Some("User:CN=alice"));
    }

    #[test]
    fn tls_external_round_trips() {
        let auth = Authentication::TlsExternal;
        let j = serde_json::to_string(&auth).unwrap();
        assert!(j == r#"{"type":"tls-external"}"#);
        let back: Authentication = serde_json::from_str(&j).unwrap();
        assert!(back == auth);
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
        for want in [
            "\"type\":\"tls-external\"",
            "\"name\":\"orders\"",
            "\"producerByteRate\":1048576",
        ] {
            assert!(j.contains(want), "case {want:?}; got: {j}");
        }
        let back: KafkaUserSpec = serde_json::from_str(&j).unwrap();
        assert!(back == spec);
    }

    #[test]
    fn tls_external_minimum_spec_parses() {
        let json = r#"{"authentication":{"type":"tls-external"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert!(
            spec == KafkaUserSpec {
                authentication: Authentication::TlsExternal,
                authorization: None,
                quotas: None,
            }
        );
    }

    #[test]
    fn status_external_field_emits_when_true() {
        let status = KafkaUserStatus {
            external: StatusFlag(true),
            ..Default::default()
        };
        let v = serde_json::to_value(&status).unwrap();
        assert!(v.get("external") == Some(&serde_json::Value::Bool(true)));
    }

    #[test]
    fn status_external_field_emits_default_false() {
        let status = KafkaUserStatus::default();
        let j = serde_json::to_string(&status).unwrap();
        assert!(j.contains("\"external\":false"), "got: {j}");
    }

    #[test]
    fn delegation_token_authentication_round_trip() {
        let yaml = r#"
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
    renewers: ["User:bob", "User:carol"]
    maxLifetime: 1d
    renewBeforeExpiry: 2h
"#;
        let user: KafkaUser = serde_yaml::from_str(yaml).unwrap();
        let Authentication::DelegationToken(dt) = user.spec.authentication else {
            panic!("expected DelegationToken variant");
        };
        assert!(
            dt == DelegationTokenAuth {
                renewers: vec!["User:bob".to_string(), "User:carol".to_string()],
                max_lifetime: Some(crabka_units::days(1)),
                renew_before_expiry: Some(crabka_units::hours(2)),
            }
        );
    }

    #[test]
    fn delegation_token_authentication_minimal_omits_optional_fields() {
        let yaml = "
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
";
        let user: KafkaUser = serde_yaml::from_str(yaml).unwrap();
        let Authentication::DelegationToken(dt) = user.spec.authentication else {
            panic!("expected DelegationToken variant");
        };
        assert!(
            dt == DelegationTokenAuth {
                renewers: vec![],
                max_lifetime: None,
                renew_before_expiry: None,
            }
        );
    }

    #[test]
    fn delegation_token_time_fields_are_uom_strings_in_the_schema() {
        let crd = serde_json::to_value(KafkaUser::crd()).expect("serialize CRD");
        let auth = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
            ["authentication"]["properties"];

        assert!(auth["maxLifetime"]["type"] == "string");
        assert!(auth["renewBeforeExpiry"]["type"] == "string");
        assert!(auth.get("maxLifetimeMs").is_none());
        assert!(auth.get("renewBeforeExpiryMs").is_none());
    }
}
