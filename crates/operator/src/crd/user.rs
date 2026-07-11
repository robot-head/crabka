//! `KafkaUser` CRD. Strimzi-shaped — SCRAM-SHA-512 + mTLS
//! authentication, simple ACL authorization, and
//! optional per-user quotas.

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

    /// Optional per-user client quotas (KIP-13/124/257).
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
    /// SCRAM-SHA-256 sibling of `ScramSha512`. The operator
    /// provisions the password Secret + ACLs + quotas exactly as for
    /// SHA-512; the only differences on the wire are the mechanism
    /// byte (1 vs 2) and the HMAC algorithm. Pair with broker-side
    /// `enabled_sasl_mechanisms` covering `ScramSha256`.
    #[serde(rename = "scram-sha-256")]
    ScramSha256(ScramSha256Auth),
    #[serde(rename = "tls")]
    Tls(TlsAuth),
    /// Credential-less user. The operator provisions ACLs +
    /// quotas under `User:<metadata.name>` but does not create a Secret
    /// or issue a cert — credentials are managed out-of-band (e.g. an
    /// OIDC provider for SASL/OAUTHBEARER, or a CA outside Crabka for
    /// mTLS). Mirrors Strimzi's `tls-external`.
    #[serde(rename = "tls-external")]
    TlsExternal,
    /// KIP-48 delegation-token authentication. The operator
    /// acts-as a super-user to mint a token owned by this user, persists
    /// `(token-id, hmac)` into a Secret, and periodically renews ahead
    /// of expiry.
    #[serde(rename = "delegation-token")]
    DelegationToken(DelegationTokenAuth),
}

/// Per-user knobs that flow into `CreateDelegationToken` and
/// the operator's renew loop. The reconciler mints the token via the
/// admin client (operator acts-as a super-user), persists
/// `(token-id, hmac)` into the user's Secret, and renews ahead of
/// `expiry_timestamp_ms`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenAuth {
    /// Principal strings (e.g. `"User:bob"`) allowed to renew/expire
    /// this token in addition to the owner. Default empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renewers: Vec<String>,

    /// Hard upper bound on token lifetime in milliseconds. `None` →
    /// broker's `delegation_token_max_lifetime_ms` (7d default). Capped
    /// by the broker even when explicitly set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_lifetime_ms: Option<i64>,

    /// Renew when `expiry_timestamp_ms - now <= this`. Default 24h.
    /// Minimum 60s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60_000))]
    pub renew_before_expiry_ms: Option<i64>,
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
            "maxLifetimeMs": { "type": "integer", "minimum": 1 },
            "renewBeforeExpiryMs": { "type": "integer", "minimum": 60000 },
        },
    })
}

macro_rules! scram_auth {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
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
    };
}

scram_auth!(ScramSha512Auth);

scram_auth!(
    /// SCRAM-SHA-256 sibling of [`ScramSha512Auth`]. Same field
    /// shape; the only semantic difference is the wire mechanism + HMAC
    /// algorithm picked up by the reconciler's match arm.
    ScramSha256Auth
);

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

    /// True once SCRAM-SHA-256 credentials are provisioned.
    #[serde(default)]
    pub scram_sha256: bool,

    /// True once the spec's `quotas` (if any) have been reflected in the
    /// broker's `(user)` client-quota state. False when `spec.quotas`
    /// is `None` (the operator does not touch broker quotas).
    #[serde(default)]
    pub quotas_in_sync: bool,

    /// `true` once a TLS user has a current cert Secret. Mirrors
    /// `scram_sha512`.
    #[serde(default)]
    pub tls: bool,

    /// `true` once a credential-less user
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
    /// `User:CN=alice` for TLS users, `User:alice` for SCRAM and
    /// `tls-external` users). Always populated when the user is
    /// provisioned. Load-bearing for debugging "why isn't my ACL
    /// matching" issues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_principal: Option<String>,

    /// Persisted `token_id` (UUID) of the operator-managed
    /// delegation token for this user. Used across reconciles to find
    /// the same token via `DescribeDelegationToken`. Present once the
    /// operator has successfully issued a token via
    /// `CreateDelegationToken`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_id: Option<String>,

    /// Current `expiry_timestamp_ms` of the operator-managed
    /// delegation token (extended on each successful renew). Compared
    /// against `now` to decide when to renew per
    /// `spec.authentication.renewBeforeExpiryMs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_expiry_timestamp_ms: Option<i64>,

    /// Token's absolute upper bound (`max_timestamp_ms`).
    /// Renew can never push `expiry_timestamp_ms` past this — the
    /// operator stops renewing and surfaces `TokenExpiring` once
    /// further extension is impossible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token_max_timestamp_ms: Option<i64>,
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

    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaUser::crd();
        assert2::assert!(crd.spec.group.as_str() == "crabka.io");
        assert2::assert!(crd.spec.names.kind.as_str() == "KafkaUser");
        assert2::assert!(crd.spec.names.plural.as_str() == "kafkausers");
        assert2::assert!(crd.spec.names.short_names == Some(vec!["ku".to_string()]));
        assert2::assert!(
            crd.spec
                .versions
                .iter()
                .map(|v| v.name.as_str())
                .collect::<Vec<_>>()
                == vec!["v1alpha1"]
        );
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
            assert2::assert!(json.contains(want));
        }
        let back: KafkaUser = serde_json::from_str(&json).unwrap();
        assert2::assert!(back.spec == ku.spec);
    }

    #[test]
    fn minimum_spec_parses() {
        let json = r#"{"authentication":{"type":"scram-sha-512"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert2::assert!(
            spec == KafkaUserSpec {
                authentication: Authentication::ScramSha512(ScramSha512Auth {
                    iterations: None,
                    password_length: None,
                }),
                authorization: None,
                quotas: None,
            }
        );
    }

    #[test]
    fn acl_rule_defaults_host_and_permission() {
        let json = r#"{
            "resource": {"type":"topic","name":"orders"},
            "operations":["Read"]
        }"#;
        let rule: AclRule = serde_json::from_str(json).unwrap();
        assert2::assert!(
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
    fn acl_rule_host_serialization_cases() {
        for (_name, host, expected_host) in [
            ("default host omitted", "*", None),
            ("non-default host emitted", "10.0.0.0", Some("10.0.0.0")),
        ] {
            let rule = AclRule {
                resource: AclResource {
                    kind: AclResourceKind::Topic,
                    name: "orders".into(),
                    pattern_type: AclPatternType::Literal,
                },
                operations: vec![AclOp::Read],
                host: host.into(),
                permission: AclPermission::Allow,
            };
            let value = serde_json::to_value(&rule).unwrap();
            assert2::assert!(
                value.get("host").and_then(serde_json::Value::as_str) == expected_host
            );
        }
    }

    #[test]
    fn quotas_empty_serializes_as_empty_object() {
        let q = KafkaUserQuotas::default();
        let j = serde_json::to_string(&q).unwrap();
        assert2::assert!(j == "{}");
        assert2::assert!(q.to_quota_map().is_empty());
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
        assert2::assert!(
            m == std::collections::BTreeMap::from([
                ("producer_byte_rate".to_string(), 1_048_576.0),
                ("request_percentage".to_string(), 25.0),
            ])
        );
    }

    #[test]
    fn quotas_to_map_carries_controller_mutation_rate_as_double() {
        let q = KafkaUserQuotas {
            controller_mutation_rate: Some(2.5),
            ..Default::default()
        };
        let m = q.to_quota_map();
        assert2::assert!(
            m == std::collections::BTreeMap::from([("controller_mutation_rate".to_string(), 2.5)])
        );
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
        assert2::assert!(
            q == KafkaUserQuotas {
                producer_byte_rate: Some(1_048_576),
                consumer_byte_rate: Some(2_097_152),
                request_percentage: Some(55),
                controller_mutation_rate: Some(10.5),
            }
        );
    }

    #[test]
    fn quota_presence_cases() {
        for (_name, json, expected) in [
            (
                "present empty quotas clear broker state",
                r#"{"authentication":{"type":"scram-sha-512"},"quotas":{}}"#,
                Some(KafkaUserQuotas::default()),
            ),
            (
                "omitted quotas are unmanaged",
                r#"{"authentication":{"type":"scram-sha-512"}}"#,
                None,
            ),
        ] {
            let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
            assert2::assert!(spec.quotas == expected);
        }
    }

    #[test]
    fn authentication_json_round_trip_cases() {
        for (_name, authentication, expected_json) in [
            (
                "TLS defaults",
                Authentication::Tls(TlsAuth::default()),
                serde_json::json!({"type": "tls"}),
            ),
            (
                "TLS validity overrides",
                Authentication::Tls(TlsAuth {
                    validity_days: Some(180),
                    renewal_days: Some(14),
                }),
                serde_json::json!({
                    "type": "tls",
                    "validityDays": 180,
                    "renewalDays": 14,
                }),
            ),
            (
                "SCRAM-SHA-512 defaults",
                Authentication::ScramSha512(ScramSha512Auth::default()),
                serde_json::json!({"type": "scram-sha-512"}),
            ),
            (
                "SCRAM-SHA-256 defaults",
                Authentication::ScramSha256(ScramSha256Auth::default()),
                serde_json::json!({"type": "scram-sha-256"}),
            ),
            (
                "SCRAM-SHA-256 overrides",
                Authentication::ScramSha256(ScramSha256Auth {
                    iterations: Some(16_384),
                    password_length: Some(64),
                }),
                serde_json::json!({
                    "type": "scram-sha-256",
                    "iterations": 16_384,
                    "passwordLength": 64,
                }),
            ),
            (
                "TLS external",
                Authentication::TlsExternal,
                serde_json::json!({"type": "tls-external"}),
            ),
        ] {
            let actual_json = serde_json::to_value(&authentication).unwrap();
            assert2::assert!(&actual_json == &expected_json);
            let back: Authentication = serde_json::from_value(actual_json).unwrap();
            assert2::assert!(back == authentication);
        }
    }

    #[test]
    fn status_field_json_cases() {
        for (_name, status, expected) in [
            (
                "optional fields unset",
                KafkaUserStatus::default(),
                serde_json::json!({"conditions": [], "tls": false, "scramSha512": false, "scramSha256": false, "quotasInSync": false, "external": false}),
            ),
            (
                "TLS fields populated",
                KafkaUserStatus {
                    tls: true,
                    tls_cert_not_after: Some("2027-05-19T00:00:00Z".into()),
                    tls_principal: Some("User:CN=alice".into()),
                    ..Default::default()
                },
                serde_json::json!({"conditions": [], "tls": true, "tlsCertNotAfter": "2027-05-19T00:00:00Z", "tlsPrincipal": "User:CN=alice", "scramSha512": false, "scramSha256": false, "quotasInSync": false, "external": false}),
            ),
            (
                "external user",
                KafkaUserStatus {
                    external: true,
                    ..Default::default()
                },
                serde_json::json!({"conditions": [], "tls": false, "scramSha512": false, "scramSha256": false, "quotasInSync": false, "external": true}),
            ),
        ] {
            assert2::assert!(serde_json::to_value(status).unwrap() == expected);
        }
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
            assert2::assert!(j.contains(want));
        }
        let back: KafkaUserSpec = serde_json::from_str(&j).unwrap();
        assert2::assert!(back == spec);
    }

    #[test]
    fn tls_external_minimum_spec_parses() {
        let json = r#"{"authentication":{"type":"tls-external"}}"#;
        let spec: KafkaUserSpec = serde_json::from_str(json).unwrap();
        assert2::assert!(
            spec == KafkaUserSpec {
                authentication: Authentication::TlsExternal,
                authorization: None,
                quotas: None,
            }
        );
    }

    #[test]
    fn delegation_token_authentication_yaml_cases() {
        for (name, yaml, expected) in [
            (
                "populated delegation token",
                r#"
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
    renewers: ["User:bob", "User:carol"]
    maxLifetimeMs: 86400000
    renewBeforeExpiryMs: 7200000
"#,
                DelegationTokenAuth {
                    renewers: vec!["User:bob".to_string(), "User:carol".to_string()],
                    max_lifetime_ms: Some(86_400_000),
                    renew_before_expiry_ms: Some(7_200_000),
                },
            ),
            (
                "minimal delegation token",
                "
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
",
                DelegationTokenAuth {
                    renewers: vec![],
                    max_lifetime_ms: None,
                    renew_before_expiry_ms: None,
                },
            ),
        ] {
            let user: KafkaUser = serde_yaml::from_str(yaml).unwrap();
            let Authentication::DelegationToken(actual) = user.spec.authentication else {
                panic!("case {name}: expected DelegationToken variant");
            };
            assert2::assert!(actual == expected);
        }
    }
}
