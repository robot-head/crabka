//! `Kafka.spec.listeners` schema — Strimzi-shaped.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    /// Unique within the cluster. Alphanumeric + `-`, ≤25 chars. Used
    /// as the Kafka listener name; surfaces in `bootstrap.servers`-style
    /// URLs.
    pub name: String,
    /// Container port the broker binds. Unique within the cluster.
    pub port: i32,
    /// Listener type. `internal` is in-cluster; `nodeport` /
    /// `loadbalancer` create external Services; `ingress` / `route` are
    /// accepted by the schema but rejected at reconcile until slice 27.
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// Transport-level TLS. When `true`, the listener uses the per-broker
    /// keystore signed by the cluster CA (slice 30) and clients must speak
    /// TLS to connect. Independent of `authentication` — a `tls: true`
    /// listener with no `authentication` is anonymous over TLS.
    #[serde(default)]
    pub tls: bool,
    /// Per-listener authentication mechanism. Absent means anonymous (no
    /// client identity required). When set to `type: tls`, the listener
    /// must also have `tls: true` — enforced at reconcile time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<ListenerAuthentication>,
    /// Optional listener-type-specific configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
    /// Slice 23: per-listener peer allow-list. Tri-state:
    /// - `None` → no per-listener restriction (allow-all on this port).
    /// - `Some(vec![])` → deny-all on this listener port (no per-listener
    ///   rule emitted; default-deny applies).
    /// - `Some(non_empty)` → only listed peers may reach this port.
    ///
    /// Only consulted when `Kafka.spec.networkPolicy` is set; otherwise
    /// inert. The operator auto-allow rule still fires on this port even
    /// for deny-all listeners so the operator can manage the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy_peers: Option<Vec<crate::crd::NetworkPolicyPeer>>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    #[default]
    Internal,
    Nodeport,
    Loadbalancer,
    Ingress,
    Route,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokerOverride>,
    /// `ingress` only (slice 27): the `spec.ingressClassName` set on every
    /// generated `Ingress`. Strimzi-shaped `configuration.class`. Inert for
    /// other listener types.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "class")]
    pub ingress_class: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapConfig {
    /// `nodeport` only: pin the bootstrap `NodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin the bootstrap LB IP.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "loadBalancerIP"
    )]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): bootstrap hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Annotations to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Labels to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrokerOverride {
    /// Broker id this override applies to (matches the node id).
    pub broker: i32,
    /// Override the computed advertised host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_host: Option<String>,
    /// Override the computed advertised port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_port: Option<i32>,
    /// `nodeport` only: pin this broker's `Service.spec.ports[0].nodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin this broker's `Service.spec.loadBalancerIP`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "loadBalancerIP"
    )]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): per-broker hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// Per-listener authentication mechanism.
///
/// The `schema_with` workaround avoids a kube-rs 3.x `StructuralSchemaRewriter`
/// panic when `oneOf` branches share a `type` discriminator with differing `enum`
/// values — same pattern as `Authentication` in `user.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type")]
#[schemars(schema_with = "listener_authentication_schema")]
pub enum ListenerAuthentication {
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "scram-sha-512")]
    ScramSha512,
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    #[serde(rename = "oauth")]
    OAuth(ListenerAuthenticationOAuth),
}

/// Config for `authentication: { type: oauth }` on a listener. The
/// reconciler (T3) renders these into the broker-global
/// `[oauthbearer]` TOML block and appends `OAUTHBEARER` to the
/// listener's `sasl_mechanisms`. Slice 50 narrows Strimzi's surface to
/// the fields the 49b validator honors; see the umbrella roadmap
/// (`2026-05-23-crabka-oauth-parity-roadmap-design.md`) for fields
/// deferred to later slices.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationOAuth {
    /// Expected `iss` claim. Broker rejects tokens whose `iss` differs.
    pub valid_issuer_uri: String,
    /// HTTPS URL the broker fetches the signing JWKS from.
    pub jwks_endpoint_uri: String,
    /// Optional expected `aud` claim. Absent means no audience check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_audience: Option<String>,
    /// Claim name to use as the Kafka principal. Defaults broker-side
    /// to `sub`; set e.g. to `preferred_username` for Keycloak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name_claim: Option<String>,
    /// Optional required-scope check; see `OAuthCustomClaimCheck`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_claim_check: Option<OAuthCustomClaimCheck>,
    /// JWKS refresh cadence in seconds. Reconciler (T3) enforces
    /// `>= 30`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_refresh_seconds: Option<u32>,
    /// Allowed clock skew in seconds for `exp`/`nbf`/`iat` checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_clock_skew_seconds: Option<u32>,
    /// Whether to advertise `OAUTHBEARER` in the listener's
    /// `sasl_mechanisms`. Defaults `true`; setting `false` keeps the
    /// listener anonymous-over-SASL but still validates tokens if any
    /// arrive via other mechanisms (rare; mirrors Strimzi).
    #[serde(default = "default_true", skip_serializing_if = "is_default_true")]
    pub enable_oauth_bearer: bool,
    /// Slice 50b: Strimzi-shaped list of `{secretName, certificate}`
    /// entries naming source Secrets (same namespace as the `Kafka`
    /// CR) whose listed PEM keys are concatenated into a managed
    /// Secret `{kafka.name}-oauth-jwks-trust` and mounted into broker
    /// pods. The broker reads the concatenated bundle at the path
    /// written into `[oauthbearer].idp_tls_trust` (slice 49c). Empty
    /// list (default) → no managed Secret, no mount, no TOML line.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_trusted_certificates: Vec<TlsTrustedCertificate>,
}

fn default_true() -> bool {
    true
}
// serde's `skip_serializing_if` predicate signature requires `&T`.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_true(b: &bool) -> bool {
    *b
}

/// Narrowed shape of Strimzi's `customClaimCheck`. Strimzi accepts a
/// JsonPath-ish expression language; slice 50 only honors
/// "<scopeClaim> contains <scope>" because that's what 49b's validator
/// implements. Wider expression support is deferred to slice 50f
/// (paired with broker slice 49g) in the umbrella roadmap.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OAuthCustomClaimCheck {
    /// Required scope value. The token's `scopeClaim` (default
    /// `scope`) must contain this value.
    pub scope: String,
    /// Override the claim the broker reads scopes from. Defaults to
    /// `scope`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_claim: Option<String>,
}

/// Slice 50b. One entry in
/// `ListenerAuthenticationOAuth.tls_trusted_certificates`. Names a
/// source `Secret` (in the same namespace as the `Kafka` CR) and the
/// key within that Secret whose value is a PEM-encoded CA certificate.
/// The operator concatenates all listed entries into a managed Secret
/// the broker mounts and reads as its OAUTHBEARER JWKS trust store.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsTrustedCertificate {
    pub secret_name: String,
    pub certificate: String,
}

fn listener_authentication_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["tls", "scram-sha-512", "scram-sha-256", "oauth"],
            },
            "validIssuerUri": { "type": "string", "minLength": 1 },
            "jwksEndpointUri": { "type": "string", "minLength": 1 },
            "validAudience": { "type": "string" },
            "userNameClaim": { "type": "string" },
            "customClaimCheck": {
                "type": "object",
                "required": ["scope"],
                "properties": {
                    "scope": { "type": "string", "minLength": 1 },
                    "scopeClaim": { "type": "string" },
                },
            },
            "jwksRefreshSeconds": { "type": "integer", "minimum": 0 },
            "maxClockSkewSeconds": { "type": "integer", "minimum": 0 },
            "enableOauthBearer": { "type": "boolean" },
            "tlsTrustedCertificates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["secretName", "certificate"],
                    "properties": {
                        "secretName": { "type": "string", "minLength": 1 },
                        "certificate": { "type": "string", "minLength": 1 },
                    },
                },
            },
        },
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// `host:port` clients should put in `bootstrap.servers`.
    pub bootstrap_servers: String,
    #[serde(default)]
    pub addresses: Vec<ListenerAddress>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAddress {
    pub host: String,
    pub port: i32,
}

#[cfg(test)]
mod auth_tests {
    use super::*;

    #[test]
    fn listener_deserializes_tls_authentication() {
        let l: Listener = serde_yaml::from_str(
            r"
name: mtls
port: 9095
type: internal
tls: true
authentication:
  type: tls
",
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::Tls));
    }

    #[test]
    fn listener_deserializes_scram_sha_512_authentication() {
        let l: Listener = serde_yaml::from_str(
            r"
name: scram
port: 9094
type: internal
tls: true
authentication:
  type: scram-sha-512
",
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::ScramSha512));
    }

    #[test]
    fn listener_deserializes_scram_sha_256_authentication() {
        let l: Listener = serde_yaml::from_str(
            r"
name: scram256
port: 9094
type: internal
tls: true
authentication:
  type: scram-sha-256
",
        )
        .unwrap();
        assert_eq!(l.authentication, Some(ListenerAuthentication::ScramSha256));
    }

    #[test]
    fn listener_deserializes_without_authentication() {
        let l: Listener = serde_yaml::from_str(
            r"
name: plain
port: 9092
type: internal
",
        )
        .unwrap();
        assert!(l.authentication.is_none());
    }

    #[test]
    fn unknown_authentication_type_rejected() {
        let err = serde_yaml::from_str::<Listener>(
            r"
name: bad
port: 9092
type: internal
authentication:
  type: kerberos
",
        )
        .err();
        assert!(
            err.is_some(),
            "unknown auth type should fail to deserialize"
        );
    }

    #[test]
    fn listener_deserializes_oauth_authentication_full_config() {
        let l: Listener = serde_yaml::from_str(
            r"
name: oauth
port: 9096
type: internal
tls: true
authentication:
  type: oauth
  validIssuerUri: https://kc.example.com/realms/kafka
  jwksEndpointUri: https://kc.example.com/realms/kafka/protocol/openid-connect/certs
  validAudience: kafka
  userNameClaim: preferred_username
  customClaimCheck:
    scope: kafka.write
    scopeClaim: scp
  jwksRefreshSeconds: 300
  maxClockSkewSeconds: 30
  enableOauthBearer: false
",
        )
        .unwrap();
        let expected = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://kc.example.com/realms/kafka".into(),
            jwks_endpoint_uri: "https://kc.example.com/realms/kafka/protocol/openid-connect/certs"
                .into(),
            valid_audience: Some("kafka".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: Some(OAuthCustomClaimCheck {
                scope: "kafka.write".into(),
                scope_claim: Some("scp".into()),
            }),
            jwks_refresh_seconds: Some(300),
            max_clock_skew_seconds: Some(30),
            enable_oauth_bearer: false,
            tls_trusted_certificates: vec![],
        };
        assert_eq!(
            l.authentication,
            Some(ListenerAuthentication::OAuth(expected))
        );
    }

    #[test]
    fn listener_deserializes_oauth_authentication_minimum_required() {
        let l: Listener = serde_yaml::from_str(
            r"
name: oauth-min
port: 9097
type: internal
tls: true
authentication:
  type: oauth
  validIssuerUri: https://issuer.example.com/
  jwksEndpointUri: https://issuer.example.com/jwks
",
        )
        .unwrap();
        let Some(ListenerAuthentication::OAuth(oauth)) = l.authentication else {
            panic!("expected OAuth authentication, got {:?}", l.authentication);
        };
        assert_eq!(oauth.valid_issuer_uri, "https://issuer.example.com/");
        assert_eq!(oauth.jwks_endpoint_uri, "https://issuer.example.com/jwks");
        assert_eq!(oauth.valid_audience, None);
        assert_eq!(oauth.user_name_claim, None);
        assert_eq!(oauth.custom_claim_check, None);
        assert_eq!(oauth.jwks_refresh_seconds, None);
        assert_eq!(oauth.max_clock_skew_seconds, None);
        // `enable_oauth_bearer` defaults to true when omitted.
        assert!(oauth.enable_oauth_bearer);
    }

    #[test]
    fn oauth_with_custom_claim_check_deserializes() {
        let l: Listener = serde_yaml::from_str(
            r"
name: oauth-scope
port: 9098
type: internal
tls: true
authentication:
  type: oauth
  validIssuerUri: https://issuer.example.com/
  jwksEndpointUri: https://issuer.example.com/jwks
  customClaimCheck:
    scope: kafka.write
",
        )
        .unwrap();
        let Some(ListenerAuthentication::OAuth(oauth)) = l.authentication else {
            panic!("expected OAuth authentication");
        };
        let ccc = oauth.custom_claim_check.expect("customClaimCheck present");
        assert_eq!(ccc.scope, "kafka.write");
        assert_eq!(ccc.scope_claim, None);
    }

    #[test]
    fn oauth_default_enable_omitted_on_serialize() {
        let auth = ListenerAuthentication::OAuth(ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: "https://issuer.example.com/jwks".into(),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
        });
        let json = serde_json::to_string(&auth).unwrap();
        assert!(
            !json.contains("enableOauthBearer"),
            "default-true enable_oauth_bearer must be omitted; got: {json}"
        );
    }

    #[test]
    fn oauth_enable_false_round_trips() {
        let auth = ListenerAuthentication::OAuth(ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: "https://issuer.example.com/jwks".into(),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: false,
            tls_trusted_certificates: vec![],
        });
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains("\"enableOauthBearer\":false"), "got: {json}");
        let back: ListenerAuthentication = serde_json::from_str(&json).unwrap();
        assert_eq!(back, auth);
    }

    #[test]
    fn oauth_unknown_subfield_silently_ignored() {
        // ListenerAuthenticationOAuth does not use `#[serde(deny_unknown_fields)]`,
        // so serde silently accepts unknown sibling keys. Apiserver-side
        // structural-schema rejection (preserveUnknownFields=false in the
        // generated CRD) is the wire-level guard against typos; this test
        // pins the Rust-side behavior so it doesn't change unnoticed.
        let l: Listener = serde_yaml::from_str(
            r#"
name: oauth-typo
port: 9099
type: internal
tls: true
authentication:
  type: oauth
  validIssuerUri: https://issuer.example.com/
  jwksEndpointUri: https://issuer.example.com/jwks
  madeUpField: "x"
"#,
        )
        .expect("serde should accept the unknown sibling field");
        let Some(ListenerAuthentication::OAuth(oauth)) = l.authentication else {
            panic!("expected OAuth authentication");
        };
        assert_eq!(oauth.valid_issuer_uri, "https://issuer.example.com/");
        assert_eq!(oauth.jwks_endpoint_uri, "https://issuer.example.com/jwks");
    }

    #[test]
    fn listener_authentication_schema_contains_oauth_discriminator_and_sibling_keys() {
        // Regression guard: if someone drops the
        // `#[schemars(schema_with = "listener_authentication_schema")]`
        // attribute on `ListenerAuthentication`, the hand-rolled flat
        // object schema goes away and we silently lose the `oauth`
        // discriminator + its sibling property keys. This test invokes
        // the schema function directly and pins both.
        let mut generator = schemars::SchemaGenerator::default();
        let schema = listener_authentication_schema(&mut generator);
        let v = serde_json::to_value(&schema).unwrap();

        // Discriminator enum contains all four variants.
        let type_enum = v
            .pointer("/properties/type/enum")
            .and_then(|x| x.as_array())
            .expect("schema must have properties.type.enum array");
        let names: Vec<&str> = type_enum.iter().filter_map(|x| x.as_str()).collect();
        for want in ["tls", "scram-sha-512", "scram-sha-256", "oauth"] {
            assert!(names.contains(&want), "missing {want} in {names:?}");
        }

        // OAuth sibling property keys are present at the top level
        // (the schema is a flat object with all variants' fields as
        // siblings of `type` — see the kube-rs 3.x oneOf workaround
        // comment on ListenerAuthentication).
        let props = v
            .pointer("/properties")
            .and_then(|x| x.as_object())
            .expect("schema must have properties object");
        for want in [
            "validIssuerUri",
            "jwksEndpointUri",
            "validAudience",
            "userNameClaim",
            "customClaimCheck",
            "jwksRefreshSeconds",
            "maxClockSkewSeconds",
            "enableOauthBearer",
            "tlsTrustedCertificates",
        ] {
            assert!(props.contains_key(want), "missing property {want}");
        }
    }

    #[test]
    fn oauth_with_tls_trusted_certificates_round_trips() {
        let original = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: "https://issuer.example.com/jwks".into(),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![
                TlsTrustedCertificate {
                    secret_name: "kc-ca".into(),
                    certificate: "ca.crt".into(),
                },
                TlsTrustedCertificate {
                    secret_name: "intermediate-ca".into(),
                    certificate: "tls.crt".into(),
                },
            ],
        };
        let json = serde_json::to_string(&original).unwrap();
        assert!(
            json.contains("\"tlsTrustedCertificates\":["),
            "expected tlsTrustedCertificates array in JSON; got: {json}"
        );
        let round_tripped: ListenerAuthenticationOAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, original);
    }

    #[test]
    fn oauth_tls_trusted_certificates_default_omitted_on_serialize() {
        let auth = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: "https://issuer.example.com/jwks".into(),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
        };
        let json = serde_json::to_string(&auth).unwrap();
        assert!(
            !json.contains("tlsTrustedCertificates"),
            "empty tls_trusted_certificates must be omitted; got: {json}"
        );
    }

    #[test]
    fn tls_trusted_certificate_required_fields_missing_rejected() {
        let missing_certificate = serde_yaml::from_str::<TlsTrustedCertificate>(r"secretName: foo");
        assert!(
            missing_certificate.is_err(),
            "entry without certificate must fail to deserialize"
        );
        let missing_secret_name =
            serde_yaml::from_str::<TlsTrustedCertificate>(r"certificate: bar");
        assert!(
            missing_secret_name.is_err(),
            "entry without secretName must fail to deserialize"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_listener_round_trips_through_json() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"type\":\"internal\""), "got: {json}");
        assert!(json.contains("\"port\":9092"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn nodeport_with_broker_overrides_round_trips() {
        let l = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    node_port: Some(32099),
                    ..Default::default()
                }),
                brokers: vec![BrokerOverride {
                    broker: 0,
                    advertised_host: Some("public.host".into()),
                    node_port: Some(32100),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(
            json.contains("\"advertisedHost\":\"public.host\""),
            "got: {json}"
        );
        assert!(json.contains("\"nodePort\":32100"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn camelcase_wire_shape() {
        let cfg = ListenerConfiguration {
            bootstrap: Some(BootstrapConfig {
                load_balancer_ip: Some("10.0.0.5".into()),
                ..Default::default()
            }),
            brokers: vec![],
            ingress_class: None,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains("\"loadBalancerIP\":\"10.0.0.5\""),
            "got: {json}"
        );
    }

    #[test]
    fn listener_without_peers_omits_field() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(!json.contains("networkPolicyPeers"), "got: {json}");
    }

    #[test]
    fn listener_with_empty_peers_round_trips() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: Some(vec![]),
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"networkPolicyPeers\":[]"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn listener_with_named_peer_round_trips() {
        use crate::crd::NetworkPolicyPeer;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        use std::collections::BTreeMap;

        let mut match_labels = BTreeMap::new();
        match_labels.insert("role".to_string(), "client".to_string());
        let peer = NetworkPolicyPeer {
            pod_selector: Some(LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            }),
            namespace_selector: None,
        };
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: Some(vec![peer]),
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"networkPolicyPeers\""), "got: {json}");
        assert!(
            json.contains("\"matchLabels\":{\"role\":\"client\"}"),
            "got: {json}"
        );
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }
}
