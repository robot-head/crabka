//! `Kafka.spec.listeners` schema. The schema is Strimzi-shaped.

use std::collections::BTreeMap;

use crabka_units::Time;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    /// Unique within the cluster. Alphanumeric characters and `-`, ≤25
    /// chars. It is the Kafka listener name, and it appears in
    /// `bootstrap.servers`-style URLs.
    pub name: String,
    /// Container port the broker binds. Unique within the cluster.
    pub port: i32,
    /// Listener type. `internal` is in-cluster. `nodeport` and
    /// `loadbalancer` create external Services. The schema accepts `ingress`
    /// and `route`, but the reconcile rejects them.
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// Transport-level TLS. When `true`, the listener uses the per-broker
    /// keystore that the cluster CA signed, and clients must use TLS to
    /// connect. This field is independent of `authentication`. A `tls: true`
    /// listener with no `authentication` is anonymous over TLS.
    #[serde(default)]
    pub tls: bool,
    /// Per-listener authentication mechanism. Absent means anonymous, and no
    /// client identity is required. When the field is `type: tls`, the
    /// listener must also have `tls: true`. The reconcile enforces this
    /// rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<ListenerAuthentication>,
    /// Optional listener-type-specific configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
    /// Per-listener peer allow-list. It is tri-state:
    /// - `None` gives no per-listener restriction, that is, allow-all on this
    ///   port.
    /// - `Some(vec![])` gives deny-all on this listener port. The operator
    ///   emits no per-listener rule, and the default-deny applies.
    /// - `Some(non_empty)` lets only the listed peers reach this port.
    ///
    /// The operator reads this field only when `Kafka.spec.networkPolicy` is
    /// set. In any other case the field is inert. The operator auto-allow rule
    /// still fires on this port for deny-all listeners, so that the operator
    /// can manage the cluster.
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
    /// `ingress` only: the `spec.ingressClassName` that the operator sets on
    /// every generated `Ingress`. This is the Strimzi-shaped
    /// `configuration.class`. It is inert for other listener types.
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
    /// `ingress` / `route` only: bootstrap hostname.
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
    /// `ingress` / `route` only: per-broker hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// Per-listener authentication mechanism.
///
/// The `schema_with` workaround avoids a kube-rs 3.x `StructuralSchemaRewriter`
/// panic when `oneOf` branches share a `type` discriminator with different
/// `enum` values. This is the same pattern as `Authentication` in `user.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
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
    OAuth(Box<ListenerAuthenticationOAuth>),
    #[serde(rename = "gssapi")]
    Gssapi(ListenerAuthenticationGssapi),
}

/// Config for `authentication: { type: oauth }` on a listener. The reconciler
/// renders these fields into the broker-global `[oauthbearer]` TOML block and
/// appends `OAUTHBEARER` to the listener's `sasl_mechanisms`. This narrows the
/// Strimzi surface to the fields that the JWT validator honors.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationOAuth {
    /// Expected `iss` claim. The broker rejects tokens with a different
    /// `iss`.
    pub valid_issuer_uri: String,
    /// JWKS endpoint URL (RFC 7517). Required when `accessTokenIsJwt: true`,
    /// which is the default. Rejected when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,
    /// Optional expected `aud` claim. Absent means no audience check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_audience: Option<String>,
    /// Claim name to use as the Kafka principal. The broker-side default is
    /// `sub`. Set it to `preferred_username` for Keycloak, for example.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name_claim: Option<String>,
    /// `JsonPath` expression that the broker evaluates against the token's
    /// claim set. The expression is RFC 9535, and jsonpath-rust evaluates it.
    /// The broker rejects the token when the expression yields empty, null, or
    /// false. Two examples in RFC 9535 syntax, which has no parens around the
    /// filter predicate:
    ///   `"$.scope[?@ == 'kafka.write']"`, where the `scope` claim is an array
    ///     that contains 'kafka.write'.
    ///   `"$[?@.aud == 'kafka-broker']"`, where the token's `aud` claim equals
    ///     'kafka-broker'.
    /// CRD-validated `minLength: 1` when set.
    ///
    /// Note: Strimzi uses Jayway `JsonPath` syntax, `$[?(@.x == 'y')]`. Crabka
    /// uses RFC 9535, `$[?@.x == 'y']`, which has no parens. Operators that
    /// migrate from Strimzi must rewrite their expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_claim_check: Option<String>,
    /// JWKS refresh cadence in seconds. The reconciler enforces `>= 30`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_refresh_seconds: Option<u32>,
    /// Allowed clock skew in seconds for `exp`/`nbf`/`iat` checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_clock_skew_seconds: Option<u32>,
    /// Whether to advertise `OAUTHBEARER` in the listener's
    /// `sasl_mechanisms`. Default: `true`. A value of `false` keeps the
    /// listener anonymous-over-SASL, but the broker still validates tokens
    /// that arrive through other mechanisms. This case is rare, and it is the
    /// same as Strimzi.
    #[serde(
        default = "default_true",
        skip_serializing_if = "std::clone::Clone::clone"
    )]
    pub enable_oauth_bearer: bool,
    /// Strimzi-shaped list of `{secretName, certificate}` entries. Each entry
    /// names a source Secret in the same namespace as the `Kafka` CR. The
    /// operator concatenates the listed PEM keys into a managed Secret
    /// `{kafka.name}-oauth-jwks-trust` and mounts it into the broker pods. The
    /// broker reads the concatenated bundle at the path that the operator
    /// writes into `[oauthbearer].idp_tls_trust`. An empty list, which is the
    /// default, gives no managed Secret, no mount, and no TOML line.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_trusted_certificates: Vec<TlsTrustedCertificate>,
    /// Strimzi-shape. When `true`, which is the default, the broker validates
    /// tokens as signed JWTs against `jwksEndpointUri`. When `false`, the
    /// broker calls `introspectionEndpointUri` for each token. This field
    /// drives the operator-side validation. See also the cross-mode rules in
    /// the listeners reconciler.
    #[serde(
        default = "default_true",
        skip_serializing_if = "std::clone::Clone::clone"
    )]
    pub access_token_is_jwt: bool,
    /// RFC 7662 introspection endpoint. Required when
    /// `accessTokenIsJwt: false`. Rejected when `accessTokenIsJwt: true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_endpoint_uri: Option<String>,
    /// Optional OIDC userinfo endpoint. It is permitted only with
    /// `accessTokenIsJwt: false`. When set, the broker calls userinfo after
    /// each successful introspection and merges the profile claims. This is
    /// userinfo enrichment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_info_endpoint_uri: Option<String>,
    /// HTTP Basic Auth `client_id` the broker uses against the
    /// introspection endpoint. Required when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Reference to a Kubernetes `Secret` in the same namespace that holds
    /// the client-secret material for the introspection endpoint's Basic Auth.
    /// The operator mounts the source Secret into the broker pod with a
    /// projected `items` mapping, so the broker reads from a fixed path for
    /// any user source key name. Required when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<OauthClientSecretRef>,
    /// Timeout in seconds for the introspection and userinfo HTTP requests.
    /// The operator converts it to ms for the broker TOML. The field is
    /// optional, and the broker default is 10 seconds. It is permitted only
    /// with `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_http_timeout_seconds: Option<u32>,
    /// Maximum SASL session lifetime in seconds before the broker forces a
    /// re-authentication through KIP-368. It is a ceiling on top of the
    /// token's `exp`. The effective session is
    /// `min(token_exp - now, maxSecondsWithoutReauthentication)`. When the
    /// field is unset, which is the default, sessions last until the token's
    /// natural `exp`. This is a Strimzi-shape field, CRD-validated
    /// `minimum: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_seconds_without_reauthentication: Option<u32>,
    /// When set, the JWT `typ` header must equal this string. The field is
    /// JWT-mode only. The operator rejects it with
    /// `ListenersValid=False reason=ListenerOauthValidTokenTypeRejectedInIntrospectionMode`
    /// when it is set on an `accessTokenIsJwt: false` listener, because
    /// introspection responses have no JWT header. CRD-validated
    /// `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_token_type: Option<String>,
    /// Alternate claim name for the principal-name fallback when
    /// `userNameClaim`, which defaults to `sub`, is absent or empty on the
    /// token. The Strimzi convention is `client_id` for Keycloak
    /// service-account tokens whose `sub` is a UUID. This is a flat claim name
    /// and NOT a `JsonPath`. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_claim: Option<String>,
    /// The operator prepends this string to the resolved principal name ONLY
    /// when the fallback claim fires. If the primary claim is present, there
    /// is no prefix. The Strimzi convention is `"service-account-"`, which
    /// namespaces the fallback-derived principals, so that ACLs can separate
    /// service accounts from human users. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_prefix: Option<String>,
    /// `JsonPath` expression that extracts group memberships from the token
    /// claims. The expression is RFC 9535, and jsonpath-rust evaluates it. Two
    /// examples are `"$.groups"`, a top-level array, and
    /// `"$.realm_access.roles[*]"`, the Keycloak realm-roles shape. When the
    /// path resolves to an array, each string element is a group. When the
    /// path resolves to a string and `groupsClaimDelimiter` is set, the broker
    /// splits the string. The operator attaches the result to the Kafka
    /// principal for broker-side authorization. CRD-validated `minLength: 1`.
    ///
    /// Note: Strimzi uses Jayway `JsonPath`, `$[?(@.x == 'y')]`. Crabka uses
    /// RFC 9535, `$[?@.x == 'y']`, which has no parens, because Crabka uses
    /// `jsonpath-rust`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim: Option<String>,
    /// Delimiter that splits the `groupsClaim` results when the claim
    /// resolves to a string, for example `","` or `" "`. The broker ignores it
    /// when the claim is an array. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim_delimiter: Option<String>,
    /// Minimum pause in seconds between the on-demand JWKS refreshes that
    /// tokens with an unknown `kid` trigger. When the broker receives a token
    /// whose `kid` is not in the cached JWKS, the broker triggers an immediate
    /// refresh. This field rate-limits those refreshes, so that a stream of
    /// bad tokens cannot overload the `IdP`. The Strimzi default is 1.
    /// CRD-validated `minimum: 0`, where 0 means no rate-limit. The field is
    /// JWT-mode only, and the operator rejects it when
    /// `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_min_refresh_pause_seconds: Option<u32>,
    /// Maximum age in seconds of the cached JWKS. After that age the
    /// validators reject tokens until the next successful refresh. This field
    /// is different from `jwksRefreshSeconds`, which is the periodic cadence.
    /// This field is the HARD expiry that fails closed if the `IdP` is
    /// unreachable for too long. The Strimzi default is 360, that is, 6
    /// minutes. CRD-validated `minimum: 1`. The field is JWT-mode only, and
    /// the operator rejects it when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expiry_seconds: Option<u32>,
    /// When true, the broker accepts JWKS keys with any `use` field value and
    /// not only `sig`. The default of false matches the Strimzi and JWS
    /// behavior, which filters out the encryption-only keys. Set it to true
    /// for `IdPs` that mis-tag their signing keys. The field is JWT-mode only,
    /// and the operator rejects it when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_ignore_key_use: Option<bool>,
}

fn default_true() -> bool {
    true
}

/// One entry in `ListenerAuthenticationOAuth.tls_trusted_certificates`. It
/// names a source `Secret` in the same namespace as the `Kafka` CR, and the
/// key within that Secret whose value is a PEM-encoded CA certificate. The
/// operator concatenates all listed entries into a managed Secret. The broker
/// mounts that Secret and reads it as its OAUTHBEARER JWKS trust store.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsTrustedCertificate {
    pub secret_name: String,
    pub certificate: String,
}

/// Strimzi-shape Secret reference for the OAUTHBEARER
/// introspection client secret. The source Secret must exist in
/// the same namespace as the `Kafka` CR.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OauthClientSecretRef {
    /// Name of the Kubernetes Secret holding the client secret.
    pub secret_name: String,
    /// Key within the Secret whose value is the client-secret material. The
    /// operator mounts this key as a file at a fixed path inside the broker
    /// pod. The projected `items` hide the user's key name from the broker.
    pub key: String,
}

/// Config for `authentication: { type: gssapi }`. It has full parity with the
/// broker's `GssapiConfig`. The reconciler renders these fields into the
/// broker-global `[gssapi]` TOML block and appends `GSSAPI` to the listener's
/// `sasl_mechanisms`. `[gssapi]` is broker-global, so all GSSAPI listeners on
/// a cluster must agree.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationGssapi {
    /// Secret in the same namespace as the `Kafka` CR that holds the service
    /// keytab. The operator mounts it into the broker pods at a fixed path
    /// with projected items.
    pub keytab_secret_ref: KeytabSecretRef,
    /// `sasl.kerberos.service.name`, the SPN primary. Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs. The broker applies them in order, and the
    /// first match wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm. The broker uses it when a principal omits its
    /// realm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    /// KDC endpoint for the initiate path, for example `tcp://kdc:88`. When
    /// omitted, the broker falls back to krb5.conf discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kdc: Option<String>,
    /// Maximum tolerated difference between client and broker clocks.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub max_time_skew: Option<Time>,
}

/// Reference to a Secret in the same namespace as the `Kafka` CR that holds a
/// Kerberos keytab. The operator mounts `key` at a fixed in-pod path, so the
/// broker reads it for any user key name.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeytabSecretRef {
    /// Name of the Secret holding the keytab.
    pub secret_name: String,
    /// Key within the Secret whose value is the keytab bytes. The operator
    /// mounts it at a fixed in-pod path for any value of this key name.
    pub key: String,
}

fn listener_authentication_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["tls", "scram-sha-512", "scram-sha-256", "oauth", "gssapi"],
            },
            "validIssuerUri": { "type": "string", "minLength": 1 },
            "jwksEndpointUri": { "type": "string", "minLength": 1 },
            "validAudience": { "type": "string" },
            "userNameClaim": { "type": "string" },
            "customClaimCheck": { "type": "string", "minLength": 1 },
            "validTokenType": { "type": "string", "minLength": 1 },
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
            "accessTokenIsJwt": { "type": "boolean" },
            "introspectionEndpointUri": { "type": "string", "minLength": 1 },
            "userInfoEndpointUri": { "type": "string", "minLength": 1 },
            "clientId": { "type": "string", "minLength": 1 },
            "clientSecret": {
                "type": "object",
                "required": ["secretName", "key"],
                "properties": {
                    "secretName": { "type": "string", "minLength": 1 },
                    "key":        { "type": "string", "minLength": 1 },
                },
            },
            "introspectionHttpTimeoutSeconds": { "type": "integer", "minimum": 1 },
            "maxSecondsWithoutReauthentication": { "type": "integer", "format": "int32", "minimum": 1 },
            "fallbackUserNameClaim":  { "type": "string", "minLength": 1 },
            "fallbackUserNamePrefix": { "type": "string", "minLength": 1 },
            "groupsClaim":            { "type": "string", "minLength": 1 },
            "groupsClaimDelimiter":   { "type": "string", "minLength": 1 },
            "jwksMinRefreshPauseSeconds": { "type": "integer", "format": "int32", "minimum": 0 },
            "jwksExpirySeconds":          { "type": "integer", "format": "int32", "minimum": 1 },
            "jwksIgnoreKeyUse":           { "type": "boolean" },
            "keytabSecretRef": {
                "type": "object",
                "required": ["secretName", "key"],
                "properties": {
                    "secretName": { "type": "string", "minLength": 1 },
                    "key":        { "type": "string", "minLength": 1 },
                },
            },
            "serviceName": { "type": "string", "minLength": 1 },
            "principalToLocalRules": {
                "type": "array",
                "items": { "type": "string", "minLength": 1 },
            },
            "realm": { "type": "string", "minLength": 1 },
            "kdc": { "type": "string", "minLength": 1 },
            "maxTimeSkew": { "type": "string", "minLength": 1 },
        },
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// `host:port` that clients put in `bootstrap.servers`.
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
    use assert2::assert;

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
        assert!(l.authentication == Some(ListenerAuthentication::Tls));
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
        assert!(l.authentication == Some(ListenerAuthentication::ScramSha512));
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
        assert!(l.authentication == Some(ListenerAuthentication::ScramSha256));
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
            r#"
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
  customClaimCheck: "$.scope[?@ == 'kafka.write']"
  jwksRefreshSeconds: 300
  maxClockSkewSeconds: 30
  enableOauthBearer: false
"#,
        )
        .unwrap();
        let expected = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://kc.example.com/realms/kafka".into(),
            jwks_endpoint_uri: Some(
                "https://kc.example.com/realms/kafka/protocol/openid-connect/certs".into(),
            ),
            valid_audience: Some("kafka".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
            jwks_refresh_seconds: Some(300),
            max_clock_skew_seconds: Some(30),
            enable_oauth_bearer: false,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        assert!(l.authentication == Some(ListenerAuthentication::OAuth(Box::new(expected))));
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
        // `enable_oauth_bearer` defaults to true when omitted.
        let expected = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        assert!(*oauth == expected);
    }

    #[test]
    fn oauth_with_custom_claim_check_deserializes() {
        let l: Listener = serde_yaml::from_str(
            r#"
name: oauth-scope
port: 9098
type: internal
tls: true
authentication:
  type: oauth
  validIssuerUri: https://issuer.example.com/
  jwksEndpointUri: https://issuer.example.com/jwks
  customClaimCheck: "$.scope[?@ == 'kafka.write']"
"#,
        )
        .unwrap();
        let Some(ListenerAuthentication::OAuth(oauth)) = l.authentication else {
            panic!("expected OAuth authentication");
        };
        assert!(oauth.custom_claim_check.as_deref() == Some("$.scope[?@ == 'kafka.write']"));
    }

    #[test]
    fn oauth_default_enable_omitted_on_serialize() {
        let auth = ListenerAuthentication::OAuth(Box::new(ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        }));
        let json = serde_json::to_string(&auth).unwrap();
        assert!(
            !json.contains("enableOauthBearer"),
            "default-true enable_oauth_bearer must be omitted; got: {json}"
        );
    }

    #[test]
    fn oauth_enable_false_round_trips() {
        let auth = ListenerAuthentication::OAuth(Box::new(ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: false,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        }));
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains("\"enableOauthBearer\":false"), "got: {json}");
        let back: ListenerAuthentication = serde_json::from_str(&json).unwrap();
        assert!(back == auth);
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
        assert!(oauth.valid_issuer_uri == "https://issuer.example.com/");
        assert!(oauth.jwks_endpoint_uri.as_deref() == Some("https://issuer.example.com/jwks"));
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

        // Discriminator enum contains all five variants.
        let type_enum = v
            .pointer("/properties/type/enum")
            .and_then(|x| x.as_array())
            .expect("schema must have properties.type.enum array");
        let names: Vec<&str> = type_enum.iter().filter_map(|x| x.as_str()).collect();
        for want in ["tls", "scram-sha-512", "scram-sha-256", "oauth", "gssapi"] {
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
            "accessTokenIsJwt",
            "introspectionEndpointUri",
            "userInfoEndpointUri",
            "clientId",
            "clientSecret",
            "introspectionHttpTimeoutSeconds",
            "maxSecondsWithoutReauthentication",
            "validTokenType",
            "fallbackUserNameClaim",
            "fallbackUserNamePrefix",
            "groupsClaim",
            "groupsClaimDelimiter",
            "jwksMinRefreshPauseSeconds",
            "jwksExpirySeconds",
            "jwksIgnoreKeyUse",
            "keytabSecretRef",
        ] {
            assert!(props.contains_key(want), "missing property {want}");
        }

        // customClaimCheck is a string (not an object).
        let ccc = v
            .pointer("/properties/customClaimCheck")
            .expect("customClaimCheck must be present");
        assert!(
            ccc.pointer("/type").and_then(|x| x.as_str()) == Some("string"),
            "customClaimCheck must be a string; got: {ccc}"
        );

        // validTokenType is also a string with minLength 1.
        let vtt = v
            .pointer("/properties/validTokenType")
            .expect("validTokenType must be present");
        assert!(
            vtt.pointer("/type").and_then(|x| x.as_str()) == Some("string"),
            "validTokenType must be a string; got: {vtt}"
        );
    }

    #[test]
    fn oauth_with_tls_trusted_certificates_round_trips() {
        let original = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
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
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        assert!(
            json.contains("\"tlsTrustedCertificates\":["),
            "expected tlsTrustedCertificates array in JSON; got: {json}"
        );
        let round_tripped: ListenerAuthenticationOAuth = serde_json::from_str(&json).unwrap();
        assert!(round_tripped == original);
    }

    #[test]
    fn oauth_tls_trusted_certificates_default_omitted_on_serialize() {
        let auth = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
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

    #[test]
    fn oauth_with_access_token_is_jwt_false_introspection_round_trips() {
        let original = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://issuer.example.com/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(OauthClientSecretRef {
                secret_name: "kafka-broker-oauth".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        for want in [
            "\"accessTokenIsJwt\":false",
            "\"introspectionEndpointUri\":\"https://issuer.example.com/introspect\"",
            "\"clientId\":\"kafka-broker\"",
            "\"clientSecret\":{\"secretName\":\"kafka-broker-oauth\",\"key\":\"client-secret\"}",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let round_tripped: ListenerAuthenticationOAuth = serde_json::from_str(&json).unwrap();
        assert!(round_tripped == original);
    }

    #[test]
    fn oauth_access_token_is_jwt_default_omitted_on_serialize() {
        let auth = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let json = serde_json::to_string(&auth).unwrap();
        assert!(
            !json.contains("accessTokenIsJwt"),
            "default-true access_token_is_jwt must be omitted; got: {json}"
        );
    }

    #[test]
    fn oauth_client_secret_round_trips() {
        let original = OauthClientSecretRef {
            secret_name: "my-secret".into(),
            key: "client-secret".into(),
        };
        let json = serde_json::to_string(&original).unwrap();
        assert!(json == r#"{"secretName":"my-secret","key":"client-secret"}"#);
        let round_tripped: OauthClientSecretRef = serde_json::from_str(&json).unwrap();
        assert!(round_tripped == original);
    }

    #[test]
    fn oauth_jwks_endpoint_uri_now_optional_omits_when_none() {
        let auth = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://issuer.example.com/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(OauthClientSecretRef {
                secret_name: "kafka-broker-oauth".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let json = serde_json::to_string(&auth).unwrap();
        assert!(
            !json.contains("jwksEndpointUri"),
            "None jwks_endpoint_uri must be omitted from JSON; got: {json}"
        );
    }

    #[test]
    fn oauth_with_userinfo_endpoint_round_trips() {
        let original = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://issuer.example.com/introspect".into()),
            user_info_endpoint_uri: Some("https://idp.example/userinfo".into()),
            client_id: Some("kafka-broker".into()),
            client_secret: Some(OauthClientSecretRef {
                secret_name: "kafka-broker-oauth".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        assert!(
            json.contains("\"userInfoEndpointUri\":\"https://idp.example/userinfo\""),
            "expected userInfoEndpointUri in JSON; got: {json}"
        );
        let round_tripped: ListenerAuthenticationOAuth = serde_json::from_str(&json).unwrap();
        assert!(round_tripped == original);
    }

    #[test]
    fn oauth_round_trip_with_max_seconds_without_reauthentication() {
        let yaml = r"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
maxSecondsWithoutReauthentication: 300
";
        let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
        let ListenerAuthentication::OAuth(oauth) = &parsed else {
            panic!("expected oauth variant");
        };
        assert!(oauth.max_seconds_without_reauthentication == Some(300));
    }

    #[test]
    fn oauth_round_trip_without_max_seconds_without_reauthentication_omits_field() {
        let cfg = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let auth = ListenerAuthentication::OAuth(Box::new(cfg));
        let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
        assert!(
            !yaml.contains("maxSecondsWithoutReauthentication"),
            "None field must be omitted from YAML; got:\n{yaml}"
        );
    }

    #[test]
    fn oauth_round_trip_with_custom_claim_check_string() {
        let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
customClaimCheck: "$.scope[?@ == 'kafka.write']"
validTokenType: JWT
"#;
        let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
        let ListenerAuthentication::OAuth(oauth) = &parsed else {
            panic!("expected oauth variant");
        };
        assert!(oauth.custom_claim_check.as_deref() == Some("$.scope[?@ == 'kafka.write']"));
        assert!(oauth.valid_token_type.as_deref() == Some("JWT"));
    }

    #[test]
    fn oauth_round_trip_without_custom_claim_check_and_valid_token_type_omits_both() {
        let cfg = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let auth = ListenerAuthentication::OAuth(Box::new(cfg));
        let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
        assert!(
            !yaml.contains("customClaimCheck"),
            "None field must be omitted; got:\n{yaml}"
        );
        assert!(
            !yaml.contains("validTokenType"),
            "None field must be omitted; got:\n{yaml}"
        );
    }

    #[test]
    fn oauth_old_custom_claim_check_object_shape_no_longer_parses() {
        // The legacy object shape `{ scope: ... }` is gone.
        let yaml = r"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
customClaimCheck:
  scope: kafka.write
";
        let result: Result<ListenerAuthentication, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "old object shape must be rejected; got Ok");
    }

    #[test]
    fn oauth_round_trip_with_claims_mapping_fields() {
        let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
fallbackUserNameClaim: client_id
fallbackUserNamePrefix: "service-account-"
groupsClaim: "$.realm_access.roles[*]"
groupsClaimDelimiter: ","
"#;
        let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
        let ListenerAuthentication::OAuth(oauth) = &parsed else {
            panic!("expected oauth variant");
        };
        let expected = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: Some("client_id".into()),
            fallback_user_name_prefix: Some("service-account-".into()),
            groups_claim: Some("$.realm_access.roles[*]".into()),
            groups_claim_delimiter: Some(",".into()),
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        assert!(oauth.as_ref() == &expected);
    }

    #[test]
    fn oauth_round_trip_without_claims_mapping_fields_omits_them() {
        let cfg = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let auth = ListenerAuthentication::OAuth(Box::new(cfg));
        let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
        for key in [
            "fallbackUserNameClaim",
            "fallbackUserNamePrefix",
            "groupsClaim",
            "groupsClaimDelimiter",
        ] {
            assert!(!yaml.contains(key), "{key} must be omitted; got:\n{yaml}");
        }
    }

    #[test]
    fn oauth_round_trip_with_jwks_policy_fields() {
        let yaml = r"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
jwksMinRefreshPauseSeconds: 1
jwksExpirySeconds: 3600
jwksIgnoreKeyUse: false
";
        let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
        let ListenerAuthentication::OAuth(oauth) = &parsed else {
            panic!("expected oauth variant");
        };
        let expected = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: Some(1),
            jwks_expiry_seconds: Some(3600),
            jwks_ignore_key_use: Some(false),
        };
        assert!(oauth.as_ref() == &expected);
    }

    #[test]
    fn oauth_round_trip_without_jwks_policy_fields_omits_them() {
        let cfg = ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example/".into(),
            jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let auth = ListenerAuthentication::OAuth(Box::new(cfg));
        let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
        for key in [
            "jwksMinRefreshPauseSeconds",
            "jwksExpirySeconds",
            "jwksIgnoreKeyUse",
        ] {
            assert!(!yaml.contains(key), "{key} must be omitted; got:\n{yaml}");
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn gssapi_clock_skew_round_trips_as_unit_bearing_string() {
        let listener: Listener = serde_json::from_value(serde_json::json!({
            "name": "gss",
            "port": 9092,
            "type": "internal",
            "authentication": {
                "type": "gssapi",
                "keytabSecretRef": { "secretName": "kt", "key": "keytab" },
                "maxTimeSkew": "17s"
            }
        }))
        .unwrap();
        let ListenerAuthentication::Gssapi(gssapi) = listener.authentication.unwrap() else {
            panic!("expected gssapi authentication");
        };
        assert!(gssapi.max_time_skew == Some(crabka_units::secs(17)));

        let schema = serde_json::to_string(&schemars::schema_for!(Listener)).unwrap();
        assert!(schema.contains("maxTimeSkew"));
    }

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
        assert!(back == l);
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
        assert!(back == l);
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
        assert!(back == l);
    }

    #[test]
    fn listener_with_named_peer_round_trips() {
        use std::collections::BTreeMap;

        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

        use crate::crd::NetworkPolicyPeer;

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
        assert!(back == l);
    }
}
