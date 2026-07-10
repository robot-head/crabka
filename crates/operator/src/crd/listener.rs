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
    /// accepted by the schema but rejected at reconcile.
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// Transport-level TLS. When `true`, the listener uses the per-broker
    /// keystore signed by the cluster CA and clients must speak
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
    /// Per-listener peer allow-list. Tri-state:
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
    /// `ingress` only: the `spec.ingressClassName` set on every
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
/// panic when `oneOf` branches share a `type` discriminator with differing `enum`
/// values — same pattern as `Authentication` in `user.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type")]
#[schemars(schema_with = "listener_authentication_schema")]
// Operator state, not a hot-path enum — a few KB per ListenerAuthentication is fine.
// Boxing the OAuth variant would cascade through every match site for no measurable
// benefit (a handful of these per Kafka CR).
#[allow(clippy::large_enum_variant)]
pub enum ListenerAuthentication {
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "scram-sha-512")]
    ScramSha512,
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    #[serde(rename = "oauth")]
    OAuth(ListenerAuthenticationOAuth),
    #[serde(rename = "gssapi")]
    Gssapi(ListenerAuthenticationGssapi),
}

/// Config for `authentication: { type: oauth }` on a listener. The
/// reconciler renders these into the broker-global
/// `[oauthbearer]` TOML block and appends `OAUTHBEARER` to the
/// listener's `sasl_mechanisms`. This narrows Strimzi's surface to
/// the fields the JWT validator honors.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationOAuth {
    /// Expected `iss` claim. Broker rejects tokens whose `iss` differs.
    pub valid_issuer_uri: String,
    /// JWKS endpoint URL (RFC 7517). Required when
    /// `accessTokenIsJwt: true` (the default); rejected when
    /// `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_endpoint_uri: Option<String>,
    /// Optional expected `aud` claim. Absent means no audience check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_audience: Option<String>,
    /// Claim name to use as the Kafka principal. Defaults broker-side
    /// to `sub`; set e.g. to `preferred_username` for Keycloak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name_claim: Option<String>,
    /// `JsonPath` expression
    /// (RFC 9535 via jsonpath-rust) evaluated against the token's claim
    /// set. Token is rejected when the expression yields empty/null/false.
    /// Examples (RFC 9535 syntax — note no parens around filter predicate):
    ///   `"$.scope[?@ == 'kafka.write']"` — `scope` claim is an array
    ///     containing 'kafka.write'.
    ///   `"$[?@.aud == 'kafka-broker']"` — token's `aud` claim equals
    ///     'kafka-broker'.
    /// CRD-validated `minLength: 1` when set.
    ///
    /// Note: Strimzi uses Jayway `JsonPath` syntax (`$[?(@.x == 'y')]`); Crabka
    /// uses RFC 9535 (`$[?@.x == 'y']` — no parens). Operators migrating
    /// from Strimzi rewrite expressions accordingly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_claim_check: Option<String>,
    /// JWKS refresh cadence in seconds. The reconciler enforces `>= 30`.
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
    /// Strimzi-shaped list of `{secretName, certificate}`
    /// entries naming source Secrets (same namespace as the `Kafka`
    /// CR) whose listed PEM keys are concatenated into a managed
    /// Secret `{kafka.name}-oauth-jwks-trust` and mounted into broker
    /// pods. The broker reads the concatenated bundle at the path
    /// written into `[oauthbearer].idp_tls_trust`. Empty
    /// list (default) → no managed Secret, no mount, no TOML line.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_trusted_certificates: Vec<TlsTrustedCertificate>,
    /// Strimzi-shape: when `true` (default), the broker validates
    /// tokens as signed JWTs against `jwksEndpointUri`.
    /// When `false`, the broker calls `introspectionEndpointUri` for
    /// each token. Drives operator-side validation: see
    /// also the cross-mode rules in the listeners reconciler.
    #[serde(default = "default_true", skip_serializing_if = "is_default_true")]
    pub access_token_is_jwt: bool,
    /// RFC 7662 introspection endpoint. Required when
    /// `accessTokenIsJwt: false`; rejected when `accessTokenIsJwt: true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_endpoint_uri: Option<String>,
    /// Optional OIDC userinfo endpoint. Permitted only with
    /// `accessTokenIsJwt: false`. When set, the broker calls userinfo
    /// after each successful introspection and merges the profile
    /// claims (userinfo enrichment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_info_endpoint_uri: Option<String>,
    /// HTTP Basic Auth `client_id` the broker uses against the
    /// introspection endpoint. Required when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Reference to a Kubernetes `Secret` in the same namespace
    /// holding the client-secret material for the introspection
    /// endpoint's Basic Auth. The operator mounts the source Secret
    /// directly into the broker pod with a projected `items` mapping
    /// so the broker reads from a fixed path regardless of the
    /// user's source key name. Required when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<OauthClientSecretRef>,
    /// Timeout for the introspection (and userinfo) HTTP
    /// requests, in seconds. Operator converts to ms for the broker
    /// TOML. Optional; broker default is 10 seconds. Permitted only
    /// with `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introspection_http_timeout_seconds: Option<u32>,
    /// Maximum SASL session lifetime (seconds) before the
    /// broker forces re-authentication via KIP-368. Acts as a ceiling on
    /// top of the token's `exp` — the effective session is
    /// `min(token_exp - now, maxSecondsWithoutReauthentication)`. When
    /// unset (the default), sessions last until the token's natural
    /// `exp`. Strimzi-shape field;
    /// CRD-validated `minimum: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_seconds_without_reauthentication: Option<u32>,
    /// When set, the JWT `typ` header must equal this
    /// string. JWT-mode only — rejected with
    /// `ListenersValid=False reason=ListenerOauthValidTokenTypeRejectedInIntrospectionMode`
    /// when set on an `accessTokenIsJwt: false` listener (no JWT
    /// header in introspection responses). CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_token_type: Option<String>,
    /// Alternate claim name for principal-name fallback when
    /// `userNameClaim` (default `sub`) is absent/empty on the token.
    /// Strimzi convention: `client_id` for Keycloak service-account
    /// tokens whose `sub` is a UUID. Flat claim name, NOT `JsonPath`.
    /// CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_claim: Option<String>,
    /// Prepended to the resolved principal name ONLY when
    /// the fallback claim fires (primary present → no prefix). Strimzi
    /// convention: `"service-account-"` to namespace
    /// fallback-derived principals so ACLs can distinguish service
    /// accounts from human users. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_prefix: Option<String>,
    /// `JsonPath` expression (RFC 9535 via jsonpath-rust)
    /// extracting group memberships from token claims. Examples:
    /// `"$.groups"` (top-level array), `"$.realm_access.roles[*]"`
    /// (Keycloak realm-roles shape). When the path resolves to an
    /// array, each string element is a group; when it resolves to a
    /// string and `groupsClaimDelimiter` is set, the string is split.
    /// Result attached to the Kafka principal for broker-side authorization.
    /// CRD-validated `minLength: 1`.
    ///
    /// Note: Strimzi uses Jayway `JsonPath` (`$[?(@.x == 'y')]`); Crabka
    /// uses RFC 9535 (`$[?@.x == 'y']` — no parens) per the
    /// choice of `jsonpath-rust`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim: Option<String>,
    /// Delimiter to split `groupsClaim` results when the
    /// claim resolves to a string (e.g., `","` or `" "`). Ignored
    /// when the claim is an array. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim_delimiter: Option<String>,
    /// Minimum pause (seconds) between on-demand JWKS
    /// refreshes triggered by tokens with unknown `kid`. When the
    /// broker receives a token whose `kid` isn't in the cached
    /// JWKS, it triggers an immediate refresh; this field
    /// rate-limits to protect the `IdP` from being hammered by a
    /// stream of bad tokens. Strimzi default: 1. CRD-validated
    /// `minimum: 0` (0 = no rate-limit). JWT-mode only — rejected
    /// when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_min_refresh_pause_seconds: Option<u32>,
    /// Maximum age (seconds) of the cached JWKS before
    /// validators reject tokens until next successful refresh.
    /// Distinct from `jwksRefreshSeconds` (the periodic cadence) —
    /// this is the HARD expiry that fails closed if the `IdP` is
    /// unreachable for too long. Strimzi default: 360 (6 minutes).
    /// CRD-validated `minimum: 1`. JWT-mode only — rejected when
    /// `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expiry_seconds: Option<u32>,
    /// When true, accept JWKS keys with any `use` field
    /// value (not just `sig`). Default false matches Strimzi/JWS
    /// behavior of filtering out encryption-only keys. Set true for
    /// `IdPs` that mis-tag their signing keys. JWT-mode only —
    /// rejected when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_ignore_key_use: Option<bool>,
}

fn default_true() -> bool {
    true
}
// serde's `skip_serializing_if` predicate signature requires `&T`.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_true(b: &bool) -> bool {
    *b
}

/// One entry in
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

/// Strimzi-shape Secret reference for the OAUTHBEARER
/// introspection client secret. The source Secret must exist in
/// the same namespace as the `Kafka` CR.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OauthClientSecretRef {
    /// Name of the Kubernetes Secret holding the client secret.
    pub secret_name: String,
    /// Key within the Secret whose value is the client-secret
    /// material. The operator mounts this key as a file at a fixed
    /// path inside the broker pod (the user's key name is hidden
    /// from the broker via projected `items`).
    pub key: String,
}

/// Config for `authentication: { type: gssapi }`. Full parity with the
/// broker's `GssapiConfig`. The reconciler renders these into the
/// broker-global `[gssapi]` TOML block and appends `GSSAPI` to the
/// listener's `sasl_mechanisms`. `[gssapi]` is broker-global, so all
/// GSSAPI listeners on a cluster must agree.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAuthenticationGssapi {
    /// Secret (same namespace as the `Kafka` CR) holding the service
    /// keytab. Mounted into broker pods at a fixed path via projected items.
    pub keytab_secret_ref: KeytabSecretRef,
    /// `sasl.kerberos.service.name` (the SPN primary). Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order; first match wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm (used when a principal omits its realm).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    /// KDC endpoint (e.g. `tcp://kdc:88`) for the initiate path; falls
    /// back to krb5.conf discovery when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kdc: Option<String>,
}

/// Reference to a Secret (same namespace as the `Kafka` CR) holding a
/// Kerberos keytab. The operator mounts `key` at a fixed in-pod path so
/// the broker reads it regardless of the user's key name.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeytabSecretRef {
    /// Name of the Secret holding the keytab.
    pub secret_name: String,
    /// Key within the Secret whose value is the keytab bytes. Mounted at a fixed in-pod path regardless of this key name.
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
    use assert2::assert;

    use super::*;

    fn minimal_oauth() -> ListenerAuthenticationOAuth {
        ListenerAuthenticationOAuth {
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
        }
    }

    #[test]
    fn listener_authentication_yaml_cases() {
        for (name, yaml, expected) in [
            (
                "TLS",
                "name: mtls\nport: 9095\ntype: internal\ntls: true\nauthentication:\n  type: tls\n",
                Some(ListenerAuthentication::Tls),
            ),
            (
                "SCRAM-SHA-512",
                "name: scram\nport: 9094\ntype: internal\ntls: true\nauthentication:\n  type: scram-sha-512\n",
                Some(ListenerAuthentication::ScramSha512),
            ),
            (
                "SCRAM-SHA-256",
                "name: scram\nport: 9094\ntype: internal\ntls: true\nauthentication:\n  type: scram-sha-256\n",
                Some(ListenerAuthentication::ScramSha256),
            ),
            (
                "no authentication",
                "name: plain\nport: 9092\ntype: internal\n",
                None,
            ),
        ] {
            let listener: Listener = serde_yaml::from_str(yaml).unwrap();
            assert_eq!(listener.authentication, expected, "case {name}");
        }
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
    fn listener_oauth_yaml_cases() {
        for (name, yaml, expected) in [
            (
                "full OAuth config",
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
                ListenerAuthenticationOAuth {
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
                },
            ),
            (
                "minimum OAuth config",
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
                ListenerAuthenticationOAuth {
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
                },
            ),
            (
                "custom claim check",
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
                ListenerAuthenticationOAuth {
                    custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
                    ..minimal_oauth()
                },
            ),
        ] {
            let listener: Listener = serde_yaml::from_str(yaml)
                .unwrap_or_else(|error| panic!("case {name}: YAML must parse: {error}"));
            assert_eq!(
                listener.authentication,
                Some(ListenerAuthentication::OAuth(expected)),
                "case {name}"
            );
        }
    }

    #[test]
    fn oauth_default_fields_omitted_on_serialize() {
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
        for field in [
            "enableOauthBearer",
            "tlsTrustedCertificates",
            "accessTokenIsJwt",
        ] {
            assert!(!json.contains(field), "case {field:?}; got: {json}");
        }
    }

    #[test]
    fn oauth_json_round_trip_cases() {
        let introspection = ListenerAuthenticationOAuth {
            jwks_endpoint_uri: None,
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://issuer.example.com/introspect".into()),
            client_id: Some("kafka-broker".into()),
            client_secret: Some(OauthClientSecretRef {
                secret_name: "kafka-broker-oauth".into(),
                key: "client-secret".into(),
            }),
            ..minimal_oauth()
        };
        for (name, config, present, absent) in [
            (
                "OAuth bearer disabled",
                ListenerAuthenticationOAuth {
                    enable_oauth_bearer: false,
                    ..minimal_oauth()
                },
                &["\"enableOauthBearer\":false"][..],
                &[][..],
            ),
            (
                "trusted certificates",
                ListenerAuthenticationOAuth {
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
                    ..minimal_oauth()
                },
                &["\"tlsTrustedCertificates\":["][..],
                &[][..],
            ),
            (
                "introspection mode",
                introspection.clone(),
                &[
                    "\"accessTokenIsJwt\":false",
                    "\"introspectionEndpointUri\":\"https://issuer.example.com/introspect\"",
                    "\"clientId\":\"kafka-broker\"",
                    "\"clientSecret\":{\"secretName\":\"kafka-broker-oauth\",\"key\":\"client-secret\"}",
                ][..],
                &["jwksEndpointUri"][..],
            ),
            (
                "userinfo endpoint",
                ListenerAuthenticationOAuth {
                    user_info_endpoint_uri: Some("https://idp.example/userinfo".into()),
                    ..introspection
                },
                &["\"userInfoEndpointUri\":\"https://idp.example/userinfo\""][..],
                &["jwksEndpointUri"][..],
            ),
        ] {
            let authentication = ListenerAuthentication::OAuth(config);
            let json = serde_json::to_string(&authentication).unwrap();
            for fragment in present {
                assert!(
                    json.contains(fragment),
                    "case {name}: missing {fragment:?}; {json}"
                );
            }
            for fragment in absent {
                assert!(
                    !json.contains(fragment),
                    "case {name}: found {fragment:?}; {json}"
                );
            }
            let decoded: ListenerAuthentication = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, authentication, "case {name}");
        }
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
        assert_eq!(
            oauth.valid_issuer_uri.as_str(),
            "https://issuer.example.com/"
        );
        assert_eq!(
            oauth.jwks_endpoint_uri.as_deref(),
            Some("https://issuer.example.com/jwks")
        );
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
    fn tls_trusted_certificate_required_fields_missing_rejected() {
        for (name, yaml) in [
            ("missing certificate", r"secretName: foo"),
            ("missing secret name", r"certificate: bar"),
        ] {
            assert!(
                serde_yaml::from_str::<TlsTrustedCertificate>(yaml).is_err(),
                "case {name}"
            );
        }
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
    fn oauth_optional_fields_are_omitted_when_unset() {
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
        let auth = ListenerAuthentication::OAuth(cfg);
        let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
        for field in [
            "maxSecondsWithoutReauthentication",
            "customClaimCheck",
            "validTokenType",
            "fallbackUserNameClaim",
            "fallbackUserNamePrefix",
            "groupsClaim",
            "groupsClaimDelimiter",
            "jwksMinRefreshPauseSeconds",
            "jwksExpirySeconds",
            "jwksIgnoreKeyUse",
        ] {
            assert!(!yaml.contains(field), "case {field:?}; got:\n{yaml}");
        }
    }

    #[test]
    fn oauth_old_custom_claim_check_object_shape_no_longer_parses() {
        // The legacy object shape `{ scope: ... }` is gone.
        let yaml = r"
type: oauth
validIssuerUri: https://issuer.example.com/
jwksEndpointUri: https://issuer.example.com/jwks
customClaimCheck:
  scope: kafka.write
";
        let result: Result<ListenerAuthentication, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "old object shape must be rejected; got Ok");
    }

    #[test]
    fn oauth_populated_field_round_trip_cases() {
        for (name, yaml, expected) in [
            (
                "maximum reauthentication interval",
                r"
type: oauth
validIssuerUri: https://issuer.example.com/
jwksEndpointUri: https://issuer.example.com/jwks
maxSecondsWithoutReauthentication: 300
",
                ListenerAuthenticationOAuth {
                    max_seconds_without_reauthentication: Some(300),
                    ..minimal_oauth()
                },
            ),
            (
                "custom claim and token type",
                r#"
type: oauth
validIssuerUri: https://issuer.example.com/
jwksEndpointUri: https://issuer.example.com/jwks
customClaimCheck: "$.scope[?@ == 'kafka.write']"
validTokenType: JWT
"#,
                ListenerAuthenticationOAuth {
                    custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
                    valid_token_type: Some("JWT".into()),
                    ..minimal_oauth()
                },
            ),
            (
                "claims mapping fields",
                r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
fallbackUserNameClaim: client_id
fallbackUserNamePrefix: "service-account-"
groupsClaim: "$.realm_access.roles[*]"
groupsClaimDelimiter: ","
"#,
                ListenerAuthenticationOAuth {
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
                },
            ),
            (
                "JWKS policy fields",
                r"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
jwksMinRefreshPauseSeconds: 1
jwksExpirySeconds: 3600
jwksIgnoreKeyUse: false
",
                ListenerAuthenticationOAuth {
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
                },
            ),
        ] {
            let parsed: ListenerAuthentication = serde_yaml::from_str(yaml)
                .unwrap_or_else(|error| panic!("{name}: YAML must parse: {error}"));
            let ListenerAuthentication::OAuth(actual) = parsed else {
                panic!("{name}: expected OAuth variant");
            };
            assert!(actual == expected, "{name}");
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn internal_listener_json_round_trip_cases() {
        for (name, peers, expected_json) in [
            (
                "peers omitted",
                None,
                serde_json::json!({"name":"PLAIN","port":9092,"type":"internal","tls":false}),
            ),
            (
                "empty peers retained",
                Some(vec![]),
                serde_json::json!({"name":"PLAIN","port":9092,"type":"internal","tls":false,"networkPolicyPeers":[]}),
            ),
        ] {
            let listener = Listener {
                name: "PLAIN".into(),
                port: 9092,
                type_: ListenerType::Internal,
                tls: false,
                authentication: None,
                configuration: None,
                network_policy_peers: peers,
            };
            let actual_json = serde_json::to_value(&listener).unwrap();
            assert_eq!(&actual_json, &expected_json, "case {name}");
            assert_eq!(
                serde_json::from_value::<Listener>(actual_json).unwrap(),
                listener,
                "case {name}"
            );
        }
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
