//! Listener rendering and validation.
//!
//! This code has its own module so that `controller/kafka.rs` and
//! `controller/common.rs` do not become larger.

use std::{collections::BTreeMap, net::IpAddr};

use crabka_security::{ListenerProtocol, SaslMechanism, ca::SubjectAltName};
use crabka_units::fmt::Human as _;
use k8s_openapi::api::{
    core::v1::{Node, Service},
    networking::v1::Ingress,
};
use kube::Resource as _;

use crate::{
    controller::common::{APP_LABEL, ReconcileError, owner_ref},
    crd::{Kafka, Listener, ListenerAuthentication, ListenerAuthenticationOAuth, ListenerType},
};

pub(crate) fn listener_protocol(l: &Listener) -> ListenerProtocol {
    use ListenerAuthentication::{Gssapi, OAuth, ScramSha256, ScramSha512, Tls};
    match (l.tls, &l.authentication) {
        (false, None) => ListenerProtocol::Plaintext,
        (true, None | Some(Tls)) => ListenerProtocol::Ssl,
        (false, Some(ScramSha512 | ScramSha256 | OAuth(_) | Gssapi(_))) => {
            ListenerProtocol::SaslPlaintext
        }
        (true, Some(ScramSha512 | ScramSha256 | OAuth(_) | Gssapi(_))) => ListenerProtocol::SaslSsl,
        (false, Some(Tls)) => unreachable!(
            "validation rejects mTLS without transport TLS; saw listener '{}'",
            l.name
        ),
    }
}

fn sasl_mechanism(auth: &ListenerAuthentication) -> Option<SaslMechanism> {
    match auth {
        ListenerAuthentication::ScramSha512 => Some(SaslMechanism::ScramSha512),
        ListenerAuthentication::ScramSha256 => Some(SaslMechanism::ScramSha256),
        ListenerAuthentication::OAuth(cfg) => {
            if cfg.enable_oauth_bearer {
                Some(SaslMechanism::OAuthBearer)
            } else {
                None
            }
        }
        ListenerAuthentication::Gssapi(_) => Some(SaslMechanism::Gssapi),
        ListenerAuthentication::Tls => None,
    }
}

/// Reason values for the `ListenersValid` status condition.
///
/// These strings are stable. `kubectl wait --for=condition=…` reads them
/// and the tests assert on them.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    DuplicateListenerName(String),
    DuplicateListenerPort(i32),
    /// `ingress` / `route` listener with `tls: false`. SNI-passthrough routing
    /// needs TLS, because the controller routes by the TLS `ClientHello` SNI.
    ListenerIngressRequiresTls(String),
    /// `ingress` / `route` listener with no `configuration.bootstrap.host`.
    /// The operator cannot derive a bootstrap hostname. The user must supply one.
    ListenerIngressBootstrapHostMissing(String),
    DuplicateBrokerOverride {
        listener: String,
        broker: i32,
    },
    InterBrokerListenerMissing(String),
    InterBrokerListenerNotInternal(String),
    NoInternalListener,
    ListenerMtlsRequiresTransportTls(String),
    /// `authentication: oauth` listener with `tls: false`. The signed-JWT
    /// validator needs the bearer token to stay confidential. Without TLS,
    /// the access token is visible on the network.
    ListenerOauthRequiresTransportTls(String),
    /// `authentication.oauth.validIssuerUri` empty / unset.
    ListenerOauthIssuerUriEmpty(String),
    /// `authentication.oauth.jwksEndpointUri` does not start with
    /// `http://` or `https://`.
    ListenerOauthJwksUriBadScheme(String),
    /// `authentication.oauth.jwksRefreshSeconds` set below the 30-second
    /// floor. A smaller value sends too many requests to the `IdP`.
    ListenerOauthJwksRefreshTooSmall {
        listener: String,
        got: u32,
    },
    /// `validTokenType` set on an `accessTokenIsJwt: false`
    /// listener. Introspection-mode validation has no JWT header to
    /// check `typ` against, so the operator rejects the field. The
    /// `String` gives the listener name and a description for the user.
    ListenerOauthValidTokenTypeRejectedInIntrospectionMode(String),
    /// Any of `jwksMinRefreshPauseSeconds`,
    /// `jwksExpirySeconds`, `jwksIgnoreKeyUse` set on an
    /// `accessTokenIsJwt: false` listener. Introspection mode
    /// does not use JWKS. These fields are a configuration error, and the
    /// operator reports it at apply time. The `String` gives the listener
    /// name and the fields that the operator rejected.
    ListenerOauthJwksFieldsRejectedInIntrospectionMode(String),
    /// Two or more OAuth listeners declare different configurations. The
    /// broker `[oauthbearer]` block is broker-global, so the operator
    /// cannot give each listener its own OAuth configuration.
    ConflictingOAuthListenerConfig,
    /// An OAuth listener's `accessTokenIsJwt` setting disagrees
    /// with which mode-specific fields are set. The `String` gives a
    /// description for the user with the listener name and the broken
    /// invariant. JWT mode needs `jwksEndpointUri` and forbids all
    /// introspection-mode fields. Introspection mode needs
    /// `introspectionEndpointUri`, `clientId`, and `clientSecret`, and
    /// forbids `jwksEndpointUri`.
    ListenerOauthAccessTokenIsJwtInvalid(String),
    /// `type: gssapi` listener missing `keytabSecretRef` (secretName/key).
    ListenerGssapiKeytabSecretMissing(String),
    /// A `principalToLocalRules` entry failed `auth_to_local` parsing.
    ListenerGssapiInvalidRule(String),
    /// Two or more GSSAPI listeners declare different configurations. The
    /// broker `[gssapi]` block is broker-global, so the operator cannot
    /// give each listener its own GSSAPI configuration.
    ConflictingGssapiListenerConfig,
    /// The inter-broker listener is `type: gssapi` but
    /// `spec.interBrokerKerberos` is absent.
    InterBrokerGssapiRequiresKerberosConfig(String),
}

#[allow(dead_code)]
impl ValidationError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::DuplicateListenerName(_) => "DuplicateListenerName",
            Self::DuplicateListenerPort(_) => "DuplicateListenerPort",
            Self::ListenerIngressRequiresTls(_) => "ListenerIngressRequiresTls",
            Self::ListenerIngressBootstrapHostMissing(_) => "ListenerIngressBootstrapHostMissing",
            Self::DuplicateBrokerOverride { .. } => "DuplicateBrokerOverride",
            Self::InterBrokerListenerMissing(_) => "InterBrokerListenerMissing",
            Self::InterBrokerListenerNotInternal(_) => "InterBrokerListenerNotInternal",
            Self::NoInternalListener => "NoInternalListener",
            Self::ListenerMtlsRequiresTransportTls(_) => "ListenerMtlsRequiresTransportTls",
            Self::ListenerOauthRequiresTransportTls(_) => "ListenerOauthRequiresTransportTls",
            Self::ListenerOauthIssuerUriEmpty(_) | Self::ListenerOauthJwksUriBadScheme(_) => {
                "ListenerOauthInvalidUri"
            }
            Self::ListenerOauthJwksRefreshTooSmall { .. } => "ListenerOauthInvalidRefresh",
            Self::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(_) => {
                "ListenerOauthValidTokenTypeRejectedInIntrospectionMode"
            }
            Self::ListenerOauthJwksFieldsRejectedInIntrospectionMode(_) => {
                "ListenerOauthJwksFieldsRejectedInIntrospectionMode"
            }
            Self::ConflictingOAuthListenerConfig => "ConflictingOAuthConfig",
            Self::ListenerOauthAccessTokenIsJwtInvalid(_) => "ListenerOauthAccessTokenIsJwtInvalid",
            Self::ListenerGssapiKeytabSecretMissing(_) => "ListenerGssapiKeytabSecretMissing",
            Self::ListenerGssapiInvalidRule(_) => "ListenerGssapiInvalidRule",
            Self::ConflictingGssapiListenerConfig => "ListenerGssapiConfigConflict",
            Self::InterBrokerGssapiRequiresKerberosConfig(_) => {
                "InterBrokerGssapiRequiresKerberosConfig"
            }
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::DuplicateListenerName(n) => {
                format!("listener name '{n}' is used more than once")
            }
            Self::DuplicateListenerPort(p) => {
                format!("listener port {p} is used more than once")
            }
            Self::ListenerIngressRequiresTls(n) => format!(
                "listener '{n}': type=ingress/route requires tls: true (SNI-passthrough routing needs TLS)"
            ),
            Self::ListenerIngressBootstrapHostMissing(n) => {
                format!("listener '{n}': type=ingress/route requires configuration.bootstrap.host")
            }
            Self::DuplicateBrokerOverride { listener, broker } => format!(
                "listener '{listener}' has duplicate configuration.brokers entries for broker {broker}"
            ),
            Self::InterBrokerListenerMissing(n) => {
                format!("spec.interBrokerListenerName='{n}' does not match any listener")
            }
            Self::InterBrokerListenerNotInternal(n) => {
                format!("spec.interBrokerListenerName='{n}' points to a non-internal listener")
            }
            Self::NoInternalListener => {
                "spec.listeners is non-empty but contains no internal-type listener".into()
            }
            Self::ListenerMtlsRequiresTransportTls(n) => {
                format!("listener '{n}': authentication.type=tls requires tls: true")
            }
            Self::ListenerOauthRequiresTransportTls(n) => {
                format!("listener '{n}': authentication.type=oauth requires tls: true")
            }
            Self::ListenerOauthIssuerUriEmpty(n) => {
                format!("listener '{n}': authentication.oauth.validIssuerUri is required")
            }
            Self::ListenerOauthJwksUriBadScheme(n) => {
                format!(
                    "listener '{n}': authentication.oauth.jwksEndpointUri must be http:// or https://"
                )
            }
            Self::ListenerOauthJwksRefreshTooSmall { listener, got } => {
                format!(
                    "listener '{listener}': authentication.oauth.jwksRefreshSeconds must be >= 30 (got {got})"
                )
            }
            Self::ConflictingOAuthListenerConfig => {
                "all OAuth listeners must share identical config (the broker oauthbearer block is broker-global)".to_string()
            }
            Self::ListenerOauthAccessTokenIsJwtInvalid(msg)
            | Self::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(msg)
            | Self::ListenerOauthJwksFieldsRejectedInIntrospectionMode(msg)
            | Self::ListenerGssapiInvalidRule(msg) => msg.clone(),
            Self::ListenerGssapiKeytabSecretMissing(n) => {
                format!("listener '{n}': authentication.type=gssapi requires keytabSecretRef.secretName and .key")
            }
            Self::ConflictingGssapiListenerConfig => {
                "all GSSAPI listeners must share identical config (the broker [gssapi] block is broker-global)".to_string()
            }
            Self::InterBrokerGssapiRequiresKerberosConfig(n) => format!(
                "interBrokerListenerName='{n}' is type=gssapi but spec.interBrokerKerberos is not set"
            ),
        }
    }
}

/// Returns a canonical form of an OAuth listener configuration.
///
/// Cross-listener conflict detection uses this form. The broker
/// `[oauthbearer]` block is broker-global, so only one field can differ
/// between per-listener OAuth configurations without a contradiction of
/// that block: `enable_oauth_bearer`, which gates only the per-listener
/// `sasl_mechanisms`. This function masks that field to a constant, so two
/// listeners that differ only in it give the same canonical value.
#[must_use]
fn oauth_canonical(cfg: &ListenerAuthenticationOAuth) -> ListenerAuthenticationOAuth {
    let mut out = cfg.clone();
    out.enable_oauth_bearer = true;
    out
}

/// Returns a canonical form of a GSSAPI listener configuration.
///
/// Cross-listener conflict detection uses this form. The broker `[gssapi]`
/// block is broker-global, so every field must agree between GSSAPI
/// listeners. Compare the whole struct.
#[must_use]
fn gssapi_canonical(
    cfg: &crate::crd::ListenerAuthenticationGssapi,
) -> crate::crd::ListenerAuthenticationGssapi {
    cfg.clone()
}

fn validate_unique_listeners(listeners: &[Listener]) -> Result<(), ValidationError> {
    for (index, listener) in listeners.iter().enumerate() {
        for prior in &listeners[..index] {
            if prior.name == listener.name {
                return Err(ValidationError::DuplicateListenerName(
                    listener.name.clone(),
                ));
            }
            if prior.port == listener.port {
                return Err(ValidationError::DuplicateListenerPort(listener.port));
            }
        }
    }
    Ok(())
}

fn validate_gssapi_listener(
    listener: &Listener,
    config: &crate::crd::ListenerAuthenticationGssapi,
) -> Result<(), ValidationError> {
    if config.keytab_secret_ref.secret_name.is_empty() || config.keytab_secret_ref.key.is_empty() {
        return Err(ValidationError::ListenerGssapiKeytabSecretMissing(
            listener.name.clone(),
        ));
    }
    for specification in &config.principal_to_local_rules {
        if crabka_security::gssapi::name::Rule::parse(specification).is_err() {
            return Err(ValidationError::ListenerGssapiInvalidRule(format!(
                "listener '{}': invalid principalToLocalRules entry {specification:?}",
                listener.name
            )));
        }
    }
    Ok(())
}

/// Validates `spec.listeners` and `spec.interBrokerListenerName`.
///
/// This function returns `Ok(())` when all values are well-formed. If not,
/// it returns the first error that it finds. Validation stops at the first
/// error to show the most useful problem instead of a list.
pub fn validate_listeners(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> Result<(), ValidationError> {
    validate_unique_listeners(listeners)?;

    // Per-listener type/tls/override checks.
    for l in listeners {
        if matches!(l.authentication, Some(ListenerAuthentication::Tls)) && !l.tls {
            return Err(ValidationError::ListenerMtlsRequiresTransportTls(
                l.name.clone(),
            ));
        }
        if let Some(ListenerAuthentication::OAuth(cfg)) = &l.authentication {
            if !l.tls {
                return Err(ValidationError::ListenerOauthRequiresTransportTls(
                    l.name.clone(),
                ));
            }
            // Cross-mode invariants. Fire first so operators see
            // the mode-shape problem before any field-by-field complaint
            // (e.g. "no JWKS URI" is more actionable than "JWKS URI must
            // be http/https" when the user meant introspection mode).
            if cfg.access_token_is_jwt {
                if cfg.jwks_endpoint_uri.is_none() {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=true requires jwksEndpointUri",
                            l.name
                        ),
                    ));
                }
                if cfg.introspection_endpoint_uri.is_some()
                    || cfg.user_info_endpoint_uri.is_some()
                    || cfg.client_id.is_some()
                    || cfg.client_secret.is_some()
                    || cfg.introspection_http_timeout_seconds.is_some()
                {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=true forbids introspection-mode fields (introspectionEndpointUri/userInfoEndpointUri/clientId/clientSecret/introspectionHttpTimeoutSeconds)",
                            l.name
                        ),
                    ));
                }
            } else {
                if cfg.jwks_endpoint_uri.is_some() {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=false forbids jwksEndpointUri",
                            l.name
                        ),
                    ));
                }
                if cfg.introspection_endpoint_uri.is_none() {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=false requires introspectionEndpointUri",
                            l.name
                        ),
                    ));
                }
                if cfg.client_id.is_none() {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=false requires clientId",
                            l.name
                        ),
                    ));
                }
                if cfg.client_secret.is_none() {
                    return Err(ValidationError::ListenerOauthAccessTokenIsJwtInvalid(
                        format!(
                            "listener '{}': accessTokenIsJwt=false requires clientSecret",
                            l.name
                        ),
                    ));
                }
                if cfg.valid_token_type.is_some() {
                    return Err(
                        ValidationError::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(
                            format!(
                                "listener '{}': accessTokenIsJwt=false forbids validTokenType (no JWT header in introspection responses)",
                                l.name
                            ),
                        ),
                    );
                }
                // JWKS-only fields are rejected in introspection mode.
                let mut jwks_fields_set = Vec::new();
                if cfg.jwks_min_refresh_pause_seconds.is_some() {
                    jwks_fields_set.push("jwksMinRefreshPauseSeconds");
                }
                if cfg.jwks_expiry_seconds.is_some() {
                    jwks_fields_set.push("jwksExpirySeconds");
                }
                if cfg.jwks_ignore_key_use.is_some() {
                    jwks_fields_set.push("jwksIgnoreKeyUse");
                }
                if !jwks_fields_set.is_empty() {
                    return Err(
                        ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode(
                            format!(
                                "listener '{}': accessTokenIsJwt=false forbids JWKS-only fields ({})",
                                l.name,
                                jwks_fields_set.join(", "),
                            ),
                        ),
                    );
                }
            }
            if cfg.valid_issuer_uri.is_empty() {
                return Err(ValidationError::ListenerOauthIssuerUriEmpty(l.name.clone()));
            }
            // Cross-mode block above guarantees jwks_endpoint_uri is Some
            // iff access_token_is_jwt; only validate scheme when set.
            if let Some(uri) = &cfg.jwks_endpoint_uri
                && !uri.starts_with("http://")
                && !uri.starts_with("https://")
            {
                return Err(ValidationError::ListenerOauthJwksUriBadScheme(
                    l.name.clone(),
                ));
            }
            if let Some(s) = cfg.jwks_refresh_seconds
                && s < 30
            {
                return Err(ValidationError::ListenerOauthJwksRefreshTooSmall {
                    listener: l.name.clone(),
                    got: s,
                });
            }
        }
        if let Some(ListenerAuthentication::Gssapi(cfg)) = &l.authentication {
            validate_gssapi_listener(l, cfg)?;
        }
        if matches!(l.type_, ListenerType::Ingress | ListenerType::Route) {
            if !l.tls {
                return Err(ValidationError::ListenerIngressRequiresTls(l.name.clone()));
            }
            let has_bootstrap_host = l
                .configuration
                .as_ref()
                .and_then(|c| c.bootstrap.as_ref())
                .and_then(|b| b.host.as_ref())
                .is_some();
            if !has_bootstrap_host {
                return Err(ValidationError::ListenerIngressBootstrapHostMissing(
                    l.name.clone(),
                ));
            }
        }
        if let Some(cfg) = &l.configuration {
            let mut seen = std::collections::HashSet::new();
            for ovr in &cfg.brokers {
                if !seen.insert(ovr.broker) {
                    return Err(ValidationError::DuplicateBrokerOverride {
                        listener: l.name.clone(),
                        broker: ovr.broker,
                    });
                }
            }
        }
    }

    // Cross-listener OAuth conflict check. The broker `[oauthbearer]` block is
    // broker-global, so two OAuth listeners with diverging configs
    // (different issuer, audience, JWKS, claim names, …) can't be honored
    // simultaneously. Listeners that differ ONLY in whether they advertise
    // OAUTHBEARER on the wire (`enable_oauth_bearer`) are fine — the global
    // validator config is the same; the per-listener bit just gates whether the
    // mechanism appears in this listener's `sasl_mechanisms`.
    let oauth_canonicals: Vec<ListenerAuthenticationOAuth> = listeners
        .iter()
        .filter_map(|l| match &l.authentication {
            Some(ListenerAuthentication::OAuth(cfg)) => Some(oauth_canonical(cfg)),
            _ => None,
        })
        .collect();
    let mut deduped = oauth_canonicals;
    deduped.dedup();
    // dedup() only removes adjacent duplicates; sort+dedup gives a true set.
    // But ListenerAuthenticationOAuth isn't Ord. Do an O(n²) distinct count:
    let mut distinct: Vec<ListenerAuthenticationOAuth> = Vec::new();
    for c in deduped {
        if !distinct.iter().any(|d| d == &c) {
            distinct.push(c);
        }
    }
    if distinct.len() > 1 {
        return Err(ValidationError::ConflictingOAuthListenerConfig);
    }

    // Broker-global [gssapi] block: all GSSAPI listeners must agree.
    let mut gssapi_canon: Option<crate::crd::ListenerAuthenticationGssapi> = None;
    for l in listeners {
        if let Some(ListenerAuthentication::Gssapi(cfg)) = &l.authentication {
            let canon = gssapi_canonical(cfg);
            match &gssapi_canon {
                None => gssapi_canon = Some(canon),
                Some(prev) if *prev != canon => {
                    return Err(ValidationError::ConflictingGssapiListenerConfig);
                }
                Some(_) => {}
            }
        }
    }

    // Inter-broker listener resolution.
    if !listeners.is_empty() {
        let has_internal = listeners.iter().any(|l| l.type_ == ListenerType::Internal);
        if !has_internal {
            return Err(ValidationError::NoInternalListener);
        }
        if let Some(name) = inter_broker_listener_name {
            match listeners.iter().find(|l| l.name == name) {
                None => return Err(ValidationError::InterBrokerListenerMissing(name.into())),
                Some(l) if l.type_ != ListenerType::Internal => {
                    return Err(ValidationError::InterBrokerListenerNotInternal(name.into()));
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Checks that a GSSAPI inter-broker listener has `spec.interBrokerKerberos`.
///
/// When the resolved inter-broker listener uses GSSAPI,
/// `spec.interBrokerKerberos` must be present. `ib_kerberos_present` is
/// `spec.inter_broker_kerberos.is_some()`.
pub fn validate_inter_broker_gssapi(
    listeners: &[Listener],
    inter_broker_listener_name: &str,
    ib_kerberos_present: bool,
) -> Result<(), ValidationError> {
    let ib_is_gssapi = listeners.iter().any(|l| {
        l.name == inter_broker_listener_name
            && matches!(l.authentication, Some(ListenerAuthentication::Gssapi(_)))
    });
    if ib_is_gssapi && !ib_kerberos_present {
        return Err(ValidationError::InterBrokerGssapiRequiresKerberosConfig(
            inter_broker_listener_name.to_string(),
        ));
    }
    Ok(())
}

/// Returns one warning string for each listener that has SCRAM
/// authentication without transport TLS.
///
/// These conditions are not errors, because SCRAM itself is
/// cryptographically safe. But the SCRAM exchange crosses the network
/// before the authentication is complete. On a plaintext connection, a
/// passive eavesdropper can see the credentials.
pub(crate) fn weak_auth_warnings(listeners: &[Listener]) -> Vec<String> {
    let mut warnings: Vec<String> = listeners
        .iter()
        .filter(|l| {
            !l.tls
                && matches!(
                    l.authentication,
                    Some(ListenerAuthentication::ScramSha512 | ListenerAuthentication::ScramSha256)
                )
        })
        .map(|l| {
            format!(
                "listener '{}' has SCRAM auth without transport TLS; credentials traverse \
                 the network in cleartext during the SCRAM exchange. Consider tls: true.",
                l.name
            )
        })
        .collect();
    for l in listeners {
        if let Some(ListenerAuthentication::OAuth(cfg)) = &l.authentication
            && let Some(uri) = &cfg.jwks_endpoint_uri
            && uri.starts_with("http://")
        {
            warnings.push(format!(
                "listener '{}' has http:// JWKS endpoint; key material traverses the network in cleartext. Consider https.",
                l.name
            ));
        }
    }
    warnings
}

/// Picks the inter-broker listener name.
///
/// An explicit override wins. If there is no override, this function picks
/// the first `internal` listener. When `listeners` is empty, it returns the
/// synthesized default name `"PLAIN"`. This is the no-listeners
/// compatibility path.
#[allow(dead_code)]
#[must_use]
pub fn effective_inter_broker_listener_name(
    listeners: &[Listener],
    explicit: Option<&str>,
) -> String {
    if let Some(s) = explicit {
        return s.to_string();
    }
    if listeners.is_empty() {
        return "PLAIN".to_string();
    }
    listeners
        .iter()
        .find(|l| l.type_ == ListenerType::Internal)
        .map_or_else(|| "PLAIN".to_string(), |l| l.name.clone())
}

/// Renders the per-broker external Service for one listener and broker id.
///
/// The Service selector uses the built-in
/// `statefulset.kubernetes.io/pod-name` label (K8s 1.28+). This pins the
/// Service to the one pod that hosts this broker.
///
/// `pod_name` is the pod name that the `StatefulSet` allocated, for example
/// `demo-controller-0`. The caller computes it from the pool and the
/// ordinal.
///
/// `nodeport` and `loadbalancer` give a `NodePort` or `LoadBalancer`
/// Service. `ingress` and `route` give a `ClusterIP` Service that the
/// Ingress or Route uses as its backend.
///
/// # Panics
///
/// Panics if called with the `internal` listener type. Internal listeners
/// use the cluster-wide headless Service and never get a per-broker
/// Service.
#[allow(dead_code)]
pub fn render_broker_service(
    owner: &Kafka,
    listener: &Listener,
    broker_id: i32,
    pod_name: &str,
) -> Result<Service, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-{broker_id}", listener.name);

    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/broker".into(), broker_id.to_string());

    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert(
        "statefulset.kubernetes.io/pod-name".into(),
        pod_name.to_string(),
    );

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort",
        ListenerType::Loadbalancer => "LoadBalancer",
        ListenerType::Ingress | ListenerType::Route => "ClusterIP",
        ListenerType::Internal => panic!(
            "render_broker_service called with non-external type {:?}",
            listener.type_
        ),
    };

    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = override_.and_then(|o| o.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = override_.and_then(|o| o.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": svc_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": spec,
    }))?;
    Ok(svc)
}

/// Renders the bootstrap Service for one external listener.
///
/// Its selector matches every broker pod of the cluster.
///
/// `nodeport` and `loadbalancer` give a `NodePort` or `LoadBalancer`
/// Service. `ingress` and `route` give a `ClusterIP` Service that the
/// bootstrap Ingress or Route uses as its backend.
///
/// # Panics
///
/// Panics if called with the `internal` listener type.
#[allow(dead_code)]
pub fn render_bootstrap_service(
    owner: &Kafka,
    listener: &Listener,
) -> Result<Service, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-bootstrap", listener.name);

    let bootstrap = listener
        .configuration
        .as_ref()
        .and_then(|c| c.bootstrap.as_ref());

    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/role".into(), "bootstrap".into());
    if let Some(b) = bootstrap {
        for (k, v) in &b.labels {
            labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    if let Some(b) = bootstrap {
        for (k, v) in &b.annotations {
            annotations.insert(k.clone(), v.clone());
        }
    }

    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), cluster_name.clone());

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort",
        ListenerType::Loadbalancer => "LoadBalancer",
        ListenerType::Ingress | ListenerType::Route => "ClusterIP",
        ListenerType::Internal => panic!(
            "render_bootstrap_service called with non-external type {:?}",
            listener.type_
        ),
    };

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = bootstrap.and_then(|b| b.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = bootstrap.and_then(|b| b.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let mut meta = serde_json::json!({
        "name": svc_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner_ref::<Kafka>(owner)?],
    });
    if !annotations.is_empty() {
        meta["annotations"] = serde_json::to_value(&annotations)?;
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": meta,
        "spec": spec,
    }))?;
    Ok(svc)
}

// ---------------------------------------------------------------------------
// Ingress / Route external listeners
// ---------------------------------------------------------------------------

/// Advertised port for `ingress` and `route` listeners.
///
/// This is the standard HTTPS port that the ingress controller or the
/// `OpenShift` router terminates on. Each broker can override it with
/// `configuration.brokers[].advertisedPort`.
pub(crate) const INGRESS_PORT: i32 = 443;

/// KIP-405: mount path for the local-tier RSM.
///
/// The operator writes this path into the broker TOML as
/// `[remote_storage].storage_dir`, and into the `tier-storage`
/// `volumeMount` of the broker pod template. Both use the same path, so
/// the broker's `LocalTieredStorage` writes through one canonical
/// location.
pub(crate) const TIER_STORAGE_PATH: &str = "/var/lib/crabka/remote";

/// Fixed in-pod path where the operator mounts the GSSAPI keytab.
///
/// Both the `[gssapi]` and `[inter_broker_credentials]` TOML blocks refer
/// to it.
pub(crate) const GSSAPI_KEYTAB_PATH: &str = "/etc/crabka/gssapi-keytab/keytab";

/// Directory that the operator mounts the GSSAPI keytab Secret into.
///
/// The projected item path `keytab` below this directory gives
/// `GSSAPI_KEYTAB_PATH`.
pub(crate) const GSSAPI_KEYTAB_DIR: &str = "/etc/crabka/gssapi-keytab";

/// KIP-405: fixed in-pod directory for the GCS service-account key.
///
/// The operator mounts an explicit GCS service-account JSON key Secret
/// here for file-based credentials. The projected item of the Secret lands
/// at `key.json` below this directory, so
/// `[remote_storage.gcs].service_account_path` in the broker TOML points
/// at `<GCS_CREDENTIALS_DIR>/key.json`. The operator uses this directory
/// only when `gcs.credentials` is set. Keyless Workload Identity and ADC
/// mount nothing.
pub(crate) const GCS_CREDENTIALS_DIR: &str = "/etc/crabka/gcs-credentials";

/// Projected filename for the GCS service-account JSON key below
/// [`GCS_CREDENTIALS_DIR`].
///
/// The full `service_account_path` that the operator writes into the
/// broker TOML is `<GCS_CREDENTIALS_DIR>/<GCS_CREDENTIALS_FILE>`.
pub(crate) const GCS_CREDENTIALS_FILE: &str = "key.json";

/// Escapes a string for use inside a TOML basic, double-quoted string.
///
/// This render must escape only `\` and `"`. The operator rejects newlines
/// and other control characters during CRD validation, so the TOML parser
/// of the broker never sees them here.
fn toml_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// The de-facto annotation that tells the nginx ingress controller not to
/// terminate TLS.
///
/// The controller forwards the raw TLS stream to the backend and routes it
/// by SNI. The annotation does no damage on controllers that ignore it.
/// Kafka-over-Ingress needs it.
const SSL_PASSTHROUGH_ANNOTATION: &str = "nginx.ingress.kubernetes.io/ssl-passthrough";

/// Resolves the external hostname for one ingress or route listener and
/// broker.
///
/// The `advertisedHost` override wins over the `host` field. This function
/// returns `None` when neither field is set. That is a configuration error,
/// and the operator reports it when it computes the advertised addresses.
#[must_use]
pub(crate) fn ingress_broker_host(listener: &Listener, broker_id: i32) -> Option<String> {
    let o = listener
        .configuration
        .as_ref()?
        .brokers
        .iter()
        .find(|b| b.broker == broker_id)?;
    o.advertised_host.clone().or_else(|| o.host.clone())
}

/// Resolves the bootstrap hostname for an ingress or route listener.
///
/// For a listener that passed `validate_listeners`, validation guarantees
/// that the result is `Some`.
#[must_use]
pub(crate) fn ingress_bootstrap_host(listener: &Listener) -> Option<String> {
    listener
        .configuration
        .as_ref()
        .and_then(|c| c.bootstrap.as_ref())
        .and_then(|b| b.host.clone())
}

fn ingress_labels(cluster_name: &str, listener: &Listener) -> BTreeMap<String, String> {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert(
        "app.kubernetes.io/instance".into(),
        cluster_name.to_string(),
    );
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels
}

/// Renders one `Ingress` (networking.k8s.io/v1) that routes `host` to
/// `service_name:listener.port`.
///
/// The route uses TLS passthrough with SNI. Both the per-broker Ingress
/// object and the bootstrap Ingress object come from this function.
fn build_ingress(
    owner: &Kafka,
    listener: &Listener,
    object_name: &str,
    host: &str,
    service_name: &str,
    mut labels: BTreeMap<String, String>,
    extra_annotations: &BTreeMap<String, String>,
) -> Result<Ingress, ReconcileError> {
    let namespace = owner.meta().namespace.clone();
    labels
        .entry("app.kubernetes.io/name".into())
        .or_insert_with(|| APP_LABEL.into());

    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    annotations.insert(SSL_PASSTHROUGH_ANNOTATION.into(), "true".into());
    for (k, v) in extra_annotations {
        annotations.insert(k.clone(), v.clone());
    }

    let mut spec = serde_json::json!({
        "tls": [{ "hosts": [host] }],
        "rules": [{
            "host": host,
            "http": {
                "paths": [{
                    "path": "/",
                    "pathType": "Prefix",
                    "backend": {
                        "service": {
                            "name": service_name,
                            "port": { "number": listener.port },
                        }
                    }
                }]
            }
        }],
    });
    if let Some(class) = listener
        .configuration
        .as_ref()
        .and_then(|c| c.ingress_class.as_ref())
    {
        spec["ingressClassName"] = serde_json::json!(class);
    }

    let ingress: Ingress = serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": object_name,
            "namespace": namespace,
            "labels": labels,
            "annotations": annotations,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": spec,
    }))?;
    Ok(ingress)
}

/// Renders the per-broker Ingress `<cluster>-<listener>-<broker>`.
///
/// It routes the hostname of the broker to the `ClusterIP` backend Service
/// of that broker.
#[allow(dead_code)]
pub fn render_broker_ingress(
    owner: &Kafka,
    listener: &Listener,
    broker_id: i32,
    host: &str,
) -> Result<Ingress, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let object_name = format!("{cluster_name}-{}-{broker_id}", listener.name);
    let service_name = object_name.clone();
    let mut labels = ingress_labels(&cluster_name, listener);
    labels.insert("crabka.io/broker".into(), broker_id.to_string());
    build_ingress(
        owner,
        listener,
        &object_name,
        host,
        &service_name,
        labels,
        &BTreeMap::new(),
    )
}

/// Renders the bootstrap Ingress `<cluster>-<listener>-bootstrap`.
///
/// It routes the bootstrap hostname to the all-pods bootstrap Service.
#[allow(dead_code)]
pub fn render_bootstrap_ingress(
    owner: &Kafka,
    listener: &Listener,
    host: &str,
) -> Result<Ingress, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let object_name = format!("{cluster_name}-{}-bootstrap", listener.name);
    let service_name = object_name.clone();
    let mut labels = ingress_labels(&cluster_name, listener);
    labels.insert("crabka.io/role".into(), "bootstrap".into());
    let extra = listener
        .configuration
        .as_ref()
        .and_then(|c| c.bootstrap.as_ref())
        .map(|b| b.annotations.clone())
        .unwrap_or_default();
    build_ingress(
        owner,
        listener,
        &object_name,
        host,
        &service_name,
        labels,
        &extra,
    )
}

/// Renders one `OpenShift` `Route` (`route.openshift.io/v1`) as a JSON
/// body.
///
/// The operator applies this body dynamically, because `k8s-openapi` does
/// not hold the type. Passthrough TLS termination makes the router route
/// the raw TLS stream to the broker by SNI.
fn build_route(
    owner: &Kafka,
    listener: &Listener,
    object_name: &str,
    host: &str,
    service_name: &str,
    mut labels: BTreeMap<String, String>,
) -> Result<serde_json::Value, ReconcileError> {
    let namespace = owner.meta().namespace.clone().unwrap_or_default();
    labels
        .entry("app.kubernetes.io/name".into())
        .or_insert_with(|| APP_LABEL.into());
    Ok(serde_json::json!({
        "apiVersion": "route.openshift.io/v1",
        "kind": "Route",
        "metadata": {
            "name": object_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": {
            "host": host,
            "port": { "targetPort": listener.port },
            "tls": { "termination": "passthrough" },
            "to": { "kind": "Service", "name": service_name, "weight": 100 },
        }
    }))
}

/// Renders the per-broker Route `<cluster>-<listener>-<broker>`.
#[allow(dead_code)]
pub fn render_broker_route(
    owner: &Kafka,
    listener: &Listener,
    broker_id: i32,
    host: &str,
) -> Result<serde_json::Value, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let object_name = format!("{cluster_name}-{}-{broker_id}", listener.name);
    let service_name = object_name.clone();
    let mut labels = ingress_labels(&cluster_name, listener);
    labels.insert("crabka.io/broker".into(), broker_id.to_string());
    build_route(owner, listener, &object_name, host, &service_name, labels)
}

/// Renders the bootstrap Route `<cluster>-<listener>-bootstrap`.
#[allow(dead_code)]
pub fn render_bootstrap_route(
    owner: &Kafka,
    listener: &Listener,
    host: &str,
) -> Result<serde_json::Value, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let object_name = format!("{cluster_name}-{}-bootstrap", listener.name);
    let service_name = object_name.clone();
    let mut labels = ingress_labels(&cluster_name, listener);
    labels.insert("crabka.io/role".into(), "bootstrap".into());
    build_route(owner, listener, &object_name, host, &service_name, labels)
}

#[cfg(test)]
mod service_rendering_tests {
    use assert2::{assert, check};

    use super::*;
    use crate::crd::{BootstrapConfig, BrokerOverride, KafkaSpec, ListenerConfiguration};

    fn kafka(name: &str) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
                gres_registry: None,
            },
        );
        k.meta_mut().namespace = Some("default".into());
        k.meta_mut().uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn nodeport_broker_service_has_pod_name_selector_and_nodeport() {
        let k = kafka("demo");
        let listener = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    node_port: Some(32100),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        assert!(svc.metadata.name.as_deref() == Some("demo-external-0"));
        let spec = svc.spec.as_ref().unwrap();
        assert!(spec.type_.as_deref() == Some("NodePort"));
        let sel = spec.selector.as_ref().unwrap();
        check!(sel.get("statefulset.kubernetes.io/pod-name") == Some(&"demo-pool-0".to_string()));
        check!(spec.ports.as_ref().unwrap()[0].port == 9094);
        check!(spec.ports.as_ref().unwrap()[0].node_port == Some(32100));
    }

    #[test]
    fn loadbalancer_broker_service_uses_lb_ip_override() {
        let k = kafka("demo");
        let listener = Listener {
            name: "lb".into(),
            port: 9094,
            type_: ListenerType::Loadbalancer,
            tls: false,
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    load_balancer_ip: Some("10.0.0.5".into()),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        let spec = svc.spec.as_ref().unwrap();
        assert!(spec.type_.as_deref() == Some("LoadBalancer"));
        assert!(spec.load_balancer_ip.as_deref() == Some("10.0.0.5"));
    }

    #[test]
    fn bootstrap_service_selects_all_broker_pods() {
        let k = kafka("demo");
        let listener = Listener {
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
                brokers: vec![],
                ingress_class: None,
            }),
            network_policy_peers: None,
        };
        let svc = render_bootstrap_service(&k, &listener).unwrap();
        assert!(svc.metadata.name.as_deref() == Some("demo-external-bootstrap"));
        let spec = svc.spec.as_ref().unwrap();
        let sel = spec.selector.as_ref().unwrap();
        check!(sel.get("app.kubernetes.io/instance") == Some(&"demo".to_string()));
        check!(sel.get("statefulset.kubernetes.io/pod-name").is_none());
        check!(spec.ports.as_ref().unwrap()[0].node_port == Some(32099));
    }

    fn ingress_listener(type_: ListenerType) -> Listener {
        Listener {
            name: "ext".into(),
            port: 9094,
            type_,
            tls: true,
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    host: Some("bootstrap.kafka.example.com".into()),
                    ..Default::default()
                }),
                brokers: vec![BrokerOverride {
                    broker: 0,
                    host: Some("broker-0.kafka.example.com".into()),
                    ..Default::default()
                }],
                ingress_class: Some("nginx".into()),
            }),
            network_policy_peers: None,
        }
    }

    #[test]
    fn ingress_broker_backend_service_is_clusterip() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Ingress);
        let svc = render_broker_service(&k, &l, 0, "demo-pool-0").unwrap();
        let spec = svc.spec.as_ref().unwrap();
        assert!(spec.type_.as_deref() == Some("ClusterIP"));
        assert!(
            spec.selector
                .as_ref()
                .unwrap()
                .get("statefulset.kubernetes.io/pod-name")
                == Some(&"demo-pool-0".to_string())
        );
    }

    #[test]
    fn route_bootstrap_backend_service_is_clusterip() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Route);
        let svc = render_bootstrap_service(&k, &l).unwrap();
        assert!(svc.spec.as_ref().unwrap().type_.as_deref() == Some("ClusterIP"));
    }

    #[test]
    fn render_broker_ingress_shape() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Ingress);
        let ing = render_broker_ingress(&k, &l, 0, "broker-0.kafka.example.com").unwrap();
        assert!(ing.metadata.name.as_deref() == Some("demo-ext-0"));
        let ann = ing.metadata.annotations.as_ref().unwrap();
        assert!(
            ann.get("nginx.ingress.kubernetes.io/ssl-passthrough") == Some(&"true".to_string())
        );
        let spec = ing.spec.as_ref().unwrap();
        assert!(spec.ingress_class_name.as_deref() == Some("nginx"));
        let rule = &spec.rules.as_ref().unwrap()[0];
        assert!(rule.host.as_deref() == Some("broker-0.kafka.example.com"));
        let path = &rule.http.as_ref().unwrap().paths[0];
        let backend = path.backend.service.as_ref().unwrap();
        assert!(backend.name == "demo-ext-0");
        assert!(backend.port.as_ref().unwrap().number == Some(9094));
        let tls = &spec.tls.as_ref().unwrap()[0];
        assert!(tls.hosts.as_ref().unwrap()[0] == "broker-0.kafka.example.com".to_string());
        assert!(tls.secret_name.is_none(), "passthrough has no secretName");
    }

    #[test]
    fn render_bootstrap_ingress_uses_bootstrap_host() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Ingress);
        let ing = render_bootstrap_ingress(&k, &l, "bootstrap.kafka.example.com").unwrap();
        assert!(ing.metadata.name.as_deref() == Some("demo-ext-bootstrap"));
        let rule = &ing.spec.as_ref().unwrap().rules.as_ref().unwrap()[0];
        assert!(rule.host.as_deref() == Some("bootstrap.kafka.example.com"));
    }

    #[test]
    fn render_broker_route_is_passthrough() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Route);
        let route = render_broker_route(&k, &l, 0, "broker-0.kafka.example.com").unwrap();
        for (pointer, want) in [
            ("/apiVersion", serde_json::json!("route.openshift.io/v1")),
            ("/kind", serde_json::json!("Route")),
            ("/metadata/name", serde_json::json!("demo-ext-0")),
            (
                "/spec/host",
                serde_json::json!("broker-0.kafka.example.com"),
            ),
            ("/spec/tls/termination", serde_json::json!("passthrough")),
            ("/spec/port/targetPort", serde_json::json!(9094)),
            ("/spec/to/kind", serde_json::json!("Service")),
            ("/spec/to/name", serde_json::json!("demo-ext-0")),
        ] {
            assert!(route.pointer(pointer) == Some(&want), "pointer {pointer}");
        }
    }

    #[test]
    fn render_bootstrap_route_uses_bootstrap_host() {
        let k = kafka("demo");
        let l = ingress_listener(ListenerType::Route);
        let route = render_bootstrap_route(&k, &l, "bootstrap.kafka.example.com").unwrap();
        for (pointer, want) in [
            ("/metadata/name", serde_json::json!("demo-ext-bootstrap")),
            (
                "/spec/host",
                serde_json::json!("bootstrap.kafka.example.com"),
            ),
            ("/spec/to/name", serde_json::json!("demo-ext-bootstrap")),
        ] {
            assert!(route.pointer(pointer) == Some(&want), "pointer {pointer}");
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::crd::{BrokerOverride, ListenerConfiguration};

    fn internal(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn empty_listeners_is_valid() {
        assert!(validate_listeners(&[], None).is_ok());
    }

    #[test]
    fn gssapi_keytab_path_is_dir_plus_keytab() {
        assert!(
            GSSAPI_KEYTAB_PATH == format!("{GSSAPI_KEYTAB_DIR}/keytab"),
            "the mounted keytab dir + item path must equal the rendered keytab_path"
        );
    }

    #[test]
    fn one_internal_is_valid() {
        let ls = [internal("PLAIN", 9092)];
        assert!(validate_listeners(&ls, None).is_ok());
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let ls = [internal("PLAIN", 9092), nodeport("PLAIN", 9094)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerName(_)));
        assert!(err.reason() == "DuplicateListenerName");
    }

    #[test]
    fn duplicate_port_is_rejected() {
        let ls = [internal("A", 9092), nodeport("B", 9092)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerPort(9092)));
    }

    #[test]
    fn ingress_without_tls_is_rejected() {
        // An ingress listener needs an internal listener too (NoInternalListener
        // would otherwise fire first), so validate it in isolation by giving it tls.
        let mut l = internal("ing", 9094);
        l.type_ = ListenerType::Ingress;
        l.tls = false;
        assert!(
            validate_listeners(&[l], None).unwrap_err().reason() == "ListenerIngressRequiresTls"
        );
    }

    #[test]
    fn route_without_tls_is_rejected() {
        let mut l = internal("rt", 9094);
        l.type_ = ListenerType::Route;
        l.tls = false;
        assert!(
            validate_listeners(&[l], None).unwrap_err().reason() == "ListenerIngressRequiresTls"
        );
    }

    #[test]
    fn ingress_without_bootstrap_host_is_rejected() {
        let mut l = internal("ing", 9094);
        l.type_ = ListenerType::Ingress;
        l.tls = true;
        assert!(
            validate_listeners(&[l], None).unwrap_err().reason()
                == "ListenerIngressBootstrapHostMissing"
        );
    }

    #[test]
    fn ingress_with_tls_and_bootstrap_host_and_internal_is_valid() {
        let internal_l = internal("PLAIN", 9092);
        let mut ing = internal("ext", 9094);
        ing.type_ = ListenerType::Ingress;
        ing.tls = true;
        ing.configuration = Some(ListenerConfiguration {
            bootstrap: Some(crate::crd::BootstrapConfig {
                host: Some("bootstrap.kafka.example.com".into()),
                ..Default::default()
            }),
            brokers: vec![],
            ingress_class: Some("nginx".into()),
        });
        validate_listeners(&[internal_l, ing], None).unwrap();
    }

    #[test]
    fn duplicate_broker_override_is_rejected() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(ListenerConfiguration {
            bootstrap: None,
            brokers: vec![
                BrokerOverride {
                    broker: 0,
                    ..Default::default()
                },
                BrokerOverride {
                    broker: 0,
                    ..Default::default()
                },
            ],
            ingress_class: None,
        });
        let err = validate_listeners(&[l], None).unwrap_err();
        assert!(err.reason() == "DuplicateBrokerOverride");
    }

    #[test]
    fn missing_internal_when_non_empty_is_rejected() {
        let ls = [nodeport("ext", 9094)];
        assert!(validate_listeners(&ls, None).unwrap_err().reason() == "NoInternalListener");
    }

    #[test]
    fn inter_broker_listener_must_match_a_listener() {
        let ls = [internal("PLAIN", 9092)];
        let err = validate_listeners(&ls, Some("MISSING")).unwrap_err();
        assert!(err.reason() == "InterBrokerListenerMissing");
    }

    #[test]
    fn inter_broker_listener_must_be_internal() {
        let ls = [internal("PLAIN", 9092), nodeport("ext", 9094)];
        let err = validate_listeners(&ls, Some("ext")).unwrap_err();
        assert!(err.reason() == "InterBrokerListenerNotInternal");
    }

    #[test]
    fn effective_name_explicit_wins() {
        assert!(effective_inter_broker_listener_name(&[], Some("FOO")) == "FOO");
    }

    #[test]
    fn effective_name_picks_first_internal() {
        let ls = [
            nodeport("ext", 9094),
            internal("ib", 9092),
            internal("other", 9095),
        ];
        assert!(effective_inter_broker_listener_name(&ls, None) == "ib");
    }

    #[test]
    fn effective_name_empty_defaults_to_plain() {
        assert!(effective_inter_broker_listener_name(&[], None) == "PLAIN");
    }

    #[test]
    fn validate_listeners_rejects_mtls_without_transport_tls() {
        let listeners = vec![Listener {
            name: "bad".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: false,
            authentication: Some(crate::crd::ListenerAuthentication::Tls),
            configuration: None,
            network_policy_peers: None,
        }];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(
            matches!(err, ValidationError::ListenerMtlsRequiresTransportTls(ref n) if n == "bad")
        );
    }

    #[test]
    fn validate_listeners_accepts_scram_without_tls() {
        let listeners = vec![Listener {
            name: "scram".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: false,
            authentication: Some(crate::crd::ListenerAuthentication::ScramSha512),
            configuration: None,
            network_policy_peers: None,
        }];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_accepts_tls_without_auth() {
        let listeners = vec![Listener {
            name: "tls".into(),
            port: 9093,
            type_: ListenerType::Internal,
            tls: true,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_accepts_mtls_with_tls() {
        let listeners = vec![Listener {
            name: "mtls".into(),
            port: 9095,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(crate::crd::ListenerAuthentication::Tls),
            configuration: None,
            network_policy_peers: None,
        }];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_accepts_scram_with_tls() {
        let listeners = vec![Listener {
            name: "scram".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(crate::crd::ListenerAuthentication::ScramSha256),
            configuration: None,
            network_policy_peers: None,
        }];
        validate_listeners(&listeners, None).unwrap();
    }

    // ---------------------------------------------------------------------
    // OAuth listener validation
    // ---------------------------------------------------------------------

    fn oauth_cfg_minimal() -> crate::crd::ListenerAuthenticationOAuth {
        crate::crd::ListenerAuthenticationOAuth {
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

    fn oauth_listener(
        name: &str,
        port: i32,
        tls: bool,
        cfg: crate::crd::ListenerAuthenticationOAuth,
    ) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls,
            authentication: Some(crate::crd::ListenerAuthentication::OAuth(Box::new(cfg))),
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_without_tls() {
        let listeners = vec![oauth_listener("oauth", 9095, false, oauth_cfg_minimal())];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthRequiresTransportTls");
        assert!(matches!(
            err,
            ValidationError::ListenerOauthRequiresTransportTls(ref n) if n == "oauth"
        ));
    }

    #[test]
    fn validate_listeners_accepts_oauth_with_http_jwks_uri() {
        let mut cfg = oauth_cfg_minimal();
        cfg.jwks_endpoint_uri = Some("http://issuer.example.com/jwks".into());
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_rejects_oauth_with_ftp_jwks_uri() {
        let mut cfg = oauth_cfg_minimal();
        cfg.jwks_endpoint_uri = Some("ftp://issuer.example.com/jwks".into());
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthInvalidUri");
        assert!(matches!(
            err,
            ValidationError::ListenerOauthJwksUriBadScheme(ref n) if n == "oauth"
        ));
    }

    #[test]
    fn validate_listeners_rejects_oauth_with_empty_issuer_uri() {
        let mut cfg = oauth_cfg_minimal();
        cfg.valid_issuer_uri = String::new();
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthInvalidUri");
        assert!(matches!(
            err,
            ValidationError::ListenerOauthIssuerUriEmpty(ref n) if n == "oauth"
        ));
    }

    #[test]
    fn validate_listeners_accepts_oauth_with_non_uri_issuer_string() {
        // The broker compares the `iss` claim as a literal string. The CRD does
        // not require `validIssuerUri` to parse as a URL — Keycloak deployments
        // commonly use e.g. `kafka-cluster` as the issuer.
        let mut cfg = oauth_cfg_minimal();
        cfg.valid_issuer_uri = "kafka-cluster".into();
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_rejects_oauth_with_short_jwks_refresh() {
        let mut cfg = oauth_cfg_minimal();
        cfg.jwks_refresh_seconds = Some(29);
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthInvalidRefresh");
        assert!(matches!(
            err,
            ValidationError::ListenerOauthJwksRefreshTooSmall { ref listener, got: 29 } if listener == "oauth"
        ));
    }

    #[test]
    fn validate_listeners_accepts_two_oauth_listeners_with_identical_config() {
        let cfg = oauth_cfg_minimal();
        let listeners = vec![
            oauth_listener("oauth-a", 9095, true, cfg.clone()),
            oauth_listener("oauth-b", 9096, true, cfg),
        ];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    fn validate_listeners_accepts_two_oauth_listeners_differing_only_in_enable_oauth_bearer() {
        let mut a = oauth_cfg_minimal();
        a.enable_oauth_bearer = true;
        let mut b = oauth_cfg_minimal();
        b.enable_oauth_bearer = false;
        let listeners = vec![
            oauth_listener("oauth-a", 9095, true, a),
            oauth_listener("oauth-b", 9096, true, b),
        ];
        validate_listeners(&listeners, None).unwrap();
    }

    #[test]
    // Table-driven test: a single `perturbations` vec literal walks every
    // field in `oauth_canonical`'s output. Splitting the function would
    // obscure the field-by-field correspondence with the OAuth config
    // surface and force the base fixture to be duplicated or threaded
    // through a helper for no real gain.
    fn validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field()
    {
        // Walk every field in `oauth_canonical`'s output (i.e. every
        // field except `enable_oauth_bearer`, which is intentionally
        // masked). For each, build a base full-config OAuth listener
        // plus a sibling that differs only in that one field, and
        // assert the validator rejects the pair with
        // ConflictingOAuthListenerConfig. This guards against a future
        // `oauth_canonical` refactor that accidentally masks too much
        // and would otherwise let divergent broker-global OAuth config
        // through unnoticed.
        let base = crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
            valid_audience: Some("kafka".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
            jwks_refresh_seconds: Some(300),
            max_clock_skew_seconds: Some(30),
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
        let perturbations: Vec<(&str, crate::crd::ListenerAuthenticationOAuth)> = vec![
            (
                "valid_issuer_uri",
                crate::crd::ListenerAuthenticationOAuth {
                    valid_issuer_uri: "https://other.example.com/".into(),
                    ..base.clone()
                },
            ),
            (
                "jwks_endpoint_uri",
                crate::crd::ListenerAuthenticationOAuth {
                    jwks_endpoint_uri: Some("https://other.example.com/jwks".into()),
                    ..base.clone()
                },
            ),
            (
                "valid_audience",
                crate::crd::ListenerAuthenticationOAuth {
                    valid_audience: Some("other-kafka".into()),
                    ..base.clone()
                },
            ),
            (
                "user_name_claim",
                crate::crd::ListenerAuthenticationOAuth {
                    user_name_claim: Some("sub".into()),
                    ..base.clone()
                },
            ),
            (
                "custom_claim_check",
                crate::crd::ListenerAuthenticationOAuth {
                    custom_claim_check: Some("$.scope[?@ == 'kafka.read']".into()),
                    ..base.clone()
                },
            ),
            (
                "jwks_refresh_seconds",
                crate::crd::ListenerAuthenticationOAuth {
                    jwks_refresh_seconds: Some(600),
                    ..base.clone()
                },
            ),
            (
                "max_clock_skew_seconds",
                crate::crd::ListenerAuthenticationOAuth {
                    max_clock_skew_seconds: Some(120),
                    ..base.clone()
                },
            ),
            (
                "tls_trusted_certificates",
                crate::crd::ListenerAuthenticationOAuth {
                    tls_trusted_certificates: vec![crate::crd::TlsTrustedCertificate {
                        secret_name: "different-secret".into(),
                        certificate: "tls.crt".into(),
                    }],
                    ..base.clone()
                },
            ),
            // access_token_is_jwt flips the validator mode. The
            // perturbed config switches to introspection mode AND wires up
            // the introspection-mode required fields so the cross-mode
            // validation doesn't reject the perturbed config standalone
            // before we even get to the conflict check.
            (
                "access_token_is_jwt",
                crate::crd::ListenerAuthenticationOAuth {
                    access_token_is_jwt: false,
                    jwks_endpoint_uri: None,
                    introspection_endpoint_uri: Some("https://idp.example/introspect".into()),
                    client_id: Some("kafka-broker".into()),
                    client_secret: Some(crate::crd::OauthClientSecretRef {
                        secret_name: "introspection-creds".into(),
                        key: "client-secret".into(),
                    }),
                    ..base.clone()
                },
            ),
            (
                "max_seconds_without_reauthentication",
                crate::crd::ListenerAuthenticationOAuth {
                    max_seconds_without_reauthentication: Some(600),
                    ..base.clone()
                },
            ),
            (
                "valid_token_type",
                crate::crd::ListenerAuthenticationOAuth {
                    valid_token_type: Some("JWT".into()),
                    ..base.clone()
                },
            ),
            (
                "fallback_user_name_claim",
                crate::crd::ListenerAuthenticationOAuth {
                    fallback_user_name_claim: Some("client_id".into()),
                    ..base.clone()
                },
            ),
            (
                "fallback_user_name_prefix",
                crate::crd::ListenerAuthenticationOAuth {
                    fallback_user_name_prefix: Some("svc-".into()),
                    ..base.clone()
                },
            ),
            (
                "groups_claim",
                crate::crd::ListenerAuthenticationOAuth {
                    groups_claim: Some("$.groups".into()),
                    ..base.clone()
                },
            ),
            (
                "groups_claim_delimiter",
                crate::crd::ListenerAuthenticationOAuth {
                    groups_claim_delimiter: Some(",".into()),
                    ..base.clone()
                },
            ),
            (
                "jwks_min_refresh_pause_seconds",
                crate::crd::ListenerAuthenticationOAuth {
                    jwks_min_refresh_pause_seconds: Some(5),
                    ..base.clone()
                },
            ),
            (
                "jwks_expiry_seconds",
                crate::crd::ListenerAuthenticationOAuth {
                    jwks_expiry_seconds: Some(3600),
                    ..base.clone()
                },
            ),
            (
                "jwks_ignore_key_use",
                crate::crd::ListenerAuthenticationOAuth {
                    jwks_ignore_key_use: Some(true),
                    ..base.clone()
                },
            ),
        ];
        for (field, perturbed) in perturbations {
            let listeners = vec![
                oauth_listener("oauth-a", 9095, true, base.clone()),
                oauth_listener("oauth-b", 9096, true, perturbed),
            ];
            let err = validate_listeners(&listeners, None).expect_err(&format!(
                "expected ConflictingOAuthListenerConfig when only `{field}` differs"
            ));
            assert!(
                matches!(err, ValidationError::ConflictingOAuthListenerConfig),
                "field {field}: got {err:?}"
            );
        }
    }

    #[test]
    fn validate_listeners_rejects_two_oauth_listeners_with_divergent_introspection_config() {
        // Mirror of the canonical-divergence walk but for the
        // introspection-mode-only fields. The JWT-mode base used by the
        // sibling test can't carry these fields (cross-mode validation
        // rejects them), so build a dedicated introspection-mode base
        // fixture here.
        let base = crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://issuer.example.com/".into(),
            jwks_endpoint_uri: None,
            valid_audience: Some("kafka".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: Some(30),
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://idp.example/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: "base-secret".into(),
                key: "k".into(),
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
        let perturbations: Vec<(&str, crate::crd::ListenerAuthenticationOAuth)> = vec![
            (
                "introspection_endpoint_uri",
                crate::crd::ListenerAuthenticationOAuth {
                    introspection_endpoint_uri: Some("https://different/introspect".into()),
                    ..base.clone()
                },
            ),
            (
                "user_info_endpoint_uri",
                crate::crd::ListenerAuthenticationOAuth {
                    user_info_endpoint_uri: Some("https://different/userinfo".into()),
                    ..base.clone()
                },
            ),
            (
                "client_id",
                crate::crd::ListenerAuthenticationOAuth {
                    client_id: Some("other-client".into()),
                    ..base.clone()
                },
            ),
            (
                "client_secret",
                crate::crd::ListenerAuthenticationOAuth {
                    client_secret: Some(crate::crd::OauthClientSecretRef {
                        secret_name: "other".into(),
                        key: "k".into(),
                    }),
                    ..base.clone()
                },
            ),
            (
                "introspection_http_timeout_seconds",
                crate::crd::ListenerAuthenticationOAuth {
                    introspection_http_timeout_seconds: Some(20),
                    ..base.clone()
                },
            ),
        ];
        for (field, perturbed) in perturbations {
            let listeners = vec![
                oauth_listener("oauth-a", 9095, true, base.clone()),
                oauth_listener("oauth-b", 9096, true, perturbed),
            ];
            let err = validate_listeners(&listeners, None).expect_err(&format!(
                "expected ConflictingOAuthListenerConfig when only `{field}` differs"
            ));
            assert!(
                matches!(err, ValidationError::ConflictingOAuthListenerConfig),
                "field {field}: got {err:?}"
            );
        }
    }

    // -----------------------------------------------------------------
    // Cross-mode (accessTokenIsJwt) validation tests
    // -----------------------------------------------------------------

    fn oauth_introspection_cfg_minimal() -> crate::crd::ListenerAuthenticationOAuth {
        crate::crd::ListenerAuthenticationOAuth {
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
            introspection_endpoint_uri: Some("https://idp.example/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: "introspection-creds".into(),
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
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_jwt_mode_without_jwks_endpoint_uri() {
        let mut cfg = oauth_cfg_minimal();
        cfg.jwks_endpoint_uri = None;
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=true requires jwksEndpointUri"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_introspection_mode_without_endpoint_uri() {
        let mut cfg = oauth_introspection_cfg_minimal();
        cfg.introspection_endpoint_uri = None;
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=false requires introspectionEndpointUri"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_introspection_mode_without_client_id() {
        let mut cfg = oauth_introspection_cfg_minimal();
        cfg.client_id = None;
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=false requires clientId"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_introspection_mode_without_client_secret() {
        let mut cfg = oauth_introspection_cfg_minimal();
        cfg.client_secret = None;
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=false requires clientSecret"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_jwt_mode_with_introspection_fields() {
        // JWT-mode base (jwksEndpointUri set, accessTokenIsJwt=true) that
        // also accidentally sets an introspection-mode field — should be
        // rejected, since the two configs imply contradictory broker
        // behaviour.
        let mut cfg = oauth_cfg_minimal();
        cfg.introspection_endpoint_uri = Some("https://idp.example/introspect".into());
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=true forbids introspection-mode fields"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_introspection_mode_with_jwks_endpoint_uri() {
        let mut cfg = oauth_introspection_cfg_minimal();
        cfg.jwks_endpoint_uri = Some("https://issuer.example.com/jwks".into());
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=false forbids jwksEndpointUri"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_listeners_rejects_oauth_userinfo_endpoint_without_introspection_mode() {
        // userInfoEndpointUri is an introspection-mode-only field; setting
        // it on a JWT-mode listener must be rejected by the
        // accessTokenIsJwt=true forbids-introspection-fields rule.
        let mut cfg = oauth_cfg_minimal();
        cfg.user_info_endpoint_uri = Some("https://idp.example/userinfo".into());
        let listeners = vec![oauth_listener("oauth", 9095, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthAccessTokenIsJwtInvalid");
        match err {
            ValidationError::ListenerOauthAccessTokenIsJwtInvalid(msg) => {
                assert!(
                    msg.contains("accessTokenIsJwt=true forbids introspection-mode fields"),
                    "msg: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn listener_protocol_table_all_legal_tuples() {
        use crabka_security::ListenerProtocol::*;
        let cases = [
            (false, None, Plaintext),
            (true, None, Ssl),
            (
                false,
                Some(crate::crd::ListenerAuthentication::ScramSha512),
                SaslPlaintext,
            ),
            (
                false,
                Some(crate::crd::ListenerAuthentication::ScramSha256),
                SaslPlaintext,
            ),
            (
                true,
                Some(crate::crd::ListenerAuthentication::ScramSha512),
                SaslSsl,
            ),
            (
                true,
                Some(crate::crd::ListenerAuthentication::ScramSha256),
                SaslSsl,
            ),
            (true, Some(crate::crd::ListenerAuthentication::Tls), Ssl),
        ];
        for (tls, auth, expected) in cases {
            let l = Listener {
                name: "x".into(),
                port: 1,
                type_: ListenerType::Internal,
                tls,
                authentication: auth.clone(),
                configuration: None,
                network_policy_peers: None,
            };
            assert!(
                listener_protocol(&l) == expected,
                "tls={tls}, auth={auth:?}"
            );
        }
    }

    // -----------------------------------------------------------------
    // validTokenType cross-mode validation
    // -----------------------------------------------------------------

    #[test]
    fn validate_listeners_rejects_valid_token_type_in_introspection_mode() {
        // Introspection-mode listener with validTokenType set must be
        // rejected: introspection responses carry no JWT header, so a
        // `typ` check has nothing to bind against. Mirrors the new
        // `ListenerOauthValidTokenTypeRejectedInIntrospectionMode`
        // variant.
        let cfg = crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://iss.example/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://iss.example/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: "creds".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: Some("JWT".into()),
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let listeners = vec![oauth_listener("oauth", 9096, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthValidTokenTypeRejectedInIntrospectionMode");
        assert!(
            matches!(
                err,
                ValidationError::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(_)
            ),
            "unexpected error variant: {err:?}"
        );
    }

    // -----------------------------------------------------------------
    // GSSAPI listener validation
    // -----------------------------------------------------------------

    fn gssapi_cfg_with_service(svc: &str) -> crate::crd::ListenerAuthenticationGssapi {
        crate::crd::ListenerAuthenticationGssapi {
            keytab_secret_ref: crate::crd::KeytabSecretRef {
                secret_name: "kt".into(),
                key: "keytab".into(),
            },
            service_name: Some(svc.into()),
            principal_to_local_rules: vec!["DEFAULT".into()],
            realm: None,
            kdc: None,
            max_time_skew: None,
        }
    }

    fn gssapi_cfg_with_rules(rules: Vec<String>) -> crate::crd::ListenerAuthenticationGssapi {
        let mut c = gssapi_cfg_with_service("kafka");
        c.principal_to_local_rules = rules;
        c
    }

    fn gssapi_listener(
        name: &str,
        port: i32,
        tls: bool,
        cfg: crate::crd::ListenerAuthenticationGssapi,
    ) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls,
            authentication: Some(crate::crd::ListenerAuthentication::Gssapi(cfg)),
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn gssapi_listener_without_keytab_is_invalid() {
        let g = crate::crd::ListenerAuthenticationGssapi {
            keytab_secret_ref: crate::crd::KeytabSecretRef {
                secret_name: String::new(),
                key: "k".into(),
            },
            service_name: None,
            principal_to_local_rules: vec![],
            realm: None,
            kdc: None,
            max_time_skew: None,
        };
        let l = gssapi_listener("gss", 9092, false, g);
        assert!(
            validate_listeners(&[l], None).unwrap_err().reason()
                == "ListenerGssapiKeytabSecretMissing"
        );
    }

    #[test]
    fn gssapi_listener_with_bad_rule_is_invalid() {
        let g = gssapi_cfg_with_rules(vec!["NOT_A_RULE:::".into()]);
        let l = gssapi_listener("gss", 9092, false, g);
        assert!(
            validate_listeners(&[l], None).unwrap_err().reason() == "ListenerGssapiInvalidRule"
        );
    }

    #[test]
    fn divergent_gssapi_listeners_conflict() {
        let a = gssapi_cfg_with_service("kafka");
        let b = gssapi_cfg_with_service("other");
        let la = gssapi_listener("g1", 9092, false, a);
        let lb = gssapi_listener("g2", 9093, false, b);
        assert!(
            validate_listeners(&[la, lb], None).unwrap_err().reason()
                == "ListenerGssapiConfigConflict"
        );
    }

    #[test]
    fn gssapi_listener_allows_plaintext_and_ssl() {
        // GSSAPI brings its own RFC 4752 security layer — TLS is optional.
        let plain = gssapi_listener("g", 9092, false, gssapi_cfg_with_service("kafka"));
        validate_listeners(&[plain], None).expect("plaintext+gssapi is valid");
        let ssl = gssapi_listener("g", 9092, true, gssapi_cfg_with_service("kafka"));
        validate_listeners(&[ssl], None).expect("ssl+gssapi is valid");
    }

    #[test]
    fn inter_broker_gssapi_without_kerberos_config_is_invalid() {
        let g = gssapi_cfg_with_service("kafka");
        let l = gssapi_listener("ib", 9092, false, g);
        assert!(
            validate_inter_broker_gssapi(std::slice::from_ref(&l), "ib", false)
                .unwrap_err()
                .reason()
                == "InterBrokerGssapiRequiresKerberosConfig"
        );
        validate_inter_broker_gssapi(&[l], "ib", true)
            .expect("ok when interBrokerKerberos present");
    }

    // -----------------------------------------------------------------
    // JWKS-only fields cross-mode validation
    // -----------------------------------------------------------------

    #[test]
    fn validate_listeners_rejects_jwks_fields_in_introspection_mode() {
        // Introspection-mode listener with one JWKS-only field set must
        // be rejected: JWKS isn't consulted at all in introspection
        // mode, so a JWKS refresher policy field has nothing to bind
        // against. Mirrors the new
        // `ListenerOauthJwksFieldsRejectedInIntrospectionMode` variant.
        let cfg = crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://iss.example/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://iss.example/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("kafka-broker".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: "creds".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: Some(1),
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        };
        let listeners = vec![oauth_listener("oauth", 9096, true, cfg)];
        let err = validate_listeners(&listeners, None).unwrap_err();
        assert!(err.reason() == "ListenerOauthJwksFieldsRejectedInIntrospectionMode");
        assert!(
            matches!(
                err,
                ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode(_)
            ),
            "unexpected error variant: {err:?}"
        );
    }
}

/// Per-broker resolved advertised address.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct AdvertisedAddress {
    pub host: String,
    pub port: i32,
}

/// Errors that block the computation of the advertised listeners.
///
/// They map onto `ListenersReady=False reason=PendingExternalAddresses`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AdvertisedError {
    PodNotScheduled { broker: i32 },
    NodeNotFound { broker: i32, node_name: String },
    NodeHasNoAddress { broker: i32, node_name: String },
    ServiceMissing { broker: i32, service_name: String },
    NodePortNotAllocated { broker: i32 },
    LoadBalancerPending { broker: i32, service_name: String },
    IngressBrokerHostMissing { broker: i32, listener: String },
}

#[allow(dead_code)]
impl AdvertisedError {
    pub fn message(&self) -> String {
        match self {
            Self::PodNotScheduled { broker } => {
                format!("pod for broker {broker} not yet scheduled")
            }
            Self::NodeNotFound { broker, node_name } => {
                format!("node {node_name} for broker {broker} not visible")
            }
            Self::NodeHasNoAddress { broker, node_name } => {
                format!("node {node_name} for broker {broker} has no addresses")
            }
            Self::ServiceMissing {
                broker,
                service_name,
            } => {
                format!("service {service_name} for broker {broker} missing")
            }
            Self::NodePortNotAllocated { broker } => {
                format!("nodePort for broker {broker} not allocated yet")
            }
            Self::LoadBalancerPending {
                broker,
                service_name,
            } => {
                format!("loadBalancer for service {service_name} (broker {broker}) not provisioned")
            }
            Self::IngressBrokerHostMissing { broker, listener } => format!(
                "listener '{listener}' broker {broker} has no configuration.brokers[].host \
                 (or advertisedHost); ingress/route listeners require a hostname per broker"
            ),
        }
    }
}

/// Computes the advertised host and port for one listener and broker.
///
/// `pod_node_name` is the `Pod.spec.nodeName` of the pod that hosts this
/// broker. It is `None` while the pod is not yet scheduled.
/// `nodes_by_name` is a map of all Nodes that the operator has observed.
/// `per_broker_service` is the per-broker Service that the operator just
/// rendered and applied. It is `None` until the apiserver returns it.
///
/// `ingress` and `route` listeners resolve their host from the
/// configuration, without a Node or Pod lookup, and advertise on port 443.
#[allow(dead_code)]
pub fn compute_advertised(
    listener: &Listener,
    broker_id: i32,
    pod_fqdn: &str,
    pod_node_name: Option<&str>,
    nodes_by_name: &std::collections::HashMap<String, Node>,
    per_broker_service: Option<&Service>,
) -> Result<AdvertisedAddress, AdvertisedError> {
    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    match listener.type_ {
        ListenerType::Internal => Ok(AdvertisedAddress {
            host: pod_fqdn.to_string(),
            port: listener.port,
        }),
        ListenerType::Nodeport => {
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let node_name =
                    pod_node_name.ok_or(AdvertisedError::PodNotScheduled { broker: broker_id })?;
                let node =
                    nodes_by_name
                        .get(node_name)
                        .ok_or_else(|| AdvertisedError::NodeNotFound {
                            broker: broker_id,
                            node_name: node_name.to_string(),
                        })?;
                let addrs = node.status.as_ref().and_then(|s| s.addresses.as_ref());
                addrs
                    .and_then(|a| {
                        a.iter()
                            .find(|x| x.type_ == "ExternalIP")
                            .or_else(|| a.iter().find(|x| x.type_ == "InternalIP"))
                            .map(|x| x.address.clone())
                    })
                    .ok_or_else(|| AdvertisedError::NodeHasNoAddress {
                        broker: broker_id,
                        node_name: node_name.to_string(),
                    })?
            };
            let port = if let Some(p) = override_.and_then(|o| o.advertised_port) {
                p
            } else if let Some(p) = override_.and_then(|o| o.node_port) {
                p
            } else {
                let svc = per_broker_service.ok_or_else(|| AdvertisedError::ServiceMissing {
                    broker: broker_id,
                    service_name: String::new(),
                })?;
                svc.spec
                    .as_ref()
                    .and_then(|s| s.ports.as_ref())
                    .and_then(|ps| ps.first().and_then(|p| p.node_port))
                    .ok_or(AdvertisedError::NodePortNotAllocated { broker: broker_id })?
            };
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Loadbalancer => {
            let svc_name = per_broker_service
                .and_then(|s| s.metadata.name.clone())
                .unwrap_or_default();
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let svc = per_broker_service.ok_or_else(|| AdvertisedError::ServiceMissing {
                    broker: broker_id,
                    service_name: String::new(),
                })?;
                let ingress = svc
                    .status
                    .as_ref()
                    .and_then(|st| st.load_balancer.as_ref())
                    .and_then(|lb| lb.ingress.as_ref())
                    .and_then(|ig| ig.first())
                    .ok_or_else(|| AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name.clone(),
                    })?;
                ingress
                    .hostname
                    .clone()
                    .or_else(|| ingress.ip.clone())
                    .ok_or(AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name,
                    })?
            };
            let port = override_
                .and_then(|o| o.advertised_port)
                .unwrap_or(listener.port);
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Ingress | ListenerType::Route => {
            // Host comes from config (override advertisedHost, else the broker's
            // `host`). The advertised port is always 443 — the ingress
            // controller / router terminates there — unless overridden.
            let host = ingress_broker_host(listener, broker_id).ok_or_else(|| {
                AdvertisedError::IngressBrokerHostMissing {
                    broker: broker_id,
                    listener: listener.name.clone(),
                }
            })?;
            let port = override_
                .and_then(|o| o.advertised_port)
                .unwrap_or(INGRESS_PORT);
            Ok(AdvertisedAddress { host, port })
        }
    }
}

#[cfg(test)]
mod advertised_tests {
    use std::collections::HashMap;

    use assert2::assert;
    use k8s_openapi::api::core::v1::{
        LoadBalancerIngress, LoadBalancerStatus, Node, NodeAddress, NodeStatus, Service,
        ServicePort, ServiceSpec, ServiceStatus,
    };

    use super::*;

    fn internal(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }
    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }
    fn loadbalancer(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Loadbalancer,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn internal_uses_pod_fqdn() {
        let l = internal("PLAIN", 9092);
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod.svc.local", None, &nodes, None).unwrap();
        assert!(
            a == AdvertisedAddress {
                host: "pod.svc.local".into(),
                port: 9092
            }
        );
    }

    #[test]
    fn nodeport_pending_when_pod_unscheduled() {
        let l = nodeport("ext", 9094);
        let nodes = HashMap::new();
        let err = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap_err();
        assert!(matches!(
            err,
            AdvertisedError::PodNotScheduled { broker: 0 }
        ));
    }

    #[test]
    fn nodeport_resolves_external_ip_from_node() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![
                        NodeAddress {
                            type_: "InternalIP".into(),
                            address: "10.0.0.1".into(),
                        },
                        NodeAddress {
                            type_: "ExternalIP".into(),
                            address: "1.2.3.4".into(),
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert!(
            a == AdvertisedAddress {
                host: "1.2.3.4".into(),
                port: 32100
            }
        );
    }

    #[test]
    fn nodeport_falls_back_to_internal_ip() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".into(),
                        address: "10.0.0.1".into(),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert!(a.host == "10.0.0.1");
    }

    #[test]
    fn nodeport_pending_when_service_has_no_nodeport() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".into(),
                        address: "10.0.0.1".into(),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: None,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::NodePortNotAllocated { .. }));
    }

    #[test]
    fn loadbalancer_resolves_hostname() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-lb-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec::default()),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        hostname: Some("lb.example.com".into()),
                        ip: None,
                        ip_mode: None,
                        ports: None,
                    }]),
                }),
                ..Default::default()
            }),
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert!(
            a == AdvertisedAddress {
                host: "lb.example.com".into(),
                port: 9094
            }
        );
    }

    #[test]
    fn loadbalancer_pending_when_status_missing() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-lb-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec::default()),
            status: None,
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::LoadBalancerPending { .. }));
    }

    #[test]
    fn override_advertised_host_wins() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(crate::crd::ListenerConfiguration {
            bootstrap: None,
            brokers: vec![crate::crd::BrokerOverride {
                broker: 0,
                advertised_host: Some("public.host".into()),
                ..Default::default()
            }],
            ingress_class: None,
        });
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", None, &nodes, Some(&svc)).unwrap();
        assert!(a.host == "public.host");
        assert!(a.port == 32100);
    }

    fn ingress(name: &str, port: i32, broker_host: Option<&str>) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Ingress,
            tls: true,
            authentication: None,
            configuration: broker_host.map(|h| crate::crd::ListenerConfiguration {
                bootstrap: Some(crate::crd::BootstrapConfig {
                    host: Some("bootstrap.example.com".into()),
                    ..Default::default()
                }),
                brokers: vec![crate::crd::BrokerOverride {
                    broker: 0,
                    host: Some(h.into()),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        }
    }

    #[test]
    fn ingress_uses_config_host_and_port_443() {
        let l = ingress("ext", 9094, Some("broker-0.example.com"));
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap();
        assert!(
            a == AdvertisedAddress {
                host: "broker-0.example.com".into(),
                port: 443,
            }
        );
    }

    #[test]
    fn route_uses_config_host_and_port_443() {
        let mut l = ingress("ext", 9094, Some("broker-0.example.com"));
        l.type_ = ListenerType::Route;
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap();
        assert!(a.host == "broker-0.example.com");
        assert!(a.port == 443);
    }

    #[test]
    fn ingress_advertised_port_override_wins_over_443() {
        let mut l = ingress("ext", 9094, Some("broker-0.example.com"));
        if let Some(cfg) = l.configuration.as_mut() {
            cfg.brokers[0].advertised_port = Some(8443);
        }
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap();
        assert!(a.port == 8443);
    }

    #[test]
    fn ingress_missing_broker_host_errors() {
        let l = ingress("ext", 9094, None);
        let nodes = HashMap::new();
        let err = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap_err();
        assert!(matches!(
            err,
            AdvertisedError::IngressBrokerHostMissing { broker: 0, .. }
        ));
    }
}

/// Inputs that render the TLS block of the broker config file for one
/// broker.
///
/// The operator builds this once for each reconcile and puts it into every
/// per-broker TOML. Only the leaf cert paths differ for each broker,
/// because the cert files carry the broker id inside the same mount.
#[derive(Debug, Clone)]
pub struct BrokerTlsRender {
    /// For example `"Ssl"` or `"SaslSsl"`. The operator writes it as the
    /// `controller_listener_protocol = "<v>"` line.
    pub controller_listener_protocol: String,
    /// Path to the broker's own cert, for example
    /// `/etc/crabka/broker-tls/0.crt`.
    pub cert_path: String,
    /// Path to the broker's own private key.
    pub key_path: String,
    /// Path to the cluster CA cert that verifies peer client certs.
    pub client_ca_path: String,
    /// `"Required"` for inter-broker mTLS.
    pub client_auth: String,
    /// Path to the cluster CA cert that gives the TLS trust roots when
    /// this broker dials the controller listener of a PEER (KIP-595
    /// quorum mTLS). The operator writes it as the
    /// `trust_roots_path = "<v>"` line inside `[tls_config]`.
    pub trust_roots_path: String,
}

/// Intermediate shape that renders the `[authorization]` TOML block.
///
/// [`AuthorizationRender::from_spec`] and
/// [`AuthorizationRender::auto_injected_simple`] build it from
/// `Kafka.spec.authorization` and the delegation-token enablement flag.
struct AuthorizationRender {
    /// `"simple"` or `"opa"`. These match the `AuthzType` wire names of
    /// the broker in `snake_case`. The value is never `"allow_all"`: that
    /// is the omit-block case, not a render shape.
    kind: &'static str,
    /// Final super-user list for the TOML. The operator always renders it,
    /// also when it is empty, so that the authoritative-overwrite path of
    /// the broker's `[authorization].super_users` is deterministic.
    super_users: Vec<String>,
    /// `Some` if and only if `kind == "opa"`. It holds the
    /// `[authorization.opa]` subtable inputs.
    opa: Option<AuthorizationOpaRender>,
}

/// `[authorization.opa]` subtable inputs.
///
/// This struct omits `initial_cache_capacity` on purpose. The
/// `FileOpaConfig` of the broker uses `deny_unknown_fields` and holds only
/// `maximum_cache_size`.
struct AuthorizationOpaRender {
    url: String,
    allow_on_error: Option<bool>,
    maximum_cache_size: Option<u32>,
    expire_after_ms: Option<i64>,
}

impl AuthorizationRender {
    /// Builds the render from an explicit `Kafka.spec.authorization`.
    ///
    /// When `delegation_token_enabled` is set, this function merges
    /// `"ANONYMOUS"` into `super_users`. The merge removes duplicates and
    /// keeps the order. The delegation-token act-as path needs the
    /// PLAINTEXT inter-broker principal of the operator to be a
    /// super-user.
    fn from_spec(a: &crate::crd::kafka::Authorization, delegation_token_enabled: bool) -> Self {
        match a {
            crate::crd::kafka::Authorization::Simple(s) => Self {
                kind: "simple",
                super_users: merge_anonymous(&s.super_users, delegation_token_enabled),
                opa: None,
            },
            crate::crd::kafka::Authorization::Opa(o) => Self {
                kind: "opa",
                super_users: merge_anonymous(&o.super_users, delegation_token_enabled),
                opa: Some(AuthorizationOpaRender {
                    url: o.url.clone(),
                    allow_on_error: o.allow_on_error,
                    maximum_cache_size: o.maximum_cache_size,
                    expire_after_ms: o.expire_after_ms,
                }),
            },
        }
    }

    /// Builds the injected default authorization block.
    ///
    /// When `delegation_token_enabled` is set but
    /// `Kafka.spec.authorization` is unset, the operator injects a
    /// `type = "simple", super_users = ["ANONYMOUS"]` block without a
    /// message, so that the act-as path continues to work. The spec
    /// documents this in §2.2.
    fn auto_injected_simple() -> Self {
        Self {
            kind: "simple",
            super_users: vec!["ANONYMOUS".to_string()],
            opa: None,
        }
    }
}

/// Merges `"ANONYMOUS"` into `base` when `inject` is set.
///
/// The merge keeps the order of `base` and removes duplicates. If `base`
/// already holds `"ANONYMOUS"`, the merge changes nothing.
fn merge_anonymous(base: &[String], inject: bool) -> Vec<String> {
    let mut out: Vec<String> = base.to_vec();
    if inject && !out.iter().any(|s| s == "ANONYMOUS") {
        out.push("ANONYMOUS".to_string());
    }
    out
}

/// Renders a `Vec<String>` as a TOML inline string array, for example
/// `["a", "b"]`.
///
/// This function puts double quotes around each element and escapes
/// nothing. The principal strings that the operator writes never contain
/// `"` or `\`.
fn toml_string_array(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|s| format!("\"{s}\"")).collect();
    format!("[{}]", quoted.join(", "))
}

/// Renders the complete TOML for one broker.
///
/// The output holds the cluster-wide content and the advertised addresses
/// of this broker. The render is deterministic: the same input always
/// gives byte-identical output, so the config hash is stable.
// Each arg is an independent operator-owned broker-pod render input —
// extraction obscures the single deterministic render shape.
fn render_remote_storage(
    out: &mut String,
    tiered_storage: Option<&crate::crd::kafka::TieredStorage>,
) {
    use std::fmt::Write as _;
    // KIP-405: `[remote_storage]` block. Presence of
    // `Kafka.spec.tieredStorage` flips on the broker-wide tiered-storage
    // stack. The storage path is operator-owned and
    // matches the `tier-storage` volume mounted at the same path by the
    // broker pod template (`kafka_node_pool.rs`).
    if let Some(ts) = tiered_storage {
        match ts.kind {
            crate::crd::kafka::TieredStorageType::Local => {
                let _ = writeln!(out, "[remote_storage]");
                let _ = writeln!(out, "storage_dir = \"{TIER_STORAGE_PATH}\"");
                out.push('\n');
            }
            crate::crd::kafka::TieredStorageType::S3 => {
                // Reconciler validation guarantees `s3` is populated when
                // `kind == S3`; if it isn't, we'd rather panic in unit tests
                // than emit silently-broken TOML.
                let s3 = ts
                    .s3
                    .as_ref()
                    .expect("TieredStorageType::S3 requires spec.tieredStorage.s3");
                let _ = writeln!(out, "[remote_storage]");
                let _ = writeln!(out);
                let _ = writeln!(out, "[remote_storage.s3]");
                let _ = writeln!(out, "bucket = \"{}\"", toml_escape(&s3.bucket));
                let _ = writeln!(out, "region = \"{}\"", toml_escape(&s3.region));
                if let Some(prefix) = &s3.prefix {
                    let _ = writeln!(out, "prefix = \"{}\"", toml_escape(prefix));
                }
                if let Some(endpoint) = &s3.endpoint {
                    let _ = writeln!(out, "endpoint = \"{}\"", toml_escape(endpoint));
                }
                if s3.allow_http {
                    let _ = writeln!(out, "allow_http = true");
                }
                if let Some(mt) = s3.multipart_threshold {
                    let _ = writeln!(out, "multipart_threshold = {mt}");
                }
                if let Some(cs) = s3.multipart_chunk_size {
                    let _ = writeln!(out, "multipart_chunk_size = {cs}");
                }
                // Credentials are intentionally NOT rendered into the TOML.
                // The operator wires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                // as pod env via secretKeyRef (see `kafka_node_pool.rs`);
                // `object_store`'s `AmazonS3Builder` picks them up through
                // the AWS credential chain when the corresponding TOML keys
                // are absent.
                out.push('\n');
            }
            crate::crd::kafka::TieredStorageType::Gcs => {
                // Reconciler validation guarantees `gcs` is populated when
                // `kind == Gcs`; if it isn't, we'd rather panic in unit tests
                // than emit silently-broken TOML.
                let gcs = ts
                    .gcs
                    .as_ref()
                    .expect("TieredStorageType::Gcs requires spec.tieredStorage.gcs");
                let _ = writeln!(out, "[remote_storage]");
                let _ = writeln!(out);
                let _ = writeln!(out, "[remote_storage.gcs]");
                let _ = writeln!(out, "bucket = \"{}\"", toml_escape(&gcs.bucket));
                if let Some(prefix) = &gcs.prefix {
                    let _ = writeln!(out, "prefix = \"{}\"", toml_escape(prefix));
                }
                if let Some(endpoint) = &gcs.endpoint {
                    let _ = writeln!(out, "endpoint = \"{}\"", toml_escape(endpoint));
                }
                if gcs.allow_http {
                    let _ = writeln!(out, "allow_http = true");
                }
                // Credentials: unlike S3 (env vars), GCS credentials are a
                // JSON key FILE and `object_store`'s GCS builder reads the
                // path directly (it does NOT consult
                // GOOGLE_APPLICATION_CREDENTIALS), so the path MUST be in the
                // TOML. When `credentials` is set, the operator mounts the
                // Secret as a file at `<GCS_CREDENTIALS_DIR>/key.json` (see
                // `kafka_node_pool.rs`) and we point `service_account_path` at
                // it. When absent, we render NO credential line — the broker
                // pod uses keyless Workload Identity / ADC via the bound KSA.
                if gcs.credentials.is_some() {
                    let _ = writeln!(
                        out,
                        "service_account_path = \"{GCS_CREDENTIALS_DIR}/{GCS_CREDENTIALS_FILE}\""
                    );
                }
                if let Some(mt) = gcs.multipart_threshold {
                    let _ = writeln!(out, "multipart_threshold = {mt}");
                }
                if let Some(cs) = gcs.multipart_chunk_size {
                    let _ = writeln!(out, "multipart_chunk_size = {cs}");
                }
                out.push('\n');
            }
        }

        // KIP-405: `[remote_storage.kafka_metadata]` is ALWAYS emitted when
        // tiered storage is enabled. When the CR has `type: InMemory` we emit
        // `in_memory = true` so the broker selects the in-memory RLMM (the
        // broker's default when the backend is present is topic-backed, so
        // omitting the block would silently run topic-backed instead of InMemory).
        let is_in_memory = ts
            .metadata_manager
            .as_ref()
            .is_some_and(|mm| matches!(mm.kind, crate::crd::kafka::MetadataManagerType::InMemory));
        let _ = writeln!(out, "[remote_storage.kafka_metadata]");
        if is_in_memory {
            let _ = writeln!(out, "in_memory = true");
        } else {
            let topic = ts
                .metadata_manager
                .as_ref()
                .and_then(|mm| mm.topic.as_ref());
            if let Some(t) = topic {
                let _ = writeln!(out, "bootstrap = \"{}\"", toml_escape(&t.bootstrap));
                if let Some(np) = t.num_partitions {
                    let _ = writeln!(out, "num_partitions = {np}");
                }
                if let Some(rf) = t.replication {
                    let _ = writeln!(out, "replication = {rf}");
                }
                if let Some(value) = t.topic_create_timeout {
                    let _ = writeln!(out, "topic_create_timeout = \"{}\"", value.human());
                }
                if let Some(value) = t.fetch_max_wait {
                    let _ = writeln!(out, "fetch_max_wait = \"{}\"", value.human());
                }
                if let Some(value) = t.fetch_max_bytes {
                    let _ = writeln!(out, "fetch_max_bytes = \"{}\"", value.human());
                }
                if let Some(value) = t.fetch_retry_backoff {
                    let _ = writeln!(out, "fetch_retry_backoff = \"{}\"", value.human());
                }
                if let Some(value) = t.event_queue_capacity {
                    let _ = writeln!(out, "event_queue_capacity = {value}");
                }
                if let Some(value) = t.snapshot_interval {
                    let _ = writeln!(out, "snapshot_interval = \"{}\"", value.human());
                }
            }
        }
        out.push('\n');
    }
}

fn render_listener_sections(
    out: &mut String,
    broker_id: i32,
    listeners: &[Listener],
    addresses: &std::collections::BTreeMap<String, AdvertisedAddress>,
    clients_ca_path: Option<&str>,
) {
    use std::fmt::Write as _;
    for l in listeners {
        let adv = addresses
            .get(&l.name)
            .map(|a| format!("{}:{}", a.host, a.port))
            .unwrap_or_default();
        let proto = listener_protocol(l);
        let proto_str = match proto {
            ListenerProtocol::Plaintext => "Plaintext",
            ListenerProtocol::Ssl => "Ssl",
            ListenerProtocol::SaslPlaintext => "SaslPlaintext",
            ListenerProtocol::SaslSsl => "SaslSsl",
        };
        let _ = writeln!(out, "[[listeners]]");
        let _ = writeln!(out, "name = \"{}\"", l.name);
        let _ = writeln!(out, "bind_addr = \"0.0.0.0:{}\"", l.port);
        let _ = writeln!(out, "advertised = \"{adv}\"");
        let _ = writeln!(out, "protocol = \"{proto_str}\"");

        if l.tls {
            let cert_path = format!("/etc/crabka/broker-tls/{broker_id}.crt");
            let key_path = format!("/etc/crabka/broker-tls/{broker_id}.key");
            let needs_client_ca = matches!(l.authentication, Some(ListenerAuthentication::Tls));
            let client_auth = if needs_client_ca {
                "Required"
            } else {
                "Disabled"
            };
            if needs_client_ca {
                // Mounted at /etc/crabka/clients-ca/ca.crt by the broker pod template.
                let client_ca = clients_ca_path.unwrap_or("/etc/crabka/clients-ca/ca.crt");
                let _ = writeln!(
                    out,
                    "tls_config = {{ cert_path = \"{cert_path}\", key_path = \"{key_path}\", client_ca_path = \"{client_ca}\", client_auth = \"{client_auth}\" }}"
                );
            } else {
                let _ = writeln!(
                    out,
                    "tls_config = {{ cert_path = \"{cert_path}\", key_path = \"{key_path}\", client_auth = \"{client_auth}\" }}"
                );
            }
        }

        if let Some(auth) = &l.authentication
            && let Some(mech) = sasl_mechanism(auth)
        {
            let _ = writeln!(
                out,
                "sasl_config = {{ enabled_mechanisms = [\"{}\"] }}",
                mech.wire_name()
            );
        }

        out.push('\n');
    }
}

fn render_broker_header(
    out: &mut String,
    broker_id: i32,
    inter_broker_listener_name: &str,
    tls: Option<&BrokerTlsRender>,
    controller: (&[String], &str),
) {
    use std::fmt::Write as _;
    let (controller_quorum_voters, controller_server_name) = controller;
    let _ = writeln!(out, "broker_id = {broker_id}");
    let _ = writeln!(out, "log_dir = \"/var/lib/crabka/data\"");
    let _ = writeln!(out, "heartbeat_interval = \"500ms\"");
    let _ = writeln!(out, "heartbeat_timeout = \"3s\"");
    let _ = writeln!(out, "replica_lag_time_max = \"2s\"");
    let _ = writeln!(out, "controller_election_timeout = \"500ms\"");
    let _ = writeln!(out, "controller_heartbeat_interval = \"100ms\"");
    let _ = writeln!(
        out,
        "inter_broker_listener_name = \"{inter_broker_listener_name}\""
    );

    // Emit top-level scalar TLS fields before any [[listeners]] blocks.
    // TOML requires all top-level keys to appear before array-of-tables
    // headers; a bare key after [[listeners]] would be parsed as belonging
    // to that last array entry rather than the root table.
    if let Some(tls) = tls {
        let _ = writeln!(
            out,
            "controller_listener_protocol = \"{}\"",
            tls.controller_listener_protocol
        );
    }
    // KRaft controller quorum voter set (KIP-595/853). Entries are
    // pre-formatted `<id>@<host>:9093` and the full cluster set is
    // identical across every broker. Emitted as a top-level array
    // BEFORE the first [[listeners]] header (TOML parse-order: bare
    // keys must precede array-of-tables). Omitted when empty so a
    // single-node/test render stays a standalone KRaft node.
    if !controller_quorum_voters.is_empty() {
        let _ = writeln!(
            out,
            "controller_quorum_voters = {}",
            toml_string_array(controller_quorum_voters)
        );
    }
    // TLS server-name (SNI) the broker presents when dialing a PEER's
    // controller listener for the KIP-595 quorum. The operator passes the
    // shared headless-Service FQDN — a SAN on every broker's serving cert —
    // so mTLS validation succeeds regardless of which peer (resolved to a
    // pod IP) is dialed. Top-level scalar: emitted BEFORE the first
    // [[listeners]] header (TOML bare-key parse-order). Omitted when empty
    // so single-node/test renders stay defaulted.
    if !controller_server_name.is_empty() {
        let _ = writeln!(out, "controller_server_name = \"{controller_server_name}\"");
    }
    out.push('\n');
}

pub fn render_broker_toml(
    listener_config: (
        i32,
        &[Listener],
        &std::collections::BTreeMap<String, AdvertisedAddress>,
        &str,
    ),
    broker_config: (
        &std::collections::BTreeMap<String, String>,
        Option<&BrokerTlsRender>,
        Option<&str>,
    ),
    security: (
        bool,
        Option<&crate::crd::kafka::Authorization>,
        Option<&crate::crd::kafka::InterBrokerKerberos>,
    ),
    tiered_storage: Option<&crate::crd::kafka::TieredStorage>,
    controller: (&[String], &str),
) -> String {
    use std::fmt::Write as _;
    let (broker_id, listeners, addresses_per_listener, inter_broker_listener_name) =
        listener_config;
    let (server_properties, tls, clients_ca_path) = broker_config;
    let (delegation_token_enabled, authorization, inter_broker_kerberos) = security;
    let (controller_quorum_voters, controller_server_name) = controller;
    let mut out = String::new();
    render_broker_header(
        &mut out,
        broker_id,
        inter_broker_listener_name,
        tls,
        (controller_quorum_voters, controller_server_name),
    );

    render_listener_sections(
        &mut out,
        broker_id,
        listeners,
        addresses_per_listener,
        clients_ca_path,
    );

    if !server_properties.is_empty() {
        let _ = writeln!(out, "[server_properties]");
        for (k, v) in server_properties {
            let _ = writeln!(out, "\"{k}\" = \"{v}\"");
        }
        out.push('\n');
    }

    render_remote_storage(&mut out, tiered_storage);

    // `[authorization]` block. Folds in the
    // `super_users = ["ANONYMOUS"]` hack — the broker now consumes
    // super-users exclusively via `[authorization].super_users` when the
    // block is present.
    //
    // Rules:
    //   * `authorization = Some(Simple { super_users })` → render
    //     `type = "simple"` with the spec super-users, MERGED with
    //     `"ANONYMOUS"` when `delegation_token_enabled` (operator's
    //     PLAINTEXT inter-broker connection identifies as ANONYMOUS and
    //     the broker's act-as check requires it to be a super-user).
    //   * `authorization = Some(Opa { ... })` → render `type = "opa"`
    //     plus the `[authorization.opa]` subtable, with the same
    //     ANONYMOUS-merge rule for delegation-token clusters.
    //   * `authorization = None` AND `delegation_token_enabled` → auto-
    //     inject `type = "simple", super_users = ["ANONYMOUS"]` so the
    //     delegation-token act-as path keeps working without forcing the user
    //     to author an explicit `Kafka.spec.authorization`.
    //   * `authorization = None` AND not delegation-token → omit the
    //     block entirely; broker falls back to `AllowAllAuthorizer`.
    //
    // We intentionally do NOT emit `initial_cache_capacity` from the
    // `OpaAuthorization` CRD field — the broker's `FileOpaConfig` uses
    // `deny_unknown_fields` and only carries `maximum_cache_size`.
    let authz_render = match (authorization, delegation_token_enabled) {
        (Some(a), dt) => Some(AuthorizationRender::from_spec(a, dt)),
        (None, true) => Some(AuthorizationRender::auto_injected_simple()),
        (None, false) => None,
    };
    if let Some(r) = &authz_render {
        let _ = writeln!(out, "[authorization]");
        let _ = writeln!(out, "type = \"{}\"", r.kind);
        // super_users is always rendered (even if empty) so the broker's
        // `[authorization].super_users` overwrite path is deterministic.
        let _ = writeln!(out, "super_users = {}", toml_string_array(&r.super_users));
        out.push('\n');
        if let Some(opa) = &r.opa {
            let _ = writeln!(out, "[authorization.opa]");
            let _ = writeln!(out, "url = \"{}\"", opa.url);
            if let Some(b) = opa.allow_on_error {
                let _ = writeln!(out, "allow_on_error = {b}");
            }
            if let Some(n) = opa.maximum_cache_size {
                let _ = writeln!(out, "maximum_cache_size = {n}");
            }
            if let Some(n) = opa.expire_after_ms {
                let _ = writeln!(out, "expire_after_ms = {n}");
            }
            out.push('\n');
        }
    }

    // Broker-global [oauthbearer] block. Emitted
    // when any listener declares `authentication: oauth`. Per-listener OAuth
    // divergence is rejected by `validate_listeners`, so picking the first
    // OAuth listener's config is unambiguous when we reach this point.
    if let Some(oauth_cfg) = listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::OAuth(c)) => Some(c),
        _ => None,
    }) {
        let _ = writeln!(out, "[oauthbearer]");
        // Fork on access_token_is_jwt. JWT mode (true) emits
        // jwks_endpoint_uri. Introspection mode (false)
        // emits introspection_endpoint_uri / userinfo_endpoint_uri /
        // introspection_client_id / introspection_client_secret_path /
        // introspection_http_timeout_ms in FileOAuthBearerConfig
        // field order. The cross-mode validator in `validate_listeners`
        // guarantees the relevant Option fields are populated.
        if oauth_cfg.access_token_is_jwt {
            if let Some(uri) = &oauth_cfg.jwks_endpoint_uri {
                let _ = writeln!(out, "jwks_endpoint_uri = \"{uri}\"");
            }
        } else {
            if let Some(uri) = &oauth_cfg.introspection_endpoint_uri {
                let _ = writeln!(out, "introspection_endpoint_uri = \"{uri}\"");
            }
            if let Some(uri) = &oauth_cfg.user_info_endpoint_uri {
                let _ = writeln!(out, "userinfo_endpoint_uri = \"{uri}\"");
            }
            if let Some(id) = &oauth_cfg.client_id {
                let _ = writeln!(out, "introspection_client_id = \"{id}\"");
            }
            // clientSecret bytes are mounted at this fixed path by the
            // pod-template plumbing; the path itself is constant so the
            // operator emits it whenever introspection mode is selected.
            let _ = writeln!(
                out,
                r#"introspection_client_secret_path = "/etc/crabka/oauth-introspection/client-secret""#
            );
            if let Some(s) = oauth_cfg.introspection_http_timeout_seconds {
                let _ = writeln!(
                    out,
                    "introspection_http_timeout_ms = {}",
                    u64::from(s) * 1000
                );
            }
        }
        let _ = writeln!(out, "valid_issuer_uri = \"{}\"", oauth_cfg.valid_issuer_uri);
        if let Some(aud) = &oauth_cfg.valid_audience {
            let _ = writeln!(out, "expected_audience = \"{aud}\"");
        }
        if let Some(claim) = &oauth_cfg.user_name_claim {
            let _ = writeln!(out, "principal_claim_name = \"{claim}\"");
        }
        if let Some(s) = oauth_cfg.jwks_refresh_seconds {
            let _ = writeln!(out, "jwks_refresh_interval_ms = {}", u64::from(s) * 1000);
        }
        if let Some(s) = oauth_cfg.max_clock_skew_seconds {
            let _ = writeln!(out, "allowable_clock_skew_ms = {}", i64::from(s) * 1000);
        }
        if !oauth_cfg.tls_trusted_certificates.is_empty() {
            let _ = writeln!(
                out,
                r#"idp_tls_trust = "/etc/crabka/oauth-jwks-trust/ca.crt""#,
            );
        }
        if let Some(s) = oauth_cfg.max_seconds_without_reauthentication {
            let _ = writeln!(out, "max_session_lifetime_seconds = {s}");
        }
        // customClaimCheck (JsonPath expression) — use TOML multi-line
        // literal `'''...'''` to avoid escape processing AND allow embedded `'`
        // and `"` in the expression. JsonPath expressions commonly contain both.
        if let Some(expr) = &oauth_cfg.custom_claim_check {
            let _ = writeln!(out, "custom_claim_check = '''{expr}'''");
        }
        // validTokenType (JWT typ header check).
        if let Some(typ) = &oauth_cfg.valid_token_type {
            let _ = writeln!(out, "valid_token_type = \"{typ}\"");
        }
        // Claims mapping.
        if let Some(c) = &oauth_cfg.fallback_user_name_claim {
            let _ = writeln!(out, "fallback_user_name_claim = \"{c}\"");
        }
        if let Some(p) = &oauth_cfg.fallback_user_name_prefix {
            let _ = writeln!(out, "fallback_user_name_prefix = \"{p}\"");
        }
        if let Some(expr) = &oauth_cfg.groups_claim {
            // TOML multi-line literal — JsonPath may contain `'` and `"`,
            // same convention as custom_claim_check.
            let _ = writeln!(out, "groups_claim = '''{expr}'''");
        }
        if let Some(d) = &oauth_cfg.groups_claim_delimiter {
            let _ = writeln!(out, "groups_claim_delimiter = \"{d}\"");
        }
        if let Some(s) = oauth_cfg.jwks_min_refresh_pause_seconds {
            let _ = writeln!(out, "jwks_min_refresh_pause_seconds = {s}");
        }
        if let Some(s) = oauth_cfg.jwks_expiry_seconds {
            let _ = writeln!(out, "jwks_expiry_seconds = {s}");
        }
        if let Some(b) = oauth_cfg.jwks_ignore_key_use {
            let _ = writeln!(out, "jwks_ignore_key_use = {b}");
        }
        out.push('\n');
    }

    // Broker-global [gssapi] block. Emitted when any listener is type:gssapi.
    // Per-listener divergence is rejected by validate_listeners, so the first
    // GSSAPI listener's config is unambiguous here. Keytab is mounted at a
    // fixed path by kafka_node_pool.rs.
    if let Some(g) = listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::Gssapi(c)) => Some(c),
        _ => None,
    }) {
        let _ = writeln!(out, "[gssapi]");
        let _ = writeln!(out, "keytab_path = \"{GSSAPI_KEYTAB_PATH}\"");
        let svc = g.service_name.as_deref().unwrap_or("kafka");
        let _ = writeln!(out, "service_name = \"{}\"", toml_escape(svc));
        let _ = writeln!(
            out,
            "principal_to_local_rules = {}",
            toml_string_array(&g.principal_to_local_rules)
        );
        if let Some(realm) = &g.realm {
            let _ = writeln!(out, "realm = \"{}\"", toml_escape(realm));
        }
        if let Some(kdc) = &g.kdc {
            let _ = writeln!(out, "kdc = \"{}\"", toml_escape(kdc));
        }
        if let Some(max_time_skew) = g.max_time_skew {
            let _ = writeln!(out, "max_time_skew = \"{}\"", max_time_skew.human());
        }
        out.push('\n');
    }

    // Inter-broker initiate credentials. Emitted only when the inter-broker
    // listener is type:gssapi AND spec.interBrokerKerberos is provided.
    let ib_is_gssapi = listeners.iter().any(|l| {
        l.name == inter_broker_listener_name
            && matches!(l.authentication, Some(ListenerAuthentication::Gssapi(_)))
    });
    if ib_is_gssapi && let Some(ibk) = inter_broker_kerberos {
        let _ = writeln!(out, "[inter_broker_credentials]");
        let _ = writeln!(out, "type = \"gssapi\"");
        let _ = writeln!(out, "keytab_path = \"{GSSAPI_KEYTAB_PATH}\"");
        let _ = writeln!(
            out,
            "client_principal = \"{}\"",
            toml_escape(&ibk.client_principal)
        );
        let svc = ibk.service_name.as_deref().unwrap_or("kafka");
        let _ = writeln!(out, "service_name = \"{}\"", toml_escape(svc));
        let _ = writeln!(out, "kdc_url = \"{}\"", toml_escape(&ibk.kdc_url));
        out.push('\n');
    }

    if let Some(tls) = tls {
        let _ = writeln!(out, "[tls_config]");
        let _ = writeln!(out, "cert_path = \"{}\"", tls.cert_path);
        let _ = writeln!(out, "key_path = \"{}\"", tls.key_path);
        let _ = writeln!(out, "trust_roots_path = \"{}\"", tls.trust_roots_path);
        let _ = writeln!(out, "client_ca_path = \"{}\"", tls.client_ca_path);
        let _ = writeln!(out, "client_auth = \"{}\"", tls.client_auth);
    }

    out
}

/// Builds the synthesized internal-only listener for an empty
/// `Kafka.spec.listeners`.
///
/// This function lives here so that the operator and the tests agree on
/// the bytes.
#[allow(dead_code)]
#[must_use]
pub fn synthesized_default_listener() -> Listener {
    Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }
}

#[cfg(test)]
mod toml_rendering_tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn renders_minimal_broker_toml_and_round_trips() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let toml_str = render_broker_toml(
            (0, &listeners, &addrs, "PLAIN"),
            (&props, None, None),
            (false, None, None),
            None,
            (&[], ""),
        );

        // Sanity: parses cleanly with the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        check!(parsed.broker_id == Some(0));
        check!(parsed.inter_broker_listener_name.as_deref() == Some("PLAIN"));
        check!(parsed.heartbeat_interval == Some(crabka_units::millis(500)));
        check!(parsed.heartbeat_timeout == Some(crabka_units::secs(3)));
        check!(parsed.replica_lag_time_max == Some(crabka_units::secs(2)));
        check!(parsed.controller_election_timeout == Some(crabka_units::millis(500)));
        check!(parsed.controller_heartbeat_interval == Some(crabka_units::millis(100)));
        check!(parsed.listeners.len() == 1);
        check!(parsed.listeners[0].advertised == "demo-0.svc.local:9092");
    }

    #[test]
    fn controller_quorum_voters_rendered_before_listeners_and_round_trips() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let voters = vec![
            "0@host-a:9093".to_string(),
            "1@host-b:9093".to_string(),
            "2@host-c:9093".to_string(),
        ];
        let toml_str = render_broker_toml(
            (0, &listeners, &addrs, "PLAIN"),
            (&props, None, None),
            (false, None, None),
            None,
            (&voters, ""),
        );

        // The voters key must appear, and BEFORE the first [[listeners]]
        // array-of-tables header — TOML requires all top-level keys to
        // precede array-of-tables, else the key binds to the last table.
        let key_pos = toml_str
            .find("controller_quorum_voters = [")
            .expect("controller_quorum_voters key must be present");
        let listeners_pos = toml_str
            .find("[[listeners]]")
            .expect("[[listeners]] header must be present");
        assert!(
            key_pos < listeners_pos,
            "controller_quorum_voters must precede [[listeners]], got:\n{toml_str}"
        );

        // Round-trips through the broker's FileConfig with the exact set.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert!(parsed.controller_quorum_voters == voters);
    }

    #[test]
    fn controller_quorum_voters_omitted_when_empty() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let toml_str = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !toml_str.contains("controller_quorum_voters"),
            "empty voter slice must emit no key, got:\n{toml_str}"
        );
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert!(parsed.controller_quorum_voters.is_empty());
    }

    #[test]
    fn controller_server_name_rendered_before_listeners_and_round_trips() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let server_name = "demo-broker-headless.ns.svc.cluster.local";
        let toml_str = render_broker_toml(
            (0, &listeners, &addrs, "PLAIN"),
            (&props, None, None),
            (false, None, None),
            None,
            (&[], server_name),
        );

        // The key must appear, and BEFORE the first [[listeners]]
        // array-of-tables header — it is a top-level scalar and TOML binds
        // a bare key after [[listeners]] to the last table, not the root.
        let key_pos = toml_str
            .find("controller_server_name = ")
            .expect("controller_server_name key must be present");
        let listeners_pos = toml_str
            .find("[[listeners]]")
            .expect("[[listeners]] header must be present");
        assert!(
            key_pos < listeners_pos,
            "controller_server_name must precede [[listeners]], got:\n{toml_str}"
        );

        // Round-trips through the broker's FileConfig with the exact value.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert!(parsed.controller_server_name.as_deref() == Some(server_name));
    }

    #[test]
    fn controller_server_name_omitted_when_empty() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let toml_str = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !toml_str.contains("controller_server_name"),
            "empty server name must emit no key, got:\n{toml_str}"
        );
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert!(parsed.controller_server_name.is_none());
    }

    #[test]
    fn deterministic_byte_output() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let l = vec![synthesized_default_listener()];
        let mut p = std::collections::BTreeMap::new();
        p.insert("z.last".into(), "1".into());
        p.insert("a.first".into(), "0".into());

        let t1 = render_broker_toml(
            (0, &l, &addrs, "PLAIN"),
            (&p, None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        let t2 = render_broker_toml(
            (0, &l, &addrs, "PLAIN"),
            (&p, None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(t1 == t2);
        // Sorted property keys (BTreeMap iteration).
        let a_pos = t1.find("a.first").unwrap();
        let z_pos = t1.find("z.last").unwrap();
        assert!(a_pos < z_pos);
    }

    #[test]
    fn server_properties_section_omitted_when_empty() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(!t.contains("[server_properties]"), "got:\n{t}");
    }

    #[test]
    fn render_broker_toml_emits_super_users_anonymous_when_delegation_token_set() {
        // When `delegation_token_enabled` and no explicit
        // `Kafka.spec.authorization`, the renderer auto-injects an
        // `[authorization]` block with `type = "simple", super_users =
        // ["ANONYMOUS"]` — preserving the delegation-token act-as path through
        // the pluggable-authorizer plumbing.
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (true, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "[authorization]",
            "type = \"simple\"",
            "super_users = [\"ANONYMOUS\"]",
        ] {
            assert!(
                t.contains(needle),
                "expected {needle:?} in the auto-injected [authorization] block when \
                 delegation tokens are enabled, got:\n{t}"
            );
        }
        // Round-trip: the broker's FileConfig must accept the rendered
        // block and the `[authorization].super_users` field must carry
        // ANONYMOUS.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let authz = parsed
            .authorization
            .expect("[authorization] block must round-trip into FileConfig");
        assert!(authz.super_users == vec!["ANONYMOUS".to_string()]);
    }

    #[test]
    fn render_broker_toml_omits_super_users_when_delegation_token_unset() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !t.contains("super_users"),
            "super_users must be absent when delegation tokens are disabled, got:\n{t}"
        );
        assert!(
            !t.contains("[authorization]"),
            "[authorization] block must be omitted when both `authorization` and \
             `delegation_token_enabled` are unset; broker falls back to AllowAll, \
             got:\n{t}"
        );
    }

    // -----------------------------------------------------------------
    // `[authorization]` block render
    // -----------------------------------------------------------------

    #[test]
    fn render_broker_toml_emits_simple_authorization_section() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let authz =
            crate::crd::kafka::Authorization::Simple(crate::crd::kafka::SimpleAuthorization {
                super_users: vec!["admin".into()],
            });
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, Some(&authz), None),
            None,
            (&[], ""),
        );
        // No OPA subtable when type = simple.
        for (needle, want) in [
            ("[authorization]", true),
            ("type = \"simple\"", true),
            ("super_users = [\"admin\"]", true),
            ("[authorization.opa]", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
        // Round-trip through the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let a = parsed.authorization.expect("[authorization] present");
        assert!(a.super_users == vec!["admin".to_string()]);
        assert!(a.opa.is_none());
    }

    #[test]
    fn render_broker_toml_emits_opa_authorization_section() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let authz = crate::crd::kafka::Authorization::Opa(crate::crd::kafka::OpaAuthorization {
            url: "http://opa:8181/v1/data/k/a".into(),
            allow_on_error: Some(false),
            initial_cache_capacity: None,
            maximum_cache_size: Some(1000),
            expire_after_ms: Some(60000),
            super_users: vec!["ANONYMOUS".into()],
        });
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, Some(&authz), None),
            None,
            (&[], ""),
        );
        // The CRD-level `initial_cache_capacity` MUST NOT be emitted:
        // the broker's `FileOpaConfig` uses `deny_unknown_fields` and
        // has no such field. A leaked key would refuse to parse.
        for (needle, want) in [
            ("type = \"opa\"", true),
            ("super_users = [\"ANONYMOUS\"]", true),
            ("[authorization.opa]", true),
            ("url = \"http://opa:8181/v1/data/k/a\"", true),
            ("allow_on_error = false", true),
            ("maximum_cache_size = 1000", true),
            ("expire_after_ms = 60000", true),
            ("initial_cache_capacity", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
    }

    #[test]
    fn render_broker_toml_omits_authorization_section_when_unset() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !t.contains("[authorization]"),
            "no [authorization] section must be emitted when both inputs are unset \
             (broker falls back to AllowAllAuthorizer), got:\n{t}"
        );
    }

    #[test]
    fn render_broker_toml_auto_injects_simple_authorization_for_delegation_token() {
        // Bonus: when `delegation_token_enabled` but the CRD has no
        // explicit `Kafka.spec.authorization`, the operator auto-injects
        // the `type = "simple", super_users =
        // ["ANONYMOUS"]` block so the act-as path keeps working.
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (true, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "[authorization]",
            "type = \"simple\"",
            "super_users = [\"ANONYMOUS\"]",
        ] {
            assert!(t.contains(needle), "needle {needle:?}, TOML:\n{t}");
        }
        // Verify the merge path too: explicit Simple + delegation_token
        // merges ANONYMOUS into the user's super-users list.
        let authz =
            crate::crd::kafka::Authorization::Simple(crate::crd::kafka::SimpleAuthorization {
                super_users: vec!["User:admin".into()],
            });
        let t2 = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (true, Some(&authz), None),
            None,
            (&[], ""),
        );
        assert!(
            t2.contains("super_users = [\"User:admin\", \"ANONYMOUS\"]"),
            "delegation_token must merge ANONYMOUS into user-authored super_users, got:\n{t2}"
        );
    }

    // ── tiered storage TOML render ───────────────────────────────────

    #[test]
    fn render_broker_toml_emits_remote_storage_when_tiered_local() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        assert!(
            t.contains("[remote_storage]"),
            "expected [remote_storage] block, got:\n{t}"
        );
        assert!(
            t.contains("storage_dir = \"/var/lib/crabka/remote\""),
            "expected canonical storage_dir line, got:\n{t}"
        );
        // Round-trip: the broker's FileConfig must accept the rendered
        // block and surface the path as the broker's tier storage dir.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let rs = parsed.remote_storage.expect("[remote_storage] round-trips");
        assert!(rs.storage_dir.as_deref() == Some("/var/lib/crabka/remote"));
    }

    #[test]
    fn render_broker_toml_omits_remote_storage_when_tiered_none() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !t.contains("[remote_storage]"),
            "no tieredStorage → no [remote_storage] block; got:\n{t}"
        );
    }

    // ── S3 tiered storage TOML render ────────────────────────────────

    /// A full S3 spec sets bucket, region, prefix, endpoint, `allow_http`,
    /// and the multipart overrides. The render must write every field into
    /// `[remote_storage.s3]` and round-trip through the `FileConfig` of
    /// the broker, so that the broker pod boots against the rendered TOML.
    #[test]
    fn render_broker_toml_emits_kafka_metadata_when_topic_rlmm_set() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(crate::crd::kafka::MetadataManagerSpec {
                kind: crate::crd::kafka::MetadataManagerType::Topic,
                topic: Some(crate::crd::kafka::TopicMetadataManagerSpec {
                    bootstrap: "127.0.0.1:9094".into(),
                    num_partitions: Some(8),
                    replication: Some(1),
                    topic_create_timeout: Some(crabka_units::secs(45)),
                    fetch_max_wait: Some(crabka_units::millis(750)),
                    fetch_max_bytes: Some(crabka_units::mebibytes(2)),
                    fetch_retry_backoff: Some(crabka_units::millis(300)),
                    event_queue_capacity: Some(2048),
                    snapshot_interval: Some(crabka_units::secs(90)),
                }),
            }),
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        for needle in [
            "[remote_storage.kafka_metadata]",
            "bootstrap = \"127.0.0.1:9094\"",
            "num_partitions = 8",
            "replication = 1",
            "topic_create_timeout = \"45s\"",
            "fetch_max_wait = \"750ms\"",
            "fetch_max_bytes = \"2MiB\"",
            "fetch_retry_backoff = \"300ms\"",
            "event_queue_capacity = 2048",
            "snapshot_interval = \"1.5m\"",
        ] {
            assert!(t.contains(needle), "needle {needle:?} missing, got:\n{t}");
        }
        // Round-trip through the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let km = parsed
            .remote_storage
            .as_ref()
            .expect("[remote_storage] round-trips")
            .kafka_metadata
            .as_ref()
            .expect("kafka_metadata round-trips");
        check!(km.bootstrap == "127.0.0.1:9094");
        check!(km.num_partitions == Some(8));
        check!(km.replication == Some(1));
        check!(km.topic_create_timeout == Some(crabka_units::secs(45)));
        check!(km.fetch_max_wait == Some(crabka_units::millis(750)));
        check!(km.fetch_max_bytes == Some(crabka_units::mebibytes(2)));
        check!(km.fetch_retry_backoff == Some(crabka_units::millis(300)));
        check!(km.event_queue_capacity == Some(2048));
        check!(km.snapshot_interval == Some(crabka_units::secs(90)));

        let mut broker = crabka_broker::BrokerConfig::default();
        parsed.apply_to(&mut broker).expect("apply rendered TOML");
        let crabka_broker::RlmmKind::TopicBacked(policy) = broker.remote_log_metadata else {
            panic!("rendered policy must select topic-backed RLMM");
        };
        check!(policy.topic_create_timeout == crabka_units::secs(45));
        check!(policy.fetch_max_wait == crabka_units::millis(750));
        check!(policy.fetch_max_bytes == crabka_units::mebibytes(2));
        check!(policy.fetch_retry_backoff == crabka_units::millis(300));
        check!(policy.event_queue_capacity.capacity() == 2048);
        check!(policy.snapshot_interval == crabka_units::secs(90));
    }

    #[test]
    fn render_broker_toml_emits_in_memory_opt_out_when_rlmm_inmemory() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(crate::crd::kafka::MetadataManagerSpec {
                kind: crate::crd::kafka::MetadataManagerType::InMemory,
                topic: None,
            }),
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // The block must always be emitted so the broker knows to select InMemory.
        assert!(
            t.contains("[remote_storage.kafka_metadata]"),
            "kafka_metadata block missing for InMemory, got:\n{t}"
        );
        assert!(
            t.contains("in_memory = true"),
            "in_memory = true missing for InMemory, got:\n{t}"
        );
    }

    #[test]
    fn render_broker_toml_emits_kafka_metadata_by_default_when_tiered_and_mm_unset() {
        // Tiered storage ON (Local), metadataManager entirely unset → the topic-backed
        // RLMM block must STILL be rendered (it's the production default).
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        assert!(
            t.contains("[remote_storage.kafka_metadata]"),
            "expected kafka_metadata block by default, got:\n{t}"
        );
    }

    #[test]
    fn render_broker_toml_emits_kafka_metadata_for_default_metadata_manager() {
        // An empty metadataManager block (MetadataManagerSpec::default(), i.e.
        // kind=Topic, topic=None) must also render the topic-backed RLMM header,
        // consistent with omitting the field entirely.
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(crate::crd::kafka::MetadataManagerSpec {
                kind: crate::crd::kafka::MetadataManagerType::default(),
                topic: None,
            }),
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        assert!(
            t.contains("[remote_storage.kafka_metadata]"),
            "expected kafka_metadata block for default MetadataManagerSpec, got:\n{t}"
        );
        // No bootstrap line — the bare header is enough; broker fills defaults.
        assert!(
            !t.contains("bootstrap ="),
            "unexpected bootstrap line for bare Topic manager, got:\n{t}"
        );
    }

    #[test]
    fn render_broker_toml_emits_remote_storage_s3_full_spec() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::S3,
            s3: Some(crate::crd::kafka::S3StorageSpec {
                bucket: "crabka-tier".into(),
                region: "us-east-1".into(),
                prefix: Some("cluster-a".into()),
                endpoint: Some("http://minio.svc:9000".into()),
                credentials: None,
                allow_http: true,
                multipart_threshold: Some(4096),
                multipart_chunk_size: Some(1024),
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // Credentials must NEVER appear in the TOML — they're sourced
        // from pod env via `secretKeyRef`.
        for (needle, want) in [
            ("[remote_storage]", true),
            ("[remote_storage.s3]", true),
            ("bucket = \"crabka-tier\"", true),
            ("region = \"us-east-1\"", true),
            ("prefix = \"cluster-a\"", true),
            ("endpoint = \"http://minio.svc:9000\"", true),
            ("allow_http = true", true),
            ("multipart_threshold = 4096", true),
            ("multipart_chunk_size = 1024", true),
            ("access_key_id", false),
            ("secret_access_key", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
        // Broker round-trip.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let rs = parsed.remote_storage.expect("[remote_storage] round-trips");
        let s3 = rs.s3.expect("[remote_storage.s3] round-trips");
        check!(s3.bucket == "crabka-tier");
        check!(s3.region == "us-east-1");
        check!(s3.prefix.as_deref() == Some("cluster-a"));
        check!(s3.endpoint.as_deref() == Some("http://minio.svc:9000"));
        check!(s3.allow_http);
        check!(s3.multipart_threshold == Some(4096));
        check!(s3.multipart_chunk_size == Some(1024));
        check!(s3.access_key_id.is_none());
        check!(s3.secret_access_key.is_none());
    }

    /// A minimal S3 spec sets only `bucket` and `region`. The rendered
    /// TOML must omit the optional fields, so that the broker uses its
    /// defaults: the multipart threshold and chunk size, no prefix, no
    /// endpoint override, and `allow_http = false`.
    #[test]
    fn render_broker_toml_emits_remote_storage_s3_minimal_spec() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::S3,
            s3: Some(crate::crd::kafka::S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // S3 must NOT render the Local `storage_dir` key, and all unset
        // optional fields must be omitted.
        for (needle, want) in [
            ("[remote_storage.s3]", true),
            ("bucket = \"b\"", true),
            ("region = \"r\"", true),
            ("prefix =", false),
            ("endpoint =", false),
            ("allow_http", false),
            ("multipart_threshold", false),
            ("multipart_chunk_size", false),
            ("storage_dir", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
        // Broker round-trip.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let s3 = parsed
            .remote_storage
            .expect("[remote_storage] round-trips")
            .s3
            .expect("[remote_storage.s3] round-trips");
        assert!(s3.bucket == "b");
        assert!(s3.region == "r");
    }

    /// The render must escape the reserved TOML metacharacters `"` and
    /// `\` in a string field that the user supplied. It must not let them
    /// pass through verbatim. One extra quote in `prefix` gives TOML that
    /// the broker cannot parse.
    #[test]
    fn render_broker_toml_escapes_toml_metacharacters_in_s3_strings() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::S3,
            s3: Some(crate::crd::kafka::S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                prefix: Some(r#"weird"prefix\"#.into()),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // The rendered TOML must parse; if escaping is broken the
        // broker's FileConfig would error out before the assertion below.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("escaped TOML must parse");
        let s3 = parsed
            .remote_storage
            .expect("[remote_storage]")
            .s3
            .expect("[remote_storage.s3]");
        assert!(s3.prefix.as_deref() == Some(r#"weird"prefix\"#));
    }

    /// A GCS backend with an explicit service-account key Secret. The
    /// `[remote_storage.gcs]` block must render the non-credential fields
    /// verbatim, and add a `service_account_path` that points at the
    /// mounted file.
    #[test]
    fn render_broker_toml_emits_remote_storage_gcs_with_credentials() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Gcs,
            s3: None,
            gcs: Some(crate::crd::kafka::GcsStorageSpec {
                bucket: "crabka-tier".into(),
                prefix: Some("cluster-a".into()),
                endpoint: Some("http://fake-gcs.svc:4443".into()),
                credentials: Some(crate::crd::kafka::GcsCredentials {
                    service_account_key: crate::crd::kafka::SecretKeyRef {
                        name: "crabka-gcs-creds".into(),
                        key: Some("key.json".into()),
                    },
                }),
                allow_http: true,
                multipart_threshold: Some(4096),
                multipart_chunk_size: Some(1024),
            }),
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // The Local-only `storage_dir` key must never appear for GCS.
        for (needle, want) in [
            ("[remote_storage]", true),
            ("[remote_storage.gcs]", true),
            ("bucket = \"crabka-tier\"", true),
            ("prefix = \"cluster-a\"", true),
            ("endpoint = \"http://fake-gcs.svc:4443\"", true),
            ("allow_http = true", true),
            (
                "service_account_path = \"/etc/crabka/gcs-credentials/key.json\"",
                true,
            ),
            ("multipart_threshold = 4096", true),
            ("multipart_chunk_size = 1024", true),
            ("storage_dir", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
        // Broker round-trip: the rendered TOML must parse into FileConfig.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let gcs = parsed
            .remote_storage
            .expect("[remote_storage] round-trips")
            .gcs
            .expect("[remote_storage.gcs] round-trips");
        check!(gcs.bucket == "crabka-tier");
        check!(gcs.prefix.as_deref() == Some("cluster-a"));
        check!(gcs.endpoint.as_deref() == Some("http://fake-gcs.svc:4443"));
        check!(gcs.allow_http);
        check!(gcs.service_account_path.as_deref() == Some("/etc/crabka/gcs-credentials/key.json"));
        check!(gcs.multipart_threshold == Some(4096));
        check!(gcs.multipart_chunk_size == Some(1024));
    }

    /// Keyless GCS with Workload Identity or ADC. With `credentials`
    /// unset, NO `service_account_path` line may appear. The broker pod
    /// resolves the credentials from the bound KSA through the metadata
    /// server.
    #[test]
    fn render_broker_toml_gcs_keyless_omits_service_account_path() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let ts = crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Gcs,
            s3: None,
            gcs: Some(crate::crd::kafka::GcsStorageSpec {
                bucket: "b".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        let t = render_broker_toml(
            (0, &[synthesized_default_listener()], &addrs, "PLAIN"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            Some(&ts),
            (&[], ""),
        );
        // Keyless GCS must not render service_account_path; optional
        // fields omitted → broker defaults apply.
        for (needle, want) in [
            ("[remote_storage.gcs]", true),
            ("bucket = \"b\"", true),
            ("service_account_path", false),
            ("prefix =", false),
            ("endpoint =", false),
            ("allow_http", false),
        ] {
            assert!(
                t.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, got:\n{t}"
            );
        }
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&t).expect("rendered TOML must parse with broker FileConfig");
        let gcs = parsed
            .remote_storage
            .expect("[remote_storage] round-trips")
            .gcs
            .expect("[remote_storage.gcs] round-trips");
        assert!(gcs.bucket == "b");
        assert!(gcs.service_account_path.is_none());
    }

    #[test]
    fn render_with_tls_block_round_trips_with_broker_fileconfig() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let tls = BrokerTlsRender {
            controller_listener_protocol: "Ssl".into(),
            cert_path: "/etc/crabka/broker-tls/0.crt".into(),
            key_path: "/etc/crabka/broker-tls/0.key".into(),
            client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
            client_auth: "Required".into(),
            trust_roots_path: "/etc/crabka/cluster-ca/ca.crt".into(),
        };
        let toml_str = render_broker_toml(
            (0, &listeners, &addrs, "PLAIN"),
            (&props, Some(&tls), None),
            (false, None, None),
            None,
            (&[], ""),
        );

        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert!(
            parsed.controller_listener_protocol == Some(crabka_security::ListenerProtocol::Ssl)
        );
        let parsed_tls = parsed.tls_config.expect("tls_config emitted");
        assert!(parsed_tls.cert_path == std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt"));
        // The cluster CA must be wired as the controller-quorum TLS trust
        // roots inside [tls_config] so the outbound raft dialer trusts peer
        // serving certs (KIP-595 controller mTLS).
        assert!(
            parsed_tls.trust_roots_path
                == Some(std::path::PathBuf::from("/etc/crabka/cluster-ca/ca.crt"))
        );
    }

    #[test]
    fn render_without_tls_omits_tls_block() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let toml_str = render_broker_toml(
            (0, &listeners, &addrs, "PLAIN"),
            (&props, None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(!toml_str.contains("[tls_config]"));
        assert!(!toml_str.contains("controller_listener_protocol"));
    }

    #[test]
    fn render_broker_toml_emits_scram_ssl_listener_with_inline_configs() {
        use std::collections::BTreeMap;
        let listeners = vec![Listener {
            name: "scram".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(crate::crd::ListenerAuthentication::ScramSha512),
            configuration: None,
            network_policy_peers: None,
        }];
        let mut addrs = BTreeMap::new();
        addrs.insert(
            "scram".to_string(),
            AdvertisedAddress {
                host: "broker-0".into(),
                port: 9094,
            },
        );
        let toml = render_broker_toml(
            (0, &listeners, &addrs, "scram"),
            (
                &BTreeMap::new(),
                Some(&BrokerTlsRender {
                    controller_listener_protocol: "Ssl".into(),
                    cert_path: "/etc/crabka/broker-tls/0.crt".into(),
                    key_path: "/etc/crabka/broker-tls/0.key".into(),
                    client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
                    client_auth: "Required".into(),
                    trust_roots_path: "/etc/crabka/cluster-ca/ca.crt".into(),
                }),
                None,
            ),
            (false, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "protocol = \"SaslSsl\"",
            "tls_config = { cert_path = \"/etc/crabka/broker-tls/0.crt\"",
            "sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-512\"] }",
            "[tls_config]",
        ] {
            assert!(toml.contains(needle), "needle {needle:?}, TOML: {toml}");
        }
    }

    fn oauth_full_cfg() -> crate::crd::ListenerAuthenticationOAuth {
        crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://kc.example.com/realms/kafka".into(),
            jwks_endpoint_uri: Some(
                "https://kc.example.com/realms/kafka/protocol/openid-connect/certs".into(),
            ),
            valid_audience: Some("kafka".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: Some("$.scope[?@ == 'kafka-broker']".into()),
            jwks_refresh_seconds: Some(300),
            max_clock_skew_seconds: Some(30),
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

    fn oauth_listener_for_render(
        name: &str,
        port: i32,
        tls: bool,
        cfg: crate::crd::ListenerAuthenticationOAuth,
    ) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls,
            authentication: Some(crate::crd::ListenerAuthentication::OAuth(Box::new(cfg))),
            configuration: None,
            network_policy_peers: None,
        }
    }

    fn addrs_for(name: &str, port: i32) -> std::collections::BTreeMap<String, AdvertisedAddress> {
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            name.to_string(),
            AdvertisedAddress {
                host: "broker-0".into(),
                port,
            },
        );
        m
    }

    fn render_tls() -> BrokerTlsRender {
        BrokerTlsRender {
            controller_listener_protocol: "Ssl".into(),
            cert_path: "/etc/crabka/broker-tls/0.crt".into(),
            key_path: "/etc/crabka/broker-tls/0.key".into(),
            client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
            client_auth: "Required".into(),
            trust_roots_path: "/etc/crabka/cluster-ca/ca.crt".into(),
        }
    }

    fn gssapi_cfg_with_service(svc: &str) -> crate::crd::ListenerAuthenticationGssapi {
        crate::crd::ListenerAuthenticationGssapi {
            keytab_secret_ref: crate::crd::KeytabSecretRef {
                secret_name: "kt".into(),
                key: "keytab".into(),
            },
            service_name: Some(svc.into()),
            principal_to_local_rules: vec!["DEFAULT".into()],
            realm: None,
            kdc: None,
            max_time_skew: None,
        }
    }

    fn gssapi_listener(
        name: &str,
        port: i32,
        tls: bool,
        cfg: crate::crd::ListenerAuthenticationGssapi,
    ) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls,
            authentication: Some(crate::crd::ListenerAuthentication::Gssapi(cfg)),
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn render_emits_gssapi_block_and_mechanism() {
        let mut config = gssapi_cfg_with_service("kafka");
        config.max_time_skew = Some(crabka_units::secs(17));
        let l = gssapi_listener("gss", 9092, false, config);
        let addrs = addrs_for("gss", 9092);
        let toml = render_broker_toml(
            (0, &[l], &addrs, "gss"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        for (needle, want) in [
            ("[gssapi]", true),
            (r#"keytab_path = "/etc/crabka/gssapi-keytab/keytab""#, true),
            (r#"service_name = "kafka""#, true),
            (r#"principal_to_local_rules = ["DEFAULT"]"#, true),
            (r#"max_time_skew = "17s""#, true),
            (r#"enabled_mechanisms = ["GSSAPI"]"#, true),
            ("[inter_broker_credentials]", false),
        ] {
            assert!(
                toml.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, toml:\n{toml}"
            );
        }
    }

    #[test]
    fn render_emits_inter_broker_credentials_when_ib_listener_is_gssapi() {
        let l = gssapi_listener("gss", 9092, false, gssapi_cfg_with_service("kafka"));
        let addrs = addrs_for("gss", 9092);
        let ibk = crate::crd::kafka::InterBrokerKerberos {
            client_principal: "kafka@EXAMPLE.COM".into(),
            service_name: Some("kafka".into()),
            kdc_url: "tcp://kdc:88".into(),
        };
        let toml = render_broker_toml(
            (0, &[l], &addrs, "gss"),
            (&std::collections::BTreeMap::new(), None, None),
            (false, None, Some(&ibk)),
            None,
            (&[], ""),
        );
        for needle in [
            "[inter_broker_credentials]",
            r#"type = "gssapi""#,
            r#"keytab_path = "/etc/crabka/gssapi-keytab/keytab""#,
            r#"client_principal = "kafka@EXAMPLE.COM""#,
            r#"kdc_url = "tcp://kdc:88""#,
        ] {
            assert!(toml.contains(needle), "needle {needle:?}, toml:\n{toml}");
        }
    }

    #[test]
    fn render_broker_toml_emits_oauthbearer_block_for_oauth_listener() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "[oauthbearer]",
            "jwks_endpoint_uri = \"https://kc.example.com/realms/kafka/protocol/openid-connect/certs\"",
            "valid_issuer_uri = \"https://kc.example.com/realms/kafka\"",
            "expected_audience = \"kafka\"",
            "principal_claim_name = \"preferred_username\"",
            "jwks_refresh_interval_ms = 300000",
            "allowable_clock_skew_ms = 30000",
            "custom_claim_check = '''$.scope[?@ == 'kafka-broker']'''",
        ] {
            assert!(toml.contains(needle), "needle {needle:?}, TOML: {toml}");
        }
    }

    #[test]
    fn render_broker_toml_omits_oauthbearer_optional_keys_when_unset() {
        use std::collections::BTreeMap;
        let cfg = crate::crd::ListenerAuthenticationOAuth {
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
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        for (needle, want) in [
            ("[oauthbearer]", true),
            (
                "jwks_endpoint_uri = \"https://issuer.example.com/jwks\"",
                true,
            ),
            ("valid_issuer_uri = \"https://issuer.example.com/\"", true),
            ("expected_audience", false),
            ("principal_claim_name", false),
            ("scope_claim_name", false),
            ("required_scope", false),
            ("jwks_refresh_interval_ms", false),
            ("allowable_clock_skew_ms", false),
        ] {
            assert!(
                toml.contains(needle) == want,
                "needle {needle:?}: expected contains == {want}, TOML: {toml}"
            );
        }
    }

    #[test]
    fn render_broker_toml_emits_idp_tls_trust_when_trust_certs_present() {
        use std::collections::BTreeMap;
        let mut cfg = oauth_full_cfg();
        cfg.tls_trusted_certificates = vec![crate::crd::TlsTrustedCertificate {
            secret_name: "x".into(),
            certificate: "tls.crt".into(),
        }];
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("idp_tls_trust = \"/etc/crabka/oauth-jwks-trust/ca.crt\""),
            "TOML: {toml}"
        );
    }

    #[test]
    fn render_broker_toml_omits_idp_tls_trust_when_no_trust_certs() {
        use std::collections::BTreeMap;
        let cfg = crate::crd::ListenerAuthenticationOAuth {
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
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(!toml.contains("idp_tls_trust"), "TOML: {toml}");
    }

    #[test]
    fn render_broker_toml_emits_max_session_lifetime_seconds_when_set() {
        // When the listener's maxSecondsWithoutReauthentication
        // is set, the rendered [oauthbearer] block carries
        // `max_session_lifetime_seconds = N` so the broker clamps
        // OAUTHBEARER session_lifetime_ms tighter than the token's exp.
        use std::collections::BTreeMap;
        let mut cfg = oauth_full_cfg();
        cfg.max_seconds_without_reauthentication = Some(300);
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("max_session_lifetime_seconds = 300"),
            "expected TOML to contain max_session_lifetime_seconds = 300; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_omits_max_session_lifetime_seconds_when_unset() {
        // When maxSecondsWithoutReauthentication is unset
        // (default), the rendered TOML must NOT contain the key — the
        // broker then leaves session lifetime at the token's natural exp
        // Default behavior.
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !toml.contains("max_session_lifetime_seconds"),
            "TOML must omit max_session_lifetime_seconds when unset; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_appends_oauthbearer_to_listener_sasl_mechanisms() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("sasl_config = { enabled_mechanisms = [\"OAUTHBEARER\"] }"),
            "TOML: {toml}"
        );
        assert!(toml.contains("protocol = \"SaslSsl\""));
    }

    #[test]
    fn render_broker_toml_with_enable_false_keeps_oauthbearer_block_but_omits_mechanism() {
        use std::collections::BTreeMap;
        let mut cfg = oauth_full_cfg();
        cfg.enable_oauth_bearer = false;
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(toml.contains("[oauthbearer]"), "TOML: {toml}");
        assert!(!toml.contains("sasl_config"), "TOML: {toml}");
    }

    #[test]
    fn render_broker_toml_does_not_emit_oauthbearer_block_when_no_oauth_listener() {
        use std::collections::BTreeMap;
        let toml = render_broker_toml(
            (
                0,
                &[synthesized_default_listener()],
                &addrs_for("PLAIN", 9092),
                "PLAIN",
            ),
            (&BTreeMap::new(), None, None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(!toml.contains("[oauthbearer]"), "TOML: {toml}");
    }

    #[test]
    fn render_broker_toml_oauthbearer_block_parses_with_broker_file_config() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml).expect("rendered TOML must parse with broker FileConfig");
        let ob = parsed.oauthbearer.expect("oauthbearer block emitted");
        check!(
            ob.jwks_endpoint_uri.as_deref()
                == Some("https://kc.example.com/realms/kafka/protocol/openid-connect/certs")
        );
        check!(ob.valid_issuer_uri.as_deref() == Some("https://kc.example.com/realms/kafka"));
        check!(ob.expected_audience.as_deref() == Some("kafka"));
        check!(ob.principal_claim_name.as_deref() == Some("preferred_username"));
        check!(ob.custom_claim_check.as_deref() == Some("$.scope[?@ == 'kafka-broker']"));
        check!(ob.jwks_refresh_interval_ms == Some(300_000));
        check!(ob.allowable_clock_skew_ms == Some(30_000));
    }

    #[test]
    fn render_broker_toml_oauthbearer_render_is_deterministic() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_full_cfg(),
        )];
        let a = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        let b = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(a == b);
    }

    #[test]
    fn render_broker_toml_oauthbearer_block_emits_keys_in_canonical_order() {
        // Pin the exact byte sequence of the `[oauthbearer]` block. The
        // config hash that drives StatefulSet rollouts is taken over the
        // rendered TOML bytes, so a key reorder here is a silent
        // behavioural change. Use fixture values that are unique enough
        // that no other section of the TOML could accidentally contain
        // this substring.
        use std::collections::BTreeMap;
        let cfg = crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://idp.example/realms/kafka".into(),
            jwks_endpoint_uri: Some("https://idp.example/certs".into()),
            valid_audience: Some("kafka-broker".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
            jwks_refresh_seconds: Some(300),
            max_clock_skew_seconds: Some(60),
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
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        let expected = "[oauthbearer]\n\
            jwks_endpoint_uri = \"https://idp.example/certs\"\n\
            valid_issuer_uri = \"https://idp.example/realms/kafka\"\n\
            expected_audience = \"kafka-broker\"\n\
            principal_claim_name = \"preferred_username\"\n\
            jwks_refresh_interval_ms = 300000\n\
            allowable_clock_skew_ms = 60000\n\
            custom_claim_check = '''$.scope[?@ == 'kafka.write']'''\n";
        assert!(
            toml.contains(expected),
            "expected canonical [oauthbearer] block not found.\n--- expected ---\n{expected}\n--- got ---\n{toml}"
        );
    }

    // -----------------------------------------------------------------
    // Introspection-mode [oauthbearer] rendering
    // -----------------------------------------------------------------

    fn oauth_introspection_full_cfg() -> crate::crd::ListenerAuthenticationOAuth {
        crate::crd::ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://idp.example/realms/kafka".into(),
            jwks_endpoint_uri: None,
            valid_audience: Some("kafka-broker".into()),
            user_name_claim: Some("preferred_username".into()),
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: Some(60),
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://idp.example/introspect".into()),
            user_info_endpoint_uri: Some("https://idp.example/userinfo".into()),
            client_id: Some("kafka-broker".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: "introspection-creds".into(),
                key: "client-secret".into(),
            }),
            introspection_http_timeout_seconds: Some(15),
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
    fn render_broker_toml_emits_introspection_keys_when_introspection_mode() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_introspection_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "[oauthbearer]",
            "introspection_endpoint_uri = \"https://idp.example/introspect\"",
            "introspection_client_id = \"kafka-broker\"",
            "introspection_client_secret_path = \"/etc/crabka/oauth-introspection/client-secret\"",
        ] {
            assert!(toml.contains(needle), "missing {needle:?} in TOML: {toml}");
        }
    }

    #[test]
    fn render_broker_toml_omits_jwks_endpoint_uri_in_introspection_mode() {
        use std::collections::BTreeMap;
        let listeners = vec![oauth_listener_for_render(
            "oauth",
            9095,
            true,
            oauth_introspection_full_cfg(),
        )];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(!toml.contains("jwks_endpoint_uri"), "TOML: {toml}");
    }

    #[test]
    fn render_broker_toml_emits_userinfo_endpoint_when_set() {
        use std::collections::BTreeMap;
        let cfg = oauth_introspection_full_cfg();
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("userinfo_endpoint_uri = \"https://idp.example/userinfo\""),
            "TOML: {toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_introspection_http_timeout_ms_when_set() {
        use std::collections::BTreeMap;
        let mut cfg = oauth_introspection_full_cfg();
        cfg.introspection_http_timeout_seconds = Some(15);
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("introspection_http_timeout_ms = 15000"),
            "TOML: {toml}"
        );
    }

    #[test]
    fn render_broker_toml_oauthbearer_block_emits_introspection_keys_in_canonical_order() {
        // Pin the exact byte sequence of the introspection-mode keys
        // inside the `[oauthbearer]` block. The config hash that drives
        // StatefulSet rollouts is taken over the rendered TOML bytes, so
        // a key reorder here is a silent behavioural change.
        use std::collections::BTreeMap;
        let cfg = oauth_introspection_full_cfg();
        let listeners = vec![oauth_listener_for_render("oauth", 9095, true, cfg)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9095), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        let expected = "introspection_endpoint_uri = \"https://idp.example/introspect\"\n\
            userinfo_endpoint_uri = \"https://idp.example/userinfo\"\n\
            introspection_client_id = \"kafka-broker\"\n\
            introspection_client_secret_path = \"/etc/crabka/oauth-introspection/client-secret\"\n\
            introspection_http_timeout_ms = 15000\n";
        assert!(
            toml.contains(expected),
            "expected canonical introspection-mode block not found.\n--- expected ---\n{expected}\n--- got ---\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_mtls_listener_with_client_auth_required() {
        use std::collections::BTreeMap;
        let listeners = vec![Listener {
            name: "mtls".into(),
            port: 9095,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(crate::crd::ListenerAuthentication::Tls),
            configuration: None,
            network_policy_peers: None,
        }];
        let mut addrs = BTreeMap::new();
        addrs.insert(
            "mtls".to_string(),
            AdvertisedAddress {
                host: "broker-0".into(),
                port: 9095,
            },
        );
        let toml = render_broker_toml(
            (0, &listeners, &addrs, "mtls"),
            (
                &BTreeMap::new(),
                None,
                Some("/etc/crabka/clients-ca/ca.crt"),
            ),
            (false, None, None),
            None,
            (&[], ""),
        );
        for needle in [
            "protocol = \"Ssl\"",
            "client_ca_path = \"/etc/crabka/clients-ca/ca.crt\"",
            "client_auth = \"Required\"",
        ] {
            assert!(toml.contains(needle), "missing {needle:?} in TOML: {toml}");
        }
    }

    // -----------------------------------------------------------------
    // customClaimCheck + validTokenType render
    // -----------------------------------------------------------------

    #[test]
    fn render_broker_toml_emits_custom_claim_check_when_set() {
        // The JsonPath expression must be emitted in a TOML multi-line
        // literal (`'''...'''`) so embedded `'` and `"` characters in the
        // expression don't trip escape processing or string termination.
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.custom_claim_check = Some("$.scope[?@ == 'kafka.write']".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("custom_claim_check = '''$.scope[?@ == 'kafka.write']'''"),
            "expected custom_claim_check render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_valid_token_type_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.valid_token_type = Some("JWT".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("valid_token_type = \"JWT\""),
            "expected valid_token_type render; got:\n{toml}"
        );
    }

    // -----------------------------------------------------------------
    // Claims mapping render
    // -----------------------------------------------------------------

    #[test]
    fn render_broker_toml_emits_fallback_user_name_claim_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.fallback_user_name_claim = Some("client_id".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("fallback_user_name_claim = \"client_id\""),
            "expected fallback_user_name_claim render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_fallback_user_name_prefix_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.fallback_user_name_prefix = Some("service-account-".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("fallback_user_name_prefix = \"service-account-\""),
            "expected fallback_user_name_prefix render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_groups_claim_with_jsonpath_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.groups_claim = Some("$.realm_access.roles[*]".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("groups_claim = '''$.realm_access.roles[*]'''"),
            "expected groups_claim render (TOML multi-line literal); got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_groups_claim_delimiter_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.groups_claim_delimiter = Some(",".into());
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("groups_claim_delimiter = \",\""),
            "expected groups_claim_delimiter render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_omits_custom_claim_check_when_unset() {
        // Default oauth_full_cfg() now has custom_claim_check set; clear
        // it explicitly so the omit branch is exercised.
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.custom_claim_check = None;
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            !toml.contains("custom_claim_check"),
            "TOML must omit custom_claim_check when None; got:\n{toml}"
        );
    }

    // -----------------------------------------------------------------
    // JWKS refresher policy fields render
    // -----------------------------------------------------------------

    #[test]
    fn render_broker_toml_emits_jwks_min_refresh_pause_seconds_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.jwks_min_refresh_pause_seconds = Some(2);
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("jwks_min_refresh_pause_seconds = 2"),
            "expected jwks_min_refresh_pause_seconds render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_jwks_expiry_seconds_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.jwks_expiry_seconds = Some(3600);
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("jwks_expiry_seconds = 3600"),
            "expected jwks_expiry_seconds render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_emits_jwks_ignore_key_use_when_set() {
        use std::collections::BTreeMap;
        let mut oauth = oauth_full_cfg();
        oauth.jwks_ignore_key_use = Some(true);
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        assert!(
            toml.contains("jwks_ignore_key_use = true"),
            "expected jwks_ignore_key_use render; got:\n{toml}"
        );
    }

    #[test]
    fn render_broker_toml_omits_jwks_policy_fields_when_unset() {
        // Default oauth_full_cfg() leaves all 3 fields None; render
        // must not emit any of the keys.
        use std::collections::BTreeMap;
        let oauth = oauth_full_cfg();
        let listeners = vec![oauth_listener_for_render("oauth", 9096, true, oauth)];
        let toml = render_broker_toml(
            (0, &listeners, &addrs_for("oauth", 9096), "oauth"),
            (&BTreeMap::new(), Some(&render_tls()), None),
            (false, None, None),
            None,
            (&[], ""),
        );
        for key in [
            "jwks_min_refresh_pause_seconds",
            "jwks_expiry_seconds",
            "jwks_ignore_key_use",
        ] {
            assert!(
                !toml.contains(key),
                "TOML must omit {key} when None; got:\n{toml}"
            );
        }
    }
}

/// Serializes the intent of `spec.listeners` deterministically.
///
/// Empty or absent listeners give the empty string, so that a cluster with
/// no `spec.listeners` keeps its config hash through an upgrade.
#[allow(dead_code)]
pub fn canonical_listener_intent(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> String {
    use std::fmt::Write as _;
    if listeners.is_empty() {
        return String::new();
    }
    let mut s = String::new();
    if let Some(name) = inter_broker_listener_name {
        let _ = writeln!(s, "inter_broker={name}");
    }
    for l in listeners {
        let _ = writeln!(
            s,
            "listener:name={},port={},type={:?},tls={}",
            l.name, l.port, l.type_, l.tls
        );
        if let Some(cfg) = &l.configuration {
            if let Some(b) = &cfg.bootstrap {
                if let Some(np) = b.node_port {
                    let _ = writeln!(s, "  bootstrap.nodePort={np}");
                }
                if let Some(ip) = &b.load_balancer_ip {
                    let _ = writeln!(s, "  bootstrap.loadBalancerIP={ip}");
                }
            }
            let mut sorted = cfg.brokers.clone();
            sorted.sort_by_key(|o| o.broker);
            for o in &sorted {
                if let Some(h) = &o.advertised_host {
                    let _ = writeln!(s, "  broker{}.advertisedHost={h}", o.broker);
                }
                if let Some(p) = o.advertised_port {
                    let _ = writeln!(s, "  broker{}.advertisedPort={p}", o.broker);
                }
                if let Some(np) = o.node_port {
                    let _ = writeln!(s, "  broker{}.nodePort={np}", o.broker);
                }
                if let Some(ip) = &o.load_balancer_ip {
                    let _ = writeln!(s, "  broker{}.loadBalancerIP={ip}", o.broker);
                }
            }
        }
    }
    s
}

#[cfg(test)]
mod intent_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn empty_listeners_yields_empty_string() {
        assert!(canonical_listener_intent(&[], None) == "");
    }

    #[test]
    fn non_empty_listeners_yield_content() {
        let l = vec![synthesized_default_listener()];
        assert!(!canonical_listener_intent(&l, Some("PLAIN")).is_empty());
    }

    #[test]
    fn deterministic() {
        let l = vec![Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: Some(crate::crd::ListenerConfiguration {
                bootstrap: None,
                brokers: vec![
                    crate::crd::BrokerOverride {
                        broker: 1,
                        advertised_host: Some("h1".into()),
                        ..Default::default()
                    },
                    crate::crd::BrokerOverride {
                        broker: 0,
                        advertised_host: Some("h0".into()),
                        ..Default::default()
                    },
                ],
                ingress_class: None,
            }),
            network_policy_peers: None,
        }];
        let a = canonical_listener_intent(&l, Some("PLAIN"));
        let b = canonical_listener_intent(&l, Some("PLAIN"));
        assert!(a == b);
        // Sorted by broker id.
        let h0 = a.find("broker0.advertisedHost").unwrap();
        let h1 = a.find("broker1.advertisedHost").unwrap();
        assert!(h0 < h1);
    }
}

// ---------------------------------------------------------------------------
// SAN computation for external listeners
// ---------------------------------------------------------------------------

/// Observed addresses from the Kubernetes API for external listeners.
#[derive(Debug, Clone, Default)]
pub(crate) struct ListenerObservedAddresses {
    /// External node addresses for `NodePort` listeners.
    pub nodeport_node_addresses: Vec<NodeAddress>,
    /// Per-broker `LoadBalancer` ingress entries, keyed by broker id.
    pub lb_per_broker: BTreeMap<i32, Vec<LbIngress>>,
    /// Bootstrap Service `LoadBalancer` ingress entries.
    pub lb_bootstrap: Vec<LbIngress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeAddress {
    ExternalIp(IpAddr),
    ExternalDns(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LbIngress {
    Ip(IpAddr),
    Hostname(String),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum SanComputationError {
    #[error("LoadBalancer ingress not ready for broker {broker_id} on listener '{listener}'")]
    SansNotReady { broker_id: i32, listener: String },
}

/// Computes the extra SANs that external TLS listeners need for one
/// broker.
///
/// This is a pure function and does no I/O. It returns
/// `Err(SansNotReady)` when a `LoadBalancer` listener has TLS but the
/// per-broker ingress is not yet provisioned. Callers then skip the cert
/// issuance for that broker and requeue.
pub(crate) fn compute_extra_sans(
    broker_id: i32,
    listeners: &[Listener],
    observed: &ListenerObservedAddresses,
) -> Result<Vec<SubjectAltName>, SanComputationError> {
    let mut sans: Vec<SubjectAltName> = Vec::new();
    for l in listeners {
        if !l.tls {
            continue;
        }
        match l.type_ {
            ListenerType::Internal => {}
            ListenerType::Ingress | ListenerType::Route => {
                // SNI routing means the client presents the broker's external
                // hostname in the TLS ClientHello; the broker cert must carry it.
                if let Some(h) = ingress_broker_host(l, broker_id) {
                    sans.push(SubjectAltName::Dns(h));
                }
                if let Some(h) = ingress_bootstrap_host(l) {
                    sans.push(SubjectAltName::Dns(h));
                }
            }
            ListenerType::Nodeport => {
                for addr in &observed.nodeport_node_addresses {
                    match addr {
                        NodeAddress::ExternalIp(ip) => sans.push(SubjectAltName::Ip(*ip)),
                        NodeAddress::ExternalDns(d) => sans.push(SubjectAltName::Dns(d.clone())),
                    }
                }
                if let Some(cfg) = &l.configuration {
                    for ovr in &cfg.brokers {
                        if ovr.broker == broker_id
                            && let Some(h) = &ovr.advertised_host
                        {
                            sans.push(SubjectAltName::Dns(h.clone()));
                        }
                    }
                }
            }
            ListenerType::Loadbalancer => {
                let per_broker = observed.lb_per_broker.get(&broker_id);
                let bootstrap = &observed.lb_bootstrap;
                let Some(entries) = per_broker else {
                    return Err(SanComputationError::SansNotReady {
                        broker_id,
                        listener: l.name.clone(),
                    });
                };
                if entries.is_empty() {
                    return Err(SanComputationError::SansNotReady {
                        broker_id,
                        listener: l.name.clone(),
                    });
                }
                for ingress in entries.iter().chain(bootstrap.iter()) {
                    match ingress {
                        LbIngress::Ip(ip) => sans.push(SubjectAltName::Ip(*ip)),
                        LbIngress::Hostname(h) => sans.push(SubjectAltName::Dns(h.clone())),
                    }
                }
            }
        }
    }
    sans.sort();
    sans.dedup();
    Ok(sans)
}

/// Reads the external addresses that the SAN extension needs from the
/// Kubernetes API.
///
/// This function reads `Node.status.addresses` for `NodePort` listeners
/// and `Service.status.loadBalancer.ingress` for `LoadBalancer` listeners.
pub(crate) async fn observe_listener_addresses(
    ctx: &crate::context::Context,
    namespace: &str,
    cluster_name: &str,
    listeners: &[Listener],
    broker_ids: &[i32],
) -> Result<ListenerObservedAddresses, ReconcileError> {
    use kube::{Api, api::ListParams};

    let mut out = ListenerObservedAddresses::default();
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);

    let needs_node_addrs = listeners
        .iter()
        .any(|l| l.type_ == ListenerType::Nodeport && l.tls);
    if needs_node_addrs {
        let node_api: Api<Node> = Api::all(ctx.client.clone());
        let nodes = node_api.list(&ListParams::default()).await?;
        for node in &nodes {
            if let Some(status) = &node.status
                && let Some(addresses) = &status.addresses
            {
                for addr in addresses {
                    match addr.type_.as_str() {
                        "ExternalIP" => {
                            if let Ok(ip) = addr.address.parse() {
                                out.nodeport_node_addresses
                                    .push(NodeAddress::ExternalIp(ip));
                            }
                        }
                        "ExternalDNS" | "Hostname" => {
                            out.nodeport_node_addresses
                                .push(NodeAddress::ExternalDns(addr.address.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    for l in listeners {
        if l.type_ != ListenerType::Loadbalancer || !l.tls {
            continue;
        }
        for &broker_id in broker_ids {
            let svc_name = format!("{cluster_name}-{}-{broker_id}", l.name);
            if let Ok(svc) = svc_api.get(&svc_name).await
                && let Some(status) = svc.status
                && let Some(lb) = status.load_balancer
                && let Some(ingresses) = lb.ingress
            {
                for ingress in ingresses {
                    if let Some(ip) = ingress.ip.and_then(|s| s.parse().ok()) {
                        out.lb_per_broker
                            .entry(broker_id)
                            .or_default()
                            .push(LbIngress::Ip(ip));
                    }
                    if let Some(hn) = ingress.hostname {
                        out.lb_per_broker
                            .entry(broker_id)
                            .or_default()
                            .push(LbIngress::Hostname(hn));
                    }
                }
            }
        }
        let bootstrap_svc_name = format!("{cluster_name}-{}-bootstrap", l.name);
        if let Ok(svc) = svc_api.get(&bootstrap_svc_name).await
            && let Some(status) = svc.status
            && let Some(lb) = status.load_balancer
            && let Some(ingresses) = lb.ingress
        {
            for ingress in ingresses {
                if let Some(ip) = ingress.ip.and_then(|s| s.parse().ok()) {
                    out.lb_bootstrap.push(LbIngress::Ip(ip));
                }
                if let Some(hn) = ingress.hostname {
                    out.lb_bootstrap.push(LbIngress::Hostname(hn));
                }
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod san_tests {
    use assert2::assert;

    use super::*;

    fn internal_tls(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: true,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn compute_extra_sans_internal_only_returns_empty() {
        let listeners = vec![Listener {
            name: "internal".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        let observed = ListenerObservedAddresses::default();
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.is_empty());
    }

    #[test]
    fn compute_extra_sans_internal_tls_returns_empty() {
        let listeners = vec![internal_tls("internal", 9093)];
        let observed = ListenerObservedAddresses::default();
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.is_empty());
    }

    #[test]
    fn compute_extra_sans_nodeport_includes_node_external_addrs() {
        let listeners = vec![Listener {
            name: "ext".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: true,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        let observed = ListenerObservedAddresses {
            nodeport_node_addresses: vec![
                NodeAddress::ExternalIp("203.0.113.10".parse().unwrap()),
                NodeAddress::ExternalDns("node1.example.com".into()),
            ],
            ..Default::default()
        };
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.contains(&SubjectAltName::Ip("203.0.113.10".parse().unwrap())));
        assert!(sans.contains(&SubjectAltName::Dns("node1.example.com".into())));
    }

    #[test]
    fn compute_extra_sans_loadbalancer_includes_per_broker_and_bootstrap_ips() {
        let listeners = vec![Listener {
            name: "lb".into(),
            port: 9094,
            type_: ListenerType::Loadbalancer,
            tls: true,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        let mut observed = ListenerObservedAddresses::default();
        observed
            .lb_per_broker
            .insert(0, vec![LbIngress::Ip("203.0.113.20".parse().unwrap())]);
        observed.lb_bootstrap = vec![LbIngress::Ip("203.0.113.30".parse().unwrap())];
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.contains(&SubjectAltName::Ip("203.0.113.20".parse().unwrap())));
        assert!(sans.contains(&SubjectAltName::Ip("203.0.113.30".parse().unwrap())));
    }

    #[test]
    fn compute_extra_sans_loadbalancer_pending_returns_sans_not_ready() {
        let listeners = vec![Listener {
            name: "lb".into(),
            port: 9094,
            type_: ListenerType::Loadbalancer,
            tls: true,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        let observed = ListenerObservedAddresses::default();
        let result = compute_extra_sans(0, &listeners, &observed);
        assert!(matches!(
            result,
            Err(SanComputationError::SansNotReady { broker_id: 0, .. })
        ));
    }

    #[test]
    fn compute_extra_sans_ingress_includes_config_hostnames() {
        let listeners = vec![Listener {
            name: "ext".into(),
            port: 9094,
            type_: ListenerType::Ingress,
            tls: true,
            authentication: None,
            configuration: Some(crate::crd::ListenerConfiguration {
                bootstrap: Some(crate::crd::BootstrapConfig {
                    host: Some("bootstrap.kafka.example.com".into()),
                    ..Default::default()
                }),
                brokers: vec![crate::crd::BrokerOverride {
                    broker: 0,
                    host: Some("broker-0.kafka.example.com".into()),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        }];
        let observed = ListenerObservedAddresses::default();
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.contains(&SubjectAltName::Dns("broker-0.kafka.example.com".into())));
        assert!(sans.contains(&SubjectAltName::Dns("bootstrap.kafka.example.com".into())));
    }

    #[test]
    fn compute_extra_sans_route_includes_config_hostnames() {
        let listeners = vec![Listener {
            name: "ext".into(),
            port: 9094,
            type_: ListenerType::Route,
            tls: true,
            authentication: None,
            configuration: Some(crate::crd::ListenerConfiguration {
                bootstrap: Some(crate::crd::BootstrapConfig {
                    host: Some("bootstrap.kafka.example.com".into()),
                    ..Default::default()
                }),
                brokers: vec![crate::crd::BrokerOverride {
                    broker: 0,
                    host: Some("broker-0.kafka.example.com".into()),
                    ..Default::default()
                }],
                ingress_class: None,
            }),
            network_policy_peers: None,
        }];
        let observed = ListenerObservedAddresses::default();
        let sans = compute_extra_sans(0, &listeners, &observed).unwrap();
        assert!(sans.contains(&SubjectAltName::Dns("broker-0.kafka.example.com".into())));
        assert!(sans.contains(&SubjectAltName::Dns("bootstrap.kafka.example.com".into())));
    }
}

#[cfg(test)]
mod weak_auth_tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn weak_auth_warnings_emitted_for_scram_without_tls() {
        let listeners = vec![Listener {
            name: "scram-plain".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: false,
            authentication: Some(ListenerAuthentication::ScramSha512),
            configuration: None,
            network_policy_peers: None,
        }];
        let warnings = weak_auth_warnings(&listeners);
        assert!(warnings.len() == 1);
        check!(warnings[0].contains("scram-plain"));
        check!(warnings[0].contains("cleartext") || warnings[0].contains("TLS"));
    }

    #[test]
    fn weak_auth_warnings_empty_for_scram_with_tls() {
        let listeners = vec![Listener {
            name: "scram-tls".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::ScramSha512),
            configuration: None,
            network_policy_peers: None,
        }];
        let warnings = weak_auth_warnings(&listeners);
        assert!(warnings.is_empty());
    }

    #[test]
    fn weak_auth_warnings_empty_for_no_auth() {
        let listeners = vec![Listener {
            name: "plain".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }];
        assert!(weak_auth_warnings(&listeners).is_empty());
    }

    #[test]
    fn weak_auth_warnings_emitted_for_scram_256_without_tls() {
        let listeners = vec![Listener {
            name: "scram256-plain".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: false,
            authentication: Some(ListenerAuthentication::ScramSha256),
            configuration: None,
            network_policy_peers: None,
        }];
        assert!(weak_auth_warnings(&listeners).len() == 1);
    }

    fn oauth_listener(name: &str, jwks: &str) -> Listener {
        Listener {
            name: name.into(),
            port: 9095,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::OAuth(Box::new(
                crate::crd::ListenerAuthenticationOAuth {
                    valid_issuer_uri: "https://issuer.example.com/".into(),
                    jwks_endpoint_uri: Some(jwks.into()),
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
            ))),
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn weak_auth_warnings_emitted_for_oauth_with_http_jwks_uri() {
        let listeners = vec![oauth_listener("oauth", "http://idp/jwks")];
        let warnings = weak_auth_warnings(&listeners);
        assert!(warnings.len() == 1);
        for needle in ["oauth", "http://", "https"] {
            check!(warnings[0].contains(needle), "missing {needle:?}");
        }
    }

    #[test]
    fn weak_auth_warnings_empty_for_oauth_with_https_jwks_uri() {
        let listeners = vec![oauth_listener("oauth", "https://idp/jwks")];
        assert!(weak_auth_warnings(&listeners).is_empty());
    }
}
