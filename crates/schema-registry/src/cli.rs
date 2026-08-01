//! Clap-free CLI → [`SecurityConfig`] assembly.
//!
//! The binary (`src/bin/schema-registry.rs`) owns the `clap` `Args` derive and
//! maps it into the plain [`SecurityCliInput`] below; this module does the
//! validation and assembly so the security-config logic is unit-testable (a
//! binary's `main`/helpers are never executed by tests, which left the
//! assembly's many `bail!` branches uncovered). The mapping is intentionally
//! mechanical — behaviour is identical to the previous in-binary helpers.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use crabka_client_core::{
    ClientSecurity,
    security::{SaslCredentials, TlsConnectorConfig},
};
use crabka_security::{
    ClientAuthMode, Jwks, JwksHandle, ListenerProtocol, OAuthBearerValidator, SaslMechanism,
    SignedJwsValidator, TlsConfig,
};
use crabka_units::prelude::*;

use crate::config::{
    AuthzConfig, BasicAuthConfig, BearerAuthConfig, DEFAULT_ACL_REFRESH, SecurityConfig,
};

/// Default JWKS refresh interval.
pub const DEFAULT_JWKS_REFRESH: Time = minutes(1);

/// Plain (clap-free) inputs for assembling [`SecurityConfig`]. The binary maps
/// its clap `Args` into this; the lib does the validation/assembly so it is
/// unit-testable. The all-[`Default`] value (no TLS, no auth, no authz,
/// plaintext broker client) yields the fully-open [`SecurityConfig::default`]
/// behaviour.
#[derive(Debug, Default, Clone)]
pub struct SecurityCliInput {
    /// Reject unauthenticated (anonymous) requests with `401`.
    pub require_auth: bool,
    /// `WWW-Authenticate: basic realm="<realm>"` realm advertised on `401`.
    pub realm: String,
    /// htpasswd-style `user:cred` file for HTTP Basic.
    pub basic_auth_file: Option<PathBuf>,
    /// Inline Basic credentials as `user:cred` (repeatable).
    pub basic_users: Vec<String>,
    /// Bearer-token mode: `off` | `unsecured` | `jwks`.
    pub bearer: String,
    /// JWT claim whose value becomes the principal name (Bearer mode).
    pub bearer_principal_claim: String,
    // ── JWKS fields (bearer = "jwks" only) ─────────────────────────────────
    pub jwks_endpoint_uri: Option<String>,
    pub jwks_valid_issuer: Option<String>,
    pub jwks_expected_audience: Option<String>,
    pub jwks_ca: Option<std::path::PathBuf>,
    pub jwks_principal_claim: Option<String>,
    /// JWKS refresh interval. `None` uses [`DEFAULT_JWKS_REFRESH`].
    pub jwks_refresh: Option<Time>,
    /// Server cert chain (PEM); enables HTTPS with `tls_key`.
    pub tls_cert: Option<PathBuf>,
    /// Server private key (PEM).
    pub tls_key: Option<PathBuf>,
    /// CA(s) verifying incoming client certs (mTLS).
    pub tls_client_ca: Option<PathBuf>,
    /// Client-cert mode: `disabled` | `optional` | `required`.
    pub tls_client_auth: String,
    /// Enable topic-ACL authorization.
    pub authz: bool,
    /// Super-user principal names that bypass ACL checks.
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval. `None` uses
    /// [`DEFAULT_ACL_REFRESH`](crate::config::DEFAULT_ACL_REFRESH).
    pub acl_refresh: Option<Time>,
    /// Kafka client protocol: `PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` | `SASL_SSL`.
    pub kafka_security_protocol: String,
    /// SASL mechanism: `PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`.
    pub kafka_sasl_mechanism: String,
    /// SASL username (PLAIN / SCRAM).
    pub kafka_sasl_username: Option<String>,
    /// SASL password (PLAIN / SCRAM).
    pub kafka_sasl_password: Option<String>,
    /// CA(s) (PEM) trusted for the broker's server cert (SSL / `SASL_SSL`).
    pub kafka_tls_ca: Option<PathBuf>,
    /// TLS SNI / server name for the broker connection (SSL / `SASL_SSL`).
    pub kafka_tls_server_name: Option<String>,
}

/// JWKS key-set handle plus the metadata the binary needs to drive the
/// periodic refresh task. Returned by [`build_security`] when
/// `--bearer=jwks` is configured; the binary spawns `run_jwks_refresher`.
#[derive(Debug)]
pub struct JwksHandleForRefresh {
    /// The live key-set cell shared with the `SignedJwsValidator`.
    pub handle: JwksHandle,
    /// URL of the JWKS endpoint (`--bearer-jwks-endpoint-uri`).
    pub endpoint_uri: String,
    /// Optional CA bundle trusted for the JWKS HTTPS connection.
    pub ca_path: Option<std::path::PathBuf>,
    /// How often the key set is re-fetched.
    pub refresh: Time,
}

/// Return value of [`build_security`]: the assembled [`SecurityConfig`] plus,
/// when `--bearer=jwks` is set, a [`JwksHandleForRefresh`] the binary must
/// hand to `run_jwks_refresher`.
#[derive(Debug)]
pub struct SecurityOutput {
    pub config: SecurityConfig,
    pub jwks_handle: Option<JwksHandleForRefresh>,
}

impl std::ops::Deref for SecurityOutput {
    type Target = SecurityConfig;
    fn deref(&self) -> &SecurityConfig {
        &self.config
    }
}

/// Assemble [`SecurityConfig`] from [`SecurityCliInput`]. The all-defaults case
/// (no TLS, no auth, no authz, plaintext broker client) yields the fully-open
/// [`SecurityConfig::default`] behaviour.
///
/// # Errors
///
/// Returns an error for an invalid `bearer`/`tls_client_auth`/
/// `kafka_security_protocol`/`kafka_sasl_mechanism` value, a `tls_cert` set
/// without `tls_key` (or vice versa), or a `SASL_*` protocol missing its SASL
/// username/password.
pub fn build_security(input: &SecurityCliInput) -> anyhow::Result<SecurityOutput> {
    let (bearer, jwks_handle) = build_bearer(input)?;
    Ok(SecurityOutput {
        config: SecurityConfig {
            require_auth: input.require_auth,
            realm: input.realm.clone(),
            basic: build_basic(input),
            bearer,
            tls: build_tls(input)?,
            authz: build_authz(input),
            client: build_client_security(input)?,
        },
        jwks_handle,
    })
}

/// Build [`BasicAuthConfig`] from `basic_auth_file` / `basic_users`. Returns
/// `None` when neither is supplied (Basic disabled). A malformed `basic_users`
/// entry (no `:`) is warned about and skipped.
fn build_basic(input: &SecurityCliInput) -> Option<BasicAuthConfig> {
    if input.basic_auth_file.is_none() && input.basic_users.is_empty() {
        return None;
    }
    let mut users = HashMap::new();
    for entry in &input.basic_users {
        if let Some((u, c)) = entry.split_once(':') {
            users.insert(u.to_string(), c.to_string());
        } else {
            tracing::warn!(entry = %entry, "ignoring malformed --basic-user (want user:cred)");
        }
    }
    Some(BasicAuthConfig {
        users,
        file: input.basic_auth_file.clone(),
    })
}

/// Build [`BearerAuthConfig`] from `bearer`. `off` ⇒ `None`; `unsecured` ⇒ a
/// dev `UnsecuredJwsValidator` (mirrors the gateway); `jwks` ⇒ a
/// `SignedJwsValidator` backed by a refreshable [`JwksHandle`].
fn build_bearer(
    input: &SecurityCliInput,
) -> anyhow::Result<(Option<BearerAuthConfig>, Option<JwksHandleForRefresh>)> {
    match input.bearer.as_str() {
        "off" => Ok((None, None)),
        "unsecured" => {
            let validator =
                OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
                    principal_claim_name: input.bearer_principal_claim.clone(),
                    ..Default::default()
                });
            Ok((
                Some(BearerAuthConfig {
                    validator: Arc::new(validator),
                }),
                None,
            ))
        }
        "jwks" => {
            let (cfg, refresh) = build_bearer_jwks(input)?;
            Ok((Some(cfg), Some(refresh)))
        }
        other => anyhow::bail!("invalid --bearer: {other} (want off|unsecured|jwks)"),
    }
}

fn build_bearer_jwks(
    input: &SecurityCliInput,
) -> anyhow::Result<(BearerAuthConfig, JwksHandleForRefresh)> {
    let endpoint_uri = input
        .jwks_endpoint_uri
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!("--bearer-jwks-endpoint-uri is required when --bearer=jwks")
        })?
        .clone();

    let handle = JwksHandle::new(Jwks::empty());
    let mut validator = SignedJwsValidator::new(handle.clone());
    validator.principal_claim_name = input
        .jwks_principal_claim
        .clone()
        .unwrap_or_else(|| input.bearer_principal_claim.clone());
    validator.valid_issuer.clone_from(&input.jwks_valid_issuer);
    validator
        .expected_audience
        .clone_from(&input.jwks_expected_audience);

    let cfg = BearerAuthConfig {
        validator: Arc::new(OAuthBearerValidator::Signed(validator)),
    };
    let refresh = JwksHandleForRefresh {
        handle,
        endpoint_uri,
        ca_path: input.jwks_ca.clone(),
        refresh: input.jwks_refresh.unwrap_or(DEFAULT_JWKS_REFRESH),
    };
    Ok((cfg, refresh))
}

/// Build server [`TlsConfig`] from the `tls_*` inputs. Requires both cert+key
/// or neither; `None` ⇒ plain HTTP.
fn build_tls(input: &SecurityCliInput) -> anyhow::Result<Option<TlsConfig>> {
    match (input.tls_cert.clone(), input.tls_key.clone()) {
        (Some(cert_chain_path), Some(private_key_path)) => {
            let client_auth = match input.tls_client_auth.as_str() {
                "disabled" => ClientAuthMode::Disabled,
                "optional" => ClientAuthMode::Optional,
                "required" => ClientAuthMode::Required,
                other => anyhow::bail!("invalid --tls-client-auth: {other}"),
            };
            Ok(Some(TlsConfig {
                cert_chain_path,
                private_key_path,
                trust_roots_path: input.tls_client_ca.clone(),
                client_ca_path: input.tls_client_ca.clone(),
                client_auth,
            }))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
    }
}

/// Build [`AuthzConfig`] from the `authz` / `super_users` inputs.
fn build_authz(input: &SecurityCliInput) -> Option<AuthzConfig> {
    if !input.authz {
        return None;
    }
    let super_users: HashSet<String> = input.super_users.iter().cloned().collect();
    Some(AuthzConfig {
        enabled: true,
        super_users,
        acl_refresh: input.acl_refresh.unwrap_or(DEFAULT_ACL_REFRESH),
    })
}

/// Build SR → broker [`ClientSecurity`] from `kafka_*`. `PLAINTEXT` ⇒ `None`
/// (plaintext, the pre-security default). PLAIN/SCRAM + TLS-CA are covered;
/// GSSAPI and client-cert (mTLS to the broker) are config-struct-supported but
/// not yet CLI-exposed.
fn build_client_security(input: &SecurityCliInput) -> anyhow::Result<Option<ClientSecurity>> {
    let protocol = match input.kafka_security_protocol.to_ascii_uppercase().as_str() {
        "PLAINTEXT" => return Ok(None),
        "SSL" => ListenerProtocol::Ssl,
        "SASL_PLAINTEXT" => ListenerProtocol::SaslPlaintext,
        "SASL_SSL" => ListenerProtocol::SaslSsl,
        other => anyhow::bail!(
            "invalid --kafka-security-protocol: {other} (want PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL)"
        ),
    };

    let tls = if protocol.requires_tls() {
        Some(TlsConnectorConfig {
            trust_roots_pem: input.kafka_tls_ca.clone(),
            server_name: input
                .kafka_tls_server_name
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
            // One-way TLS to the broker (no client cert / mTLS).
            client_identity: None,
        })
    } else {
        None
    };

    let sasl = if protocol.requires_sasl() {
        Some(build_sasl(input)?)
    } else {
        None
    };

    Ok(Some(ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host: None,
    }))
}

/// Build the SASL credential set for a `SASL_*` broker protocol.
fn build_sasl(input: &SecurityCliInput) -> anyhow::Result<SaslCredentials> {
    let username = input
        .kafka_sasl_username
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--kafka-sasl-username required for SASL_* protocols"))?;
    let password = input
        .kafka_sasl_password
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--kafka-sasl-password required for SASL_* protocols"))?;
    match input.kafka_sasl_mechanism.to_ascii_uppercase().as_str() {
        "PLAIN" => Ok(SaslCredentials::Plain { username, password }),
        "SCRAM-SHA-256" => Ok(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha256,
            username,
            password,
        }),
        "SCRAM-SHA-512" => Ok(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username,
            password,
        }),
        other => anyhow::bail!(
            "invalid --kafka-sasl-mechanism: {other} (want PLAIN|SCRAM-SHA-256|SCRAM-SHA-512); \
             GSSAPI is not yet CLI-exposed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Convenience: the binary's clap defaults for the string-typed knobs, so a
    /// test sets only the fields it exercises. Mirrors the `Args` `default_value`
    /// attributes (bearer `off`, client-auth `disabled`, broker protocol
    /// `PLAINTEXT`, SASL mechanism `PLAIN`, ACL refresh `30s`).
    fn input() -> SecurityCliInput {
        SecurityCliInput {
            bearer: "off".to_string(),
            bearer_principal_claim: "sub".to_string(),
            tls_client_auth: "disabled".to_string(),
            acl_refresh: Some(secs(30)),
            kafka_security_protocol: "PLAINTEXT".to_string(),
            kafka_sasl_mechanism: "PLAIN".to_string(),
            ..Default::default()
        }
    }

    /// Unwrap the [`SecurityConfig`] out of a [`SecurityOutput`] for the
    /// existing tests that only care about the assembled config.
    fn sec(input: &SecurityCliInput) -> SecurityConfig {
        build_security(input).unwrap().config
    }

    // ---- defaults --------------------------------------------------------

    #[test]
    fn default_input_is_fully_open() {
        let s = sec(&input());
        check!(
            (
                s.require_auth,
                s.realm.is_empty(),
                s.basic.is_none(),
                s.bearer.is_none(),
                s.tls.is_none(),
                s.authz.is_none(),
                s.client.is_none(),
            ) == (false, true, true, true, true, true, true)
        );
    }

    #[test]
    fn require_auth_and_realm_passthrough() {
        let s = sec(&SecurityCliInput {
            require_auth: true,
            realm: "MyRealm".to_string(),
            ..input()
        });
        assert2::assert!(s.require_auth);
        assert2::assert!(s.realm.as_str() == "MyRealm");
    }

    // ---- basic -----------------------------------------------------------

    #[test]
    fn basic_inline_users() {
        let s = sec(&SecurityCliInput {
            basic_users: vec!["alice:pw".to_string(), "bob:bpw".to_string()],
            ..input()
        });
        let b = s.basic.expect("Basic enabled by inline users");
        assert2::assert!(
            b.users
                == [
                    ("alice".to_string(), "pw".to_string()),
                    ("bob".to_string(), "bpw".to_string())
                ]
                .into_iter()
                .collect()
        );
        assert2::assert!(b.file == None);
    }

    #[test]
    fn basic_malformed_inline_user_is_skipped() {
        // Entry with no ':' is warned + dropped; the valid one still loads.
        let s = sec(&SecurityCliInput {
            basic_users: vec!["no-colon".to_string(), "alice:pw".to_string()],
            ..input()
        });
        let b = s.basic.expect("Basic still enabled");
        assert2::assert!(
            b.users
                == [("alice".to_string(), "pw".to_string())]
                    .into_iter()
                    .collect()
        );
    }

    #[test]
    fn basic_file_only() {
        let path = PathBuf::from("/etc/sr/htpasswd");
        let s = sec(&SecurityCliInput {
            basic_auth_file: Some(path.clone()),
            ..input()
        });
        let b = s.basic.expect("Basic enabled by file");
        assert2::assert!(b.users == std::collections::HashMap::new());
        assert2::assert!(b.file == Some(path));
    }

    #[test]
    fn basic_file_and_inline_both_set() {
        let path = PathBuf::from("/etc/sr/htpasswd");
        let s = sec(&SecurityCliInput {
            basic_auth_file: Some(path.clone()),
            basic_users: vec!["alice:pw".to_string()],
            ..input()
        });
        let b = s.basic.expect("Basic enabled");
        assert2::assert!(
            b.users
                == [("alice".to_string(), "pw".to_string())]
                    .into_iter()
                    .collect()
        );
        assert2::assert!(b.file == Some(path));
    }

    // ---- bearer ----------------------------------------------------------

    #[test]
    fn bearer_off_is_none() {
        let s = sec(&SecurityCliInput {
            bearer: "off".to_string(),
            ..input()
        });
        assert2::assert!(s.bearer.is_none());
    }

    #[test]
    fn bearer_unsecured_builds_validator_with_principal_claim() {
        let s = sec(&SecurityCliInput {
            bearer: "unsecured".to_string(),
            bearer_principal_claim: "preferred_username".to_string(),
            ..input()
        });
        let b = s.bearer.expect("Bearer enabled");
        match &*b.validator {
            OAuthBearerValidator::Unsecured(v) => {
                assert2::assert!(v.principal_claim_name == "preferred_username");
            }
            other => panic!("expected Unsecured validator, got {other:?}"),
        }
    }

    #[test]
    fn bearer_invalid_value_errors() {
        let bad = SecurityCliInput {
            bearer: "signed".to_string(),
            ..input()
        };
        assert2::assert!(build_security(&bad).is_err());
    }

    // ---- tls -------------------------------------------------------------

    #[test]
    fn tls_cert_and_key_build_config_with_each_client_auth() {
        for (mode, expected) in [
            ("disabled", ClientAuthMode::Disabled),
            ("optional", ClientAuthMode::Optional),
            ("required", ClientAuthMode::Required),
        ] {
            let s = sec(&SecurityCliInput {
                tls_cert: Some(PathBuf::from("/c.pem")),
                tls_key: Some(PathBuf::from("/k.pem")),
                tls_client_ca: Some(PathBuf::from("/ca.pem")),
                tls_client_auth: mode.to_string(),
                ..input()
            });
            let tls = s.tls.unwrap_or_else(|| panic!("TLS enabled for {mode}"));
            assert2::assert!(tls.cert_chain_path == PathBuf::from("/c.pem"));
            assert2::assert!(tls.private_key_path == PathBuf::from("/k.pem"));
            assert2::assert!(tls.client_ca_path == Some(PathBuf::from("/ca.pem")));
            assert2::assert!(tls.trust_roots_path == Some(PathBuf::from("/ca.pem")));
            assert2::assert!(tls.client_auth == expected);
        }
    }

    #[test]
    fn tls_invalid_configuration_cases_error() {
        let cases = [
            (
                "certificate without key",
                SecurityCliInput {
                    tls_cert: Some(PathBuf::from("/c.pem")),
                    tls_key: None,
                    ..input()
                },
            ),
            (
                "key without certificate",
                SecurityCliInput {
                    tls_cert: None,
                    tls_key: Some(PathBuf::from("/k.pem")),
                    ..input()
                },
            ),
            (
                "invalid client auth",
                SecurityCliInput {
                    tls_cert: Some(PathBuf::from("/c.pem")),
                    tls_key: Some(PathBuf::from("/k.pem")),
                    tls_client_auth: "mutual".to_string(),
                    ..input()
                },
            ),
        ];
        for (_name, bad) in cases {
            assert2::assert!(build_security(&bad).is_err());
        }
    }

    // ---- authz -----------------------------------------------------------

    #[test]
    fn authz_enabled_builds_config() {
        let s = sec(&SecurityCliInput {
            authz: true,
            super_users: vec!["admin".to_string(), "root".to_string()],
            acl_refresh: Some(secs(45)),
            ..input()
        });
        let a = s.authz.expect("authz enabled");
        assert2::assert!(
            a == crate::config::AuthzConfig {
                enabled: true,
                super_users: ["admin".to_string(), "root".to_string()]
                    .into_iter()
                    .collect(),
                acl_refresh: secs(45),
            }
        );
    }

    #[test]
    fn authz_default_refresh() {
        let s = sec(&SecurityCliInput {
            authz: true,
            ..input()
        });
        let a = s.authz.expect("authz enabled");
        assert2::assert!(
            a == crate::config::AuthzConfig {
                enabled: true,
                super_users: std::collections::HashSet::new(),
                acl_refresh: secs(30),
            }
        );
    }

    // ---- client (SR → broker) security -----------------------------------

    #[test]
    fn client_plaintext_protocol_cases_are_none() {
        for (_name, protocol) in [
            ("canonical_uppercase", "PLAINTEXT"),
            ("case_insensitive_lowercase", "plaintext"),
        ] {
            let security = sec(&SecurityCliInput {
                kafka_security_protocol: protocol.to_string(),
                ..input()
            });
            assert2::assert!(security.client.is_none());
        }
    }

    #[test]
    fn client_ssl_has_tls_no_sasl() {
        let s = sec(&SecurityCliInput {
            kafka_security_protocol: "SSL".to_string(),
            kafka_tls_ca: Some(PathBuf::from("/broker-ca.pem")),
            kafka_tls_server_name: Some("broker.internal".to_string()),
            ..input()
        });
        let c = s.client.expect("SSL ⇒ Some(ClientSecurity)");
        let tls = c.tls.expect("SSL requires TLS");
        assert2::assert!(c.protocol == ListenerProtocol::Ssl);
        assert2::assert!(tls.trust_roots_pem == Some(PathBuf::from("/broker-ca.pem")));
        assert2::assert!(tls.server_name == "broker.internal".to_string());
        assert2::assert!(tls.client_identity == None);
        assert2::assert!(c.sasl.is_none());
    }

    #[test]
    fn client_ssl_default_server_name_is_localhost() {
        let s = sec(&SecurityCliInput {
            kafka_security_protocol: "SSL".to_string(),
            ..input()
        });
        let tls = s.client.unwrap().tls.unwrap();
        assert2::assert!(tls.server_name == "localhost".to_string());
        assert2::assert!(tls.trust_roots_pem == None);
        assert2::assert!(tls.client_identity == None);
    }

    #[test]
    fn client_sasl_plaintext_has_sasl_no_tls() {
        let s = sec(&SecurityCliInput {
            kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
            kafka_sasl_mechanism: "PLAIN".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        });
        let c = s.client.expect("SASL_PLAINTEXT ⇒ Some");
        let credentials = match c.sasl.expect("SASL configured") {
            SaslCredentials::Plain { username, password } => (username, password),
            other => panic!("expected PLAIN, got {other:?}"),
        };
        assert2::assert!(c.protocol == ListenerProtocol::SaslPlaintext);
        assert2::assert!(c.tls.is_none());
        assert2::assert!(credentials.0.as_str() == "u");
        assert2::assert!(credentials.1.as_str() == "p");
    }

    #[test]
    fn client_sasl_ssl_has_both() {
        let s = sec(&SecurityCliInput {
            kafka_security_protocol: "SASL_SSL".to_string(),
            kafka_sasl_mechanism: "SCRAM-SHA-256".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        });
        let c = s.client.expect("SASL_SSL ⇒ Some");
        assert2::assert!(c.protocol == ListenerProtocol::SaslSsl);
        assert2::assert!(c.tls.is_some());
        assert2::assert!(matches!(c.sasl, Some(SaslCredentials::Scram { .. })));
    }

    #[test]
    fn client_sasl_mechanisms_map() {
        for (mech, want) in [
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
        ] {
            let s = sec(&SecurityCliInput {
                kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
                kafka_sasl_mechanism: mech.to_string(),
                kafka_sasl_username: Some("u".to_string()),
                kafka_sasl_password: Some("p".to_string()),
                ..input()
            });
            match s.client.unwrap().sasl.unwrap() {
                SaslCredentials::Scram { mechanism, .. } => {
                    assert2::assert!(mechanism == want);
                }
                other => panic!("expected SCRAM for {mech}, got {other:?}"),
            }
        }
    }

    #[test]
    fn invalid_client_security_cases() {
        for (_name, protocol, mechanism, username, password) in [
            (
                "missing_username",
                "SASL_PLAINTEXT",
                "PLAIN",
                None,
                Some("p"),
            ),
            (
                "missing_password",
                "SASL_PLAINTEXT",
                "PLAIN",
                Some("u"),
                None,
            ),
            ("bad_protocol", "SASL_GSSAPI", "PLAIN", None, None),
            (
                "bad_mechanism",
                "SASL_PLAINTEXT",
                "GSSAPI",
                Some("u"),
                Some("p"),
            ),
        ] {
            let bad = SecurityCliInput {
                kafka_security_protocol: protocol.to_owned(),
                kafka_sasl_mechanism: mechanism.to_owned(),
                kafka_sasl_username: username.map(str::to_owned),
                kafka_sasl_password: password.map(str::to_owned),
                ..input()
            };
            assert2::assert!(build_security(&bad).is_err());
        }
    }

    // ---- JWKS bearer --------------------------------------------------------

    #[test]
    fn bearer_jwks_off_variant_returns_none_handle() {
        let out = build_security(&input()).unwrap();
        assert2::assert!(out.jwks_handle.is_none());
    }

    #[test]
    fn bearer_jwks_builds_signed_validator() {
        let i = SecurityCliInput {
            bearer: "jwks".into(),
            jwks_endpoint_uri: Some("https://idp.example.com/.well-known/jwks.json".into()),
            ..input()
        };
        let out = build_security(&i).unwrap();
        let bearer_configured = out.bearer.is_some();
        let h = out.jwks_handle.unwrap();
        assert2::assert!(bearer_configured);
        assert2::assert!(
            h.endpoint_uri.as_str() == "https://idp.example.com/.well-known/jwks.json"
        );
        assert2::assert!(h.ca_path == None);
        assert2::check!(h.refresh == minutes(1));
    }

    #[test]
    fn bearer_jwks_missing_endpoint_errors() {
        let i = SecurityCliInput {
            bearer: "jwks".into(),
            jwks_endpoint_uri: None,
            ..input()
        };
        let err = build_security(&i).unwrap_err().to_string();
        assert2::assert!(err.contains("--bearer-jwks-endpoint-uri"));
    }

    #[test]
    fn bearer_jwks_sets_issuer_and_audience() {
        let i = SecurityCliInput {
            bearer: "jwks".into(),
            jwks_endpoint_uri: Some("https://idp/jwks".into()),
            jwks_valid_issuer: Some("https://idp".into()),
            jwks_expected_audience: Some("kafka-sr".into()),
            jwks_principal_claim: Some("email".into()),
            ..input()
        };
        let out = build_security(&i).unwrap();
        let bearer = out.bearer.as_ref().expect("signed bearer config");
        let OAuthBearerValidator::Signed(validator) = bearer.validator.as_ref() else {
            panic!("expected signed validator");
        };
        let h = out.jwks_handle.as_ref().unwrap();
        assert2::assert!(h.endpoint_uri.as_str() == "https://idp/jwks");
        assert2::assert!(validator.valid_issuer.as_deref() == Some("https://idp"));
        assert2::assert!(validator.expected_audience.as_deref() == Some("kafka-sr"));
        assert2::assert!(validator.principal_claim_name.as_str() == "email");
    }

    #[test]
    fn bearer_jwks_custom_refresh() {
        let i = SecurityCliInput {
            bearer: "jwks".into(),
            jwks_endpoint_uri: Some("https://idp/jwks".into()),
            jwks_refresh: Some(minutes(2)),
            ..input()
        };
        let out = build_security(&i).unwrap();
        assert2::check!(out.jwks_handle.unwrap().refresh == minutes(2));
    }
}
