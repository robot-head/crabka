//! Clap-free CLI → [`SecurityConfig`] assembly.
//!
//! The binary (`src/bin/schema-registry.rs`) owns the `clap` `Args` derive and
//! maps it into the plain [`SecurityCliInput`] below; this module does the
//! validation and assembly so the security-config logic is unit-testable (a
//! binary's `main`/helpers are never executed by tests, which left the
//! assembly's many `bail!` branches uncovered). The mapping is intentionally
//! mechanical — behaviour is identical to the previous in-binary helpers.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crabka_client_core::ClientSecurity;
use crabka_client_core::security::{SaslCredentials, TlsConnectorConfig};
use crabka_security::{
    ClientAuthMode, ListenerProtocol, OAuthBearerValidator, SaslMechanism, TlsConfig,
};

use crate::config::{AuthzConfig, BasicAuthConfig, BearerAuthConfig, SecurityConfig};

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
    /// Bearer-token mode: `off` | `unsecured`.
    pub bearer: String,
    /// JWT claim whose value becomes the principal name (Bearer mode).
    pub bearer_principal_claim: String,
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
    /// ACL-cache refresh interval (seconds).
    pub acl_refresh_secs: u64,
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
pub fn build_security(input: &SecurityCliInput) -> anyhow::Result<SecurityConfig> {
    Ok(SecurityConfig {
        require_auth: input.require_auth,
        realm: input.realm.clone(),
        basic: build_basic(input),
        bearer: build_bearer(input)?,
        tls: build_tls(input)?,
        authz: build_authz(input),
        client: build_client_security(input)?,
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
/// dev `UnsecuredJwsValidator` (mirrors the gateway). Signed/`JWKS` validators
/// are supported by the config struct but not yet CLI-exposed.
fn build_bearer(input: &SecurityCliInput) -> anyhow::Result<Option<BearerAuthConfig>> {
    match input.bearer.as_str() {
        "off" => Ok(None),
        "unsecured" => {
            let validator =
                OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
                    principal_claim_name: input.bearer_principal_claim.clone(),
                    ..Default::default()
                });
            Ok(Some(BearerAuthConfig {
                validator: Arc::new(validator),
            }))
        }
        other => anyhow::bail!("invalid --bearer: {other} (want off|unsecured)"),
    }
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
        acl_refresh: std::time::Duration::from_secs(input.acl_refresh_secs),
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
    use super::*;

    /// Convenience: the binary's clap defaults for the string-typed knobs, so a
    /// test sets only the fields it exercises. Mirrors the `Args` `default_value`
    /// attributes (bearer `off`, client-auth `disabled`, broker protocol
    /// `PLAINTEXT`, SASL mechanism `PLAIN`, ACL refresh `30`).
    fn input() -> SecurityCliInput {
        SecurityCliInput {
            bearer: "off".to_string(),
            bearer_principal_claim: "sub".to_string(),
            tls_client_auth: "disabled".to_string(),
            acl_refresh_secs: 30,
            kafka_security_protocol: "PLAINTEXT".to_string(),
            kafka_sasl_mechanism: "PLAIN".to_string(),
            ..Default::default()
        }
    }

    // ---- defaults --------------------------------------------------------

    #[test]
    fn default_input_is_fully_open() {
        let s = build_security(&input()).unwrap();
        assert!(!s.require_auth);
        assert!(s.realm.is_empty());
        assert!(s.basic.is_none());
        assert!(s.bearer.is_none());
        assert!(s.tls.is_none());
        assert!(s.authz.is_none());
        assert!(s.client.is_none(), "PLAINTEXT broker client ⇒ None");
    }

    #[test]
    fn require_auth_and_realm_passthrough() {
        let s = build_security(&SecurityCliInput {
            require_auth: true,
            realm: "MyRealm".to_string(),
            ..input()
        })
        .unwrap();
        assert!(s.require_auth);
        assert_eq!(s.realm, "MyRealm");
    }

    // ---- basic -----------------------------------------------------------

    #[test]
    fn basic_inline_users() {
        let s = build_security(&SecurityCliInput {
            basic_users: vec!["alice:pw".to_string(), "bob:bpw".to_string()],
            ..input()
        })
        .unwrap();
        let b = s.basic.expect("Basic enabled by inline users");
        assert_eq!(b.users.get("alice").map(String::as_str), Some("pw"));
        assert_eq!(b.users.get("bob").map(String::as_str), Some("bpw"));
        assert!(b.file.is_none());
    }

    #[test]
    fn basic_malformed_inline_user_is_skipped() {
        // Entry with no ':' is warned + dropped; the valid one still loads.
        let s = build_security(&SecurityCliInput {
            basic_users: vec!["no-colon".to_string(), "alice:pw".to_string()],
            ..input()
        })
        .unwrap();
        let b = s.basic.expect("Basic still enabled");
        assert_eq!(b.users.len(), 1);
        assert_eq!(b.users.get("alice").map(String::as_str), Some("pw"));
    }

    #[test]
    fn basic_file_only() {
        let path = PathBuf::from("/etc/sr/htpasswd");
        let s = build_security(&SecurityCliInput {
            basic_auth_file: Some(path.clone()),
            ..input()
        })
        .unwrap();
        let b = s.basic.expect("Basic enabled by file");
        assert!(b.users.is_empty(), "file is read at load(), not here");
        assert_eq!(b.file, Some(path));
    }

    #[test]
    fn basic_file_and_inline_both_set() {
        let path = PathBuf::from("/etc/sr/htpasswd");
        let s = build_security(&SecurityCliInput {
            basic_auth_file: Some(path.clone()),
            basic_users: vec!["alice:pw".to_string()],
            ..input()
        })
        .unwrap();
        let b = s.basic.expect("Basic enabled");
        assert_eq!(b.file, Some(path));
        assert_eq!(b.users.get("alice").map(String::as_str), Some("pw"));
    }

    // ---- bearer ----------------------------------------------------------

    #[test]
    fn bearer_off_is_none() {
        let s = build_security(&SecurityCliInput {
            bearer: "off".to_string(),
            ..input()
        })
        .unwrap();
        assert!(s.bearer.is_none());
    }

    #[test]
    fn bearer_unsecured_builds_validator_with_principal_claim() {
        let s = build_security(&SecurityCliInput {
            bearer: "unsecured".to_string(),
            bearer_principal_claim: "preferred_username".to_string(),
            ..input()
        })
        .unwrap();
        let b = s.bearer.expect("Bearer enabled");
        match &*b.validator {
            OAuthBearerValidator::Unsecured(v) => {
                assert_eq!(v.principal_claim_name, "preferred_username");
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
        assert!(build_security(&bad).is_err());
    }

    // ---- tls -------------------------------------------------------------

    #[test]
    fn tls_cert_and_key_build_config_with_each_client_auth() {
        for (mode, expected) in [
            ("disabled", ClientAuthMode::Disabled),
            ("optional", ClientAuthMode::Optional),
            ("required", ClientAuthMode::Required),
        ] {
            let s = build_security(&SecurityCliInput {
                tls_cert: Some(PathBuf::from("/c.pem")),
                tls_key: Some(PathBuf::from("/k.pem")),
                tls_client_ca: Some(PathBuf::from("/ca.pem")),
                tls_client_auth: mode.to_string(),
                ..input()
            })
            .unwrap();
            let tls = s.tls.unwrap_or_else(|| panic!("TLS enabled for {mode}"));
            assert_eq!(tls.cert_chain_path, PathBuf::from("/c.pem"));
            assert_eq!(tls.private_key_path, PathBuf::from("/k.pem"));
            assert_eq!(tls.client_ca_path, Some(PathBuf::from("/ca.pem")));
            assert_eq!(tls.trust_roots_path, Some(PathBuf::from("/ca.pem")));
            assert_eq!(tls.client_auth, expected, "client_auth for {mode}");
        }
    }

    #[test]
    fn tls_cert_without_key_errors() {
        let bad = SecurityCliInput {
            tls_cert: Some(PathBuf::from("/c.pem")),
            tls_key: None,
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    #[test]
    fn tls_key_without_cert_errors() {
        let bad = SecurityCliInput {
            tls_cert: None,
            tls_key: Some(PathBuf::from("/k.pem")),
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    #[test]
    fn tls_bad_client_auth_value_errors() {
        let bad = SecurityCliInput {
            tls_cert: Some(PathBuf::from("/c.pem")),
            tls_key: Some(PathBuf::from("/k.pem")),
            tls_client_auth: "mutual".to_string(),
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    // ---- authz -----------------------------------------------------------

    #[test]
    fn authz_enabled_builds_config() {
        let s = build_security(&SecurityCliInput {
            authz: true,
            super_users: vec!["admin".to_string(), "root".to_string()],
            acl_refresh_secs: 45,
            ..input()
        })
        .unwrap();
        let a = s.authz.expect("authz enabled");
        assert!(a.enabled);
        assert!(a.super_users.contains("admin"));
        assert!(a.super_users.contains("root"));
        assert_eq!(a.acl_refresh, std::time::Duration::from_secs(45));
    }

    #[test]
    fn authz_default_refresh_secs() {
        let s = build_security(&SecurityCliInput {
            authz: true,
            ..input()
        })
        .unwrap();
        let a = s.authz.expect("authz enabled");
        assert_eq!(a.acl_refresh, std::time::Duration::from_secs(30));
        assert!(a.super_users.is_empty());
    }

    // ---- client (SR → broker) security -----------------------------------

    #[test]
    fn client_plaintext_is_none() {
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "PLAINTEXT".to_string(),
            ..input()
        })
        .unwrap();
        assert!(s.client.is_none());
    }

    #[test]
    fn client_protocol_is_case_insensitive() {
        // lower-case is upcased before matching, like the binary.
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "plaintext".to_string(),
            ..input()
        })
        .unwrap();
        assert!(s.client.is_none());
    }

    #[test]
    fn client_ssl_has_tls_no_sasl() {
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "SSL".to_string(),
            kafka_tls_ca: Some(PathBuf::from("/broker-ca.pem")),
            kafka_tls_server_name: Some("broker.internal".to_string()),
            ..input()
        })
        .unwrap();
        let c = s.client.expect("SSL ⇒ Some(ClientSecurity)");
        assert_eq!(c.protocol, ListenerProtocol::Ssl);
        let tls = c.tls.expect("SSL requires TLS");
        assert_eq!(tls.trust_roots_pem, Some(PathBuf::from("/broker-ca.pem")));
        assert_eq!(tls.server_name, "broker.internal");
        assert!(c.sasl.is_none(), "SSL is not a SASL protocol");
    }

    #[test]
    fn client_ssl_default_server_name_is_localhost() {
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "SSL".to_string(),
            ..input()
        })
        .unwrap();
        let tls = s.client.unwrap().tls.unwrap();
        assert_eq!(tls.server_name, "localhost");
        assert!(tls.trust_roots_pem.is_none());
    }

    #[test]
    fn client_sasl_plaintext_has_sasl_no_tls() {
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
            kafka_sasl_mechanism: "PLAIN".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        })
        .unwrap();
        let c = s.client.expect("SASL_PLAINTEXT ⇒ Some");
        assert_eq!(c.protocol, ListenerProtocol::SaslPlaintext);
        assert!(c.tls.is_none(), "SASL_PLAINTEXT carries no TLS");
        match c.sasl.expect("SASL configured") {
            SaslCredentials::Plain { username, password } => {
                assert_eq!(username, "u");
                assert_eq!(password, "p");
            }
            other => panic!("expected PLAIN, got {other:?}"),
        }
    }

    #[test]
    fn client_sasl_ssl_has_both() {
        let s = build_security(&SecurityCliInput {
            kafka_security_protocol: "SASL_SSL".to_string(),
            kafka_sasl_mechanism: "SCRAM-SHA-256".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        })
        .unwrap();
        let c = s.client.expect("SASL_SSL ⇒ Some");
        assert_eq!(c.protocol, ListenerProtocol::SaslSsl);
        assert!(c.tls.is_some(), "SASL_SSL requires TLS");
        assert!(matches!(c.sasl, Some(SaslCredentials::Scram { .. })));
    }

    #[test]
    fn client_sasl_mechanisms_map() {
        for (mech, want) in [
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
        ] {
            let s = build_security(&SecurityCliInput {
                kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
                kafka_sasl_mechanism: mech.to_string(),
                kafka_sasl_username: Some("u".to_string()),
                kafka_sasl_password: Some("p".to_string()),
                ..input()
            })
            .unwrap();
            match s.client.unwrap().sasl.unwrap() {
                SaslCredentials::Scram { mechanism, .. } => {
                    assert_eq!(mechanism, want, "mechanism for {mech}");
                }
                other => panic!("expected SCRAM for {mech}, got {other:?}"),
            }
        }
    }

    #[test]
    fn client_sasl_missing_username_errors() {
        let bad = SecurityCliInput {
            kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
            kafka_sasl_username: None,
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    #[test]
    fn client_sasl_missing_password_errors() {
        let bad = SecurityCliInput {
            kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: None,
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    #[test]
    fn client_bad_protocol_errors() {
        let bad = SecurityCliInput {
            kafka_security_protocol: "SASL_GSSAPI".to_string(),
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }

    #[test]
    fn client_bad_mechanism_errors() {
        let bad = SecurityCliInput {
            kafka_security_protocol: "SASL_PLAINTEXT".to_string(),
            kafka_sasl_mechanism: "GSSAPI".to_string(),
            kafka_sasl_username: Some("u".to_string()),
            kafka_sasl_password: Some("p".to_string()),
            ..input()
        };
        assert!(build_security(&bad).is_err());
    }
}
