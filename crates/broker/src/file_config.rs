//! TOML file-config surface for the `crabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and merged
//! into [`crate::BrokerConfig`]. Slice 25a only consumes
//! `[[listeners]]`, `inter_broker_listener_name`, and (passively)
//! `[server_properties]`. Other top-level keys are reserved for
//! future slices and are accepted but ignored.

use std::net::SocketAddr;

use serde::Deserialize;

use crabka_security::ListenerProtocol;

use crate::config::ListenerSpec;

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — future slices add fields and old binaries should warn rather
/// than refuse to start.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileConfig {
    pub broker_id: Option<i32>,
    pub log_dir: Option<String>,
    /// Additional JBOD data directories (KIP-113). Maps to
    /// [`crate::BrokerConfig::extra_log_dirs`].
    #[serde(default)]
    pub extra_log_dirs: Vec<String>,
    pub inter_broker_listener_name: Option<String>,
    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Slice 30: controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[serde(default)]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// Slice 30: TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,

    /// Slice 49 / 49b: SASL/OAUTHBEARER validator tuning. Only relevant when a
    /// listener enables the `OAUTHBEARER` mechanism.
    #[serde(default)]
    pub oauthbearer: Option<FileOAuthBearerConfig>,
}

/// TOML shape of `[oauthbearer]`. Maps to
/// [`crabka_security::OAuthBearerValidator`]. Setting `jwks_endpoint_uri`
/// selects the signed-JWT validator (slice 49b); otherwise the unsecured-JWS
/// validator (slice 49, development only) is used.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileOAuthBearerConfig {
    /// Claim whose value becomes the principal name. Default `sub`.
    #[serde(default)]
    pub principal_claim_name: Option<String>,
    /// Claim carrying the token scope. Default `scope`.
    #[serde(default)]
    pub scope_claim_name: Option<String>,
    /// When set, the token scope must contain this value.
    #[serde(default)]
    pub required_scope: Option<String>,
    /// Clock-skew tolerance, in milliseconds, for `exp` / `iat` / `nbf`.
    /// Default 30000.
    #[serde(default)]
    pub allowable_clock_skew_ms: Option<i64>,

    /// Slice 49b: JWKS endpoint URL. When set, tokens are validated as signed
    /// JWTs (RS256 / ES256) against the keys fetched from this URL, and the
    /// broker spawns a background refresher. When unset, the unsecured-JWS
    /// (`alg:none`) development validator is used.
    #[serde(default)]
    pub jwks_endpoint_uri: Option<String>,
    /// Slice 49b: when set, the token `iss` claim must equal this. Signed
    /// validator only.
    #[serde(default)]
    pub valid_issuer_uri: Option<String>,
    /// Slice 49b: when set, the token `aud` claim must contain this. Signed
    /// validator only.
    #[serde(default)]
    pub expected_audience: Option<String>,
    /// Slice 49b: JWKS re-fetch interval, in milliseconds. Default 300000
    /// (5 minutes). Signed validator only.
    #[serde(default)]
    pub jwks_refresh_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    #[serde(default)]
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileListenerSaslConfig {
    #[serde(default, deserialize_with = "deserialize_sasl_mechanisms")]
    pub enabled_mechanisms: Vec<crabka_security::SaslMechanism>,
}

fn deserialize_sasl_mechanisms<'de, D>(
    deserializer: D,
) -> Result<Vec<crabka_security::SaslMechanism>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let names: Vec<String> = Vec::deserialize(deserializer)?;
    names
        .into_iter()
        .map(|s| {
            crabka_security::SaslMechanism::from_wire(&s)
                .ok_or_else(|| D::Error::custom(format!("unknown SASL mechanism: {s}")))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FileListener {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,
    #[serde(default)]
    pub sasl_config: Option<FileListenerSaslConfig>,
}

impl FileConfig {
    /// Apply this file-config to a `BrokerConfig` that already holds
    /// CLI-derived values. The file fills in unset values and provides
    /// `listeners` + `inter_broker_listener_name` wholesale when those
    /// are at their respective "empty" defaults.
    ///
    /// CLI values always win — the binary's `main()` constructs the
    /// `BrokerConfig` from CLI args first, then calls `apply_to`. The
    /// file never overrides what was explicitly set on the CLI.
    ///
    /// **Caller contract:** when `--config-file` is used, the caller
    /// must NOT pass `--listen-addr` or `--advertised-listener`. The
    /// binary entrypoint enforces this (see `bin/broker.rs`); this
    /// method just merges what it's given.
    pub fn apply_to(self, cfg: &mut crate::config::BrokerConfig) {
        let defaults = crate::config::BrokerConfig::default();
        if let Some(id) = self.broker_id
            && cfg.broker_id == defaults.broker_id
        {
            cfg.broker_id = id;
        }
        if let Some(ld) = self.log_dir
            && cfg.log_dir == defaults.log_dir
        {
            cfg.log_dir = std::path::PathBuf::from(ld);
        }
        if !self.extra_log_dirs.is_empty() && cfg.extra_log_dirs.is_empty() {
            cfg.extra_log_dirs = self
                .extra_log_dirs
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect();
        }
        if !self.listeners.is_empty() {
            cfg.listeners = self
                .listeners
                .into_iter()
                .map(FileListener::into_spec)
                .collect();
        }
        if let Some(name) = self.inter_broker_listener_name {
            cfg.inter_broker_listener_name = name;
        }
        // `[server_properties]` is intentionally ignored in slice 25a.
        if let Some(proto) = self.controller_listener_protocol
            && cfg.controller_listener_protocol == defaults.controller_listener_protocol
        {
            cfg.controller_listener_protocol = proto;
        }
        if let Some(tls) = self.tls_config
            && cfg.tls_config.is_none()
        {
            use crabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
            cfg.tls_config = Some(BrokerTlsConfig {
                cert_chain_path: tls.cert_path,
                private_key_path: tls.key_path,
                trust_roots_path: None,
                client_ca_path: tls.client_ca_path,
                client_auth: match tls.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            });
        }
        if let Some(oauth) = self.oauthbearer {
            if let Some(jwks_uri) = oauth.jwks_endpoint_uri {
                // Signed-JWT validation (slice 49b). The empty key handle is
                // populated by the refresher `Broker::start` spawns.
                let mut v = crabka_security::SignedJwsValidator::new(
                    crabka_security::JwksHandle::default(),
                );
                if let Some(name) = oauth.principal_claim_name {
                    v.principal_claim_name = name;
                }
                if let Some(name) = oauth.scope_claim_name {
                    v.scope_claim_name = name;
                }
                if oauth.required_scope.is_some() {
                    v.required_scope = oauth.required_scope;
                }
                if let Some(skew) = oauth.allowable_clock_skew_ms {
                    v.allowable_clock_skew_ms = skew;
                }
                v.valid_issuer = oauth.valid_issuer_uri;
                v.expected_audience = oauth.expected_audience;
                cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Signed(v);
                cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
                if let Some(ms) = oauth.jwks_refresh_interval_ms {
                    cfg.oauthbearer_jwks_refresh_interval = std::time::Duration::from_millis(ms);
                }
            } else {
                // Unsecured-JWS validation (slice 49, development only).
                let mut v = crabka_security::UnsecuredJwsValidator::default();
                if let Some(name) = oauth.principal_claim_name {
                    v.principal_claim_name = name;
                }
                if let Some(name) = oauth.scope_claim_name {
                    v.scope_claim_name = name;
                }
                if oauth.required_scope.is_some() {
                    v.required_scope = oauth.required_scope;
                }
                if let Some(skew) = oauth.allowable_clock_skew_ms {
                    v.allowable_clock_skew_ms = skew;
                }
                cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Unsecured(v);
            }
        }
    }
}

impl FileListener {
    #[must_use]
    pub fn into_spec(self) -> ListenerSpec {
        use crabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol,
            tls_config: self.tls_config.map(|t| BrokerTlsConfig {
                cert_chain_path: t.cert_path,
                private_key_path: t.key_path,
                trust_roots_path: None,
                client_ca_path: t.client_ca_path,
                client_auth: match t.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            }),
            sasl_mechanisms: self.sasl_config.map(|s| s.enabled_mechanisms),
        }
    }
}

#[cfg(test)]
mod listener_auth_tests {
    use super::*;

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.listeners.len(), 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        assert_eq!(
            data_tls.cert_path,
            std::path::PathBuf::from("/tls/broker.crt")
        );
        assert_eq!(
            data_tls.key_path,
            std::path::PathBuf::from("/tls/broker.key")
        );
        assert_eq!(
            data_tls.client_ca_path.as_deref(),
            Some(std::path::Path::new("/tls/clients-ca.crt"))
        );
        assert_eq!(data_tls.client_auth, FileClientAuthMode::Required);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Disabled" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert_eq!(
            sasl.enabled_mechanisms,
            vec![crabka_security::SaslMechanism::ScramSha512]
        );
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_toml_round_trips() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        assert_eq!(cfg, FileConfig::default());
    }

    #[test]
    fn full_toml_round_trips() {
        let src = r#"
broker_id = 0
log_dir = "/var/lib/crabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
        assert_eq!(cfg.log_dir.as_deref(), Some("/var/lib/crabka/data"));
        assert_eq!(cfg.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(cfg.listeners.len(), 2);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].protocol, ListenerProtocol::Plaintext);
        assert_eq!(
            cfg.server_properties
                .get("log.retention.hours")
                .map(String::as_str),
            Some("24")
        );
    }

    #[test]
    fn unknown_top_level_key_is_ignored() {
        // Forward-compat: a newer config file shouldn't break older brokers.
        let src = r#"
broker_id = 0
some_future_field = "from-a-later-slice"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
    }

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "SaslSsl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.listeners[0].protocol, ListenerProtocol::SaslSsl);
    }

    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "Plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_config: None,
        };
        let spec = fl.into_spec();
        assert_eq!(spec.name, "X");
        assert_eq!(spec.advertised, "h:9094");
        assert_eq!(spec.protocol, ListenerProtocol::Plaintext);
    }

    #[test]
    fn apply_to_populates_listeners() {
        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg);

        assert_eq!(cfg.listeners.len(), 1);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].advertised, "demo-0:9092");
        assert_eq!(cfg.inter_broker_listener_name, "PLAIN");
    }

    #[test]
    fn apply_to_does_not_clobber_non_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        // simulate CLI --broker-id 7 already applied
        let mut cfg = BrokerConfig {
            broker_id: 7,
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg);

        // CLI value wins because it differs from default.
        assert_eq!(cfg.broker_id, 7);
    }

    #[test]
    fn apply_to_fills_in_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default(); // broker_id == default (1)

        file.apply_to(&mut cfg);

        assert_eq!(cfg.broker_id, 42);
    }

    #[test]
    fn tls_keys_round_trip() {
        let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/crabka/broker-tls/0.crt"
key_path  = "/etc/crabka/broker-tls/0.key"
client_ca_path = "/etc/crabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
        assert_eq!(
            cfg.controller_listener_protocol,
            Some(ListenerProtocol::Ssl)
        );
        let tls = cfg.tls_config.expect("tls_config present");
        assert_eq!(
            tls.cert_path,
            std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt")
        );
        assert_eq!(tls.client_auth, FileClientAuthMode::Required);
    }

    #[test]
    fn tls_keys_absent_round_trips() {
        let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
        assert_eq!(cfg.controller_listener_protocol, None);
        assert!(cfg.tls_config.is_none());
    }

    #[test]
    fn apply_to_propagates_tls_config() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg);
        assert_eq!(
            cfg.controller_listener_protocol,
            crabka_security::ListenerProtocol::Ssl
        );
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert_eq!(tls.cert_chain_path, std::path::PathBuf::from("/c"));
    }

    #[test]
    fn apply_to_oauthbearer_jwks_selects_signed_validator() {
        let src = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
valid_issuer_uri = "https://idp.example"
expected_audience = "kafka"
principal_claim_name = "client_id"
jwks_refresh_interval_ms = 60000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg);
        assert_eq!(
            cfg.oauthbearer_jwks_endpoint.as_deref(),
            Some("https://idp.example/jwks")
        );
        assert_eq!(
            cfg.oauthbearer_jwks_refresh_interval,
            std::time::Duration::from_mins(1)
        );
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Signed(v) => {
                assert_eq!(v.valid_issuer.as_deref(), Some("https://idp.example"));
                assert_eq!(v.expected_audience.as_deref(), Some("kafka"));
                assert_eq!(v.principal_claim_name, "client_id");
            }
            crabka_security::OAuthBearerValidator::Unsecured(_) => {
                panic!("jwks_endpoint_uri must select the Signed validator")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_jwks_stays_unsecured() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg);
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert_eq!(v.allowable_clock_skew_ms, 5000);
            }
            crabka_security::OAuthBearerValidator::Signed(_) => {
                panic!("no jwks_endpoint_uri must keep the unsecured validator")
            }
        }
    }

    #[test]
    fn apply_to_empty_listeners_does_not_clear_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = BrokerConfig {
            listeners: vec![crate::config::ListenerSpec {
                name: "X".into(),
                bind_addr: "0.0.0.0:9094".parse().unwrap(),
                advertised: "h:9094".into(),
                protocol: crabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
            }],
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg);

        assert_eq!(cfg.listeners.len(), 1);
        assert_eq!(cfg.listeners[0].name, "X");
    }
}
