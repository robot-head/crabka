//! Resolves catalog `ForeignServer`, `UserMapping`, and table OPTIONS into a
//! [`ConnProfile`]. The scanner and the source use that profile to connect to
//! Kafka.

use crabka_client_core::security::{ClientSecurity, SaslCredentials, TlsConnectorConfig};
use crabka_security::{ListenerProtocol, SaslMechanism};

use crate::{decode::Wire, error::KafkaFdwError};

/// A fully-resolved connection profile for one Kafka foreign table scan.
#[derive(Debug)]
pub struct ConnProfile {
    /// Bootstrap broker addresses, e.g. `["h1:9092", "h2:9092"]`.
    pub bootstrap: Vec<String>,
    /// Schema registry URL (empty string when not needed).
    pub registry_url: String,
    /// TLS/SASL security settings; `None` for plain PLAINTEXT with no auth.
    pub security: Option<ClientSecurity>,
    /// The Kafka topic to scan.
    pub topic: String,
    /// Wire encoding for the message value.
    pub value_format: Wire,
    /// Wire encoding for the message key.
    pub key_format: Wire,
}

/// Resolves a [`crabka_pgcatalog::ForeignServer`], an optional [`crabka_pgcatalog::UserMapping`],
/// and foreign-table OPTIONS into a [`ConnProfile`].
///
/// # Required options
/// * **Server option** `bootstrap` is a comma-separated `host:port` list. It
///   is required unless a caller-supplied default bootstrap is present.
/// * **Table option** `topic` is the Kafka topic name.
///
/// # Optional options
/// | Source  | Key                 | Default       |
/// |---------|---------------------|---------------|
/// | Server  | `registry_url`      | `""`          |
/// | Server  | `security_protocol` | `"PLAINTEXT"` |
/// | Mapping | `sasl_mechanism`    | —             |
/// | Mapping | `username`          | —             |
/// | Mapping | `password`          | —             |
/// | Table   | `value_format`      | `"raw"`       |
/// | Table   | `key_format`        | `"raw"`       |
///
/// This function silently ignores unknown option keys, which matches
/// `PostgreSQL` FDW leniency.
///
/// # Errors
/// Returns [`KafkaFdwError::Config`] when a required option is absent, or when
/// an option value is unrecognised, for example an unknown
/// `security_protocol`.
pub fn resolve(
    server: &crabka_pgcatalog::ForeignServer,
    mapping: Option<&crabka_pgcatalog::UserMapping>,
    table_options: &[(String, String)],
    default_bootstrap: Option<&str>,
) -> Result<ConnProfile, KafkaFdwError> {
    // ---- helpers ----
    let server_opt = |key: &str| -> Option<&str> {
        server
            .options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let table_opt = |key: &str| -> Option<&str> {
        table_options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };

    // ---- bootstrap (required unless caller supplied a default) ----
    let bootstrap = resolve_bootstrap(server_opt("bootstrap"), default_bootstrap)?;

    // ---- registry_url (optional, default empty) ----
    let registry_url = server_opt("registry_url").unwrap_or("").to_string();

    // ---- topic (required) ----
    let topic = table_opt("topic")
        .ok_or_else(|| KafkaFdwError::Config("missing required option: topic".to_string()))?
        .to_string();

    // ---- value_format / key_format (optional, default Raw) ----
    let value_format = parse_wire(table_opt("value_format").unwrap_or("raw"))?;
    let key_format = parse_wire(table_opt("key_format").unwrap_or("raw"))?;

    // ---- security_protocol (optional, default PLAINTEXT) ----
    let protocol_str = server_opt("security_protocol").unwrap_or("PLAINTEXT");
    let protocol = parse_listener_protocol(protocol_str)?;

    // ---- security ----
    let mapping_options: &[(String, String)] = mapping.map_or(&[], |m| m.options.as_slice());
    let security = build_security(protocol, &bootstrap, mapping_options)?;

    Ok(ConnProfile {
        bootstrap,
        registry_url,
        security,
        topic,
        value_format,
        key_format,
    })
}

/// Server-level connection info, resolved without a per-table `topic`.
///
/// `IMPORT FOREIGN SCHEMA` has no table OPTIONS to supply a `topic`, because
/// it *discovers* topics, so it resolves only the bootstrap, the registry,
/// and the security from the [`crabka_pgcatalog::ForeignServer`]
/// and the [`crabka_pgcatalog::UserMapping`].
#[derive(Debug)]
pub struct ServerProfile {
    /// Bootstrap broker addresses, e.g. `["h1:9092", "h2:9092"]`.
    pub bootstrap: Vec<String>,
    /// Schema registry URL (empty string when not configured).
    pub registry_url: String,
    /// TLS/SASL security settings; `None` for plain PLAINTEXT with no auth.
    pub security: Option<ClientSecurity>,
}

/// Resolves a [`crabka_pgcatalog::ForeignServer`], and an optional
/// [`crabka_pgcatalog::UserMapping`], into the connection-level
/// [`ServerProfile`] **without** a `topic`. The `IMPORT FOREIGN SCHEMA` path
/// uses it.
///
/// # Errors
/// Returns [`KafkaFdwError::Config`] when `bootstrap` is missing, the
/// `security_protocol` is unrecognised, or SASL credentials are required but
/// absent.
pub fn resolve_server(
    server: &crabka_pgcatalog::ForeignServer,
    mapping: Option<&crabka_pgcatalog::UserMapping>,
    default_bootstrap: Option<&str>,
) -> Result<ServerProfile, KafkaFdwError> {
    let server_opt = |key: &str| -> Option<&str> {
        server
            .options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };

    let bootstrap = resolve_bootstrap(server_opt("bootstrap"), default_bootstrap)?;

    let registry_url = server_opt("registry_url").unwrap_or("").to_string();

    let protocol_str = server_opt("security_protocol").unwrap_or("PLAINTEXT");
    let protocol = parse_listener_protocol(protocol_str)?;
    let mapping_options: &[(String, String)] = mapping.map_or(&[], |m| m.options.as_slice());
    let security = build_security(protocol, &bootstrap, mapping_options)?;

    Ok(ServerProfile {
        bootstrap,
        registry_url,
        security,
    })
}

fn resolve_bootstrap(
    explicit_bootstrap: Option<&str>,
    default_bootstrap: Option<&str>,
) -> Result<Vec<String>, KafkaFdwError> {
    let bootstrap_raw = explicit_bootstrap
        .or(default_bootstrap)
        .ok_or_else(|| KafkaFdwError::Config("missing required option: bootstrap".to_string()))?;
    Ok(bootstrap_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn parse_wire(s: &str) -> Result<Wire, KafkaFdwError> {
    match s {
        "raw" => Ok(Wire::Raw),
        "avro" => Ok(Wire::Avro),
        "json" => Ok(Wire::Json),
        "protobuf" => Ok(Wire::Protobuf),
        other => Err(KafkaFdwError::Config(format!(
            "unknown wire format: {other:?}; expected one of: raw, avro, json, protobuf"
        ))),
    }
}

fn parse_listener_protocol(s: &str) -> Result<ListenerProtocol, KafkaFdwError> {
    match s {
        "PLAINTEXT" => Ok(ListenerProtocol::Plaintext),
        "SSL" => Ok(ListenerProtocol::Ssl),
        "SASL_PLAINTEXT" => Ok(ListenerProtocol::SaslPlaintext),
        "SASL_SSL" => Ok(ListenerProtocol::SaslSsl),
        other => Err(KafkaFdwError::Config(format!(
            "unknown security_protocol: {other:?}; expected one of: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"
        ))),
    }
}

/// Builds [`ClientSecurity`] from the resolved protocol and mapping options.
///
/// `SaslCredentials` has **no** `ScramSha256` or `ScramSha512` variant. It
/// uses a single `Scram { mechanism, username, password }` variant, where
/// `mechanism` is [`crabka_security::SaslMechanism`]. Both `SCRAM-SHA-256`
/// and `SCRAM-SHA-512` map to this variant with the matching
/// `SaslMechanism::ScramSha256` or `SaslMechanism::ScramSha512`
/// discriminant.
fn build_security(
    protocol: ListenerProtocol,
    bootstrap: &[String],
    mapping_options: &[(String, String)],
) -> Result<Option<ClientSecurity>, KafkaFdwError> {
    let map_opt = |key: &str| -> Option<&str> {
        mapping_options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };

    // PLAINTEXT with no SASL → no security object needed.
    if protocol == ListenerProtocol::Plaintext {
        return Ok(None);
    }

    // TLS config: present for SSL and SASL_SSL.
    let tls = if protocol.requires_tls() {
        // SNI = first bootstrap host (strip port if present).
        let server_name = bootstrap
            .first()
            .map(|addr| {
                addr.rsplit_once(':')
                    .map_or(addr.as_str(), |(host, _)| host)
                    .to_string()
            })
            .unwrap_or_default();
        Some(TlsConnectorConfig {
            trust_roots_pem: None,
            server_name,
            client_identity: None,
        })
    } else {
        None
    };

    // SASL credentials: required when protocol requires SASL.
    let sasl = if protocol.requires_sasl() {
        let mechanism_str = map_opt("sasl_mechanism").ok_or_else(|| {
            KafkaFdwError::Config(
                "SASL protocol requires sasl_mechanism in the user mapping".to_string(),
            )
        })?;
        let username = map_opt("username")
            .ok_or_else(|| {
                KafkaFdwError::Config(
                    "SASL protocol requires username in the user mapping".to_string(),
                )
            })?
            .to_string();
        let password = map_opt("password")
            .ok_or_else(|| {
                KafkaFdwError::Config(
                    "SASL protocol requires password in the user mapping".to_string(),
                )
            })?
            .to_string();

        let creds = match mechanism_str {
            "PLAIN" => SaslCredentials::Plain { username, password },
            "SCRAM-SHA-256" => SaslCredentials::Scram {
                mechanism: SaslMechanism::ScramSha256,
                username,
                password,
            },
            "SCRAM-SHA-512" => SaslCredentials::Scram {
                mechanism: SaslMechanism::ScramSha512,
                username,
                password,
            },
            other => {
                return Err(KafkaFdwError::Config(format!(
                    "unknown sasl_mechanism: {other:?}; expected one of: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512"
                )));
            }
        };
        Some(creds)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_server(opts: &[(&str, &str)]) -> crabka_pgcatalog::ForeignServer {
        crabka_pgcatalog::ForeignServer {
            name: "s".into(),
            wrapper: "crabka_gres_fdw".into(),
            options: opts
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn make_mapping(opts: &[(&str, &str)]) -> crabka_pgcatalog::UserMapping {
        crabka_pgcatalog::UserMapping {
            user: "public".into(),
            server: "s".into(),
            options: opts
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn table_opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Verbatim test from the task brief.
    #[test]
    fn resolve_builds_scram_profile() {
        let server = crabka_pgcatalog::ForeignServer {
            name: "s".into(),
            wrapper: "crabka_gres_fdw".into(),
            options: vec![
                ("bootstrap".into(), "h:9092".into()),
                ("registry_url".into(), "http://r".into()),
                ("security_protocol".into(), "SASL_SSL".into()),
            ],
        };
        let mapping = crabka_pgcatalog::UserMapping {
            user: "public".into(),
            server: "s".into(),
            options: vec![
                ("sasl_mechanism".into(), "SCRAM-SHA-256".into()),
                ("username".into(), "u".into()),
                ("password".into(), "p".into()),
            ],
        };
        let p = crate::config::resolve(
            &server,
            Some(&mapping),
            &[
                ("topic".into(), "orders".into()),
                ("value_format".into(), "avro".into()),
            ],
            None,
        )
        .expect("resolve");
        assert_eq!(p.topic, "orders");
        assert_eq!(p.bootstrap, vec!["h:9092".to_string()]);
        assert!(matches!(p.value_format, crate::decode::Wire::Avro));
        assert!(p.security.is_some());
    }

    #[test]
    fn plaintext_no_mapping_yields_no_security() {
        let server = make_server(&[
            ("bootstrap", "broker:9092"),
            ("security_protocol", "PLAINTEXT"),
        ]);
        let p = resolve(&server, None, &table_opts(&[("topic", "events")]), None)
            .expect("resolve PLAINTEXT");
        assert!(p.security.is_none(), "PLAINTEXT should produce no security");
        assert_eq!(p.topic, "events");
        assert_eq!(p.bootstrap, vec!["broker:9092".to_string()]);
        assert!(matches!(p.value_format, Wire::Raw));
        assert!(matches!(p.key_format, Wire::Raw));
    }

    #[test]
    fn missing_topic_returns_config_error() {
        let server = make_server(&[("bootstrap", "broker:9092")]);
        let err = resolve(&server, None, &[], None).expect_err("missing topic should error");
        assert!(
            matches!(err, KafkaFdwError::Config(ref msg) if msg.contains("topic")),
            "expected Config error mentioning 'topic', got: {err}"
        );
    }

    #[test]
    fn missing_bootstrap_returns_config_error() {
        let server = make_server(&[]);
        let err = resolve(&server, None, &table_opts(&[("topic", "t")]), None)
            .expect_err("missing bootstrap should error");
        assert!(
            matches!(err, KafkaFdwError::Config(ref msg) if msg.contains("bootstrap")),
            "expected Config error mentioning 'bootstrap', got: {err}"
        );
    }

    #[test]
    fn sasl_plaintext_with_plain_credentials() {
        let server = make_server(&[
            ("bootstrap", "b:9093"),
            ("security_protocol", "SASL_PLAINTEXT"),
        ]);
        let mapping = make_mapping(&[
            ("sasl_mechanism", "PLAIN"),
            ("username", "alice"),
            ("password", "secret"),
        ]);
        let p = resolve(
            &server,
            Some(&mapping),
            &table_opts(&[("topic", "logins")]),
            None,
        )
        .expect("resolve SASL_PLAINTEXT/PLAIN");
        let sec = p
            .security
            .expect("security must be Some for SASL_PLAINTEXT");
        assert!(sec.tls.is_none(), "SASL_PLAINTEXT should have no TLS");
        assert!(
            matches!(sec.sasl, Some(SaslCredentials::Plain { .. })),
            "expected Plain credentials"
        );
    }

    #[test]
    fn ssl_no_sasl_builds_tls_only_security() {
        let server = make_server(&[
            ("bootstrap", "secure-host:9093"),
            ("security_protocol", "SSL"),
        ]);
        let p = resolve(
            &server,
            None,
            &table_opts(&[("topic", "secure-topic")]),
            None,
        )
        .expect("resolve SSL");
        let sec = p.security.expect("security must be Some for SSL");
        assert!(sec.tls.is_some(), "SSL should have TLS config");
        assert!(
            sec.sasl.is_none(),
            "SSL with no mapping should have no SASL"
        );
        let tls = sec
            .tls
            .expect("SSL security should have a TlsConnectorConfig");
        assert_eq!(
            tls.server_name, "secure-host",
            "SNI should be the first bootstrap host without port"
        );
    }

    #[test]
    fn default_formats_are_raw() {
        let server = make_server(&[("bootstrap", "b:9092")]);
        let p = resolve(&server, None, &table_opts(&[("topic", "t")]), None).expect("resolve");
        assert!(matches!(p.value_format, Wire::Raw));
        assert!(matches!(p.key_format, Wire::Raw));
    }

    #[test]
    fn json_wire_format_parses() {
        let server = make_server(&[("bootstrap", "b:9092")]);
        let p = resolve(
            &server,
            None,
            &table_opts(&[
                ("topic", "t"),
                ("value_format", "json"),
                ("key_format", "avro"),
            ]),
            None,
        )
        .expect("resolve json/avro");
        assert!(matches!(p.value_format, Wire::Json));
        assert!(matches!(p.key_format, Wire::Avro));
    }

    #[test]
    fn registry_url_defaults_to_empty_string() {
        let server = make_server(&[("bootstrap", "b:9092")]);
        let p = resolve(&server, None, &table_opts(&[("topic", "t")]), None).expect("resolve");
        assert_eq!(p.registry_url, "");
    }

    #[test]
    fn registry_url_is_forwarded() {
        let server = make_server(&[("bootstrap", "b:9092"), ("registry_url", "http://sr:8081")]);
        let p = resolve(&server, None, &table_opts(&[("topic", "t")]), None).expect("resolve");
        assert_eq!(p.registry_url, "http://sr:8081");
    }

    #[test]
    fn unknown_security_protocol_errors() {
        let server = make_server(&[("bootstrap", "b:9092"), ("security_protocol", "BOGUS")]);
        let err = resolve(&server, None, &table_opts(&[("topic", "t")]), None)
            .expect_err("unknown security_protocol should error");
        assert!(
            matches!(err, KafkaFdwError::Config(ref msg) if msg.contains("security_protocol")),
            "expected Config error mentioning 'security_protocol', got: {err}"
        );
    }

    #[test]
    fn comma_separated_bootstrap_is_split() {
        let server = make_server(&[("bootstrap", "h1:9092 , h2:9092, h3:9092")]);
        let p = resolve(&server, None, &table_opts(&[("topic", "t")]), None).expect("resolve");
        assert_eq!(
            p.bootstrap,
            vec![
                "h1:9092".to_string(),
                "h2:9092".to_string(),
                "h3:9092".to_string()
            ]
        );
    }

    #[test]
    fn missing_bootstrap_uses_default_bootstrap() {
        let server = make_server(&[]);
        let p = resolve(
            &server,
            None,
            &table_opts(&[("topic", "t")]),
            Some("default-a:9092, default-b:9092"),
        )
        .expect("resolve with default bootstrap");

        assert_eq!(
            p.bootstrap,
            vec!["default-a:9092".to_string(), "default-b:9092".to_string()]
        );
    }

    #[test]
    fn explicit_bootstrap_overrides_default_bootstrap() {
        let server = make_server(&[("bootstrap", "explicit:9092")]);
        let p = resolve(
            &server,
            None,
            &table_opts(&[("topic", "t")]),
            Some("default:9092"),
        )
        .expect("resolve with explicit bootstrap");

        assert_eq!(p.bootstrap, vec!["explicit:9092".to_string()]);
    }

    #[test]
    fn resolve_server_uses_default_bootstrap() {
        let server = make_server(&[]);
        let p = resolve_server(&server, None, Some("default:9092"))
            .expect("resolve server with default bootstrap");

        assert_eq!(p.bootstrap, vec!["default:9092".to_string()]);
    }
}
