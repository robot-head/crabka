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
    #[serde(default)]
    pub broker_id: Option<i32>,
    #[serde(default)]
    pub log_dir: Option<String>,
    #[serde(default)]
    pub inter_broker_listener_name: Option<String>,
    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FileListener {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: FileListenerProtocol,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileListenerProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl From<FileListenerProtocol> for ListenerProtocol {
    fn from(v: FileListenerProtocol) -> Self {
        match v {
            FileListenerProtocol::Plaintext => ListenerProtocol::Plaintext,
            FileListenerProtocol::Ssl => ListenerProtocol::Ssl,
            FileListenerProtocol::SaslPlaintext => ListenerProtocol::SaslPlaintext,
            FileListenerProtocol::SaslSsl => ListenerProtocol::SaslSsl,
        }
    }
}

impl FileListener {
    #[must_use]
    pub fn into_spec(self) -> ListenerSpec {
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol.into(),
        }
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
protocol = "plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
        assert_eq!(cfg.log_dir.as_deref(), Some("/var/lib/crabka/data"));
        assert_eq!(cfg.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(cfg.listeners.len(), 2);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].protocol, FileListenerProtocol::Plaintext);
        assert_eq!(
            cfg.server_properties.get("log.retention.hours").map(String::as_str),
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
protocol = "sasl_ssl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.listeners[0].protocol, FileListenerProtocol::SaslSsl);
    }

    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "plaintext"
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
            protocol: FileListenerProtocol::Plaintext,
        };
        let spec = fl.into_spec();
        assert_eq!(spec.name, "X");
        assert_eq!(spec.advertised, "h:9094");
        assert_eq!(spec.protocol, ListenerProtocol::Plaintext);
    }
}
