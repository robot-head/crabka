//! `Kafka.spec.logging` — operator-side surface for the broker's
//! `tracing` log filter. Strimzi-shaped `Logging`: `type: inline` carries a
//! `loggers` map (tracing target → level) that the operator composes into a
//! single `RUST_LOG` env-filter directive; `type: external` references a
//! user-managed `ConfigMap` key holding a raw filter string.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Logging {
    #[serde(default)]
    pub r#type: LoggingType,
    /// Inline loggers: tracing target → level. The key `root`
    /// (case-insensitive) sets the global default level (a bare env-filter
    /// directive); any other key is a tracing target (a Rust module path,
    /// e.g. `crabka_broker`). Levels are `trace|debug|info|warn|error|off`
    /// (case-insensitive; `fatal` is accepted as an alias for `error`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub loggers: BTreeMap<String, String>,
    /// External logging source. Required when `type: external`; the
    /// referenced `ConfigMap` key's value is used verbatim as the broker's
    /// `RUST_LOG` filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_from: Option<ExternalLoggingSource>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LoggingType {
    #[default]
    Inline,
    External,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalLoggingSource {
    pub config_map_key_ref: ConfigMapKeyRef,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeyRef {
    pub name: String,
    pub key: String,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn logging_defaults_type_inline() {
        let lg: Logging = serde_json::from_str("{}").unwrap();
        assert2::assert!(
            lg == Logging {
                r#type: LoggingType::Inline,
                loggers: BTreeMap::new(),
                value_from: None,
            }
        );
    }

    #[test]
    fn logging_inline_round_trips() {
        let lg = Logging {
            r#type: LoggingType::Inline,
            loggers: [
                ("root".to_string(), "info".to_string()),
                ("crabka_broker".to_string(), "debug".to_string()),
            ]
            .into(),
            value_from: None,
        };
        let j = serde_json::to_string(&lg).unwrap();
        for (_name, expected) in [
            ("loggers object", "\"loggers\""),
            ("broker level", "\"crabka_broker\":\"debug\""),
        ] {
            assert2::assert!(j.contains(expected));
        }
        let back: Logging = serde_json::from_str(&j).unwrap();
        assert2::assert!(back == lg);
    }

    #[test]
    fn logging_external_round_trips() {
        let json = r#"{"type":"external","valueFrom":{"configMapKeyRef":{"name":"my-log-cm","key":"rust.log"}}}"#;
        let lg: Logging = serde_json::from_str(json).unwrap();
        assert2::assert!(
            lg == Logging {
                r#type: LoggingType::External,
                loggers: BTreeMap::new(),
                value_from: Some(ExternalLoggingSource {
                    config_map_key_ref: ConfigMapKeyRef {
                        name: "my-log-cm".into(),
                        key: "rust.log".into(),
                    },
                }),
            }
        );
    }

    #[test]
    fn logging_type_rejects_unknown() {
        let err = serde_json::from_str::<LoggingType>("\"log4j\"").unwrap_err();
        assert2::assert!(err.to_string().contains("inline"));
    }
}
