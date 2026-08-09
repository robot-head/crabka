//! `Kafka.spec.logging`, the operator-side surface for the broker's
//! `tracing` log filter.
//!
//! This is a Strimzi-shaped `Logging`. With `type: inline`, the field carries
//! a `loggers` map from tracing target to level. The operator composes that
//! map into a single `RUST_LOG` env-filter directive. With `type: external`,
//! the field references a user-managed `ConfigMap` key that holds a raw
//! filter string.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Logging {
    #[serde(default)]
    pub r#type: LoggingType,
    /// Inline loggers, from tracing target to level. The key `root` is
    /// case-insensitive and sets the global default level as a bare
    /// env-filter directive. Any other key is a tracing target, that is, a
    /// Rust module path such as `crabka_broker`. The levels are
    /// `trace|debug|info|warn|error|off`, and they are case-insensitive.
    /// `fatal` is accepted as an alias for `error`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub loggers: BTreeMap<String, String>,
    /// External logging source. This field is required when
    /// `type: external`. The operator uses the value of the referenced
    /// `ConfigMap` key verbatim as the broker's `RUST_LOG` filter.
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
    use assert2::assert;

    use super::*;

    #[test]
    fn logging_defaults_type_inline() {
        let lg: Logging = serde_json::from_str("{}").unwrap();
        assert!(
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
        assert!(j.contains("\"loggers\""), "got: {j}");
        assert!(j.contains("\"crabka_broker\":\"debug\""), "got: {j}");
        let back: Logging = serde_json::from_str(&j).unwrap();
        assert!(back == lg);
    }

    #[test]
    fn logging_external_round_trips() {
        let json = r#"{"type":"external","valueFrom":{"configMapKeyRef":{"name":"my-log-cm","key":"rust.log"}}}"#;
        let lg: Logging = serde_json::from_str(json).unwrap();
        assert!(lg.r#type == LoggingType::External);
        let src = lg.value_from.expect("value_from present");
        assert!(src.config_map_key_ref.name == "my-log-cm");
        assert!(src.config_map_key_ref.key == "rust.log");
    }

    #[test]
    fn logging_type_rejects_unknown() {
        let err = serde_json::from_str::<LoggingType>("\"log4j\"").unwrap_err();
        assert!(err.to_string().contains("inline"), "got: {err}");
    }
}
