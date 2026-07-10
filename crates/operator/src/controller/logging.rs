//! Logging resolution — compose the broker's `RUST_LOG` env-filter
//! from `Kafka.spec.logging`.
//!
//! `inline` loggers are composed by a pure, deterministic function
//! ([`compose_inline_filter`]); `external` references are read from a
//! user-managed `ConfigMap` at reconcile time. The resolved filter is
//! rendered into the broker `ConfigMap` (`rust.log` key) by
//! [`crate::controller::common::render_configmap`] and folded into the
//! config hash so a change rolls the cluster.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;

use crate::{
    context::Context,
    controller::common::{ReconcileError, condition},
    crd::{Kafka, KafkaCondition, LoggingType},
};

/// Canonicalize a level string to a `tracing` env-filter level. Accepts the
/// `tracing` set (`trace|debug|info|warn|error|off`) case-insensitively, plus
/// the log4j-friendly aliases `warning` → `warn`, `fatal` → `error`, and
/// `none` → `off`. Returns `None` for anything else.
fn normalize_level(level: &str) -> Option<&'static str> {
    match level.trim().to_ascii_lowercase().as_str() {
        "trace" => Some("trace"),
        "debug" => Some("debug"),
        "info" => Some("info"),
        "warn" | "warning" => Some("warn"),
        "error" | "fatal" => Some("error"),
        "off" | "none" => Some("off"),
        _ => None,
    }
}

/// Logging resolution failures. Each maps to a `LoggingReady=False` reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoggingError {
    /// `type: inline` with an empty `loggers` map.
    EmptyLoggers,
    /// A logger key was blank after trimming.
    EmptyLoggerName,
    /// A level value was not a recognized `tracing` level.
    InvalidLevel { logger: String, level: String },
    /// `type: external` without a `valueFrom.configMapKeyRef`.
    ExternalMissingRef,
    /// The referenced `ConfigMap` does not exist.
    ExternalConfigMapNotFound { name: String },
    /// The referenced `ConfigMap` exists but the key is missing or blank.
    ExternalKeyNotFound { config_map: String, key: String },
}

impl LoggingError {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            LoggingError::EmptyLoggers => "EmptyLoggers",
            LoggingError::EmptyLoggerName => "EmptyLoggerName",
            LoggingError::InvalidLevel { .. } => "InvalidLogLevel",
            LoggingError::ExternalMissingRef => "ExternalRefMissing",
            LoggingError::ExternalConfigMapNotFound { .. } => "LoggingConfigMapNotFound",
            LoggingError::ExternalKeyNotFound { .. } => "LoggingConfigMapKeyNotFound",
        }
    }

    pub(crate) fn message(&self) -> String {
        match self {
            LoggingError::EmptyLoggers => {
                "logging.type=inline requires a non-empty loggers map".into()
            }
            LoggingError::EmptyLoggerName => "logging.loggers contains a blank key".into(),
            LoggingError::InvalidLevel { logger, level } => format!(
                "logger '{logger}' has invalid level '{level}' (want trace|debug|info|warn|error|off)"
            ),
            LoggingError::ExternalMissingRef => {
                "logging.type=external requires valueFrom.configMapKeyRef".into()
            }
            LoggingError::ExternalConfigMapNotFound { name } => {
                format!("logging ConfigMap '{name}' not found")
            }
            LoggingError::ExternalKeyNotFound { config_map, key } => {
                format!("logging ConfigMap '{config_map}' has no non-empty key '{key}'")
            }
        }
    }
}

/// Compose an env-filter directive string from an inline `loggers` map.
///
/// Pure + deterministic: directives are sorted so the resulting string (and
/// therefore the config hash) is stable across reconciles regardless of map
/// iteration order. The key `root` (case-insensitive) yields a bare level
/// token (the env-filter global default); every other key yields
/// `target=level`.
///
/// # Errors
///
/// Returns [`LoggingError::EmptyLoggers`] for an empty map,
/// [`LoggingError::EmptyLoggerName`] for a blank key, and
/// [`LoggingError::InvalidLevel`] for an unrecognized level.
pub fn compose_inline_filter(loggers: &BTreeMap<String, String>) -> Result<String, LoggingError> {
    if loggers.is_empty() {
        return Err(LoggingError::EmptyLoggers);
    }
    let mut directives: Vec<String> = Vec::with_capacity(loggers.len());
    for (logger, level) in loggers {
        let logger = logger.trim();
        if logger.is_empty() {
            return Err(LoggingError::EmptyLoggerName);
        }
        let lvl = normalize_level(level).ok_or_else(|| LoggingError::InvalidLevel {
            logger: logger.to_string(),
            level: level.clone(),
        })?;
        if logger.eq_ignore_ascii_case("root") {
            directives.push(lvl.to_string());
        } else {
            directives.push(format!("{logger}={lvl}"));
        }
    }
    directives.sort();
    Ok(directives.join(","))
}

/// Outcome of resolving `Kafka.spec.logging`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoggingOutcome {
    /// `spec.logging` is unset.
    Disabled,
    /// A `RUST_LOG` filter string was successfully composed/read.
    Resolved(String),
    /// `spec.logging` is set but could not be resolved (user error).
    Invalid(LoggingError),
}

impl LoggingOutcome {
    /// The resolved filter string, if any. `None` for `Disabled`/`Invalid`.
    #[must_use]
    pub fn filter(&self) -> Option<&str> {
        match self {
            LoggingOutcome::Resolved(f) => Some(f.as_str()),
            _ => None,
        }
    }
}

/// Resolve `Kafka.spec.logging` to a `RUST_LOG` filter. `inline` is composed
/// in-process; `external` issues a single `ConfigMap` GET. A transient API
/// error during that GET propagates as `Err` (the reconcile requeues); a
/// missing ConfigMap/key surfaces as `Ok(Invalid(..))` (a user error the
/// operator reports without retry-spinning).
pub async fn resolve_logging(
    ctx: &Context,
    owner: &Kafka,
    namespace: &str,
) -> Result<LoggingOutcome, ReconcileError> {
    let Some(logging) = owner.spec.logging.as_ref() else {
        return Ok(LoggingOutcome::Disabled);
    };
    match logging.r#type {
        LoggingType::Inline => Ok(match compose_inline_filter(&logging.loggers) {
            Ok(f) => LoggingOutcome::Resolved(f),
            Err(e) => LoggingOutcome::Invalid(e),
        }),
        LoggingType::External => {
            let Some(src) = logging.value_from.as_ref() else {
                return Ok(LoggingOutcome::Invalid(LoggingError::ExternalMissingRef));
            };
            let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
            let Some(cm) = cm_api.get_opt(&src.config_map_key_ref.name).await? else {
                return Ok(LoggingOutcome::Invalid(
                    LoggingError::ExternalConfigMapNotFound {
                        name: src.config_map_key_ref.name.clone(),
                    },
                ));
            };
            let value = cm
                .data
                .as_ref()
                .and_then(|d| d.get(&src.config_map_key_ref.key))
                .map(|v| v.trim())
                .filter(|v| !v.is_empty());
            Ok(match value {
                Some(v) => LoggingOutcome::Resolved(v.to_string()),
                None => LoggingOutcome::Invalid(LoggingError::ExternalKeyNotFound {
                    config_map: src.config_map_key_ref.name.clone(),
                    key: src.config_map_key_ref.key.clone(),
                }),
            })
        }
    }
}

/// Map a [`LoggingOutcome`] to the cluster's `LoggingReady` condition.
/// Mirrors the `MetricsReady` shape: `Disabled` surfaces a
/// `False`/`Disabled` condition rather than omitting it.
#[must_use]
pub fn condition_for(outcome: &LoggingOutcome) -> KafkaCondition {
    match outcome {
        LoggingOutcome::Disabled => condition(
            "LoggingReady",
            "False",
            "Disabled",
            "spec.logging is not set",
        ),
        LoggingOutcome::Resolved(filter) => condition(
            "LoggingReady",
            "True",
            "Available",
            &format!("RUST_LOG filter resolved: {filter}"),
        ),
        LoggingOutcome::Invalid(e) => condition("LoggingReady", "False", e.reason(), &e.message()),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn loggers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn compose_inline_filter_success_cases() {
        for (name, input, expected) in [
            ("root is a bare level", &[("root", "info")][..], "info"),
            (
                "target includes its level",
                &[("crabka_broker", "debug")][..],
                "crabka_broker=debug",
            ),
            (
                "directives are sorted",
                &[
                    ("root", "info"),
                    ("crabka_raft", "warn"),
                    ("crabka_broker", "debug"),
                ][..],
                "crabka_broker=debug,crabka_raft=warn,info",
            ),
            (
                "levels are canonicalized",
                &[
                    ("root", "INFO"),
                    ("crabka_broker", "WARNING"),
                    ("crabka_log", "FATAL"),
                    ("crabka_raft", "OFF"),
                ][..],
                "crabka_broker=warn,crabka_log=error,crabka_raft=off,info",
            ),
            (
                "root is case insensitive",
                &[("ROOT", "debug")][..],
                "debug",
            ),
        ] {
            assert_eq!(
                compose_inline_filter(&loggers(input)),
                Ok(expected.to_string()),
                "case {name}"
            );
        }
    }

    #[test]
    fn compose_inline_filter_error_cases() {
        for (name, input, expected, expected_reason) in [
            (
                "empty logger map",
                &[][..],
                LoggingError::EmptyLoggers,
                "EmptyLoggers",
            ),
            (
                "blank logger name",
                &[("  ", "info")][..],
                LoggingError::EmptyLoggerName,
                "EmptyLoggerName",
            ),
            (
                "invalid level",
                &[("root", "verbose")][..],
                LoggingError::InvalidLevel {
                    logger: "root".into(),
                    level: "verbose".into(),
                },
                "InvalidLogLevel",
            ),
        ] {
            let actual = compose_inline_filter(&loggers(input)).unwrap_err();
            let actual_reason = actual.reason();
            assert_eq!(actual, expected, "case {name}");
            assert_eq!(actual_reason, expected_reason, "case {name}");
        }
    }

    #[test]
    fn outcome_filter_accessor() {
        for (outcome, want) in [
            (LoggingOutcome::Resolved("info".into()), Some("info")),
            (LoggingOutcome::Disabled, None),
            (LoggingOutcome::Invalid(LoggingError::EmptyLoggers), None),
        ] {
            assert!(outcome.filter() == want, "case {outcome:?}");
        }
    }

    #[test]
    fn condition_cases() {
        for (name, outcome, expected, message_fragment) in [
            (
                "disabled",
                LoggingOutcome::Disabled,
                ("LoggingReady", "False", "Disabled"),
                None,
            ),
            (
                "resolved",
                LoggingOutcome::Resolved("crabka_broker=debug,info".into()),
                ("LoggingReady", "True", "Available"),
                Some("crabka_broker=debug,info"),
            ),
            (
                "missing external ConfigMap",
                LoggingOutcome::Invalid(LoggingError::ExternalConfigMapNotFound {
                    name: "missing-cm".into(),
                }),
                ("LoggingReady", "False", "LoggingConfigMapNotFound"),
                Some("missing-cm"),
            ),
        ] {
            let c = condition_for(&outcome);
            check!(
                (c.type_.as_str(), c.status.as_str(), c.reason.as_str()) == expected,
                "case {name}"
            );
            if let Some(fragment) = message_fragment {
                check!(c.message.contains(fragment), "case {name}");
            }
        }
    }
}
