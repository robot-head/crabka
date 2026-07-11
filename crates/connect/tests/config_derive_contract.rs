use std::time::Duration;

use crabka_connect::{
    ConfigDef, ConfigError, ConnectorConfig, EnvSecretResolver, FromResolvedValue, ResolveOptions,
    ResolvedConfig, SecretString,
};
use serde_json::{Value, json};

#[derive(Debug)]
struct ManualConfig {
    database_url: String,
    password: SecretString,
    enabled: bool,
    signed_limit: i16,
    max_batch: u16,
    ratio: f32,
    topics: Vec<String>,
    metadata: Value,
    optional_label: Option<String>,
    poll_interval: Duration,
}

impl ConnectorConfig for ManualConfig {
    fn config_def() -> ConfigDef {
        ConfigDef::new("manual")
            .required("database_url", String::KIND)
            .secret("password")
            .default("enabled", bool::KIND, json!(true))
            .required("signed_limit", i16::KIND)
            .default("max_batch", u16::KIND, json!(500))
            .default("ratio", f32::KIND, json!(0.75))
            .default("topics", Vec::<String>::KIND, json!(["alpha", "beta"]))
            .default("metadata", Value::KIND, json!({"mode": "snapshot"}))
            .optional("optional_label", Option::<String>::KIND)
            .default("poll_interval", Duration::KIND, json!(1500))
    }

    fn from_resolved(config: &ResolvedConfig) -> crabka_connect::ConfigResult<Self> {
        Ok(Self {
            database_url: String::from_resolved_value(config, "database_url")?,
            password: SecretString::from_resolved_value(config, "password")?,
            enabled: bool::from_resolved_value(config, "enabled")?,
            signed_limit: i16::from_resolved_value(config, "signed_limit")?,
            max_batch: u16::from_resolved_value(config, "max_batch")?,
            ratio: f32::from_resolved_value(config, "ratio")?,
            topics: Vec::<String>::from_resolved_value(config, "topics")?,
            metadata: Value::from_resolved_value(config, "metadata")?,
            optional_label: Option::<String>::from_resolved_value(config, "optional_label")?,
            poll_interval: Duration::from_resolved_value(config, "poll_interval")?,
        })
    }
}

#[tokio::test]
async fn manual_connector_config_contract_builds_typed_config() {
    let raw = serde_json::Map::from_iter([
        (
            "database_url".to_string(),
            json!("postgres://localhost/app"),
        ),
        ("password".to_string(), json!("secret")),
        ("signed_limit".to_string(), json!(-12)),
    ]);

    let resolved = ManualConfig::config_def()
        .resolve_with_options(
            raw,
            &EnvSecretResolver,
            ResolveOptions {
                allow_literal_secrets: true,
            },
        )
        .await
        .unwrap();
    let config = ManualConfig::from_resolved(&resolved).unwrap();

    assert2::assert!(config.database_url.as_str() == "postgres://localhost/app");
    assert2::assert!(config.password.expose_secret() == "secret");
    assert2::assert!(config.enabled);
    assert2::assert!(config.signed_limit == -12);
    assert2::assert!(config.max_batch == 500);
    assert2::assert!(config.topics == vec!["alpha".to_string(), "beta".to_string()]);
    assert2::assert!(config.metadata == json!({"mode": "snapshot"}));
    assert2::assert!(config.optional_label == None);
    assert2::assert!(config.poll_interval == Duration::from_millis(1500));
    assert2::assert!((config.ratio - 0.75).abs() < f32::EPSILON);
}

#[tokio::test]
async fn optional_config_field_reads_some_when_present() {
    let raw = serde_json::Map::from_iter([
        (
            "database_url".to_string(),
            json!("postgres://localhost/app"),
        ),
        ("password".to_string(), json!("secret")),
        ("signed_limit".to_string(), json!(-12)),
        ("optional_label".to_string(), json!("snapshot")),
    ]);

    let resolved = ManualConfig::config_def()
        .resolve_with_options(
            raw,
            &EnvSecretResolver,
            ResolveOptions {
                allow_literal_secrets: true,
            },
        )
        .await
        .unwrap();
    let config = ManualConfig::from_resolved(&resolved).unwrap();

    assert2::assert!(config.optional_label == Some("snapshot".to_string()));
}

#[tokio::test]
async fn unsigned_fields_reject_negative_values() {
    let def = ConfigDef::new("manual").required("max_batch", u16::KIND);
    let raw = serde_json::Map::from_iter([("max_batch".to_string(), json!(-1))]);

    let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

    assert2::assert!(
        matches!(err, ConfigError::WrongType { key, expected: "unsigned integer" } if key == "max_batch")
    );
}

#[tokio::test]
async fn narrow_unsigned_fields_report_range_errors() {
    let def = ConfigDef::new("manual").required("max_batch", u16::KIND);
    let raw =
        serde_json::Map::from_iter([("max_batch".to_string(), json!(u64::from(u16::MAX) + 1))]);
    let resolved = def.resolve(raw, &EnvSecretResolver).await.unwrap();

    let err = u16::from_resolved_value(&resolved, "max_batch").unwrap_err();

    assert2::assert!(
        matches!(err, ConfigError::WrongType { key, expected: "unsigned integer in range for u16" } if key == "max_batch")
    );
}

#[tokio::test]
async fn f32_fields_reject_values_outside_f32_range() {
    let def = ConfigDef::new("manual").required("ratio", f32::KIND);
    let raw = serde_json::Map::from_iter([("ratio".to_string(), json!(f64::from(f32::MAX) * 2.0))]);
    let resolved = def.resolve(raw, &EnvSecretResolver).await.unwrap();

    let err = f32::from_resolved_value(&resolved, "ratio").unwrap_err();

    assert2::assert!(
        matches!(err, ConfigError::WrongType { key, expected: "float in range for f32" } if key == "ratio")
    );
}

#[tokio::test]
async fn duration_fields_reject_negative_milliseconds() {
    let def = ConfigDef::new("manual").required("poll_interval", Duration::KIND);
    let raw = serde_json::Map::from_iter([("poll_interval".to_string(), json!(-1))]);

    let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

    assert2::assert!(
        matches!(err, ConfigError::WrongType { key, expected: "duration milliseconds" } if key == "poll_interval")
    );
}
