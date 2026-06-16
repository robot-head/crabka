use crabka_connect::{
    ConfigDef, ConnectorConfig, EnvSecretResolver, FromResolvedValue, ResolveOptions,
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

    assert_eq!(config.database_url, "postgres://localhost/app");
    assert_eq!(config.password.expose_secret(), "secret");
    assert!(config.enabled);
    assert_eq!(config.signed_limit, -12);
    assert_eq!(config.max_batch, 500);
    assert_eq!(config.ratio, 0.75);
    assert_eq!(config.topics, vec!["alpha".to_string(), "beta".to_string()]);
    assert_eq!(config.metadata, json!({"mode": "snapshot"}));
}
