#![cfg(feature = "derive")]

use crabka_connect::{
    ConfigKind, ConnectorConfig, EnvSecretResolver, ResolveOptions, SecretString,
};
use serde_json::json;

#[derive(ConnectorConfig)]
struct PostgresSourceConfig {
    #[config(required)]
    database_url: String,
    #[config(secret)]
    password: SecretString,
    #[config(required, secret)]
    rotation_token: Option<SecretString>,
    #[config(default = "public")]
    schema: String,
    #[config(default = 500)]
    max_batch: u16,
    #[config(name = "topics")]
    topic_names: Vec<String>,
    omitted_note: Option<String>,
}

#[tokio::test]
async fn derive_builds_def_and_typed_config() {
    let def = PostgresSourceConfig::config_def();
    let keys = def
        .keys()
        .map(|key| (key.name.as_str(), key.kind, key.required))
        .collect::<Vec<_>>();

    for expected in [
        ("database_url", ConfigKind::String, true),
        ("password", ConfigKind::Secret, true),
        ("rotation_token", ConfigKind::Secret, true),
        ("schema", ConfigKind::String, false),
        ("max_batch", ConfigKind::UnsignedInteger, false),
        ("topics", ConfigKind::StringList, true),
        ("omitted_note", ConfigKind::String, false),
    ] {
        assert!(keys.contains(&expected), "missing key {expected:?}");
    }

    let raw = serde_json::Map::from_iter([
        (
            "database_url".to_string(),
            json!("postgres://localhost/app"),
        ),
        ("password".to_string(), json!("secret")),
        ("rotation_token".to_string(), json!("rotate")),
        ("topics".to_string(), json!(["a", "b"])),
    ]);
    let resolved = def
        .resolve_with_options(
            raw,
            &EnvSecretResolver,
            ResolveOptions {
                allow_literal_secrets: true,
            },
        )
        .await
        .unwrap();

    let config = PostgresSourceConfig::from_resolved(&resolved).unwrap();

    assert_eq!(config.database_url, "postgres://localhost/app");
    assert_eq!(config.password.expose_secret(), "secret");
    assert_eq!(
        config
            .rotation_token
            .as_ref()
            .map(SecretString::expose_secret),
        Some("rotate")
    );
    assert_eq!(config.schema, "public");
    assert_eq!(config.max_batch, 500);
    assert_eq!(config.topic_names, vec!["a", "b"]);
    assert_eq!(config.omitted_note, None);
}
