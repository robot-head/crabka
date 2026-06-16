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

    assert!(keys.contains(&("database_url", ConfigKind::String, true)));
    assert!(keys.contains(&("password", ConfigKind::Secret, true)));
    assert!(keys.contains(&("schema", ConfigKind::String, false)));
    assert!(keys.contains(&("max_batch", ConfigKind::UnsignedInteger, false)));
    assert!(keys.contains(&("topics", ConfigKind::StringList, false)));
    assert!(keys.contains(&("omitted_note", ConfigKind::String, false)));

    let raw = serde_json::Map::from_iter([
        (
            "database_url".to_string(),
            json!("postgres://localhost/app"),
        ),
        ("password".to_string(), json!("secret")),
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
    assert_eq!(config.schema, "public");
    assert_eq!(config.max_batch, 500);
    assert_eq!(config.topic_names, vec!["a", "b"]);
    assert_eq!(config.omitted_note, None);
}
