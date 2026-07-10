use crabka_connect::{ConnectorConfig, SecretString};

#[derive(Debug, Clone, ConnectorConfig)]
pub struct PostgresSourceConfig {
    #[config(required, secret)]
    pub database_url: SecretString,
    #[config(required)]
    pub slot_name: String,
    #[config(default = "crabka_connect")]
    pub publication_name: String,
    #[config(default = "public")]
    pub schema: String,
    #[config(name = "tables")]
    pub table_names: Vec<String>,
    #[config(default = 1000)]
    pub max_messages_per_poll: u32,
}

#[cfg(test)]
mod tests {
    use crabka_connect::{ConfigKind, ConnectorConfig, EnvSecretResolver, ResolveOptions};
    use serde_json::json;

    use super::PostgresSourceConfig;

    #[tokio::test]
    async fn config_def_materializes_typed_postgres_config() {
        let def = PostgresSourceConfig::config_def();
        let keys = def
            .keys()
            .map(|key| (key.name.as_str(), key.kind, key.required))
            .collect::<Vec<_>>();

        assert_eq!(
            keys,
            vec![
                ("database_url", ConfigKind::Secret, true),
                ("max_messages_per_poll", ConfigKind::UnsignedInteger, false),
                ("publication_name", ConfigKind::String, false),
                ("schema", ConfigKind::String, false),
                ("slot_name", ConfigKind::String, true),
                ("tables", ConfigKind::StringList, true),
            ]
        );

        let raw = serde_json::Map::from_iter([
            (
                "database_url".to_string(),
                json!("postgres://localhost/app"),
            ),
            ("slot_name".to_string(), json!("crabka_slot")),
            ("tables".to_string(), json!(["accounts", "transactions"])),
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

        assert_eq!(
            config.database_url.expose_secret(),
            "postgres://localhost/app"
        );
        assert_eq!(config.slot_name.as_str(), "crabka_slot");
        assert_eq!(config.publication_name.as_str(), "crabka_connect");
        assert_eq!(config.schema.as_str(), "public");
        assert_eq!(
            config.table_names.as_slice(),
            ["accounts".to_string(), "transactions".to_string()].as_slice()
        );
        assert_eq!(config.max_messages_per_poll, 1000);
    }
}
