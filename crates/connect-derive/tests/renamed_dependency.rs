use crabka_connect_derive::ConnectorConfig;
use renamed_connect::{ConnectorConfig as _, SecretString};

#[derive(ConnectorConfig)]
#[allow(dead_code)]
struct AutomaticallyRenamedConfig {
    database_url: String,
    #[config(secret)]
    password: SecretString,
}

#[derive(ConnectorConfig)]
#[allow(dead_code)]
#[config(crate = "renamed_connect")]
struct ExplicitlyRenamedConfig {
    optional: Option<String>,
}

#[test]
fn derive_works_when_crabka_connect_dependency_is_renamed() {
    let _ = AutomaticallyRenamedConfig::config_def();
    let _ = ExplicitlyRenamedConfig::config_def();
}
