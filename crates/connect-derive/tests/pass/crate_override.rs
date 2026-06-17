extern crate crabka_connect as renamed_connect;

use crabka_connect_derive::ConnectorConfig;
use renamed_connect::{ConnectorConfig as _, SecretString};

#[derive(ConnectorConfig)]
#[config(crate = "renamed_connect")]
struct RenamedConfig {
    database_url: String,
    #[config(secret)]
    password: SecretString,
    optional: Option<String>,
}

fn main() {
    let _ = RenamedConfig::config_def();
}
