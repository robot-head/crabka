use crabka_connect::SecretString;
use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct SecretDefaultConfig {
    #[config(secret, default = "password")]
    password: SecretString,
}

fn main() {}
