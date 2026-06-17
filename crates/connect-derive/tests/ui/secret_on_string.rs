use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct SecretOnStringConfig {
    #[config(secret)]
    password: String,
}

fn main() {}
