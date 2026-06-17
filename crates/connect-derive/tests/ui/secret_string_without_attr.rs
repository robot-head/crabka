use crabka_connect::SecretString;
use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct SecretStringWithoutAttrConfig {
    password: SecretString,
}

fn main() {}
