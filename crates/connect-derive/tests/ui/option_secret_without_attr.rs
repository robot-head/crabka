use crabka_connect::SecretString;
use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct OptionSecretWithoutAttrConfig {
    password: Option<SecretString>,
}

fn main() {}
