use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct Bad {
    #[config(name = "same")]
    first: String,
    #[config(name = "same")]
    second: String,
}

fn main() {}
