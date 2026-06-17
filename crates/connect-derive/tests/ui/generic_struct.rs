use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct Bad<T> {
    value: T,
}

fn main() {}
