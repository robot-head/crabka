use crabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct UnsupportedTypeConfig {
    endpoint: std::net::SocketAddr,
}

fn main() {}
