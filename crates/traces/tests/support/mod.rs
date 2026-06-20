//! Minimal in-process broker helper for traces integration tests.

#![allow(dead_code)]

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use tempfile::TempDir;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub bootstrap: String,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-traces-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        bootstrap,
        _tempdir: tempdir,
    }
}
