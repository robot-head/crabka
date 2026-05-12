//! Spin up an in-process `crabka-broker` and a `crabka-client-core`
//! `Client` pointed at it.

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use tempfile::TempDir;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-broker-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}
