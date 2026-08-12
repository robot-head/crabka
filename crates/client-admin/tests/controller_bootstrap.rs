use std::collections::BTreeMap;

use assert2::check;
use crabka_broker::{Broker, BrokerConfig, NodeId};
use crabka_client_admin::{AdminClient, AdminError, CreateTopicSpec};

async fn start_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_addr = data_listener.local_addr().unwrap();
    let controller_addr = controller_listener.local_addr().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
    let broker =
        Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
            .await
            .unwrap();
    (broker, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_routes_supported_and_rejects_unsupported_admin_rpc() {
    let (broker, _dir) = start_broker().await;
    let broker_bootstrap = broker.listen_addr().to_string();
    let mut broker_admin = AdminClient::connect(std::slice::from_ref(&broker_bootstrap))
        .await
        .unwrap();
    let created = broker_admin
        .create_topics(
            &[CreateTopicSpec {
                name: "controller-admin".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(5),
        )
        .await
        .unwrap();
    check!(created[0].error.is_none());

    let controller_bootstrap = broker.controller_addr().to_string();
    let mut controller_admin =
        AdminClient::connect_controller(std::slice::from_ref(&controller_bootstrap))
            .await
            .unwrap();
    let configs = controller_admin
        .describe_configs(&["controller-admin"])
        .await
        .unwrap();

    check!(configs.len() == 1);
    check!(configs[0].topic == "controller-admin");

    let unsupported_reconciliation = controller_admin
        .reconcile_topic_replication_factor("controller-admin", 1, crabka_units::secs(5))
        .await;
    check!(matches!(
        unsupported_reconciliation,
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));

    let unsupported = controller_admin
        .create_topics(
            &[CreateTopicSpec {
                name: "not-supported-through-controller".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(5),
        )
        .await;
    check!(matches!(
        unsupported,
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));

    // Kafka's KIP-919 error 115 is a local AdminClient preflight failure. The
    // same controller connection therefore remains usable after rejection.
    let configs = controller_admin
        .describe_configs(&["controller-admin"])
        .await
        .unwrap();
    check!(configs.len() == 1);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_rejects_broker_endpoint() {
    let (broker, _dir) = start_broker().await;
    let result = AdminClient::connect_controller(&[broker.listen_addr().to_string()]).await;

    check!(matches!(
        result,
        Err(AdminError::Broker {
            api: "DescribeCluster",
            code: 114,
            name: "MISMATCHED_ENDPOINT_TYPE",
            ..
        })
    ));
    broker.shutdown().await;
}
