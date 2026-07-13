//! Component B integration test: a controller-only node + a broker-only
//! observer. The observer replicates metadata via fetch (not openraft), a
//! `CreateTopics` forwarded through it lands on the controller and
//! propagates back to the observer's image, and the observer never joins
//! the voter set.

use std::collections::BTreeSet;

use assert2::assert;
use crabka_broker::{BootstrapMode, Broker, config::NodeRole};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tempfile::TempDir;

mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_observes_and_forwards() {
    support::init_tracing();

    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(2).await;
    // Only the controller (node 1) is a voter. The broker-only node (node 2)
    // observes via fetch and must never appear in the quorum.
    let voters = vec![(1u64, controller_addrs[0])];

    // Unpack held listeners for each node up front, before any broker starts.
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (data0, controller0) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (data1, controller1) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());

    // Controller-only node (node 1): bootstrap singleton, elects itself.
    let ctrl_dir = TempDir::new().unwrap();
    let mut ctrl_cfg = support::broker_config(
        0,
        &client_addrs,
        &controller_addrs,
        &voters,
        ctrl_dir.path(),
        BootstrapMode::Bootstrap,
    );
    ctrl_cfg.roles = vec![NodeRole::Controller];
    let controller = Broker::start_with_listeners(ctrl_cfg, Some(controller0), Some(data0))
        .await
        .expect("controller-only start");

    // Wait until the controller is leader before starting the observer, so
    // the observer's first fetch already has a committed log to replicate.
    // (Node 1 is the only voter, so it elects itself.)
    controller.wait_until_controller_leader().await;

    // Broker-only node (node 2): no openraft voter. Keeps its metadata image
    // current by fetching `__cluster_metadata` from the controller, and
    // forwards writes to the controller quorum.
    let broker_dir = TempDir::new().unwrap();
    let mut broker_cfg = support::broker_config(
        1,
        &client_addrs,
        &controller_addrs,
        &voters,
        broker_dir.path(),
        BootstrapMode::Join,
    );
    broker_cfg.roles = vec![NodeRole::Broker];
    let broker_only = Broker::start_with_listeners(broker_cfg, Some(controller1), Some(data1))
        .await
        .expect("broker-only start");
    let broker_only_id = broker_only.node_id();

    // The broker-only node self-registers (it IS a broker) by forwarding the
    // registration to the controller; wait until the controller's committed
    // image reflects it, so CreateTopics has a broker to place replicas on.
    controller.wait_until_brokers_registered(1).await;

    // CreateTopics against the broker-only node — forwarded to the controller
    // quorum via the observer's write path.
    let topic = "rolesep-observed";
    let client = Client::builder()
        .bootstrap(broker_only.listen_addr().to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 0,
        "create via broker-only node forwards to the controller and succeeds"
    );

    // Assertion 1: the topic propagates back to the broker-only node's image
    // via observer fetch (it is not a voter, so this cannot be a raft apply).
    broker_only.wait_until_partition_present(topic, 0).await;

    // Assertion 2: the controller itself committed the forwarded topic.
    assert!(
        controller.has_partition(topic, 0),
        "controller committed the forwarded CreateTopics"
    );

    // Assertion 3: the broker-only node is NOT in the controller's voter set.
    let quorum_voters: BTreeSet<u64> = controller
        .quorum_voters_for_test()
        .into_iter()
        .map(|n| n.0)
        .collect();
    assert!(quorum_voters.contains(&1), "the controller is a voter");
    assert!(
        !quorum_voters.contains(&broker_only_id),
        "the broker-only node must never join the voter quorum"
    );

    broker_only.shutdown().await;
    controller.shutdown().await;
}
