//! KIP-853 dynamic-voters end-to-end integration tests.
//!
//! These tests exercise the *real* auto-join path. Broker 0 self-bootstraps as
//! the sole voter. Brokers 1 to n then boot in `Join` mode with
//! `auto_join = true` and grow the quorum by sending `AddRaftVoter(self)` to
//! the leader over the wire. The shrink test then calls `remove_voter` on the
//! leader and asserts that the committed voter set contracts.
//!
//! openraft's debug assertions race on the hosted Windows scheduler, so these
//! tests are gated off Windows, like the other multi-node suites.

use assert2::assert;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId};
use crabka_raft::reconfig::{ReconfigOutcome, RemoveVoter};
use tempfile::TempDir;

async fn start_dynamic_cluster(n: u64) -> Vec<(BrokerHandle, TempDir)> {
    let cluster_id = uuid::Uuid::from_u128(853);
    let mut cluster = Vec::new();
    let mut bootstrap_controller: Option<std::net::SocketAddr> = None;

    for id in 1..=n {
        let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_addr = data_listener.local_addr().unwrap();
        let controller_addr = controller_listener.local_addr().unwrap();
        let dir = TempDir::new().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.broker_id = i32::try_from(id).unwrap();
        config.node_id = NodeId(id);
        config.directory_id = uuid::Uuid::from_u128(u128::from(id));
        config.cluster_id = Some(cluster_id);
        config.listen_addr = data_addr;
        config.advertised_listener = data_addr.to_string();
        config.controller_listen_addr = controller_addr;
        config.controller_election_timeout = crabka_units::millis(200);
        config.auto_join_retry_backoff = crabka_units::millis(20);
        config.startup_leader_wait_timeout = crabka_units::secs(10);

        if let Some(bootstrap) = bootstrap_controller {
            config.bootstrap_mode = BootstrapMode::Join;
            config.controller_quorum_voters = vec![(NodeId(1), bootstrap.to_string())];
            config.bootstrap_servers = vec![bootstrap.to_string()];
            config.auto_join = true;
        } else {
            config.bootstrap_mode = BootstrapMode::Bootstrap;
            config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
        }

        let handle = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener)),
        )
        .await
        .expect("dynamic controller start timed out")
        .expect("dynamic controller start");
        if bootstrap_controller.is_none() {
            bootstrap_controller = Some(handle.controller_addr());
            let outcome = handle
                .finalize_kraft_version_for_test(1)
                .await
                .expect("activate kraft.version 1");
            assert!(matches!(outcome, ReconfigOutcome::Committed));
        }
        eprintln!(
            "started node {id}: voters={} kraft.version={} leader={:?}",
            handle.voter_count_for_test(),
            handle.kraft_version_for_test(),
            handle.controller_leader_id()
        );
        cluster.push((handle, dir));
    }
    cluster
}

/// Auto-join must grow a fresh cluster from one voter to three. Broker 0
/// bootstraps alone, and brokers 1 and 2 join over the wire.
///
/// `start_n_node` already waits for convergence. This test asserts again
/// against the leader's committed image, so that a convergence regression
/// fails here rather than through the harness's `Startup` error.
#[tokio::test]
async fn auto_join_grows_quorum_to_three() {
    let cluster = start_dynamic_cluster(3).await;

    let leader = cluster
        .iter()
        .map(|(handle, _)| handle)
        .find(|handle| {
            handle.controller_leader_id() == Some(crabka_broker::NodeId(handle.node_id()))
        })
        .expect("an elected controller leader");

    leader.wait_for_image(|img| img.voters().len() == 3).await;

    // Every node should eventually agree on the 3-voter set, not just the
    // leader.
    for (h, _) in &cluster {
        h.wait_for_image(|img| img.voters().len() == 3).await;
    }
}

/// After the cluster grows to three, a call to the leader's `remove_voter` for
/// one follower must shrink the committed voter set to two.
#[tokio::test]
async fn remove_voter_shrinks_quorum() {
    let cluster = start_dynamic_cluster(3).await;

    let leader = cluster
        .iter()
        .map(|(handle, _)| handle)
        .find(|handle| {
            handle.controller_leader_id() == Some(crabka_broker::NodeId(handle.node_id()))
        })
        .expect("an elected controller leader");
    let leader_id = leader.node_id();

    leader.wait_for_image(|img| img.voters().len() == 3).await;

    // Pick a follower (any voter that isn't the leader) and read its
    // directory id straight from the committed image — `remove_voter` keys on
    // (id, directory_id).
    let victim = leader
        .voter_ids_for_test()
        .into_iter()
        .find(|&id| id != crabka_broker::NodeId(leader_id))
        .expect("a follower voter to remove");
    let victim_dir = leader
        .voter_directory_id_for_test(victim)
        .expect("victim's directory id present in image");

    let outcome = leader
        .remove_voter_for_test(RemoveVoter {
            id: victim,
            directory_id: victim_dir,
        })
        .await
        .expect("remove_voter RPC");
    assert!(
        matches!(outcome, ReconfigOutcome::Committed),
        "remove_voter should commit on the leader, got {outcome:?}"
    );

    leader.wait_for_image(|img| img.voters().len() == 2).await;
    assert!(
        !leader.voter_ids_for_test().contains(&victim),
        "removed voter {victim} still present in committed voter set"
    );
}
