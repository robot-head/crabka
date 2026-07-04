//! KIP-853 dynamic-voters end-to-end integration tests.
//!
//! These exercise the *real* auto-join path: broker 0 self-bootstraps as the
//! sole voter, then brokers 1..n boot in `Join` mode with `auto_join = true`
//! and grow the quorum by sending `AddRaftVoter(self)` to the leader over the
//! wire. The shrink test then drives `remove_voter` on the leader and asserts
//! the committed voter set contracts.
//!
//! openraft's debug assertions race on the hosted Windows scheduler, so these
//! are gated off Windows like the other multi-node suites.

use assert2::assert;

use crabka_raft::reconfig::{ReconfigOutcome, RemoveVoter};

mod support;
use support::start_n_node;

/// Auto-join must grow a fresh cluster from one voter to three: broker 0
/// bootstraps alone, brokers 1 and 2 join over the wire. `start_n_node`
/// already waits for convergence, but we re-assert here against the leader's
/// committed image so the test fails loudly (rather than the harness's
/// `Startup` error) if convergence regresses.
#[tokio::test]
#[ignore = "KIP-853 dynamic reconfig: Slice 5"]
async fn auto_join_grows_quorum_to_three() {
    let cluster = start_n_node(3).await.expect("3-node cluster via auto-join");

    // broker 0 is the bootstrap node and the initial (only) leader.
    let leader = &cluster[0].0;

    leader.wait_for_image(|img| img.voters().len() == 3).await;

    // Every node should eventually agree on the 3-voter set, not just the
    // leader.
    for (h, _, _) in &cluster {
        h.wait_for_image(|img| img.voters().len() == 3).await;
    }
}

/// After growing to three, removing one follower via the leader's
/// `remove_voter` must shrink the committed voter set to two.
#[tokio::test]
#[ignore = "KIP-853 dynamic reconfig: Slice 5"]
async fn remove_voter_shrinks_quorum() {
    let cluster = start_n_node(3).await.expect("3-node cluster via auto-join");

    let leader = &cluster[0].0;
    let leader_id = leader.node_id();

    leader.wait_for_image(|img| img.voters().len() == 3).await;

    // Pick a follower (any voter that isn't the leader) and read its
    // directory id straight from the committed image — `remove_voter` keys on
    // (id, directory_id).
    let victim = leader
        .voter_ids_for_test()
        .into_iter()
        .find(|&id| id != leader_id)
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
