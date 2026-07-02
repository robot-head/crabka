// Tests the coordinator's safety guards against an in-memory mock (no real raft).
use std::{collections::BTreeSet, sync::Mutex as StdMutex};

use assert2::assert;
use crabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
use crabka_raft::{
    Node, NodeId, RaftError,
    reconfig::{AddVoter, Coordinator, ReconfigOps, ReconfigOutcome, RemoveVoter, UpdateVoter},
};

#[derive(Default)]
struct MockState {
    voters: VoterSet,
    leader_index: u64,
    observer_index: std::collections::HashMap<NodeId, u64>,
    is_leader: bool,
    submitted: Vec<crabka_metadata::MetadataRecord>,
    membership: Option<BTreeSet<NodeId>>,
}

struct Mock(StdMutex<MockState>);

#[async_trait::async_trait]
impl ReconfigOps for Mock {
    fn current_voters(&self) -> VoterSet {
        self.0.lock().unwrap().voters.clone()
    }
    fn leader(&self) -> Option<NodeId> {
        Some(1)
    }
    fn is_leader(&self) -> bool {
        self.0.lock().unwrap().is_leader
    }
    fn leader_last_index(&self) -> u64 {
        self.0.lock().unwrap().leader_index
    }
    fn observer_index(&self, id: NodeId) -> Option<u64> {
        self.0.lock().unwrap().observer_index.get(&id).copied()
    }
    async fn add_learner(&self, id: NodeId, _node: Node) -> Result<(), RaftError> {
        self.0.lock().unwrap().observer_index.entry(id).or_insert(0);
        Ok(())
    }
    async fn change_membership(&self, ids: BTreeSet<NodeId>) -> Result<(), RaftError> {
        self.0.lock().unwrap().membership = Some(ids);
        Ok(())
    }
    async fn submit_records(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), RaftError> {
        self.0.lock().unwrap().submitted.extend(records);
        Ok(())
    }
}

fn voter(id: NodeId) -> Voter {
    Voter {
        id,
        directory_id: uuid::Uuid::from_u128(u128::from(id)),
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
        }],
        kraft_version: KRaftVersionRange::default(),
    }
}

#[tokio::test]
async fn add_voter_rejects_lagging_observer() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        leader_index: 1000,
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let err = coord
        .add_voter(AddVoter { voter: voter(2) })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::VoterNotCaughtUp { id: 2, .. }));
}

#[tokio::test]
async fn add_voter_succeeds_when_caught_up() {
    let mut observer_index = std::collections::HashMap::new();
    observer_index.insert(2u64, 1000u64);
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        leader_index: 1000,
        is_leader: true,
        observer_index,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let out = coord.add_voter(AddVoter { voter: voter(2) }).await.unwrap();
    assert!(out == ReconfigOutcome::Committed);
    let st = mock.0.lock().unwrap();
    assert!(st.membership.as_ref().unwrap() == &BTreeSet::from([1, 2]));
    assert!(st.submitted.len() == 1); // one V1Voters record
}

#[tokio::test]
async fn add_voter_accepts_observer_at_lag_bound() {
    let mut observer_index = std::collections::HashMap::new();
    observer_index.insert(2u64, 990u64);
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        leader_index: 1000,
        is_leader: true,
        observer_index,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);

    let out = coord.add_voter(AddVoter { voter: voter(2) }).await.unwrap();

    assert!(out == ReconfigOutcome::Committed);
    let st = mock.0.lock().unwrap();
    assert!(st.membership.as_ref().unwrap() == &BTreeSet::from([1, 2]));
    assert!(st.submitted.len() == 1);
}

#[tokio::test]
async fn remove_last_voter_is_rejected() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let err = coord
        .remove_voter(RemoveVoter {
            id: 1,
            directory_id: uuid::Uuid::from_u128(1),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::ReconfigRejected(_)));
}

#[tokio::test]
async fn add_voter_on_follower_reports_not_leader() {
    let mock = Mock(StdMutex::new(MockState {
        is_leader: false,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let out = coord.add_voter(AddVoter { voter: voter(2) }).await.unwrap();
    assert!(matches!(out, ReconfigOutcome::NotLeader { .. }));
}

#[tokio::test]
async fn second_reconfig_reports_in_progress_when_lock_held() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1), voter(2)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    // Hold the coordinator's lock externally to simulate an in-flight reconfig.
    let _held = lock.lock().await;
    let coord = Coordinator::new(&mock, &lock, 10);
    let err = coord
        .remove_voter(RemoveVoter {
            id: 2,
            directory_id: uuid::Uuid::from_u128(2),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::ReconfigInProgress));
}

#[tokio::test]
async fn update_voter_submits_record_without_membership_change() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1), voter(2)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let mut updated = voter(2);
    updated.endpoints = vec![VoterEndpoint {
        name: "CONTROLLER".into(),
        host: "10.0.0.2".into(),
        port: 9094,
    }];
    let out = coord
        .update_voter(UpdateVoter { voter: updated })
        .await
        .unwrap();
    assert!(out == ReconfigOutcome::Committed);
    let st = mock.0.lock().unwrap();
    assert!(st.membership.is_none()); // id set unchanged -> no change_membership
    assert!(st.submitted.len() == 1); // one V1Voters record
}

#[tokio::test]
async fn remove_non_last_voter_succeeds() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1), voter(2)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let out = coord
        .remove_voter(RemoveVoter {
            id: 2,
            directory_id: uuid::Uuid::from_u128(2),
        })
        .await
        .unwrap();
    assert!(out == ReconfigOutcome::Committed);
    let st = mock.0.lock().unwrap();
    assert!(st.membership.as_ref().unwrap() == &BTreeSet::from([1]));
    assert!(st.submitted.len() == 1); // one V1Voters record
}

#[tokio::test]
async fn remove_voter_with_mismatched_directory_id_is_noop() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1), voter(2)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    // Stale request targeting an old incarnation of node 2 (wrong directory_id).
    let out = coord
        .remove_voter(RemoveVoter {
            id: 2,
            directory_id: uuid::Uuid::from_u128(999),
        })
        .await
        .unwrap();
    assert!(out == ReconfigOutcome::Committed); // idempotent no-op
    let st = mock.0.lock().unwrap();
    assert!(st.membership.is_none()); // current voter must NOT be removed
    assert!(st.submitted.is_empty());
}
