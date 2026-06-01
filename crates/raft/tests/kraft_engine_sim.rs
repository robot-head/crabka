//! Multi-node `KraftController` async driver simulation — the **isolation
//! acceptance** for the KIP-595 consensus engine (Slice 3c, Task 6).
//!
//! Three real [`KraftController`]s run over tempdir [`KraftLog`]s on a tokio
//! multi-thread runtime, wired to each other through an in-memory [`PeerSender`]
//! ([`SimNet`]) — no TCP. Each engine's `PeerSender` routes a
//! `(peer, api_key, body)` to the target engine's [`KraftController::deliver`]
//! and returns the response body. Because every engine's loop is non-blocking
//! (peer sends are spawned fire-and-forget; the loop never `.await`s a send
//! inline), reciprocal RPCs between engines cannot deadlock.
//!
//! This exercises the real engine/loop/log/apply path — election, record-carrying
//! Fetch replication, leader failover, and restart recovery — deterministically
//! enough to be the debugging anchor when the TCP integration (Task 10)
//! misbehaves.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use tokio::sync::oneshot;

use crabka_raft::RaftError;
use crabka_raft::kraft::transport::{Inbound, api_key};
use crabka_raft::kraft::{KraftConfig, KraftController, KraftLog, NodeId, PeerSender, QuorumState};

/// Shared registry of in-process engines, keyed by node id. Each engine holds a
/// clone of one of these (via [`SimNet`]) so its outbound peer sends can reach
/// the others. A node can be removed (leader-kill / restart) by dropping it from
/// the registry: subsequent sends to it fail as unreachable, mirroring a crash.
#[derive(Default)]
struct Registry {
    nodes: HashMap<NodeId, KraftController>,
}

/// An in-memory [`PeerSender`]: routes the encoded request body to the target
/// engine's [`KraftController::deliver`] and awaits the oneshot reply.
#[derive(Clone)]
struct SimNet {
    registry: Arc<Mutex<Registry>>,
}

impl SimNet {
    fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
        }
    }

    fn register(&self, id: NodeId, ctrl: KraftController) {
        self.registry.lock().unwrap().nodes.insert(id, ctrl);
    }

    fn remove(&self, id: NodeId) {
        self.registry.lock().unwrap().nodes.remove(&id);
    }

    fn get(&self, id: NodeId) -> Option<KraftController> {
        self.registry.lock().unwrap().nodes.get(&id).cloned()
    }
}

#[async_trait::async_trait]
impl PeerSender for SimNet {
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        // Look up the target engine. A removed/crashed node is unreachable.
        let target = self.get(peer).ok_or(RaftError::NotLeader {
            current_leader: None,
        })?;
        let (reply, rx) = oneshot::channel();
        let inbound = match api_key {
            api_key::VOTE => Inbound::Vote { req: body, reply },
            api_key::BEGIN_QUORUM_EPOCH => Inbound::BeginQuorumEpoch { req: body, reply },
            api_key::END_QUORUM_EPOCH => Inbound::EndQuorumEpoch { req: body, reply },
            api_key::FETCH => Inbound::Fetch { req: body, reply },
            other => panic!("sim: unexpected api_key {other}"),
        };
        // Deliver to the target loop (non-blocking enqueue) and await its reply.
        // The loop processes inbound concurrently with our caller's loop, so this
        // never deadlocks even when engines RPC each other reciprocally.
        target
            .deliver(inbound)
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}

fn voter_set(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
    crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
        crabka_metadata::voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
        }
    }))
}

fn topic_record(name: &str, id: u128) -> crabka_metadata::MetadataRecord {
    crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
        name: name.to_string(),
        topic_id: uuid::Uuid::from_u128(id),
        partitions: 1,
        replication_factor: 1,
    })
}

/// Build (but do not register) a single engine over a fresh tempdir log.
fn build_engine(
    me: NodeId,
    ids: &[NodeId],
    cluster_id: uuid::Uuid,
    election_timeout_ms: u64,
    net: &SimNet,
) -> (KraftController, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path()).expect("open log");
    let ctrl = KraftController::spawn(
        KraftConfig {
            me,
            cluster_id,
            initial_state: QuorumState::bootstrap(cluster_id, voter_set(ids)),
            election_timeout_ms,
            peers: Arc::new(net.clone()),
            snapshot_interval_records: 0,
        },
        log,
        dir.path().to_path_buf(),
    );
    (ctrl, dir)
}

/// Poll `f` until it returns `Some`, bounded by `timeout`. Yields between polls
/// so the engine loops make progress. Returns the value or panics on timeout.
async fn await_until<T, F>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Option<T>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "await_until timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The set of `(node, leader_id, leader_epoch)` each live engine currently
/// believes, read via the (non-mutating) `quorum_state` handle op.
async fn leaders(net: &SimNet, ids: &[NodeId]) -> Vec<(NodeId, Option<NodeId>, u32)> {
    let mut out = Vec::new();
    for &id in ids {
        if let Some(ctrl) = net.get(id)
            && let Ok(qs) = ctrl.quorum_state().await
        {
            out.push((id, qs.leader_id, qs.leader_epoch));
        }
    }
    out
}

/// Wait until exactly one node believes itself leader and all live nodes agree
/// on that leader id + epoch. Returns `(leader_id, epoch)`.
async fn await_single_leader(net: &SimNet, ids: &[NodeId], timeout: Duration) -> (NodeId, u32) {
    let net = net.clone();
    let ids = ids.to_vec();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let snap = leaders(&net, &ids).await;
        // All live nodes must report the same Some(leader) and same epoch, and
        // that leader must be one of the live nodes.
        if !snap.is_empty() {
            let first_leader = snap[0].1;
            let first_epoch = snap[0].2;
            if let Some(leader) = first_leader {
                let agree = snap
                    .iter()
                    .all(|(_, l, e)| *l == Some(leader) && *e == first_epoch);
                let live = ids.contains(&leader);
                if agree && live {
                    return (leader, first_epoch);
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no single agreed leader within timeout: {:?}",
            leaders(&net, &ids).await
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// 1. Three engines elect exactly one leader and agree on the epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_engines_elect_one_leader() {
    let net = SimNet::new();
    let ids = [1u64, 2, 3];
    let cid = uuid::Uuid::from_u128(100);

    // Staggered election timeouts so one node reliably wins the first round.
    let timeouts = [150u64, 300, 450];
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    let (leader, epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;
    assert!(
        epoch >= 1,
        "leader epoch should have advanced past bootstrap"
    );
    // Exactly one node reports itself as the leader.
    let mut self_leaders = 0;
    for &id in &ids {
        let qs = net.get(id).unwrap().quorum_state().await.unwrap();
        if qs.leader_id == Some(id) {
            self_leaders += 1;
        }
    }
    assert!(
        self_leaders == 1,
        "exactly one self-leader, got {self_leaders}"
    );
    assert!(ids.contains(&leader));

    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 2. `submit_change` on a follower forwards to the leader, commits via
///    record-carrying replication, and the topic appears in ALL three images.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_submit_change_propagates() {
    let net = SimNet::new();
    let ids = [1u64, 2, 3];
    let cid = uuid::Uuid::from_u128(200);

    let timeouts = [150u64, 300, 450];
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    let (leader, _epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Submit on a FOLLOWER. The follower rejects with NotLeader{leader}; we then
    // submit to the leader (the handle layer's forward is Task 8 — here we drive
    // the forward explicitly via the leader handle the hint points at).
    let follower = *ids.iter().find(|&&id| id != leader).unwrap();
    let fol = net.get(follower).unwrap();
    let res = fol.submit_change(vec![topic_record("orders", 1)]).await;
    let leader_hint = match res {
        Err(RaftError::NotLeader { current_leader }) => current_leader,
        other => panic!("follower submit should reject with NotLeader, got {other:?}"),
    };
    assert!(
        leader_hint == Some(leader),
        "leader hint should point at the elected leader"
    );

    // Forward to the leader (record-carrying replication commits it on a majority).
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(leader)
            .unwrap()
            .submit_change(vec![topic_record("orders", 1)]),
    )
    .await
    .expect("leader submit did not hang")
    .expect("leader submit ok");

    // The topic must appear in ALL three engines' current_image (real replication
    // carried the record bytes to the followers, which applied on HWM advance).
    for &id in &ids {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
            ctrl.current_image().topic("orders").map(|_| ())
        })
        .await;
        assert!(
            ctrl.current_image().topic("orders").is_some(),
            "node {id} missing replicated topic"
        );
    }

    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 3. Killing the leader → the remaining two re-elect a single new leader, and a
///    `submit_change` to the new leader commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_reelects() {
    let net = SimNet::new();
    let ids = [1u64, 2, 3];
    let cid = uuid::Uuid::from_u128(300);

    let timeouts = [150u64, 300, 450];
    let mut dirs = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    let (leader, epoch1) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Kill the leader: shut it down and remove it from the registry so peers see
    // it as unreachable.
    net.get(leader).unwrap().shutdown().await;
    net.remove(leader);

    // The two survivors must elect a NEW single leader at a higher epoch.
    let survivors: Vec<NodeId> = ids.iter().copied().filter(|&id| id != leader).collect();
    let (new_leader, epoch2) = await_single_leader(&net, &survivors, Duration::from_secs(15)).await;
    assert!(new_leader != leader, "a new leader must be chosen");
    assert!(epoch2 > epoch1, "new term must have a higher epoch");

    // A submit to the new leader commits across the two survivors.
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(new_leader)
            .unwrap()
            .submit_change(vec![topic_record("post-failover", 7)]),
    )
    .await
    .expect("post-failover submit did not hang")
    .expect("post-failover submit ok");

    for &id in &survivors {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
            ctrl.current_image().topic("post-failover").map(|_| ())
        })
        .await;
    }

    for &id in &survivors {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 4. Restart recovery: commit, snapshot, drop one engine, reopen over its dir,
///    assert the image is rebuilt from checkpoint + log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_image() {
    let net = SimNet::new();
    let ids = [1u64, 2, 3];
    let cid = uuid::Uuid::from_u128(400);

    let timeouts = [150u64, 300, 450];
    // Keep per-node data dirs so we can reopen one.
    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for (i, &id) in ids.iter().enumerate() {
        let (ctrl, dir) = build_engine(id, &ids, cid, timeouts[i], &net);
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    let (leader, _epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Commit a topic and ensure it is replicated everywhere.
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(leader)
            .unwrap()
            .submit_change(vec![topic_record("persistent", 9)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit ok");

    for &id in &ids {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
            ctrl.current_image().topic("persistent").map(|_| ())
        })
        .await;
    }

    // Pick a follower to restart so the cluster keeps a leader meanwhile.
    let victim = *ids.iter().find(|&&id| id != leader).unwrap();
    let victim_ctrl = net.get(victim).unwrap();
    // Snapshot the victim's image, then drop it.
    victim_ctrl.trigger_snapshot().await.unwrap();
    victim_ctrl.shutdown().await;
    net.remove(victim);
    // Let the loop drain.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let victim_dir = dirs.get(&victim).unwrap().path().to_path_buf();
    let reopened = KraftController::open(
        victim_dir,
        victim,
        cid,
        voter_set(&ids),
        timeouts[usize::try_from(victim - 1).unwrap()],
        Arc::new(net.clone()),
        0,
    )
    .expect("reopen");
    // The recovered image must contain the committed topic.
    assert!(
        reopened.current_image().topic("persistent").is_some(),
        "reopened node did not recover its image"
    );
    net.register(victim, reopened);

    for &id in &ids {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
