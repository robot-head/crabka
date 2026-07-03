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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::assert;
use bytes::Bytes;
use crabka_raft::{
    RaftError,
    kraft::{
        KraftConfig, KraftController, KraftLog, NodeId, PeerSender, QuorumState,
        transport::{Inbound, api_key},
    },
};
use tokio::sync::oneshot;

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
            api_key::FETCH_SNAPSHOT => Inbound::FetchSnapshot { req: body, reply },
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
    build_engine_with_snapshot_interval(me, ids, cluster_id, election_timeout_ms, net, 0)
}

/// Like [`build_engine`] but with a caller-chosen `snapshot_interval_records`
/// (`0` disables snapshotting). The snapshot catch-up acceptance uses a small
/// interval so the leader snapshots + prunes its log after a short burst.
fn build_engine_with_snapshot_interval(
    me: NodeId,
    ids: &[NodeId],
    cluster_id: uuid::Uuid,
    election_timeout_ms: u64,
    net: &SimNet,
    snapshot_interval_records: u64,
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
            snapshot_interval_records,
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
        tokio::task::yield_now().await;
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
        tokio::task::yield_now().await;
    }
}

/// 1. Three engines elect exactly one leader and agree on the epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_engines_elect_one_leader() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
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

/// 1b. A bare majority (exactly 2 of a 3-voter set) elects a stable leader even
///     with UNIFORM election timeouts and in-process lockstep. This guards the
///     split-vote livelock fix: without per-(node, epoch) election-timeout jitter
///     the two closely-synchronized voters both become candidates every round,
///     self-vote, and never reach majority (the 3rd voter is down) — churning for
///     tens of seconds. `start_n_node`-style "all voters up" topologies never
///     exercised this; the mixed JVM+Crabka quorum (JVM boots slowly) did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bare_majority_two_of_three_elects_with_uniform_timeouts() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(150);

    // UNIFORM timeout for both live voters (no manual stagger) — the production
    // controller config uses a single election timeout for every node. Only
    // voters 1 and 2 are started; voter 3 stays down, so {1,2} is the bare
    // majority of the 3-voter set.
    let mut dirs = Vec::new();
    for &id in &[NodeId(1), NodeId(2)] {
        let (ctrl, dir) = build_engine(id, &ids, cid, 200, &net);
        net.register(id, ctrl);
        dirs.push(dir);
    }

    // Must converge quickly via self-staggering; without the jitter fix this
    // livelocks well past the deadline.
    let (leader, epoch) =
        await_single_leader(&net, &[NodeId(1), NodeId(2)], Duration::from_secs(8)).await;
    assert!(epoch >= 1);
    assert!(
        leader == NodeId(1) || leader == NodeId(2),
        "leader must be a live voter"
    );

    for &id in &[NodeId(1), NodeId(2)] {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 2. `submit_change` on a follower forwards to the leader, commits via
///    record-carrying replication, and the topic appears in ALL three images.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_submit_change_propagates() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
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
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
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
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
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
    // intentional: let the shutdown-signalled engine task exit and drop its
    // KraftLog before we reopen the same data dir. `shutdown()` only sends
    // `Command::Shutdown`; the loop is spawned fire-and-forget with no JoinHandle,
    // so there is no accessor to await loop teardown / log-handle release.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let victim_dir = dirs.get(&victim).unwrap().path().to_path_buf();
    let reopened = KraftController::open(
        victim_dir,
        victim,
        cid,
        voter_set(&ids),
        timeouts[usize::try_from(victim.0 - 1).unwrap()],
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

/// 5. KIP-630 snapshot catch-up (the Slice-4 acceptance): a lagging controller
///    follower whose own log is empty and far behind the leader's pruned
///    `log_start` catches up purely via `FetchSnapshot`, not log replication.
///
///    Topology: all three voters are configured up front (so an election can
///    reach a majority), but the lagging node's engine is started LATE, on a
///    fresh empty tempdir. The two timely voters (leader + one follower) commit
///    a burst of distinct metadata records larger than `snapshot_interval_records`,
///    which forces the leader to write a checkpoint and prune its log
///    (`log_start_offset` advances past 0). When the lagging node finally joins,
///    its `LEO == 0 < leader.log_start`, so its first Fetch is answered with a
///    `snapshot_id`; the engine then runs the `FetchSnapshot` loop, installs the
///    snapshot, and resumes a normal Fetch from the snapshot boundary. The
///    follower's published `MetadataImage` must converge to the leader's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_via_snapshot() {
    let net = SimNet::new();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cid = uuid::Uuid::from_u128(500);
    // Snapshot after every 5 committed records past the last checkpoint.
    let interval = 5u64;

    // Start only TWO voters (leader + one follower): that is a majority of three,
    // so submits commit while the third node stays down. Staggered timeouts so
    // node 1 reliably wins. Node 3 is the lagging node, started later.
    let timeouts = [150u64, 300, 450];
    let mut dirs: HashMap<NodeId, tempfile::TempDir> = HashMap::new();
    for &id in &[NodeId(1), NodeId(2)] {
        let idx = usize::try_from(id.0 - 1).unwrap();
        let (ctrl, dir) = build_engine_with_snapshot_interval(
            id,
            &ids, // full voter set: the quorum is three even though one is down
            cid,
            timeouts[idx],
            &net,
            interval,
        );
        net.register(id, ctrl);
        dirs.insert(id, dir);
    }

    // The two live voters elect a leader among themselves (two of three is a
    // majority). The lagging node 3 is down, so only poll the live pair.
    let live = [NodeId(1), NodeId(2)];
    let (leader, _epoch) = await_single_leader(&net, &live, Duration::from_secs(10)).await;

    // Commit MORE than `interval` distinct topics so the leader snapshots and
    // prunes at least once. Distinct names make the image grow per record.
    let burst = usize::try_from(interval).unwrap() * 3; // comfortably past the threshold
    for i in 0..burst {
        tokio::time::timeout(
            Duration::from_secs(10),
            net.get(leader)
                .unwrap()
                .submit_change(vec![topic_record(&format!("t{i}"), 1000 + i as u128)]),
        )
        .await
        .expect("burst submit did not hang")
        .expect("burst submit ok");
    }

    // The leader must have snapshotted and pruned: its log_start advanced past 0.
    // Poll briefly — the prune happens on the apply that crosses the threshold,
    // which is synchronous with the last submit's commit, but give the watch a
    // moment to republish the quorum snapshot.
    let leader_ctrl = net.get(leader).unwrap();
    await_until(Duration::from_secs(10), || {
        (leader_ctrl.quorum_snapshot().log_start_offset > 0).then_some(())
    })
    .await;
    let leader_log_start = leader_ctrl.quorum_snapshot().log_start_offset;
    assert!(
        leader_log_start > 0,
        "leader did not prune its log (log_start_offset still 0); snapshot never happened"
    );

    // Capture the leader's converged image to compare against.
    let leader_image = leader_ctrl.current_image();
    // Sanity: every burst topic is in the leader image.
    for i in 0..burst {
        assert!(
            leader_image.topic(&format!("t{i}")).is_some(),
            "leader image missing burst topic t{i}"
        );
    }

    // Now bring the lagging node 3 up on a FRESH empty tempdir: its LEO is 0,
    // far below the leader's pruned log_start, so it can ONLY catch up by
    // fetching the snapshot.
    let (lag_ctrl, lag_dir) =
        build_engine_with_snapshot_interval(NodeId(3), &ids, cid, timeouts[2], &net, interval);
    net.register(NodeId(3), lag_ctrl);
    dirs.insert(NodeId(3), lag_dir);

    // Wait until the lagging follower's image equals the leader's. Catch-up runs
    // through the FetchSnapshot path (its LEO 0 < leader.log_start), reassembling
    // and installing the snapshot, then resuming normal fetch.
    let lag = net.get(NodeId(3)).unwrap();
    let want = leader_image.clone();
    await_until(Duration::from_secs(10), || {
        (*lag.current_image() == *want).then_some(())
    })
    .await;

    assert!(
        *lag.current_image() == *leader_image,
        "lagging follower image did not converge to the leader's via snapshot"
    );
    // It really used a snapshot: the follower's log_start is at the snapshot
    // boundary, not 0 (a pure log replication from 0 would leave it at 0).
    let lag_snap = lag.quorum_snapshot();
    assert!(
        lag_snap.log_start_offset > 0,
        "lagging follower's log_start is still 0 — it did not install a snapshot"
    );

    for &id in &ids {
        if let Some(c) = net.get(id) {
            c.shutdown().await;
        }
    }
}
